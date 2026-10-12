"""Integration tests exercising the transactional outbox, the RabbitMQ
relay, and the worker -- against the REAL PostgreSQL and RabbitMQ started
by docker-compose.yml. No mocks for the database or broker: this is the
suite that proves the end-to-end pipeline, not just that each class method
runs.

Backoff timing is made test-fast via a custom Settings instance (tiny
base/max backoff) injected into the worker -- the retry/DLQ *logic* is
exercised for real, just without waiting on production-sized delays.
"""
import asyncio
import json
import time
import uuid

from sqlalchemy import select

from app.core.config import Settings
from app.core.constants import TaskStatus
from app.core.serialization import payload_to_json
from app.db.models import OutboxEventDB, TaskDB
from app.db.repositories import OutboxRepository, TaskRepository
from app.mq.publisher import MessagePublisher
from app.outbox.relay import OutboxRelay
from app.workers.worker import SimpleWorker, default_handlers


def _fast_settings() -> Settings:
    return Settings(
        max_task_retries=3,
        retry_backoff_base_seconds=0.2,
        retry_backoff_max_seconds=1.0,
        worker_concurrency=5,
    )


async def _create_task_with_outbox_event(db, task_type: str, payload: dict) -> str:
    """Mirror what the API endpoint does: task + outbox event, one transaction."""
    task_id = str(uuid.uuid4())
    async with db() as session:
        task_repo = TaskRepository(session)
        outbox_repo = OutboxRepository(session)

        task = TaskDB(
            id=task_id,
            task_type=task_type,
            payload=payload_to_json(payload),
            status=TaskStatus.queued.value,
            attempts=0,
            max_attempts=3,
        )
        await task_repo.create(task)
        await outbox_repo.create(
            task_id=task_id,
            event_type="task_created",
            payload=payload_to_json({"task_type": task_type, "payload": payload}),
        )
        await session.commit()
    return task_id


async def _run_worker_until(
    worker: SimpleWorker, task_repo_factory, task_id, terminal_statuses, timeout=10
):
    """Run the worker in the background until the task reaches a terminal status.

    IMPORTANT: each poll opens its OWN session. Reusing one session/identity
    map across the whole wait would silently return the same cached ORM
    object every time -- SQLAlchemy doesn't refresh an already-loaded
    instance's attributes just because another transaction updated the row
    -- so the loop would spin on a stale "queued" status forever even after
    the worker's own (separate) session committed "succeeded".
    """
    run_task = asyncio.create_task(worker.start())
    try:
        async def poll():
            while True:
                async with task_repo_factory() as session:
                    repo = TaskRepository(session)
                    task = await repo.get(task_id)
                if task and task.status in terminal_statuses:
                    return task
                await asyncio.sleep(0.1)

        return await asyncio.wait_for(poll(), timeout=timeout)
    finally:
        await worker.stop()
        await asyncio.wait_for(run_task, timeout=5)


async def _get_dead_letter_message_for(mq, task_id: str, timeout: float = 5.0) -> dict:
    """Return the decoded body of the "tasks.dead_letter" message for task_id.

    Matches on task_id instead of taking whatever message is at the head of
    the queue, so a leftover message from another run can't make a test pass.
    Non-matching messages are requeued untouched; the matching one is acked.
    """
    queue = await mq.channel.get_queue("tasks.dead_letter")
    skipped = []
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = await queue.get(timeout=5, fail=False)
            if message is None:
                await asyncio.sleep(0.1)
                continue
            body = json.loads(message.body.decode())
            if body.get("task_id") == task_id:
                await message.ack()
                return body
            skipped.append(message)
        raise AssertionError(f"no dead-letter message for task {task_id} within {timeout}s")
    finally:
        for message in skipped:
            await message.nack(requeue=True)


async def _outbox_event(db, task_id: str, event_type: str) -> OutboxEventDB:
    async with db() as session:
        result = await session.execute(
            select(OutboxEventDB).where(
                OutboxEventDB.task_id == task_id, OutboxEventDB.event_type == event_type
            )
        )
        return result.scalar_one()


async def _relay_dead_letter_notice(db, mq, task_id: str) -> dict:
    """The worker only writes the task_dead_lettered outbox event; the relay
    publishes it. Check both halves and return the tasks.dead_letter body."""
    event = await _outbox_event(db, task_id, "task_dead_lettered")
    assert (event.status, event.published) == ("pending", False)

    await OutboxRelay(session_factory=db, mq=mq).run_once()

    event = await _outbox_event(db, task_id, "task_dead_lettered")
    assert (event.status, event.published) == ("published", True)
    return await _get_dead_letter_message_for(mq, task_id)


async def test_outbox_event_created_in_same_transaction(db):
    """Task row and outbox row both exist right after the one commit -- no
    separate publish step has run yet."""
    task_id = await _create_task_with_outbox_event(db, "test_outbox_tx", {})

    async with db() as session:
        task_repo = TaskRepository(session)
        outbox_repo = OutboxRepository(session)
        task = await task_repo.get(task_id)
        assert task is not None
        assert task.status == "queued"

        pending = await outbox_repo.get_pending(limit=100)
        assert any(e.task_id == task_id and e.event_type == "task_created" for e in pending)


async def test_relay_publishes_pending_event_to_rabbitmq(db, mq):
    task_id = await _create_task_with_outbox_event(db, "test_relay_publish", {})

    relay = OutboxRelay(session_factory=db, mq=mq, poll_interval_seconds=0.1)
    published_count = await relay.run_once()
    assert published_count >= 1

    async with db() as session:
        outbox_repo = OutboxRepository(session)
        pending = await outbox_repo.get_pending(limit=100)
    assert not any(e.task_id == task_id for e in pending), "event should be marked published"

    # Confirm THIS task's message actually arrived on the real "tasks" queue
    # (matched by task_id, so a leftover message can't satisfy the check).
    queue = await mq.channel.get_queue("tasks")
    others, body = [], None
    try:
        while (message := await queue.get(timeout=5, fail=False)) is not None:
            decoded = json.loads(message.body.decode())
            if decoded.get("task_id") == task_id:
                body = decoded
                await message.ack()
                break
            others.append(message)
    finally:
        for message in others:
            await message.nack(requeue=True)
    assert body is not None, "task_created was not delivered to the tasks queue"
    assert body["task_type"] == "test_relay_publish"


async def test_worker_processes_task_end_to_end(db, mq):
    """Full pipeline: outbox -> relay -> RabbitMQ -> worker -> DB succeeded."""
    task_id = await _create_task_with_outbox_event(db, "test_worker_e2e", {})

    relay = OutboxRelay(session_factory=db, mq=mq)
    await relay.run_once()

    worker = SimpleWorker(
        mq=mq, session_factory=db, handlers=default_handlers(), app_settings=_fast_settings()
    )
    task = await _run_worker_until(worker, db, task_id, {"succeeded", "failed", "dead_lettered"})
    assert task.status == "succeeded"
    assert task.result is not None

    created = await _outbox_event(db, task_id, "task_created")
    assert (created.status, created.published) == ("published", True)
    # task_succeeded has no broker consumer: the relay records it but must
    # not claim it was published.
    await OutboxRelay(session_factory=db, mq=mq).run_once()
    succeeded = await _outbox_event(db, task_id, "task_succeeded")
    assert (succeeded.status, succeeded.published) == ("recorded", False)


async def test_worker_idempotent_on_redelivered_terminal_task(db, mq):
    """Re-publishing a task that's already succeeded must be a no-op, not a
    double-processing bug (this is the guarantee that makes at-least-once
    outbox delivery safe)."""
    task_id = await _create_task_with_outbox_event(db, "test_worker_idem", {})
    relay = OutboxRelay(session_factory=db, mq=mq)
    await relay.run_once()

    settings = _fast_settings()
    worker_kwargs = dict(
        mq=mq, session_factory=db, handlers=default_handlers(), app_settings=settings
    )
    worker = SimpleWorker(**worker_kwargs)
    task = await _run_worker_until(worker, db, task_id, {"succeeded"})
    assert task.status == "succeeded"
    first_updated_at = task.updated_at

    # Simulate an at-least-once redelivery of the same "task_created" message.
    publisher = MessagePublisher(mq.channel)
    await publisher.publish_task(task_id=task_id, task_type="test_worker_idem", payload={})

    worker2 = SimpleWorker(**worker_kwargs)
    run_task = asyncio.create_task(worker2.start())
    await asyncio.sleep(1.0)  # give it a chance to (wrongly) reprocess if it were buggy
    await worker2.stop()
    await asyncio.wait_for(run_task, timeout=5)

    async with db() as session:
        task_repo = TaskRepository(session)
        task = await task_repo.get(task_id)
    assert task.status == "succeeded"
    assert task.attempts == 0  # untouched by the redelivery
    assert task.updated_at == first_updated_at


async def test_worker_retries_transient_failure_then_succeeds(db, mq, created_task_ids):
    # task_type MUST be "flaky" to hit default_handlers()'s registered
    # handler -- cleanup can't rely on the "test_*" prefix here, so the
    # task_id is tracked explicitly via created_task_ids instead.
    task_id = await _create_task_with_outbox_event(db, "flaky", {"fail_until_attempt": 2})
    created_task_ids.append(task_id)
    relay = OutboxRelay(session_factory=db, mq=mq)
    await relay.run_once()

    worker = SimpleWorker(
        mq=mq, session_factory=db, handlers=default_handlers(), app_settings=_fast_settings()
    )
    task = await _run_worker_until(
        worker, db, task_id, {"succeeded", "failed", "dead_lettered"}, timeout=15
    )
    assert task.status == "succeeded"
    assert task.attempts == 1  # failed once (attempt 1), succeeded on attempt 2


async def test_worker_dead_letters_permanent_failure_immediately(db, mq, created_task_ids):
    # task_type MUST be "always_fail" to hit default_handlers()'s registered
    # handler; see the comment in the retry test above re: cleanup tracking.
    task_id = await _create_task_with_outbox_event(db, "always_fail", {})
    created_task_ids.append(task_id)
    relay = OutboxRelay(session_factory=db, mq=mq)
    await relay.run_once()

    worker = SimpleWorker(
        mq=mq, session_factory=db, handlers=default_handlers(), app_settings=_fast_settings()
    )
    task = await _run_worker_until(worker, db, task_id, {"dead_lettered"}, timeout=5)
    assert task.status == "dead_lettered"
    assert task.attempts == 0  # permanent errors skip the retry counter entirely
    assert "permanent" in task.error.lower()

    dead_letter = await _relay_dead_letter_notice(db, mq, task_id)
    assert dead_letter["task_type"] == "always_fail"
    assert dead_letter["label"] == "dead_letter"
    assert "permanent" in dead_letter["reason"].lower()


async def test_worker_dead_letters_after_exhausting_transient_retries(db, mq, created_task_ids):
    """Transient failures on every attempt: the task goes through the
    tasks.retry delay queue (per-message TTL, DLX back onto "tasks") until
    max_attempts is used up, then ends dead_lettered with a message on
    tasks.dead_letter. The worker only ever consumes "tasks", so reaching
    attempt 3 at all proves the TTL + DLX round trip happened twice."""
    # fail_until_attempt=99 > max_attempts=3 -> every attempt fails transiently.
    task_id = await _create_task_with_outbox_event(db, "flaky", {"fail_until_attempt": 99})
    created_task_ids.append(task_id)
    relay = OutboxRelay(session_factory=db, mq=mq)
    await relay.run_once()

    settings = _fast_settings()  # backoff 0.2s, 0.4s (base 0.2, doubling, max 1.0)
    worker = SimpleWorker(
        mq=mq, session_factory=db, handlers=default_handlers(), app_settings=settings
    )
    started = time.monotonic()
    task = await _run_worker_until(worker, db, task_id, {"succeeded", "dead_lettered"}, timeout=15)
    elapsed = time.monotonic() - started

    assert task.status == "dead_lettered"
    assert task.attempts == task.max_attempts == 3
    assert task.error_category == "permanent"
    assert task.error.startswith("retries exhausted (3/3)")
    assert "simulated transient failure on attempt 3" in task.error
    # Two backoff delays (0.2s + 0.4s) must actually have elapsed in the broker.
    assert elapsed >= 0.6, f"finished in {elapsed:.2f}s -- retry delays were not applied"

    dead_letter = await _relay_dead_letter_notice(db, mq, task_id)
    assert dead_letter["task_type"] == "flaky"
    assert dead_letter["label"] == "dead_letter"
    assert dead_letter["reason"].startswith("retries exhausted (3/3)")

    retry_queue = await mq.channel.declare_queue("tasks.retry", passive=True)
    assert retry_queue.declaration_result.message_count == 0, "nothing left waiting to retry"
