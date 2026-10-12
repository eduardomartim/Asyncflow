"""Outbox relay delivery semantics, against the real PostgreSQL and RabbitMQ.

What these pin down:
- an event is only marked published after RabbitMQ accepted the publish;
- a failing publish is retried on later cycles and gives up after
  max_attempts, without blocking the other events in the batch;
- two relays running at once (the API's and the worker's) publish each
  pending event exactly once;
- audit-only events are "recorded", never "published".
"""
import asyncio
import json
import uuid

from sqlalchemy import select

from app.core.serialization import payload_to_json
from app.db.models import OutboxEventDB, TaskDB
from app.db.repositories import OutboxRepository, TaskRepository
from app.mq.publisher import MessagePublisher
from app.outbox.relay import OutboxRelay


async def _create_task_and_event(db, event_type: str = "task_created", payload=None) -> str:
    task_id = str(uuid.uuid4())
    async with db() as session:
        await TaskRepository(session).create(
            TaskDB(
                id=task_id,
                task_type="test_outbox_delivery",
                payload=payload_to_json({}),
                status="queued",
                attempts=0,
                max_attempts=3,
            )
        )
        if payload is None:
            payload = {"task_type": "test_outbox_delivery", "payload": {}}
        await OutboxRepository(session).create(
            task_id=task_id, event_type=event_type, payload=payload_to_json(payload)
        )
        await session.commit()
    return task_id


async def _event(db, task_id: str) -> OutboxEventDB:
    async with db() as session:
        result = await session.execute(
            select(OutboxEventDB).where(OutboxEventDB.task_id == task_id)
        )
        return result.scalar_one()


async def _drain_queue_for(mq, queue_name: str, task_ids: set[str]) -> list[str]:
    """Ack and return the task_ids of every message for `task_ids` on the
    queue; any other message is requeued untouched."""
    queue = await mq.channel.get_queue(queue_name)
    found, others = [], []
    try:
        while (message := await queue.get(timeout=5, fail=False)) is not None:
            task_id = json.loads(message.body.decode()).get("task_id")
            if task_id in task_ids:
                found.append(task_id)
                await message.ack()
            else:
                others.append(message)
    finally:
        for message in others:
            await message.nack(requeue=True)
    return found


async def test_failed_publish_is_retried_then_marked_failed_without_blocking_batch(
    db, mq, monkeypatch
):
    bad_id = await _create_task_and_event(db)
    good_id = await _create_task_and_event(db)

    original_publish_task = MessagePublisher.publish_task

    async def publish_task(self, task_id, *args, **kwargs):
        if task_id == bad_id:
            raise ConnectionError("simulated broker failure")
        return await original_publish_task(self, task_id, *args, **kwargs)

    monkeypatch.setattr(MessagePublisher, "publish_task", publish_task)
    relay = OutboxRelay(session_factory=db, mq=mq)

    # Cycle 1: the bad event fails (and must not raise out of run_once), the
    # good one behind it in the same batch is still published.
    await relay.run_once()
    bad, good = await _event(db, bad_id), await _event(db, good_id)
    assert (bad.status, bad.published, bad.attempts) == ("pending", False, 1)
    assert (good.status, good.published) == ("published", True)

    # Cycles 2 and 3: still failing -> after max_attempts (3) it gives up.
    await relay.run_once()
    assert (await _event(db, bad_id)).attempts == 2
    await relay.run_once()
    bad = await _event(db, bad_id)
    assert (bad.status, bad.published, bad.attempts) == ("failed", False, 3)

    # A failed event is no longer picked up.
    await relay.run_once()
    assert (await _event(db, bad_id)).attempts == 3

    assert await _drain_queue_for(mq, "tasks", {bad_id, good_id}) == [good_id]


async def test_concurrent_relays_publish_each_event_exactly_once(db, mq):
    task_ids = {await _create_task_and_event(db) for _ in range(20)}

    relay_a = OutboxRelay(session_factory=db, mq=mq)
    relay_b = OutboxRelay(session_factory=db, mq=mq)
    handled_a, handled_b = await asyncio.gather(relay_a.run_once(), relay_b.run_once())

    published = await _drain_queue_for(mq, "tasks", task_ids)
    assert sorted(published) == sorted(task_ids), "every event published exactly once"
    assert handled_a + handled_b >= len(task_ids)
    for task_id in task_ids:
        event = await _event(db, task_id)
        assert (event.status, event.published) == ("published", True)


async def test_audit_only_event_is_recorded_not_published(db, mq):
    task_id = await _create_task_and_event(db, "task_succeeded", {"result": {"ok": True}})

    await OutboxRelay(session_factory=db, mq=mq).run_once()

    event = await _event(db, task_id)
    assert (event.status, event.published, event.published_at) == ("recorded", False, None)
    assert await _drain_queue_for(mq, "tasks", {task_id}) == []
    assert await _drain_queue_for(mq, "tasks.dead_letter", {task_id}) == []


async def test_dead_letter_event_without_task_type_uses_task_row(db, mq):
    """task_dead_lettered events written before task_type was put in the
    payload (only {"reason": ...}) still publish with the right task_type."""
    task_id = await _create_task_and_event(db, "task_dead_lettered", {"reason": "legacy row"})

    await OutboxRelay(session_factory=db, mq=mq).run_once()

    event = await _event(db, task_id)
    assert (event.status, event.published) == ("published", True)
    queue = await mq.channel.get_queue("tasks.dead_letter")
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
    assert body == {
        "task_id": task_id,
        "task_type": "test_outbox_delivery",
        "reason": "legacy row",
        "label": "dead_letter",
    }
