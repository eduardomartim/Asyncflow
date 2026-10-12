"""Transactional outbox relay.

The API writes a task row and an `outbox_events` row in the SAME database
transaction (see app/api/v1/endpoints.py::create_task + app/db/base.py
get_session's commit-on-exit). That makes "task persisted" and "an event
exists saying it needs publishing" atomic. This module is the second half:
it polls `outbox_events` for rows still `pending` and publishes them to
RabbitMQ, marking each `published` only after the broker has accepted it.

Delivery guarantee this provides: AT-LEAST-ONCE, not exactly-once. If the
process crashes after RabbitMQ acks the publish but before the DB commit
that flips the row to `published`, the same event is republished on the
next poll. The worker that ultimately consumes these task messages is
written to tolerate that (see app/workers/worker.py): it re-reads the task's
current status from Postgres before acting, so a duplicate "task_created"
delivery for an already-processing/succeeded task is a no-op.

Both the API process and the worker process run a relay. Each event is
claimed with SELECT ... FOR UPDATE SKIP LOCKED in its own transaction, so
two relays never publish the same row concurrently.

Event types and what "handled" means for each:

- ``task_created``       -> published to the ``tasks`` exchange; row becomes
                            status="published", published=True.
- ``task_dead_lettered`` -> published to the ``dead_letter`` exchange (the
                            ``tasks.dead_letter`` queue); status="published",
                            published=True. Going through the outbox (rather
                            than the worker publishing directly after its DB
                            commit) means a crash between the two can't lose
                            the dead-letter notice.
- ``task_succeeded``     -> audit record only, no broker consumer exists:
                            status="recorded", published stays False.

"published" is only ever set after ``exchange.publish`` returned, and the
channels are opened with publisher confirms (aio-pika's default), so it
means the broker acknowledged the message. A publish that raises leaves
the row pending with ``attempts`` incremented; after ``max_attempts``
failures it becomes status="failed" and is no longer retried.
"""
import asyncio
import logging

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.serialization import payload_from_json
from app.db.repositories import OutboxRepository, TaskRepository
from app.mq.connection import RabbitMQConnection
from app.mq.publisher import MessagePublisher

logger = logging.getLogger("asyncflow.outbox")

_wake_event = asyncio.Event()

# Event types the relay publishes to RabbitMQ. Anything else (currently only
# "task_succeeded") is an audit record with no broker consumer in this
# project's scope: it is marked status="recorded", never "published".
_FORWARDABLE_EVENT_TYPES = {"task_created", "task_dead_lettered"}


def notify_relay() -> None:
    """Wake the relay loop immediately instead of waiting for its poll interval.

    Best-effort latency optimization only; the poll loop below is what
    actually guarantees delivery even if this is never called.
    """
    _wake_event.set()


class OutboxRelay:
    """Background task: poll outbox_events, publish pending ones, mark them."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        mq: RabbitMQConnection,
        poll_interval_seconds: float = 2.0,
        batch_size: int = 100,
    ) -> None:
        self._session_factory = session_factory
        self._mq = mq
        self._poll_interval = poll_interval_seconds
        self._batch_size = batch_size
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("outbox_relay_started")
        while not self._stop_event.is_set():
            try:
                published = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox_relay_iteration_failed")
                published = 0

            if published == 0 and not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(_wake_event.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
                _wake_event.clear()
        logger.info("outbox_relay_stopped")

    def stop(self) -> None:
        self._stop_event.set()
        _wake_event.set()

    async def run_once(self) -> int:
        """Publish one batch of pending events. Returns how many were handled.

        Public on purpose: the integration test suite drives the relay
        deterministically (one explicit cycle) rather than racing its
        background poll loop.
        """
        if not self._mq.is_connected:
            logger.warning("outbox_relay_mq_disconnected_skipping_cycle")
            return 0

        async with self._session_factory() as session:
            pending = await OutboxRepository(session).get_pending(limit=self._batch_size)
            event_ids = [event.id for event in pending]

        handled = 0
        for event_id in event_ids:
            if await self._relay_one(event_id):
                handled += 1
        return handled

    async def _relay_one(self, event_id: int) -> bool:
        """Claim, publish and mark one event in its own transaction.

        Returns True if the event was handled, False if another relay holds
        it, it is no longer pending, or publishing failed.
        """
        publisher = MessagePublisher(self._mq.channel)
        async with self._session_factory() as session:
            repo = OutboxRepository(session)
            event = await repo.claim_pending(event_id)
            if event is None:
                return False
            # Read before anything can roll back: a rollback expires ORM
            # instances, and touching an expired attribute in an async
            # session raises MissingGreenlet instead of reloading it.
            attempts, max_attempts = event.attempts, event.max_attempts
            try:
                await self._relay_event(session, repo, publisher, event)
                await session.commit()
                return True
            except Exception:
                logger.exception("outbox_event_relay_failed", extra={"outbox_event_id": event_id})
                await session.rollback()

            # Record the failed attempt in a fresh transaction so one bad
            # event can't block the rest of the batch forever.
            await repo.increment_attempts(event_id)
            if attempts + 1 >= max_attempts:
                await repo.mark_failed(event_id, reason="publish attempts exhausted")
            await session.commit()
            return False

    async def _relay_event(self, session, repo: OutboxRepository, publisher, event) -> None:
        data = payload_from_json(event.payload) or {}

        if event.event_type == "task_created":
            await publisher.publish_task(
                task_id=event.task_id,
                task_type=data.get("task_type", "unknown"),
                payload=data.get("payload", {}),
            )
            await repo.mark_published(event.id)
        elif event.event_type == "task_dead_lettered":
            task_type = data.get("task_type")
            if task_type is None:  # events written before task_type was added to the payload
                task = await TaskRepository(session).get(event.task_id)
                task_type = task.task_type if task else "unknown"
            await publisher.publish_dead_letter(
                task_id=event.task_id,
                task_type=task_type,
                reason=data.get("reason", ""),
            )
            await repo.mark_published(event.id)
        else:
            assert event.event_type not in _FORWARDABLE_EVENT_TYPES  # by construction
            logger.debug(
                "outbox_event_audit_only_recorded",
                extra={"outbox_event_id": event.id, "event_type": event.event_type},
            )
            await repo.mark_recorded(event.id)
