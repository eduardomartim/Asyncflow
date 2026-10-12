"""AsyncFlow worker for processing tasks from RabbitMQ.

Each in-flight message gets its own DB session (``prefetch_count`` lets
several messages be handled concurrently, and AsyncSession is not safe to
share across concurrent coroutines). The worker is idempotent: before doing
any work it re-reads the task's current status from Postgres and no-ops if
it's already terminal, so an at-least-once redelivery (outbox relay replay,
broker redelivery after a crash) never double-processes a finished task.

Dead-lettering is written to Postgres only (task status + a
``task_dead_lettered`` outbox event, one transaction); the outbox relay then
publishes the notice to ``tasks.dead_letter``. The standalone worker process
runs its own relay (see ``run_worker``) so this doesn't depend on the API
being up.
"""
import asyncio
import json
import logging
from typing import Any, Protocol

from aio_pika.abc import AbstractChannel, AbstractIncomingMessage
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import Settings
from app.core.config import settings as default_settings
from app.core.constants import ErrorCategory, QueueName, TaskStatus
from app.core.exceptions import TaskPermanentError, TaskRetryableError
from app.core.serialization import payload_to_json
from app.db.repositories import OutboxRepository, TaskRepository
from app.mq.connection import TASKS_QUEUE_ARGUMENTS, RabbitMQConnection
from app.mq.publisher import MessagePublisher
from app.outbox.relay import OutboxRelay, notify_relay

logger = logging.getLogger("asyncflow.worker")

_TERMINAL_STATUS_VALUES = {
    TaskStatus.succeeded.value,
    TaskStatus.failed.value,
    TaskStatus.dead_lettered.value,
    TaskStatus.cancelled.value,
}


class TaskHandler(Protocol):
    async def __call__(self, payload: dict[str, Any], attempt: int) -> Any: ...


class BaseWorker:
    """Consumes the "tasks" queue and drives each task to a terminal state."""

    def __init__(
        self,
        mq: RabbitMQConnection,
        session_factory: async_sessionmaker,
        app_settings: Settings = default_settings,
    ) -> None:
        self._mq = mq
        self._session_factory = session_factory
        self._settings = app_settings
        self._channel: AbstractChannel | None = None
        self._stop_event = asyncio.Event()
        self._in_flight: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Begin consuming. Blocks until stop() is called."""
        self._channel = await self._mq.connection.channel()
        await self._channel.set_qos(prefetch_count=self._settings.worker_concurrency)

        queue = await self._channel.declare_queue(
            QueueName.tasks.value, durable=True, arguments=TASKS_QUEUE_ARGUMENTS
        )
        logger.info("worker_started", extra={"queue": QueueName.tasks.value})

        consumer_tag = await queue.consume(self._on_message, no_ack=False)

        await self._stop_event.wait()

        await queue.cancel(consumer_tag)
        if self._in_flight:
            logger.info(
                "worker_draining_in_flight_messages", extra={"count": len(self._in_flight)}
            )
            await asyncio.wait(
                self._in_flight, timeout=self._settings.gracefully_shutdown_timeout_seconds
            )
        logger.info("worker_stopped")

    async def stop(self) -> None:
        """Signal the worker to stop gracefully (finish in-flight, stop consuming)."""
        self._stop_event.set()

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._in_flight.add(task)
        try:
            async with message.process(requeue=False, ignore_processed=True):
                await self._handle_message(message)
        finally:
            if task is not None:
                self._in_flight.discard(task)

    async def _handle_message(self, message: AbstractIncomingMessage) -> None:
        try:
            data = json.loads(message.body.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.error("worker_received_malformed_message")
            return  # unparseable; acking it away is correct, there is no task_id to act on

        task_id = data.get("task_id")
        if not task_id:
            logger.error("worker_message_missing_task_id")
            return

        async with self._session_factory() as session:
            task_repo = TaskRepository(session)
            outbox_repo = OutboxRepository(session)

            task = await task_repo.get(task_id)
            if not task:
                logger.error("worker_task_not_found", extra={"task_id": task_id})
                return

            if task.status in _TERMINAL_STATUS_VALUES:
                logger.info(
                    "worker_task_already_terminal_skipping",
                    extra={"task_id": task_id, "status": task.status},
                )
                return

            await task_repo.update_status(task_id=task_id, status=TaskStatus.processing.value)
            await session.commit()

            payload = data.get("payload", {})
            try:
                result = await self.execute(task.task_type, payload, attempt=task.attempts + 1)
                await task_repo.update_status(
                    task_id=task_id,
                    status=TaskStatus.succeeded.value,
                    result=payload_to_json(result),
                )
                await outbox_repo.create(
                    task_id=task_id,
                    event_type="task_succeeded",
                    payload=payload_to_json({"result": result}),
                )
                await session.commit()
                logger.info("worker_task_succeeded", extra={"task_id": task_id})
                return
            except TaskPermanentError as exc:
                await session.rollback()
                await self._dead_letter(task_repo, outbox_repo, session, task, str(exc))
                return
            except TaskRetryableError as exc:
                await session.rollback()
                updated = await task_repo.increment_attempts(task_id)
                await session.commit()
                attempts = updated.attempts if updated else task.attempts + 1
                if attempts < task.max_attempts:
                    await self._retry(task_repo, session, task, payload, attempts, str(exc))
                else:
                    await self._dead_letter(
                        task_repo, outbox_repo, session, task,
                        f"retries exhausted ({attempts}/{task.max_attempts}): {exc}",
                    )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - unknown handler bug, default to transient
                await session.rollback()
                updated = await task_repo.increment_attempts(task_id)
                await session.commit()
                attempts = updated.attempts if updated else task.attempts + 1
                logger.exception("worker_handler_raised_unexpectedly", extra={"task_id": task_id})
                if attempts < task.max_attempts:
                    await self._retry(task_repo, session, task, payload, attempts, str(exc))
                else:
                    await self._dead_letter(
                        task_repo, outbox_repo, session, task,
                        f"retries exhausted ({attempts}/{task.max_attempts}): {exc}",
                    )
                return

    async def _retry(self, task_repo, session, task, payload, attempts, error_message) -> None:
        delay = min(
            self._settings.retry_backoff_base_seconds * (2 ** (attempts - 1)),
            self._settings.retry_backoff_max_seconds,
        )
        await task_repo.update_status(
            task_id=task.id,
            status=TaskStatus.retrying.value,
            error=error_message,
            error_category=ErrorCategory.transient.value,
        )
        await session.commit()
        publisher = MessagePublisher(self._channel)
        await publisher.publish_retry(
            task_id=task.id,
            task_type=task.task_type,
            payload=payload,
            attempt=attempts,
            delay_seconds=delay,
            error=error_message,
        )
        logger.info(
            "worker_task_scheduled_for_retry",
            extra={"task_id": task.id, "attempt": attempts, "delay_seconds": delay},
        )

    async def _dead_letter(
        self, task_repo, outbox_repo, session, task, reason: str
    ) -> None:
        # Status and the dead-letter event commit together; the outbox relay
        # publishes the notice to tasks.dead_letter afterwards. Publishing it
        # here, after the commit, would lose it if the process died in
        # between: the redelivered message would find a terminal task and
        # be skipped.
        await task_repo.update_status(
            task_id=task.id,
            status=TaskStatus.dead_lettered.value,
            error=reason,
            error_category=ErrorCategory.permanent.value,
        )
        await outbox_repo.create(
            task_id=task.id,
            event_type="task_dead_lettered",
            payload=payload_to_json({"task_type": task.task_type, "reason": reason}),
        )
        await session.commit()
        notify_relay()
        logger.warning("worker_task_dead_lettered", extra={"task_id": task.id, "reason": reason})

    async def execute(self, task_type: str, payload: dict[str, Any], attempt: int) -> Any:
        """Override in a subclass. Return the task's result, or raise
        TaskRetryableError / TaskPermanentError to signal failure."""
        raise NotImplementedError


class SimpleWorker(BaseWorker):
    """Worker that dispatches to a registry of per-task-type handlers.

    Unregistered task types succeed trivially (placeholder "processed"
    result) -- this is what makes the mission's integration_test task type
    work out of the box without a bespoke handler.
    """

    def __init__(
        self,
        mq: RabbitMQConnection,
        session_factory: async_sessionmaker,
        handlers: dict[str, TaskHandler] | None = None,
        app_settings: Settings = default_settings,
    ) -> None:
        super().__init__(mq, session_factory, app_settings)
        self.handlers = handlers or {}

    async def execute(self, task_type: str, payload: dict[str, Any], attempt: int) -> Any:
        handler = self.handlers.get(task_type)
        if handler is None:
            return {"processed": True, "task_type": task_type, "detail": "no handler registered"}
        return await handler(payload, attempt)


# --- Demo/test handlers -----------------------------------------------------
# Deterministic by `attempt` (not wall-clock or randomness), so retry/DLQ
# behavior can be exercised reliably in integration tests and manual demos.

async def _flaky_handler(payload: dict[str, Any], attempt: int) -> Any:
    """Fails with a transient error until `fail_until_attempt`, then succeeds."""
    fail_until = int(payload.get("fail_until_attempt", 1))
    if attempt < fail_until:
        raise TaskRetryableError(f"simulated transient failure on attempt {attempt}")
    return {"processed": True, "succeeded_on_attempt": attempt}


async def _always_fail_handler(_payload: dict[str, Any], _attempt: int) -> Any:
    """Always fails with a permanent error -> dead-lettered on the first attempt."""
    raise TaskPermanentError("simulated permanent failure")


def default_handlers() -> dict[str, TaskHandler]:
    return {
        "flaky": _flaky_handler,
        "always_fail": _always_fail_handler,
    }


async def serve_until_stopped(worker: BaseWorker) -> None:
    """Run ``worker.start()`` until ``stop()`` is called or this is cancelled.

    Cancellation is how ``asyncio.run`` delivers Ctrl+C when it can't install
    a loop signal handler (always the case on Windows): it cancels the main
    coroutine instead of raising KeyboardInterrupt inside it. Cancelling
    ``worker.start()`` directly would abandon in-flight tasks, so the worker
    runs in its own task (shielded) and a cancellation is turned into a
    graceful ``stop()`` that drains in-flight messages before re-raising.
    """
    start_task = asyncio.create_task(worker.start())
    try:
        await asyncio.shield(start_task)
    except asyncio.CancelledError:
        logger.info("worker_shutdown_requested")
        await worker.stop()
        await start_task
        raise


async def run_worker() -> None:
    """Standalone worker process entry point."""
    import signal

    from app.core.config import settings as app_settings
    from app.db.base import (
        assert_schema_is_current,
        get_engine,
        get_session_factory_singleton,
        init_db,
    )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    init_db(app_settings)
    await assert_schema_is_current()
    mq = RabbitMQConnection()
    await mq.connect()

    worker = SimpleWorker(
        mq=mq,
        session_factory=get_session_factory_singleton(),
        handlers=default_handlers(),
        app_settings=app_settings,
    )

    # This process's own outbox relay: publishes the dead-letter notices the
    # worker writes (and any pending task_created events) even when the API
    # is not running. Safe alongside the API's relay (rows are claimed with
    # SKIP LOCKED).
    relay = OutboxRelay(session_factory=get_session_factory_singleton(), mq=mq)
    relay_task = asyncio.create_task(relay.run())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))
        except (NotImplementedError, AttributeError, ValueError):
            # Windows: no loop signal handlers. asyncio.run then turns Ctrl+C
            # into a cancellation of this coroutine, which
            # serve_until_stopped converts into a graceful stop.
            pass

    try:
        await serve_until_stopped(worker)
    finally:
        relay.stop()
        await asyncio.wait([relay_task], timeout=10)
        await mq.close()
        await get_engine().dispose()


def main() -> None:
    """Console-script entry point (see pyproject.toml ``asyncflow-worker``)."""
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
