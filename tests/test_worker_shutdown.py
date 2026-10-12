"""Graceful shutdown: stop() and Ctrl+C let an in-flight task finish first.

Runs an in-process worker against the real PostgreSQL and RabbitMQ with a
slow handler registered only for this test's task type. No external
process is started or signalled.
"""
import asyncio
import time
import uuid

from app.core.config import Settings
from app.core.serialization import payload_to_json
from app.db.models import TaskDB
from app.db.repositories import TaskRepository
from app.mq.publisher import MessagePublisher
from app.workers.worker import SimpleWorker, serve_until_stopped

HANDLER_SECONDS = 1.0


async def test_stop_drains_in_flight_task_before_returning(db, mq):
    task_id = str(uuid.uuid4())
    async with db() as session:
        await TaskRepository(session).create(
            TaskDB(
                id=task_id,
                task_type="test_slow_shutdown",
                payload=payload_to_json({}),
                status="queued",
                attempts=0,
                max_attempts=3,
            )
        )
        await session.commit()

    handler_started = asyncio.Event()
    handler_finished = asyncio.Event()

    async def slow_handler(_payload, _attempt):
        handler_started.set()
        await asyncio.sleep(HANDLER_SECONDS)
        handler_finished.set()
        return {"slow": True}

    worker = SimpleWorker(
        mq=mq,
        session_factory=db,
        handlers={"test_slow_shutdown": slow_handler},
        app_settings=Settings(gracefully_shutdown_timeout_seconds=10),
    )
    run_task = asyncio.create_task(worker.start())
    await MessagePublisher(mq.channel).publish_task(
        task_id=task_id, task_type="test_slow_shutdown", payload={}
    )
    await asyncio.wait_for(handler_started.wait(), timeout=5)

    stop_requested = time.monotonic()
    await worker.stop()
    await asyncio.wait_for(run_task, timeout=15)
    waited = time.monotonic() - stop_requested

    # start() only returned after the handler completed and the result was saved.
    assert handler_finished.is_set()
    assert waited >= HANDLER_SECONDS * 0.5
    async with db() as session:
        task = await TaskRepository(session).get(task_id)
    assert task.status == "succeeded"

    # The message was acked, not left for redelivery.
    queue = await mq.channel.declare_queue("tasks", passive=True)
    assert queue.declaration_result.message_count == 0


async def test_ctrl_c_cancellation_drains_in_flight_task(db, mq):
    """On Windows, asyncio.run delivers Ctrl+C by cancelling the main
    coroutine (asyncio.Runner._on_sigint -> main_task.cancel()). This does
    the same to serve_until_stopped and checks the in-flight task still
    completes before the cancellation propagates."""
    task_id = str(uuid.uuid4())
    async with db() as session:
        await TaskRepository(session).create(
            TaskDB(
                id=task_id,
                task_type="test_slow_ctrl_c",
                payload=payload_to_json({}),
                status="queued",
                attempts=0,
                max_attempts=3,
            )
        )
        await session.commit()

    handler_started = asyncio.Event()
    handler_finished = asyncio.Event()

    async def slow_handler(_payload, _attempt):
        handler_started.set()
        await asyncio.sleep(HANDLER_SECONDS)
        handler_finished.set()
        return {"slow": True}

    worker = SimpleWorker(
        mq=mq,
        session_factory=db,
        handlers={"test_slow_ctrl_c": slow_handler},
        app_settings=Settings(gracefully_shutdown_timeout_seconds=10),
    )
    main_task = asyncio.create_task(serve_until_stopped(worker))
    await MessagePublisher(mq.channel).publish_task(
        task_id=task_id, task_type="test_slow_ctrl_c", payload={}
    )
    await asyncio.wait_for(handler_started.wait(), timeout=5)

    main_task.cancel()  # what asyncio.run does on Ctrl+C
    try:
        await asyncio.wait_for(main_task, timeout=15)
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("cancellation must still propagate after draining")

    assert handler_finished.is_set()
    async with db() as session:
        task = await TaskRepository(session).get(task_id)
    assert task.status == "succeeded"
    queue = await mq.channel.declare_queue("tasks", passive=True)
    assert queue.declaration_result.message_count == 0
