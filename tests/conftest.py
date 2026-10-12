"""Shared pytest fixtures.

These are INTEGRATION fixtures: they talk to the real PostgreSQL and
RabbitMQ containers started by docker-compose.yml (see README for how to
bring them up). There is no mock/in-memory substitute for the database or
broker in this suite -- the mission this project serves explicitly calls
for proving the real integration, not a stand-in for it.

Test isolation: every row this suite creates uses a task_type prefixed
with "test_" and/or a uuid4-based idempotency_key, and the `cleanup_db`
fixture deletes exactly those rows afterwards. Nothing pre-existing in the
database is ever touched.
"""
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, select

from app.core.config import Settings
from app.db.base import (
    assert_schema_is_current,
    dispose_engine,
    get_session_factory_singleton,
    init_db,
)
from app.db.models import OutboxEventDB, TaskDB
from app.mq.connection import RabbitMQConnection


@pytest_asyncio.fixture
async def db():
    """Point the shared engine/session-factory singletons at the real DB.

    The suite runs against the schema built by `alembic upgrade head` (the
    same one the app uses) -- it never creates tables itself."""
    settings = Settings()
    init_db(settings)
    await assert_schema_is_current()
    yield get_session_factory_singleton()
    await dispose_engine()


@pytest_asyncio.fixture
async def mq(db):  # noqa: ARG001 - `db` is an ordering dependency (init before connect), not used by name
    """A real RabbitMQ connection with topology declared."""
    connection = RabbitMQConnection()
    await connection.connect()
    yield connection
    await connection.close()


@pytest_asyncio.fixture
async def api_client(db):  # noqa: ARG001 - `db` is an ordering dependency, not used by name
    """An httpx client driving the real FastAPI app in-process (ASGI, no
    socket) against the real DB. The app's own lifespan (which also opens a
    RabbitMQ connection for the outbox relay) is intentionally NOT started
    here -- tests that need the relay/worker wire those up explicitly via
    the `mq` fixture for precise control over timing."""
    from app.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.fixture
def created_task_ids() -> list[str]:
    """Tests that must use a specific task_type (to hit a registered worker
    handler, e.g. "flaky") can't rely on the "test_*" prefix for cleanup.
    They append the task_id here instead; `cleanup_db` deletes exactly
    those rows by id, in addition to anything "test_*"-prefixed."""
    return []


@pytest_asyncio.fixture(autouse=True)
async def cleanup_db(db, created_task_ids):
    """Delete only the rows this test run creates: task_type LIKE 'test_%',
    plus anything explicitly listed in `created_task_ids`."""
    yield
    async with db() as session:
        test_task_ids = select(TaskDB.id).where(TaskDB.task_type.like("test_%"))
        all_ids_to_delete = TaskDB.id.in_(test_task_ids) | TaskDB.id.in_(created_task_ids)
        outbox_target_ids = select(TaskDB.id).where(all_ids_to_delete)
        await session.execute(
            delete(OutboxEventDB).where(OutboxEventDB.task_id.in_(outbox_target_ids))
        )
        await session.execute(delete(TaskDB).where(all_ids_to_delete))
        await session.commit()


def unique_idempotency_key() -> str:
    return f"test-idem-{uuid.uuid4()}"
