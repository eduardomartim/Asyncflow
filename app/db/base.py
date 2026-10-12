"""Database base and session configuration.

This module owns the lifecycle of the single async engine / session factory
used by the running process (API, worker, outbox relay). ``init_db()`` must
be called once at process startup (see the ``lifespan`` in ``app.api.main``
and the worker/relay entry points) before any request tries to resolve
``get_session`` as a FastAPI dependency.
"""
import logging
from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings

logger = logging.getLogger("asyncflow.db")

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def create_engine(settings: Settings) -> AsyncEngine:
    """Create an async SQLAlchemy engine for the given settings."""
    return create_async_engine(
        settings.postgres_dsn,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )


def get_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create a session factory bound to the engine."""
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def init_db(settings: Settings) -> None:
    """Initialize the process-wide engine/session factory singletons.

    Idempotent: calling it again replaces the singletons (used by tests to
    point the app at a different database).
    """
    global _engine, _session_factory
    _engine = create_engine(settings)
    _session_factory = get_session_factory(_engine)
    logger.info("database_engine_initialized", extra={"host": settings.postgres_host})


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Database not initialized. Call init_db(settings) first.")
    return _engine


def get_session_factory_singleton() -> async_sessionmaker[AsyncSession]:
    if _session_factory is None:
        raise RuntimeError("Database not initialized. Call init_db(settings) first.")
    return _session_factory


class SchemaNotMigratedError(RuntimeError):
    """The database is not at the Alembic head revision this code expects."""


def expected_schema_revisions() -> set[str]:
    """Alembic head revision(s) of the migrations shipped with this checkout."""
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    config = Config(str(_ALEMBIC_INI))
    return set(ScriptDirectory.from_config(config).get_heads())


async def assert_schema_is_current() -> None:
    """Fail fast unless the database has been migrated to the Alembic head.

    The schema is owned exclusively by the Alembic migrations under
    ``migrations/`` (``alembic upgrade head``); processes never create or
    alter tables themselves. This check only READS ``alembic_version``, so a
    missing or outdated schema surfaces as one clear startup error instead
    of an obscure SQL error on the first request.
    """
    from sqlalchemy import text
    from sqlalchemy.exc import ProgrammingError

    expected = expected_schema_revisions()
    try:
        async with get_engine().connect() as conn:
            rows = await conn.execute(text("SELECT version_num FROM alembic_version"))
            current = {row[0] for row in rows}
    except ProgrammingError:  # alembic_version table does not exist
        current = set()
    if current != expected:
        raise SchemaNotMigratedError(
            f"database schema revision {sorted(current) or 'none'} != expected "
            f"{sorted(expected)}; run `alembic upgrade head` first"
        )


async def dispose_engine() -> None:
    """Dispose of the engine's connection pool (call on process shutdown)."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency: yield a session, commit on success, rollback on error.

    Committing here (rather than inside repositories) is what makes the
    task-create + outbox-event-create sequence in the endpoint transactional:
    both inserts share this one session/transaction and are only durably
    committed together.
    """
    session_factory = get_session_factory_singleton()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
