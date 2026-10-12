"""The Alembic migrations must build the full schema from an EMPTY database.

Runs `alembic upgrade head` + `alembic check` against a throwaway database
created (and dropped) by this test on the same Postgres server, so the
project's real `asyncflow` database is never touched. This guards the
startup strategy: processes never create tables, so the migrations alone
must be enough.
"""
import os
import subprocess
import sys
import uuid
from pathlib import Path

import asyncpg
import pytest_asyncio

from app.core.config import Settings
from app.db.base import (
    SchemaNotMigratedError,
    assert_schema_is_current,
    dispose_engine,
    init_db,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest_asyncio.fixture
async def empty_database():
    settings = Settings()
    name = f"asyncflow_migtest_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        database="postgres",
    )
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        yield name
    finally:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.close()


def _alembic(database: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=PROJECT_ROOT,
        env={**os.environ, "POSTGRES_DB": database},
        capture_output=True,
        text=True,
        timeout=120,
    )


async def test_migrations_build_schema_from_empty_database(empty_database):
    init_db(Settings(postgres_db=empty_database))
    try:
        # Before migrating, the startup check must refuse the empty database.
        try:
            await assert_schema_is_current()
        except SchemaNotMigratedError:
            pass
        else:
            raise AssertionError("schema check accepted an unmigrated database")
    finally:
        await dispose_engine()

    upgrade = _alembic(empty_database, "upgrade", "head")
    assert upgrade.returncode == 0, upgrade.stderr

    # ORM models and migrated schema agree (no autogenerate drift).
    check = _alembic(empty_database, "check")
    assert check.returncode == 0, check.stdout + check.stderr
    assert "No new upgrade operations detected" in check.stdout + check.stderr

    init_db(Settings(postgres_db=empty_database))
    try:
        await assert_schema_is_current()
    finally:
        await dispose_engine()


async def test_downgrade_behaviour_matches_readme(empty_database):
    """README: one step back (to fc46ce9cb5a6) and forward again works;
    going below fc46ce9cb5a6 is unsupported and fails without leaving a
    half-migrated database (Postgres DDL is transactional)."""
    assert _alembic(empty_database, "upgrade", "head").returncode == 0

    down_one = _alembic(empty_database, "downgrade", "fc46ce9cb5a6")
    assert down_one.returncode == 0, down_one.stderr
    up_again = _alembic(empty_database, "upgrade", "head")
    assert up_again.returncode == 0, up_again.stderr

    below = _alembic(empty_database, "downgrade", "0b5e1a7c2d3f")
    assert below.returncode != 0
    assert "cannot be cast automatically to type uuid" in below.stderr

    current = _alembic(empty_database, "current")
    assert "6c10892b4578 (head)" in current.stdout, current.stdout + current.stderr
