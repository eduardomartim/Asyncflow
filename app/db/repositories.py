"""Repositories for AsyncFlow database access."""

import logging

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import OutboxEventDB, TaskDB

logger = logging.getLogger("asyncflow.db")


class TaskRepository:
    """Repository for task database operations."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, task: TaskDB) -> "TaskRepository":
        """Create a new task."""
        self.session.add(task)
        await self.session.flush()
        return self

    async def get(self, task_id: str) -> TaskDB | None:
        """Get a task by ID."""
        result = await self.session.execute(
            select(TaskDB).where(TaskDB.id == task_id)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> TaskDB | None:
        """Get a task by idempotency key."""
        result = await self.session.execute(
            select(TaskDB).where(TaskDB.idempotency_key == key)
        )
        return result.scalar_one_or_none()

    async def update_status(
        self,
        task_id: str,
        status: str,
        result: str | None = None,
        error: str | None = None,
        error_category: str | None = None,
        next_attempt_at = None,
    ) -> TaskDB | None:
        """Update task status."""
        await self.session.execute(
            update(TaskDB)
            .where(TaskDB.id == task_id)
            .values(
                status=status,
                result=result,
                error=error,
                error_category=error_category,
                updated_at=func.now(),
                next_attempt_at=next_attempt_at,
                completed_at=(
                    func.now() if status in ("succeeded", "failed", "dead_lettered") else None
                ),
            )
        )
        await self.session.flush()
        return await self.get(task_id)

    async def increment_attempts(self, task_id: str) -> TaskDB | None:
        """Increment task attempt counter."""
        await self.session.execute(
            update(TaskDB)
            .where(TaskDB.id == task_id)
            .values(attempts=TaskDB.attempts + 1, updated_at=func.now())
        )
        await self.session.flush()
        return await self.get(task_id)


class OutboxRepository:
    """Repository for outbox event database operations."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        task_id: str,
        event_type: str,
        payload: str | None = None,
    ) -> "OutboxRepository":
        """Create a new outbox event."""
        event = OutboxEventDB(
            task_id=task_id,
            event_type=event_type,
            payload=payload,
            status="pending",
        )
        self.session.add(event)
        await self.session.flush()
        return self

    async def get_pending(self, limit: int = 100) -> list["OutboxEventDB"]:
        """Get pending outbox events, oldest first."""
        result = await self.session.execute(
            select(OutboxEventDB)
            .where(OutboxEventDB.status == "pending")
            .order_by(OutboxEventDB.id)
            .limit(limit)
        )
        return result.scalars().all()

    async def claim_pending(self, event_id: int) -> OutboxEventDB | None:
        """Lock one still-pending event for this transaction, or return None.

        ``FOR UPDATE SKIP LOCKED``: if another relay (the API and the worker
        each run one) is already handling this row, skip it instead of
        publishing it a second time. The lock is held until commit/rollback.
        """
        result = await self.session.execute(
            select(OutboxEventDB)
            .where(OutboxEventDB.id == event_id, OutboxEventDB.status == "pending")
            .with_for_update(skip_locked=True)
        )
        return result.scalar_one_or_none()

    async def mark_published(self, event_id: int) -> "OutboxRepository":
        """Mark an outbox event as published."""
        await self.session.execute(
            update(OutboxEventDB)
            .where(OutboxEventDB.id == event_id)
            .values(
                published=True,
                published_at=func.now(),
                status="published",
            )
        )
        await self.session.flush()
        return self

    async def mark_recorded(self, event_id: int) -> "OutboxRepository":
        """Mark an event that has no broker destination as handled.

        Used for audit-only event types: the row leaves the pending queue,
        but ``published`` stays False because nothing was sent to RabbitMQ.
        """
        await self.session.execute(
            update(OutboxEventDB)
            .where(OutboxEventDB.id == event_id)
            .values(status="recorded")
        )
        await self.session.flush()
        return self

    async def increment_attempts(self, event_id: int) -> "OutboxRepository":
        """Increment the publish-attempt counter for an outbox event."""
        await self.session.execute(
            update(OutboxEventDB)
            .where(OutboxEventDB.id == event_id)
            .values(attempts=OutboxEventDB.attempts + 1)
        )
        await self.session.flush()
        return self

    async def mark_failed(self, event_id: int, reason: str) -> "OutboxRepository":
        """Mark an outbox event as permanently failed (publish attempts exhausted)."""
        await self.session.execute(
            update(OutboxEventDB)
            .where(OutboxEventDB.id == event_id)
            .values(status="failed")
        )
        await self.session.flush()
        logger.warning("outbox_event_failed", extra={"outbox_event_id": event_id, "reason": reason})
        return self