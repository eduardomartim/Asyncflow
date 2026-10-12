"""Database models for AsyncFlow."""
from sqlalchemy import Boolean, Column, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class TaskDB(Base):
    """Database model for async tasks."""
    __tablename__ = "tasks"

    # NOTE: status/idempotency_key are indexed explicitly in __table_args__
    # below (idempotency_key's uniquely so) -- do NOT also pass index=True
    # on those columns, SQLAlchemy would give the auto-generated index the
    # exact same name as the explicit one and the two definitions collide
    # (this previously made the DB's index silently non-unique instead of
    # unique, defeating the idempotency guarantee).
    id = Column(String, primary_key=True, index=True)
    task_type = Column(String, nullable=False, index=True)
    payload = Column(Text, nullable=False)
    status = Column(String, nullable=False, default="queued")
    result = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    attempts = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=5, nullable=False)
    next_attempt_at = Column(DateTime, nullable=True)
    error_category = Column(String, nullable=True, index=True)
    idempotency_key = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    completed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_tasks_status", "status"),
        Index("ix_tasks_idempotency_key", "idempotency_key", unique=True),
        Index("ix_tasks_next_attempt", "next_attempt_at"),
    )


class OutboxEventDB(Base):
    """Database model for transactional outbox events."""
    __tablename__ = "outbox_events"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False)
    payload = Column(Text, nullable=True)
    status = Column(String, nullable=False, default="pending", index=True)
    attempts = Column(Integer, default=0, nullable=False)
    max_attempts = Column(Integer, default=3, nullable=False)
    published = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    published_at = Column(DateTime, nullable=True)