"""Pydantic response/request schemas for the v1 Tasks API."""
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.constants import TaskStatus


class TaskCreateResponse(BaseModel):
    """Response returned right after a task is accepted (or deduplicated)."""

    id: str
    status: TaskStatus
    task_type: str | None = None
    idempotency_key: str | None = None
    message: str


class TaskDetailResponse(BaseModel):
    """Full task record, as returned by GET /tasks/{task_id} and list items."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    task_type: str
    status: TaskStatus
    result: Any | None = None
    error: str | None = None
    attempts: int
    max_attempts: int
    created_at: datetime
    updated_at: datetime | None = None


class TaskListResponse(BaseModel):
    """Paginated list of tasks."""

    items: list[TaskDetailResponse]
    page: int
    size: int
    total: int = Field(description="Total number of tasks matching the filter")
