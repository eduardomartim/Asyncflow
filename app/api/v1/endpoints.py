"""AsyncFlow v1 API endpoints for task management."""
import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.schemas import TaskCreateResponse, TaskDetailResponse, TaskListResponse
from app.core.config import settings
from app.core.constants import TaskStatus
from app.core.serialization import payload_from_json, payload_to_json
from app.db.base import get_session
from app.db.models import TaskDB
from app.db.repositories import OutboxRepository, TaskRepository
from app.outbox.relay import notify_relay as notify_outbox_relay

logger = logging.getLogger("asyncflow.api")

router = APIRouter(tags=["v1 - Tasks"])

# task_type is attacker-controlled free text used as a routing/label value
# downstream (logs, RabbitMQ routing); keep it to a conservative charset.
_TASK_TYPE_PATTERN = r"^[a-zA-Z0-9_\-\.]{1,100}$"


async def get_task_repo(
    session: AsyncSession = Depends(get_session),
) -> TaskRepository:
    """Dependency to get task repository."""
    return TaskRepository(session)


async def get_outbox_repo(
    session: AsyncSession = Depends(get_session),
) -> OutboxRepository:
    """Dependency to get outbox repository."""
    return OutboxRepository(session)


def _to_task_detail(task: TaskDB) -> TaskDetailResponse:
    return TaskDetailResponse(
        id=task.id,
        task_type=task.task_type,
        status=TaskStatus(task.status),
        result=payload_from_json(task.result),
        error=task.error,
        attempts=task.attempts,
        max_attempts=task.max_attempts,
        created_at=task.created_at,
        updated_at=task.updated_at,
    )


@router.post(
    "/tasks",
    response_model=TaskCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a new async task",
    description="""
    Submit a new asynchronous task for processing.

    - **task_type**: Type/category of the task (e.g., "email", "image_processing", "report")
    - **payload**: The data to be processed (JSON-compatible object)
    - **idempotency_key**: Optional key for idempotent submission. If provided,
      submitting the same key again returns the existing task instead of
      creating a duplicate (HTTP 200 instead of 201).

    On success, the task row and its transactional-outbox event are written
    in the SAME database transaction. A background relay then publishes the
    outbox event to RabbitMQ, so the message reaches the queue at-least-once
    even if the process crashes right after the HTTP response is sent.
    """,
)
async def create_task(
    payload: dict[str, Any],
    response: Response,
    task_type: str = Query(..., pattern=_TASK_TYPE_PATTERN),
    idempotency_key: str | None = Query(default=None, max_length=200),
    task_repo: TaskRepository = Depends(get_task_repo),
    outbox_repo: OutboxRepository = Depends(get_outbox_repo),
) -> TaskCreateResponse:
    """Create a new async task."""
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="payload must be a JSON object",
        )

    # Check idempotency key if provided.
    if idempotency_key:
        existing_task = await task_repo.get_by_idempotency_key(idempotency_key)
        if existing_task:
            response.status_code = status.HTTP_200_OK
            return TaskCreateResponse(
                id=existing_task.id,
                status=TaskStatus(existing_task.status),
                task_type=existing_task.task_type,
                idempotency_key=idempotency_key,
                message="Task already submitted with this idempotency key",
            )

    task_id = str(uuid.uuid4())
    payload_json = payload_to_json(payload)

    task = TaskDB(
        id=task_id,
        task_type=task_type,
        payload=payload_json,
        status=TaskStatus.queued.value,
        idempotency_key=idempotency_key,
        max_attempts=settings.max_task_retries,
        attempts=0,
    )

    try:
        await task_repo.create(task)
        await outbox_repo.create(
            task_id=task_id,
            event_type="task_created",
            payload=payload_to_json({"task_type": task_type, "payload": payload}),
        )
    except IntegrityError:
        # Lost a race against a concurrent request using the same
        # idempotency_key: the unique index rejected our insert. The other
        # request's task is now authoritative; fetch and return it instead
        # of a duplicate-key 500.
        await task_repo.session.rollback()
        if idempotency_key:
            existing_task = await task_repo.get_by_idempotency_key(idempotency_key)
            if existing_task:
                response.status_code = status.HTTP_200_OK
                return TaskCreateResponse(
                    id=existing_task.id,
                    status=TaskStatus(existing_task.status),
                    task_type=existing_task.task_type,
                    idempotency_key=idempotency_key,
                    message="Task already submitted with this idempotency key",
                )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Task could not be created due to a conflicting write",
        ) from None

    # Nudge the outbox relay so a freshly-created event doesn't have to wait
    # for its next polling cycle. Best-effort: the relay's own polling loop
    # is the correctness guarantee, this is purely a latency optimization.
    notify_outbox_relay()

    return TaskCreateResponse(
        id=task_id,
        status=TaskStatus.queued,
        task_type=task_type,
        idempotency_key=idempotency_key,
        message="Task submitted successfully",
    )


@router.get(
    "/tasks/{task_id}",
    response_model=TaskDetailResponse,
    summary="Get task status by ID",
    description="Retrieve the current status and result of a task by its ID.",
)
async def get_task(
    task_id: str,
    task_repo: TaskRepository = Depends(get_task_repo),
) -> TaskDetailResponse:
    """Get task status."""
    task = await task_repo.get(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )
    return _to_task_detail(task)


@router.get(
    "/tasks",
    response_model=TaskListResponse,
    summary="List tasks with pagination and filters",
    description="List tasks with optional status filter and pagination.",
)
async def list_tasks(
    status_filter: TaskStatus | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> TaskListResponse:
    """List tasks with pagination and optional status filter."""
    query = select(TaskDB)
    count_query = select(func.count()).select_from(TaskDB)

    if status_filter:
        query = query.where(TaskDB.status == status_filter.value)
        count_query = count_query.where(TaskDB.status == status_filter.value)

    total = (await session.execute(count_query)).scalar_one()

    query = query.order_by(TaskDB.created_at.desc())
    result = await session.execute(query.offset((page - 1) * size).limit(size))
    tasks = result.scalars().all()

    return TaskListResponse(
        items=[_to_task_detail(t) for t in tasks],
        page=page,
        size=size,
        total=total,
    )
