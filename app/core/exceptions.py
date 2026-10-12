"""AsyncFlow custom exceptions."""
from http import HTTPStatus
from typing import Any


class AsyncFlowError(Exception):
    """Base exception for AsyncFlow errors."""
    def __init__(
        self,
        code: str = "asyncflow_error",
        message: str = "An unexpected error occurred",
        status_code: int = HTTPStatus.INTERNAL_SERVER_ERROR,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class TaskNotFoundError(AsyncFlowError):
    """Task not found error."""
    def __init__(self, task_id: str) -> None:
        super().__init__(
            code="task_not_found",
            message=f"Task {task_id} not found",
            status_code=HTTPStatus.NOT_FOUND,
            details={"task_id": task_id},
        )


class TaskValidationError(AsyncFlowError):
    """Task validation error."""
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(
            code="task_validation_error",
            message=message,
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            details=details,
        )


class IdempotencyError(AsyncFlowError):
    """Idempotency violation error."""
    def __init__(self, key: str, message: str = "Idempotency violation") -> None:
        super().__init__(
            code="idempotency_error",
            message=f"{message}: key={key}",
            status_code=HTTPStatus.CONFLICT,
            details={"idempotency_key": key},
        )


class RetryExhaustedError(AsyncFlowError):
    """Retry exhausted error."""
    def __init__(self, task_id: str, attempts: int, max_attempts: int) -> None:
        super().__init__(
            code="retry_exhausted",
            message=f"Maximum retry attempts exceeded for task {task_id}",
            status_code=HTTPStatus.GONE,
            details={
                "task_id": task_id,
                "attempts": attempts,
                "max_attempts": max_attempts,
            },
        )


class DLQError(AsyncFlowError):
    """Dead-letter queue error."""
    def __init__(self, task_id: str, reason: str) -> None:
        super().__init__(
            code="dlq_error",
            message=f"Task {task_id} moved to dead-letter queue: {reason}",
            status_code=HTTPStatus.GONE,
            details={"task_id": task_id, "reason": reason},
        )


class TaskRetryableError(Exception):
    """Raised by a worker task handler to signal a transient failure.

    The worker catches this, increments the task's attempt counter, and
    schedules a retry with exponential backoff (while attempts remain).
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class TaskPermanentError(Exception):
    """Raised by a worker task handler to signal a non-retryable failure.

    The worker catches this and dead-letters the task immediately,
    regardless of how many attempts remain.
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)