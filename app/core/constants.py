"""AsyncFlow - Core constants and state machine."""
from enum import Enum, StrEnum


class TaskStatus(StrEnum):
    """Task lifecycle states."""
    queued = "queued"
    processing = "processing"
    retrying = "retrying"
    succeeded = "succeeded"
    failed = "failed"
    dead_lettered = "dead_lettered"
    cancelled = "cancelled"

    def can_transition_to(self, target: "TaskStatus") -> bool:
        return target.value in _ALLOWED_TASK_TRANSITIONS[self.value]


# Permitted state transitions. Kept OUTSIDE the Enum body on purpose: assigning
# a plain dict/set as a class attribute inside an Enum class makes EnumMeta try
# to turn it into a (broken, unhashable-value) member instead of a helper.
_ALLOWED_TASK_TRANSITIONS: dict[str, list[str]] = {
    "queued": ["processing", "cancelled"],
    "processing": ["succeeded", "failed", "retrying", "cancelled"],
    "retrying": ["succeeded", "failed", "processing"],
    "succeeded": [],
    "failed": ["retrying"],
    "dead_lettered": [],
    "cancelled": [],
}


class TaskPriority(int, Enum):
    """Task priority levels."""
    low = 1
    medium = 2
    high = 3
    critical = 4

    def __lt__(self, other: object) -> bool:
        if isinstance(other, TaskPriority):
            return self.value < other.value
        return NotImplemented


class ErrorCategory(StrEnum):
    """Error categorization for retry/DLQ logic."""
    transient = "transient"
    permanent = "permanent"
    validation = "validation"

    @classmethod
    def is_retryable(cls, category: "ErrorCategory | str") -> bool:
        """Transient and validation* errors are eligible for a retry.

        (*Pragmatic choice: a validation error caused by bad input will keep
        failing identically on retry, but we don't want a single flaky
        upstream validator to immediately dead-letter a task. Retries are
        bounded by max_attempts either way.)
        """
        value = category.value if isinstance(category, ErrorCategory) else category
        return value in {cls.transient.value, cls.validation.value}


class ErrorCode(StrEnum):
    """Standardized error codes."""
    not_found = "not_found"
    validation_error = "validation_error"
    internal_error = "internal_error"
    queue_unavailable = "queue_unavailable"
    idempotency_violation = "idempotency_violation"


class MessageLabel(StrEnum):
    """RabbitMQ message labels for correlation."""
    task = "task"
    outbox = "outbox"
    dead_letter = "dead_letter"


class QueueName(StrEnum):
    """RabbitMQ queue names."""
    tasks = "tasks"
    tasks_dead_letter = "tasks.dead_letter"
    tasks_retry = "tasks.retry"
    outbox = "outbox"


class ExchangeName(StrEnum):
    """RabbitMQ exchange names."""
    tasks = "tasks"
    dead_letter = "dead_letter"


class RoutingKey(StrEnum):
    """RabbitMQ routing keys."""
    task = "task"
    retry = "retry"
    dead_letter = "dead_letter"
    outbox = "outbox"