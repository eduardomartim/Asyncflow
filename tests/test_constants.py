"""Unit tests for app/core/constants.py.

Regression coverage for a real bug found while diagnosing the project:
assigning a dict/set as a class attribute inside an Enum body makes
EnumMeta try to turn it into a (broken) member instead of a helper. No I/O;
pure logic.
"""
from app.core.constants import ErrorCategory, TaskStatus


def test_task_status_has_no_spurious_members():
    """ALLOWED_TRANSITIONS must not leak into the enum as a fake member."""
    assert {s.value for s in TaskStatus} == {
        "queued",
        "processing",
        "retrying",
        "succeeded",
        "failed",
        "dead_lettered",
        "cancelled",
    }


def test_task_status_allowed_transitions():
    assert TaskStatus.queued.can_transition_to(TaskStatus.processing)
    assert TaskStatus.queued.can_transition_to(TaskStatus.cancelled)
    assert not TaskStatus.queued.can_transition_to(TaskStatus.succeeded)

    assert TaskStatus.processing.can_transition_to(TaskStatus.succeeded)
    assert TaskStatus.processing.can_transition_to(TaskStatus.retrying)

    assert not TaskStatus.succeeded.can_transition_to(TaskStatus.queued)
    assert not TaskStatus.dead_lettered.can_transition_to(TaskStatus.retrying)


def test_error_category_is_retryable():
    assert ErrorCategory.is_retryable(ErrorCategory.transient)
    assert ErrorCategory.is_retryable("validation")
    assert not ErrorCategory.is_retryable(ErrorCategory.permanent)
    assert not ErrorCategory.is_retryable("permanent")
