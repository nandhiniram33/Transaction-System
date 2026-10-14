"""Unit tests for the RetryService backoff engine and error classification."""

from datetime import datetime, timezone
from app.services.retry_service import RetryService


def test_classify_retryable_and_permanent_errors():
    """Verify permanent client errors vs transient server errors."""
    service = RetryService(max_attempts=3)

    # Transient / Retryable
    assert (
        service.is_retryable_error(
            status_code=500, is_timeout=False, is_network_error=False
        )
        is True
    )
    assert (
        service.is_retryable_error(
            status_code=503, is_timeout=False, is_network_error=False
        )
        is True
    )
    assert (
        service.is_retryable_error(
            status_code=None, is_timeout=True, is_network_error=False
        )
        is True
    )
    assert (
        service.is_retryable_error(
            status_code=None, is_timeout=False, is_network_error=True
        )
        is True
    )

    # Permanent Client Errors (non-retryable)
    assert (
        service.is_retryable_error(
            status_code=400, is_timeout=False, is_network_error=False
        )
        is False
    )
    assert (
        service.is_retryable_error(
            status_code=422, is_timeout=False, is_network_error=False
        )
        is False
    )
    assert (
        service.is_retryable_error(
            status_code=409, is_timeout=False, is_network_error=False
        )
        is False
    )


def test_backoff_exponential_growth_and_cap():
    """Verify exponential backoff grows and respects max cap without jitter."""
    service = RetryService(
        initial_backoff=1.0,
        max_backoff=10.0,
        multiplier=2.0,
        jitter_enabled=False,
    )

    assert service.calculate_backoff_seconds(0) == 1.0
    assert service.calculate_backoff_seconds(1) == 2.0
    assert service.calculate_backoff_seconds(2) == 4.0
    assert service.calculate_backoff_seconds(3) == 8.0
    assert service.calculate_backoff_seconds(4) == 10.0  # Capped at 10.0
    assert service.calculate_backoff_seconds(5) == 10.0  # Still capped


def test_backoff_jitter_bounds():
    """Verify jitter generates values within the specified randomized window."""
    service = RetryService(
        initial_backoff=2.0,
        max_backoff=16.0,
        multiplier=2.0,
        jitter_enabled=True,
    )
    for _ in range(20):
        val = service.calculate_backoff_seconds(2)  # raw is 8.0
        assert 1.0 <= val <= 8.0


def test_max_retry_limit():
    """Verify can_retry enforces the configured limit."""
    service = RetryService(max_attempts=3)
    assert service.can_retry(0) is True
    assert service.can_retry(1) is True
    assert service.can_retry(2) is True
    assert service.can_retry(3) is False
    assert service.can_retry(4) is False


def test_next_retry_time_future():
    """Verify next retry time is in the future."""
    service = RetryService(initial_backoff=5.0, jitter_enabled=False)
    now = datetime.now(timezone.utc)
    next_time = service.get_next_retry_time(0)
    assert next_time > now
