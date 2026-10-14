"""Retry policy and exponential backoff engine with jitter."""

from datetime import datetime, timedelta, timezone
import random
from typing import Optional
from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class RetryService:
    """Calculates backoff intervals and determines retryability of failures."""

    def __init__(
        self,
        max_attempts: Optional[int] = None,
        initial_backoff: Optional[float] = None,
        max_backoff: Optional[float] = None,
        multiplier: Optional[float] = None,
        jitter_enabled: Optional[bool] = None,
    ):
        self.max_attempts = max_attempts or settings.MAX_RETRY_ATTEMPTS
        self.initial_backoff = initial_backoff or settings.INITIAL_BACKOFF_SECONDS
        self.max_backoff = max_backoff or settings.MAX_BACKOFF_SECONDS
        self.multiplier = multiplier or settings.BACKOFF_MULTIPLIER
        self.jitter_enabled = (
            jitter_enabled if jitter_enabled is not None else settings.JITTER_ENABLED
        )

    def is_retryable_error(self, status_code: Optional[int], is_timeout: bool, is_network_error: bool) -> bool:
        """Distinguish transient/retryable errors from permanent client errors.
        
        Permanent errors:
          - 400, 422: malformed data rejected by remote
          - 409: conflicting data
        Transient errors:
          - Timeouts, Connection resets, 500, 502, 503, 504
        """
        if is_timeout or is_network_error:
            return True

        if status_code is None:
            return True

        # 4xx client errors are permanent
        if 400 <= status_code < 500 and status_code != 408:
            return False

        # 5xx server errors are transient
        if status_code >= 500:
            return True

        return False

    def can_retry(self, current_retry_count: int) -> bool:
        """Check whether maximum retry attempts have been reached."""
        return current_retry_count < self.max_attempts

    def calculate_backoff_seconds(self, retry_count: int) -> float:
        """Compute exponential backoff with full jitter: delay = uniform(0, min(max_backoff, initial * (multiplier^count)))."""
        raw_backoff = self.initial_backoff * (self.multiplier ** retry_count)
        capped_backoff = min(self.max_backoff, raw_backoff)

        if self.jitter_enabled:
            # Full jitter prevents thundering herd / synchronization
            return random.uniform(0.5 * self.initial_backoff, capped_backoff)
        return capped_backoff

    def get_next_retry_time(self, retry_count: int) -> datetime:
        """Return absolute UTC datetime for next scheduled attempt."""
        delay = self.calculate_backoff_seconds(retry_count)
        return datetime.now(timezone.utc) + timedelta(seconds=delay)


# Global retry service instance
retry_service = RetryService()
