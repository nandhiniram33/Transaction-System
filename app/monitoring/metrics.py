"""In-memory metrics collector tracking latency distributions, throughput, and error rates."""

import statistics
import time
from collections import deque
from threading import Lock
from typing import Any, Dict


class MetricsCollector:
    """Thread-safe collector for request latencies, throughput counters, and error stats."""

    def __init__(self, latency_window_size: int = 1000):
        self._lock = Lock()
        self.latency_window_size = latency_window_size
        self._latencies_ms: deque = deque(maxlen=latency_window_size)
        self.total_submitted: int = 0
        self.total_completed: int = 0
        self.total_failed: int = 0
        self.total_retries: int = 0
        self.total_conflicts: int = 0
        self.total_duplicates: int = 0
        self.start_time: float = time.time()

    def record_submission_latency(self, latency_ms: float) -> None:
        """Record an API request duration in milliseconds."""
        with self._lock:
            self.total_submitted += 1
            self._latencies_ms.append(latency_ms)

    def record_completed(self) -> None:
        """Increment count of successfully posted transactions."""
        with self._lock:
            self.total_completed += 1

    def record_failed(self) -> None:
        """Increment count of terminal failures."""
        with self._lock:
            self.total_failed += 1

    def record_retry(self) -> None:
        """Increment count of scheduled retries."""
        with self._lock:
            self.total_retries += 1

    def record_conflict(self) -> None:
        """Increment count of detected payload conflicts."""
        with self._lock:
            self.total_conflicts += 1

    def record_duplicate(self) -> None:
        """Increment count of recognized identical duplicate requests."""
        with self._lock:
            self.total_duplicates += 1

    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """Generate point-in-time statistics including p50, p95, and p99 percentiles."""
        with self._lock:
            latencies = list(self._latencies_ms)
            submitted = self.total_submitted
            completed = self.total_completed
            failed = self.total_failed
            retries = self.total_retries
            conflicts = self.total_conflicts
            duplicates = self.total_duplicates

        uptime_seconds = max(1.0, time.time() - self.start_time)
        tps = submitted / uptime_seconds

        p50 = 0.0
        p95 = 0.0
        p99 = 0.0
        avg = 0.0

        if latencies:
            sorted_lat = sorted(latencies)
            n = len(sorted_lat)
            avg = round(statistics.mean(sorted_lat), 2)
            p50 = round(sorted_lat[int(0.50 * (n - 1))], 2)
            p95 = round(sorted_lat[int(0.95 * (n - 1))], 2)
            p99 = round(sorted_lat[int(0.99 * (n - 1))], 2)

        return {
            "uptime_seconds": round(uptime_seconds, 1),
            "throughput_tps": round(tps, 2),
            "total_submitted": submitted,
            "total_completed": completed,
            "total_failed": failed,
            "total_retries": retries,
            "total_conflicts": conflicts,
            "total_duplicates": duplicates,
            "latency_ms": {
                "sample_count": len(latencies),
                "avg": avg,
                "p50": p50,
                "p95": p95,
                "p99": p99,
            },
        }


# Global metrics singleton
metrics_collector = MetricsCollector()
