"""Structured logging configuration with correlation ID support."""

import contextvars
import logging
import sys
from typing import Optional

correlation_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "correlation_id", default=None
)


class StructuredFormatter(logging.Formatter):
    """Custom formatter that injects correlation ID and formats log records."""

    def format(self, record: logging.LogRecord) -> str:
        cid = correlation_id_ctx.get()
        record.correlation_id = cid if cid else "-"
        return super().format(record)


def setup_logging(level: str = "INFO") -> None:
    """Configure application-wide logging."""
    log_level = getattr(logging, level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = StructuredFormatter(
        fmt="%(asctime)s [%(levelname)s] [corr_id:%(correlation_id)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Avoid duplicate handlers if setup_logging is called multiple times
    root_logger.handlers = [handler]

    # Suppress overly noisy logs from third-party libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Obtain a named logger."""
    return logging.getLogger(name)
