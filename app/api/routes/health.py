"""Health and monitoring endpoints for system observability."""

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.db import get_db_session
from app.monitoring.metrics import metrics_collector
from app.queue.queue_client import queue_client
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.transaction_repository import TransactionRepository

router = APIRouter(prefix="/api", tags=["Monitoring"])


@router.get("/health", summary="System health, dependencies, and queue status")
async def health_check(
    response: Response,
    session: AsyncSession = Depends(get_db_session),
):
    """Check status of SQLite, Redis, queue depth, outbox, and processing metrics."""
    # 1. Database check
    db_healthy = False
    status_counts = {}
    pending_outbox_count = 0
    try:
        await session.execute(text("SELECT 1"))
        db_healthy = True
        tx_repo = TransactionRepository()
        outbox_repo = OutboxRepository()
        status_counts = await tx_repo.get_status_counts(session)
        pending_outbox_count = await outbox_repo.get_pending_count(session)
    except Exception:
        db_healthy = False

    # 2. Redis queue check
    redis_healthy = False
    queue_depth = 0
    try:
        redis_healthy = await queue_client.is_connected()
        if redis_healthy:
            queue_depth = await queue_client.get_queue_depth()
    except Exception:
        redis_healthy = False

    # Determine overall system health
    if not db_healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        overall_status = "unhealthy"
    elif not redis_healthy:
        response.status_code = status.HTTP_200_OK
        overall_status = "degraded"
    else:
        response.status_code = status.HTTP_200_OK
        overall_status = "healthy"

    metrics_snapshot = metrics_collector.get_metrics_snapshot()

    return {
        "status": overall_status,
        "api": "ok",
        "database": "connected" if db_healthy else "disconnected",
        "redis": "connected" if redis_healthy else "disconnected",
        "queue": {
            "depth": queue_depth,
            "pending_outbox_records": pending_outbox_count,
        },
        "transactions_by_status": status_counts,
        "worker": {
            "status": "active" if overall_status != "unhealthy" else "degraded",
        },
        "metrics": metrics_snapshot,
    }


@router.get("/metrics", summary="Detailed application metrics")
async def get_metrics():
    """Return raw metrics counters and latency distribution."""
    return metrics_collector.get_metrics_snapshot()
