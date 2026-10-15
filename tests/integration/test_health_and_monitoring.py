"""Integration tests for the Health and Monitoring endpoints."""

import pytest
from httpx import AsyncClient
from app.queue.queue_client import queue_client


@pytest.mark.asyncio
async def test_health_check_healthy(async_client: AsyncClient):
    """Verify GET /api/health reports healthy system status."""
    res = await async_client.get("/api/health")
    assert res.status_code == 200
    data = res.json()

    assert data["status"] in ("healthy", "degraded")
    assert data["api"] == "ok"
    assert data["database"] == "connected"
    assert "queue" in data
    assert "metrics" in data
    assert "depth" in data["queue"]


@pytest.mark.asyncio
async def test_health_live(async_client: AsyncClient):
    """Verify GET /api/health/live reports liveness status."""
    res = await async_client.get("/api/health/live")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "live"
    assert data["api"] == "ok"


@pytest.mark.asyncio
async def test_health_ready_healthy(async_client: AsyncClient):
    """Verify GET /api/health/ready reports 200 when dependencies are healthy."""
    res = await async_client.get("/api/health/ready")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"
    assert data["redis"] == "connected"
    assert "queue" in data
    assert "pending_outbox_records" in data["queue"]


@pytest.mark.asyncio
async def test_health_ready_dependency_failure(async_client: AsyncClient, monkeypatch):
    """Verify GET /api/health/ready returns 503 when a required dependency fails."""

    async def mock_is_connected():
        return False

    monkeypatch.setattr(queue_client, "is_connected", mock_is_connected)

    res = await async_client.get("/api/health/ready")
    assert res.status_code == 503
    data = res.json()
    assert data["status"] == "unhealthy"
    assert data["redis"] == "disconnected"


@pytest.mark.asyncio
async def test_metrics_endpoint(
    async_client: AsyncClient, sample_transaction_payload: dict
):
    """Verify metrics collector records requests and exposes latency distribution."""
    # Send a transaction to register metrics
    await async_client.post("/api/transactions", json=sample_transaction_payload)

    res = await async_client.get("/api/metrics")
    assert res.status_code == 200
    data = res.json()

    assert data["total_submitted"] >= 1
    assert "latency_ms" in data
    assert "p50" in data["latency_ms"]
    assert "p95" in data["latency_ms"]
    assert "p99" in data["latency_ms"]


@pytest.mark.asyncio
async def test_monitoring_alias_endpoint(async_client: AsyncClient):
    """Verify /api/monitoring acts as an alias to /api/metrics."""
    res_metrics = await async_client.get("/api/metrics")
    res_monitoring = await async_client.get("/api/monitoring")

    assert res_monitoring.status_code == 200
    assert res_metrics.status_code == 200
    data_metrics = res_metrics.json()
    data_monitoring = res_monitoring.json()

    assert "total_submitted" in data_monitoring
    assert "latency_ms" in data_monitoring
    assert data_monitoring["total_submitted"] == data_metrics["total_submitted"]
