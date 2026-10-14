"""Integration tests for the Health and Monitoring endpoints."""

import pytest
from httpx import AsyncClient


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
async def test_metrics_endpoint(async_client: AsyncClient, sample_transaction_payload: dict):
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
