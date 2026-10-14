"""Integration tests for the Transaction REST API endpoints."""

import time
import uuid
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_submit_transaction_accepted(async_client: AsyncClient, sample_transaction_payload: dict):
    """Verify newly submitted transaction returns 202 Accepted within 100ms."""
    start_time = time.perf_counter()
    response = await async_client.post("/api/transactions", json=sample_transaction_payload)
    elapsed_ms = (time.perf_counter() - start_time) * 1000.0

    assert response.status_code == 202
    assert elapsed_ms < 100.0  # Strict sub-100ms requirement

    data = response.json()
    assert data["transactionId"] == sample_transaction_payload["id"]
    assert data["status"] in ("pending", "processing", "completed")
    assert "submittedAt" in data
    assert data["error"] is None


@pytest.mark.asyncio
async def test_get_transaction_by_id(async_client: AsyncClient, sample_transaction_payload: dict):
    """Verify querying an existing transaction by ID."""
    # First submit
    post_res = await async_client.post("/api/transactions", json=sample_transaction_payload)
    assert post_res.status_code == 202
    tx_id = sample_transaction_payload["id"]

    # Query status
    get_res = await async_client.get(f"/api/transactions/{tx_id}")
    assert get_res.status_code == 200
    data = get_res.json()
    assert data["transactionId"] == tx_id
    assert "status" in data
    assert "submittedAt" in data


@pytest.mark.asyncio
async def test_get_unknown_transaction_returns_404(async_client: AsyncClient):
    """Verify querying non-existent transaction returns HTTP 404."""
    unknown_id = str(uuid.uuid4())
    res = await async_client.get(f"/api/transactions/{unknown_id}")
    assert res.status_code == 404
    assert "not found" in res.json()["detail"].lower()


@pytest.mark.asyncio
async def test_identical_duplicate_handling(async_client: AsyncClient, sample_transaction_payload: dict):
    """Verify submitting the identical payload returns existing transaction without error."""
    # Initial submission
    res1 = await async_client.post("/api/transactions", json=sample_transaction_payload)
    assert res1.status_code == 202

    # Duplicate submission
    res2 = await async_client.post("/api/transactions", json=sample_transaction_payload)
    assert res2.status_code == 200
    assert res2.json()["transactionId"] == sample_transaction_payload["id"]


@pytest.mark.asyncio
async def test_conflicting_payload_detection(async_client: AsyncClient, sample_transaction_payload: dict):
    """Verify submitting conflicting payload with same ID returns HTTP 409 Conflict."""
    # Initial submission
    res1 = await async_client.post("/api/transactions", json=sample_transaction_payload)
    assert res1.status_code == 202

    # Conflicting payload with different amount
    conflicting_payload = sample_transaction_payload.copy()
    conflicting_payload["amount"] = 999.99

    res2 = await async_client.post("/api/transactions", json=conflicting_payload)
    assert res2.status_code == 409
    assert "different payload" in res2.json()["detail"].lower() or "conflict" in res2.json()["detail"].lower()


@pytest.mark.asyncio
async def test_validation_errors(async_client: AsyncClient):
    """Verify input validation rejections."""
    # Negative amount
    res = await async_client.post(
        "/api/transactions",
        json={
            "id": str(uuid.uuid4()),
            "amount": -10.0,
            "currency": "USD",
            "description": "Negative",
            "timestamp": "2026-10-12T10:00:00Z",
        },
    )
    assert res.status_code == 422

    # Malformed UUID
    res = await async_client.post(
        "/api/transactions",
        json={
            "id": "not-a-valid-uuid",
            "amount": 10.0,
            "currency": "USD",
            "description": "Invalid UUID",
            "timestamp": "2026-10-12T10:00:00Z",
        },
    )
    assert res.status_code == 422
