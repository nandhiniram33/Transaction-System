"""Failure scenario tests for Pre-write and Post-write posting service failures."""

from decimal import Decimal
import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.db import async_session_maker
from app.repositories.transaction_repository import TransactionRepository
from app.schemas.transaction import TransactionCreateRequest
from app.services.posting_service_client import PostingServiceClient
from app.services.transaction_service import transaction_service
from app.workers.transaction_worker import TransactionWorker
import tests.mock_posting_server as mock_server


@pytest.fixture
def mock_client_app():
    """Create PostingServiceClient wired directly to mock_server ASGI application."""
    transport = ASGITransport(app=mock_server.app)
    client = PostingServiceClient(base_url="http://test")
    # Override underlying client creation
    client._client = AsyncClient(transport=transport, base_url="http://test")
    return client


@pytest.mark.asyncio
async def test_post_write_failure_recovery(db_session: AsyncSession, mock_client_app: PostingServiceClient):
    """Scenario: Mock service commits the write, but returns HTTP 500 (Post-Write failure).
    
    The worker should query GET /transactions/{id}, detect the transaction exists,
    and mark it completed without duplicating.
    """
    mock_server.FAILURE_MODE = "post_write_failure"

    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("300.00"),
        currency="USD",
        description="Post-write failure test",
        timestamp="2026-10-12T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    job_payload = {
        "id": tx_id,
        "amount": 300.00,
        "currency": "USD",
        "description": "Post-write failure test",
        "timestamp": "2026-10-12T10:00:00Z",
    }

    worker = TransactionWorker(worker_id="worker-post-write", p_client=mock_client_app)
    processed = await worker.process_job(job_payload)

    # Worker should detect post-write existence and consider it completed
    assert processed is True

    # Verify DB status is 'completed'
    tx_repo = TransactionRepository()
    async with async_session_maker() as session:
        tx = await tx_repo.get_by_id(session, tx_id)
        assert tx.status == "completed"
        assert tx.completed_at is not None

    # Verify no duplicate in mock server (exactly 1 record)
    matches = [r for r in mock_server.mock_db if r["id"] == tx_id]
    assert len(matches) == 1


@pytest.mark.asyncio
async def test_pre_write_failure_schedules_retry(db_session: AsyncSession, mock_client_app: PostingServiceClient):
    """Scenario: Mock service fails before writing (Pre-Write failure).
    
    The worker queries GET /transactions/{id}, finds it absent, and schedules a retry
    with exponential backoff instead of blind re-posting.
    """
    mock_server.FAILURE_MODE = "pre_write_failure"

    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("450.00"),
        currency="USD",
        description="Pre-write failure test",
        timestamp="2026-10-12T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    job_payload = {
        "id": tx_id,
        "amount": 450.00,
        "currency": "USD",
        "description": "Pre-write failure test",
        "timestamp": "2026-10-12T10:00:00Z",
    }

    worker = TransactionWorker(worker_id="worker-pre-write", p_client=mock_client_app)
    processed = await worker.process_job(job_payload)

    assert processed is False

    # Verify DB status is 'pending' with retry_count incremented and next_retry_at set
    tx_repo = TransactionRepository()
    async with async_session_maker() as session:
        tx = await tx_repo.get_by_id(session, tx_id)
        assert tx.status == "pending"
        assert tx.retry_count == 1
        assert tx.next_retry_at is not None
        assert "Simulated Pre-Write Failure" in tx.error

    # Remote database should have 0 records for this tx
    matches = [r for r in mock_server.mock_db if r["id"] == tx_id]
    assert len(matches) == 0

    # Now simulate subsequent retry when remote service has recovered
    mock_server.FAILURE_MODE = None
    processed_retry = await worker.process_job(job_payload)
    assert processed_retry is True

    async with async_session_maker() as session:
        tx_after = await tx_repo.get_by_id(session, tx_id)
        assert tx_after.status == "completed"
        assert tx_after.error is None

    matches_after = [r for r in mock_server.mock_db if r["id"] == tx_id]
    assert len(matches_after) == 1
