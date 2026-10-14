"""Failure scenario tests for concurrency, worker crashes, leases, and Redis outages."""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.db import async_session_maker
from app.models.transaction import TransactionModel
from app.queue.dispatcher import OutboxDispatcher
from app.queue.queue_client import QueueClient, queue_client
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.transaction_repository import TransactionRepository
from app.schemas.transaction import TransactionCreateRequest
from app.services.posting_service_client import PostingServiceClient
from app.services.retry_service import RetryService
from app.services.transaction_service import transaction_service
from app.workers.reconciliation_worker import ReconciliationWorker
from app.workers.transaction_worker import TransactionWorker
import tests.mock_posting_server as mock_server


@pytest.fixture
def mock_client_app():
    transport = ASGITransport(app=mock_server.app)
    client = PostingServiceClient(base_url="http://test")
    client._client = AsyncClient(transport=transport, base_url="http://test")
    return client


@pytest.mark.asyncio
async def test_concurrent_worker_claim_deduplication(db_session: AsyncSession, mock_client_app: PostingServiceClient):
    """Scenario: Two concurrent workers try to process the same queued transaction simultaneously.
    
    Only ONE worker must acquire the lease and submit to the remote service.
    Zero duplicate remote writes must occur.
    """
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("500.00"),
        currency="USD",
        description="Concurrency test",
        timestamp="2026-10-12T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    job_payload = {
        "id": tx_id,
        "amount": 500.00,
        "currency": "USD",
        "description": "Concurrency test",
        "timestamp": "2026-10-12T10:00:00Z",
    }

    worker1 = TransactionWorker(worker_id="worker-alpha", p_client=mock_client_app)
    worker2 = TransactionWorker(worker_id="worker-beta", p_client=mock_client_app)

    # Run both concurrently
    res1, res2 = await asyncio.gather(
        worker1.process_job(job_payload),
        worker2.process_job(job_payload),
    )

    # Exactly one worker should succeed, the other should skip gracefully
    assert (res1 is True and res2 is False) or (res1 is False and res2 is True)

    # Remote database must contain exactly 1 record (NO DUPLICATES!)
    matches = [r for r in mock_server.mock_db if r["id"] == tx_id]
    assert len(matches) == 1


@pytest.mark.asyncio
async def test_reconciliation_recovers_crashed_worker(db_session: AsyncSession):
    """Scenario: Worker crashes during processing, leaving an unexpired or expired lease.
    
    The ReconciliationWorker must identify the expired lease, revert state to pending,
    and re-enqueue the job so it is not lost.
    """
    tx_id = str(uuid.uuid4())
    past = datetime.now(timezone.utc) - timedelta(seconds=120)

    # Seed transaction stuck in processing with expired lease
    tx = TransactionModel(
        id=tx_id,
        amount=Decimal("700.00"),
        currency="USD",
        description="Crashed worker test",
        timestamp=past,
        payload_fingerprint="fp_crashed",
        status="processing",
        worker_id="crashed-worker-99",
        lease_expires_at=past,
        submitted_at=past,
    )
    tx_repo = TransactionRepository()
    await tx_repo.create(db_session, tx)
    await db_session.commit()

    # Run reconciliation
    reconciler = ReconciliationWorker()
    recovered = await reconciler.recover_stale_leases(db_session)
    assert recovered == 1

    # Verify transaction returned to pending
    async with async_session_maker() as session:
        rechecked = await tx_repo.get_by_id(session, tx_id)
        assert rechecked.status == "pending"
        assert rechecked.worker_id is None
        assert rechecked.lease_expires_at is None

    # Verify job was placed in Redis queue
    job = await queue_client.dequeue(timeout_seconds=1.0)
    assert job is not None
    assert job["id"] == tx_id


@pytest.mark.asyncio
async def test_outbox_survives_redis_outage(db_session: AsyncSession):
    """Scenario: Redis queue is temporarily unreachable when transaction is accepted.
    
    Transactional outbox must keep records safely in SQLite until Redis becomes reachable again.
    """
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("850.00"),
        currency="EUR",
        description="Redis outage test",
        timestamp="2026-10-12T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    # Create dummy broken queue client that fails enqueue
    class BrokenQueueClient(QueueClient):
        async def enqueue_batch(self, payloads):
            return 0  # Emulate total Redis outage

    broken_dispatcher = OutboxDispatcher(q_client=BrokenQueueClient())
    dispatched = await broken_dispatcher.dispatch_cycle(db_session)
    assert dispatched == 0

    # Verify outbox event is still pending in database (ZERO DATA LOSS!)
    outbox_repo = OutboxRepository()
    pending = await outbox_repo.get_pending_events(db_session, limit=10)
    assert any(e.transaction_id == tx_id for e in pending)

    # Now simulate Redis recovery with working queue client
    working_dispatcher = OutboxDispatcher(q_client=queue_client)
    dispatched_recovered = await working_dispatcher.dispatch_cycle(db_session)
    assert dispatched_recovered >= 1

    pending_after = await outbox_repo.get_pending_events(db_session, limit=10)
    assert not any(e.transaction_id == tx_id for e in pending_after)


@pytest.mark.asyncio
async def test_terminal_failure_after_max_retries(db_session: AsyncSession, mock_client_app: PostingServiceClient):
    """Scenario: Remote service persistently fails past maximum retry attempts.
    
    Transaction must be marked as 'failed' with error explanation.
    """
    mock_server.FAILURE_MODE = "pre_write_failure"

    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("999.00"),
        currency="USD",
        description="Max retries test",
        timestamp="2026-10-12T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    # Manually configure retry service with max_attempts=1
    custom_retry = RetryService(max_attempts=1)
    worker = TransactionWorker(
        worker_id="worker-fail",
        p_client=mock_client_app,
        r_service=custom_retry,
    )

    job_payload = {
        "id": tx_id,
        "amount": 999.00,
        "currency": "USD",
        "description": "Max retries test",
        "timestamp": "2026-10-12T10:00:00Z",
    }

    # Attempt 1 -> schedules retry (count becomes 1)
    await worker.process_job(job_payload)

    # Attempt 2 -> exceeds max_attempts (1) -> terminal failure!
    await worker.process_job(job_payload)

    async with async_session_maker() as session:
        tx_repo = TransactionRepository()
        final_tx = await tx_repo.get_by_id(session, tx_id)
        assert final_tx.status == "failed"
        assert final_tx.error is not None
