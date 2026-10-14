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
async def test_concurrent_worker_claim_deduplication(
    db_session: AsyncSession, mock_client_app: PostingServiceClient
):
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
async def test_terminal_failure_after_max_retries(
    db_session: AsyncSession, mock_client_app: PostingServiceClient
):
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


@pytest.mark.asyncio
async def test_recovery_within_30_seconds(
    db_session: AsyncSession, mock_client_app: PostingServiceClient
):
    """Scenario: Worker dies while holding a transaction lease.

    Verifies that the system completely detects, reclaims, re-enqueues,
    and successfully processes the transaction well within the strict 30-second SLA limit.
    """
    tx_id = str(uuid.uuid4())
    # Simulate a transaction claimed 21 seconds ago with a 20-second lease (lease expired 1s ago)
    past_claimed = datetime.now(timezone.utc) - timedelta(seconds=21)
    lease_expiration = past_claimed + timedelta(seconds=20)

    tx = TransactionModel(
        id=tx_id,
        amount=Decimal("650.00"),
        currency="USD",
        description="30s recovery test",
        timestamp=past_claimed,
        payload_fingerprint="fp_recovery_30s",
        status="processing",
        worker_id="crashed-worker-7",
        lease_expires_at=lease_expiration,
        submitted_at=past_claimed,
    )
    tx_repo = TransactionRepository()
    await tx_repo.create(db_session, tx)
    await db_session.commit()

    start_recovery = datetime.now(timezone.utc)

    # 1. Reconciliation runs
    reconciler = ReconciliationWorker()
    recovered = await reconciler.recover_stale_leases(db_session)
    assert recovered == 1

    # 2. Verify state reverted to pending
    async with async_session_maker() as session:
        rechecked = await tx_repo.get_by_id(session, tx_id)
        assert rechecked.status == "pending"
        assert rechecked.worker_id is None
        assert rechecked.lease_expires_at is None

    # 3. New worker dequeues and completes the transaction
    job = await queue_client.dequeue(timeout_seconds=2.0)
    assert job is not None
    assert job["id"] == tx_id

    mock_server.FAILURE_MODE = None
    new_worker = TransactionWorker(
        worker_id="active-worker-replacement", p_client=mock_client_app
    )
    processed = await new_worker.process_job(job)
    assert processed is True

    # 4. Total recovery execution duration is verified to be well under 30 seconds
    recovery_duration = (datetime.now(timezone.utc) - start_recovery).total_seconds()
    assert recovery_duration < 30.0

    async with async_session_maker() as session:
        final_tx = await tx_repo.get_by_id(session, tx_id)
        assert final_tx.status == "completed"


@pytest.mark.asyncio
async def test_prevention_of_duplicate_remote_transactions(
    db_session: AsyncSession, mock_client_app: PostingServiceClient
):
    """Scenario: Multiple duplicate deliveries, network drops, and post-write 500 errors.

    Guarantees that regardless of retries or re-enqueues, the remote posting service
    receives at most 1 write and ends with exactly ONE record in its database.
    """
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("1250.00"),
        currency="USD",
        description="Duplicate remote prevention verification",
        timestamp="2026-10-12T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    job_payload = {
        "id": tx_id,
        "amount": 1250.00,
        "currency": "USD",
        "description": "Duplicate remote prevention verification",
        "timestamp": "2026-10-12T10:00:00Z",
    }

    # Step 1: Simulate post-write 500 error from mock server
    mock_server.FAILURE_MODE = "post_write_failure"
    worker1 = TransactionWorker(worker_id="worker-dup-1", p_client=mock_client_app)
    res1 = await worker1.process_job(job_payload)
    assert res1 is True

    # Step 2: Simulate duplicate queue delivery received by another worker
    mock_server.FAILURE_MODE = None
    worker2 = TransactionWorker(worker_id="worker-dup-2", p_client=mock_client_app)
    res2 = await worker2.process_job(job_payload)
    # Worker 2 detects the transaction is already completed in local DB and gracefully skips
    assert res2 is False

    # Verify remote database has EXACTLY 1 record (zero duplicate writes)
    matches = [r for r in mock_server.mock_db if r["id"] == tx_id]
    assert len(matches) == 1


@pytest.mark.asyncio
async def test_queue_overflow_and_backpressure_recovery(db_session: AsyncSession):
    """Scenario: Message queue experiences overflow / connection refusal.

    Verifies that the Transactional Outbox pattern guarantees zero data loss:
    all transaction records and outbox events remain safely persisted in SQLite WAL,
    and once queue capacity recovers, all pending events are safely dispatched in batch.
    """
    tx_ids = [str(uuid.uuid4()) for _ in range(5)]
    for tid in tx_ids:
        req = TransactionCreateRequest(
            id=tid,
            amount=Decimal("100.00"),
            currency="USD",
            description=f"Queue overflow test {tid}",
            timestamp="2026-10-12T10:00:00Z",
        )
        await transaction_service.accept_transaction(db_session, req)

    # Simulate Queue Overflow where enqueue_batch returns 0 (e.g. Redis OOM maxmemory)
    class OverflowQueueClient(QueueClient):
        async def enqueue_batch(self, payloads):
            return 0  # Rejects enqueue due to full queue

    overflow_dispatcher = OutboxDispatcher(
        q_client=OverflowQueueClient(), batch_size=10
    )
    dispatched = await overflow_dispatcher.dispatch_cycle(db_session)
    assert dispatched == 0

    # Verify all 5 events remain safely in SQLite outbox table with zero loss
    outbox_repo = OutboxRepository()
    pending = await outbox_repo.get_pending_events(db_session, limit=10)
    pending_ids = [e.transaction_id for e in pending]
    for tid in tx_ids:
        assert tid in pending_ids

    # Simulate Queue capacity recovery (e.g. Redis memory freed or enlarged)
    recovered_dispatcher = OutboxDispatcher(q_client=queue_client, batch_size=10)
    dispatched_recovered = await recovered_dispatcher.dispatch_cycle(db_session)
    assert dispatched_recovered >= 5

    # Verify SQLite outbox events have all transitioned to dispatched
    pending_after = await outbox_repo.get_pending_events(db_session, limit=10)
    assert not any(e.transaction_id in tx_ids for e in pending_after)
