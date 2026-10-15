"""Reliability tests for critical production edge cases.

Covers:
1. GET-before-POST duplicate detection (remote check before submitting POST)
2. POST timeout and ambiguous write recovery (post-write confirmation vs retry scheduling)
3. Duplicate-request idempotency (identical duplicates return 200, conflicts return 409)
4. Worker restart recovery (simulating lease expiry, reconciliation re-enqueue, and fresh worker pickup)
5. Cleanup safety (lifecycle cleanup purges only eligible old completed records, never pending or processing work)
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database.db import async_session_maker
from app.models.outbox import OutboxModel
from app.models.transaction import TransactionModel
from app.queue.queue_client import queue_client
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.transaction_repository import TransactionRepository
from app.schemas.transaction import TransactionCreateRequest
from app.services.posting_service_client import PostingServiceClient, PostingResponse
from app.services.transaction_service import transaction_service
from app.workers.cleanup_worker import CleanupWorker
from app.workers.reconciliation_worker import ReconciliationWorker
from app.workers.transaction_worker import TransactionWorker
import tests.mock_posting_server as mock_server


@pytest.fixture
def mock_client_app():
    transport = ASGITransport(app=mock_server.app)
    client = PostingServiceClient(base_url="http://test")
    client._client = AsyncClient(transport=transport, base_url="http://test")
    return client


# =========================================================================
# 1. GET-before-POST Duplicate Detection
# =========================================================================
@pytest.mark.asyncio
async def test_get_before_post_duplicate_detection(
    db_session: AsyncSession, mock_client_app: PostingServiceClient
):
    """When a transaction already exists remotely, worker MUST detect it via GET

    and mark it completed WITHOUT calling POST /transactions.
    """
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("199.99"),
        currency="USD",
        description="GET-before-POST test",
        timestamp="2026-10-15T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    # Seed mock remote store beforehand
    mock_server.mock_db.append(
        {
            "id": tx_id,
            "amount": 199.99,
            "currency": "USD",
            "description": "Pre-existing on remote server",
            "timestamp": "2026-10-15T10:00:00Z",
        }
    )

    post_called = False

    class TrackingClient(PostingServiceClient):
        async def get_transaction(self, transaction_id: str) -> PostingResponse:
            return await mock_client_app.get_transaction(transaction_id)

        async def post_transaction(self, payload: dict) -> PostingResponse:
            nonlocal post_called
            post_called = True
            return await mock_client_app.post_transaction(payload)

    tracking_client = TrackingClient(base_url="http://test")
    tracking_client._client = mock_client_app._client

    worker = TransactionWorker(
        worker_id="test-worker-pre-check", p_client=tracking_client
    )
    job_payload = {
        "id": tx_id,
        "amount": 199.99,
        "currency": "USD",
        "description": "GET-before-POST test",
        "timestamp": "2026-10-15T10:00:00Z",
    }

    result = await worker.process_job(job_payload)
    assert result is True
    assert (
        not post_called
    ), "Worker must NOT call POST when GET finds existing remote record"

    async with async_session_maker() as session:
        tx = await TransactionRepository().get_by_id(session, tx_id)
        assert tx.status == "completed"
        assert tx.completed_at is not None


# =========================================================================
# 2. POST Timeout and Ambiguous Write Recovery
# =========================================================================
@pytest.mark.asyncio
async def test_post_timeout_ambiguous_write_recovery_confirmed(
    db_session: AsyncSession,
):
    """When POST times out but the remote service actually persisted the record (post-write timeout),

    the verification GET confirms it and completes the transaction cleanly without duplicate write.
    """
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("250.00"),
        currency="USD",
        description="Timeout ambiguous write test",
        timestamp="2026-10-15T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    class TimeoutWithRemoteCommitClient(PostingServiceClient):
        def __init__(self):
            super().__init__(base_url="http://test")
            self._posted = False

        async def get_transaction(self, transaction_id: str) -> PostingResponse:
            if self._posted:
                # Simulates that the remote server wrote the data before timeout
                return PostingResponse(
                    success=True,
                    status_code=200,
                    data={"id": transaction_id, "amount": 250.00},
                )
            return PostingResponse(success=False, status_code=404)

        async def post_transaction(self, payload: dict) -> PostingResponse:
            self._posted = True
            # Raise/return timeout
            return PostingResponse(
                success=False,
                status_code=None,
                error="Request timed out after 15.0 seconds",
                is_timeout=True,
            )

    client = TimeoutWithRemoteCommitClient()
    worker = TransactionWorker(worker_id="test-worker-timeout", p_client=client)
    job_payload = {
        "id": tx_id,
        "amount": 250.00,
        "currency": "USD",
        "description": "Timeout ambiguous write test",
        "timestamp": "2026-10-15T10:00:00Z",
    }

    result = await worker.process_job(job_payload)
    assert result is True

    async with async_session_maker() as session:
        tx = await TransactionRepository().get_by_id(session, tx_id)
        assert tx.status == "completed"
        assert tx.completed_at is not None


@pytest.mark.asyncio
async def test_post_timeout_ambiguous_write_recovery_unconfirmed_schedules_retry(
    db_session: AsyncSession,
):
    """When POST times out and verification GET confirms transaction is NOT on remote service,

    the worker schedules a retry with backoff.
    """
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("350.00"),
        currency="USD",
        description="Timeout pre-write failure test",
        timestamp="2026-10-15T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    class TimeoutPreWriteClient(PostingServiceClient):
        def __init__(self):
            super().__init__(base_url="http://test")

        async def get_transaction(self, transaction_id: str) -> PostingResponse:
            return PostingResponse(success=False, status_code=404)

        async def post_transaction(self, payload: dict) -> PostingResponse:
            return PostingResponse(
                success=False,
                status_code=None,
                error="Connection timed out",
                is_timeout=True,
            )

    client = TimeoutPreWriteClient()
    worker = TransactionWorker(worker_id="test-worker-timeout-retry", p_client=client)

    # Monkeypatch verify_remote_state to return False quickly without waiting full window
    async def fast_verify(tx_id, max_wait_seconds=12.0):
        return False

    worker.verify_remote_state = fast_verify

    job_payload = {
        "id": tx_id,
        "amount": 350.00,
        "currency": "USD",
        "description": "Timeout pre-write failure test",
        "timestamp": "2026-10-15T10:00:00Z",
    }

    result = await worker.process_job(job_payload)
    assert result is False

    async with async_session_maker() as session:
        tx = await TransactionRepository().get_by_id(session, tx_id)
        assert tx.status == "pending"
        assert tx.retry_count == 1
        assert tx.next_retry_at is not None


# =========================================================================
# 3. Duplicate-Request Idempotency
# =========================================================================
@pytest.mark.asyncio
async def test_duplicate_request_idempotency(async_client: AsyncClient):
    """POST /api/transactions with identical payload must return 200 without creating duplicates.

    POST with same ID but different payload must return 409 Conflict.
    """
    tx_id = str(uuid.uuid4())
    payload = {
        "id": tx_id,
        "amount": 100.50,
        "currency": "USD",
        "description": "Idempotency test",
        "timestamp": "2026-10-15T12:00:00Z",
    }

    # First submission -> 202 Accepted
    res1 = await async_client.post("/api/transactions", json=payload)
    assert res1.status_code == 202
    assert res1.json()["transactionId"] == tx_id
    assert res1.json()["status"] == "pending"

    # Second submission (identical) -> 200 OK (idempotent duplicate)
    res2 = await async_client.post("/api/transactions", json=payload)
    assert res2.status_code == 200
    assert res2.json()["transactionId"] == tx_id
    assert res2.json()["status"] == "pending"

    # Third submission (conflicting payload: different amount) -> 409 Conflict
    conflict_payload = dict(payload, amount=999.99)
    res3 = await async_client.post("/api/transactions", json=conflict_payload)
    assert res3.status_code == 409
    assert "already exists with different" in res3.json()["detail"]


# =========================================================================
# 4. Worker Restart Recovery
# =========================================================================
@pytest.mark.asyncio
async def test_worker_restart_recovery(
    db_session: AsyncSession, mock_client_app: PostingServiceClient
):
    """When a worker holding a lease crashes, reconciliation detects expired lease,

    resets the transaction to pending, re-enqueues it, and a newly started worker
    completes it.
    """
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("420.00"),
        currency="USD",
        description="Worker crash recovery test",
        timestamp="2026-10-15T10:00:00Z",
    )
    await transaction_service.accept_transaction(db_session, req)

    # Worker A claims the job
    worker_a = TransactionWorker(worker_id="crashed-worker-A", p_client=mock_client_app)
    claimed = await worker_a.claim_job(db_session, tx_id)
    assert claimed is True
    await db_session.commit()

    # Simulate worker crash: Worker A stops executing, time passes, lease expires
    past_expiration = datetime.now(timezone.utc) - timedelta(seconds=10)
    tx_repo = TransactionRepository()
    async with async_session_maker() as session:
        tx = await tx_repo.get_by_id(session, tx_id)
        tx.lease_expires_at = past_expiration
        await session.commit()

    # Worker B (new process/instance) starts and reconciliation runs
    reconciler = ReconciliationWorker(q_client=queue_client)
    async with async_session_maker() as session:
        recovered_count = await reconciler.recover_stale_leases(session)
        assert recovered_count == 1

    # Verify transaction was reset to pending and lease cleared
    async with async_session_maker() as session:
        tx_after = await tx_repo.get_by_id(session, tx_id)
        assert tx_after.status == "pending"
        assert tx_after.worker_id is None
        assert tx_after.lease_expires_at is None

    # New worker B dequeues the re-enqueued job and processes it
    job = await queue_client.dequeue(timeout_seconds=1.0)
    assert job is not None
    assert job["id"] == tx_id

    worker_b = TransactionWorker(
        worker_id="recovered-worker-B", p_client=mock_client_app
    )
    done = await worker_b.process_job(job)
    assert done is True

    async with async_session_maker() as session:
        final_tx = await tx_repo.get_by_id(session, tx_id)
        assert final_tx.status == "completed"
        assert final_tx.completed_at is not None


# =========================================================================
# 5. Cleanup Safety
# =========================================================================
@pytest.mark.asyncio
async def test_cleanup_safety_deletes_only_eligible_records(db_session: AsyncSession):
    """The cleanup worker must delete ONLY eligible dispatched outbox records

    for COMPLETED transactions older than OUTBOX_RETENTION_SECONDS.
    It must NEVER delete pending outbox, recent outbox, or actively processing transactions.
    """
    now = datetime.now(timezone.utc)
    old_time = now - timedelta(days=2)  # 48 hours ago (> 24 hr retention)
    recent_time = now - timedelta(hours=1)  # 1 hour ago (< 24 hr retention)

    tx_repo = TransactionRepository()
    outbox_repo = OutboxRepository()

    # Record 1: ELIGIBLE -> Dispatched outbox, completed transaction, old
    tx1_id = "tx-eligible-old-completed"
    tx1 = TransactionModel(
        id=tx1_id,
        amount=Decimal("10.00"),
        currency="USD",
        description="Eligible old",
        timestamp=old_time,
        payload_fingerprint="fp1",
        status="completed",
        submitted_at=old_time,
        completed_at=old_time,
    )
    ob1 = OutboxModel(
        transaction_id=tx1_id,
        payload="{}",
        status="dispatched",
        created_at=old_time,
        dispatched_at=old_time,
    )

    # Record 2: INELIGIBLE -> Dispatched outbox, completed transaction, but RECENT
    tx2_id = "tx-ineligible-recent-completed"
    tx2 = TransactionModel(
        id=tx2_id,
        amount=Decimal("20.00"),
        currency="USD",
        description="Recent completed",
        timestamp=recent_time,
        payload_fingerprint="fp2",
        status="completed",
        submitted_at=recent_time,
        completed_at=recent_time,
    )
    ob2 = OutboxModel(
        transaction_id=tx2_id,
        payload="{}",
        status="dispatched",
        created_at=recent_time,
        dispatched_at=recent_time,
    )

    # Record 3: INELIGIBLE -> PENDING outbox (never delete pending!)
    tx3_id = "tx-ineligible-pending-outbox"
    tx3 = TransactionModel(
        id=tx3_id,
        amount=Decimal("30.00"),
        currency="USD",
        description="Pending outbox",
        timestamp=old_time,
        payload_fingerprint="fp3",
        status="pending",
        submitted_at=old_time,
    )
    ob3 = OutboxModel(
        transaction_id=tx3_id,
        payload="{}",
        status="pending",
        created_at=old_time,
    )

    # Record 4: INELIGIBLE -> ACTIVELY PROCESSING transaction (never delete processing!)
    tx4_id = "tx-ineligible-processing"
    tx4 = TransactionModel(
        id=tx4_id,
        amount=Decimal("40.00"),
        currency="USD",
        description="Actively processing",
        timestamp=old_time,
        payload_fingerprint="fp4",
        status="processing",
        worker_id="active-worker",
        lease_expires_at=now + timedelta(minutes=5),
        submitted_at=old_time,
    )
    ob4 = OutboxModel(
        transaction_id=tx4_id,
        payload="{}",
        status="dispatched",
        created_at=old_time,
        dispatched_at=old_time,
    )

    # Persist all 4
    for tx in (tx1, tx2, tx3, tx4):
        await tx_repo.create(db_session, tx)
    for ob in (ob1, ob2, ob3, ob4):
        await outbox_repo.create(db_session, ob)
    await db_session.commit()

    # Run cleanup worker with 86400 seconds (24 hours) retention
    cleanup = CleanupWorker(retention_seconds=86400)
    deleted = await cleanup.run_cycle()

    # Exactly 1 record (Record 1) must be deleted
    assert deleted == 1

    # Verify database contents
    async with async_session_maker() as session:
        remaining_stmt = select(OutboxModel.transaction_id)
        res = await session.execute(remaining_stmt)
        remaining_tx_ids = set(res.scalars().all())

        assert tx1_id not in remaining_tx_ids, "Eligible record 1 must be deleted"
        assert (
            tx2_id in remaining_tx_ids
        ), "Recent completed record 2 must NOT be deleted"
        assert (
            tx3_id in remaining_tx_ids
        ), "Pending outbox record 3 must NEVER be deleted"
        assert (
            tx4_id in remaining_tx_ids
        ), "Actively processing record 4 must NEVER be deleted"
