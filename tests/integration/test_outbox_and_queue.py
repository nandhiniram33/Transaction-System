"""Integration tests for transactional outbox dispatching and worker end-to-end processing."""

from decimal import Decimal
import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.db import async_session_maker
from app.queue.dispatcher import OutboxDispatcher
from app.queue.queue_client import queue_client
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.transaction_repository import TransactionRepository
from app.schemas.transaction import TransactionCreateRequest
from app.services.transaction_service import transaction_service
from app.workers.transaction_worker import TransactionWorker
import tests.mock_posting_server as mock_server


@pytest.mark.asyncio
async def test_outbox_atomic_persistence_and_dispatch(db_session: AsyncSession):
    """Verify transaction and outbox event are saved atomically, then dispatched to queue."""
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("250.00"),
        currency="USD",
        description="Outbox test",
        timestamp="2026-10-12T10:00:00Z",
    )

    # 1. Accept transaction
    tx, is_new = await transaction_service.accept_transaction(db_session, req)
    assert is_new is True

    # Verify both exist in DB
    outbox_repo = OutboxRepository()
    pending = await outbox_repo.get_pending_events(db_session, limit=10)
    assert any(e.transaction_id == tx_id for e in pending)

    # 2. Dispatch outbox events
    dispatcher = OutboxDispatcher()
    dispatched_count = await dispatcher.dispatch_cycle(db_session)
    assert dispatched_count >= 1

    # Verify outbox record is marked dispatched
    pending_after = await outbox_repo.get_pending_events(db_session, limit=10)
    assert not any(e.transaction_id == tx_id for e in pending_after)

    # 3. Verify Redis has the job
    job = await queue_client.dequeue(timeout_seconds=1.0)
    assert job is not None
    assert job["id"] == tx_id


@pytest.mark.asyncio
async def test_worker_end_to_end_successful_processing(
    db_session: AsyncSession, mock_client_app
):
    """Verify worker picks up job, calls mock posting service, and completes transaction."""
    tx_id = str(uuid.uuid4())
    req = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("120.00"),
        currency="EUR",
        description="Worker E2E test",
        timestamp="2026-10-12T10:00:00Z",
    )

    await transaction_service.accept_transaction(db_session, req)
    dispatcher = OutboxDispatcher()
    await dispatcher.dispatch_cycle(db_session)

    job = await queue_client.dequeue(timeout_seconds=1.0)
    assert job is not None

    worker = TransactionWorker(worker_id="test-worker-e2e", p_client=mock_client_app)
    success = await worker.process_job(job)
    assert success is True

    # Verify database state is updated to completed
    tx_repo = TransactionRepository()
    async with async_session_maker() as verify_session:
        updated_tx = await tx_repo.get_by_id(verify_session, tx_id)
        assert updated_tx.status == "completed"
        assert updated_tx.completed_at is not None
        assert updated_tx.error is None

    # Verify mock posting service received the record
    assert any(rec["id"] == tx_id for rec in mock_server.mock_db)
