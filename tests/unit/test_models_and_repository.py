"""Unit tests for SQLAlchemy models, repositories, and atomic transitions."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.transaction import TransactionModel
from app.models.outbox import OutboxModel
from app.repositories.transaction_repository import TransactionRepository
from app.repositories.outbox_repository import OutboxRepository


@pytest.mark.asyncio
async def test_transaction_create_and_query(db_session: AsyncSession):
    """Verify transaction creation and query by ID."""
    repo = TransactionRepository()
    tx_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    tx = TransactionModel(
        id=tx_id,
        amount=Decimal("199.99"),
        currency="USD",
        description="Repository test",
        timestamp=now,
        payload_fingerprint="dummy_hash",
        status="pending",
        submitted_at=now,
    )
    await repo.create(db_session, tx)
    await db_session.commit()

    fetched = await repo.get_by_id(db_session, tx_id)
    assert fetched is not None
    assert fetched.id == tx_id
    assert fetched.amount == Decimal("199.99")
    assert fetched.status == "pending"


@pytest.mark.asyncio
async def test_atomic_claim_lease(db_session: AsyncSession):
    """Verify worker atomic claim succeeds once and prevents concurrent claims."""
    repo = TransactionRepository()
    tx_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    tx = TransactionModel(
        id=tx_id,
        amount=Decimal("50.00"),
        currency="EUR",
        description="Claim test",
        timestamp=now,
        payload_fingerprint="hash_claim",
        status="pending",
        submitted_at=now,
    )
    await repo.create(db_session, tx)
    await db_session.commit()

    lease_exp = now + timedelta(seconds=30)

    # Worker 1 claims -> succeeds
    claimed1 = await repo.claim_transaction(db_session, tx_id, "worker-1", lease_exp)
    await db_session.commit()
    assert claimed1 is True

    # Worker 2 tries to claim same transaction -> fails!
    claimed2 = await repo.claim_transaction(db_session, tx_id, "worker-2", lease_exp)
    await db_session.commit()
    assert claimed2 is False

    # Check state
    updated = await repo.get_by_id(db_session, tx_id)
    assert updated.status == "processing"
    assert updated.worker_id == "worker-1"


@pytest.mark.asyncio
async def test_stale_lease_reclaim(db_session: AsyncSession):
    """Verify expired lease allows re-claiming by another worker."""
    repo = TransactionRepository()
    tx_id = str(uuid.uuid4())
    past = datetime.now(timezone.utc) - timedelta(seconds=60)

    tx = TransactionModel(
        id=tx_id,
        amount=Decimal("75.00"),
        currency="GBP",
        description="Stale lease test",
        timestamp=past,
        payload_fingerprint="hash_stale",
        status="processing",
        worker_id="dead-worker",
        lease_expires_at=past,
        submitted_at=past,
    )
    await repo.create(db_session, tx)
    await db_session.commit()

    new_lease = datetime.now(timezone.utc) + timedelta(seconds=30)
    reclaimed = await repo.claim_transaction(
        db_session, tx_id, "active-worker", new_lease
    )
    await db_session.commit()
    assert reclaimed is True

    updated = await repo.get_by_id(db_session, tx_id)
    assert updated.worker_id == "active-worker"


@pytest.mark.asyncio
async def test_outbox_repository_lifecycle(db_session: AsyncSession):
    """Verify creating, querying pending, and marking dispatched outbox events."""
    repo = OutboxRepository()
    tx_id = str(uuid.uuid4())
    event = OutboxModel(
        transaction_id=tx_id,
        event_type="TRANSACTION_CREATED",
        payload='{"id": "123"}',
        status="pending",
    )
    await repo.create(db_session, event)
    await db_session.commit()

    pending = await repo.get_pending_events(db_session, limit=10)
    assert len(pending) == 1
    assert pending[0].transaction_id == tx_id

    dispatched_count = await repo.mark_dispatched(db_session, [pending[0].id])
    await db_session.commit()
    assert dispatched_count == 1

    remaining = await repo.get_pending_events(db_session, limit=10)
    assert len(remaining) == 0
