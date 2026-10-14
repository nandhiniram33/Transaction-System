"""Data access repository for Transaction records with atomic operations."""

from datetime import datetime, timezone
from typing import Dict, List, Optional
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.transaction import TransactionModel


class TransactionRepository:
    """Repository handling transaction persistence, atomic state transitions, and leases."""

    async def create(
        self, session: AsyncSession, transaction: TransactionModel
    ) -> TransactionModel:
        """Persist a new transaction entity."""
        session.add(transaction)
        return transaction

    async def get_by_id(
        self, session: AsyncSession, transaction_id: str
    ) -> Optional[TransactionModel]:
        """Fetch transaction by UUID."""
        stmt = select(TransactionModel).where(TransactionModel.id == transaction_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def claim_transaction(
        self,
        session: AsyncSession,
        transaction_id: str,
        worker_id: str,
        lease_expires_at: datetime,
    ) -> bool:
        """Atomically claim a transaction for worker execution.

        Succeeds only if the transaction is pending, or if an existing lease has expired.
        Returns True if claim succeeded, False if already claimed or finished.
        """
        now = datetime.now(timezone.utc)
        stmt = (
            update(TransactionModel)
            .where(
                TransactionModel.id == transaction_id,
                (
                    (TransactionModel.status == "pending")
                    | (
                        (TransactionModel.status == "processing")
                        & (TransactionModel.lease_expires_at < now)
                    )
                ),
            )
            .values(
                status="processing",
                worker_id=worker_id,
                lease_expires_at=lease_expires_at,
                last_attempt_at=now,
            )
        )
        result = await session.execute(stmt)
        return result.rowcount > 0

    async def mark_completed(
        self,
        session: AsyncSession,
        transaction_id: str,
        completed_at: Optional[datetime] = None,
    ) -> bool:
        """Mark transaction as completed and clear worker leases."""
        if completed_at is None:
            completed_at = datetime.now(timezone.utc)

        stmt = (
            update(TransactionModel)
            .where(TransactionModel.id == transaction_id)
            .values(
                status="completed",
                completed_at=completed_at,
                error=None,
                worker_id=None,
                lease_expires_at=None,
            )
        )
        result = await session.execute(stmt)
        return result.rowcount > 0

    async def mark_failed(
        self,
        session: AsyncSession,
        transaction_id: str,
        error_message: str,
    ) -> bool:
        """Mark transaction permanently failed with reason."""
        stmt = (
            update(TransactionModel)
            .where(TransactionModel.id == transaction_id)
            .values(
                status="failed",
                error=error_message,
                worker_id=None,
                lease_expires_at=None,
            )
        )
        result = await session.execute(stmt)
        return result.rowcount > 0

    async def schedule_retry(
        self,
        session: AsyncSession,
        transaction_id: str,
        next_retry_at: datetime,
        retry_count: int,
        error_message: str,
    ) -> bool:
        """Schedule transaction for subsequent retry."""
        stmt = (
            update(TransactionModel)
            .where(TransactionModel.id == transaction_id)
            .values(
                status="pending",
                retry_count=retry_count,
                next_retry_at=next_retry_at,
                error=error_message,
                worker_id=None,
                lease_expires_at=None,
            )
        )
        result = await session.execute(stmt)
        return result.rowcount > 0

    async def get_stale_processing(
        self,
        session: AsyncSession,
        now: datetime,
        limit: int = 50,
    ) -> List[TransactionModel]:
        """Find transactions stuck in 'processing' whose lease expired."""
        stmt = (
            select(TransactionModel)
            .where(
                TransactionModel.status == "processing",
                TransactionModel.lease_expires_at <= now,
            )
            .limit(limit)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def get_status_counts(self, session: AsyncSession) -> Dict[str, int]:
        """Aggregate counts of transactions grouped by status."""
        stmt = select(
            TransactionModel.status, func.count(TransactionModel.id)
        ).group_by(TransactionModel.status)
        result = await session.execute(stmt)
        counts = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
        for status, count in result.all():
            counts[status] = count
        return counts
