"""Data access repository for Transactional Outbox events."""

from datetime import datetime, timedelta, timezone
from typing import List
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxModel
from app.models.transaction import TransactionModel


class OutboxRepository:
    """Repository managing outbox events for guaranteed at-least-once dispatch."""

    async def create(
        self, session: AsyncSession, outbox_event: OutboxModel
    ) -> OutboxModel:
        """Persist a new outbox event."""
        session.add(outbox_event)
        return outbox_event

    async def get_pending_events(
        self, session: AsyncSession, limit: int = 100
    ) -> List[OutboxModel]:
        """Fetch oldest undispatched outbox events."""
        stmt = (
            select(OutboxModel)
            .where(OutboxModel.status == "pending")
            .order_by(OutboxModel.id.asc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def mark_dispatched(self, session: AsyncSession, event_ids: List[int]) -> int:
        """Mark outbox events as dispatched after successful queue push."""
        if not event_ids:
            return 0
        now = datetime.now(timezone.utc)
        stmt = (
            update(OutboxModel)
            .where(OutboxModel.id.in_(event_ids))
            .values(status="dispatched", dispatched_at=now)
        )
        result = await session.execute(stmt)
        return result.rowcount

    async def get_pending_count(self, session: AsyncSession) -> int:
        """Get total count of undispatched events in the outbox."""
        stmt = select(func.count(OutboxModel.id)).where(OutboxModel.status == "pending")
        result = await session.execute(stmt)
        return result.scalar() or 0

    async def delete_eligible_completed_records(
        self, session: AsyncSession, retention_seconds: int, limit: int = 1000
    ) -> int:
        """Delete outbox records that are dispatched and whose transaction is completed, older than retention cutoff.

        Never deletes outbox records that are 'pending' or whose transactions are still
        'pending' or 'processing'.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=retention_seconds)

        stmt = (
            select(OutboxModel.id)
            .join(TransactionModel, OutboxModel.transaction_id == TransactionModel.id)
            .where(
                OutboxModel.status == "dispatched",
                TransactionModel.status == "completed",
                (
                    (OutboxModel.dispatched_at <= cutoff)
                    | (
                        (OutboxModel.dispatched_at.is_(None))
                        & (OutboxModel.created_at <= cutoff)
                    )
                ),
            )
            .limit(limit)
        )
        result = await session.execute(stmt)
        ids_to_delete = list(result.scalars().all())
        if not ids_to_delete:
            return 0

        delete_stmt = delete(OutboxModel).where(OutboxModel.id.in_(ids_to_delete))
        res = await session.execute(delete_stmt)
        return res.rowcount
