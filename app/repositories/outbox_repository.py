"""Data access repository for Transactional Outbox events."""

from datetime import datetime, timezone
from typing import List
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxModel


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
