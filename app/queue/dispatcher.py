"""Transactional Outbox Dispatcher ensuring reliable message forwarding to Redis."""

import asyncio
import json
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.database.db import async_session_maker
from app.queue.queue_client import QueueClient, queue_client
from app.repositories.outbox_repository import OutboxRepository

logger = get_logger(__name__)
settings = get_settings()


class OutboxDispatcher:
    """Dispatches persisted outbox events to the Redis message queue."""

    def __init__(
        self,
        q_client: Optional[QueueClient] = None,
        batch_size: Optional[int] = None,
    ):
        self.queue = q_client or queue_client
        self.outbox_repo = OutboxRepository()
        self.batch_size = batch_size or settings.OUTBOX_BATCH_SIZE
        self._is_running: bool = False

    async def dispatch_cycle(self, session: AsyncSession) -> int:
        """Execute a single sweep of undispatched outbox records."""
        events = await self.outbox_repo.get_pending_events(session, limit=self.batch_size)
        if not events:
            return 0

        payloads = []
        event_ids = []
        for event in events:
            try:
                data = json.loads(event.payload)
                payloads.append(data)
                event_ids.append(event.id)
            except Exception as exc:
                logger.error("Corrupted outbox payload for event %s: %s", event.id, exc)

        if not payloads:
            return 0

        # Enqueue batch to Redis
        dispatched_count = await self.queue.enqueue_batch(payloads)
        if dispatched_count > 0:
            # Mark successfully enqueued events in database
            await self.outbox_repo.mark_dispatched(session, event_ids[:dispatched_count])
            await session.commit()
            logger.info("Dispatched %d outbox events to Redis queue", dispatched_count)
            return dispatched_count
        else:
            logger.warning("Queue unavailable; %d outbox events retained in DB", len(payloads))
            await session.rollback()
            return 0

    async def run_loop(self, poll_interval: Optional[float] = None) -> None:
        """Continuous background dispatch loop."""
        interval = poll_interval or settings.OUTBOX_POLL_INTERVAL_SECONDS
        self._is_running = True
        logger.info("Outbox dispatcher loop started (poll_interval=%ss)", interval)

        while self._is_running:
            try:
                async with async_session_maker() as session:
                    dispatched = await self.dispatch_cycle(session)
                    # If we dispatched a full batch, cycle immediately without sleep
                    if dispatched < self.batch_size:
                        await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.info("Outbox dispatcher loop cancelled.")
                break
            except Exception as exc:
                logger.error("Error in outbox dispatcher cycle: %s", exc, exc_info=True)
                await asyncio.sleep(interval)

    def stop(self) -> None:
        """Signal the dispatch loop to terminate."""
        self._is_running = False


# Global dispatcher instance
outbox_dispatcher = OutboxDispatcher()
