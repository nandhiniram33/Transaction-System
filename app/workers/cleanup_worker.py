"""Background lifecycle-managed cleanup worker to prune old completed outbox records."""

import asyncio
from typing import Optional

from app.core.config import get_settings
from app.core.logging import get_logger
from app.database.db import async_session_maker
from app.repositories.outbox_repository import OutboxRepository

logger = get_logger(__name__)
settings = get_settings()


class CleanupWorker:
    """Periodically purges old dispatched outbox records for completed transactions."""

    def __init__(
        self,
        retention_seconds: Optional[int] = None,
        poll_interval: Optional[float] = None,
    ):
        self.retention_seconds = (
            retention_seconds
            if retention_seconds is not None
            else settings.OUTBOX_RETENTION_SECONDS
        )
        self.poll_interval = (
            poll_interval
            if poll_interval is not None
            else settings.CLEANUP_INTERVAL_SECONDS
        )
        self.outbox_repo = OutboxRepository()
        self._is_running: bool = False

    async def run_cycle(self) -> int:
        """Run a single cleanup sweep."""
        async with async_session_maker() as session:
            try:
                deleted_count = (
                    await self.outbox_repo.delete_eligible_completed_records(
                        session, retention_seconds=self.retention_seconds
                    )
                )
                if deleted_count > 0:
                    await session.commit()
                    logger.info(
                        "Outbox cleanup deleted %d eligible expired records",
                        deleted_count,
                    )
                return deleted_count
            except Exception as exc:
                await session.rollback()
                logger.error("Error in outbox cleanup cycle: %s", exc, exc_info=True)
                return 0

    async def run_loop(self) -> None:
        """Continuous background cleanup loop."""
        self._is_running = True
        logger.info(
            "Outbox cleanup worker started (retention=%ss, interval=%ss)",
            self.retention_seconds,
            self.poll_interval,
        )

        while self._is_running:
            try:
                await self.run_cycle()
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                logger.info("Outbox cleanup worker received stop signal.")
                break
            except Exception as exc:
                logger.error("Error in outbox cleanup loop: %s", exc, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        """Signal cleanup worker to stop."""
        self._is_running = False


# Global singleton instance
cleanup_worker = CleanupWorker()
