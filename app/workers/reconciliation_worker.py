"""Reconciliation worker recovering stale leases and re-enqueuing due retries."""

import asyncio
from datetime import datetime, timezone
import json
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.database.db import async_session_maker
from app.models.transaction import TransactionModel
from app.queue.queue_client import QueueClient, queue_client
from app.repositories.transaction_repository import TransactionRepository

logger = get_logger(__name__)
settings = get_settings()


class ReconciliationWorker:
    """Recovers crashed worker leases and dispatches scheduled retries."""

    def __init__(
        self,
        q_client: Optional[QueueClient] = None,
        poll_interval: Optional[float] = None,
    ):
        self.queue = q_client or queue_client
        self.poll_interval = poll_interval or settings.RECONCILIATION_INTERVAL_SECONDS
        self.tx_repo = TransactionRepository()
        self._is_running: bool = False

    async def recover_stale_leases(self, session: AsyncSession) -> int:
        """Find transactions stuck in 'processing' whose worker lease expired."""
        now = datetime.now(timezone.utc)
        stale_txs = await self.tx_repo.get_stale_processing(session, now=now, limit=100)
        if not stale_txs:
            return 0

        recovered_count = 0
        for tx in stale_txs:
            logger.warning(
                "Recovering orphaned transaction %s (lease expired at %s, worker %s)",
                tx.id,
                tx.lease_expires_at,
                tx.worker_id,
            )
            # Revert to pending and clear lease
            tx.status = "pending"
            tx.worker_id = None
            tx.lease_expires_at = None

            # Re-enqueue job
            job_payload = {
                "id": tx.id,
                "amount": float(tx.amount),
                "currency": tx.currency,
                "description": tx.description,
                "timestamp": tx.timestamp.isoformat(),
                "metadata": json.loads(tx.metadata_json) if tx.metadata_json else {},
            }
            await self.queue.enqueue(job_payload)
            recovered_count += 1

        await session.commit()
        logger.info("Successfully recovered %d stale processing transactions", recovered_count)
        return recovered_count

    async def dispatch_due_retries(self, session: AsyncSession) -> int:
        """Find pending transactions whose next_retry_at has elapsed and enqueue them."""
        now = datetime.now(timezone.utc)
        stmt = (
            select(TransactionModel)
            .where(
                TransactionModel.status == "pending",
                TransactionModel.next_retry_at.is_not(None),
                TransactionModel.next_retry_at <= now,
            )
            .limit(100)
        )
        result = await session.execute(stmt)
        due_txs = list(result.scalars().all())

        if not due_txs:
            return 0

        dispatched_count = 0
        for tx in due_txs:
            # Clear next_retry_at so we don't re-enqueue multiple times
            tx.next_retry_at = None

            job_payload = {
                "id": tx.id,
                "amount": float(tx.amount),
                "currency": tx.currency,
                "description": tx.description,
                "timestamp": tx.timestamp.isoformat(),
                "metadata": json.loads(tx.metadata_json) if tx.metadata_json else {},
            }
            await self.queue.enqueue(job_payload)
            dispatched_count += 1

        await session.commit()
        logger.info("Dispatched %d due retry transactions to Redis", dispatched_count)
        return dispatched_count

    async def run_cycle(self) -> None:
        """Run a single reconciliation sweep."""
        async with async_session_maker() as session:
            await self.recover_stale_leases(session)
            await self.dispatch_due_retries(session)

    async def run_loop(self) -> None:
        """Continuous background reconciliation loop."""
        self._is_running = True
        logger.info(
            "Reconciliation worker started (interval=%ss)", self.poll_interval
        )

        while self._is_running:
            try:
                await self.run_cycle()
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                logger.info("Reconciliation worker received stop signal.")
                break
            except Exception as exc:
                logger.error("Error in reconciliation cycle: %s", exc, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        """Signal reconciliation worker to stop."""
        self._is_running = False


# Global reconciliation worker instance
reconciliation_worker = ReconciliationWorker()
