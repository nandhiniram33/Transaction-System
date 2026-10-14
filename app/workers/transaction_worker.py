"""Worker processing queued transactions with deduplication and failure verification."""

import asyncio
from datetime import datetime, timedelta, timezone
import os
import uuid
from typing import Any, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.database.db import async_session_maker
from app.monitoring.metrics import metrics_collector
from app.queue.queue_client import QueueClient, queue_client
from app.repositories.transaction_repository import TransactionRepository
from app.services.posting_service_client import PostingServiceClient, posting_client
from app.services.retry_service import RetryService, retry_service

logger = get_logger(__name__)
settings = get_settings()


class TransactionWorker:
    """Processes transaction jobs from the Redis queue with atomic leases and remote verification."""

    def __init__(
        self,
        worker_id: Optional[str] = None,
        q_client: Optional[QueueClient] = None,
        p_client: Optional[PostingServiceClient] = None,
        r_service: Optional[RetryService] = None,
    ):
        self.worker_id = worker_id or f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.queue = q_client or queue_client
        self.posting_client = p_client or posting_client
        self.retry_service = r_service or retry_service
        self.tx_repo = TransactionRepository()
        self._is_running: bool = False

    async def claim_job(self, session: AsyncSession, tx_id: str) -> bool:
        """Atomically claim a transaction lease to prevent concurrent execution."""
        lease_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=settings.PROCESSING_LEASE_TIMEOUT_SECONDS
        )
        return await self.tx_repo.claim_transaction(
            session=session,
            transaction_id=tx_id,
            worker_id=self.worker_id,
            lease_expires_at=lease_expires_at,
        )

    async def verify_remote_state(self, tx_id: str, max_wait_seconds: float = 12.0) -> bool:
        """Poll the remote service to confirm whether an ambiguous POST was eventually committed.
        
        The mock posting service introduces a 2-10 second write delay. We safely poll
        over the delay window before determining whether the write genuinely failed.
        """
        start_time = asyncio.get_event_loop().time()
        poll_interval = 2.0

        while (asyncio.get_event_loop().time() - start_time) < max_wait_seconds:
            get_res = await self.posting_client.get_transaction(tx_id)
            if get_res.success:
                logger.info("Delayed remote write confirmed for transaction %s", tx_id)
                return True
            await asyncio.sleep(poll_interval)

        # Final check after timeout window
        final_res = await self.posting_client.get_transaction(tx_id)
        return final_res.success

    async def process_job(self, job_data: Dict[str, Any]) -> bool:
        """Execute the full transaction processing lifecycle."""
        tx_id = job_data.get("id")
        if not tx_id:
            logger.error("Received invalid job data missing 'id': %s", job_data)
            return False

        logger.info("[%s] Beginning processing for transaction %s", self.worker_id, tx_id)

        # 1. Atomically claim lease in database
        async with async_session_maker() as session:
            claimed = await self.claim_job(session, tx_id)
            if not claimed:
                logger.info(
                    "Transaction %s already claimed by another worker or finalized. Skipping.",
                    tx_id,
                )
                return False
            await session.commit()

        # 2. Query Mock Service: GET /transactions/{id}
        get_res = await self.posting_client.get_transaction(tx_id)
        if get_res.success:
            logger.info("Transaction %s already exists in posting service. Marking completed.", tx_id)
            async with async_session_maker() as session:
                await self.tx_repo.mark_completed(session, tx_id)
                await session.commit()
            metrics_collector.record_completed()
            return True

        # 3. If missing, submit transaction: POST /transactions
        post_payload = {
            "id": tx_id,
            "amount": float(job_data["amount"]),
            "currency": job_data["currency"],
            "description": job_data["description"],
            "timestamp": job_data["timestamp"],
        }
        if "metadata" in job_data and job_data["metadata"]:
            post_payload["metadata"] = job_data["metadata"]

        post_res = await self.posting_client.post_transaction(post_payload)

        # 4. Handle POST outcome
        if post_res.success:
            logger.info("Transaction %s successfully posted to remote service.", tx_id)
            async with async_session_maker() as session:
                await self.tx_repo.mark_completed(session, tx_id)
                await session.commit()
            metrics_collector.record_completed()
            return True

        # 5. POST failed or timed out: Handle pre-write vs post-write failure
        logger.warning(
            "POST failed for %s (status=%s, timeout=%s, error=%s). Initiating verification.",
            tx_id,
            post_res.status_code,
            post_res.is_timeout,
            post_res.error,
        )

        # Check immediately if it was a post-write failure
        immediate_check = await self.posting_client.get_transaction(tx_id)
        if immediate_check.success:
            logger.info("Post-write failure confirmed for %s: transaction exists in remote store.", tx_id)
            async with async_session_maker() as session:
                await self.tx_repo.mark_completed(session, tx_id)
                await session.commit()
            metrics_collector.record_completed()
            return True

        # If still missing and error might be in-flight (timeout or 5xx), verify over delay window
        is_retryable = self.retry_service.is_retryable_error(
            post_res.status_code, post_res.is_timeout, post_res.is_network_error
        )

        if post_res.is_timeout or (post_res.status_code and post_res.status_code >= 500):
            confirmed_saved = await self.verify_remote_state(
                tx_id, max_wait_seconds=settings.VERIFICATION_WINDOW_SECONDS
            )
            if confirmed_saved:
                logger.info("Delayed post confirmed saved for %s during verification window.", tx_id)
                async with async_session_maker() as session:
                    await self.tx_repo.mark_completed(session, tx_id)
                    await session.commit()
                metrics_collector.record_completed()
                return True

        # 6. Pre-write failure confirmed: Schedule retry or mark failed
        async with async_session_maker() as session:
            tx = await self.tx_repo.get_by_id(session, tx_id)
            curr_retries = tx.retry_count if tx else 0

            if is_retryable and self.retry_service.can_retry(curr_retries):
                next_count = curr_retries + 1
                next_retry_time = self.retry_service.get_next_retry_time(next_count)
                err_msg = post_res.error or "Transient posting failure"
                logger.info(
                    "Scheduling retry #%d for transaction %s at %s",
                    next_count,
                    tx_id,
                    next_retry_time.isoformat(),
                )
                await self.tx_repo.schedule_retry(
                    session=session,
                    transaction_id=tx_id,
                    next_retry_at=next_retry_time,
                    retry_count=next_count,
                    error_message=err_msg,
                )
                await session.commit()
                metrics_collector.record_retry()
            else:
                final_err = post_res.error or "Exceeded maximum retry attempts"
                logger.error("Terminal failure for transaction %s: %s", tx_id, final_err)
                await self.tx_repo.mark_failed(session, tx_id, final_err)
                await session.commit()
                metrics_collector.record_failed()

        return False

    async def run_loop(self) -> None:
        """Main worker dequeue loop."""
        self._is_running = True
        logger.info("Transaction worker %s started.", self.worker_id)

        while self._is_running:
            try:
                job_data = await self.queue.dequeue(timeout_seconds=settings.WORKER_POLL_TIMEOUT_SECONDS)
                if job_data:
                    await self.process_job(job_data)
            except asyncio.CancelledError:
                logger.info("Worker %s received cancellation signal.", self.worker_id)
                break
            except Exception as exc:
                logger.error("Worker %s encountered unhandled error: %s", self.worker_id, exc, exc_info=True)
                await asyncio.sleep(1.0)

    def stop(self) -> None:
        """Signal worker to stop processing."""
        self._is_running = False
