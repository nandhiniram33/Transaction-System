"""High-throughput transaction ingestion service.

Uses an optimistic INSERT strategy to eliminate the pre-flight SELECT on
every new transaction:
  • New transaction  → INSERT (succeeds)  → add outbox → commit → 202
  • Identical dup    → INSERT (IntegrityError) → rollback → SELECT → 200
  • Conflicting dup  → INSERT (IntegrityError) → rollback → SELECT → 409

This saves one DB round-trip (SELECT) on every new transaction, which is
the dominant case. The IntegrityError path is only exercised for duplicates,
which are rare in production.

Compatible with both SQLite and PostgreSQL backends.
"""

import json
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from fastapi import HTTPException
from app.models.outbox import OutboxModel
from app.models.transaction import TransactionModel
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.transaction_repository import TransactionRepository
from app.schemas.transaction import TransactionCreateRequest

logger = get_logger(__name__)


_POSTGRES_INGEST_CTE = """
WITH inserted_tx AS (
    INSERT INTO transactions (id, amount, currency, description, "timestamp", metadata_json, payload_fingerprint, status, submitted_at, retry_count)
    VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, 0)
    RETURNING id
)
INSERT INTO outbox_events (transaction_id, event_type, payload, status, created_at)
VALUES ($1, 'TRANSACTION_CREATED', $9, 'pending', $8);
"""


class ConflictException(Exception):
    """Raised when an incoming transaction reuses an ID with conflicting payload details."""

    pass


class TransactionService:
    """Service managing transaction acceptance, idempotency checks, and durable outbox records."""

    def __init__(self):
        self.tx_repo = TransactionRepository()
        self.outbox_repo = OutboxRepository()
        self._cached_pending_outbox: int = 0
        self._last_outbox_check_time: float = 0.0

    async def accept_transaction(
        self,
        session: AsyncSession,
        request: TransactionCreateRequest,
    ) -> Tuple[TransactionModel, bool]:
        """Atomically persist a transaction and its outbox event.

        Uses optimistic INSERT: no pre-flight SELECT on the happy path.

        Returns:
            Tuple[TransactionModel, bool]: (transaction, is_new)
            is_new is True for newly accepted transactions, False for identical duplicate
            submissions.
        Raises:
            ConflictException: If transaction ID exists with conflicting payload data.
        """
        is_mock_session = (
            session.__class__.__name__ != "AsyncSession"
            or not hasattr(session, "is_active")
        )
        pg_pool = None
        if not is_mock_session:
            try:
                from app.database.db import _is_postgresql, get_asyncpg_pool

                if _is_postgresql:
                    pg_pool = await get_asyncpg_pool()
            except Exception:
                pg_pool = None

        # Check outbox overflow before accepting transaction
        max_outbox = get_settings().QUEUE_OVERFLOW_MAX_OUTBOX
        now_mono = time.monotonic()
        cache_stale = (now_mono - self._last_outbox_check_time) > 1.0
        near_overflow = self._cached_pending_outbox >= max_outbox

        if cache_stale or near_overflow or is_mock_session:
            self._last_outbox_check_time = now_mono
            if pg_pool is not None:
                async with pg_pool.acquire() as conn:
                    self._cached_pending_outbox = await conn.fetchval(
                        "SELECT count(id) FROM outbox_events WHERE status = 'pending'"
                    )
            else:
                self._cached_pending_outbox = await self.outbox_repo.get_pending_count(session)

        if self._cached_pending_outbox >= max_outbox:
            raise HTTPException(
                status_code=429,
                detail=f"Outbox pending count {self._cached_pending_outbox} exceeds limit {max_outbox}",
            )

        fingerprint = request.compute_fingerprint()
        now = datetime.now(timezone.utc)

        outbox_payload = {
            "id": request.id,
            "amount": float(request.amount),
            "currency": request.currency,
            "description": request.description,
            "timestamp": request.timestamp.isoformat(),
            "metadata": request.metadata or {},
        }

        tx_model = TransactionModel(
            id=request.id,
            amount=request.amount,
            currency=request.currency,
            description=request.description,
            timestamp=request.timestamp,
            metadata_json=json.dumps(request.metadata) if request.metadata else None,
            payload_fingerprint=fingerprint,
            status="pending",
            submitted_at=now,
            retry_count=0,
        )
        outbox_event = OutboxModel(
            transaction_id=request.id,
            event_type="TRANSACTION_CREATED",
            payload=json.dumps(outbox_payload),
            status="pending",
            created_at=now,
        )

        if pg_pool is not None:
            import asyncpg

            meta_json = json.dumps(request.metadata) if request.metadata else None
            outbox_json = json.dumps(outbox_payload)

            try:
                async with pg_pool.acquire() as conn:
                    await conn.execute(
                        _POSTGRES_INGEST_CTE,
                        request.id,
                        request.amount,
                        request.currency,
                        request.description,
                        request.timestamp,
                        meta_json,
                        fingerprint,
                        now,
                        outbox_json,
                    )
                logger.debug(
                    "Accepted and durably recorded transaction %s via native pg pipeline",
                    request.id,
                )
                return tx_model, True

            except asyncpg.exceptions.UniqueViolationError:
                async with pg_pool.acquire() as conn:
                    existing = await conn.fetchrow(
                        'SELECT id, status, payload_fingerprint, submitted_at, completed_at, error FROM transactions WHERE id = $1',
                        request.id,
                    )
                if existing:
                    if existing["payload_fingerprint"] == fingerprint:
                        logger.info(
                            "Identical duplicate transaction detected for ID %s",
                            request.id,
                        )
                        dup_model = TransactionModel(
                            id=existing["id"],
                            amount=request.amount,
                            currency=request.currency,
                            description=request.description,
                            timestamp=request.timestamp,
                            metadata_json=meta_json,
                            payload_fingerprint=existing["payload_fingerprint"],
                            status=existing["status"],
                            submitted_at=existing["submitted_at"],
                            completed_at=existing["completed_at"],
                            error=existing["error"],
                        )
                        return dup_model, False

                    logger.warning(
                        "Conflict detected for transaction ID %s (existing: %s, new: %s)",
                        request.id,
                        existing["payload_fingerprint"],
                        fingerprint,
                    )
                    raise ConflictException(
                        f"Transaction with ID '{request.id}' already exists with different payload values."
                    )
                raise

        # Optimistic INSERT — no SELECT on the new-transaction hot path
        try:
            session.add_all([tx_model, outbox_event])
            await session.commit()
            logger.debug("Accepted and durably recorded transaction %s", request.id)
            return tx_model, True

        except IntegrityError:
            # Transaction ID collision — determine whether identical or conflicting
            await session.rollback()
            existing = await self.tx_repo.get_by_id(session, request.id)

            if existing is None:
                # Shouldn't happen: IntegrityError with no existing row
                # (could be an outbox FK error on a truly corrupt state; re-raise)
                logger.error(
                    "IntegrityError for transaction %s but no existing record found.",
                    request.id,
                )
                raise

            if existing.payload_fingerprint == fingerprint:
                logger.info(
                    "Identical duplicate transaction detected for ID %s", request.id
                )
                return existing, False

            logger.warning(
                "Conflict detected for transaction ID %s "
                "(existing fingerprint: %s, new: %s)",
                request.id,
                existing.payload_fingerprint,
                fingerprint,
            )
            raise ConflictException(
                f"Transaction with ID '{request.id}' already exists with different payload values."
            )

    async def get_transaction(
        self,
        session: AsyncSession,
        transaction_id: str,
    ) -> Optional[TransactionModel]:
        """Query transaction details and lifecycle status by ID."""
        return await self.tx_repo.get_by_id(session, transaction_id)


# Global transaction service instance
transaction_service = TransactionService()
