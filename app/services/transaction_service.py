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
from datetime import datetime, timezone
from typing import Optional, Tuple

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from fastapi import HTTPException
from app.models.outbox import OutboxModel
from app.models.transaction import TransactionModel
from app.repositories.outbox_repository import OutboxRepository
from app.repositories.transaction_repository import TransactionRepository
from app.schemas.transaction import TransactionCreateRequest

logger = get_logger(__name__)


class ConflictException(Exception):
    """Raised when an incoming transaction reuses an ID with conflicting payload details."""

    pass


class TransactionService:
    """Service managing transaction acceptance, idempotency checks, and durable outbox records."""

    def __init__(self):
        self.tx_repo = TransactionRepository()
        self.outbox_repo = OutboxRepository()

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
        # Check outbox overflow before accepting transaction
        from app.core.config import get_settings
        from app.repositories.outbox_repository import OutboxRepository

        outbox_repo = OutboxRepository()
        pending = await outbox_repo.get_pending_count(session)
        max_outbox = get_settings().QUEUE_OVERFLOW_MAX_OUTBOX
        if pending >= max_outbox:
            raise HTTPException(
                status_code=429,
                detail=f"Outbox pending count {pending} exceeds limit {max_outbox}",
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

        # Optimistic INSERT — no SELECT on the new-transaction hot path
        try:
            session.add(tx_model)
            session.add(outbox_event)
            await session.commit()
            logger.info("Accepted and durably recorded transaction %s", request.id)
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
