"""High-throughput transaction ingestion service implementing transactional outbox and idempotency."""

import json
from datetime import datetime, timezone
from typing import Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
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
        
        Returns:
            Tuple[TransactionModel, bool]: (transaction, is_new)
            is_new is True for newly accepted transactions, False for identical duplicate submissions.
        Raises:
            ConflictException: If transaction ID exists with conflicting payload data.
        """
        fingerprint = request.compute_fingerprint()

        # Check for existing transaction with same ID
        existing = await self.tx_repo.get_by_id(session, request.id)
        if existing:
            if existing.payload_fingerprint == fingerprint:
                logger.info("Identical duplicate transaction detected for ID %s", request.id)
                return existing, False
            else:
                logger.warning(
                    "Conflict detected for transaction ID %s (existing fingerprint: %s, new: %s)",
                    request.id,
                    existing.payload_fingerprint,
                    fingerprint,
                )
                raise ConflictException(
                    f"Transaction with ID '{request.id}' already exists with different payload values."
                )

        now = datetime.now(timezone.utc)
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

        # Prepare outbox event for atomic commit
        outbox_payload = {
            "id": request.id,
            "amount": float(request.amount),
            "currency": request.currency,
            "description": request.description,
            "timestamp": request.timestamp.isoformat(),
            "metadata": request.metadata or {},
        }
        outbox_event = OutboxModel(
            transaction_id=request.id,
            event_type="TRANSACTION_CREATED",
            payload=json.dumps(outbox_payload),
            status="pending",
            created_at=now,
        )

        # Save both atomically in SQLite
        await self.tx_repo.create(session, tx_model)
        await self.outbox_repo.create(session, outbox_event)
        await session.commit()

        logger.info("Accepted and durably recorded transaction %s", request.id)
        return tx_model, True

    async def get_transaction(
        self,
        session: AsyncSession,
        transaction_id: str,
    ) -> Optional[TransactionModel]:
        """Query transaction details and lifecycle status by ID."""
        return await self.tx_repo.get_by_id(session, transaction_id)


# Global transaction service instance
transaction_service = TransactionService()
