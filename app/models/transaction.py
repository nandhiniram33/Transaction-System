"""Transaction entity model for relational persistence."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from sqlalchemy import DateTime, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class TransactionModel(Base):
    """Represents a transaction record with idempotency, status, and lease tracking."""

    __tablename__ = "transactions"

    # Primary Identifier (Client-supplied UUID)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, index=True)

    # Core Financial Data
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Idempotency & Conflict Detection
    payload_fingerprint: Mapped[str] = mapped_column(
        String(64), nullable=False, index=True
    )

    # Lifecycle State
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True
    )
    submitted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Worker Lease & Concurrency Control
    worker_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )

    # Retry Tracking & Scheduling
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_transactions_status_lease", "status", "lease_expires_at"),
        Index("ix_transactions_status_retry", "status", "next_retry_at"),
    )
