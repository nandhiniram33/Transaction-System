"""Pydantic schemas and validation for transaction ingestion and queries."""

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator


class TransactionStatus(str, Enum):
    """Transaction lifecycle statuses as specified in the PDF."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


# ISO 4217 Currency Code pattern (3 uppercase alphabetic characters)
CURRENCY_REGEX = re.compile(r"^[A-Z]{3}$")


class TransactionCreateRequest(BaseModel):
    """Incoming transaction submission payload."""

    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    id: str = Field(
        ...,
        description="Unique UUID identifying the transaction",
        examples=["123e4567-e89b-12d3-a456-426614174000"],
    )
    amount: Decimal = Field(
        ...,
        gt=0,
        description="Positive transaction amount",
        examples=[100.50],
    )
    currency: str = Field(
        ...,
        description="3-letter ISO 4217 currency code",
        examples=["USD"],
    )
    description: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Transaction description",
        examples=["Test transaction"],
    )
    timestamp: datetime = Field(
        ...,
        description="ISO 8601 transaction timestamp",
        examples=["2025-01-15T10:30:00Z"],
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional metadata key-value object",
    )

    @field_validator("id")
    @classmethod
    def validate_uuid(cls, value: str) -> str:
        try:
            parsed = uuid.UUID(str(value))
            return str(parsed)
        except (ValueError, AttributeError, TypeError):
            raise ValueError("Transaction id must be a valid UUID string")

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, value: Decimal) -> Decimal:
        if value <= Decimal("0"):
            raise ValueError("Amount must be strictly positive")
        return value

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, value: str) -> str:
        upper = value.upper()
        if not CURRENCY_REGEX.match(upper):
            raise ValueError(
                "Currency must be a valid 3-letter ISO 4217 code (e.g. USD, EUR)"
            )
        return upper

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            # Assume UTC if naive
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def compute_fingerprint(self) -> str:
        """Compute deterministic SHA-256 hash of payload fields to detect conflicts."""
        canonical_dict = {
            "amount": str(self.amount),
            "currency": self.currency,
            "description": self.description,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata or {},
        }
        canonical_str = json.dumps(canonical_dict, sort_keys=True)
        return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()


class TransactionResponse(BaseModel):
    """Transaction query response schema as specified in PDF page 3-4."""

    model_config = ConfigDict(
        populate_by_name=True,
    )

    transaction_id: str = Field(alias="transactionId", description="Transaction UUID")
    status: TransactionStatus = Field(description="Transaction processing status")
    submitted_at: datetime = Field(
        alias="submittedAt", description="Submission timestamp"
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        alias="completedAt",
        description="Completion timestamp when available",
    )
    error: Optional[str] = Field(default=None, description="Error message if failed")


class HealthResponse(BaseModel):
    """System health and monitoring response schema."""

    status: str
    api: str
    database: str
    redis: str
    queue: Dict[str, Any]
    worker: Dict[str, Any]
    metrics: Dict[str, Any]
