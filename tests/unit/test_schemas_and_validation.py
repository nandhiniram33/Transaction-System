"""Unit tests for transaction payload schemas, validation, and fingerprinting."""

from datetime import datetime, timezone
from decimal import Decimal
import uuid
import pytest
from pydantic import ValidationError

from app.schemas.transaction import TransactionCreateRequest


def test_valid_transaction_creation():
    """Verify standard valid payload parses correctly with Decimal precision."""
    tx_id = str(uuid.uuid4())
    payload = {
        "id": tx_id,
        "amount": "100.5050",
        "currency": "USD",
        "description": "Payment for services",
        "timestamp": "2026-10-12T10:00:00Z",
        "metadata": {"dept": "finance"},
    }
    req = TransactionCreateRequest(**payload)
    assert req.id == tx_id
    assert req.amount == Decimal("100.5050")
    assert req.currency == "USD"
    assert req.description == "Payment for services"
    assert req.timestamp.tzinfo == timezone.utc
    assert req.metadata == {"dept": "finance"}


def test_reject_negative_or_zero_amount():
    """Verify strictly positive amounts are enforced."""
    base = {
        "id": str(uuid.uuid4()),
        "currency": "USD",
        "description": "Test",
        "timestamp": "2026-10-12T10:00:00Z",
    }
    with pytest.raises(ValidationError):
        TransactionCreateRequest(amount=Decimal("0.00"), **base)

    with pytest.raises(ValidationError):
        TransactionCreateRequest(amount=Decimal("-50.00"), **base)


def test_currency_validation():
    """Verify 3-letter uppercase ISO 4217 validation."""
    base = {
        "id": str(uuid.uuid4()),
        "amount": Decimal("10.00"),
        "description": "Test",
        "timestamp": "2026-10-12T10:00:00Z",
    }
    # Lowercase gets normalized to uppercase
    req = TransactionCreateRequest(currency="usd", **base)
    assert req.currency == "USD"

    # Non-3-letter codes fail
    with pytest.raises(ValidationError):
        TransactionCreateRequest(currency="US", **base)

    with pytest.raises(ValidationError):
        TransactionCreateRequest(currency="USDT", **base)

    with pytest.raises(ValidationError):
        TransactionCreateRequest(currency="123", **base)


def test_reject_malformed_uuid():
    """Verify invalid UUID strings are rejected."""
    base = {
        "amount": Decimal("10.00"),
        "currency": "USD",
        "description": "Test",
        "timestamp": "2026-10-12T10:00:00Z",
    }
    with pytest.raises(ValidationError):
        TransactionCreateRequest(id="not-a-uuid", **base)

    with pytest.raises(ValidationError):
        TransactionCreateRequest(id="12345", **base)


def test_timestamp_timezone_conversion():
    """Verify naive timestamp is converted to UTC."""
    base = {
        "id": str(uuid.uuid4()),
        "amount": Decimal("25.00"),
        "currency": "EUR",
        "description": "Test",
    }
    req = TransactionCreateRequest(timestamp=datetime(2026, 10, 12, 10, 30, 0), **base)
    assert req.timestamp.tzinfo == timezone.utc


def test_fingerprint_deterministic_and_conflict_detection():
    """Verify payload fingerprint is stable for identical fields and different for altered fields."""
    tx_id = str(uuid.uuid4())
    req1 = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("100.00"),
        currency="USD",
        description="Invoice 1",
        timestamp=datetime(2026, 10, 12, 10, 0, 0, tzinfo=timezone.utc),
        metadata={"order_id": 99},
    )
    req2 = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("100.00"),
        currency="USD",
        description="Invoice 1",
        timestamp=datetime(2026, 10, 12, 10, 0, 0, tzinfo=timezone.utc),
        metadata={"order_id": 99},
    )
    # Different amount
    req3 = TransactionCreateRequest(
        id=tx_id,
        amount=Decimal("150.00"),
        currency="USD",
        description="Invoice 1",
        timestamp=datetime(2026, 10, 12, 10, 0, 0, tzinfo=timezone.utc),
        metadata={"order_id": 99},
    )

    assert req1.compute_fingerprint() == req2.compute_fingerprint()
    assert req1.compute_fingerprint() != req3.compute_fingerprint()
