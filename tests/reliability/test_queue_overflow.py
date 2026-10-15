import pytest
import uuid
from fastapi import HTTPException
from app.services.transaction_service import transaction_service
from app.schemas.transaction import TransactionCreateRequest
from app.core.config import get_settings


@pytest.mark.asyncio
async def test_queue_overflow_guard(monkeypatch):
    # Mock OutboxRepository to simulate overflow
    async def mock_get_pending_count(self, session, limit: int = 100):
        return get_settings().QUEUE_OVERFLOW_MAX_OUTBOX + 1

    from app.repositories.outbox_repository import OutboxRepository

    monkeypatch.setattr(OutboxRepository, "get_pending_count", mock_get_pending_count)

    request = TransactionCreateRequest(
        id=str(uuid.uuid4()),
        amount=10.0,
        currency="USD",
        description="Overflow test",
        timestamp=__import__("datetime").datetime.now(),
        metadata=None,
    )

    # Use a dummy async session (the transaction_service does not hit DB before overflow check)
    class DummySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def commit(self):
            pass

        async def rollback(self):
            pass

        async def add(self, obj):
            pass

    async with DummySession() as session:
        with pytest.raises(HTTPException) as exc_info:
            await transaction_service.accept_transaction(session, request)
        assert exc_info.value.status_code == 429
        assert "Outbox pending count" in exc_info.value.detail
