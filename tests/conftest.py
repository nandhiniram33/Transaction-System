"""Pytest test configuration, fixtures, and database lifecycle management."""

import asyncio
import os
import uuid
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

# Ensure test environment variables
os.environ["APP_ENV"] = "testing"
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./data/test_transactions.db"
os.environ["SYNC_DATABASE_URL"] = "sqlite:///./data/test_transactions.db"
os.environ["REDIS_URL"] = "fakeredis://"

from app.database.db import Base, engine, async_session_maker
from app.main import app
from app.queue.queue_client import queue_client
from app.services.posting_service_client import posting_client
import tests.mock_posting_server as mock_server


@pytest.fixture(scope="session")
def event_loop():
    """Create session-scoped event loop for pytest-asyncio."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function", autouse=True)
async def reset_database():
    """Drop and recreate all tables before each test function for strict test isolation."""
    # Clear mock server database
    mock_server.mock_db.clear()
    mock_server.FAILURE_MODE = None
    mock_server.SIMULATE_DELAY = False

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    # Clear fake redis queue
    if queue_client._redis is not None:
        await queue_client._redis.flushall()

    yield

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def db_session() -> AsyncSession:
    """Provide a fresh database session for a test."""
    async with async_session_maker() as session:
        yield session


@pytest_asyncio.fixture
async def async_client() -> AsyncClient:
    """Async test HTTP client invoking the FastAPI application."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def sample_transaction_payload() -> dict:
    """Generate a valid transaction request payload."""
    return {
        "id": str(uuid.uuid4()),
        "amount": 150.75,
        "currency": "USD",
        "description": "Standard test transaction",
        "timestamp": "2026-10-12T12:00:00Z",
        "metadata": {"source": "unit_test", "customer_id": "cust_123"},
    }


@pytest.fixture
def mock_client_app():
    """Create PostingServiceClient wired directly to mock_server ASGI application."""
    transport = ASGITransport(app=mock_server.app)
    client = posting_client.__class__(base_url="http://test")
    client._client = AsyncClient(transport=transport, base_url="http://test")
    return client
