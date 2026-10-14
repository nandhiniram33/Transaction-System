"""Database engine and session management with SQLite WAL optimizations."""

from pathlib import Path
from typing import AsyncGenerator
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from app.core.config import get_settings
from app.core.logging import get_logger
from app.models.base import Base

# Import all models so Base.metadata is populated
from app.models.transaction import TransactionModel  # noqa: F401
from app.models.outbox import OutboxModel  # noqa: F401

logger = get_logger(__name__)

settings = get_settings()

# Ensure parent directory for SQLite file exists
if "sqlite" in settings.DATABASE_URL:
    db_path_str = settings.DATABASE_URL.split("///")[-1]
    db_file_path = Path(db_path_str)
    if db_file_path.parent:
        db_file_path.parent.mkdir(parents=True, exist_ok=True)


# Configure Async SQLite Engine with optimized settings
engine: AsyncEngine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.SQL_ECHO,
    connect_args={"check_same_thread": False, "timeout": 30.0},
)


@event.listens_for(engine.sync_engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Apply high-performance SQLite PRAGMAs on every connection."""
    cursor = dbapi_connection.cursor()
    # Write-Ahead Logging allows concurrent readers and single writer without blocking
    cursor.execute("PRAGMA journal_mode=WAL;")
    # Synchronous NORMAL ensures durability across application crashes while boosting IOPS
    cursor.execute("PRAGMA synchronous=NORMAL;")
    # Busy timeout ensures transactions wait up to 10s for write locks instead of failing
    cursor.execute("PRAGMA busy_timeout=10000;")
    # Cache size in KiB (-64000 = 64MB)
    cursor.execute("PRAGMA cache_size=-64000;")
    cursor.execute("PRAGMA foreign_keys=ON;")
    cursor.close()


async_session_maker = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for yielding transactional async database sessions."""
    async with async_session_maker() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """Create database tables if they do not exist."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database schema initialized successfully.")


async def close_db() -> None:
    """Dispose of engine connection pool on shutdown."""
    await engine.dispose()
    logger.info("Database connections closed.")
