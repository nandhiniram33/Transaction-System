"""Database engine and session management with dual SQLite/PostgreSQL support.

SQLite (default):   WAL-mode with optimized PRAGMAs.
PostgreSQL (recommended for 1,000+ TPS): asyncpg with connection pool.
Select via DATABASE_URL environment variable.
"""

from pathlib import Path
from typing import AsyncGenerator

from sqlalchemy import event, text
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
from app.models.outbox import OutboxModel  # noqa: F401
from app.models.transaction import TransactionModel  # noqa: F401

logger = get_logger(__name__)
settings = get_settings()

_is_postgresql: bool = settings.DATABASE_URL.startswith("postgresql")
_is_sqlite: bool = "sqlite" in settings.DATABASE_URL

_asyncpg_pool = None


async def get_asyncpg_pool():
    """Returns or initialises a native asyncpg pool for PostgreSQL high-throughput writes."""
    global _asyncpg_pool
    if _asyncpg_pool is None and _is_postgresql:
        try:
            import asyncpg

            clean_url = settings.DATABASE_URL.replace(
                "postgresql+asyncpg://", "postgresql://"
            )
            _asyncpg_pool = await asyncpg.create_pool(
                clean_url,
                min_size=settings.DB_POOL_SIZE,
                max_size=settings.DB_POOL_SIZE + settings.DB_MAX_OVERFLOW,
                command_timeout=10.0,
                server_settings={"timezone": "UTC"},
            )
            logger.info("Native asyncpg connection pool established for PostgreSQL")
        except Exception as exc:
            logger.warning("Could not establish native asyncpg pool: %s", exc)
            _asyncpg_pool = None
    return _asyncpg_pool


def _build_engine() -> AsyncEngine:
    """Create the async SQLAlchemy engine for the configured database backend."""

    if _is_postgresql:
        # PostgreSQL via asyncpg — true concurrent writes, connection pool
        logger.info(
            "Initializing PostgreSQL engine (pool_size=%d, max_overflow=%d)",
            settings.DB_POOL_SIZE,
            settings.DB_MAX_OVERFLOW,
        )
        return create_async_engine(
            settings.DATABASE_URL,
            echo=settings.SQL_ECHO,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            pool_pre_ping=False,
            pool_recycle=settings.DB_POOL_RECYCLE_SECONDS,
            # asyncpg connection init: set timezone
            connect_args={"server_settings": {"timezone": "UTC"}},
        )

    # SQLite: WAL-mode, single-file, optimised for local throughput
    if _is_sqlite:
        db_path_str = settings.DATABASE_URL.split("///")[-1]
        db_file_path = Path(db_path_str)
        if db_file_path.parent:
            db_file_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Initializing SQLite WAL engine")
    return create_async_engine(
        settings.DATABASE_URL,
        echo=settings.SQL_ECHO,
        # aiosqlite: one connection per coroutine is fine; NullPool avoids
        # multi-threaded handoff overhead on Windows
        connect_args={"check_same_thread": False, "timeout": 30.0},
    )


engine: AsyncEngine = _build_engine()


if _is_sqlite:

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        """Apply high-performance SQLite PRAGMAs on every new connection."""
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        # NORMAL: fsync only at WAL checkpoints — fast + crash-safe
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute("PRAGMA busy_timeout=30000;")
        # 256 MB page cache in memory
        cursor.execute("PRAGMA cache_size=-65536;")
        # Memory-mapped I/O for the WAL file (512 MB)
        cursor.execute("PRAGMA mmap_size=536870912;")
        cursor.execute("PRAGMA temp_store=MEMORY;")
        # Checkpoint only every 2000 pages to reduce I/O during heavy ingestion
        cursor.execute("PRAGMA wal_autocheckpoint=2000;")
        cursor.execute("PRAGMA foreign_keys=ON;")
        # Optimise query planner after each connection warmup
        cursor.execute("PRAGMA optimize;")
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
        # For PostgreSQL: ensure FK enforcement (it's always on by default)
        if _is_postgresql:
            await conn.execute(text("SELECT 1"))
    if _is_postgresql:
        await get_asyncpg_pool()
    logger.info(
        "Database schema initialised (%s).",
        "PostgreSQL" if _is_postgresql else "SQLite",
    )


async def close_db() -> None:
    """Dispose of engine connection pool on shutdown."""
    global _asyncpg_pool
    if _asyncpg_pool is not None:
        try:
            await _asyncpg_pool.close()
        except Exception:
            pass
        _asyncpg_pool = None
    await engine.dispose()
    logger.info("Database connections closed.")
