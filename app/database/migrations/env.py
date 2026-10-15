"""Alembic environment configuration for database migrations.

Supports both SQLite and PostgreSQL backends. The SYNC_DATABASE_URL
setting selects the synchronous driver used by Alembic.
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.core.config import get_settings
from app.models.base import Base

# Import models to ensure they are registered on Base.metadata
from app.models.outbox import OutboxModel  # noqa: F401
from app.models.transaction import TransactionModel  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

settings = get_settings()

# Resolve the synchronous migration URL from SYNC_DATABASE_URL.
# Handle the case where the user may set DATABASE_URL but forget SYNC_DATABASE_URL.
sync_url = settings.SYNC_DATABASE_URL

# Strip async driver prefixes if accidentally passed
if "aiosqlite" in sync_url:
    sync_url = sync_url.replace("sqlite+aiosqlite", "sqlite")
if "asyncpg" in sync_url:
    sync_url = sync_url.replace("postgresql+asyncpg", "postgresql+psycopg2")

config.set_main_option("sqlalchemy.url", sync_url)

target_metadata = Base.metadata

# Determine whether batch mode is needed (required for SQLite ALTER TABLE)
_use_batch_mode = "sqlite" in sync_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (generate SQL without a live connection)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=_use_batch_mode,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode against a live database connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=_use_batch_mode,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
