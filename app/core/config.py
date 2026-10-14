"""Application configuration and environment settings."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central application configuration using Pydantic Settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # General Application
    APP_NAME: str = "High Performance Transaction Processing System"
    APP_VERSION: str = "1.0.0"
    APP_ENV: str = "development"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    # Database Settings
    DATABASE_URL: str = "sqlite+aiosqlite:///./data/transactions.db"
    SYNC_DATABASE_URL: str = "sqlite:///./data/transactions.db"
    SQL_ECHO: bool = False

    # Redis Queue Settings
    REDIS_URL: str = "fakeredis://"
    REDIS_QUEUE_NAME: str = "transaction_processing_queue"
    REDIS_CONNECT_TIMEOUT_SECONDS: float = 2.0

    # Mock Posting Service Settings
    MOCK_POSTING_SERVICE_URL: str = "http://localhost:8080"
    POSTING_REQUEST_TIMEOUT_SECONDS: float = 15.0

    # Retry Policies
    MAX_RETRY_ATTEMPTS: int = 5
    INITIAL_BACKOFF_SECONDS: float = 1.0
    MAX_BACKOFF_SECONDS: float = 30.0
    BACKOFF_MULTIPLIER: float = 2.0
    JITTER_ENABLED: bool = True

    # Outbox & Dispatcher Settings
    OUTBOX_BATCH_SIZE: int = 100
    OUTBOX_POLL_INTERVAL_SECONDS: float = 0.1

    # Worker & Reconciliation Settings
    WORKER_CONCURRENCY: int = 5
    WORKER_POLL_TIMEOUT_SECONDS: float = 1.0
    PROCESSING_LEASE_TIMEOUT_SECONDS: float = 30.0
    RECONCILIATION_INTERVAL_SECONDS: float = 5.0
    VERIFICATION_WINDOW_SECONDS: float = 12.0


@lru_cache()
def get_settings() -> Settings:
    """Cached singleton accessor for application settings."""
    return Settings()
