"""FastAPI main application entrypoint with lifespan event handling."""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.routes.health import router as health_router
from app.api.routes.transactions import router as transactions_router
from app.core.config import get_settings
from app.core.logging import get_logger, setup_logging
from app.database.db import close_db, init_db
from app.queue.dispatcher import outbox_dispatcher
from app.queue.queue_client import queue_client
from app.services.posting_service_client import posting_client
from app.workers.reconciliation_worker import reconciliation_worker
from app.workers.transaction_worker import TransactionWorker

logger = get_logger(__name__)
settings = get_settings()

_background_tasks = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management for database, queue, and background workers."""
    setup_logging(settings.LOG_LEVEL)
    logger.info("Starting up %s (version: %s)", settings.APP_NAME, settings.APP_VERSION)

    # 1. Initialize Database
    await init_db()

    # 2. Connect Redis Queue
    await queue_client.connect()

    # 3. Start background outbox dispatcher
    dispatcher_task = asyncio.create_task(outbox_dispatcher.run_loop())
    _background_tasks.append(dispatcher_task)

    # 4. Start background reconciliation worker
    reconciler_task = asyncio.create_task(reconciliation_worker.run_loop())
    _background_tasks.append(reconciler_task)

    # 5. Start embedded transaction workers for local execution and seamless testing
    workers = []
    for i in range(settings.WORKER_CONCURRENCY):
        w = TransactionWorker(worker_id=f"api-worker-{i+1}")
        w_task = asyncio.create_task(w.run_loop())
        workers.append(w)
        _background_tasks.append(w_task)

    logger.info("All background tasks and workers initialized successfully.")

    yield

    # Shutdown sequence
    logger.info("Initiating graceful shutdown...")
    outbox_dispatcher.stop()
    reconciliation_worker.stop()
    for w in workers:
        w.stop()

    for task in _background_tasks:
        task.cancel()

    await asyncio.gather(*_background_tasks, return_exceptions=True)

    await posting_client.close()
    await queue_client.close()
    await close_db()
    logger.info("Application shutdown complete.")


app = FastAPI(
    title="High Performance Transaction Processing System",
    description=(
        "Production-grade financial transaction processing intermediary "
        "providing durable acceptance, zero-data-loss outbox, duplicate prevention, "
        "and resilient asynchronous delivery to unreliable remote posting services."
    ),
    version=settings.APP_VERSION,
    lifespan=lifespan,
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Format Pydantic validation errors cleanly."""
    errors = exc.errors()
    error_messages = [f"{e.get('loc', ['field'])[-1]}: {e.get('msg')}" for e in errors]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "detail": "Validation error in request payload",
            "errors": error_messages,
        },
    )


@app.get("/", include_in_schema=False)
async def root():
    """Service root metadata."""
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "online",
        "docs_url": "/docs",
        "health_url": "/api/health",
    }


# Include Routers
app.include_router(transactions_router)
app.include_router(health_router)
