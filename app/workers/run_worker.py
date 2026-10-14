"""Standalone background worker entrypoint for multi-process or containerized deployment."""

import asyncio
import signal
import sys
from app.core.config import get_settings
from app.core.logging import get_logger, setup_logging
from app.database.db import close_db, init_db
from app.queue.queue_client import queue_client
from app.services.posting_service_client import posting_client
from app.workers.reconciliation_worker import reconciliation_worker
from app.workers.transaction_worker import TransactionWorker

logger = get_logger("worker_process")
settings = get_settings()


async def main():
    setup_logging(settings.LOG_LEVEL)
    logger.info("Initializing standalone worker process...")

    await init_db()
    await queue_client.connect()

    stop_event = asyncio.Event()

    def handle_exit(*args):
        logger.info("Received termination signal.")
        stop_event.set()

    # Handle OS signals where available
    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, handle_exit)

    workers = []
    tasks = []
    for i in range(settings.WORKER_CONCURRENCY):
        w = TransactionWorker(worker_id=f"standalone-worker-{i+1}")
        workers.append(w)
        tasks.append(asyncio.create_task(w.run_loop()))

    # Also run reconciliation in the background
    tasks.append(asyncio.create_task(reconciliation_worker.run_loop()))

    logger.info(
        "Worker pool active with %d workers. Awaiting jobs...",
        settings.WORKER_CONCURRENCY,
    )

    try:
        if sys.platform == "win32":
            # On Windows, keep running until cancelled
            await asyncio.gather(*tasks)
        else:
            await stop_event.wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Worker process shutting down...")
    finally:
        for w in workers:
            w.stop()
        reconciliation_worker.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await posting_client.close()
        await queue_client.close()
        await close_db()
        logger.info("Worker process stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
