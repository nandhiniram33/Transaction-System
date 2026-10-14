"""Mock Posting Service matching vinhopenfabric/mock-posting-service specification."""

import asyncio
import random
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

app = FastAPI(title="Mock Posting Service", version="1.0.0")

# In-memory database reproducing the remote posting service behavior
# Note: uses a list to demonstrate that duplicate POSTs create duplicate records!
mock_db: List[Dict[str, Any]] = []

# Configurable simulation flags for deterministic testing
SIMULATE_DELAY: bool = False
MIN_DELAY_SECONDS: float = 0.05
MAX_DELAY_SECONDS: float = 0.2
FAILURE_MODE: Optional[str] = (
    None  # None, "random_5_percent", "pre_write_failure", "post_write_failure", "timeout"
)


class RemoteTransactionPayload(BaseModel):
    id: str
    amount: float
    currency: str
    description: str
    timestamp: str
    metadata: Optional[Dict[str, Any]] = None


@app.get("/")
async def root():
    """Root info page for the mock posting service."""
    return {
        "service": "Mock Posting Service",
        "status": "online",
        "docs_url": "/docs",
        "health_url": "/health",
        "transactions_url": "/transactions",
        "records_count": len(mock_db),
    }


@app.post("/transactions", status_code=status.HTTP_201_CREATED)
async def post_transaction(payload: RemoteTransactionPayload):
    """Submit a transaction with simulated delay and failure modes."""
    global FAILURE_MODE, SIMULATE_DELAY

    # 1. Simulate Latency (2-10 seconds in production mock, configurable in tests)
    if SIMULATE_DELAY:
        delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
        await asyncio.sleep(delay)

    # 2. Check for Pre-Write Failure
    if FAILURE_MODE == "pre_write_failure":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Simulated Pre-Write Failure: Database write aborted",
        )

    if FAILURE_MODE == "random_5_percent" and random.random() < 0.05:
        # 5% random failure: 50% pre-write, 50% post-write
        if random.random() < 0.5:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Random 5% Pre-Write Failure",
            )
        else:
            # Post-write failure: save first, then fail!
            mock_db.append(payload.model_dump())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Random 5% Post-Write Failure",
            )

    # 3. Save to database
    record = payload.model_dump()
    mock_db.append(record)

    # 4. Check for Post-Write Failure
    if FAILURE_MODE == "post_write_failure":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Simulated Post-Write Failure: Record committed but response failed",
        )

    return record


@app.get("/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    """Query transaction by ID."""
    # Find newest matching record
    for record in reversed(mock_db):
        if record["id"] == transaction_id:
            return record

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found"
    )


@app.get("/transactions")
async def list_transactions(limit: int = 50, offset: int = 0):
    """List transactions with pagination."""
    items = mock_db[offset : offset + limit]
    return {
        "items": items,
        "total": len(mock_db),
        "limit": limit,
        "offset": offset,
    }


@app.post("/cleanup")
async def cleanup():
    """Clear all transactions from database (testing only)."""
    global mock_db
    count = len(mock_db)
    mock_db.clear()
    return {"status": "ok", "deleted": count}


@app.get("/health")
async def health():
    return {"status": "ok", "database": "connected", "records": len(mock_db)}
