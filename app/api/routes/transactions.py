"""API route handlers for transaction submission and status queries."""

import time
from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import correlation_id_ctx, get_logger
from app.database.db import get_db_session
from app.monitoring.metrics import metrics_collector
from app.schemas.transaction import (
    TransactionCreateRequest,
    TransactionResponse,
    TransactionStatus,
)
from app.services.transaction_service import ConflictException, transaction_service

router = APIRouter(prefix="/api/transactions", tags=["Transactions"])
logger = get_logger(__name__)


@router.post(
    "",
    response_model=TransactionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a transaction for asynchronous processing",
    responses={
        202: {"description": "Transaction accepted for processing"},
        200: {"description": "Identical duplicate transaction returned"},
        409: {"description": "Conflict: Transaction ID exists with different payload"},
        422: {"description": "Payload validation error"},
    },
)
async def submit_transaction(
    payload: TransactionCreateRequest,
    response: Response,
    session: AsyncSession = Depends(get_db_session),
) -> TransactionResponse:
    """Accepts transaction requests via REST API with sub-100ms response time."""
    start_time = time.perf_counter()
    correlation_id_ctx.set(payload.id)

    try:
        tx_model, is_new = await transaction_service.accept_transaction(
            session=session,
            request=payload,
        )

        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        metrics_collector.record_submission_latency(elapsed_ms)

        if is_new:
            response.status_code = status.HTTP_202_ACCEPTED
        else:
            metrics_collector.record_duplicate()
            response.status_code = status.HTTP_200_OK

        return TransactionResponse(
            transactionId=tx_model.id,
            status=TransactionStatus(tx_model.status),
            submittedAt=tx_model.submitted_at,
            completedAt=tx_model.completed_at,
            error=tx_model.error,
        )

    except ConflictException as exc:
        metrics_collector.record_conflict()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )
    except Exception as exc:
        logger.error(
            "Unhandled error accepting transaction %s: %s",
            payload.id,
            exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error processing transaction",
        )


@router.get(
    "/{transaction_id}",
    response_model=TransactionResponse,
    summary="Query transaction status and lifecycle details",
    responses={
        200: {"description": "Transaction details found"},
        404: {"description": "Transaction not found"},
    },
)
async def get_transaction(
    transaction_id: str,
    session: AsyncSession = Depends(get_db_session),
) -> TransactionResponse:
    """Fetch transaction details, status, timestamps, and error reasons."""
    correlation_id_ctx.set(transaction_id)
    tx_model = await transaction_service.get_transaction(session, transaction_id)
    if not tx_model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction '{transaction_id}' was not found.",
        )

    return TransactionResponse(
        transactionId=tx_model.id,
        status=TransactionStatus(tx_model.status),
        submittedAt=tx_model.submitted_at,
        completedAt=tx_model.completed_at,
        error=tx_model.error,
    )
