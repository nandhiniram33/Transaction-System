"""Client for interacting with the external Mock Posting Service."""

from dataclasses import dataclass
from typing import Any, Dict, Optional
import httpx

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class PostingResponse:
    """Standardized result of a remote posting service operation."""
    success: bool
    status_code: Optional[int] = None
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    is_timeout: bool = False
    is_network_error: bool = False


class PostingServiceClient:
    """HTTP Client for communicating with the remote Mock Posting Service."""

    def __init__(self, base_url: Optional[str] = None, timeout: Optional[float] = None):
        self.base_url = (base_url or settings.MOCK_POSTING_SERVICE_URL).rstrip("/")
        self.timeout = timeout or settings.POSTING_REQUEST_TIMEOUT_SECONDS
        self._client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        """Reuse HTTPX AsyncClient with connection pooling."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout, connect=5.0),
                limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
            )
        return self._client

    async def get_transaction(self, transaction_id: str) -> PostingResponse:
        """Query transaction existence by ID (GET /transactions/{id})."""
        client = await self.get_client()
        url = f"/transactions/{transaction_id}"
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return PostingResponse(
                    success=True,
                    status_code=200,
                    data=resp.json(),
                )
            elif resp.status_code == 404:
                return PostingResponse(
                    success=False,
                    status_code=404,
                    error="Transaction not found",
                )
            else:
                return PostingResponse(
                    success=False,
                    status_code=resp.status_code,
                    error=f"Remote returned unexpected status: {resp.status_code}",
                )
        except httpx.TimeoutException as exc:
            logger.warning("Timeout querying transaction %s: %s", transaction_id, exc)
            return PostingResponse(success=False, is_timeout=True, error=str(exc))
        except httpx.RequestError as exc:
            logger.warning("Network error querying transaction %s: %s", transaction_id, exc)
            return PostingResponse(success=False, is_network_error=True, error=str(exc))

    async def post_transaction(self, payload: Dict[str, Any]) -> PostingResponse:
        """Submit a transaction to the remote service (POST /transactions)."""
        client = await self.get_client()
        url = "/transactions"
        try:
            resp = await client.post(url, json=payload)
            if resp.status_code in (200, 201):
                try:
                    data = resp.json()
                except Exception:
                    data = {"raw": resp.text}
                return PostingResponse(
                    success=True,
                    status_code=resp.status_code,
                    data=data,
                )
            else:
                return PostingResponse(
                    success=False,
                    status_code=resp.status_code,
                    error=f"POST failed with HTTP {resp.status_code}: {resp.text}",
                )
        except httpx.TimeoutException as exc:
            logger.warning("Timeout executing POST /transactions for %s: %s", payload.get("id"), exc)
            return PostingResponse(success=False, is_timeout=True, error=str(exc))
        except httpx.RequestError as exc:
            logger.warning("Network error executing POST /transactions for %s: %s", payload.get("id"), exc)
            return PostingResponse(success=False, is_network_error=True, error=str(exc))

    async def cleanup(self) -> bool:
        """Trigger testing reset endpoint (POST /cleanup)."""
        client = await self.get_client()
        try:
            resp = await client.post("/cleanup")
            return resp.status_code == 200
        except Exception as exc:
            logger.error("Failed to execute cleanup: %s", exc)
            return False

    async def close(self) -> None:
        """Close underlying HTTPX client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None


# Global posting service client instance
posting_client = PostingServiceClient()
