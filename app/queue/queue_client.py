"""Resilient asynchronous Redis queue client with local/in-memory fallback."""

import json
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis
from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Module-level fake redis server instance for in-memory queue sharing
_fake_server: Optional[Any] = None


class QueueClient:
    """Async Redis queue client supporting durable enqueue, batch enqueue, and dequeue."""

    def __init__(
        self, redis_url: Optional[str] = None, queue_name: Optional[str] = None
    ):
        self.redis_url = redis_url or settings.REDIS_URL
        self.queue_name = queue_name or settings.REDIS_QUEUE_NAME
        self._redis: Optional[aioredis.Redis] = None
        self._is_fake: bool = False

    async def connect(self) -> None:
        """Establish Redis connection with automatic fallback for local/testing environments."""
        global _fake_server

        if self.redis_url.startswith("fakeredis"):
            import fakeredis.aioredis

            if _fake_server is None:
                _fake_server = fakeredis.FakeServer()
            self._redis = fakeredis.aioredis.FakeRedis(
                server=_fake_server, decode_responses=True
            )
            self._is_fake = True
            logger.info("Connected to in-memory FakeRedis queue engine.")
            return

        try:
            client = aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=settings.REDIS_CONNECT_TIMEOUT_SECONDS,
                socket_timeout=settings.REDIS_CONNECT_TIMEOUT_SECONDS,
            )
            await client.ping()
            self._redis = client
            self._is_fake = False
            logger.info("Connected successfully to Redis server at %s", self.redis_url)
        except Exception as exc:
            logger.warning(
                "Failed to connect to real Redis at %s (%s). Falling back to FakeRedis.",
                self.redis_url,
                str(exc),
            )
            import fakeredis.aioredis

            if _fake_server is None:
                _fake_server = fakeredis.FakeServer()
            self._redis = fakeredis.aioredis.FakeRedis(
                server=_fake_server, decode_responses=True
            )
            self._is_fake = True

    async def is_connected(self) -> bool:
        """Check if Redis connection is active."""
        if self._redis is None:
            return False
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    async def enqueue(self, payload: Dict[str, Any]) -> bool:
        """Push a transaction job to the tail of the queue."""
        if self._redis is None:
            await self.connect()
        try:
            serialized = json.dumps(payload)
            await self._redis.rpush(self.queue_name, serialized)
            return True
        except Exception as exc:
            logger.error("Failed to enqueue job to Redis: %s", exc)
            return False

    async def enqueue_batch(self, payloads: List[Dict[str, Any]]) -> int:
        """Batch push multiple transaction jobs using a Redis pipeline."""
        if not payloads:
            return 0
        if self._redis is None:
            await self.connect()

        try:
            serialized_list = [json.dumps(p) for p in payloads]
            pipe = self._redis.pipeline()
            for item in serialized_list:
                pipe.rpush(self.queue_name, item)
            await pipe.execute()
            return len(payloads)
        except Exception as exc:
            logger.error("Failed to enqueue batch to Redis: %s", exc)
            return 0

    async def dequeue(self, timeout_seconds: float = 1.0) -> Optional[Dict[str, Any]]:
        """Blocking pop a transaction job from the head of the queue."""
        if self._redis is None:
            await self.connect()

        try:
            result = await self._redis.blpop(
                [self.queue_name], timeout=int(max(1, timeout_seconds))
            )
            if result:
                _, item_str = result
                return json.loads(item_str)
            return None
        except Exception as exc:
            logger.error("Failed to dequeue job from Redis: %s", exc)
            return None

    async def get_queue_depth(self) -> int:
        """Return the current count of pending jobs in the queue."""
        if self._redis is None:
            await self.connect()
        try:
            return int(await self._redis.llen(self.queue_name))
        except Exception as exc:
            logger.error("Failed to query Redis queue depth: %s", exc)
            return 0

    async def close(self) -> None:
        """Close connection to Redis."""
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None


# Global queue client instance
queue_client = QueueClient()
