"""Redis helpers: sliding-window rate limiting and ingest idempotency.

Design note — Redis is a *availability aid*, not a correctness dependency.
During a cyclone the worst possible failure mode is dropping genuine citizen
reports because a cache node restarted, so every helper here fails **open**:
if Redis is unreachable the request is allowed through and the degradation is
logged loudly.
"""

from __future__ import annotations

import time
from typing import Final

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.core.config import Settings, get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Sliding-window counter via a sorted set. Atomic so concurrent uvicorn workers
# share one honest count instead of each keeping its own.
_RATE_LIMIT_LUA: Final[str] = """
local key      = KEYS[1]
local now_ms   = tonumber(ARGV[1])
local window   = tonumber(ARGV[2])
local limit    = tonumber(ARGV[3])
local member   = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now_ms - window)
local count = redis.call('ZCARD', key)
if count >= limit then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_ms = window - (now_ms - tonumber(oldest[2]))
    return {0, count, retry_ms}
end
redis.call('ZADD', key, now_ms, member)
redis.call('PEXPIRE', key, window)
return {1, count + 1, 0}
"""


class RedisGateway:
    """Thin async wrapper with connection lifecycle and fail-open semantics."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client: aioredis.Redis | None = None
        self._rate_limit_script = None
        self._degraded = False

    # ------------------------------------------------------------ lifecycle --
    async def connect(self) -> None:
        if not self._settings.redis_enabled:
            logger.warning("Redis disabled by configuration; running without cache")
            return
        try:
            self._client = aioredis.from_url(
                self._settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
                health_check_interval=30,
                retry_on_timeout=True,
            )
            await self._client.ping()
            self._rate_limit_script = self._client.register_script(_RATE_LIMIT_LUA)
            self._degraded = False
            logger.info("Redis connected", extra={"url": self._settings.redis_url})
        except (RedisError, OSError) as exc:
            self._client = None
            self._degraded = True
            logger.error("Redis unavailable; continuing degraded", extra={"error": str(exc)})

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.ping()
            return True
        except (RedisError, OSError):
            return False

    @property
    def degraded(self) -> bool:
        return self._degraded or (self._settings.redis_enabled and self._client is None)

    # ---------------------------------------------------------- rate limits --
    async def check_rate_limit(
        self, identity: str, *, limit: int | None = None, window_s: int = 60
    ) -> tuple[bool, int, int]:
        """Return ``(allowed, current_count, retry_after_seconds)``.

        Fails open: an unreachable Redis returns ``(True, 0, 0)``.
        """
        effective_limit = limit or self._settings.rate_limit_per_minute
        if self._client is None or self._rate_limit_script is None:
            return True, 0, 0

        now_ms = int(time.time() * 1000)
        member = f"{now_ms}-{time.perf_counter_ns()}"
        try:
            allowed, count, retry_ms = await self._rate_limit_script(
                keys=[f"ratelimit:{identity}"],
                args=[now_ms, window_s * 1000, effective_limit, member],
            )
            return bool(allowed), int(count), max(1, int(retry_ms) // 1000)
        except (RedisError, OSError) as exc:
            logger.warning("Rate-limit check failed open", extra={"error": str(exc)})
            return True, 0, 0

    # --------------------------------------------------------- idempotency --
    async def claim_once(self, key: str, ttl_s: int | None = None) -> bool:
        """Claim ``key`` for first-time processing.

        ``True`` means this caller won the claim. ``False`` means the exact same
        payload was already accepted inside the TTL window and should be
        treated as a replay. Fails open with ``True``.
        """
        if self._client is None:
            return True
        try:
            acquired = await self._client.set(
                f"idem:{key}", "1", nx=True, ex=ttl_s or self._settings.idempotency_ttl_s
            )
            return bool(acquired)
        except (RedisError, OSError) as exc:
            logger.warning("Idempotency claim failed open", extra={"error": str(exc)})
            return True

    async def get_json(self, key: str) -> str | None:
        if self._client is None:
            return None
        try:
            return await self._client.get(key)
        except (RedisError, OSError):
            return None

    async def set_json(self, key: str, value: str, ttl_s: int = 300) -> None:
        if self._client is None:
            return
        try:
            await self._client.set(key, value, ex=ttl_s)
        except (RedisError, OSError) as exc:
            logger.debug("Redis set skipped", extra={"error": str(exc), "key": key})


_gateway: RedisGateway | None = None


def get_redis() -> RedisGateway:
    """Process-wide gateway singleton."""
    global _gateway
    if _gateway is None:
        _gateway = RedisGateway()
    return _gateway
