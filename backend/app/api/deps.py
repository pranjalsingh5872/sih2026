"""FastAPI dependencies.

Auth and rate limiting are expressed as dependencies rather than middleware so
each route declares exactly what it needs, and so the health endpoints stay
reachable when Redis is down.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Header, Request

from app.core.config import Settings, get_settings
from app.core.errors import AuthenticationError, RateLimitExceededError
from app.core.logging import get_logger
from app.core.redis_client import RedisGateway, get_redis
from app.core.security import Principal, Scope, authenticate
from app.messaging.producer import EventProducer, get_producer

logger = get_logger(__name__)

API_KEY_HEADER = "X-API-Key"


def settings_dep() -> Settings:
    return get_settings()


def producer_dep() -> EventProducer:
    return get_producer()


def redis_dep() -> RedisGateway:
    return get_redis()


async def current_principal(
    x_api_key: Annotated[str | None, Header(alias=API_KEY_HEADER)] = None,
    settings: Annotated[Settings, Depends(settings_dep)] = None,  # type: ignore[assignment]
) -> Principal:
    """Resolve the caller from the API key header."""
    principal = authenticate(x_api_key, settings)
    logger.debug(
        "Request authenticated",
        extra={"subject": principal.subject, "scopes": sorted(s.value for s in principal.scopes)},
    )
    return principal


async def require_ingest_scope(
    principal: Annotated[Principal, Depends(current_principal)],
) -> Principal:
    principal.require(Scope.INGEST)
    return principal


async def require_admin_scope(
    principal: Annotated[Principal, Depends(current_principal)],
) -> Principal:
    principal.require(Scope.ADMIN)
    return principal


async def enforce_rate_limit(
    request: Request,
    principal: Annotated[Principal, Depends(current_principal)],
    redis: Annotated[RedisGateway, Depends(redis_dep)],
    settings: Annotated[Settings, Depends(settings_dep)],
) -> None:
    """Sliding-window limit, keyed on credential rather than IP.

    Keying on the API key means one abusive client cannot exhaust the quota of
    everyone behind the same carrier-grade NAT — which, on Indian mobile
    networks during a disaster, would be a large fraction of genuine reporters.
    """
    identity = principal.key_fingerprint or (
        request.client.host if request.client else "unknown"
    )
    allowed, count, retry_after = await redis.check_rate_limit(
        identity, limit=settings.rate_limit_per_minute, window_s=60
    )
    if not allowed:
        logger.warning(
            "Rate limit exceeded",
            extra={"subject": principal.subject, "count": count, "path": request.url.path},
        )
        raise RateLimitExceededError(
            f"Rate limit of {settings.rate_limit_per_minute}/min exceeded",
            retry_after_s=retry_after,
        )


# Convenience aliases for route signatures.
SettingsDep = Annotated[Settings, Depends(settings_dep)]
ProducerDep = Annotated[EventProducer, Depends(producer_dep)]
RedisDep = Annotated[RedisGateway, Depends(redis_dep)]
IngestPrincipal = Annotated[Principal, Depends(require_ingest_scope)]
AdminPrincipal = Annotated[Principal, Depends(require_admin_scope)]
RateLimited = Annotated[None, Depends(enforce_rate_limit)]
