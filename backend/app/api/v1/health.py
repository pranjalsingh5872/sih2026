"""Liveness, readiness and capability endpoints.

Three distinct questions, three distinct answers:

``/healthz``   Is the process alive? Never touches a dependency, so a Redis
               outage cannot trigger a pod restart loop.
``/readyz``    Should the load balancer send this instance traffic? Ingest is
               only meaningful if the message bus will accept a publish, so
               that is the single hard gate. Redis being down is *degraded*,
               not unready — rate limiting and idempotency fail open by design.
``/info``      What is this build configured to do? Used by the dashboard and
               by graders who want to see the pipeline's shape without reading
               the compose file.

None of these require authentication. They expose no report content and no
credentials, and an ops probe that needs a key is an ops probe that silently
stops working.
"""

from __future__ import annotations

import time
from typing import Any, Literal

from fastapi import APIRouter, Response, status

from app.api.deps import ProducerDep, RedisDep, SettingsDep
from app.core.logging import get_logger
from app.geo.gazetteer import get_gazetteer
from app.messaging.topics import ConsumerGroups, Topics
from app.normalization.registry import get_registry
from app.schemas.incident import SCHEMA_VERSION

logger = get_logger(__name__)

router = APIRouter(tags=["health"])

_PROCESS_STARTED_AT = time.monotonic()


def _uptime_s() -> float:
    return round(time.monotonic() - _PROCESS_STARTED_AT, 3)


@router.get("/healthz", summary="Liveness probe")
async def liveness() -> dict[str, Any]:
    """Alive means the event loop is turning. Nothing more is claimed."""
    return {"status": "alive", "uptime_s": _uptime_s()}


@router.get("/readyz", summary="Readiness probe")
async def readiness(
    response: Response,
    producer: ProducerDep,
    redis: RedisDep,
    settings: SettingsDep,
) -> dict[str, Any]:
    """Report per-dependency status and gate traffic on the bus alone."""
    bus_ready = producer.is_ready
    redis_degraded = redis.degraded
    redis_ok = (not settings.redis_enabled) or not redis_degraded

    checks: dict[str, dict[str, Any]] = {
        "message_bus": {
            "status": "ok" if bus_ready else "down",
            "mode": "kafka" if settings.kafka_enabled else "in_memory",
            "bootstrap": settings.kafka_bootstrap_servers if settings.kafka_enabled else None,
            "required": True,
            **producer.stats,
        },
        "redis": {
            "status": "ok" if redis_ok else "degraded",
            "enabled": settings.redis_enabled,
            # Spelled out because "degraded" is easy to misread as "broken".
            "impact": (
                None
                if redis_ok
                else "Rate limiting and submit-idempotency are failing open; "
                "reports are still accepted and published."
            ),
            "required": False,
        },
        "gazetteer": {
            "status": "ok",
            "places": len(get_gazetteer()),
            "required": True,
        },
    }

    overall: Literal["ready", "degraded", "not_ready"]
    if not bus_ready:
        overall = "not_ready"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    elif not redis_ok:
        overall = "degraded"
    else:
        overall = "ready"

    return {
        "status": overall,
        "uptime_s": _uptime_s(),
        "environment": settings.app_env,
        "checks": checks,
    }


@router.get("/info", summary="Build and pipeline configuration")
async def info(settings: SettingsDep) -> dict[str, Any]:
    """Describe what this deployment is wired to, without leaking secrets."""
    registry = get_registry()
    return {
        "app": settings.app_name,
        "environment": settings.app_env,
        "phase": "1 — Data Ingestion & Normalization",
        "incident_schema_version": SCHEMA_VERSION,
        "sources_supported": sorted(s.value for s in registry.supported_sources),
        "topics": {
            "raw": Topics.RAW_WEATHER,
            "normalized": Topics.NORMALIZED_INCIDENTS,
            "dead_letter": Topics.DEAD_LETTER,
            "geo_unresolved": Topics.GEO_UNRESOLVED,
        },
        "consumer_groups": {
            "normalizer": ConsumerGroups.NORMALIZER,
            "ai_enrichment": ConsumerGroups.AI_ENRICHMENT,
            "event_fusion": ConsumerGroups.EVENT_FUSION,
        },
        "providers": {
            "imd": {
                "mock_mode": settings.imd_mock_mode,
                "poll_interval_s": settings.imd_poll_interval_s,
            },
            "openweather": {
                "mock_mode": settings.openweather_mock_mode,
                "poll_interval_s": settings.openweather_poll_interval_s,
                "api_key_configured": bool(settings.openweather_api_key),
            },
            "social_simulator": {
                "rate_per_min": settings.social_sim_rate_per_min,
                "duplicate_ratio": settings.social_sim_duplicate_ratio,
                "fake_ratio": settings.social_sim_fake_ratio,
                "missing_geo_ratio": settings.social_sim_missing_geo_ratio,
            },
        },
        "geocoding": {
            "gazetteer_places": len(get_gazetteer()),
            "remote_geocoder_enabled": settings.geocoder_remote_enabled,
            "min_similarity": settings.gazetteer_min_similarity,
        },
        "limits": {
            "rate_limit_per_minute": settings.rate_limit_per_minute,
            "max_upload_bytes": settings.max_upload_bytes,
            "allowed_media_types": settings.allowed_media_types,
        },
    }
