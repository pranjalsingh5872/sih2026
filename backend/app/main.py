"""FastAPI application entrypoint.

The application factory owns three things and delegates everything else:

1. **Lifespan** — dependencies are started once at boot and torn down on
   SIGTERM. The Kafka producer in particular must be connected before the
   first request, or the first citizen report of a disaster pays the
   connection latency.
2. **Correlation IDs** — every request gets one, it is threaded into the Kafka
   envelope, and it comes back on the response header. That is what lets an
   operator trace a single report from the phone that sent it, through the
   normalizer, to the event it ended up in.
3. **Error translation** — domain exceptions become HTTP responses in exactly
   one place, so routes raise meaning rather than status codes.

Boot is deliberately tolerant. If Redis is unreachable the app still serves;
if the gazetteer file is missing it does not, because a geo pipeline without a
gazetteer would silently mislocate every report.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Awaitable

from fastapi import FastAPI, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api.v1.router import api_router
from app.core.config import Settings, get_settings
from app.core.errors import ApiError, PlatformError
from app.core.logging import (
    configure_logging,
    get_correlation_id,
    get_logger,
    new_correlation_id,
    set_correlation_id,
)
from app.core.redis_client import get_redis
from app.geo.gazetteer import get_gazetteer
from app.geo.geocoder import get_geo_resolver
from app.messaging.producer import get_producer
from app.normalization.registry import get_registry

logger = get_logger(__name__)

CORRELATION_HEADER = "X-Correlation-ID"

DESCRIPTION = """
Unified multi-source weather and disaster intelligence pipeline for
**SIH26069 — National Weather Big Data Analytics Platform**.

**Phase 1–4:** ingestion, normalization, AI credibility scoring, PostGIS
spatial DBSCAN clustering, and emergency dispatch alerts.
"""

TAGS_METADATA = [
    {
        "name": "analytics",
        "description": "Spatial hotspot clusters, verified incidents, and emergency dispatch alerts.",
    },
    {
        "name": "ingestion",
        "description": "Citizen incident submission. Accepts JSON or multipart "
        "with a photo, from which EXIF GPS is recovered when the payload has "
        "no coordinates.",
    },
    {
        "name": "health",
        "description": "Liveness, readiness and pipeline configuration.",
    },
]


# ---------------------------------------------------------------- lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start and stop shared resources exactly once per process."""
    settings: Settings = get_settings()
    started = time.monotonic()

    # Hard dependency: a mislocating pipeline is worse than a stopped one.
    gazetteer = get_gazetteer()
    if len(gazetteer) == 0:
        raise RuntimeError("Gazetteer loaded zero places; refusing to start.")

    # Warm the normalizers so the first message does not pay import cost.
    registry = get_registry()

    producer = get_producer()
    await producer.start()

    redis = get_redis()
    await redis.connect()

    try:
        settings.media_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - depends on host mount
        logger.warning(
            "Media root not writable; photo uploads will fail",
            extra={"path": str(settings.media_root), "error": str(exc)},
        )

    logger.info(
        "API ready",
        extra={
            "environment": settings.app_env,
            "bus": "kafka" if settings.kafka_enabled else "in_memory",
            "redis_degraded": redis.degraded,
            "gazetteer_places": len(gazetteer),
            "sources": sorted(s.value for s in registry.supported_sources),
            "boot_ms": round((time.monotonic() - started) * 1000, 1),
        },
    )

    try:
        yield
    finally:
        logger.info("API shutting down", extra={"producer_stats": producer.stats})
        await producer.stop()
        await get_geo_resolver().aclose()
        await redis.close()


# ------------------------------------------------------------- app factory ---
def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(
        level=settings.log_level, json_output=settings.log_json, service="api"
    )

    app = FastAPI(
        title=settings.app_name,
        description=DESCRIPTION,
        version="1.0.0-phase4",
        openapi_tags=TAGS_METADATA,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=[CORRELATION_HEADER, "Retry-After"],
    )

    _register_middleware(app)
    _register_exception_handlers(app)

    app.include_router(api_router, prefix=settings.api_v1_prefix)

    # Uploaded photos are served back for the operator triage view.
    try:
        settings.media_root.mkdir(parents=True, exist_ok=True)
        app.mount(
            "/media",
            StaticFiles(directory=str(settings.media_root)),
            name="media",
        )
    except OSError as exc:  # pragma: no cover - depends on host mount
        logger.warning(
            "Skipping /media mount; directory unavailable",
            extra={"path": str(settings.media_root), "error": str(exc)},
        )

    dashboard_file = Path(__file__).resolve().parent / "static" / "index.html"

    @app.get("/healthz", include_in_schema=False)
    async def root_healthz() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/readyz", include_in_schema=False)
    async def root_readyz() -> dict[str, str]:
        return {"status": "ready"}

    @app.get("/", include_in_schema=False)
    async def root(request: Request) -> Response:
        accept = request.headers.get("accept", "")
        # Browsers send 'text/html,...'
        if "text/html" in accept and dashboard_file.is_file():
            return FileResponse(str(dashboard_file))
        return JSONResponse({
            "service": settings.app_name,
            "status": "online",
            "docs": "/docs",
            "hotspots": f"{settings.api_v1_prefix}/analytics/hotspots",
            "alerts": f"{settings.api_v1_prefix}/analytics/alerts",
        })

    return app


def _register_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def correlation_and_timing(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        incoming = request.headers.get(CORRELATION_HEADER)
        cid = incoming or new_correlation_id()
        set_correlation_id(cid)

        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            logger.exception(
                "Unhandled error in request",
                extra={
                    "path": request.url.path,
                    "method": request.method,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                },
            )
            raise
        finally:
            set_correlation_id(None)

        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        response.headers[CORRELATION_HEADER] = cid
        response.headers["X-Response-Time-ms"] = str(duration_ms)

        if not request.url.path.endswith(("/healthz", "/readyz")):
            logger.info(
                "request",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": duration_ms,
                },
            )
        return response


def _error_body(
    *, code: str, message: str, status_code: int, details: Any = None
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "error": {
            "code": code,
            "message": message,
            "status": status_code,
        },
        "correlation_id": get_correlation_id(),
    }
    if details is not None:
        body["error"]["details"] = details
    return body


def _register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(request: Request, exc: ApiError) -> JSONResponse:
        headers: dict[str, str] = {}
        retry_after = getattr(exc, "retry_after_s", None)
        if retry_after is not None:
            headers["Retry-After"] = str(int(retry_after))

        logger.warning(
            "API error",
            extra={
                "path": request.url.path,
                "error_code": exc.error_code,
                "status": exc.status_code,
            },
        )
        return JSONResponse(
            status_code=exc.status_code,
            headers=headers,
            content=_error_body(
                code=exc.error_code,
                message=exc.message,
                status_code=exc.status_code,
                details=exc.context or None,
            ),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        details = [
            {
                "field": ".".join(str(p) for p in err.get("loc", ())),
                "issue": err.get("msg", "invalid"),
                "type": err.get("type", "value_error"),
            }
            for err in exc.errors()
        ]
        logger.info(
            "Request validation failed",
            extra={"path": request.url.path, "fields": [d["field"] for d in details]},
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=_error_body(
                code="VALIDATION_ERROR",
                message="Request payload failed validation",
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                details=details,
            ),
        )

    @app.exception_handler(PlatformError)
    async def handle_platform_error(
        request: Request, exc: PlatformError
    ) -> JSONResponse:
        logger.error(
            "Domain error escaped to the API boundary",
            extra={"path": request.url.path, **exc.to_dict()},
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_body(
                code=exc.error_code,
                message="Internal error while processing the request",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception", extra={"path": request.url.path})
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error_body(
                code="INTERNAL_ERROR",
                message="Internal server error",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ),
        )


app = create_app()