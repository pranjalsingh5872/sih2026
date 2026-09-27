"""Aggregates every v1 router behind a single include in ``app.main``.

Phases 2–4 add their routers here (``events``, ``verification``, ``analytics``,
``alerts``) without touching the application factory.
"""

from __future__ import annotations

from fastapi import APIRouter

from app.api.v1 import analytics, health, ingest

api_router = APIRouter()

# Probes sit at the prefix root (/api/v1/healthz)
api_router.include_router(health.router)
api_router.include_router(ingest.router)
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])

__all__ = ["api_router"]