"""Shared fixtures.

Tests run entirely offline. The in-memory bus stands in for Kafka, Redis is
disabled so the gateway takes its fail-open path, and the remote geocoder is
switched off so nothing reaches for the network. What that leaves under test is
the logic the demo actually depends on: schema invariants, the geo fallback
chain, every normalizer, and the API contract.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import pytest

# Must be set before app.core.config is imported anywhere, since Settings is
# an lru_cached singleton hydrated from the environment at first access.
os.environ.setdefault("APP_ENV", "local")
os.environ.setdefault("KAFKA_ENABLED", "false")
os.environ.setdefault("REDIS_ENABLED", "false")
os.environ.setdefault("GEOCODER_REMOTE_ENABLED", "false")
os.environ.setdefault("IMD_MOCK_MODE", "true")
os.environ.setdefault("OPENWEATHER_MOCK_MODE", "true")
os.environ.setdefault("LOG_JSON", "false")
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.setdefault("SOCIAL_SIM_SEED", "26069")

from app.core.config import get_settings  # noqa: E402
from app.geo.gazetteer import get_gazetteer  # noqa: E402
from app.geo.geocoder import GeoResolver  # noqa: E402
from app.messaging.producer import get_memory_bus  # noqa: E402
from app.normalization.registry import NormalizerRegistry  # noqa: E402
from app.schemas.enums import PipelineStage, SourceType  # noqa: E402
from app.schemas.raw import RawEnvelope  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _media_root(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """Point media writes at a temp dir instead of /data/media."""
    root = tmp_path_factory.mktemp("media")
    settings = get_settings()
    original = settings.media_root
    settings.media_root = root
    yield root
    settings.media_root = original


@pytest.fixture
def settings():
    return get_settings()


@pytest.fixture
def gazetteer():
    return get_gazetteer()


@pytest.fixture
def resolver() -> GeoResolver:
    """A resolver exercising the offline chain only.

    ``GEOCODER_REMOTE_ENABLED`` is false in the test environment, so the
    Nominatim step short-circuits and no HTTP client is ever constructed.
    """
    return GeoResolver()


@pytest.fixture
def registry(resolver: GeoResolver) -> NormalizerRegistry:
    return NormalizerRegistry(geo_resolver=resolver)


@pytest.fixture
def bus() -> Iterator[Any]:
    """Fresh in-memory bus per test."""
    memory_bus = get_memory_bus()
    memory_bus.clear()
    yield memory_bus
    memory_bus.clear()


@pytest.fixture
def now() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture
def envelope_factory(now: datetime):
    """Build a RawEnvelope around an arbitrary payload."""

    def _factory(
        source_type: SourceType,
        payload: dict[str, Any],
        *,
        source_name: str | None = None,
        external_id: str | None = None,
        observed_at: datetime | None = None,
    ) -> RawEnvelope:
        return RawEnvelope(
            source_type=source_type,
            source_name=source_name or f"{source_type.value.lower()}_test",
            external_id=external_id,
            observed_at=observed_at,
            payload=payload,
            ingest_stage=PipelineStage.PROVIDER_POLL,
            producer_component="pytest",
        )

    return _factory


@pytest.fixture
def imd_warning_payload(now: datetime) -> dict[str, Any]:
    return {
        "record_type": "warning",
        "bulletin_id": "IMD-20260920-00042",
        "hazard_type": "Heavy Rainfall",
        "colour_code": "RED",
        "headline": "Heavy Rainfall warning for Indore, Madhya Pradesh",
        "description": (
            "Extremely heavy rainfall very likely at isolated places over "
            "Indore district of Madhya Pradesh during the next 24 hours."
        ),
        "district": "Indore",
        "state": "Madhya Pradesh",
        "area": "Indore",
        "lat": 22.7196,
        "lon": 75.8577,
        "issue_time": now.isoformat(),
        "valid_from": now.isoformat(),
        "valid_until": (now + timedelta(hours=24)).isoformat(),
        "valid_hours": 24,
        "issuing_office": "RMC Madhya Pradesh",
        "expected_rainfall_mm": 185.0,
    }


@pytest.fixture
def imd_observation_payload(now: datetime) -> dict[str, Any]:
    return {
        "record_type": "observation",
        "station_id": "AWS-41234",
        "station_name": "Indore AWS",
        "station_type": "AWS",
        "district": "Indore",
        "state": "Madhya Pradesh",
        "lat": 22.7196,
        "lon": 75.8577,
        "observation_time": now.isoformat(),
        "rainfall_mm": 96.4,
        "rainfall_window_hours": 24,
        "temperature_c": 26.1,
        "humidity_pct": 94.0,
        "wind_speed_kmh": 18.0,
        "wind_gust_kmh": 41.0,
        "pressure_hpa": 1002.0,
        "visibility_m": 3500,
    }


@pytest.fixture
def openweather_payload(now: datetime) -> dict[str, Any]:
    return {
        "record_type": "current",
        "id": 1269743,
        "name": "Indore",
        "dt": int(now.timestamp()),
        "coord": {"lat": 22.7196, "lon": 75.8577},
        "weather": [{"id": 502, "main": "Rain", "description": "heavy intensity rain"}],
        "main": {"temp": 299.15, "feels_like": 303.2, "humidity": 92, "pressure": 1001},
        "wind": {"speed": 7.2, "deg": 210, "gust": 13.4},
        "clouds": {"all": 95},
        "visibility": 2500,
        "rain": {"1h": 28.5},
    }


@pytest.fixture
def citizen_payload(now: datetime) -> dict[str, Any]:
    return {
        "submission_id": "cz-abc123def456abc123def456",
        "description": "Water entered our street near Rajwada, Indore. Knee deep flooding.",
        "lat": 22.7180,
        "lon": 75.8550,
        "location_accuracy_m": 12.0,
        "place_name": "Indore",
        "district": "Indore",
        "state": "Madhya Pradesh",
        "pincode": "452001",
        "observed_at": now.isoformat(),
        "category_hint": None,
        "water_level_cm": 55.0,
        "author_id": "a" * 32,
        "reporter_handle": None,
        "client_app_version": "1.0.0",
        "submitted_via": "rest_api",
        "media": [],
        "exif": None,
    }


@pytest.fixture
def social_payload(now: datetime) -> dict[str, Any]:
    return {
        "post_id": "p0123456789abcdef",
        "platform": "x",
        "text": "Massive waterlogging near Indore railway station #IndoreRains roads submerged",
        "lang": "en",
        "created_at": now.isoformat(),
        "lat": 22.7150,
        "lon": 75.8600,
        "geo": {"lat": 22.7150, "lon": 75.8600, "place_name": "Indore"},
        "author": {
            "id": "u123456",
            "handle": "user_4242",
            "verified": False,
            "account_age_days": 900,
            "follower_count": 1500,
        },
        "repost_count": 12,
        "reply_count": 3,
        "like_count": 48,
        "media": [],
        "url": "https://social.example.invalid/p/abc123",
        "is_repost": False,
    }


@pytest.fixture
def sensor_payload(now: datetime) -> dict[str, Any]:
    return {
        "sensor_id": "MPSDMA-RG-0091",
        "sensor_type": "rain_gauge",
        "operator": "MP SDMA",
        "timestamp": now.isoformat(),
        "lat": 22.7196,
        "lon": 75.8577,
        "rain": 88.2,
        "temp": 25.4,
        "rh": 96,
    }
