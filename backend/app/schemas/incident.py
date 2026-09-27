"""The unified incident contract.

Everything the platform ingests — an IMD nowcast, a Marathi tweet, a photo from
a citizen standing in knee-deep water — becomes a :class:`NormalizedIncident`.
Downstream phases depend on *this* model and never on a provider's payload
shape, which is what keeps adding a sixth source a one-file change.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)

from app.schemas.enums import (
    GEO_METHOD_CONFIDENCE,
    GeoMethod,
    HazardCategory,
    MediaKind,
    PipelineStage,
    SeverityHint,
    SourceType,
)

SCHEMA_VERSION = 1

# Stable namespace so the same logical report always yields the same UUID,
# whichever worker or replay produced it.
INCIDENT_NAMESPACE = uuid.UUID("6f2a1c4e-9b7d-5a3f-8e21-000000026069")

# Reject observations implausibly far in the future — a mis-set device clock
# would otherwise poison the Phase 3 temporal clustering window.
MAX_CLOCK_SKEW = timedelta(hours=2)
MAX_BACKFILL_AGE = timedelta(days=30)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class GeoPoint(BaseModel):
    """WGS84 coordinate pair."""

    model_config = ConfigDict(frozen=True)

    lat: float = Field(ge=-90.0, le=90.0, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180.0, le=180.0, description="Longitude in decimal degrees")

    @model_validator(mode="after")
    def _reject_null_island(self) -> Self:
        """(0, 0) is overwhelmingly a missing-value sentinel, not a real fix."""
        if abs(self.lat) < 1e-9 and abs(self.lon) < 1e-9:
            raise ValueError("Coordinates (0, 0) rejected as a null-island sentinel")
        return self

    def to_geojson(self) -> dict[str, Any]:
        return {"type": "Point", "coordinates": [self.lon, self.lat]}

    def to_wkt(self) -> str:
        return f"SRID=4326;POINT({self.lon} {self.lat})"


class GeoContext(BaseModel):
    """Location plus the provenance of how it was determined."""

    point: GeoPoint | None = None
    method: GeoMethod = GeoMethod.UNRESOLVED
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    place_label: str | None = Field(default=None, max_length=256)
    district: str | None = Field(default=None, max_length=128)
    state: str | None = Field(default=None, max_length=128)
    # Radius in km within which the true location is believed to lie. A district
    # centroid carries tens of km of error and Phase 3 must not cluster on it as
    # though it were a GPS fix.
    uncertainty_radius_km: float | None = Field(default=None, ge=0.0)
    outside_india: bool = False

    @model_validator(mode="after")
    def _enforce_consistency(self) -> Self:
        if self.point is None:
            # No point means no claim to confidence, whatever the caller said.
            object.__setattr__(self, "method", GeoMethod.UNRESOLVED)
            object.__setattr__(self, "confidence", 0.0)
        elif self.confidence == 0.0 and self.method is not GeoMethod.UNRESOLVED:
            object.__setattr__(self, "confidence", GEO_METHOD_CONFIDENCE[self.method])
        return self

    @property
    def is_resolved(self) -> bool:
        return self.point is not None


class MediaAsset(BaseModel):
    """A photo/video attached to a report."""

    url: str = Field(max_length=2048)
    kind: MediaKind = MediaKind.UNKNOWN
    content_type: str | None = Field(default=None, max_length=128)
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, min_length=64, max_length=64)
    captured_at: datetime | None = None
    exif_gps: GeoPoint | None = None
    # Phase 2 fills these in (reverse image search, manipulation detection).
    integrity_checked: bool = False


class AuthorRef(BaseModel):
    """Pseudonymous reporter reference.

    No raw phone numbers, device ids or handles are persisted — see
    ``app.core.security.pseudonymous_author_id``.
    """

    author_id: str = Field(max_length=128)
    display_handle: str | None = Field(default=None, max_length=128)
    platform: str | None = Field(default=None, max_length=64)
    is_verified_account: bool = False
    account_age_days: int | None = Field(default=None, ge=0)
    follower_count: int | None = Field(default=None, ge=0)
    prior_report_count: int | None = Field(default=None, ge=0)


class Measurements(BaseModel):
    """Quantitative meteorological values, in fixed SI-ish units.

    Every source that reports numbers converts into *these* units at
    normalization time. Phase 2's meteorological-alignment check compares a
    citizen claim against the nearest AWS reading, and that comparison is only
    meaningful if nobody downstream has to wonder about mm vs cm or K vs °C.
    """

    model_config = ConfigDict(extra="forbid")

    rainfall_mm: float | None = Field(default=None, ge=0.0, le=2000.0)
    rainfall_window_hours: float | None = Field(default=None, gt=0.0, le=168.0)
    temperature_c: float | None = Field(default=None, ge=-60.0, le=60.0)
    feels_like_c: float | None = Field(default=None, ge=-80.0, le=80.0)
    humidity_pct: float | None = Field(default=None, ge=0.0, le=100.0)
    wind_speed_kmh: float | None = Field(default=None, ge=0.0, le=500.0)
    wind_gust_kmh: float | None = Field(default=None, ge=0.0, le=600.0)
    wind_direction_deg: float | None = Field(default=None, ge=0.0, le=360.0)
    pressure_hpa: float | None = Field(default=None, ge=800.0, le=1100.0)
    visibility_m: float | None = Field(default=None, ge=0.0, le=100_000.0)
    water_level_cm: float | None = Field(default=None, ge=0.0, le=2000.0)

    def has_any(self) -> bool:
        return any(v is not None for v in self.model_dump().values())


class TraceEntry(BaseModel):
    """One step in the processing lineage."""

    stage: PipelineStage
    at: datetime = Field(default_factory=utcnow)
    component: str = Field(max_length=128)
    note: str | None = Field(default=None, max_length=512)


class NormalizedIncident(BaseModel):
    """The canonical record on ``normalized-incident-stream``.

    Phase 2 subclasses this into ``EnrichedIncident`` to add
    ``credibility_score``, ``is_duplicate`` and ``verification_status``. This
    model stays free of judgement: it records what was observed and how
    confidently it was located, nothing more.
    """

    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
        ser_json_timedelta="iso8601",
    )

    schema_version: int = SCHEMA_VERSION
    incident_id: uuid.UUID
    correlation_id: str | None = Field(default=None, max_length=64)

    # ---- provenance ------------------------------------------------------
    source_type: SourceType
    source_name: str = Field(max_length=128, description="Concrete feed, e.g. 'imd_nowcast'")
    external_id: str | None = Field(default=None, max_length=256)

    # ---- time ------------------------------------------------------------
    observed_at: datetime = Field(description="When the phenomenon was observed/issued")
    ingested_at: datetime = Field(default_factory=utcnow)
    normalized_at: datetime = Field(default_factory=utcnow)

    # ---- space -----------------------------------------------------------
    geo: GeoContext = Field(default_factory=GeoContext)

    # ---- content ---------------------------------------------------------
    raw_text: str | None = Field(default=None, max_length=8192)
    normalized_text: str | None = Field(default=None, max_length=8192)
    language: str | None = Field(default=None, max_length=16, description="ISO-639-1 or 'und'")

    reported_category: HazardCategory = HazardCategory.UNKNOWN
    severity_hint: SeverityHint = SeverityHint.UNKNOWN
    measurements: Measurements = Field(default_factory=Measurements)

    media: list[MediaAsset] = Field(default_factory=list, max_length=10)
    author: AuthorRef | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    trace: list[TraceEntry] = Field(default_factory=list, max_length=32)

    # ---------------------------------------------------------- validators --
    @field_validator("observed_at", "ingested_at", "normalized_at")
    @classmethod
    def _require_tz(cls, value: datetime) -> datetime:
        """Naive datetimes are a silent-corruption hazard in a clustering pipeline."""
        if value.tzinfo is None:
            raise ValueError("Timestamps must be timezone-aware (UTC preferred)")
        return value.astimezone(timezone.utc)

    @field_validator("language")
    @classmethod
    def _normalize_language(cls, value: str | None) -> str | None:
        return value.lower().strip() if value else value

    @model_validator(mode="after")
    def _validate_temporal_sanity(self) -> Self:
        now = utcnow()
        if self.observed_at > now + MAX_CLOCK_SKEW:
            raise ValueError(
                f"observed_at {self.observed_at.isoformat()} is implausibly "
                "far in the future (clock skew?)"
            )
        if self.observed_at < now - MAX_BACKFILL_AGE:
            raise ValueError(
                f"observed_at {self.observed_at.isoformat()} predates the "
                f"{MAX_BACKFILL_AGE.days}-day backfill horizon"
            )
        return self

    @field_serializer("incident_id")
    def _ser_uuid(self, value: uuid.UUID) -> str:
        return str(value)

    # ------------------------------------------------------------- derived --
    @computed_field  # type: ignore[prop-decorator]
    @property
    def content_hash(self) -> str:
        """Stable digest of the semantically meaningful fields.

        Deliberately excludes ``ingested_at`` and ``incident_id`` so that the
        *same* claim arriving twice hashes identically. This is exact-match
        dedup only — the embedding-based near-duplicate detection is Phase 2.
        """
        point = self.geo.point
        parts = [
            self.source_type.value,
            (self.normalized_text or self.raw_text or "").strip().lower(),
            f"{point.lat:.4f},{point.lon:.4f}" if point else "nogeo",
            self.observed_at.replace(second=0, microsecond=0).isoformat(),
            self.reported_category.value,
        ]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def has_location(self) -> bool:
        return self.geo.is_resolved

    @property
    def age_seconds(self) -> float:
        return (utcnow() - self.observed_at).total_seconds()

    # -------------------------------------------------------------- helpers --
    def add_trace(self, stage: PipelineStage, component: str, note: str | None = None) -> None:
        if len(self.trace) < 32:
            entry = TraceEntry(stage=stage, component=component, note=note)
            # validate_assignment is on, so rebind rather than mutate in place.
            self.trace = [*self.trace, entry]

    def to_geojson_feature(self) -> dict[str, Any]:
        """GeoJSON Feature for the Phase 5 map layer."""
        return {
            "type": "Feature",
            "id": str(self.incident_id),
            "geometry": self.geo.point.to_geojson() if self.geo.point else None,
            "properties": {
                "source_type": self.source_type.value,
                "source_name": self.source_name,
                "observed_at": self.observed_at.isoformat(),
                "category": self.reported_category.value,
                "severity_hint": self.severity_hint.value,
                "text": self.normalized_text,
                "language": self.language,
                "geo_method": self.geo.method.value,
                "geo_confidence": round(self.geo.confidence, 3),
                "place_label": self.geo.place_label,
                "district": self.geo.district,
                "state": self.geo.state,
                "media_count": len(self.media),
            },
        }

    @staticmethod
    def derive_id(source_type: SourceType, external_id: str) -> uuid.UUID:
        """Deterministic id from a source's own identifier.

        Idempotent replays — a poller restarting and re-fetching the same
        bulletin — collapse onto one incident rather than multiplying.
        """
        return uuid.uuid5(INCIDENT_NAMESPACE, f"{source_type.value}:{external_id}")
