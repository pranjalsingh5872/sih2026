"""Pre-normalization contracts.

``RawEnvelope`` is what sits on ``raw-weather-stream``: the provider's payload,
untouched, wrapped in just enough metadata to route and audit it. Keeping the
original bytes means a normalizer bug is replayable rather than a data loss.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.schemas.enums import HazardCategory, PipelineStage, SourceType
from app.schemas.incident import GeoPoint, utcnow


class RawEnvelope(BaseModel):
    """A provider payload as received, plus routing metadata."""

    model_config = ConfigDict(extra="forbid")

    envelope_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    correlation_id: str | None = Field(default=None, max_length=64)

    source_type: SourceType
    source_name: str = Field(max_length=128)
    external_id: str | None = Field(default=None, max_length=256)

    fetched_at: datetime = Field(default_factory=utcnow)
    # Provider-declared event time when available; normalizers fall back to
    # whatever is inside the payload, then to ``fetched_at``.
    observed_at: datetime | None = None

    payload: dict[str, Any] = Field(description="Verbatim provider payload")
    ingest_stage: PipelineStage = PipelineStage.PROVIDER_POLL
    producer_component: str = Field(default="unknown", max_length=128)

    @field_validator("fetched_at", "observed_at")
    @classmethod
    def _require_tz(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("Envelope timestamps must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _payload_not_empty(self) -> Self:
        if not self.payload:
            raise ValueError("RawEnvelope.payload must not be empty")
        return self

    def partition_key(self) -> str:
        """Kafka key.

        Keyed by source so one provider's ordering is preserved within a
        partition and a flood of simulated social traffic cannot starve the
        IMD feed's ordering guarantees.
        """
        return f"{self.source_type.value}:{self.external_id or self.envelope_id}"


class DeadLetter(BaseModel):
    """A message the pipeline refused, preserved for operator review."""

    model_config = ConfigDict(extra="forbid")

    dead_letter_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    occurred_at: datetime = Field(default_factory=utcnow)
    correlation_id: str | None = None
    stage: PipelineStage
    source_type: SourceType | None = None
    error_code: str = Field(max_length=64)
    error_type: str = Field(max_length=128)
    error_detail: str = Field(max_length=4096)
    retry_count: int = Field(default=0, ge=0)
    payload: dict[str, Any] = Field(default_factory=dict)


# ===========================================================================
# Citizen reporting API
# ===========================================================================
class CitizenReportRequest(BaseModel):
    """Body of ``POST /api/v1/incidents/report``.

    Coordinates are optional on purpose. A phone with GPS disabled, a photo
    stripped of EXIF by a messaging app, an SMS-relayed report from a low-end
    handset — all are ordinary during a disaster, and all still carry signal.
    The geo-resolution chain recovers what it can and marks the rest
    ``UNRESOLVED`` for manual triage rather than discarding it.
    """

    model_config = ConfigDict(extra="forbid")

    description: str = Field(
        min_length=3,
        max_length=4000,
        description="Free-text report, any Indian language",
    )
    lat: float | None = Field(default=None, ge=-90.0, le=90.0)
    lon: float | None = Field(default=None, ge=-180.0, le=180.0)
    location_accuracy_m: float | None = Field(default=None, ge=0.0, le=100_000.0)

    place_name: str | None = Field(default=None, max_length=256)
    district: str | None = Field(default=None, max_length=128)
    state: str | None = Field(default=None, max_length=128)
    pincode: str | None = Field(default=None, pattern=r"^\d{6}$")

    observed_at: datetime | None = Field(
        default=None, description="Defaults to server receipt time"
    )
    category_hint: HazardCategory | None = None

    reporter_identifier: str | None = Field(
        default=None,
        max_length=256,
        description="Device/session/phone id. Hashed immediately; never stored raw.",
    )
    reporter_handle: str | None = Field(default=None, max_length=128)
    water_level_cm: float | None = Field(default=None, ge=0.0, le=2000.0)

    client_app_version: str | None = Field(default=None, max_length=32)

    @model_validator(mode="after")
    def _coordinates_are_paired(self) -> Self:
        if (self.lat is None) != (self.lon is None):
            raise ValueError("lat and lon must be supplied together or not at all")
        return self

    @field_validator("observed_at")
    @classmethod
    def _tz_and_not_future(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        aware = aware.astimezone(timezone.utc)
        if (aware - utcnow()).total_seconds() > 7200:
            raise ValueError("observed_at is more than 2 hours in the future")
        return aware

    @field_validator("description")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("description must contain non-whitespace text")
        return value.strip()

    def coordinates(self) -> GeoPoint | None:
        if self.lat is None or self.lon is None:
            return None
        try:
            return GeoPoint(lat=self.lat, lon=self.lon)
        except ValueError:
            # Null island or otherwise rejected — treat as no fix supplied.
            return None


class CitizenReportResponse(BaseModel):
    """Acknowledgement returned to the reporting client.

    Note what this does *not* say: it never tells the citizen their report is
    "verified". It is an acknowledgement of receipt. Verification is a
    downstream, partly human decision.
    """

    model_config = ConfigDict(extra="forbid")

    incident_id: uuid.UUID
    accepted: bool
    duplicate_of_submission: bool = Field(
        default=False,
        description="True when this exact payload was already received recently",
    )
    location_resolved: bool
    geo_method: str
    place_label: str | None = None
    queued_to_topic: str
    received_at: datetime = Field(default_factory=utcnow)
    message: str = "Report received and queued for verification."
