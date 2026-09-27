"""Normalizer contract.

Every source implements :class:`BaseNormalizer`. The template method in
:meth:`normalize` owns the parts that must be identical across sources —
id derivation, geo resolution, trace stamping, timestamp defaulting — so a new
provider only has to answer the questions that are actually provider-specific.
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from app.core.errors import NormalizationError
from app.core.logging import get_logger
from app.geo.geocoder import GeoResolutionInput, GeoResolver
from app.schemas.enums import (
    HazardCategory,
    PipelineStage,
    SeverityHint,
    SourceType,
)
from app.schemas.incident import (
    AuthorRef,
    GeoContext,
    MediaAsset,
    Measurements,
    NormalizedIncident,
)
from app.schemas.raw import RawEnvelope

logger = get_logger(__name__)


@dataclass(slots=True)
class ParsedFields:
    """What a source-specific parser is responsible for producing."""

    external_id: str
    observed_at: datetime
    raw_text: str | None = None
    geo_input: GeoResolutionInput | None = None
    # Set when the source already knows its exact coordinates (an AWS station,
    # for instance) and geo resolution should be bypassed entirely.
    resolved_geo: GeoContext | None = None
    category: HazardCategory = HazardCategory.UNKNOWN
    severity: SeverityHint = SeverityHint.UNKNOWN
    measurements: Measurements | None = None
    media: list[MediaAsset] | None = None
    author: AuthorRef | None = None
    metadata: dict[str, object] | None = None


class BaseNormalizer(ABC):
    """Maps one provider's payload onto :class:`NormalizedIncident`."""

    #: Which source this normalizer claims.
    source_type: SourceType
    #: Concrete feed label written into ``source_name``.
    source_name: str = "unknown"

    def __init__(self, geo_resolver: GeoResolver) -> None:
        self._geo = geo_resolver

    # ---------------------------------------------------- to be implemented --
    @abstractmethod
    def parse(self, envelope: RawEnvelope) -> ParsedFields:
        """Extract provider-specific fields.

        Raise :class:`NormalizationError` for a payload that cannot be mapped —
        the consumer will dead-letter it rather than retrying forever.
        """

    # ------------------------------------------------------ template method --
    async def normalize(self, envelope: RawEnvelope) -> NormalizedIncident:
        """Produce a validated incident from a raw envelope."""
        try:
            parsed = self.parse(envelope)
        except NormalizationError:
            raise
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise NormalizationError(
                f"{type(self).__name__} could not parse payload: {exc}",
                source_type=self.source_type.value,
                envelope_id=str(envelope.envelope_id),
            ) from exc

        geo = parsed.resolved_geo
        if geo is None:
            geo = await self._geo.resolve(parsed.geo_input or GeoResolutionInput())

        incident_id = (
            NormalizedIncident.derive_id(self.source_type, parsed.external_id)
            if parsed.external_id
            else uuid.uuid4()
        )

        try:
            incident = NormalizedIncident(
                incident_id=incident_id,
                correlation_id=envelope.correlation_id,
                source_type=self.source_type,
                source_name=envelope.source_name or self.source_name,
                external_id=parsed.external_id,
                observed_at=parsed.observed_at,
                ingested_at=envelope.fetched_at,
                geo=geo,
                raw_text=parsed.raw_text,
                normalized_text=self.build_normalized_text(parsed),
                language=self.detect_language(parsed),
                reported_category=parsed.category,
                severity_hint=parsed.severity,
                measurements=parsed.measurements or Measurements(),
                media=parsed.media or [],
                author=parsed.author,
                metadata=dict(parsed.metadata or {}),
            )
        except ValueError as exc:
            # Schema validation rejected it — a clock-skewed timestamp, an
            # over-length field. Permanent by construction.
            raise NormalizationError(
                f"Incident failed schema validation: {exc}",
                source_type=self.source_type.value,
                external_id=parsed.external_id,
            ) from exc

        incident.add_trace(
            PipelineStage.NORMALIZATION,
            component=type(self).__name__,
            note=f"geo={geo.method.value} conf={geo.confidence:.2f}",
        )
        return incident

    # -------------------------------------------------------------- hooks ---
    def build_normalized_text(self, parsed: ParsedFields) -> str | None:
        """Override to apply source-specific cleaning. Default: pass through."""
        return parsed.raw_text

    def detect_language(self, parsed: ParsedFields) -> str | None:
        """Override for sources with user-generated text. Default: English."""
        return "en"

    def __repr__(self) -> str:  # pragma: no cover
        return f"<{type(self).__name__} source={self.source_type.value}>"
