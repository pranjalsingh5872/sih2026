"""Normalizer registry.

Adding a sixth source means writing one :class:`BaseNormalizer` subclass and
adding one line here. Nothing in the worker, the API or the schemas changes.
"""

from __future__ import annotations

from app.core.errors import UnsupportedSourceError
from app.core.logging import get_logger
from app.geo.geocoder import GeoResolver, get_geo_resolver
from app.normalization.base import BaseNormalizer
from app.normalization.citizen import CitizenNormalizer
from app.normalization.imd import IMDNormalizer
from app.normalization.openweather import OpenWeatherNormalizer
from app.normalization.sensor import SensorNormalizer
from app.normalization.social import SocialNormalizer
from app.schemas.enums import SourceType
from app.schemas.incident import NormalizedIncident
from app.schemas.raw import RawEnvelope

logger = get_logger(__name__)

_NORMALIZER_CLASSES: dict[SourceType, type[BaseNormalizer]] = {
    SourceType.IMD: IMDNormalizer,
    SourceType.OPENWEATHER: OpenWeatherNormalizer,
    SourceType.CITIZEN: CitizenNormalizer,
    SourceType.SOCIAL: SocialNormalizer,
    SourceType.SENSOR: SensorNormalizer,
}


class NormalizerRegistry:
    """Owns one normalizer instance per source type."""

    def __init__(self, geo_resolver: GeoResolver | None = None) -> None:
        resolver = geo_resolver or get_geo_resolver()
        self._normalizers: dict[SourceType, BaseNormalizer] = {
            source: cls(resolver) for source, cls in _NORMALIZER_CLASSES.items()
        }
        logger.info(
            "Normalizer registry ready",
            extra={"sources": sorted(s.value for s in self._normalizers)},
        )

    def get(self, source_type: SourceType | str) -> BaseNormalizer:
        try:
            resolved = SourceType(source_type) if isinstance(source_type, str) else source_type
        except ValueError:
            raise UnsupportedSourceError(
                f"No normalizer registered for {source_type}",
                source_type=str(source_type),
            ) from None

        normalizer = self._normalizers.get(resolved)
        if normalizer is None:
            raise UnsupportedSourceError(
                f"No normalizer registered for {resolved.value}",
                source_type=resolved.value,
            )
        return normalizer

    async def normalize(self, envelope: RawEnvelope) -> NormalizedIncident:
        """Route an envelope to its normalizer and return the incident."""
        return await self.get(envelope.source_type).normalize(envelope)

    @property
    def supported_sources(self) -> tuple[SourceType, ...]:
        return tuple(self._normalizers)


_registry: NormalizerRegistry | None = None


def get_registry() -> NormalizerRegistry:
    global _registry
    if _registry is None:
        _registry = NormalizerRegistry()
    return _registry
