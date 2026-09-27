"""Enumerated domains for the ingestion pipeline.

These mirror the PostgreSQL enums in ``infra/postgres/init/001_schema.sql``.
If you add a member here, add it there too — the test suite asserts the two
stay in step.
"""

from __future__ import annotations

from enum import StrEnum


class SourceType(StrEnum):
    """Where a report entered the platform from."""

    IMD = "IMD"                  # India Meteorological Department (authoritative)
    OPENWEATHER = "OPENWEATHER"  # third-party commercial API
    CITIZEN = "CITIZEN"          # first-party mobile/web submission
    SOCIAL = "SOCIAL"            # scraped social/RSS feed
    SENSOR = "SENSOR"            # AWS/ARG and partner telemetry


class GeoMethod(StrEnum):
    """How a report's coordinates were established.

    Phase 2 credibility scoring weights this: a GPS fix from a photo's EXIF is
    materially stronger evidence than a district centroid guessed from text.
    """

    GPS_PAYLOAD = "GPS_PAYLOAD"                  # device-supplied lat/lon
    EXIF_GPS = "EXIF_GPS"                        # extracted from uploaded photo
    PROVIDER_STATION = "PROVIDER_STATION"        # known IMD/AWS station coords
    PLACE_NAME_LOOKUP = "PLACE_NAME_LOOKUP"      # structured place field
    GAZETTEER_TEXT_MATCH = "GAZETTEER_TEXT_MATCH"  # place name mined from text
    REMOTE_GEOCODER = "REMOTE_GEOCODER"          # Nominatim / external service
    ADMIN_CENTROID = "ADMIN_CENTROID"            # district/state centroid fallback
    UNRESOLVED = "UNRESOLVED"                    # no location could be derived


# Prior confidence per resolution method. Used as the `geo_confidence` seed and
# consumed again by the Phase 2 credibility model.
GEO_METHOD_CONFIDENCE: dict[GeoMethod, float] = {
    GeoMethod.GPS_PAYLOAD: 0.97,
    GeoMethod.EXIF_GPS: 0.95,
    GeoMethod.PROVIDER_STATION: 0.99,
    GeoMethod.PLACE_NAME_LOOKUP: 0.75,
    GeoMethod.GAZETTEER_TEXT_MATCH: 0.60,
    GeoMethod.REMOTE_GEOCODER: 0.70,
    GeoMethod.ADMIN_CENTROID: 0.35,
    GeoMethod.UNRESOLVED: 0.0,
}


class HazardCategory(StrEnum):
    """Hazard taxonomy.

    Phase 1 assigns these only from explicit provider fields or high-confidence
    keyword rules. The zero-shot classifier in Phase 2 refines everything left
    as ``UNKNOWN``.
    """

    HEAVY_RAINFALL = "HEAVY_RAINFALL"
    FLASH_FLOOD = "FLASH_FLOOD"
    URBAN_FLOODING = "URBAN_FLOODING"
    THUNDERSTORM = "THUNDERSTORM"
    LIGHTNING = "LIGHTNING"
    CYCLONE = "CYCLONE"
    HEATWAVE = "HEATWAVE"
    COLDWAVE = "COLDWAVE"
    DENSE_FOG = "DENSE_FOG"
    DUST_STORM = "DUST_STORM"
    HAILSTORM = "HAILSTORM"
    LANDSLIDE = "LANDSLIDE"
    SNOWFALL = "SNOWFALL"
    UNKNOWN = "UNKNOWN"


class SeverityHint(StrEnum):
    """Severity as *claimed* by the source, not as adjudicated by the platform.

    Deliberately distinct from the fused severity Phase 3 computes for an
    event — a single alarmed tweet must not set an event to CRITICAL.
    """

    INFO = "INFO"
    MINOR = "MINOR"
    MODERATE = "MODERATE"
    SEVERE = "SEVERE"
    EXTREME = "EXTREME"
    UNKNOWN = "UNKNOWN"


# IMD publishes warnings as a colour code; this is the documented mapping.
IMD_COLOUR_TO_SEVERITY: dict[str, SeverityHint] = {
    "GREEN": SeverityHint.INFO,
    "YELLOW": SeverityHint.MINOR,
    "AMBER": SeverityHint.MODERATE,
    "ORANGE": SeverityHint.SEVERE,
    "RED": SeverityHint.EXTREME,
}


class MediaKind(StrEnum):
    IMAGE = "IMAGE"
    VIDEO = "VIDEO"
    AUDIO = "AUDIO"
    UNKNOWN = "UNKNOWN"


class PipelineStage(StrEnum):
    """Stage labels stamped onto the processing trace and dead letters."""

    INGEST_API = "INGEST_API"
    PROVIDER_POLL = "PROVIDER_POLL"
    SOCIAL_SIMULATOR = "SOCIAL_SIMULATOR"
    NORMALIZATION = "NORMALIZATION"
    GEO_RESOLUTION = "GEO_RESOLUTION"
    PUBLISH = "PUBLISH"
