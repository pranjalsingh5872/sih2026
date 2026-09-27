"""India Meteorological Department normalizer.

IMD is the authoritative source, so two things matter more here than anywhere
else. First, station coordinates are used verbatim — an AWS/ARG gauge has a
surveyed position and must never be run through fuzzy text geocoding. Second,
the warning colour code is mapped to severity by IMD's own published scale
rather than by keyword guessing.

Two payload shapes are handled: ``nowcast``/``warning`` bulletins and
``observation`` records from automatic weather stations.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.core.errors import NormalizationError
from app.geo.geocoder import GeoResolutionInput
from app.normalization.base import BaseNormalizer, ParsedFields
from app.normalization.text import classify_by_keywords, clean_text, truncate
from app.schemas.enums import (
    GEO_METHOD_CONFIDENCE,
    GeoMethod,
    HazardCategory,
    IMD_COLOUR_TO_SEVERITY,
    SeverityHint,
    SourceType,
)
from app.schemas.incident import GeoContext, GeoPoint, Measurements
from app.schemas.raw import RawEnvelope

# IMD's own hazard vocabulary as it appears in bulletin payloads.
_IMD_HAZARD_MAP: dict[str, HazardCategory] = {
    "heavy rain": HazardCategory.HEAVY_RAINFALL,
    "heavy rainfall": HazardCategory.HEAVY_RAINFALL,
    "very heavy rainfall": HazardCategory.HEAVY_RAINFALL,
    "extremely heavy rainfall": HazardCategory.HEAVY_RAINFALL,
    "thunderstorm": HazardCategory.THUNDERSTORM,
    "thunderstorm with lightning": HazardCategory.THUNDERSTORM,
    "squall": HazardCategory.THUNDERSTORM,
    "lightning": HazardCategory.LIGHTNING,
    "hailstorm": HazardCategory.HAILSTORM,
    "heat wave": HazardCategory.HEATWAVE,
    "severe heat wave": HazardCategory.HEATWAVE,
    "cold wave": HazardCategory.COLDWAVE,
    "cold day": HazardCategory.COLDWAVE,
    "dense fog": HazardCategory.DENSE_FOG,
    "very dense fog": HazardCategory.DENSE_FOG,
    "dust storm": HazardCategory.DUST_STORM,
    "duststorm": HazardCategory.DUST_STORM,
    "cyclone": HazardCategory.CYCLONE,
    "cyclonic storm": HazardCategory.CYCLONE,
    "depression": HazardCategory.CYCLONE,
    "snowfall": HazardCategory.SNOWFALL,
    "flash flood": HazardCategory.FLASH_FLOOD,
}


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse the several timestamp shapes IMD payloads use."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        for parser in (
            lambda t: datetime.fromisoformat(t),
            lambda t: datetime.strptime(t, "%Y-%m-%d %H:%M:%S"),
            lambda t: datetime.strptime(t, "%d-%m-%Y %H:%M"),
            lambda t: datetime.strptime(t, "%Y%m%d%H%M"),
        ):
            try:
                parsed = parser(text)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
    return None


def _as_float(value: Any, lo: float, hi: float) -> float | None:
    """Coerce and range-check a numeric field.

    Out-of-range readings are dropped rather than clamped: a rain gauge
    reporting 9999 mm is a sentinel for 'no data', and clamping it to 2000 mm
    would manufacture a catastrophe.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return number if lo <= number <= hi else None


class IMDNormalizer(BaseNormalizer):
    source_type = SourceType.IMD
    source_name = "imd"

    def parse(self, envelope: RawEnvelope) -> ParsedFields:
        payload = envelope.payload
        record_type = str(payload.get("record_type") or "warning").lower()

        if record_type in ("observation", "aws", "arg", "station"):
            return self._parse_observation(envelope, payload)
        return self._parse_warning(envelope, payload)

    # ------------------------------------------------------------ warnings --
    def _parse_warning(self, envelope: RawEnvelope, payload: dict[str, Any]) -> ParsedFields:
        bulletin_id = payload.get("bulletin_id") or payload.get("id")
        if not bulletin_id:
            raise NormalizationError("IMD warning is missing bulletin_id")

        issued_at = (
            _parse_timestamp(payload.get("issue_time"))
            or _parse_timestamp(payload.get("valid_from"))
            or envelope.observed_at
            or envelope.fetched_at
        )

        headline = str(payload.get("headline") or payload.get("warning") or "").strip()
        description = str(payload.get("description") or "").strip()
        raw_text = " — ".join(part for part in (headline, description) if part) or None

        hazard_raw = str(payload.get("hazard_type") or headline or "").strip().casefold()
        category = HazardCategory.UNKNOWN
        for key, mapped in _IMD_HAZARD_MAP.items():
            if key in hazard_raw:
                category = mapped
                break
        if category is HazardCategory.UNKNOWN and raw_text:
            category, _ = classify_by_keywords(raw_text)

        colour = str(payload.get("colour_code") or payload.get("color_code") or "").upper()
        severity = IMD_COLOUR_TO_SEVERITY.get(colour, SeverityHint.UNKNOWN)

        geo_input = GeoResolutionInput(
            lat=_as_float(payload.get("lat") or payload.get("latitude"), -90, 90),
            lon=_as_float(payload.get("lon") or payload.get("longitude"), -180, 180),
            district=payload.get("district"),
            state=payload.get("state"),
            place_name=payload.get("area") or payload.get("district"),
            free_text=raw_text,
        )

        return ParsedFields(
            external_id=str(bulletin_id),
            observed_at=issued_at,
            raw_text=truncate(raw_text),
            geo_input=geo_input,
            category=category,
            severity=severity,
            measurements=Measurements(
                rainfall_mm=_as_float(payload.get("expected_rainfall_mm"), 0, 2000),
                rainfall_window_hours=_as_float(payload.get("valid_hours"), 0.1, 168),
                wind_speed_kmh=_as_float(payload.get("wind_speed_kmh"), 0, 500),
                wind_gust_kmh=_as_float(payload.get("wind_gust_kmh"), 0, 600),
            ),
            metadata={
                "record_type": "warning",
                "colour_code": colour or None,
                "valid_from": payload.get("valid_from"),
                "valid_until": payload.get("valid_until"),
                "issuing_office": payload.get("issuing_office"),
                "hazard_type_raw": payload.get("hazard_type"),
                "is_official": True,
            },
        )

    # --------------------------------------------------------- observations --
    def _parse_observation(self, envelope: RawEnvelope, payload: dict[str, Any]) -> ParsedFields:
        station_id = payload.get("station_id") or payload.get("station_code")
        if not station_id:
            raise NormalizationError("IMD observation is missing station_id")

        observed_at = (
            _parse_timestamp(payload.get("observation_time"))
            or _parse_timestamp(payload.get("timestamp"))
            or envelope.observed_at
            or envelope.fetched_at
        )

        lat = _as_float(payload.get("lat") or payload.get("latitude"), -90, 90)
        lon = _as_float(payload.get("lon") or payload.get("longitude"), -180, 180)

        # A surveyed station position is ground truth. Bypass the resolver.
        resolved_geo: GeoContext | None = None
        geo_input: GeoResolutionInput | None = None
        if lat is not None and lon is not None:
            try:
                resolved_geo = GeoContext(
                    point=GeoPoint(lat=lat, lon=lon),
                    method=GeoMethod.PROVIDER_STATION,
                    confidence=GEO_METHOD_CONFIDENCE[GeoMethod.PROVIDER_STATION],
                    place_label=payload.get("station_name"),
                    district=payload.get("district"),
                    state=payload.get("state"),
                    uncertainty_radius_km=0.01,
                )
            except ValueError:
                resolved_geo = None
        if resolved_geo is None:
            geo_input = GeoResolutionInput(
                place_name=payload.get("station_name"),
                district=payload.get("district"),
                state=payload.get("state"),
            )

        measurements = Measurements(
            rainfall_mm=_as_float(payload.get("rainfall_mm"), 0, 2000),
            rainfall_window_hours=_as_float(payload.get("rainfall_window_hours") or 24, 0.1, 168),
            temperature_c=_as_float(payload.get("temperature_c"), -60, 60),
            humidity_pct=_as_float(payload.get("humidity_pct"), 0, 100),
            wind_speed_kmh=_as_float(payload.get("wind_speed_kmh"), 0, 500),
            wind_gust_kmh=_as_float(payload.get("wind_gust_kmh"), 0, 600),
            wind_direction_deg=_as_float(payload.get("wind_direction_deg"), 0, 360),
            pressure_hpa=_as_float(payload.get("pressure_hpa"), 800, 1100),
            visibility_m=_as_float(payload.get("visibility_m"), 0, 100_000),
        )

        station_name = payload.get("station_name") or station_id
        summary = self._observation_summary(station_name, measurements)
        category = self._category_from_measurements(measurements)

        return ParsedFields(
            external_id=f"obs-{station_id}-{int(observed_at.timestamp())}",
            observed_at=observed_at,
            raw_text=truncate(summary),
            geo_input=geo_input,
            resolved_geo=resolved_geo,
            category=category,
            severity=SeverityHint.UNKNOWN,  # a gauge reading claims no severity
            measurements=measurements,
            metadata={
                "record_type": "observation",
                "station_id": str(station_id),
                "station_name": station_name,
                "station_type": payload.get("station_type") or "AWS",
                "is_official": True,
            },
        )

    # --------------------------------------------------------------- helpers --
    @staticmethod
    def _observation_summary(station_name: str, m: Measurements) -> str:
        """Render a gauge reading as text so it embeds alongside human reports."""
        parts: list[str] = [f"Automatic weather station {station_name}"]
        if m.rainfall_mm is not None:
            window = int(m.rainfall_window_hours or 24)
            parts.append(f"recorded {m.rainfall_mm:.1f} mm rainfall over {window}h")
        if m.temperature_c is not None:
            parts.append(f"temperature {m.temperature_c:.1f}C")
        if m.wind_gust_kmh is not None:
            parts.append(f"wind gusting to {m.wind_gust_kmh:.0f} km/h")
        if m.visibility_m is not None and m.visibility_m < 1000:
            parts.append(f"visibility {m.visibility_m:.0f} m")
        return ", ".join(parts) + "."

    @staticmethod
    def _category_from_measurements(m: Measurements) -> HazardCategory:
        """Tag an observation using IMD's published quantitative thresholds."""
        window = m.rainfall_window_hours or 24.0
        if m.rainfall_mm is not None and window >= 12:
            # IMD: 64.5-115.5 mm/24h is 'heavy'; above that, heavier still.
            if m.rainfall_mm >= 64.5:
                return HazardCategory.HEAVY_RAINFALL
        elif m.rainfall_mm is not None and m.rainfall_mm >= 50 and window <= 3:
            return HazardCategory.HEAVY_RAINFALL

        if m.temperature_c is not None and m.temperature_c >= 45.0:
            return HazardCategory.HEATWAVE
        if m.temperature_c is not None and m.temperature_c <= 4.0:
            return HazardCategory.COLDWAVE
        if m.visibility_m is not None and m.visibility_m < 200:
            return HazardCategory.DENSE_FOG
        if m.wind_gust_kmh is not None and m.wind_gust_kmh >= 62.0:
            return HazardCategory.THUNDERSTORM
        return HazardCategory.UNKNOWN

    def build_normalized_text(self, parsed: ParsedFields) -> str | None:
        return truncate(clean_text(parsed.raw_text or ""), 8192) or None
