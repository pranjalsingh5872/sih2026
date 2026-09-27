"""OpenWeather normalizer.

OpenWeather reports Kelvin, metres per second and a nested ``rain.1h`` block.
All of it is converted here into the canonical :class:`Measurements` units, so
that Phase 2's meteorological-alignment check can compare an OpenWeather
reading against an IMD gauge without either side knowing the other exists.

Handles both the current-weather response and the ``alerts`` array from One
Call, which is where the genuinely actionable content lives.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.core.errors import NormalizationError
from app.geo.geocoder import GeoResolutionInput
from app.normalization.base import BaseNormalizer, ParsedFields
from app.normalization.imd import _as_float, _parse_timestamp
from app.normalization.text import classify_by_keywords, clean_text, severity_from_keywords, truncate
from app.schemas.enums import (
    GEO_METHOD_CONFIDENCE,
    GeoMethod,
    HazardCategory,
    SeverityHint,
    SourceType,
)
from app.schemas.incident import GeoContext, GeoPoint, Measurements
from app.schemas.raw import RawEnvelope

# OpenWeather condition-code bands (https://openweathermap.org/weather-conditions).
_CONDITION_BANDS: tuple[tuple[int, int, HazardCategory], ...] = (
    (200, 232, HazardCategory.THUNDERSTORM),
    (300, 321, HazardCategory.HEAVY_RAINFALL),
    (500, 504, HazardCategory.HEAVY_RAINFALL),
    (511, 531, HazardCategory.HEAVY_RAINFALL),
    (600, 622, HazardCategory.SNOWFALL),
    (701, 701, HazardCategory.DENSE_FOG),
    (711, 731, HazardCategory.DUST_STORM),
    (741, 741, HazardCategory.DENSE_FOG),
    (751, 761, HazardCategory.DUST_STORM),
    (762, 762, HazardCategory.DUST_STORM),
    (771, 781, HazardCategory.THUNDERSTORM),
)

KELVIN_OFFSET = 273.15
MPS_TO_KMH = 3.6


def _kelvin_to_c(value: Any) -> float | None:
    kelvin = _as_float(value, 150.0, 350.0)
    return round(kelvin - KELVIN_OFFSET, 2) if kelvin is not None else None


def _mps_to_kmh(value: Any) -> float | None:
    mps = _as_float(value, 0.0, 150.0)
    return round(mps * MPS_TO_KMH, 2) if mps is not None else None


class OpenWeatherNormalizer(BaseNormalizer):
    source_type = SourceType.OPENWEATHER
    source_name = "openweather"

    def parse(self, envelope: RawEnvelope) -> ParsedFields:
        payload = envelope.payload
        if payload.get("record_type") == "alert" or "event" in payload:
            return self._parse_alert(envelope, payload)
        return self._parse_current(envelope, payload)

    # -------------------------------------------------------------- alerts --
    def _parse_alert(self, envelope: RawEnvelope, payload: dict[str, Any]) -> ParsedFields:
        event = str(payload.get("event") or "").strip()
        if not event:
            raise NormalizationError("OpenWeather alert has no event name")

        start = _parse_timestamp(payload.get("start")) or envelope.fetched_at
        description = str(payload.get("description") or "").strip()
        raw_text = f"{event}: {description}" if description else event

        category, _ = classify_by_keywords(raw_text)
        severity = severity_from_keywords(raw_text)
        if severity is SeverityHint.UNKNOWN:
            # An issued alert is at minimum 'moderate' — it was worth issuing.
            severity = SeverityHint.MODERATE

        lat = _as_float(payload.get("lat"), -90, 90)
        lon = _as_float(payload.get("lon"), -180, 180)

        return ParsedFields(
            external_id=f"alert-{payload.get('sender_name', 'ow')}-{event}-{int(start.timestamp())}",
            observed_at=start,
            raw_text=truncate(raw_text),
            geo_input=GeoResolutionInput(
                lat=lat, lon=lon,
                place_name=payload.get("area") or payload.get("city"),
                free_text=raw_text,
            ),
            category=category,
            severity=severity,
            metadata={
                "record_type": "alert",
                "sender_name": payload.get("sender_name"),
                "tags": payload.get("tags") or [],
                "start": payload.get("start"),
                "end": payload.get("end"),
                "is_official": False,
            },
        )

    # ------------------------------------------------------------- current --
    def _parse_current(self, envelope: RawEnvelope, payload: dict[str, Any]) -> ParsedFields:
        coord = payload.get("coord") or {}
        lat = _as_float(coord.get("lat") or payload.get("lat"), -90, 90)
        lon = _as_float(coord.get("lon") or payload.get("lon"), -180, 180)
        city_id = payload.get("id") or payload.get("city_id")
        city_name = payload.get("name") or payload.get("city")

        if city_id is None and (lat is None or lon is None):
            raise NormalizationError("OpenWeather current payload has neither id nor coordinates")

        observed_at = (
            _parse_timestamp(payload.get("dt"))
            or envelope.observed_at
            or envelope.fetched_at
        )

        main = payload.get("main") or {}
        wind = payload.get("wind") or {}
        rain = payload.get("rain") or {}
        weather_list = payload.get("weather") or []
        condition = weather_list[0] if weather_list else {}

        rainfall_mm = _as_float(rain.get("1h"), 0, 2000)
        window_hours = 1.0
        if rainfall_mm is None:
            rainfall_mm = _as_float(rain.get("3h"), 0, 2000)
            window_hours = 3.0 if rainfall_mm is not None else 1.0

        measurements = Measurements(
            rainfall_mm=rainfall_mm,
            rainfall_window_hours=window_hours if rainfall_mm is not None else None,
            temperature_c=_kelvin_to_c(main.get("temp")),
            feels_like_c=_kelvin_to_c(main.get("feels_like")),
            humidity_pct=_as_float(main.get("humidity"), 0, 100),
            pressure_hpa=_as_float(main.get("pressure"), 800, 1100),
            wind_speed_kmh=_mps_to_kmh(wind.get("speed")),
            wind_gust_kmh=_mps_to_kmh(wind.get("gust")),
            wind_direction_deg=_as_float(wind.get("deg"), 0, 360),
            visibility_m=_as_float(payload.get("visibility"), 0, 100_000),
        )

        # Station coordinates from the provider are exact; skip the resolver.
        resolved_geo: GeoContext | None = None
        geo_input: GeoResolutionInput | None = None
        if lat is not None and lon is not None:
            try:
                resolved_geo = GeoContext(
                    point=GeoPoint(lat=lat, lon=lon),
                    method=GeoMethod.PROVIDER_STATION,
                    confidence=GEO_METHOD_CONFIDENCE[GeoMethod.PROVIDER_STATION],
                    place_label=city_name,
                    uncertainty_radius_km=5.0,  # city-level grid cell
                )
            except ValueError:
                resolved_geo = None
        if resolved_geo is None:
            geo_input = GeoResolutionInput(place_name=city_name)

        category = self._category_from_condition(condition, measurements)
        description = str(condition.get("description") or "").strip()
        summary = (
            f"OpenWeather observation for {city_name or 'unknown location'}: "
            f"{description or 'conditions reported'}"
        )
        if measurements.rainfall_mm:
            summary += f", {measurements.rainfall_mm:.1f} mm in {int(window_hours)}h"
        if measurements.temperature_c is not None:
            summary += f", {measurements.temperature_c:.1f}C"

        return ParsedFields(
            external_id=f"ow-{city_id or f'{lat:.3f}_{lon:.3f}'}-{int(observed_at.timestamp())}",
            observed_at=observed_at,
            raw_text=truncate(summary),
            geo_input=geo_input,
            resolved_geo=resolved_geo,
            category=category,
            severity=SeverityHint.UNKNOWN,
            measurements=measurements,
            metadata={
                "record_type": "current",
                "city_id": city_id,
                "city_name": city_name,
                "condition_id": condition.get("id"),
                "condition_main": condition.get("main"),
                "condition_description": description or None,
                "cloud_pct": (payload.get("clouds") or {}).get("all"),
                "is_official": False,
            },
        )

    # -------------------------------------------------------------- helpers --
    @staticmethod
    def _category_from_condition(
        condition: dict[str, Any], measurements: Measurements
    ) -> HazardCategory:
        code = condition.get("id")
        if isinstance(code, int):
            for low, high, category in _CONDITION_BANDS:
                if low <= code <= high:
                    # Only escalate to HEAVY_RAINFALL when the measurement
                    # actually supports it; light drizzle shares the band.
                    if category is HazardCategory.HEAVY_RAINFALL:
                        rain = measurements.rainfall_mm or 0.0
                        window = measurements.rainfall_window_hours or 1.0
                        if rain / max(window, 0.1) < 7.5:
                            return HazardCategory.UNKNOWN
                    return category

        description = str(condition.get("description") or "")
        category, _ = classify_by_keywords(description)
        return category

    def build_normalized_text(self, parsed: ParsedFields) -> str | None:
        return truncate(clean_text(parsed.raw_text or ""), 8192) or None
