"""Generic sensor telemetry normalizer.

Covers partner-operated hardware that is not IMD's own: state disaster
authority rain gauges, municipal river-level sensors, highway visibility
meters. The payloads are simple but wildly inconsistent between vendors, so
this normalizer works from a small alias table rather than fixed field names.
"""

from __future__ import annotations

from typing import Any, Iterable

from app.core.errors import NormalizationError
from app.geo.geocoder import GeoResolutionInput
from app.normalization.base import BaseNormalizer, ParsedFields
from app.normalization.imd import _as_float, _parse_timestamp
from app.normalization.text import truncate
from app.schemas.enums import (
    GEO_METHOD_CONFIDENCE,
    GeoMethod,
    HazardCategory,
    SeverityHint,
    SourceType,
)
from app.schemas.incident import GeoContext, GeoPoint, Measurements
from app.schemas.raw import RawEnvelope

# Vendor field name -> (canonical measurement, min, max)
_FIELD_ALIASES: dict[str, tuple[str, float, float]] = {
    "rain": ("rainfall_mm", 0, 2000), "rainfall": ("rainfall_mm", 0, 2000),
    "rainfall_mm": ("rainfall_mm", 0, 2000), "precip_mm": ("rainfall_mm", 0, 2000),
    "temp": ("temperature_c", -60, 60), "temp_c": ("temperature_c", -60, 60),
    "temperature": ("temperature_c", -60, 60), "temperature_c": ("temperature_c", -60, 60),
    "rh": ("humidity_pct", 0, 100), "humidity": ("humidity_pct", 0, 100),
    "humidity_pct": ("humidity_pct", 0, 100),
    "wind": ("wind_speed_kmh", 0, 500), "wind_kmh": ("wind_speed_kmh", 0, 500),
    "wind_speed_kmh": ("wind_speed_kmh", 0, 500),
    "gust": ("wind_gust_kmh", 0, 600), "wind_gust_kmh": ("wind_gust_kmh", 0, 600),
    "wind_dir": ("wind_direction_deg", 0, 360), "wind_direction_deg": ("wind_direction_deg", 0, 360),
    "pressure": ("pressure_hpa", 800, 1100), "pressure_hpa": ("pressure_hpa", 800, 1100),
    "visibility": ("visibility_m", 0, 100_000), "visibility_m": ("visibility_m", 0, 100_000),
    "water_level": ("water_level_cm", 0, 2000), "water_level_cm": ("water_level_cm", 0, 2000),
    "river_level_cm": ("water_level_cm", 0, 2000),
}


class SensorNormalizer(BaseNormalizer):
    source_type = SourceType.SENSOR
    source_name = "partner_sensor"

    def parse(self, envelope: RawEnvelope) -> ParsedFields:
        payload = envelope.payload

        sensor_id = (
            payload.get("sensor_id") or payload.get("device_id") or payload.get("station_id")
        )
        if not sensor_id:
            raise NormalizationError("Sensor payload is missing a device identifier")

        observed_at = (
            _parse_timestamp(
                payload.get("timestamp") or payload.get("observed_at") or payload.get("ts")
            )
            or envelope.observed_at
            or envelope.fetched_at
        )

        measurements = self._extract_measurements(payload)
        if not measurements.has_any():
            raise NormalizationError(
                "Sensor payload carried no recognisable measurement fields",
                sensor_id=str(sensor_id),
            )

        lat = _as_float(payload.get("lat") or payload.get("latitude"), -90, 90)
        lon = _as_float(payload.get("lon") or payload.get("longitude"), -180, 180)

        resolved_geo: GeoContext | None = None
        geo_input: GeoResolutionInput | None = None
        if lat is not None and lon is not None:
            try:
                resolved_geo = GeoContext(
                    point=GeoPoint(lat=lat, lon=lon),
                    method=GeoMethod.PROVIDER_STATION,
                    confidence=GEO_METHOD_CONFIDENCE[GeoMethod.PROVIDER_STATION],
                    place_label=payload.get("sensor_name") or str(sensor_id),
                    district=payload.get("district"),
                    state=payload.get("state"),
                    uncertainty_radius_km=0.01,
                )
            except ValueError:
                resolved_geo = None
        if resolved_geo is None:
            geo_input = GeoResolutionInput(
                place_name=payload.get("sensor_name"),
                district=payload.get("district"),
                state=payload.get("state"),
            )

        sensor_kind = str(payload.get("sensor_type") or "weather").lower()
        summary = self._summarise(sensor_id, sensor_kind, measurements)

        return ParsedFields(
            external_id=f"sensor-{sensor_id}-{int(observed_at.timestamp())}",
            observed_at=observed_at,
            raw_text=truncate(summary),
            geo_input=geo_input,
            resolved_geo=resolved_geo,
            category=self._category(sensor_kind, measurements),
            severity=SeverityHint.UNKNOWN,
            measurements=measurements,
            metadata={
                "record_type": "sensor_reading",
                "sensor_id": str(sensor_id),
                "sensor_type": sensor_kind,
                "operator": payload.get("operator") or payload.get("agency"),
                "battery_pct": payload.get("battery_pct"),
                # Vendors often flag their own reliability; carry it forward.
                "quality_flag": payload.get("quality_flag"),
                "is_official": bool(payload.get("is_government_operated", False)),
            },
        )

    # -------------------------------------------------------------- helpers --
    @staticmethod
    def _extract_measurements(payload: dict[str, Any]) -> Measurements:
        """Map vendor field names onto canonical measurements."""
        values: dict[str, float] = {}
        # Readings may sit at the top level or inside a nested block.
        sources: Iterable[dict[str, Any]] = (
            payload,
            payload.get("readings") or {},
            payload.get("data") or {},
        )
        for block in sources:
            if not isinstance(block, dict):
                continue
            for key, raw in block.items():
                alias = _FIELD_ALIASES.get(str(key).lower())
                if alias is None:
                    continue
                field, lo, hi = alias
                if field in values:
                    continue  # first source wins; top level is most specific
                parsed = _as_float(raw, lo, hi)
                if parsed is not None:
                    values[field] = parsed

        window = _as_float(payload.get("rainfall_window_hours"), 0.1, 168)
        if window is not None:
            values["rainfall_window_hours"] = window
        elif "rainfall_mm" in values:
            values["rainfall_window_hours"] = 1.0  # most gauges report hourly

        return Measurements(**values)

    @staticmethod
    def _category(sensor_kind: str, m: Measurements) -> HazardCategory:
        if sensor_kind in ("river", "water_level", "flood") and m.water_level_cm is not None:
            # Absolute level means nothing without the danger mark for that
            # gauge; Phase 3 joins against the station registry. Tagging it
            # URBAN_FLOODING here would be a guess dressed as a finding.
            return HazardCategory.UNKNOWN
        rate = (m.rainfall_mm or 0.0) / max(m.rainfall_window_hours or 1.0, 0.1)
        if rate >= 15.0:
            return HazardCategory.HEAVY_RAINFALL
        if m.visibility_m is not None and m.visibility_m < 200:
            return HazardCategory.DENSE_FOG
        if m.wind_gust_kmh is not None and m.wind_gust_kmh >= 62.0:
            return HazardCategory.THUNDERSTORM
        return HazardCategory.UNKNOWN

    @staticmethod
    def _summarise(sensor_id: Any, kind: str, m: Measurements) -> str:
        parts = [f"Sensor {sensor_id} ({kind})"]
        if m.rainfall_mm is not None:
            parts.append(f"rainfall {m.rainfall_mm:.1f} mm/{m.rainfall_window_hours or 1:.0f}h")
        if m.water_level_cm is not None:
            parts.append(f"water level {m.water_level_cm:.0f} cm")
        if m.temperature_c is not None:
            parts.append(f"temperature {m.temperature_c:.1f}C")
        if m.wind_gust_kmh is not None:
            parts.append(f"gust {m.wind_gust_kmh:.0f} km/h")
        if m.visibility_m is not None:
            parts.append(f"visibility {m.visibility_m:.0f} m")
        return ", ".join(parts) + "."
