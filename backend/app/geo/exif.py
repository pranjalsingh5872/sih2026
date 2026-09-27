"""EXIF extraction from citizen-submitted photos.

When someone photographs a flooded underpass, the phone usually stamps a GPS
fix into the file even if the app never asked for location permission at the
API level. That fix is one of the strongest location signals the platform gets,
so it is worth digging out — and worth handling defensively, because EXIF in
the wild is routinely truncated, byte-swapped or absent.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from dataclasses import dataclass

from app.core.logging import get_logger

logger = get_logger(__name__)

try:  # Pillow is a hard requirement in the image, but never crash without it.
    from PIL import ExifTags, Image

    _GPSTAGS = {v: k for k, v in ExifTags.GPSTAGS.items()}
    _PIL_AVAILABLE = True
except Exception:  # pragma: no cover - defensive import
    Image = None  # type: ignore[assignment]
    ExifTags = None  # type: ignore[assignment]
    _GPSTAGS = {}
    _PIL_AVAILABLE = False
    logger.warning("Pillow unavailable; EXIF GPS extraction disabled")

# EXIF tag ids used below, so the code does not depend on ExifTags name tables.
_TAG_GPS_INFO = 0x8825
_TAG_DATETIME_ORIGINAL = 0x9003
_TAG_DATETIME = 0x0132
_GPS_LAT_REF, _GPS_LAT = 1, 2
_GPS_LON_REF, _GPS_LON = 3, 4
_GPS_DATE, _GPS_TIME = 29, 7


@dataclass(frozen=True, slots=True)
class ExifResult:
    """What could be recovered from a photo's metadata."""

    lat: float | None = None
    lon: float | None = None
    captured_at: datetime | None = None
    camera_make: str | None = None
    width: int | None = None
    height: int | None = None
    has_exif: bool = False

    @property
    def has_gps(self) -> bool:
        return self.lat is not None and self.lon is not None


def _to_float(value: object) -> float | None:
    """Coerce an EXIF rational/int/tuple into a float."""
    try:
        if isinstance(value, (int, float)):
            return float(value)
        # Pillow returns IFDRational, which supports numerator/denominator.
        num = getattr(value, "numerator", None)
        den = getattr(value, "denominator", None)
        if num is not None and den:
            return float(num) / float(den)
        if isinstance(value, (tuple, list)) and len(value) == 2 and value[1]:
            return float(value[0]) / float(value[1])
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    return None


def _dms_to_decimal(dms: object, ref: object) -> float | None:
    """Convert degrees/minutes/seconds plus a hemisphere ref to signed decimal."""
    if not isinstance(dms, (tuple, list)) or len(dms) != 3:
        return None

    parts = [_to_float(component) for component in dms]
    if any(p is None for p in parts):
        return None

    degrees, minutes, seconds = parts  # type: ignore[misc]
    decimal = degrees + minutes / 60.0 + seconds / 3600.0

    hemisphere = str(ref).strip().upper() if ref else ""
    if hemisphere in ("S", "W"):
        decimal = -decimal

    return decimal if -180.0 <= decimal <= 180.0 else None


def _parse_exif_datetime(raw: object, gps_date: object = None) -> datetime | None:
    """Parse EXIF's ``YYYY:MM:DD HH:MM:SS`` format.

    EXIF timestamps are local-time-without-offset, which is genuinely ambiguous.
    We attach UTC so the value is usable, and the caller treats it as a weak
    hint — never as the authoritative ``observed_at``.
    """
    if not raw:
        return None
    try:
        text = str(raw).strip()
        if gps_date:
            # GPSDateStamp is genuinely UTC when present; prefer it.
            text = f"{str(gps_date).strip()} {text.split(' ')[-1]}"
        parsed = datetime.strptime(text[:19], "%Y:%m:%d %H:%M:%S")
        return parsed.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, IndexError):
        return None


def extract_exif(image_bytes: bytes) -> ExifResult:
    """Pull GPS and capture time out of image bytes.

    Never raises: a corrupt or metadata-free image yields an empty result and
    the geo-resolution chain moves on to the next strategy.
    """
    if not _PIL_AVAILABLE or not image_bytes:
        return ExifResult()

    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            width, height = img.size
            camera_make = None
            try:
                exif = img.getexif()
            except Exception:
                exif = None

            if not exif:
                return ExifResult(width=width, height=height, has_exif=False)

            make = exif.get(0x010F)
            if make:
                camera_make = str(make).strip()[:64]

            captured_at = _parse_exif_datetime(
                exif.get(_TAG_DATETIME_ORIGINAL) or exif.get(_TAG_DATETIME)
            )

            lat = lon = None
            try:
                gps = exif.get_ifd(_TAG_GPS_INFO)
            except Exception:
                gps = None

            if gps:
                lat = _dms_to_decimal(gps.get(_GPS_LAT), gps.get(_GPS_LAT_REF))
                lon = _dms_to_decimal(gps.get(_GPS_LON), gps.get(_GPS_LON_REF))
                if lat is not None and abs(lat) > 90.0:
                    lat = None
                # Null island again — a zeroed GPS block, not a location.
                if lat is not None and lon is not None and abs(lat) < 1e-9 and abs(lon) < 1e-9:
                    lat = lon = None
                gps_time = _parse_exif_datetime(gps.get(_GPS_TIME), gps.get(_GPS_DATE))
                captured_at = gps_time or captured_at

            return ExifResult(
                lat=lat,
                lon=lon,
                captured_at=captured_at,
                camera_make=camera_make,
                width=width,
                height=height,
                has_exif=True,
            )

    except Exception as exc:  # Pillow raises a wide variety on malformed input.
        logger.debug("EXIF extraction failed", extra={"error": str(exc)})
        return ExifResult()
