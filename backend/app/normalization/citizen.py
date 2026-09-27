"""Citizen report normalizer.

The distinguishing concerns here are privacy and honest uncertainty.

*Privacy*: the ingest endpoint has already replaced any device or phone
identifier with a keyed digest. This normalizer asserts that — it will refuse
to emit an incident carrying something that looks like a raw phone number.

*Uncertainty*: a citizen report is an unverified claim. It carries a
``category_hint`` from the reporter and a ``severity_hint`` derived from their
wording, and both are explicitly labelled as claims. Nothing here decides
whether the report is true; that is Phase 2's job, with a human in the loop.
"""

from __future__ import annotations

import re
from typing import Any

from app.core.errors import NormalizationError
from app.geo.exif import ExifResult
from app.geo.geocoder import GeoResolutionInput
from app.normalization.base import BaseNormalizer, ParsedFields
from app.normalization.imd import _as_float, _parse_timestamp
from app.normalization.text import (
    classify_by_keywords,
    clean_text,
    detect_language,
    severity_from_keywords,
    truncate,
)
from app.schemas.enums import HazardCategory, MediaKind, SeverityHint, SourceType
from app.schemas.incident import AuthorRef, MediaAsset, Measurements
from app.schemas.raw import RawEnvelope

# Catches a raw identifier that slipped past the API layer's hashing.
_RAW_PHONE_RE = re.compile(r"^(\+?91[\-\s]?)?[6-9]\d{9}$")

_MEDIA_KIND_BY_PREFIX: dict[str, MediaKind] = {
    "image/": MediaKind.IMAGE,
    "video/": MediaKind.VIDEO,
    "audio/": MediaKind.AUDIO,
}


class CitizenNormalizer(BaseNormalizer):
    source_type = SourceType.CITIZEN
    source_name = "citizen_app"

    def parse(self, envelope: RawEnvelope) -> ParsedFields:
        payload = envelope.payload

        description = str(payload.get("description") or "").strip()
        if not description:
            raise NormalizationError("Citizen report has an empty description")

        submission_id = payload.get("submission_id") or envelope.external_id
        if not submission_id:
            raise NormalizationError("Citizen report is missing submission_id")

        observed_at = (
            _parse_timestamp(payload.get("observed_at"))
            or envelope.observed_at
            or envelope.fetched_at
        )

        cleaned = clean_text(description)
        category, category_confidence = self._resolve_category(payload, cleaned)
        severity = severity_from_keywords(cleaned)

        exif = self._rebuild_exif(payload.get("exif"))
        geo_input = GeoResolutionInput(
            lat=_as_float(payload.get("lat"), -90, 90),
            lon=_as_float(payload.get("lon"), -180, 180),
            accuracy_m=_as_float(payload.get("location_accuracy_m"), 0, 100_000),
            exif=exif,
            place_name=payload.get("place_name"),
            district=payload.get("district"),
            state=payload.get("state"),
            free_text=cleaned,
        )

        return ParsedFields(
            external_id=str(submission_id),
            observed_at=observed_at,
            raw_text=truncate(description),
            geo_input=geo_input,
            category=category,
            severity=severity,
            measurements=Measurements(
                water_level_cm=_as_float(payload.get("water_level_cm"), 0, 2000),
            ),
            media=self._parse_media(payload.get("media") or []),
            author=self._parse_author(payload),
            metadata={
                "record_type": "citizen_report",
                "category_hint_source": "reporter" if payload.get("category_hint") else "keywords",
                "category_rule_confidence": round(category_confidence, 2),
                "pincode": payload.get("pincode"),
                "client_app_version": payload.get("client_app_version"),
                "submitted_via": payload.get("submitted_via") or "rest_api",
                # Flags consumed by the Phase 2 credibility model.
                "is_official": False,
                "is_unverified_claim": True,
                "has_media": bool(payload.get("media")),
                "exif_gps_present": bool(exif and exif.has_gps),
            },
        )

    # -------------------------------------------------------------- helpers --
    @staticmethod
    def _resolve_category(payload: dict[str, Any], cleaned: str) -> tuple[HazardCategory, float]:
        """Reporter's own tag wins; keywords fill the gap."""
        hint = payload.get("category_hint")
        if hint:
            try:
                return HazardCategory(str(hint)), 0.75
            except ValueError:
                pass  # unknown label from an out-of-date client; fall through
        return classify_by_keywords(cleaned)

    @staticmethod
    def _rebuild_exif(exif_payload: Any) -> ExifResult | None:
        """Reconstruct the EXIF result the API layer extracted.

        Extraction happens at the API boundary because that is where the bytes
        are; only the derived values travel through Kafka.
        """
        if not isinstance(exif_payload, dict):
            return None
        return ExifResult(
            lat=_as_float(exif_payload.get("lat"), -90, 90),
            lon=_as_float(exif_payload.get("lon"), -180, 180),
            captured_at=_parse_timestamp(exif_payload.get("captured_at")),
            camera_make=exif_payload.get("camera_make"),
            has_exif=bool(exif_payload.get("has_exif")),
        )

    @staticmethod
    def _parse_media(items: Any) -> list[MediaAsset]:
        if not isinstance(items, list):
            return []
        assets: list[MediaAsset] = []
        for item in items[:10]:
            if not isinstance(item, dict) or not item.get("url"):
                continue
            content_type = str(item.get("content_type") or "")
            kind = MediaKind.UNKNOWN
            for prefix, mapped in _MEDIA_KIND_BY_PREFIX.items():
                if content_type.startswith(prefix):
                    kind = mapped
                    break
            try:
                assets.append(
                    MediaAsset(
                        url=str(item["url"])[:2048],
                        kind=kind,
                        content_type=content_type or None,
                        size_bytes=item.get("size_bytes"),
                        sha256=item.get("sha256"),
                        captured_at=_parse_timestamp(item.get("captured_at")),
                    )
                )
            except ValueError:
                continue  # malformed asset: skip it, keep the report
        return assets

    @staticmethod
    def _parse_author(payload: dict[str, Any]) -> AuthorRef | None:
        author_id = payload.get("author_id")
        if not author_id:
            return None
        author_id = str(author_id)
        if _RAW_PHONE_RE.match(author_id):
            raise NormalizationError(
                "Refusing to persist a raw phone number as author_id; "
                "the ingest layer must pseudonymise it first"
            )
        return AuthorRef(
            author_id=author_id[:128],
            display_handle=(payload.get("reporter_handle") or None),
            platform="citizen_app",
            is_verified_account=bool(payload.get("is_verified_reporter")),
            prior_report_count=payload.get("prior_report_count"),
        )

    def build_normalized_text(self, parsed: ParsedFields) -> str | None:
        return truncate(clean_text(parsed.raw_text or ""), 8192) or None

    def detect_language(self, parsed: ParsedFields) -> str | None:
        return detect_language(parsed.raw_text or "")
