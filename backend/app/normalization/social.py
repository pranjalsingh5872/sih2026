"""Social media and RSS normalizer.

The lowest-trust, highest-volume stream. Its value is timeliness and density —
during the 2023 Himachal floods, social posts led official bulletins by tens of
minutes in places — and its cost is rumour, reposts and outright fabrication.

Phase 1's responsibility is not to judge any of that. It is to capture the
*signals* Phase 2 will need to judge it: account age, follower count, verified
status, repost lineage, engagement counts and whether the post carries media.
Discarding those at normalization time would make credibility scoring
impossible later, so they are preserved even though nothing reads them yet.
"""

from __future__ import annotations

from typing import Any

from app.core.errors import NormalizationError
from app.geo.geocoder import GeoResolutionInput
from app.core.security import pseudonymous_author_id
from app.normalization.base import BaseNormalizer, ParsedFields
from app.normalization.imd import _as_float, _parse_timestamp
from app.normalization.text import (
    classify_by_keywords,
    clean_text,
    detect_language,
    extract_hashtags,
    severity_from_keywords,
    truncate,
)
from app.schemas.enums import MediaKind, SourceType
from app.schemas.incident import AuthorRef, MediaAsset
from app.schemas.raw import RawEnvelope


class SocialNormalizer(BaseNormalizer):
    source_type = SourceType.SOCIAL
    source_name = "social_stream"

    def parse(self, envelope: RawEnvelope) -> ParsedFields:
        payload = envelope.payload

        text = str(payload.get("text") or payload.get("content") or "").strip()
        if not text:
            raise NormalizationError("Social post has no text content")

        post_id = payload.get("post_id") or payload.get("id") or envelope.external_id
        if not post_id:
            raise NormalizationError("Social post is missing an identifier")

        posted_at = (
            _parse_timestamp(payload.get("created_at") or payload.get("posted_at"))
            or envelope.observed_at
            or envelope.fetched_at
        )

        cleaned = clean_text(text)
        category, rule_confidence = classify_by_keywords(cleaned)
        severity = severity_from_keywords(cleaned)

        geo = payload.get("geo") or {}
        geo_input = GeoResolutionInput(
            lat=_as_float(geo.get("lat") or payload.get("lat"), -90, 90),
            lon=_as_float(geo.get("lon") or payload.get("lon"), -180, 180),
            place_name=geo.get("place_name") or payload.get("location") or payload.get("place"),
            district=geo.get("district"),
            state=geo.get("state"),
            free_text=cleaned,
        )

        platform = str(payload.get("platform") or "unknown").lower()

        return ParsedFields(
            external_id=f"{platform}-{post_id}",
            observed_at=posted_at,
            raw_text=truncate(text),
            geo_input=geo_input,
            category=category,
            severity=severity,
            media=self._parse_media(payload.get("media") or []),
            author=self._parse_author(payload),
            metadata={
                "record_type": "social_post",
                "platform": platform,
                "hashtags": extract_hashtags(text),
                "language_declared": payload.get("lang"),
                # --- engagement, for Phase 2 corroboration weighting --------
                "repost_count": payload.get("repost_count") or payload.get("retweet_count"),
                "reply_count": payload.get("reply_count"),
                "like_count": payload.get("like_count"),
                # --- provenance lineage, for repost/rumour tracing ----------
                "is_repost": bool(payload.get("is_repost")),
                "reposted_from_id": payload.get("reposted_from_id"),
                "source_url": payload.get("url"),
                # --- trust flags -------------------------------------------
                "is_official": False,
                "is_unverified_claim": True,
                "category_rule_confidence": round(rule_confidence, 2),
                # The simulator marks synthetic misinformation so Phase 2's
                # credibility model has labelled negatives to evaluate against.
                # In production this key is simply absent.
                "synthetic_label": payload.get("synthetic_label"),
            },
        )

    # -------------------------------------------------------------- helpers --
    @staticmethod
    def _parse_media(items: Any) -> list[MediaAsset]:
        if not isinstance(items, list):
            return []
        assets: list[MediaAsset] = []
        for item in items[:10]:
            url = item.get("url") if isinstance(item, dict) else item
            if not url:
                continue
            kind = MediaKind.UNKNOWN
            if isinstance(item, dict):
                raw_kind = str(item.get("type") or "").lower()
                if raw_kind in ("photo", "image"):
                    kind = MediaKind.IMAGE
                elif raw_kind in ("video", "animated_gif"):
                    kind = MediaKind.VIDEO
            try:
                assets.append(MediaAsset(url=str(url)[:2048], kind=kind))
            except ValueError:
                continue
        return assets

    @staticmethod
    def _parse_author(payload: dict[str, Any]) -> AuthorRef | None:
        author = payload.get("author") or {}
        if not isinstance(author, dict):
            return None
        author_id = author.get("id") or author.get("author_id")
        if not author_id:
            return None
        raw_id = str(author_id)
        return AuthorRef(
            author_id=pseudonymous_author_id(raw_id)[:128],
            display_handle=str(author.get("handle") or "")[:128] or None,
            platform=str(payload.get("platform") or "unknown")[:64],
            is_verified_account=bool(author.get("verified")),
            account_age_days=author.get("account_age_days"),
            follower_count=author.get("follower_count"),
        )

    def build_normalized_text(self, parsed: ParsedFields) -> str | None:
        return truncate(clean_text(parsed.raw_text or ""), 8192) or None

    def detect_language(self, parsed: ParsedFields) -> str | None:
        return detect_language(parsed.raw_text or "")
