"""Citizen incident reporting endpoints.

Two routes, one pipeline. The JSON route is for clients that have already
uploaded media elsewhere; the multipart route accepts the photo directly, which
is what the mobile app uses and what makes EXIF GPS recovery possible.

Both publish a :class:`RawEnvelope` onto ``raw-weather-stream``, exactly like
the pollers do. A citizen report gets no shortcut through the pipeline — it is
normalized, geo-resolved and (in Phase 2) scored by the same code as everything
else. That uniformity is what makes the credibility comparison meaningful.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile, status

from app.api.deps import (
    IngestPrincipal,
    ProducerDep,
    RateLimited,
    RedisDep,
    SettingsDep,
)
from app.core.errors import (
    ApiError,
    PayloadTooLargeError,
    ServiceUnavailableError,
    UnsupportedMediaError,
)
from app.core.logging import get_correlation_id, get_logger
from app.core.security import pseudonymous_author_id
from app.geo.exif import extract_exif
from app.geo.geocoder import GeoResolutionInput, get_geo_resolver
from app.messaging.topics import Topics
from app.normalization.text import clean_text
from app.schemas.enums import PipelineStage, SourceType
from app.schemas.incident import NormalizedIncident
from app.schemas.raw import CitizenReportRequest, CitizenReportResponse, RawEnvelope

logger = get_logger(__name__)

router = APIRouter(prefix="/incidents", tags=["ingestion"])


# ---------------------------------------------------------------- helpers ---
def _store_media(content: bytes, content_type: str, media_root: Path) -> dict[str, Any]:
    """Persist an upload under its own content hash.

    Content addressing deduplicates the identical photo re-shared by twenty
    accounts down to one stored object, and gives Phase 2 a stable handle for
    'this exact image has been seen before' — the cheapest recycled-footage
    check available.
    """
    digest = hashlib.sha256(content).hexdigest()
    suffix = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
    }.get(content_type, ".bin")

    # Shard by prefix to avoid a single directory with a million entries.
    target_dir = media_root / digest[:2] / digest[2:4]
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{digest}{suffix}"
        if not target.exists():
            target.write_bytes(content)
    except OSError as exc:
        logger.error("Media write failed", extra={"error": str(exc)})
        raise ServiceUnavailableError("Could not persist uploaded media") from exc

    return {
        "url": f"/media/{digest[:2]}/{digest[2:4]}/{digest}{suffix}",
        "content_type": content_type,
        "size_bytes": len(content),
        "sha256": digest,
    }


async def _publish_report(
    report: CitizenReportRequest,
    *,
    principal_subject: str,
    producer,
    redis,
    settings,
    media: list[dict[str, Any]] | None = None,
    exif_payload: dict[str, Any] | None = None,
    submitted_via: str = "rest_api",
) -> CitizenReportResponse:
    """Shared path for both ingest routes."""
    cleaned = clean_text(report.description)

    # Resolve location here, before Kafka, purely so the response can tell the
    # citizen whether their report was locatable. The normalizer resolves again
    # from the envelope — the authoritative result is the one it computes.
    resolver = get_geo_resolver()
    geo = await resolver.resolve(
        GeoResolutionInput(
            lat=report.lat,
            lon=report.lon,
            accuracy_m=report.location_accuracy_m,
            place_name=report.place_name,
            district=report.district,
            state=report.state,
            free_text=cleaned,
        )
    )

    author_id = (
        pseudonymous_author_id(report.reporter_identifier)
        if report.reporter_identifier
        else None
    )

    observed_at = report.observed_at
    payload: dict[str, Any] = {
        "submission_id": None,  # filled below once the id is derived
        "description": report.description,
        "lat": report.lat,
        "lon": report.lon,
        "location_accuracy_m": report.location_accuracy_m,
        "place_name": report.place_name,
        "district": report.district,
        "state": report.state,
        "pincode": report.pincode,
        "observed_at": observed_at.isoformat() if observed_at else None,
        "category_hint": report.category_hint.value if report.category_hint else None,
        "water_level_cm": report.water_level_cm,
        "author_id": author_id,
        "reporter_handle": report.reporter_handle,
        "client_app_version": report.client_app_version,
        "submitted_via": submitted_via,
        "media": media or [],
        "exif": exif_payload,
    }

    # Content hash over the semantic fields, so a double-tap on the submit
    # button does not become two incidents.
    fingerprint_source = "|".join(
        [
            cleaned.casefold(),
            f"{report.lat:.4f},{report.lon:.4f}" if report.lat and report.lon else "nogeo",
            author_id or principal_subject,
            (media[0]["sha256"] if media else ""),
        ]
    )
    fingerprint = hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()
    submission_id = f"cz-{fingerprint[:24]}"
    payload["submission_id"] = submission_id

    is_first = await redis.claim_once(fingerprint, ttl_s=settings.idempotency_ttl_s)
    incident_id = NormalizedIncident.derive_id(SourceType.CITIZEN, submission_id)

    if not is_first:
        logger.info(
            "Duplicate submission suppressed",
            extra={"submission_id": submission_id, "subject": principal_subject},
        )
        return CitizenReportResponse(
            incident_id=incident_id,
            accepted=True,
            duplicate_of_submission=True,
            location_resolved=geo.is_resolved,
            geo_method=geo.method.value,
            place_label=geo.place_label,
            queued_to_topic=Topics.RAW_WEATHER,
            message="This report was already received; it has not been duplicated.",
        )

    envelope = RawEnvelope(
        correlation_id=get_correlation_id(),
        source_type=SourceType.CITIZEN,
        source_name="citizen_app",
        external_id=submission_id,
        observed_at=observed_at,
        payload=payload,
        ingest_stage=PipelineStage.INGEST_API,
        producer_component="ingest-api",
    )

    published = await producer.publish(
        Topics.RAW_WEATHER, envelope, key=envelope.partition_key()
    )
    if not published:
        # The report is genuinely at risk of being lost — tell the client so it
        # can retry, rather than returning 200 over a silent failure.
        raise ServiceUnavailableError(
            "Report could not be queued for processing. Please retry."
        )

    logger.info(
        "Citizen report accepted",
        extra={
            "submission_id": submission_id,
            "geo_method": geo.method.value,
            "geo_resolved": geo.is_resolved,
            "has_media": bool(media),
            "subject": principal_subject,
        },
    )

    return CitizenReportResponse(
        incident_id=incident_id,
        accepted=True,
        location_resolved=geo.is_resolved,
        geo_method=geo.method.value,
        place_label=geo.place_label,
        queued_to_topic=Topics.RAW_WEATHER,
        message=(
            "Report received and queued for verification."
            if geo.is_resolved
            else "Report received. We could not determine the location "
            "automatically; it has been queued for manual review."
        ),
    )


# ----------------------------------------------------------------- routes ---
@router.post(
    "/report",
    response_model=CitizenReportResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a citizen incident report (JSON)",
)
async def submit_report(
    report: CitizenReportRequest,
    principal: IngestPrincipal,
    producer: ProducerDep,
    redis: RedisDep,
    settings: SettingsDep,
    _rate_limited: RateLimited = None,
) -> CitizenReportResponse:
    """Accept a report without attached media."""
    return await _publish_report(
        report,
        principal_subject=principal.subject,
        producer=producer,
        redis=redis,
        settings=settings,
    )


@router.post(
    "/report-with-media",
    response_model=CitizenReportResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a citizen incident report with a photo",
)
async def submit_report_with_media(
    request: Request,
    principal: IngestPrincipal,
    producer: ProducerDep,
    redis: RedisDep,
    settings: SettingsDep,
    report: Annotated[str, Form(description="CitizenReportRequest as a JSON string")],
    photo: Annotated[UploadFile | None, File()] = None,
    _rate_limited: RateLimited = None,
) -> CitizenReportResponse:
    """Accept a report plus an optional photo, mining EXIF GPS from it."""
    try:
        report_data = json.loads(report)
    except json.JSONDecodeError as exc:
        raise ApiError(f"`report` is not valid JSON: {exc}") from exc

    try:
        parsed_report = CitizenReportRequest.model_validate(report_data)
    except ValueError as exc:
        raise ApiError(f"`report` failed validation: {exc}") from exc

    media: list[dict[str, Any]] = []
    exif_payload: dict[str, Any] | None = None

    if photo is not None:
        content_type = (photo.content_type or "").split(";")[0].strip()
        if content_type not in settings.allowed_media_types:
            raise UnsupportedMediaError(
                f"Media type {content_type or 'unknown'!r} is not accepted",
                allowed=settings.allowed_media_types,
            )

        # Read with a hard cap. Trusting Content-Length would let a client
        # stream an unbounded body into memory.
        content = await photo.read(settings.max_upload_bytes + 1)
        if len(content) > settings.max_upload_bytes:
            raise PayloadTooLargeError(
                f"Upload exceeds {settings.max_upload_bytes // (1024 * 1024)} MB limit"
            )
        if not content:
            raise ApiError("Uploaded file is empty")

        exif = extract_exif(content)
        exif_payload = {
            "has_exif": exif.has_exif,
            "lat": exif.lat,
            "lon": exif.lon,
            "captured_at": exif.captured_at.isoformat() if exif.captured_at else None,
            "camera_make": exif.camera_make,
        }

        stored = _store_media(content, content_type, settings.media_root)
        if exif.captured_at:
            stored["captured_at"] = exif.captured_at.isoformat()
        media.append(stored)

    return await _publish_report(
        parsed_report,
        principal_subject=principal.subject,
        producer=producer,
        redis=redis,
        settings=settings,
        media=media,
        exif_payload=exif_payload,
        submitted_via="mobile_multipart",
    )
