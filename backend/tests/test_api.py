"""API contract.

These tests drive the real application through ``TestClient``, which runs the
lifespan, so the producer, Redis gateway and gazetteer are exercised exactly as
they are at boot.
"""

from __future__ import annotations

import io
import json
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.messaging.topics import Topics
from app.schemas.enums import SourceType

INGEST_KEY = "dev-citizen-key"
ADMIN_KEY = "dev-admin-key"
PREFIX = "/api/v1"


@pytest.fixture
def client(bus) -> Iterator[TestClient]:
    with TestClient(create_app()) as test_client:
        yield test_client


def _published(bus) -> list[dict]:
    return [json.loads(m) for m in bus.peek(Topics.RAW_WEATHER)]


# ================================================================== health ===
def test_liveness_needs_no_credentials(client: TestClient) -> None:
    """An ops probe that needs a key is an ops probe that silently breaks."""
    response = client.get(f"{PREFIX}/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "alive"


def test_readiness_reports_dependencies(client: TestClient) -> None:
    body = client.get(f"{PREFIX}/readyz").json()
    assert body["status"] in {"ready", "degraded"}
    assert body["checks"]["message_bus"]["status"] == "ok"
    assert body["checks"]["gazetteer"]["places"] > 100


def test_redis_being_down_is_degraded_not_unready(client: TestClient) -> None:
    """Rate limiting fails open, so Redis must never gate traffic."""
    response = client.get(f"{PREFIX}/readyz")
    assert response.status_code == 200
    assert response.json()["checks"]["redis"]["required"] is False


def test_info_exposes_pipeline_shape_without_secrets(client: TestClient) -> None:
    body = client.get(f"{PREFIX}/info").json()
    assert body["topics"]["raw"] == Topics.RAW_WEATHER
    assert set(body["sources_supported"]) == {s.value for s in SourceType}
    serialized = json.dumps(body)
    assert INGEST_KEY not in serialized and ADMIN_KEY not in serialized


def test_root_points_at_the_docs(client: TestClient) -> None:
    assert client.get("/").json()["docs"] == "/docs"


def test_openapi_schema_builds(client: TestClient) -> None:
    """A broken response model only shows up here, never at import time."""
    assert client.get("/openapi.json").status_code == 200


# ==================================================================== auth ===
def test_report_requires_an_api_key(client: TestClient) -> None:
    response = client.post(f"{PREFIX}/incidents/report", json={"description": "flooding"})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"


def test_unknown_api_key_is_rejected(client: TestClient) -> None:
    response = client.post(
        f"{PREFIX}/incidents/report",
        json={"description": "flooding"},
        headers={"X-API-Key": "not-a-real-key"},
    )
    assert response.status_code == 401


def test_admin_key_may_also_ingest(client: TestClient) -> None:
    """ADMIN is a superset scope; an operator filing a report is legitimate."""
    response = client.post(
        f"{PREFIX}/incidents/report",
        json={"description": "Control room observation: waterlogging at Indore"},
        headers={"X-API-Key": ADMIN_KEY},
    )
    assert response.status_code == 202


# ================================================================== ingest ===
def test_report_with_coordinates_is_accepted_and_queued(client: TestClient, bus) -> None:
    response = client.post(
        f"{PREFIX}/incidents/report",
        json={
            "description": "Knee deep water near Rajwada, cars are stuck",
            "lat": 22.7196,
            "lon": 75.8577,
            "location_accuracy_m": 9.0,
            "district": "Indore",
            "state": "Madhya Pradesh",
        },
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["accepted"] is True
    assert body["location_resolved"] is True
    assert body["queued_to_topic"] == Topics.RAW_WEATHER

    queued = _published(bus)
    assert queued and queued[-1]["source_type"] == "CITIZEN"


def test_report_without_coordinates_is_still_accepted(client: TestClient, bus) -> None:
    """The person with no GPS is exactly who this platform must not turn away."""
    response = client.post(
        f"{PREFIX}/incidents/report",
        json={"description": "Paani bhar gaya hai Indore mein, madad chahiye"},
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 202
    assert response.json()["accepted"] is True


def test_unlocatable_report_says_so_honestly(client: TestClient) -> None:
    body = client.post(
        f"{PREFIX}/incidents/report",
        json={"description": "please help us we are stuck here"},
        headers={"X-API-Key": INGEST_KEY},
    ).json()
    assert body["accepted"] is True
    assert body["location_resolved"] is False
    assert "manual review" in body["message"].lower()


def test_response_never_claims_the_report_is_verified(client: TestClient) -> None:
    """Telling a citizen their unverified claim is 'verified' would be a lie."""
    body = client.post(
        f"{PREFIX}/incidents/report",
        json={"description": "Flooding near Indore bus stand", "lat": 22.7, "lon": 75.85},
        headers={"X-API-Key": INGEST_KEY},
    ).json()
    assert "verified" not in body["message"].lower().replace("verification", "")


def test_invalid_payload_returns_a_field_level_422(client: TestClient) -> None:
    response = client.post(
        f"{PREFIX}/incidents/report",
        json={"description": "hi", "lat": 999.0, "lon": 75.0},
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 422
    details = response.json()["error"]["details"]
    assert any("lat" in d["field"] for d in details)


def test_half_a_coordinate_pair_is_rejected(client: TestClient) -> None:
    response = client.post(
        f"{PREFIX}/incidents/report",
        json={"description": "Flooding here", "lat": 22.7},
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 422


def test_identical_resubmission_is_not_duplicated(client: TestClient) -> None:
    """A double-tapped submit button must not become two incidents.

    With Redis disabled the gateway fails open, so both calls are accepted —
    the assertion is that the incident id is identical either way, which is
    what keeps the pipeline idempotent even when the cache is gone.
    """
    payload = {
        "description": "Tree fallen across the road near Indore GPO",
        "lat": 22.7196,
        "lon": 75.8577,
    }
    headers = {"X-API-Key": INGEST_KEY}
    first = client.post(f"{PREFIX}/incidents/report", json=payload, headers=headers).json()
    second = client.post(f"{PREFIX}/incidents/report", json=payload, headers=headers).json()
    assert first["incident_id"] == second["incident_id"]


def test_correlation_id_is_returned_and_echoed(client: TestClient) -> None:
    response = client.get(f"{PREFIX}/healthz")
    assert response.headers["X-Correlation-ID"]

    supplied = "trace-abc-123"
    echoed = client.get(f"{PREFIX}/healthz", headers={"X-Correlation-ID": supplied})
    assert echoed.headers["X-Correlation-ID"] == supplied


# =========================================================== multipart path ==
def _png_bytes() -> bytes:
    """Smallest valid PNG — enough to exercise the upload path."""
    return bytes.fromhex(
        "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
        "890000000a49444154789c6360000002000100ffff03000006000557bfabd400"
        "00000049454e44ae426082"
    )


def test_multipart_report_with_photo(client: TestClient, bus) -> None:
    response = client.post(
        f"{PREFIX}/incidents/report-with-media",
        data={
            "report": json.dumps(
                {
                    "description": "Underpass fully submerged at Indore",
                    "lat": 22.7196,
                    "lon": 75.8577,
                }
            )
        },
        files={"photo": ("flood.png", io.BytesIO(_png_bytes()), "image/png")},
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 202

    queued = _published(bus)[-1]
    assert queued["payload"]["media"]
    assert queued["payload"]["media"][0]["url"].startswith("/media/")
    assert len(queued["payload"]["media"][0]["sha256"]) == 64


def test_multipart_rejects_an_unsupported_media_type(client: TestClient) -> None:
    response = client.post(
        f"{PREFIX}/incidents/report-with-media",
        data={"report": json.dumps({"description": "Flooding at Indore"})},
        files={"photo": ("payload.exe", io.BytesIO(b"MZ\x90\x00"), "application/x-msdownload")},
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 415


def test_multipart_rejects_an_oversized_upload(client: TestClient, settings) -> None:
    oversized = b"\x00" * (settings.max_upload_bytes + 1024)
    response = client.post(
        f"{PREFIX}/incidents/report-with-media",
        data={"report": json.dumps({"description": "Flooding at Indore"})},
        files={"photo": ("big.jpg", io.BytesIO(oversized), "image/jpeg")},
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 413


def test_multipart_rejects_malformed_report_json(client: TestClient) -> None:
    response = client.post(
        f"{PREFIX}/incidents/report-with-media",
        data={"report": "{not json"},
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 400


def test_multipart_works_without_a_photo(client: TestClient) -> None:
    response = client.post(
        f"{PREFIX}/incidents/report-with-media",
        data={"report": json.dumps({"description": "Waterlogging near Indore station"})},
        headers={"X-API-Key": INGEST_KEY},
    )
    assert response.status_code == 202
