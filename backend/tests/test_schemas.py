"""Schema invariants.

These are the guarantees every downstream phase is allowed to assume. If one
of them breaks, deduplication, clustering and the PostGIS sink all break
quietly rather than loudly, so they are asserted here rather than discovered
in a demo.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.schemas.enums import (
    GEO_METHOD_CONFIDENCE,
    GeoMethod,
    HazardCategory,
    SeverityHint,
    SourceType,
)
from app.schemas.incident import (
    INCIDENT_NAMESPACE,
    GeoContext,
    GeoPoint,
    Measurements,
    NormalizedIncident,
    utcnow,
)
from app.schemas.raw import CitizenReportRequest, RawEnvelope


# ------------------------------------------------------------------ GeoPoint --
def test_geopoint_rejects_null_island() -> None:
    """(0, 0) is a missing-value sentinel, not a location in the Gulf of Guinea."""
    with pytest.raises(ValidationError):
        GeoPoint(lat=0.0, lon=0.0)


def test_geopoint_accepts_genuine_indian_coordinates() -> None:
    point = GeoPoint(lat=22.7196, lon=75.8577)
    assert point.to_geojson() == {"type": "Point", "coordinates": [75.8577, 22.7196]}
    assert point.to_wkt().startswith("SRID=4326;POINT(")


@pytest.mark.parametrize("lat,lon", [(91.0, 75.0), (-91.0, 75.0), (22.0, 181.0)])
def test_geopoint_rejects_out_of_range(lat: float, lon: float) -> None:
    with pytest.raises(ValidationError):
        GeoPoint(lat=lat, lon=lon)


# ---------------------------------------------------------------- GeoContext --
def test_geocontext_without_point_cannot_claim_confidence() -> None:
    """A method and a confidence are meaningless without coordinates."""
    ctx = GeoContext(point=None, method=GeoMethod.GPS_PAYLOAD, confidence=0.99)
    assert ctx.method is GeoMethod.UNRESOLVED
    assert ctx.confidence == 0.0
    assert ctx.is_resolved is False


def test_geocontext_backfills_confidence_from_method_prior() -> None:
    ctx = GeoContext(point=GeoPoint(lat=22.7, lon=75.8), method=GeoMethod.EXIF_GPS)
    assert ctx.confidence == GEO_METHOD_CONFIDENCE[GeoMethod.EXIF_GPS]


def test_geo_method_confidence_ordering_reflects_trust() -> None:
    """A station fix must outrank a district centroid, or Phase 3 clusters noise."""
    c = GEO_METHOD_CONFIDENCE
    assert c[GeoMethod.PROVIDER_STATION] > c[GeoMethod.GPS_PAYLOAD]
    assert c[GeoMethod.GPS_PAYLOAD] > c[GeoMethod.PLACE_NAME_LOOKUP]
    assert c[GeoMethod.PLACE_NAME_LOOKUP] > c[GeoMethod.ADMIN_CENTROID]
    assert c[GeoMethod.UNRESOLVED] == 0.0


# ----------------------------------------------------------- incident basics --
def _incident(**overrides) -> NormalizedIncident:
    # observed_at is pinned rather than "now" because content_hash truncates
    # the timestamp to the minute — two calls straddling a minute boundary
    # would otherwise hash differently and make the stability test flaky.
    base = {
        "incident_id": NormalizedIncident.derive_id(SourceType.CITIZEN, "cz-test-0001"),
        "source_type": SourceType.CITIZEN,
        "source_name": "citizen_app",
        "external_id": "cz-test-0001",
        "observed_at": utcnow().replace(second=0, microsecond=0) - timedelta(minutes=2),
        "geo": GeoContext(
            point=GeoPoint(lat=22.7196, lon=75.8577),
            method=GeoMethod.GPS_PAYLOAD,
        ),
        "raw_text": "Knee deep water near Rajwada",
        "normalized_text": "knee deep water near rajwada",
        "reported_category": HazardCategory.URBAN_FLOODING,
        "severity_hint": SeverityHint.SEVERE,
    }
    base.update(overrides)
    return NormalizedIncident(**base)


def test_derive_id_is_deterministic_across_replays() -> None:
    """A poller restart must not turn one bulletin into two incidents."""
    first = NormalizedIncident.derive_id(SourceType.IMD, "IMD-20260920-00042")
    second = NormalizedIncident.derive_id(SourceType.IMD, "IMD-20260920-00042")
    assert first == second
    assert first == uuid.uuid5(INCIDENT_NAMESPACE, "IMD:IMD-20260920-00042")


def test_derive_id_separates_sources_sharing_an_external_id() -> None:
    assert NormalizedIncident.derive_id(
        SourceType.IMD, "X-1"
    ) != NormalizedIncident.derive_id(SourceType.SOCIAL, "X-1")


def test_content_hash_is_stable_for_identical_content() -> None:
    a = _incident()
    b = _incident()
    assert a.content_hash == b.content_hash
    assert len(a.content_hash) == 64


def test_content_hash_changes_with_text() -> None:
    a = _incident()
    b = _incident(normalized_text="water receding near rajwada")
    assert a.content_hash != b.content_hash


def test_naive_timestamps_are_rejected() -> None:
    """A timestamp without a zone silently means 'whatever the server thinks'."""
    with pytest.raises(ValidationError):
        _incident(observed_at=datetime(2026, 9, 20, 10, 0, 0))


def test_future_observation_beyond_clock_skew_is_rejected() -> None:
    with pytest.raises(ValidationError):
        _incident(observed_at=utcnow() + timedelta(hours=6))


def test_small_clock_skew_is_tolerated() -> None:
    """Phone clocks drift; a few minutes ahead is not an attack."""
    incident = _incident(observed_at=utcnow() + timedelta(minutes=5))
    assert incident.observed_at is not None


def test_ancient_backfill_is_rejected() -> None:
    with pytest.raises(ValidationError):
        _incident(observed_at=utcnow() - timedelta(days=400))


def test_timestamps_are_normalized_to_utc() -> None:
    ist = timezone(timedelta(hours=5, minutes=30))
    incident = _incident(observed_at=datetime.now(ist) - timedelta(minutes=1))
    assert incident.observed_at.tzinfo == timezone.utc


def test_geojson_feature_shape_is_valid() -> None:
    feature = _incident().to_geojson_feature()
    assert feature["type"] == "Feature"
    assert feature["geometry"]["type"] == "Point"
    lon, lat = feature["geometry"]["coordinates"]
    assert (lon, lat) == (75.8577, 22.7196)
    assert feature["properties"]["source_type"] == SourceType.CITIZEN.value


def test_unresolved_incident_yields_null_geometry() -> None:
    """GeoJSON permits a null geometry; dropping the report would not."""
    feature = _incident(geo=GeoContext()).to_geojson_feature()
    assert feature["geometry"] is None


def test_measurements_reject_unknown_fields() -> None:
    """Units are fixed and explicit — a stray field usually means wrong units."""
    with pytest.raises(ValidationError):
        Measurements(rainfall_inches=3.0)


# ------------------------------------------------------------- RawEnvelope ---
def test_raw_envelope_requires_a_payload() -> None:
    with pytest.raises(ValidationError):
        RawEnvelope(source_type=SourceType.IMD, source_name="imd", payload={})


def test_raw_envelope_partition_key_is_source_scoped() -> None:
    env = RawEnvelope(
        source_type=SourceType.IMD,
        source_name="imd_warnings",
        external_id="IMD-1",
        payload={"a": 1},
    )
    assert env.partition_key() == "IMD:IMD-1"


def test_raw_envelope_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError):
        RawEnvelope(
            source_type=SourceType.IMD,
            source_name="imd",
            payload={"a": 1},
            observed_at=datetime(2026, 9, 20, 10, 0, 0),
        )


# ------------------------------------------------- citizen request contract --
def test_citizen_report_allows_missing_coordinates() -> None:
    """The whole point of the geo fallback chain."""
    report = CitizenReportRequest(description="Bahut paani bhara hua hai yahan")
    assert report.lat is None and report.lon is None


def test_citizen_report_rejects_half_a_coordinate_pair() -> None:
    """A lone latitude is a client bug, and pinning it would invent a location."""
    with pytest.raises(ValidationError):
        CitizenReportRequest(description="Flooding here", lat=22.7)


def test_citizen_report_rejects_malformed_pincode() -> None:
    with pytest.raises(ValidationError):
        CitizenReportRequest(description="Flooding here", pincode="45200")


def test_citizen_report_rejects_unknown_fields() -> None:
    """extra=forbid stops a typo'd field from being silently discarded."""
    with pytest.raises(ValidationError):
        CitizenReportRequest(description="Flooding", latitude=22.7)
