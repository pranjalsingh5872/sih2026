"""End-to-end Phase 1 flow, broker-free.

Provider mock → ``RawEnvelope`` → ``raw-weather-stream`` → normalizer worker →
``normalized-incident-stream``. This is the claim the phase actually makes, so
it is asserted as one path rather than only in pieces.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from app.messaging.topics import Topics
from app.providers.imd_client import IMDClient
from app.providers.openweather_client import OpenWeatherClient
from app.providers.social_feed import SocialFeedGenerator
from app.schemas.enums import PipelineStage, SourceType
from app.schemas.incident import NormalizedIncident
from app.schemas.raw import RawEnvelope
from app.workers.normalizer import NormalizerWorker


@dataclass
class FakeRecord:
    """Stands in for ``aiokafka.ConsumerRecord``; the handler reads two fields."""

    topic: str = Topics.RAW_WEATHER
    offset: int = 0
    partition: int = 0
    key: bytes | None = None


@pytest.fixture
async def worker(registry, bus):
    w = NormalizerWorker(registry=registry)
    await w._producer.start()
    return w


def _envelope(source: SourceType, payload: dict, external_id: str | None = None) -> dict:
    return RawEnvelope(
        source_type=source,
        source_name=f"{source.value.lower()}_feed",
        external_id=external_id,
        payload=payload,
        ingest_stage=PipelineStage.PROVIDER_POLL,
        producer_component="pytest",
    ).model_dump(mode="json")


def _normalized(bus) -> list[dict]:
    return [json.loads(m) for m in bus.peek(Topics.NORMALIZED_INCIDENTS)]


# ------------------------------------------------------------ provider mocks --
@pytest.mark.asyncio
async def test_imd_mock_produces_usable_records(settings) -> None:
    """Mocks are functional stand-ins, not decoration — they must normalize."""
    client = IMDClient(settings)
    warnings = await client.fetch_warnings()
    observations = await client.fetch_observations()

    assert 2 <= len(warnings) <= 5
    assert 4 <= len(observations) <= 9
    for record in warnings:
        assert {"bulletin_id", "hazard_type", "lat", "lon"} <= record.keys()
    for record in observations:
        assert {"station_id", "lat", "lon", "observation_time"} <= record.keys()


@pytest.mark.asyncio
async def test_openweather_mock_returns_the_real_api_shape(settings) -> None:
    """Kelvin, m/s and nested rain — so the normalizer is tested, not bypassed."""
    client = OpenWeatherClient(settings)
    batch = await client.fetch_batch(batch_size=4)

    assert batch
    sample = batch[0]
    assert sample["main"]["temp"] > 250  # Kelvin, not Celsius
    assert "coord" in sample and "weather" in sample


def test_social_simulator_emits_labelled_misinformation() -> None:
    """Phase 2 needs ground truth; the simulator supplies it via synthetic_label."""
    generator = SocialFeedGenerator(seed=26069, fake_ratio=1.0)
    posts = generator.generate_batch(12)
    assert all(p.get("synthetic_label") for p in posts)


def test_social_simulator_produces_multilingual_traffic() -> None:
    generator = SocialFeedGenerator(seed=26069, fake_ratio=0.0)
    languages = {p["lang"] for p in generator.generate_batch(60)}
    assert len(languages) > 1


def test_social_simulator_reposts_are_not_byte_identical() -> None:
    """Hash dedup must not be able to solve Phase 2's job for it."""
    generator = SocialFeedGenerator(seed=7, duplicate_ratio=1.0, fake_ratio=0.0)
    posts = generator.generate_batch(40)
    reposts = [p for p in posts if p.get("is_repost")]
    if reposts:  # the first post can never be a repost
        assert any(p["text"] != posts[0]["text"] for p in reposts)


# ------------------------------------------------------- worker end-to-end ----
@pytest.mark.asyncio
async def test_imd_record_flows_to_the_normalized_stream(
    worker, bus, imd_warning_payload
) -> None:
    await worker.handle(_envelope(SourceType.IMD, imd_warning_payload), FakeRecord())

    produced = _normalized(bus)
    assert len(produced) == 1
    incident = NormalizedIncident.model_validate(produced[0])
    assert incident.source_type is SourceType.IMD
    assert incident.geo.is_resolved


@pytest.mark.asyncio
async def test_all_five_sources_converge_on_one_schema(
    worker,
    bus,
    imd_observation_payload,
    openweather_payload,
    citizen_payload,
    social_payload,
    sensor_payload,
) -> None:
    """The point of Phase 1: five payload shapes, one contract."""
    cases = [
        (SourceType.IMD, imd_observation_payload),
        (SourceType.OPENWEATHER, openweather_payload),
        (SourceType.CITIZEN, citizen_payload),
        (SourceType.SOCIAL, social_payload),
        (SourceType.SENSOR, sensor_payload),
    ]
    for source, payload in cases:
        await worker.handle(_envelope(source, payload), FakeRecord())

    produced = _normalized(bus)
    assert len(produced) == 5

    incidents = [NormalizedIncident.model_validate(p) for p in produced]
    assert {i.source_type for i in incidents} == set(SourceType)
    # Every one validates against the same model — that is the whole assertion.
    assert all(i.schema_version == 1 for i in incidents)


@pytest.mark.asyncio
async def test_partition_key_groups_nearby_reports(
    worker, bus, imd_observation_payload, citizen_payload
) -> None:
    """Co-located reports share a partition, so Phase 3 clusters locally."""
    await worker.handle(_envelope(SourceType.IMD, imd_observation_payload), FakeRecord())
    await worker.handle(_envelope(SourceType.CITIZEN, citizen_payload), FakeRecord())

    incidents = [NormalizedIncident.model_validate(p) for p in _normalized(bus)]
    keys = {NormalizerWorker._partition_key(i) for i in incidents}
    assert len(keys) == 1  # both are in Indore


@pytest.mark.asyncio
async def test_unresolved_report_is_fanned_out_not_dropped(
    worker, bus, citizen_payload
) -> None:
    """The design decision, asserted: no location is not a reason to discard."""
    nowhere = {
        **citizen_payload,
        "lat": None,
        "lon": None,
        "place_name": None,
        "district": None,
        "state": None,
        "description": "we need help right now please come quickly",
    }
    await worker.handle(_envelope(SourceType.CITIZEN, nowhere), FakeRecord())

    assert len(_normalized(bus)) == 1
    assert len(bus.peek(Topics.GEO_UNRESOLVED)) == 1


@pytest.mark.asyncio
async def test_resolved_report_is_not_fanned_to_the_triage_topic(
    worker, bus, citizen_payload
) -> None:
    await worker.handle(_envelope(SourceType.CITIZEN, citizen_payload), FakeRecord())
    assert bus.peek(Topics.GEO_UNRESOLVED) == []


@pytest.mark.asyncio
async def test_malformed_envelope_raises_for_dead_lettering(worker) -> None:
    """A permanent error is how the consumer knows not to retry forever."""
    from app.core.errors import PermanentError

    with pytest.raises(PermanentError):
        await worker.handle({"not": "an envelope"}, FakeRecord())


@pytest.mark.asyncio
async def test_replayed_envelope_yields_the_same_incident_id(
    worker, bus, imd_warning_payload
) -> None:
    """Replay after a crash must be idempotent, not duplicative."""
    envelope = _envelope(SourceType.IMD, imd_warning_payload)
    await worker.handle(envelope, FakeRecord(offset=1))
    await worker.handle(envelope, FakeRecord(offset=1))

    produced = _normalized(bus)
    assert len(produced) == 2  # at-least-once delivery is expected
    assert produced[0]["incident_id"] == produced[1]["incident_id"]
