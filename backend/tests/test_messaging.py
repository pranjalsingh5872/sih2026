"""Messaging layer.

With ``KAFKA_ENABLED=false`` the producer writes to the in-memory bus. That is
not only a test convenience — it is the fallback the whole pipeline runs on
when a broker is unavailable, so it deserves the same assertions the real path
would get.
"""

from __future__ import annotations

import json

import pytest

from app.messaging.producer import EventProducer
from app.messaging.topics import ConsumerGroups, Topics
from app.schemas.enums import SourceType
from app.schemas.raw import RawEnvelope


@pytest.fixture
async def producer() -> EventProducer:
    p = EventProducer(client_suffix="pytest")
    await p.start()
    yield p
    await p.stop()


@pytest.mark.asyncio
async def test_publish_round_trips_through_the_bus(producer, bus) -> None:
    envelope = RawEnvelope(
        source_type=SourceType.IMD,
        source_name="imd_warnings",
        external_id="IMD-1",
        payload={"hazard_type": "Heavy Rainfall"},
    )
    assert await producer.publish(Topics.RAW_WEATHER, envelope, key=envelope.partition_key())

    messages = bus.drain(Topics.RAW_WEATHER)
    assert len(messages) == 1
    decoded = json.loads(messages[0])
    assert decoded["source_type"] == "IMD"
    assert decoded["external_id"] == "IMD-1"


@pytest.mark.asyncio
async def test_publish_accepts_a_plain_dict(producer, bus) -> None:
    assert await producer.publish(Topics.NORMALIZED_INCIDENTS, {"hello": "world"})
    assert json.loads(bus.drain(Topics.NORMALIZED_INCIDENTS)[0]) == {"hello": "world"}


@pytest.mark.asyncio
async def test_publish_many_preserves_order_within_a_topic(producer, bus) -> None:
    payloads = [{"n": i} for i in range(5)]
    sent = await producer.publish_many(Topics.NORMALIZED_INCIDENTS, payloads)
    assert sent == 5
    received = [json.loads(m)["n"] for m in bus.drain(Topics.NORMALIZED_INCIDENTS)]
    assert received == list(range(5))


@pytest.mark.asyncio
async def test_producer_reports_readiness_and_counts(producer, bus) -> None:
    assert producer.is_ready is True
    before = producer.stats["published"]
    await producer.publish(Topics.RAW_WEATHER, {"x": 1})
    assert producer.stats["published"] == before + 1


@pytest.mark.asyncio
async def test_subscriber_receives_published_messages(producer, bus) -> None:
    received: list[bytes] = []
    bus.subscribe(Topics.GEO_UNRESOLVED, lambda value, key: received.append(value))

    await producer.publish(Topics.GEO_UNRESOLVED, {"incident": "no-location"})
    assert len(received) == 1
    assert json.loads(received[0])["incident"] == "no-location"


def test_topic_names_match_the_problem_statement() -> None:
    """These two names are specified by the brief; they must not drift."""
    assert Topics.RAW_WEATHER == "raw-weather-stream"
    assert Topics.NORMALIZED_INCIDENTS == "normalized-incident-stream"


def test_all_topics_are_distinct() -> None:
    assert len(set(Topics.all())) == len(Topics.all())


def test_consumer_groups_are_role_scoped_not_replica_scoped() -> None:
    """One group per role is what makes horizontal scaling actually share work."""
    groups = {
        ConsumerGroups.NORMALIZER,
        ConsumerGroups.AI_ENRICHMENT,
        ConsumerGroups.EVENT_FUSION,
        ConsumerGroups.POSTGRES_SINK,
    }
    assert len(groups) == 4
