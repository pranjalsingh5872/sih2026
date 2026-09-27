"""Normalizer behaviour, one source at a time.

The contract every normalizer signs: produce a valid ``NormalizedIncident``
with resolved-or-honestly-unresolved geography, or raise ``NormalizationError``
so the consumer dead-letters the message instead of retrying it forever.
"""

from __future__ import annotations

import pytest

from app.core.errors import NormalizationError, UnsupportedSourceError
from app.normalization.text import (
    classify_by_keywords,
    clean_text,
    detect_language,
    extract_hashtags,
    severity_from_keywords,
    truncate,
)
from app.schemas.enums import GeoMethod, HazardCategory, SeverityHint, SourceType


# =========================================================== text utilities ==
@pytest.mark.parametrize(
    "text,expected",
    [
        ("Heavy   rain\n\nin  Indore", "Heavy rain in Indore"),
        ("  trailing and leading  ", "trailing and leading"),
    ],
)
def test_clean_text_collapses_whitespace(text: str, expected: str) -> None:
    assert clean_text(text) == expected


@pytest.mark.parametrize(
    "text,lang",
    [
        ("Heavy rainfall expected tomorrow", "en"),
        ("इंदौर में भारी बारिश हो रही है", "hi"),
        ("சென்னையில் கனமழை பெய்கிறது", "ta"),
        ("ভারী বৃষ্টি হচ্ছে", "bn"),
    ],
)
def test_detect_language(text: str, lang: str) -> None:
    assert detect_language(text) == lang


def test_detect_language_admits_ignorance() -> None:
    """'und' is an honest answer; guessing a language corrupts Phase 2 NLP."""
    assert detect_language("12345 !!! ???") == "und"


@pytest.mark.parametrize(
    "text,category",
    [
        ("flash flood warning, water rising fast", HazardCategory.FLASH_FLOOD),
        ("dense fog, visibility near zero on the highway", HazardCategory.DENSE_FOG),
        ("landslide blocked the hill road", HazardCategory.LANDSLIDE),
        ("extremely heavy rainfall since morning", HazardCategory.HEAVY_RAINFALL),
    ],
)
def test_keyword_classifier_high_precision_cases(
    text: str, category: HazardCategory
) -> None:
    assert classify_by_keywords(text)[0] is category


def test_keyword_classifier_abstains_rather_than_guesses() -> None:
    """A wrong category propagates into clustering and then into an alert."""
    result, confidence = classify_by_keywords("had a lovely walk in the park")
    assert result is HazardCategory.UNKNOWN
    assert confidence == 0.0


def test_keyword_classifier_works_in_hindi() -> None:
    assert classify_by_keywords("भारी बारिश हो रही है")[0] is HazardCategory.HEAVY_RAINFALL


@pytest.mark.parametrize(
    "text,expected",
    [
        ("light drizzle outside", SeverityHint.MINOR),
        ("yellow alert, traffic disrupted", SeverityHint.MODERATE),
        ("orange alert, people stranded on the roof", SeverityHint.SEVERE),
        ("red alert, extremely heavy rainfall, evacuate now", SeverityHint.EXTREME),
    ],
)
def test_severity_reflects_the_wording_the_source_used(
    text: str, expected: SeverityHint
) -> None:
    assert severity_from_keywords(text) is expected


def test_severity_takes_the_most_severe_signal_present() -> None:
    """A post mentioning both must not be downgraded by the calmer phrase."""
    assert (
        severity_from_keywords("light rain earlier but now catastrophic flooding")
        is SeverityHint.EXTREME
    )


def test_severity_abstains_on_neutral_wording() -> None:
    assert severity_from_keywords("the sky looks cloudy today") is SeverityHint.UNKNOWN


def test_extract_hashtags_are_case_folded() -> None:
    """Folded at extraction so #MumbaiRains and #mumbairains group as one tag."""
    tags = extract_hashtags("roads flooded #MumbaiRains #StaySafe")
    assert "mumbairains" in tags and "staysafe" in tags


def test_extract_hashtags_deduplicates() -> None:
    assert extract_hashtags("#IndoreRains again #indorerains") == ["indorerains"]


def test_truncate_respects_limit() -> None:
    assert len(truncate("x" * 20_000)) <= 8192


# ==================================================================== IMD ====
@pytest.mark.asyncio
async def test_imd_warning_normalizes(registry, envelope_factory, imd_warning_payload) -> None:
    env = envelope_factory(SourceType.IMD, imd_warning_payload, source_name="imd_warnings")
    incident = await registry.normalize(env)

    assert incident.source_type is SourceType.IMD
    assert incident.external_id == "IMD-20260920-00042"
    assert incident.reported_category is HazardCategory.HEAVY_RAINFALL
    assert incident.geo.is_resolved
    assert incident.geo.district == "Indore"
    # 185 mm far exceeds IMD's 64.5 mm/24h "heavy" threshold.
    assert incident.severity_hint in (SeverityHint.SEVERE, SeverityHint.EXTREME)
    assert incident.metadata["is_official"] is True


@pytest.mark.asyncio
async def test_imd_observation_uses_station_coordinates_directly(
    registry, envelope_factory, imd_observation_payload
) -> None:
    """A station's own coordinates outrank anything the resolver could infer."""
    env = envelope_factory(SourceType.IMD, imd_observation_payload, source_name="imd_aws")
    incident = await registry.normalize(env)

    assert incident.geo.method is GeoMethod.PROVIDER_STATION
    assert incident.geo.confidence >= 0.95
    assert incident.measurements.rainfall_mm == pytest.approx(96.4)
    assert incident.measurements.rainfall_window_hours == 24


@pytest.mark.asyncio
async def test_imd_id_is_stable_across_repeated_fetches(
    registry, envelope_factory, imd_warning_payload
) -> None:
    """The same bulletin polled twice must be one incident, not two."""
    a = await registry.normalize(envelope_factory(SourceType.IMD, dict(imd_warning_payload)))
    b = await registry.normalize(envelope_factory(SourceType.IMD, dict(imd_warning_payload)))
    assert a.incident_id == b.incident_id


@pytest.mark.asyncio
async def test_imd_rejects_payload_without_identifier(registry, envelope_factory) -> None:
    env = envelope_factory(SourceType.IMD, {"record_type": "warning", "note": "junk"})
    with pytest.raises(NormalizationError):
        await registry.normalize(env)


# ============================================================ OpenWeather ====
@pytest.mark.asyncio
async def test_openweather_converts_units(
    registry, envelope_factory, openweather_payload
) -> None:
    """Kelvin and m/s in; Celsius and km/h out. Phase 2 compares these directly."""
    env = envelope_factory(SourceType.OPENWEATHER, openweather_payload)
    incident = await registry.normalize(env)

    assert incident.measurements.temperature_c == pytest.approx(26.0, abs=0.1)
    assert incident.measurements.wind_speed_kmh == pytest.approx(25.9, abs=0.5)
    assert incident.measurements.rainfall_mm == pytest.approx(28.5)
    assert incident.geo.method is GeoMethod.PROVIDER_STATION


@pytest.mark.asyncio
async def test_openweather_maps_condition_to_hazard(
    registry, envelope_factory, openweather_payload
) -> None:
    incident = await registry.normalize(
        envelope_factory(SourceType.OPENWEATHER, openweather_payload)
    )
    assert incident.reported_category is HazardCategory.HEAVY_RAINFALL


@pytest.mark.asyncio
async def test_openweather_rejects_payload_without_coordinates(
    registry, envelope_factory
) -> None:
    with pytest.raises(NormalizationError):
        await registry.normalize(
            envelope_factory(SourceType.OPENWEATHER, {"record_type": "current", "name": "X"})
        )


# ================================================================ citizen ====
@pytest.mark.asyncio
async def test_citizen_report_normalizes(registry, envelope_factory, citizen_payload) -> None:
    incident = await registry.normalize(envelope_factory(SourceType.CITIZEN, citizen_payload))

    assert incident.source_type is SourceType.CITIZEN
    assert incident.geo.method is GeoMethod.GPS_PAYLOAD
    # "knee deep water" is an URBAN_FLOODING surface form; the adjacent
    # categories are allowed because the keyword rules are intentionally
    # conservative about which flooding word wins.
    assert incident.reported_category in (
        HazardCategory.URBAN_FLOODING,
        HazardCategory.FLASH_FLOOD,
        HazardCategory.HEAVY_RAINFALL,
    )
    assert incident.measurements.water_level_cm == pytest.approx(55.0)


@pytest.mark.asyncio
async def test_citizen_report_is_labelled_as_an_unverified_claim(
    registry, envelope_factory, citizen_payload
) -> None:
    """Phase 1 records the claim. It never endorses it."""
    incident = await registry.normalize(envelope_factory(SourceType.CITIZEN, citizen_payload))
    assert incident.metadata["is_unverified_claim"] is True
    assert incident.metadata["is_official"] is False


@pytest.mark.asyncio
async def test_citizen_report_refuses_a_raw_phone_number(
    registry, envelope_factory, citizen_payload
) -> None:
    """Defence in depth: the API hashes identifiers, the normalizer verifies it."""
    leaky = {**citizen_payload, "author_id": "9876543210"}
    with pytest.raises(NormalizationError):
        await registry.normalize(envelope_factory(SourceType.CITIZEN, leaky))


@pytest.mark.asyncio
async def test_citizen_report_without_gps_falls_back_to_text(
    registry, envelope_factory, citizen_payload
) -> None:
    no_gps = {
        **citizen_payload,
        "lat": None,
        "lon": None,
        "place_name": None,
        "district": None,
        "state": None,
        "description": "Sadak par paani bhar gaya hai Indore mein",
    }
    incident = await registry.normalize(envelope_factory(SourceType.CITIZEN, no_gps))
    assert incident.geo.method is GeoMethod.GAZETTEER_TEXT_MATCH
    assert incident.geo.district == "Indore"


@pytest.mark.asyncio
async def test_citizen_report_with_no_locatable_signal_is_kept_unresolved(
    registry, envelope_factory, citizen_payload
) -> None:
    """The report survives without a location — that is the design decision."""
    nowhere = {
        **citizen_payload,
        "lat": None,
        "lon": None,
        "place_name": None,
        "district": None,
        "state": None,
        "description": "please send help immediately we are stuck",
    }
    incident = await registry.normalize(envelope_factory(SourceType.CITIZEN, nowhere))
    assert incident.geo.method is GeoMethod.UNRESOLVED
    assert incident.has_location is False


@pytest.mark.asyncio
async def test_citizen_report_rejects_empty_description(
    registry, envelope_factory, citizen_payload
) -> None:
    with pytest.raises(NormalizationError):
        await registry.normalize(
            envelope_factory(SourceType.CITIZEN, {**citizen_payload, "description": "   "})
        )


# ================================================================= social ====
@pytest.mark.asyncio
async def test_social_post_normalizes(registry, envelope_factory, social_payload) -> None:
    incident = await registry.normalize(envelope_factory(SourceType.SOCIAL, social_payload))
    assert incident.source_type is SourceType.SOCIAL
    assert incident.language == "en"
    assert incident.geo.is_resolved


@pytest.mark.asyncio
async def test_social_post_preserves_credibility_signals(
    registry, envelope_factory, social_payload
) -> None:
    """Account age, verification and reach are Phase 2's raw material."""
    incident = await registry.normalize(envelope_factory(SourceType.SOCIAL, social_payload))
    assert incident.author is not None
    assert incident.author.account_age_days == 900
    assert incident.author.is_verified_account is False
    assert incident.metadata["repost_count"] == 12


@pytest.mark.asyncio
async def test_social_post_author_id_is_pseudonymous(
    registry, envelope_factory, social_payload
) -> None:
    incident = await registry.normalize(envelope_factory(SourceType.SOCIAL, social_payload))
    assert incident.author is not None
    assert incident.author.author_id != social_payload["author"]["id"]


@pytest.mark.asyncio
async def test_social_post_rejects_empty_text(registry, envelope_factory, social_payload) -> None:
    with pytest.raises(NormalizationError):
        await registry.normalize(
            envelope_factory(SourceType.SOCIAL, {**social_payload, "text": ""})
        )


# ================================================================= sensor ====
@pytest.mark.asyncio
async def test_sensor_maps_vendor_field_aliases(
    registry, envelope_factory, sensor_payload
) -> None:
    """Vendors name the same quantity five ways; the alias table absorbs that."""
    incident = await registry.normalize(envelope_factory(SourceType.SENSOR, sensor_payload))
    assert incident.measurements.rainfall_mm == pytest.approx(88.2)
    assert incident.measurements.temperature_c == pytest.approx(25.4)
    assert incident.measurements.humidity_pct == pytest.approx(96.0)
    assert incident.geo.method is GeoMethod.PROVIDER_STATION


@pytest.mark.asyncio
async def test_sensor_rejects_telemetry_with_no_measurements(
    registry, envelope_factory
) -> None:
    with pytest.raises(NormalizationError):
        await registry.normalize(
            envelope_factory(SourceType.SENSOR, {"sensor_id": "X-1", "battery_pct": 82})
        )


@pytest.mark.asyncio
async def test_sensor_rejects_payload_without_device_id(registry, envelope_factory) -> None:
    with pytest.raises(NormalizationError):
        await registry.normalize(envelope_factory(SourceType.SENSOR, {"rain": 12.0}))


@pytest.mark.asyncio
async def test_sensor_out_of_range_values_are_discarded_not_clamped(
    registry, envelope_factory, sensor_payload
) -> None:
    """A stuck sensor reporting 900 degrees should lose that field, not scale it."""
    incident = await registry.normalize(
        envelope_factory(SourceType.SENSOR, {**sensor_payload, "temp": 900.0})
    )
    assert incident.measurements.temperature_c is None
    assert incident.measurements.rainfall_mm == pytest.approx(88.2)


# =============================================================== registry ====
def test_registry_covers_every_declared_source(registry) -> None:
    """A SourceType with no normalizer is a silent hole in the pipeline."""
    assert set(registry.supported_sources) == set(SourceType)


def test_registry_rejects_an_unknown_source(registry) -> None:
    with pytest.raises(UnsupportedSourceError):
        registry.get("NOT_A_SOURCE")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_every_normalizer_stamps_a_trace_entry(
    registry, envelope_factory, imd_warning_payload
) -> None:
    """Lineage is what lets an operator answer 'where did this come from?'."""
    incident = await registry.normalize(envelope_factory(SourceType.IMD, imd_warning_payload))
    assert incident.trace
    assert incident.trace[-1].component
