"""Gazetteer matching and the geo-resolution fallback chain.

Geography is where this pipeline earns or loses its credibility. A report
placed in the wrong district becomes a false cluster in Phase 3 and, worst
case, an alert sent to the wrong administration. So the tests here care as
much about what the resolver *refuses* to guess as about what it gets right.
"""

from __future__ import annotations

import pytest

from app.geo.exif import ExifResult
from app.geo.gazetteer import (
    haversine_km,
    normalize_place_token,
    split_compound_tokens,
)
from app.geo.geocoder import GeoResolutionInput
from app.schemas.enums import GeoMethod


# ----------------------------------------------------------- token handling --
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Mumbai", "mumbai"),
        ("  NEW   DELHI  ", "new delhi"),
        ("Bengalūru", "bengaluru"),  # Latin diacritics folded
        ("Puducherry.", "puducherry"),
    ],
)
def test_normalize_place_token(raw: str, expected: str) -> None:
    assert normalize_place_token(raw) == expected


def test_normalize_preserves_indic_matras() -> None:
    """Folding combining marks blindly would destroy Devanagari vowel signs."""
    assert normalize_place_token("इंदौर") == "इंदौर"


def test_split_compound_tokens_breaks_camel_case_hashtags() -> None:
    """`#MumbaiRains` is one token to a tokenizer and two places to a human."""
    assert "mumbai" in split_compound_tokens("#MumbaiRains").lower()


# ------------------------------------------------------------ haversine ------
def test_haversine_matches_known_distance() -> None:
    # Indore -> Bhopal is roughly 170 km great-circle.
    km = haversine_km(22.7196, 75.8577, 23.2599, 77.4126)
    assert 160 < km < 180


def test_haversine_is_zero_for_identical_points() -> None:
    assert haversine_km(22.7196, 75.8577, 22.7196, 75.8577) == pytest.approx(0.0, abs=1e-6)


# ------------------------------------------------------------- gazetteer -----
def test_gazetteer_loaded(gazetteer) -> None:
    assert len(gazetteer) > 100


def test_exact_lookup(gazetteer) -> None:
    place = gazetteer.lookup_exact("Indore")
    assert place is not None
    assert place.state == "Madhya Pradesh"


def test_legacy_english_alias_resolves(gazetteer) -> None:
    """Operators and social posts still write Bombay, Vizag, Bangalore."""
    assert gazetteer.lookup_exact("Bombay").name == "Mumbai"
    assert gazetteer.lookup_exact("Vizag").name == "Visakhapatnam"


def test_devanagari_alias_resolves(gazetteer) -> None:
    assert gazetteer.lookup_exact("इंदौर").name == "Indore"


def test_fuzzy_match_tolerates_a_common_misspelling(gazetteer) -> None:
    match = gazetteer.lookup_fuzzy("Banglore", min_similarity=0.82)
    assert match is not None
    assert match.place.name == "Bengaluru"
    assert match.similarity > 0.85


def test_fuzzy_match_refuses_nonsense(gazetteer) -> None:
    """Better UNRESOLVED than confidently wrong."""
    assert gazetteer.lookup_fuzzy("Zzzznotaplace", min_similarity=0.82) is None


def test_extract_from_text_finds_place_in_hashtag(gazetteer) -> None:
    matches = gazetteer.extract_from_text("Roads flooded #MumbaiRains stay safe")
    assert matches
    assert matches[0].place.name == "Mumbai"


def test_extract_from_text_handles_devanagari(gazetteer) -> None:
    matches = gazetteer.extract_from_text("इंदौर में भारी बारिश हो रही है")
    assert matches and matches[0].place.name == "Indore"


def test_extract_from_text_returns_nothing_for_placeless_text(gazetteer) -> None:
    assert gazetteer.extract_from_text("the weather is quite pleasant today") == []


def test_nearest_reverse_lookup(gazetteer) -> None:
    result = gazetteer.nearest(22.7196, 75.8577, max_km=50)
    assert result is not None
    place, distance_km = result
    assert place.name == "Indore"
    assert distance_km < 1.0


def test_nearest_returns_none_outside_radius(gazetteer) -> None:
    # Mid-Indian-Ocean coordinates: no Indian settlement within 50 km.
    assert gazetteer.nearest(-5.0, 75.0, max_km=50) is None


# ------------------------------------------------- resolution fallback chain --
@pytest.mark.asyncio
async def test_payload_coordinates_win(resolver) -> None:
    geo = await resolver.resolve(
        GeoResolutionInput(lat=22.7196, lon=75.8577, accuracy_m=8.0, place_name="Bhopal")
    )
    assert geo.method is GeoMethod.GPS_PAYLOAD
    assert geo.point.lat == pytest.approx(22.7196)
    assert geo.confidence > 0.9


@pytest.mark.asyncio
async def test_exif_used_when_payload_has_no_coordinates(resolver) -> None:
    geo = await resolver.resolve(
        GeoResolutionInput(
            exif=ExifResult(has_exif=True, lat=19.0760, lon=72.8777),
        )
    )
    assert geo.method is GeoMethod.EXIF_GPS
    assert geo.point.lat == pytest.approx(19.0760)


@pytest.mark.asyncio
async def test_place_name_lookup_when_no_coordinates_at_all(resolver) -> None:
    geo = await resolver.resolve(GeoResolutionInput(place_name="Indore", state="Madhya Pradesh"))
    assert geo.method is GeoMethod.PLACE_NAME_LOOKUP
    assert geo.district == "Indore"
    assert geo.uncertainty_radius_km and geo.uncertainty_radius_km > 0


@pytest.mark.asyncio
async def test_free_text_extraction_is_the_last_locating_resort(resolver) -> None:
    geo = await resolver.resolve(
        GeoResolutionInput(free_text="heavy waterlogging reported around Kochi since morning")
    )
    assert geo.method is GeoMethod.GAZETTEER_TEXT_MATCH
    assert geo.place_label and "Kochi" in geo.place_label


@pytest.mark.asyncio
async def test_state_centroid_is_a_labelled_last_resort(resolver) -> None:
    """A state centroid is tens of km wrong; it must say so, loudly."""
    geo = await resolver.resolve(GeoResolutionInput(state="Kerala"))
    assert geo.method is GeoMethod.ADMIN_CENTROID
    assert geo.confidence < 0.5
    assert geo.uncertainty_radius_km and geo.uncertainty_radius_km > 50


@pytest.mark.asyncio
async def test_unlocatable_report_is_unresolved_not_invented(resolver) -> None:
    geo = await resolver.resolve(GeoResolutionInput(free_text="please send help quickly"))
    assert geo.method is GeoMethod.UNRESOLVED
    assert geo.point is None
    assert geo.confidence == 0.0


@pytest.mark.asyncio
async def test_null_island_coordinates_fall_through_the_chain(resolver) -> None:
    """(0,0) from a GPS that never got a fix must not become a location."""
    geo = await resolver.resolve(GeoResolutionInput(lat=0.0, lon=0.0, place_name="Indore"))
    assert geo.method is GeoMethod.PLACE_NAME_LOOKUP


@pytest.mark.asyncio
async def test_coordinates_outside_india_are_kept_but_flagged(resolver) -> None:
    """A misrouted or roaming report is still data; flag it, do not drop it."""
    geo = await resolver.resolve(GeoResolutionInput(lat=25.2048, lon=55.2708))  # Dubai
    assert geo.point is not None
    assert geo.outside_india is True


@pytest.mark.asyncio
async def test_bbox_covers_neighbouring_terrain_without_false_flagging(resolver) -> None:
    """The box is generous on purpose — border districts must not be flagged."""
    geo = await resolver.resolve(GeoResolutionInput(lat=27.7172, lon=85.3240))
    assert geo.outside_india is False


@pytest.mark.asyncio
async def test_resolution_is_idempotent(resolver) -> None:
    data = GeoResolutionInput(place_name="Indore")
    first = await resolver.resolve(data)
    second = await resolver.resolve(data)
    assert first.model_dump() == second.model_dump()
