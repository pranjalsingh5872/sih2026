"""Python enums vs the Postgres DDL.

The Phase 3 sink writes these values straight into Postgres enum columns. If
someone adds ``HazardCategory.CLOUDBURST`` in Python and forgets the migration,
nothing fails until an insert rejects a real incident during a real event. So
the two definitions are compared here instead.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.schemas.enums import GeoMethod, HazardCategory, SourceType

SCHEMA_SQL = (
    Path(__file__).resolve().parents[2] / "infra" / "postgres" / "init" / "001_schema.sql"
)


def _sql_enum_values(enum_name: str) -> set[str]:
    """Pull the literal list out of a CREATE TYPE ... AS ENUM (...) statement."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    match = re.search(
        rf"CREATE\s+TYPE\s+[\w.]*\b{enum_name}\b\s+AS\s+ENUM\s*\((.*?)\)\s*;",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        pytest.fail(f"Enum {enum_name} not found in {SCHEMA_SQL.name}")
    return set(re.findall(r"'([^']+)'", match.group(1)))


def test_schema_file_exists() -> None:
    assert SCHEMA_SQL.exists(), f"Missing {SCHEMA_SQL}"


@pytest.mark.parametrize(
    "sql_enum,python_enum",
    [
        ("source_type_enum", SourceType),
        ("geo_method_enum", GeoMethod),
        ("hazard_category_enum", HazardCategory),
    ],
)
def test_python_and_sql_enums_agree(sql_enum: str, python_enum) -> None:
    sql_values = _sql_enum_values(sql_enum)
    python_values = {member.value for member in python_enum}

    missing_in_sql = python_values - sql_values
    missing_in_python = sql_values - python_values

    assert not missing_in_sql, (
        f"{python_enum.__name__} members absent from {sql_enum}: "
        f"{sorted(missing_in_sql)} — add a migration."
    )
    assert not missing_in_python, (
        f"{sql_enum} values absent from {python_enum.__name__}: "
        f"{sorted(missing_in_python)}"
    )


def test_postgis_extension_is_declared() -> None:
    """Without PostGIS the geography columns and GIST indexes never create."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8").lower()
    assert "create extension" in sql and "postgis" in sql


def test_incidents_table_has_a_spatial_index() -> None:
    """Bounding-box queries in Phase 4 are a sequential scan without it."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8").lower()
    assert "using gist" in sql


def test_kafka_topic_script_declares_every_topic() -> None:
    """Auto-create is disabled in compose, so an undeclared topic is a dead feed."""
    from app.messaging.topics import Topics

    script = (
        Path(__file__).resolve().parents[2] / "infra" / "kafka" / "create-topics.sh"
    ).read_text(encoding="utf-8")
    for topic in Topics.all():
        assert topic in script, f"Topic {topic} missing from create-topics.sh"
