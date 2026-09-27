-- ===========================================================================
-- SIH26069 — National Weather Big Data Analytics Platform
-- Phase 1 schema: extensions, enums, and the normalized incident landing table.
--
-- Phase 2 adds enrichment columns (credibility, dedup); Phase 3 adds the
-- unified `disaster_events` table and the incident -> event foreign key.
-- ===========================================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;      -- fuzzy place-name / text search
CREATE EXTENSION IF NOT EXISTS btree_gist;   -- composite spatio-temporal indexes

CREATE SCHEMA IF NOT EXISTS weather;
SET search_path TO weather, public;

-- --------------------------------------------------------------------------
-- Enumerated domains. Kept in sync with app/schemas/enums.py.
-- --------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE source_type_enum AS ENUM (
        'IMD', 'OPENWEATHER', 'CITIZEN', 'SOCIAL', 'SENSOR'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE geo_method_enum AS ENUM (
        'GPS_PAYLOAD', 'EXIF_GPS', 'PROVIDER_STATION', 'PLACE_NAME_LOOKUP',
        'GAZETTEER_TEXT_MATCH', 'REMOTE_GEOCODER', 'ADMIN_CENTROID', 'UNRESOLVED'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE hazard_category_enum AS ENUM (
        'HEAVY_RAINFALL', 'FLASH_FLOOD', 'URBAN_FLOODING', 'THUNDERSTORM',
        'LIGHTNING', 'CYCLONE', 'HEATWAVE', 'COLDWAVE', 'DENSE_FOG',
        'DUST_STORM', 'HAILSTORM', 'LANDSLIDE', 'SNOWFALL', 'UNKNOWN'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- --------------------------------------------------------------------------
-- Landing table for the normalized-incident-stream.
-- Written by the Phase 3 sink; Phase 1 defines the contract up front so the
-- Pydantic model and the relational model never drift.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS incidents (
    incident_id        UUID PRIMARY KEY,
    schema_version     SMALLINT        NOT NULL DEFAULT 1,

    source_type        source_type_enum NOT NULL,
    source_name        TEXT             NOT NULL,
    external_id        TEXT,

    observed_at        TIMESTAMPTZ      NOT NULL,
    ingested_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    normalized_at      TIMESTAMPTZ      NOT NULL DEFAULT now(),

    -- SRID 4326 (WGS84). Nullable: geo-unresolved reports are still stored so
    -- operators can triage them manually rather than losing the signal.
    geom               GEOGRAPHY(Point, 4326),
    geo_method         geo_method_enum  NOT NULL DEFAULT 'UNRESOLVED',
    geo_confidence     REAL             NOT NULL DEFAULT 0.0
                        CHECK (geo_confidence >= 0.0 AND geo_confidence <= 1.0),
    admin_district     TEXT,
    admin_state        TEXT,
    place_label        TEXT,

    raw_text           TEXT,
    normalized_text    TEXT,
    language           TEXT,

    reported_category  hazard_category_enum NOT NULL DEFAULT 'UNKNOWN',
    measurements       JSONB            NOT NULL DEFAULT '{}'::jsonb,
    media              JSONB            NOT NULL DEFAULT '[]'::jsonb,
    author             JSONB,
    metadata           JSONB            NOT NULL DEFAULT '{}'::jsonb,

    content_hash       CHAR(64)         NOT NULL,

    CONSTRAINT incidents_external_uniq UNIQUE NULLS NOT DISTINCT (source_type, external_id)
);

-- Spatial index for point-in-polygon and radius queries (Phase 2 corroboration,
-- Phase 4 bounding-box dashboard reads).
CREATE INDEX IF NOT EXISTS idx_incidents_geom       ON incidents USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_incidents_observed   ON incidents (observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_source     ON incidents (source_type, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_hash       ON incidents (content_hash);
CREATE INDEX IF NOT EXISTS idx_incidents_category   ON incidents (reported_category, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_text_trgm  ON incidents USING GIN (normalized_text gin_trgm_ops);
-- Composite spatio-temporal index: the access pattern for "k nearest reports
-- from distinct sources within R km and Δt minutes" in Phase 2.
CREATE INDEX IF NOT EXISTS idx_incidents_geom_time  ON incidents USING GIST (geom, observed_at);

-- --------------------------------------------------------------------------
-- Offline gazetteer, mirrored from app/geo/data/india_gazetteer.json so SQL
-- side joins (Phase 3 event naming) do not need an application round-trip.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gazetteer (
    place_id     SERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    normalized   TEXT NOT NULL,
    state        TEXT NOT NULL,
    district     TEXT,
    population   INTEGER,
    geom         GEOGRAPHY(Point, 4326) NOT NULL,
    aliases      TEXT[] NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_gazetteer_geom ON gazetteer USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_gazetteer_name ON gazetteer USING GIN (normalized gin_trgm_ops);

-- --------------------------------------------------------------------------
-- Append-only audit of every pipeline rejection. A report that fails schema
-- validation must never silently vanish — the dead-letter topic is mirrored
-- here for operator review.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_dead_letters (
    id            BIGSERIAL PRIMARY KEY,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    stage         TEXT        NOT NULL,
    source_type   TEXT,
    error_type    TEXT        NOT NULL,
    error_detail  TEXT,
    payload       JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dlq_occurred ON ingest_dead_letters (occurred_at DESC);
