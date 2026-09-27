# National Weather Big Data Analytics Platform

**SIH26069 — Disaster Management Track**

A unified ingestion and intelligence pipeline for India's weather and disaster
signal: official IMD bulletins and AWS/ARG station readings, open weather APIs,
partner sensor telemetry, multi-lingual social chatter, and citizen reports from
the ground — merged into one schema, one map, one triage queue.

The guiding principle is stated up front because it shapes every design decision
below:

> **AI prioritises suspicious and high-severity reports for human verification
> using multi-signal evidence. It does not claim autonomous accuracy.**

Nothing in this system marks a report "true". It ranks what a human should look
at first, and shows the evidence for that ranking.

---

## Current status

| Phase | Scope | State |
|---|---|---|
| **1** | Data ingestion & normalization | **Complete — this build** |
| 2 | Hazard classification, dedup, credibility scoring | Designed, not built |
| 3 | Spatio-temporal fusion into disaster events | Designed, not built |
| 4 | Core APIs + CAP 1.2 alert feeds | Designed, not built |
| 5 | NDMA/SDMA control room + citizen portal | Designed, not built |

Phase 1 is a working pipeline, not a scaffold. Five heterogeneous sources enter,
one validated contract leaves, and the whole thing runs offline with no API keys.

---

## What Phase 1 does

```
  IMD warnings + AWS stations ─┐
  OpenWeather current + alerts ─┤
  Partner sensors (rain/river) ─┼──> raw-weather-stream ──> normalizer ──┬──> normalized-incident-stream
  Social feed (simulated) ──────┤         (verbatim)         (fan-in)    │         (unified contract)
  Citizen reports (REST) ───────┘                                        ├──> geo-unresolved-incidents
                                                                         └──> incident-dead-letter
```

The normalizer is the only place five payload shapes become one. Everything
downstream — Phase 2's credibility model, Phase 3's clustering, Phase 5's map —
reads exclusively from `normalized-incident-stream` and never learns that IMD
reports rainfall in millimetres while OpenWeather reports temperature in Kelvin.

---

## Quickstart

```bash
cp .env.example .env
make up          # Kafka (KRaft), Postgres+PostGIS, Redis, API, 4 workers
make health      # wait ~30s for the broker to settle
```

No credentials are needed. `IMD_MOCK_MODE` and `OPENWEATHER_MOCK_MODE` default
to true, and the social simulator generates traffic immediately, so the pipeline
starts producing incidents within a minute of boot.

Watch the deliverable:

```bash
make stream      # tail normalized-incident-stream
make logs-normalizer
```

Submit a report:

```bash
make report          # with GPS
make report-nogeo    # without — exercises the geo fallback chain
make report-photo PHOTO=./flood.jpg
```

Interactive docs at <http://localhost:8000/docs>. `make up-dev` adds Kafka UI on
:8080. `make help` lists every target.

### Running without Docker

```bash
cd backend
pip install -r requirements.txt
KAFKA_ENABLED=false REDIS_ENABLED=false uvicorn app.main:app --reload
```

With `KAFKA_ENABLED=false` the producer writes to an in-process bus and the
entire pipeline runs broker-free. This is not only a convenience — it is the
fallback path the demo survives on if Kafka dies.

---

## API

All ingest routes require `X-API-Key`. Health routes deliberately do not: an ops
probe that needs a credential is an ops probe that silently stops working.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/incidents/report` | Citizen report (JSON) |
| `POST` | `/api/v1/incidents/report-with-media` | Citizen report + photo (multipart) |
| `GET` | `/api/v1/healthz` | Liveness — touches no dependency |
| `GET` | `/api/v1/readyz` | Readiness — per-dependency status |
| `GET` | `/api/v1/info` | Topics, sources, provider modes |

```bash
curl -X POST http://localhost:8000/api/v1/incidents/report \
  -H "X-API-Key: dev-citizen-key" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Knee deep water near Rajwada, cars are stuck",
    "lat": 22.7196, "lon": 75.8577, "location_accuracy_m": 9,
    "district": "Indore", "state": "Madhya Pradesh"
  }'
```

```json
{
  "incident_id": "a3f1...",
  "accepted": true,
  "location_resolved": true,
  "geo_method": "GPS_PAYLOAD",
  "place_label": "Indore, Madhya Pradesh",
  "queued_to_topic": "raw-weather-stream",
  "message": "Report received and queued for verification."
}
```

The response says *received*, never *verified*. A citizen whose unverified claim
is echoed back as confirmed will reasonably assume help is coming.

Photo uploads run through EXIF extraction, so a report submitted with no
coordinates but with an unstripped photo still lands on the map:

```bash
curl -X POST http://localhost:8000/api/v1/incidents/report-with-media \
  -H "X-API-Key: dev-citizen-key" \
  -F 'report={"description":"Underpass fully submerged","place_name":"Indore"}' \
  -F "photo=@flood.jpg"
```

---

## Design decisions worth defending

**Unlocatable reports are kept and flagged, never dropped.** A report arriving
without GPS disproportionately comes from an old handset, a dying battery, or a
photo stripped by a messaging app — which is to say, from the people a disaster
platform exists to serve. They are published to `normalized-incident-stream`
*and* fanned to `geo-unresolved-incidents`, where an operator can usually place
them by hand from the text.

**Geography records its own provenance.** Every incident carries a `geo.method`
and a `geo.uncertainty_radius_km`. A GPS fix (50 m) and a state centroid (200 km)
are both "located", and Phase 3 must never cluster them as though they were the
same claim. The seven-step chain is: payload GPS → EXIF GPS → structured
place/district field → gazetteer match in free text → remote geocoder (opt-in) →
state centroid → `UNRESOLVED`.

**The resolver refuses to guess.** A tweet naming two places more than 100 km
apart resolves to neither. A district that disagrees with its stated state
("Aurangabad, Bihar") is skipped rather than silently snapped to Maharashtra.
Below a 0.82 similarity floor, `UNRESOLVED` wins. Mislocating a report costs more
than not locating it.

**Keyword classification abstains.** The Phase 1 hazard rules are deliberately
high-precision and low-recall, returning `UNKNOWN` rather than a plausible guess.
A wrong category propagates into clustering and then into an alert; Phase 2's
zero-shot classifier refines the `UNKNOWN`s with a calibrated score.

**Mock providers are functional, not decorative.** They emit the provider's true
response shape — Kelvin, m/s, nested `rain.1h` — so the normalizers are genuinely
exercised rather than bypassed. The social simulator additionally labels its
misinformation (`fabricated_dam_breach`, `recycled_old_footage`, and three more)
via a `synthetic_label` key, giving Phase 2's credibility model labelled
negatives to evaluate against. In production that key is simply absent.

**Reposts mutate.** The simulator recirculates recent posts with prefixes,
rewording, and word-order shuffles. Exact hashing cannot solve Phase 2's
deduplication problem for it — that is the point.

**Identifiers are pseudonymised at the door.** Phone numbers and device ids never
reach storage; an HMAC-SHA256 digest gives Phase 2 the cross-report linkability
it needs ("this handle has filed six reports today") without the platform holding
PII. The citizen normalizer asserts this independently and refuses any payload
still carrying a raw phone number.

**Redis fails open.** Rate limiting and submit-idempotency degrade to permissive
when the cache is unreachable. A Redis outage must never be the reason a genuine
emergency report is rejected. `/readyz` reports this as `degraded` with an
explicit impact string, and does not gate traffic on it.

**Rate limiting keys on credential, not IP.** Carrier-grade NAT on Indian mobile
networks puts a large share of genuine reporters behind shared addresses; IP
keying would let one abusive client exhaust everyone else's quota.

**Incident ids are deterministic.** `uuid5(namespace, "SOURCE:external_id")`
means a poller restart, a consumer replay, or an at-least-once redelivery
collapses onto one incident instead of multiplying it.

**The raw envelope is preserved verbatim.** A normalizer bug becomes a replay,
not a data loss.

---

## Layout

```
sih26069/
├── docker-compose.yml          # Kafka (KRaft), Postgres+PostGIS, Redis, ClickHouse, API, workers
├── Makefile                    # make help
├── .env.example
├── infra/
│   ├── kafka/create-topics.sh  # idempotent; auto-create is disabled on purpose
│   └── postgres/init/001_schema.sql
└── backend/
    ├── Dockerfile              # multi-stage, non-root
    ├── requirements.txt
    ├── pytest.ini
    ├── app/
    │   ├── main.py             # app factory, lifespan, correlation IDs, error mapping
    │   ├── core/               # config, logging, errors, redis, security
    │   ├── schemas/            # NormalizedIncident, RawEnvelope, enums
    │   ├── geo/                # gazetteer (137 places), EXIF, resolution chain
    │   ├── messaging/          # producer, consumer, topics, in-memory bus
    │   ├── normalization/      # one module per source + registry
    │   ├── providers/          # IMD, OpenWeather, social simulator
    │   ├── workers/            # 3 pollers + the normalizer fan-in
    │   └── api/v1/             # ingest, health
    └── tests/
```

### Kafka topics

| Topic | Partitions | Retention | Role |
|---|---|---|---|
| `raw-weather-stream` | 6 | 72h | Verbatim provider payloads |
| `normalized-incident-stream` | 6 | 7d | **The unified contract** |
| `geo-unresolved-incidents` | 3 | 7d | Manual location triage |
| `incident-dead-letter` | 1 | 14d | Refused messages, replayable |

Partition keys on the normalized stream are `geo:{lat:.1f},{lon:.1f}` — an ~11 km
grid — so co-located reports land together and Phase 3's clustering gets locality
for free.

### The unified contract

`NormalizedIncident` carries provenance (`source_type`, `source_name`,
`external_id`), time (`observed_at` / `ingested_at` / `normalized_at`, all
timezone-aware, with clock-skew and backfill guards), space (`geo` with method,
confidence and uncertainty), content (`raw_text`, `normalized_text`, `language`,
`reported_category`, `severity_hint`), fixed-unit `measurements`, `media`, a
pseudonymous `author`, source-specific `metadata`, and a processing `trace`.

It carries no judgement. Phase 2 subclasses it into `EnrichedIncident` to add
`credibility_score`, `is_duplicate`, `duplicate_of_id` and `verification_status`,
rather than mutating this model.

---

## Tests

```bash
make test        # in-container
make test-local  # on the host
```

Roughly 120 tests across schema invariants, the gazetteer and geo fallback chain,
every normalizer (including each one's malformed-payload rejection), the
messaging round-trip, the API contract, and an end-to-end path from provider mock
through the normalizer worker to the unified stream.

Two are worth calling out. `test_schema_parity.py` compares the Python enums
against the `CREATE TYPE ... AS ENUM` statements in the Postgres DDL — adding a
`HazardCategory` without a migration otherwise stays invisible until an insert
rejects a real incident during a real event. And
`test_all_five_sources_converge_on_one_schema` asserts the actual Phase 1 claim:
five payload shapes in, one validated model out.

---

## Roadmap

**Phase 2 — AI & credibility.** Zero-shot hazard classification over the
`UNKNOWN`s; sentence-transformer embeddings with cosine similarity in a rolling
3-hour window for near-duplicate detection; a multi-signal credibility score from
base source weight (official 1.0 / verified citizen 0.8 / social 0.4),
cross-source corroboration within R km and Δt minutes, and meteorological
alignment against the nearest IMD AWS gauge. Output: `AUTO_VERIFIED`,
`FLAGGED_FOR_REVIEW`, `SUSPICIOUS` — a triage ordering, not a verdict.

**Phase 3 — Spatio-temporal fusion.** DBSCAN with a Haversine metric over spatial
ε and temporal δ, collapsing correlated incidents into unified disaster events
persisted to PostGIS as points and convex hulls.

**Phase 4 — Core APIs.** `GET /events/active` (GeoJSON, bbox-filtered),
`GET /incidents/flagged` (the NDMA/SDMA triage feed), `POST /incidents/verify`
(human-in-the-loop), `GET /analytics/summary`, plus CAP 1.2 alert feeds.

**Phase 5 — Dual portal.** An NDMA/SDMA control room (WebGL map, heatmaps,
cluster markers, severity filters, live telemetry, one-click verification
sidebar) and a citizen view (safe/danger zones, shelters, warnings, report form),
both live over WebSocket/SSE.
