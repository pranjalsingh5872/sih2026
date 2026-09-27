# AAGAHI 2026 — National Weather Big Data Analytics Platform

**SIH 2026 — Disaster Management Track**  
*Unified Multi-Source Weather Ingestion, AI Credibility Scoring, PostGIS Pan-India Clustering & Real-Time Emergency Operations Center*

---

## 🌟 Executive Overview

**AAGAHI 2026** is a mission-critical big data intelligence and emergency response platform designed for national disaster management authorities (NDMA, SDMAs, and district command centers). It unifies heterogeneous, multi-velocity data streams across India:
- **Official Meteorological Telemetry**: IMD bulletins, AWS/ARG automatic weather stations, and radar alerts.
- **Global Weather Sensors**: OpenWeather current telemetry, precipitation indices, and wind vectors.
- **Citizen Ground Intelligence**: Multilingual mobile citizen reports with EXIF GPS photo extraction.
- **Social Chatter & Sensor Feeds**: High-velocity social signals, river gauges, and partner sensor telemetry.

All incoming signals are validated into a **single unified contract**, scored using **AI multi-signal credibility**, clustered into **active disaster hotspots using PostGIS DBSCAN**, and rendered in a **modern light-theme command center interface** with live mobilization controls.

---

## 🚀 Live System Credentials & Connection Settings

Below are the default developer and command center credentials configured across the environment:

| Component | Host / Endpoint | Port | Credentials / Keys | Role / Notes |
|:---|:---|:---|:---|:---|
| **Web Dashboard** | `http://localhost:8000/` | `8000` | *Public Access (No Auth)* | Modern Light-Theme Ops Center |
| **API Docs (Swagger)** | `http://localhost:8000/docs` | `8000` | *Public Access (No Auth)* | Interactive OpenAPI documentation |
| **Health Probe** | `http://localhost:8000/healthz` | `8000` | *Public Access (No Auth)* | Liveness probe (`{"status":"alive"}`) |
| **Readiness Probe** | `http://localhost:8000/readyz` | `8000` | *Public Access (No Auth)* | Deep health checks (DB, Redis, Kafka) |
| **Citizen Ingest API** | `POST /api/v1/incidents/report` | `8000` | `X-API-Key: dev-citizen-key` | Citizen report ingestion |
| **Media Ingest API** | `POST /api/v1/incidents/report-with-media`| `8000` | `X-API-Key: dev-citizen-key` | Photo + EXIF report ingestion |
| **Admin & Dispatch** | `POST /api/v1/analytics/dispatch` | `8000` | `X-API-Key: dev-admin-key` | Emergency mobilization triggers |
| **PostgreSQL + PostGIS**| `localhost` (in Docker: `postgres`)| `5432` | `User: weather`<br>`Password: weather_dev_pw`<br>`Database: weatherdb` | Spatial GIS storage, DBSCAN clustering, hazard polygons |
| **Redis Cache** | `localhost` (in Docker: `redis`) | `6379` | `redis://localhost:6379/0`<br>*(No Password in dev)* | Ingestion rate limiting, deduplication, cache |
| **Kafka (KRaft)** | `localhost` (in Docker: `kafka`) | `9092` | Broker: `localhost:9092`<br>Container: `kafka:29092` | Stream buffer (72h retention, 6 partitions) |

---

## 🗺️ Pan-India Disaster & Weather Anomaly Coverage

The platform continuously aggregates and clusters **48+ active national disaster and weather abnormality hotspots** across India with exact district and landmark spatial identification:

```
                                  [Jammu & Kashmir / Ladakh]
                            • Srinagar (Jhelum Basin)
                            • Ramban NH-44 (Mehar / Panthyal)
                                      |
                           [Himachal & Uttarakhand]
                     • Kullu & Beas Valley (Manali)
                     • Rudraprayag (Kedarnath / Gaurikund)
                     • Chamoli (Joshimath / Dhauliganga)
                                      |
        [Gujarat / Kutch]                     [Gangetic & Eastern Plains]
• Mandvi & Jakhau (Cyclone)            • Patna, Bhagalpur & Naugachia (Floods)
• Surat & Tapi Basin                   • Ranchi & Subarnarekha Basin
                                       • Mayurbhanj & Similipal (Thunderstorms)
                     \                        /
                      \                      /
           [Central & Western Ghats]        [Northeast India]
       • Narmadapuram (Tawa / Narmada) • Majuli Island (Brahmaputra)
       • Raigad (Mahad / Poladpur)     • Kamrup & Guwahati (Assam)
       • Ratnagiri & Chiplun           • Gomati River & Amarpur (Tripura)
       • Dakshina Kannada (Mangalore)
                     |
               [South India]
       • Wayanad & Chooralmala (Landslides)
       • Idukki & Munnar Ghats
       • Alappuzha (Kuttanad Inundation)
       • Cuddalore (Kollidam Basin)
```

---

## 🏗️ System Architecture

```
 ┌────────────────┐   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
 │ IMD Bulletins  │   │  OpenWeather   │   │ Social Chatter │   │ Citizen Mobile │
 │  & AWS Gauges  │   │  Telemetry API │   │   Simulator    │   │  (EXIF Photos) │
 └────────┬───────┘   └────────┬───────┘   └────────┬───────┘   └────────┬───────┘
          │                    │                    │                    │
          └────────────────────┼────────────────────┴────────────────────┘
                               ▼
                   [ raw-weather-stream ] (Kafka 72h)
                               │
                               ▼
                   ┌───────────────────────┐
                   │   Normalizer Worker   │ ◄── 137-Place Gazetteer
                   └───────────┬───────────┘
                               ▼
               [ normalized-incident-stream ] (Geo-partitioned)
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
┌───────────────────────┐             ┌───────────────────────┐
│       AI Worker       │             │    Cluster Worker     │
│ Zero-Shot Classifier  │             │   PostGIS ST_Cluster  │
│  Credibility Scorer   │             │   DBSCAN + ST_Buffer  │
└───────────┬───────────┘             └───────────┬───────────┘
            │                                     │
            └──────────────────┬──────────────────┘
                               ▼
               ┌───────────────────────────────┐
               │ PostgreSQL 16 + PostGIS 3.4   │
               │ (Hotspots, Hulls, Incidents)  │
               └───────────────┬───────────────┘
                               ▼
                   ┌───────────────────────┐
                   │  FastAPI Core Engine  │
                   │  (REST + GeoJSON + WS)│
                   └───────────┬───────────┘
                               ▼
               ┌───────────────────────────────┐
               │ Modern Light-Theme Ops Center │
               │ (CartoDB Positron, GIS Map)   │
               └───────────────────────────────┘
```

---

## ⚡ Quickstart

### 1. Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v24+)
- Python 3.11+ (for running tests or local scripts)
- Git

### 2. Launch with Docker Compose
Clone the repository and launch the full stack (Kafka, PostGIS, Redis, API, and 6 background workers):

```bash
# Clone repository
git clone https://github.com/pranjalsingh5872/sih2026.git
cd sih2026

# Start all 10 services in background
docker compose up -d

# Verify health status of all containers
docker compose ps
```

### 3. Seed Pan-India Disaster & Weather Data
To immediately populate the system with 531+ corroborated reports across 38 national disaster zones:

```bash
docker exec -it sih-api python app/scripts/seed_pan_india_disasters.py
```

### 4. Open the Command Center
Open your browser and navigate to:
👉 **`http://localhost:8000/`**

---

## 📡 API Reference & Verification Examples

### 1. Ingest a Citizen Disaster Report
```bash
curl -X POST http://localhost:8000/api/v1/incidents/report \
  -H "X-API-Key: dev-citizen-key" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Flash flood near Chooralmala market, bridge submerged under 4 feet water",
    "lat": 11.5583,
    "lon": 76.1667,
    "district": "Wayanad",
    "state": "Kerala",
    "reported_category": "FLASH_FLOOD"
  }'
```

### 2. Fetch Active Disaster Hotspots (GeoJSON)
```bash
curl -s http://localhost:8000/api/v1/analytics/hotspots | jq '.features[0]'
```

### 3. Fetch Real-Time Emergency Alerts
```bash
curl -s http://localhost:8000/api/v1/analytics/alerts | jq '.[0]'
```

### 4. Dispatch Emergency Teams (Admin)
```bash
curl -X POST http://localhost:8000/api/v1/analytics/dispatch \
  -H "X-API-Key: dev-admin-key" \
  -H "Content-Type: application/json" \
  -d '{
    "hotspot_id": "wayanad-flash-flood-01",
    "units": ["NDRF 4th Bn", "State Disaster Response Force", "Indian Army Madras Regt"],
    "action": "DEPLOY_FLOOD_RESCUE"
  }'
```

---

## 🌐 Instant Web Presentation (Exposing to Internet)

To generate a secure public HTTPS link for evaluators and judges without deploying to costly cloud servers:

### Option A: Cloudflare Tunnel (Recommended)
```powershell
# Install Cloudflare CLI
winget install --id Cloudflare.cloudflared

# Expose your local dashboard
cloudflared tunnel --url http://localhost:8000
```
*Gives you an instant URL like `https://sih2026-ops.trycloudflare.com` accessible on any phone or laptop worldwide.*

### Option B: LocalTunnel
```powershell
npx localtunnel --port 8000
```

---

## 🧪 Automated Testing

The project maintains **100% test integrity** with 151 unit and integration tests covering:
- Spatial schema parity (PostGIS DDL vs Pydantic models)
- Normalization pipelines across all 5 data sources
- Gazetteer and EXIF GPS fallback chains
- DBSCAN spatial clustering and polygon generation
- API authentication, rate limiting, and health probes

To run tests locally:
```bash
cd backend
pytest -v
```
*(Result: `151 passed, 0 failed`)*

---

## 📂 Repository Structure

```
sih2026/
├── docker-compose.yml                  # Multi-container orchestration (10 services)
├── Makefile                            # Developer shortcuts (up, down, seed, test)
├── .env.example                        # Tunable environment variables
├── infra/
│   ├── kafka/create-topics.sh          # Idempotent topic provisioner
│   └── postgres/init/001_schema.sql    # PostGIS spatial schemas & indexes
└── backend/
    ├── Dockerfile                      # Production multi-stage Docker build
    ├── requirements.txt                # Python dependencies
    ├── pytest.ini                      # Pytest runner configuration
    ├── app/
    │   ├── main.py                     # FastAPI factory, content negotiation, routes
    │   ├── core/                       # Config, logging, Redis, rate limiting
    │   ├── schemas/                    # Pydantic models & unified contracts
    │   ├── geo/                        # 137-city gazetteer, EXIF GPS extractor
    │   ├── messaging/                  # Kafka producer, consumer, in-memory bus
    │   ├── normalization/              # Source-specific transformation engines
    │   ├── providers/                  # IMD, OpenWeather, Social Simulator
    │   ├── workers/                    # Ingestion, AI scoring, DBSCAN clustering
    │   ├── scripts/                    # Pan-India disaster seed generators
    │   ├── static/index.html           # Modern Light-Theme Ops Command Center
    │   └── api/v1/                     # Ingestion, Analytics, Alerts, Dispatch
    └── tests/                          # 151 Unit and integration tests
```

---

## ⚖️ License & Acknowledgements

Developed for **Smart India Hackathon 2026**.  
Built with FastAPI, Apache Kafka, PostgreSQL PostGIS, Redis, Leaflet, and CartoDB Positron.
