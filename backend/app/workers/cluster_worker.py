import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
import asyncpg
from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","service":"cluster_worker","msg":"%(message)s"}',
    stream=sys.stdout
)
logger = logging.getLogger("cluster_worker")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INPUT_TOPIC = "verified-incident-stream"

POSTGRES_USER = os.getenv("POSTGRES_USER", "weather")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_dev_pw")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weatherdb")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

CLUSTER_CALC_QUERY = """
WITH clustered AS (
    SELECT 
        incident_id,
        COALESCE(ai_category, 'GENERAL_WEATHER') AS hazard_type,
        credibility_score,
        geom::geometry AS geom_g,
        observed_at,
        ST_ClusterDBSCAN(geom::geometry, eps := 0.75, minpoints := 1) OVER(
            PARTITION BY COALESCE(ai_category, 'GENERAL_WEATHER')
        ) AS cluster_id
    FROM weather.incidents
    WHERE geom IS NOT NULL
      AND observed_at >= NOW() - INTERVAL '7 days'
)
SELECT 
    hazard_type,
    cluster_id,
    COUNT(*) AS incident_count,
    ROUND(AVG(credibility_score)::numeric, 3) AS avg_credibility,
    ST_Centroid(ST_Collect(geom_g)) AS centroid,
    CASE 
        WHEN COUNT(*) >= 3 AND ST_GeometryType(ST_ConvexHull(ST_Collect(geom_g))) = 'ST_Polygon'
            THEN ST_ConvexHull(ST_Collect(geom_g))
        ELSE 
            ST_Buffer(ST_Centroid(ST_Collect(geom_g))::geography, 18000)::geometry
    END AS convex_hull,
    MIN(observed_at) AS first_reported,
    MAX(observed_at) AS last_reported
FROM clustered
WHERE cluster_id IS NOT NULL
GROUP BY hazard_type, cluster_id;
"""

async def get_db_pool():
    return await asyncpg.create_pool(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        min_size=2,
        max_size=10
    )

async def recalculate_hotspots(pool):
    async with pool.acquire() as conn:
        try:
            clusters = await conn.fetch(CLUSTER_CALC_QUERY)
            if not clusters:
                return

            await conn.execute("UPDATE weather.disaster_hotspots SET status = 'INACTIVE' WHERE status = 'ACTIVE';")

            for c in clusters:
                cnt = c["incident_count"]
                htype = c["hazard_type"]
                if htype in ("CYCLONE", "LANDSLIDE") or cnt >= 15:
                    sev = "CRITICAL"
                elif cnt >= 6 or htype in ("FLASH_FLOOD", "HEAVY_RAINFALL"):
                    sev = "HIGH"
                elif cnt >= 3:
                    sev = "MEDIUM"
                else:
                    sev = "LOW"

                await conn.execute("""
                    INSERT INTO weather.disaster_hotspots (
                        hazard_type, severity, incident_count, avg_credibility,
                        centroid, convex_hull, first_reported_at, last_reported_at, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'ACTIVE');
                """, htype, sev, cnt, float(c["avg_credibility"]),
                     c["centroid"], c["convex_hull"], c["first_reported"], c["last_reported"])
            logger.info(f"Generated {len(clusters)} active disaster hotspots via PostGIS ST_ClusterDBSCAN.")
        except Exception as err:
            logger.error(f"Error computing spatial hotspots: {err}")

async def run_clustering_worker():
    logger.info("Initializing Spatial Clustering Ingestion & Hotspot Worker...")
    pool = None
    for attempt in range(1, 15):
        try:
            pool = await get_db_pool()
            logger.info("Connected to PostGIS database.")
            break
        except Exception as e:
            logger.warning(f"Database connection attempt {attempt}/15 failed: {e}. Retrying...")
            await asyncio.sleep(2)

    consumer = None
    for attempt in range(1, 15):
        try:
            consumer = AIOKafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="sih26069.cluster-ingest-group-v2",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            await consumer.start()
            logger.info(f"Kafka consumer active on '{INPUT_TOPIC}'.")
            break
        except Exception as err:
            logger.warning(f"Kafka connection attempt {attempt}/15 failed: {err}. Retrying...")
            await asyncio.sleep(2)

    batch = 0
    try:
        async for msg in consumer:
            batch += 1
            payload = msg.value

            # Extract fields
            raw_id = payload.get("incident_id")
            try:
                inc_id = uuid.UUID(raw_id) if raw_id else uuid.uuid4()
            except ValueError:
                inc_id = uuid.uuid4()

            text = payload.get("raw_text") or payload.get("text") or "Weather report"
            source = payload.get("source_type", "CITIZEN").upper()
            if source not in ["CITIZEN", "SOCIAL", "IMD", "SENSOR", "OPENWEATHER"]:
                source = "CITIZEN"

            lat = payload.get("latitude")
            lon = payload.get("longitude")
            cred = float(payload.get("credibility_score", 0.5))
            status = payload.get("verification_status", "PENDING")
            category = payload.get("ai_event_category", "GENERAL_WEATHER")
            content_hash = payload.get("content_hash", str(inc_id).replace("-", "")[:64])
            source_name = payload.get("source_name", "simulator")

            async with pool.acquire() as conn:
                try:
                    if lat is not None and lon is not None:
                        await conn.execute("""
                            INSERT INTO weather.incidents (
                                incident_id, source_type, source_name, observed_at,
                                geom, geo_method, raw_text, credibility_score,
                                verification_status, ai_category, content_hash
                            ) VALUES (
                                $1, $2::weather.source_type_enum, $3, NOW(),
                                ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
                                'COORDINATES'::weather.geo_method_enum, $6, $7, $8, $9, $10
                            )
                            ON CONFLICT (incident_id) DO UPDATE
                            SET credibility_score = EXCLUDED.credibility_score,
                                verification_status = EXCLUDED.verification_status,
                                ai_category = EXCLUDED.ai_category;
                        """, inc_id, source, source_name, float(lon), float(lat), text, cred, status, category, content_hash)
                    else:
                        await conn.execute("""
                            INSERT INTO weather.incidents (
                                incident_id, source_type, source_name, observed_at,
                                geo_method, raw_text, credibility_score,
                                verification_status, ai_category, content_hash
                            ) VALUES (
                                $1, $2::weather.source_type_enum, $3, NOW(),
                                'UNRESOLVED'::weather.geo_method_enum, $4, $5, $6, $7, $8
                            )
                            ON CONFLICT (incident_id) DO UPDATE
                            SET credibility_score = EXCLUDED.credibility_score,
                                verification_status = EXCLUDED.verification_status,
                                ai_category = EXCLUDED.ai_category;
                        """, inc_id, source, source_name, text, cred, status, category, content_hash)
                except Exception as insert_err:
                    # Log unexpected insert exceptions
                    logger.debug(f"Insert skip: {insert_err}")

            if batch % 50 == 0:
                await recalculate_hotspots(pool)

    except Exception as e:
        logger.error(f"Worker runtime error: {e}", exc_info=True)
    finally:
        if consumer:
            await consumer.stop()
        if pool:
            await pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(run_clustering_worker())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker shutdown.")