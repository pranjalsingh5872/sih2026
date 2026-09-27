import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.services.ai_engine import ai_engine

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","service":"ai_worker","msg":"%(message)s"}',
    stream=sys.stdout
)
logger = logging.getLogger("ai_worker")

KAFKA_BOOTSTRAP = "kafka:29092"
INPUT_TOPIC = "normalized-incident-stream"
OUTPUT_TOPIC = "verified-incident-stream"

async def run_ai_scoring_worker():
    logger.info("Initializing AI Scoring Worker daemon...")
    consumer = None
    producer = None

    for attempt in range(1, 16):
        try:
            consumer = AIOKafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="sih26069.ai-scoring-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await consumer.start()
            await producer.start()
            logger.info(f"Connected to Kafka broker. Consuming '{INPUT_TOPIC}' -> Producing to '{OUTPUT_TOPIC}'")
            break
        except Exception as err:
            logger.warning(f"Connection attempt {attempt}/15 failed ({err}). Retrying in 2s...")
            await asyncio.sleep(2)

    if not consumer or not producer:
        logger.error("Could not establish connection to Kafka broker. Exiting.")
        sys.exit(1)

    try:
        async for msg in consumer:
            payload = msg.value
            incident_id = payload.get("incident_id", "unknown")
            text = payload.get("raw_text", "")
            source_type = payload.get("source_type", "CITIZEN")
            lat = payload.get("latitude")
            lon = payload.get("longitude")
            has_media = bool(payload.get("media_url"))
            has_gps = payload.get("location_resolved", False)

            category, cat_confidence = ai_engine.classify_event(text)

            ts = datetime.now(timezone.utc)
            is_duplicate, duplicate_of = ai_engine.check_duplicate(incident_id, text, lat, lon, ts)

            credibility, status = ai_engine.compute_credibility(
                source_type=source_type,
                has_gps=has_gps,
                has_media=has_media,
                is_duplicate=is_duplicate,
                nearby_corroborations=1 if category != "GENERAL_WEATHER" else 0
            )

            enriched_payload = {
                **payload,
                "ai_event_category": category,
                "ai_category_confidence": cat_confidence,
                "is_duplicate": is_duplicate,
                "duplicate_of_incident_id": duplicate_of,
                "credibility_score": credibility,
                "verification_status": status,
                "scored_at": datetime.now(timezone.utc).isoformat()
            }

            await producer.send_and_wait(OUTPUT_TOPIC, enriched_payload)
            logger.info(f"Processed {incident_id} | Class: {category} | Credibility: {credibility} | Status: {status}")

    except Exception as e:
        logger.error(f"Fatal consumer error: {e}", exc_info=True)
    finally:
        if consumer:
            await consumer.stop()
        if producer:
            await producer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run_ai_scoring_worker())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker gracefully terminated.")