import asyncio
import json
import logging
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.services.ai_engine import ai_engine

logging.basicConfig(level=logging.INFO, format='{"ts":"%(asctime)s","service":"ai_worker","msg":"%(message)s"}')
logger = logging.getLogger("ai_worker")

KAFKA_BOOTSTRAP = "kafka:29092"
INPUT_TOPIC = "normalized-incident-stream"
OUTPUT_TOPIC = "verified-incident-stream"

async def run_ai_scoring_worker():
    loop = asyncio.get_event_loop()
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
    logger.info(f"AI Worker started. Consuming '{INPUT_TOPIC}' -> Producing to '{OUTPUT_TOPIC}'")

    try:
        async for msg in consumer:
            payload = msg.value
            incident_id = payload.get("incident_id")
            text = payload.get("raw_text", "")
            source_type = payload.get("source_type", "CITIZEN")
            lat = payload.get("latitude")
            lon = payload.get("longitude")
            has_media = bool(payload.get("media_url"))
            has_gps = payload.get("location_resolved", False)

            # 1. Weather Event Classification
            category, cat_confidence = ai_engine.classify_event(text)

            # 2. Semantic Deduplication
            ts = datetime.now(timezone.utc)
            is_duplicate, duplicate_of = ai_engine.check_duplicate(incident_id, text, lat, lon, ts)

            # 3. Multi-Signal Credibility Calculation
            credibility, status = ai_engine.compute_credibility(
                source_type=source_type,
                has_gps=has_gps,
                has_media=has_media,
                is_duplicate=is_duplicate,
                nearby_corroborations=1 if category != "GENERAL_WEATHER" else 0
            )

            # 4. Construct Verified Output Schema
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

            # Forward to verified pipeline
            await producer.send_and_wait(OUTPUT_TOPIC, enriched_payload)
            logger.info(f"Enriched {incident_id} | Class: {category} | Credibility: {credibility} | Status: {status}")

    except Exception as e:
        logger.error(f"Worker execution failed: {e}")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(run_ai_scoring_worker())