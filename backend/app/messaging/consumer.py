"""Async Kafka consumer with at-least-once delivery.

Offsets are committed **after** the handler succeeds, never before. The
trade-off is deliberate: a crash mid-batch replays a few messages rather than
losing them, and the deterministic ``incident_id`` in the schema means a replay
collapses back onto the same record instead of inflating report counts.

Error handling splits on :class:`PermanentError` vs :class:`RetryableError`:
a payload that will never parse goes straight to the dead-letter topic and the
offset advances; a transient failure backs off and retries the same message.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

import orjson
from aiokafka import AIOKafkaConsumer, ConsumerRecord
from aiokafka.errors import KafkaError

from app.core.config import Settings, get_settings
from app.core.errors import PermanentError, RetryableError
from app.core.logging import get_logger, set_correlation_id
from app.messaging.producer import EventProducer, get_producer
from app.messaging.topics import Topics
from app.schemas.enums import PipelineStage
from app.schemas.raw import DeadLetter

logger = get_logger(__name__)

MessageHandler = Callable[[dict[str, Any], ConsumerRecord], Awaitable[None]]


class EventConsumer:
    """Consume a topic and dispatch decoded payloads to a handler."""

    def __init__(
        self,
        topic: str,
        group_id: str,
        handler: MessageHandler,
        *,
        settings: Settings | None = None,
        producer: EventProducer | None = None,
        max_retries: int = 3,
        retry_delay_s: float = 1.0,
    ) -> None:
        self._topic = topic
        self._group_id = group_id
        self._handler = handler
        self._settings = settings or get_settings()
        self._producer = producer or get_producer()
        self._max_retries = max_retries
        self._retry_delay_s = retry_delay_s

        self._consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._processed = 0
        self._dead_lettered = 0
        self._errors = 0

    # ------------------------------------------------------------ lifecycle --
    async def start(self) -> None:
        if not self._settings.kafka_enabled:
            raise RuntimeError(
                "EventConsumer requires Kafka. With KAFKA_ENABLED=false, wire "
                "handlers directly to the in-memory bus instead."
            )

        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._settings.kafka_bootstrap_list,
            group_id=f"{self._settings.kafka_consumer_group_prefix}.{self._group_id}",
            client_id=f"{self._settings.kafka_client_id_prefix}-{self._group_id}",
            # Manual commit is the whole point — see module docstring.
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=self._settings.kafka_consumer_max_poll_records,
            session_timeout_ms=self._settings.kafka_consumer_session_timeout_ms,
            heartbeat_interval_ms=min(
                10_000, self._settings.kafka_consumer_session_timeout_ms // 3
            ),
        )
        await self._consumer.start()
        self._running = True
        logger.info(
            "Consumer started",
            extra={"topic": self._topic, "group": self._group_id},
        )

    async def stop(self) -> None:
        self._running = False
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except KafkaError as exc:
                logger.warning("Consumer shutdown error", extra={"error": str(exc)})
            self._consumer = None
        logger.info(
            "Consumer stopped",
            extra={
                "topic": self._topic,
                "processed": self._processed,
                "dead_lettered": self._dead_lettered,
                "errors": self._errors,
            },
        )

    @property
    def stats(self) -> dict[str, int]:
        return {
            "processed": self._processed,
            "dead_lettered": self._dead_lettered,
            "errors": self._errors,
        }

    # ----------------------------------------------------------------- loop --
    async def run(self) -> None:
        """Consume until :meth:`stop` is called or the task is cancelled."""
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None

        try:
            while self._running:
                try:
                    batches = await self._consumer.getmany(timeout_ms=1000, max_records=200)
                except KafkaError as exc:
                    self._errors += 1
                    logger.error("Poll failed; backing off", extra={"error": str(exc)})
                    await asyncio.sleep(2.0)
                    continue

                if not batches:
                    continue

                for _partition, records in batches.items():
                    for record in records:
                        await self._process_record(record)

                try:
                    await self._consumer.commit()
                except KafkaError as exc:
                    # A failed commit means redelivery, which is safe here.
                    logger.warning("Offset commit failed", extra={"error": str(exc)})

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled", extra={"topic": self._topic})
            raise
        finally:
            await self.stop()

    # ------------------------------------------------------------ per-record --
    async def _process_record(self, record: ConsumerRecord) -> None:
        set_correlation_id(self._extract_correlation_id(record))

        try:
            payload = orjson.loads(record.value)
        except orjson.JSONDecodeError as exc:
            await self._send_to_dead_letter(
                record, exc, "INVALID_JSON", PipelineStage.NORMALIZATION
            )
            return

        if not isinstance(payload, dict):
            await self._send_to_dead_letter(
                record, TypeError("payload is not an object"),
                "INVALID_PAYLOAD", PipelineStage.NORMALIZATION,
            )
            return

        for attempt in range(1, self._max_retries + 1):
            try:
                await self._handler(payload, record)
                self._processed += 1
                return
            except PermanentError as exc:
                # Retrying will produce the identical failure. Stop immediately.
                await self._send_to_dead_letter(
                    record, exc, exc.error_code, PipelineStage.NORMALIZATION
                )
                return
            except RetryableError as exc:
                self._errors += 1
                if attempt == self._max_retries:
                    await self._send_to_dead_letter(
                        record, exc, exc.error_code, PipelineStage.NORMALIZATION
                    )
                    return
                await asyncio.sleep(self._retry_delay_s * attempt)
                logger.warning(
                    "Handler retry",
                    extra={"topic": self._topic, "attempt": attempt, "error": str(exc)},
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # unexpected: treat as permanent, but shout
                self._errors += 1
                logger.exception(
                    "Unhandled handler exception",
                    extra={"topic": self._topic, "offset": record.offset},
                )
                await self._send_to_dead_letter(
                    record, exc, "UNHANDLED_EXCEPTION", PipelineStage.NORMALIZATION
                )
                return

    async def _send_to_dead_letter(
        self,
        record: ConsumerRecord,
        error: Exception,
        error_code: str,
        stage: PipelineStage,
    ) -> None:
        try:
            raw = record.value.decode("utf-8", "replace") if record.value else ""
        except Exception:
            raw = "<undecodable>"

        dead_letter = DeadLetter(
            stage=stage,
            error_code=error_code,
            error_type=type(error).__name__,
            error_detail=str(error)[:4096],
            payload={
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "raw": raw[:4000],
            },
        )
        await self._producer.publish(Topics.DEAD_LETTER, dead_letter,
                                     dead_letter_on_failure=False)
        self._dead_lettered += 1
        logger.error(
            "Message dead-lettered",
            extra={
                "topic": record.topic,
                "offset": record.offset,
                "error_code": error_code,
                "error": str(error)[:500],
            },
        )

    @staticmethod
    def _extract_correlation_id(record: ConsumerRecord) -> str | None:
        for key, value in record.headers or ():
            if key == "correlation-id" and value:
                return value.decode("utf-8", "replace")
        return None
