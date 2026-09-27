"""Resilient async Kafka producer.

Three things this does beyond wrapping ``AIOKafkaProducer``:

* **Bounded retry with jittered backoff.** A broker leader election takes a few
  seconds; the producer should ride it out, not drop a flood report.
* **Dead-letter routing.** A message that cannot be produced after N attempts
  is preserved with its error context instead of vanishing into a log line.
* **An in-memory fallback bus.** With ``KAFKA_ENABLED=false`` the whole
  pipeline still runs end-to-end in one process. That makes the unit tests
  broker-free and gives a demo machine a path that works when the Kafka
  container refuses to start five minutes before judging.
"""

from __future__ import annotations

import asyncio
import random
from collections import defaultdict, deque
from typing import Any, Callable, Deque

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from pydantic import BaseModel

from app.core.config import Settings, get_settings
from app.core.errors import MessageBusError
from app.core.logging import get_correlation_id, get_logger
from app.messaging.topics import Topics
from app.schemas.raw import DeadLetter

logger = get_logger(__name__)


def _serialize(value: BaseModel | dict[str, Any] | bytes) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, BaseModel):
        # Pydantic's own JSON writer handles UUID/datetime/enum correctly.
        return value.model_dump_json().encode("utf-8")
    return orjson.dumps(value, default=str)


class InMemoryBus:
    """Process-local stand-in for Kafka.

    Subscribers registered by topic receive messages synchronously; anything
    unsubscribed is buffered in a bounded deque so tests can assert on it.
    """

    def __init__(self, maxlen: int = 10_000) -> None:
        self._buffers: dict[str, Deque[bytes]] = defaultdict(lambda: deque(maxlen=maxlen))
        self._subscribers: dict[str, list[Callable[[bytes, str | None], Any]]] = defaultdict(list)

    async def publish(self, topic: str, value: bytes, key: str | None = None) -> None:
        self._buffers[topic].append(value)
        for handler in self._subscribers[topic]:
            result = handler(value, key)
            if asyncio.iscoroutine(result):
                await result

    def subscribe(self, topic: str, handler: Callable[[bytes, str | None], Any]) -> None:
        self._subscribers[topic].append(handler)

    def drain(self, topic: str) -> list[bytes]:
        messages = list(self._buffers[topic])
        self._buffers[topic].clear()
        return messages

    def peek(self, topic: str) -> list[bytes]:
        return list(self._buffers[topic])

    def clear(self) -> None:
        self._buffers.clear()


_memory_bus = InMemoryBus()


def get_memory_bus() -> InMemoryBus:
    return _memory_bus


class EventProducer:
    """Application-facing publish API."""

    def __init__(self, settings: Settings | None = None, client_suffix: str = "producer") -> None:
        self._settings = settings or get_settings()
        self._client_suffix = client_suffix
        self._producer: AIOKafkaProducer | None = None
        self._started = False
        self._lock = asyncio.Lock()
        self._published = 0
        self._failed = 0
        self._dead_lettered = 0

    # ------------------------------------------------------------ lifecycle --
    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            if not self._settings.kafka_enabled:
                logger.warning("Kafka disabled; publishing to the in-memory bus")
                self._started = True
                return

            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_list,
                client_id=f"{self._settings.kafka_client_id_prefix}-{self._client_suffix}",
                acks=self._settings.kafka_producer_acks,
                linger_ms=self._settings.kafka_producer_linger_ms,
                max_batch_size=self._settings.kafka_producer_max_batch_size,
                compression_type=(
                    None
                    if self._settings.kafka_producer_compression == "none"
                    else self._settings.kafka_producer_compression
                ),
                # Exactly-once semantics at the broker for retried sends —
                # without this, a retry after a timed-out-but-successful send
                # silently duplicates the report.
                enable_idempotence=True,
                request_timeout_ms=20_000,
            )
            try:
                await self._producer.start()
            except KafkaError as exc:
                self._producer = None
                raise MessageBusError(
                    "Kafka producer failed to start",
                    bootstrap=self._settings.kafka_bootstrap_servers,
                    cause=str(exc),
                ) from exc

            self._started = True
            logger.info(
                "Kafka producer started",
                extra={"bootstrap": self._settings.kafka_bootstrap_servers},
            )

    async def stop(self) -> None:
        async with self._lock:
            if self._producer is not None:
                try:
                    await self._producer.flush()
                finally:
                    await self._producer.stop()
                self._producer = None
            self._started = False
            logger.info(
                "Kafka producer stopped",
                extra={
                    "published": self._published,
                    "failed": self._failed,
                    "dead_lettered": self._dead_lettered,
                },
            )

    @property
    def is_ready(self) -> bool:
        return self._started

    @property
    def stats(self) -> dict[str, int]:
        return {
            "published": self._published,
            "failed": self._failed,
            "dead_lettered": self._dead_lettered,
        }

    # -------------------------------------------------------------- publish --
    async def publish(
        self,
        topic: str,
        value: BaseModel | dict[str, Any],
        key: str | None = None,
        *,
        headers: dict[str, str] | None = None,
        dead_letter_on_failure: bool = True,
    ) -> bool:
        """Publish one message. Returns ``True`` when the broker acknowledged.

        Raises :class:`MessageBusError` only when the message could be neither
        delivered nor dead-lettered — at that point the caller must decide
        whether to reject the upstream request.
        """
        if not self._started:
            await self.start()

        try:
            payload = _serialize(value)
        except (TypeError, ValueError) as exc:
            # Unserializable payloads are permanent failures; never retry them.
            logger.error("Message serialization failed", extra={"topic": topic, "error": str(exc)})
            if dead_letter_on_failure:
                await self._dead_letter(topic, {"repr": repr(value)[:2000]}, exc)
            return False

        if not self._settings.kafka_enabled or self._producer is None:
            await _memory_bus.publish(topic, payload, key)
            self._published += 1
            return True

        kafka_headers = self._build_headers(headers)
        key_bytes = key.encode("utf-8") if key else None
        attempts = self._settings.kafka_producer_retry_attempts
        base_delay = self._settings.kafka_producer_retry_base_delay_s
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                await self._producer.send_and_wait(
                    topic, value=payload, key=key_bytes, headers=kafka_headers
                )
                self._published += 1
                return True
            except (KafkaError, asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                if attempt == attempts:
                    break
                # Exponential backoff with full jitter — a synchronised retry
                # storm from ten workers is how a recovering broker gets
                # knocked over a second time.
                delay = min(base_delay * (2 ** (attempt - 1)), 10.0)
                await asyncio.sleep(random.uniform(0, delay))
                logger.warning(
                    "Publish retry",
                    extra={"topic": topic, "attempt": attempt, "error": str(exc)},
                )

        self._failed += 1
        logger.error(
            "Publish failed after retries",
            extra={"topic": topic, "attempts": attempts, "error": str(last_error)},
        )
        if dead_letter_on_failure and topic != Topics.DEAD_LETTER:
            await self._dead_letter(topic, {"raw": payload.decode("utf-8", "replace")[:4000]},
                                    last_error or RuntimeError("unknown"))
            return False

        raise MessageBusError(f"Could not publish to {topic}", cause=str(last_error))

    async def publish_many(
        self, topic: str, values: list[BaseModel | dict[str, Any]],
        key_fn: Callable[[Any], str] | None = None,
    ) -> int:
        """Publish a batch, returning the count successfully acknowledged.

        Partial success is the normal outcome under broker stress and is
        reported honestly rather than raised as all-or-nothing.
        """
        succeeded = 0
        for value in values:
            key = key_fn(value) if key_fn else None
            if await self.publish(topic, value, key):
                succeeded += 1
        return succeeded

    # ---------------------------------------------------------- dead letter --
    async def _dead_letter(self, origin_topic: str, payload: dict[str, Any], error: Exception) -> None:
        from app.schemas.enums import PipelineStage

        record = DeadLetter(
            correlation_id=get_correlation_id(),
            stage=PipelineStage.PUBLISH,
            error_code="PUBLISH_FAILED",
            error_type=type(error).__name__,
            error_detail=f"{origin_topic}: {error}"[:4096],
            payload=payload,
        )
        try:
            if self._settings.kafka_enabled and self._producer is not None:
                await self._producer.send_and_wait(
                    Topics.DEAD_LETTER, value=_serialize(record)
                )
            else:
                await _memory_bus.publish(Topics.DEAD_LETTER, _serialize(record))
            self._dead_lettered += 1
        except Exception as exc:  # last line of defence — log and move on
            logger.critical(
                "Dead-letter publish failed; message lost",
                extra={"origin_topic": origin_topic, "error": str(exc)},
            )

    def _build_headers(self, extra: dict[str, str] | None) -> list[tuple[str, bytes]]:
        headers: list[tuple[str, bytes]] = []
        cid = get_correlation_id()
        if cid:
            headers.append(("correlation-id", cid.encode("utf-8")))
        for key, value in (extra or {}).items():
            headers.append((key, str(value).encode("utf-8")))
        return headers


_producer: EventProducer | None = None


def get_producer() -> EventProducer:
    """Process-wide producer singleton."""
    global _producer
    if _producer is None:
        _producer = EventProducer()
    return _producer
