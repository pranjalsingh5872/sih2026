"""Normalizer worker — the fan-in stage.

Consumes ``raw-weather-stream``, routes each envelope to the normalizer for its
source, and publishes the unified incident to ``normalized-incident-stream``.
This is the boundary where five provider-shaped feeds become one contract, and
everything downstream — the AI engine, the clustering, the dashboard — reads
only what comes out of here.

Geo-unresolved incidents are published to *both* the main stream and
``geo-unresolved-incidents``. They are still real signal, and an operator can
frequently place them by hand from the text; silently dropping them would mean
losing exactly the reports from people least able to file a clean one.
"""

from __future__ import annotations

import asyncio
import signal

from aiokafka import ConsumerRecord

from app.core.config import get_settings
from app.core.errors import NormalizationError, PermanentError
from app.core.logging import configure_logging, get_logger
from app.messaging.consumer import EventConsumer
from app.messaging.producer import get_producer
from app.messaging.topics import ConsumerGroups, Topics
from app.normalization.registry import NormalizerRegistry, get_registry
from app.schemas.enums import PipelineStage
from app.schemas.incident import NormalizedIncident
from app.schemas.raw import RawEnvelope

logger = get_logger(__name__)


class NormalizerWorker:
    """Wires a consumer, the registry and a producer together."""

    name = "normalizer"

    def __init__(self, registry: NormalizerRegistry | None = None) -> None:
        self._settings = get_settings()
        self._registry = registry or get_registry()
        self._producer = get_producer()
        self._shutdown = asyncio.Event()
        self._consumer = EventConsumer(
            topic=Topics.RAW_WEATHER,
            group_id=ConsumerGroups.NORMALIZER,
            handler=self.handle,
            settings=self._settings,
            producer=self._producer,
        )
        self._normalized = 0
        self._unresolved_geo = 0

    # -------------------------------------------------------------- handler --
    async def handle(self, payload: dict, record: ConsumerRecord) -> None:
        """Normalize one raw envelope and publish the result.

        Raises :class:`PermanentError` on unusable input so the consumer
        dead-letters it rather than retrying a payload that cannot improve.
        """
        try:
            envelope = RawEnvelope.model_validate(payload)
        except ValueError as exc:
            raise NormalizationError(
                f"Envelope failed validation: {exc}",
                topic=record.topic,
                offset=record.offset,
            ) from exc

        incident: NormalizedIncident = await self._registry.normalize(envelope)
        incident.add_trace(PipelineStage.PUBLISH, component=self.name)

        # Partition by location so that, downstream, reports about the same
        # place land on the same partition. Phase 3's clustering gets locality
        # for free; unresolved reports spread across partitions by source.
        key = self._partition_key(incident)

        published = await self._producer.publish(
            Topics.NORMALIZED_INCIDENTS, incident, key=key
        )
        if not published:
            # publish() already dead-lettered it; do not also fail the offset.
            logger.error(
                "Normalized incident could not be published",
                extra={"incident_id": str(incident.incident_id)},
            )
            return

        self._normalized += 1

        if not incident.geo.is_resolved:
            self._unresolved_geo += 1
            await self._producer.publish(
                Topics.GEO_UNRESOLVED, incident, key=key, dead_letter_on_failure=False
            )

        if self._normalized % 100 == 0:
            logger.info(
                "Normalization progress",
                extra={
                    "normalized": self._normalized,
                    "unresolved_geo": self._unresolved_geo,
                    "unresolved_pct": round(
                        100 * self._unresolved_geo / max(self._normalized, 1), 1
                    ),
                    **self._consumer.stats,
                },
            )

    @staticmethod
    def _partition_key(incident: NormalizedIncident) -> str:
        if incident.geo.point is not None:
            # ~11 km grid cell at one decimal place.
            return f"geo:{incident.geo.point.lat:.1f},{incident.geo.point.lon:.1f}"
        return f"src:{incident.source_type.value}"

    # ------------------------------------------------------------ lifecycle --
    async def start(self) -> None:
        configure_logging(
            level=self._settings.log_level,
            json_output=self._settings.log_json,
            service=self.name,
        )
        self._install_signal_handlers()

        logger.info(
            "Normalizer worker starting",
            extra={
                "consuming": Topics.RAW_WEATHER,
                "producing": Topics.NORMALIZED_INCIDENTS,
                "sources": sorted(s.value for s in self._registry.supported_sources),
            },
        )

        await self._producer.start()
        await self._consumer.start()

        consume_task = asyncio.create_task(self._consumer.run(), name="consume")
        shutdown_task = asyncio.create_task(self._shutdown.wait(), name="shutdown")

        done, pending = await asyncio.wait(
            {consume_task, shutdown_task}, return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Surface a crash in the consume loop rather than exiting zero.
        for task in done:
            if task is consume_task and not task.cancelled():
                exc = task.exception()
                if exc is not None:
                    logger.error("Consumer loop terminated", extra={"error": str(exc)})

        await self._producer.stop()
        logger.info(
            "Normalizer worker stopped",
            extra={"normalized": self._normalized, "unresolved_geo": self._unresolved_geo},
        )

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._shutdown.set)
            except NotImplementedError:  # pragma: no cover
                pass


def main() -> None:
    try:
        asyncio.run(NormalizerWorker().start())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
