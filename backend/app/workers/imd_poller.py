"""IMD polling worker.

Fetches nowcast warnings and AWS/ARG observations, wraps each record in a
:class:`RawEnvelope` and publishes to ``raw-weather-stream``.

Observations are polled on every cycle; warnings likewise, because a nowcast
bulletin is the single most time-critical input the platform receives. The
envelope keeps the payload verbatim so a normalizer fix can be replayed against
the original bytes.
"""

from __future__ import annotations

from app.core.logging import get_correlation_id, get_logger
from app.messaging.topics import Topics
from app.providers.imd_client import IMDClient
from app.schemas.enums import PipelineStage, SourceType
from app.schemas.raw import RawEnvelope
from app.workers.base import BaseWorker, run_worker

logger = get_logger(__name__)


class IMDPollerWorker(BaseWorker):
    name = "imd-poller"

    def __init__(self) -> None:
        super().__init__()
        self.interval_s = float(self._settings.imd_poll_interval_s)
        self._client = IMDClient(self._settings)

    async def on_startup(self) -> None:
        await self._client.start()
        logger.info("IMD poller ready", extra={"mock_mode": self._settings.imd_mock_mode})

    async def on_shutdown(self) -> None:
        await self._client.aclose()

    async def run_cycle(self) -> int:
        emitted = 0
        emitted += await self._publish(await self._client.fetch_warnings(), "imd_warnings")
        emitted += await self._publish(await self._client.fetch_observations(), "imd_observations")
        return emitted

    async def _publish(self, records: list[dict], feed: str) -> int:
        count = 0
        for record in records:
            external_id = str(
                record.get("bulletin_id")
                or record.get("station_id")
                or record.get("id")
                or ""
            ) or None

            envelope = RawEnvelope(
                correlation_id=get_correlation_id(),
                source_type=SourceType.IMD,
                source_name=feed,
                external_id=external_id,
                payload=record,
                ingest_stage=PipelineStage.PROVIDER_POLL,
                producer_component=self.name,
            )
            if await self._producer.publish(
                Topics.RAW_WEATHER, envelope, key=envelope.partition_key()
            ):
                count += 1
        return count


if __name__ == "__main__":
    run_worker(IMDPollerWorker())
