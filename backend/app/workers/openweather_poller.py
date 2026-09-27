"""OpenWeather polling worker.

Sweeps the monitored city list in batches and publishes each response to
``raw-weather-stream``. Batch size is kept modest so one cycle never comes
close to the free-tier rate limit.
"""

from __future__ import annotations

from app.core.logging import get_correlation_id, get_logger
from app.messaging.topics import Topics
from app.providers.openweather_client import OpenWeatherClient
from app.schemas.enums import PipelineStage, SourceType
from app.schemas.raw import RawEnvelope
from app.workers.base import BaseWorker, run_worker

logger = get_logger(__name__)


class OpenWeatherPollerWorker(BaseWorker):
    name = "openweather-poller"
    BATCH_SIZE = 10

    def __init__(self) -> None:
        super().__init__()
        self.interval_s = float(self._settings.openweather_poll_interval_s)
        self._client = OpenWeatherClient(self._settings)

    async def on_startup(self) -> None:
        await self._client.start()
        logger.info(
            "OpenWeather poller ready",
            extra={"mock_mode": self._settings.openweather_mock_mode,
                   "batch_size": self.BATCH_SIZE},
        )

    async def on_shutdown(self) -> None:
        await self._client.aclose()

    async def run_cycle(self) -> int:
        records = await self._client.fetch_batch(batch_size=self.BATCH_SIZE)
        count = 0
        for record in records:
            envelope = RawEnvelope(
                correlation_id=get_correlation_id(),
                source_type=SourceType.OPENWEATHER,
                source_name="openweather_current",
                external_id=str(record.get("id") or "") or None,
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
    run_worker(OpenWeatherPollerWorker())
