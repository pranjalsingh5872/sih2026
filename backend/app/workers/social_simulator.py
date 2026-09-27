"""Social feed simulator worker.

Emits synthetic multilingual posts at a configured rate. Unlike the pollers,
this one paces itself *within* the cycle — a burst of 90 messages followed by
55 seconds of silence would not exercise the pipeline the way a steady stream
does, and steady arrival is what the Phase 2 rolling-window dedup assumes.
"""

from __future__ import annotations

import asyncio

from app.core.logging import get_correlation_id, get_logger
from app.messaging.topics import Topics
from app.providers.social_feed import SocialFeedGenerator
from app.schemas.enums import PipelineStage, SourceType
from app.schemas.raw import RawEnvelope
from app.workers.base import BaseWorker, run_worker

logger = get_logger(__name__)


class SocialSimulatorWorker(BaseWorker):
    name = "social-simulator"
    interval_s = 1.0  # cycle length; rate is controlled per-cycle below

    def __init__(self) -> None:
        super().__init__()
        self._generator = SocialFeedGenerator(
            duplicate_ratio=self._settings.social_sim_duplicate_ratio,
            missing_geo_ratio=self._settings.social_sim_missing_geo_ratio,
            fake_ratio=self._settings.social_sim_fake_ratio,
            seed=self._settings.social_sim_seed,
        )
        rate = max(1, self._settings.social_sim_rate_per_min)
        self._per_second = rate / 60.0
        self._gap_s = 1.0 / self._per_second if self._per_second > 0 else 1.0

    async def on_startup(self) -> None:
        logger.info(
            "Social simulator ready",
            extra={
                "rate_per_min": self._settings.social_sim_rate_per_min,
                "duplicate_ratio": self._settings.social_sim_duplicate_ratio,
                "missing_geo_ratio": self._settings.social_sim_missing_geo_ratio,
                "fake_ratio": self._settings.social_sim_fake_ratio,
            },
        )

    async def run_cycle(self) -> int:
        """Emit roughly one second's worth of traffic, evenly spaced."""
        count = 0
        budget = max(1, round(self._per_second))

        for _ in range(budget):
            if self.is_shutting_down:
                break
            post = self._generator.generate()
            envelope = RawEnvelope(
                correlation_id=get_correlation_id(),
                source_type=SourceType.SOCIAL,
                source_name=f"social_{post.get('platform', 'unknown')}",
                external_id=str(post.get("post_id")),
                payload=post,
                ingest_stage=PipelineStage.SOCIAL_SIMULATOR,
                producer_component=self.name,
            )
            if await self._producer.publish(
                Topics.RAW_WEATHER, envelope, key=envelope.partition_key()
            ):
                count += 1
            if budget > 1:
                await asyncio.sleep(min(self._gap_s, 1.0 / budget))

        return count


if __name__ == "__main__":
    run_worker(SocialSimulatorWorker())
