"""Worker scaffolding: lifecycle, signals, backoff.

Containers get a SIGTERM and a grace period, not a courtesy call. A poller that
ignores it loses whatever was mid-flight. Every worker here installs signal
handlers, finishes the current cycle, flushes the producer and exits cleanly.
"""

from __future__ import annotations

import asyncio
import random
import signal
from abc import ABC, abstractmethod

from app.core.config import Settings, get_settings
from app.core.errors import RetryableError
from app.core.logging import configure_logging, get_logger, new_correlation_id
from app.messaging.producer import EventProducer, get_producer

logger = get_logger(__name__)


class BaseWorker(ABC):
    """A long-running ingestion process."""

    #: Identifies the worker in logs and Kafka client ids.
    name: str = "worker"
    #: Seconds between cycles.
    interval_s: float = 60.0
    #: Cap on the backoff applied after consecutive failures.
    max_backoff_s: float = 300.0

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._producer: EventProducer = get_producer()
        self._shutdown = asyncio.Event()
        self._consecutive_failures = 0
        self._cycles = 0
        self._emitted = 0

    # ---------------------------------------------------- to be implemented --
    @abstractmethod
    async def run_cycle(self) -> int:
        """Do one unit of work. Return the number of messages emitted."""

    async def on_startup(self) -> None:
        """Optional hook for acquiring resources."""

    async def on_shutdown(self) -> None:
        """Optional hook for releasing resources."""

    # ------------------------------------------------------------ lifecycle --
    async def start(self) -> None:
        configure_logging(
            level=self._settings.log_level,
            json_output=self._settings.log_json,
            service=self.name,
        )
        self._install_signal_handlers()

        logger.info(
            "Worker starting",
            extra={"worker": self.name, "interval_s": self.interval_s,
                   "env": self._settings.app_env},
        )

        await self._producer.start()
        await self.on_startup()

        try:
            await self._loop()
        finally:
            await self.on_shutdown()
            await self._producer.stop()
            logger.info(
                "Worker stopped",
                extra={"worker": self.name, "cycles": self._cycles, "emitted": self._emitted},
            )

    async def _loop(self) -> None:
        while not self._shutdown.is_set():
            new_correlation_id()
            delay = self.interval_s

            try:
                emitted = await self.run_cycle()
                self._emitted += emitted
                self._cycles += 1
                self._consecutive_failures = 0
                logger.info(
                    "Cycle complete",
                    extra={"worker": self.name, "emitted": emitted, "cycle": self._cycles},
                )
            except asyncio.CancelledError:
                raise
            except RetryableError as exc:
                self._consecutive_failures += 1
                delay = self._backoff_delay()
                logger.warning(
                    "Cycle failed; backing off",
                    extra={"worker": self.name, "failures": self._consecutive_failures,
                           "retry_in_s": round(delay, 1), "error": str(exc)},
                )
            except Exception as exc:
                self._consecutive_failures += 1
                delay = self._backoff_delay()
                logger.exception(
                    "Unhandled cycle error",
                    extra={"worker": self.name, "failures": self._consecutive_failures,
                           "error": str(exc)},
                )

            # Sleep, but wake immediately on shutdown rather than making the
            # orchestrator wait out a full poll interval.
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    def _backoff_delay(self) -> float:
        """Exponential backoff with jitter, floored at the normal interval."""
        base = min(self.interval_s * (2 ** min(self._consecutive_failures, 6)),
                   self.max_backoff_s)
        return max(self.interval_s, base * random.uniform(0.7, 1.3))

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._request_shutdown, sig.name)
            except NotImplementedError:  # pragma: no cover - non-POSIX
                logger.debug("Signal handlers unavailable on this platform")

    def _request_shutdown(self, signal_name: str) -> None:
        logger.info("Shutdown requested", extra={"worker": self.name, "signal": signal_name})
        self._shutdown.set()

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown.is_set()


def run_worker(worker: BaseWorker) -> None:
    """Entrypoint helper for ``python -m app.workers.<name>``."""
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        pass
