"""OpenWeather API client.

Polls current conditions for a rotating set of monitored cities. The free tier
allows 60 calls/minute, so the poller walks a slice of the city list each cycle
rather than requesting everything at once — a burst that trips the rate limit
costs more coverage than the slower sweep does.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import Any

import httpx

from app.core.config import Settings, get_settings
from app.core.errors import RateLimitedUpstreamError, UpstreamUnavailableError
from app.core.logging import get_logger
from app.geo.gazetteer import Place, get_gazetteer

logger = get_logger(__name__)

# OpenWeather condition codes paired with their canonical descriptions.
_MOCK_CONDITIONS: tuple[tuple[int, str, str], ...] = (
    (200, "Thunderstorm", "thunderstorm with light rain"),
    (202, "Thunderstorm", "thunderstorm with heavy rain"),
    (501, "Rain", "moderate rain"),
    (502, "Rain", "heavy intensity rain"),
    (503, "Rain", "very heavy rain"),
    (504, "Rain", "extreme rain"),
    (701, "Mist", "mist"),
    (741, "Fog", "fog"),
    (761, "Dust", "dust"),
    (800, "Clear", "clear sky"),
    (803, "Clouds", "broken clouds"),
)


class OpenWeatherClient:
    """Fetches current conditions for monitored cities."""

    # Largest settlements first — a fixed monitoring set keeps the polling
    # cost predictable and the coverage explainable.
    MONITORED_CITY_COUNT = 40

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client: httpx.AsyncClient | None = None
        self._rng = random.Random()
        self._cursor = 0
        self._cities: list[Place] = []

    async def __aenter__(self) -> "OpenWeatherClient":
        await self.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def start(self) -> None:
        places = sorted(
            get_gazetteer().places, key=lambda p: p.population_k, reverse=True
        )
        self._cities = places[: self.MONITORED_CITY_COUNT]

        if self._client is None and not self._settings.openweather_mock_mode:
            if not self._settings.openweather_api_key:
                raise UpstreamUnavailableError(
                    "OPENWEATHER_API_KEY is required when OPENWEATHER_MOCK_MODE is false"
                )
            self._client = httpx.AsyncClient(
                base_url=self._settings.openweather_base_url,
                timeout=self._settings.openweather_request_timeout_s,
                headers={"Accept": "application/json"},
            )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ----------------------------------------------------------- public API --
    async def fetch_batch(self, batch_size: int = 10) -> list[dict[str, Any]]:
        """Fetch the next slice of monitored cities, wrapping around."""
        if not self._cities:
            await self.start()
        if not self._cities:
            return []

        batch: list[Place] = []
        for _ in range(min(batch_size, len(self._cities))):
            batch.append(self._cities[self._cursor % len(self._cities)])
            self._cursor += 1

        if self._settings.openweather_mock_mode:
            return [self._mock_current(place) for place in batch]

        results: list[dict[str, Any]] = []
        for place in batch:
            try:
                record = await self._fetch_current(place)
            except RateLimitedUpstreamError:
                # Stop the sweep entirely; the next cycle resumes at the cursor.
                logger.warning("OpenWeather rate limited; ending batch early")
                break
            except UpstreamUnavailableError as exc:
                logger.warning(
                    "OpenWeather city fetch failed",
                    extra={"city": place.name, "error": str(exc)},
                )
                continue
            if record is not None:
                results.append(record)
            await asyncio.sleep(0.15)  # stay well inside 60 calls/min

        return results

    # --------------------------------------------------------------- remote --
    async def _fetch_current(self, place: Place) -> dict[str, Any] | None:
        if self._client is None:
            return None
        try:
            response = await self._client.get(
                "/weather",
                params={
                    "lat": place.lat,
                    "lon": place.lon,
                    "appid": self._settings.openweather_api_key,
                },
            )
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            raise UpstreamUnavailableError("OpenWeather unreachable", cause=str(exc)) from exc
        except httpx.HTTPError as exc:
            raise UpstreamUnavailableError("OpenWeather transport error", cause=str(exc)) from exc

        if response.status_code == 429:
            raise RateLimitedUpstreamError("OpenWeather quota exceeded", retry_after_s=60.0)
        if response.status_code == 401:
            # A bad key will not fix itself; say so loudly rather than
            # retrying in a loop forever.
            raise UpstreamUnavailableError("OpenWeather rejected the API key (401)")
        if response.status_code != 200:
            return None

        try:
            payload = response.json()
        except ValueError:
            return None

        payload["record_type"] = "current"
        return payload

    # ----------------------------------------------------------------- mock --
    def _mock_current(self, place: Place) -> dict[str, Any]:
        code, main, description = self._rng.choice(_MOCK_CONDITIONS)
        now = int(datetime.now(timezone.utc).timestamp())

        payload: dict[str, Any] = {
            "record_type": "current",
            "id": abs(hash(place.name)) % 9_000_000 + 1_000_000,
            "name": place.name,
            "dt": now,
            "coord": {"lat": place.lat, "lon": place.lon},
            "weather": [{"id": code, "main": main, "description": description}],
            "main": {
                # Kelvin, as the real API returns — the normalizer converts.
                "temp": round(self._rng.uniform(291, 315), 2),
                "feels_like": round(self._rng.uniform(292, 320), 2),
                "humidity": self._rng.randint(30, 98),
                "pressure": self._rng.randint(995, 1015),
            },
            "wind": {
                "speed": round(self._rng.uniform(0.5, 18.0), 2),  # m/s
                "deg": self._rng.randint(0, 359),
            },
            "clouds": {"all": self._rng.randint(0, 100)},
            "visibility": self._rng.choice([120, 800, 4000, 10000, 10000]),
        }

        if 500 <= code <= 531 or 200 <= code <= 232:
            payload["rain"] = {"1h": round(self._rng.uniform(1.0, 45.0), 2)}
        if self._rng.random() < 0.25:
            payload["wind"]["gust"] = round(payload["wind"]["speed"] * self._rng.uniform(1.3, 2.2), 2)

        return payload
