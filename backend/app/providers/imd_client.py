"""India Meteorological Department client.

IMD does not publish a stable, documented public JSON API — bulletins are
distributed through the Mausam portal, regional centre feeds and the CAP
aggregator, and the shapes differ between them. The client is therefore written
against a *configurable* endpoint and ships a mock mode that emits
representative payloads.

The mock is not decoration. It lets the whole pipeline run, be demonstrated and
be load-tested without credentials or connectivity, and — because the synthetic
warnings are generated with known ground truth — it gives Phase 2's credibility
model something to be evaluated against. Set ``IMD_MOCK_MODE=false`` with a
real endpoint and nothing downstream changes.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.core.config import Settings, get_settings
from app.core.errors import RateLimitedUpstreamError, UpstreamUnavailableError
from app.core.logging import get_logger
from app.geo.gazetteer import Place, get_gazetteer

logger = get_logger(__name__)

_HAZARDS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("Heavy Rainfall", "ORANGE", ("RED", "ORANGE", "YELLOW")),
    ("Very Heavy Rainfall", "RED", ("RED", "ORANGE")),
    ("Thunderstorm with Lightning", "YELLOW", ("YELLOW", "ORANGE")),
    ("Squall", "ORANGE", ("ORANGE", "YELLOW")),
    ("Heat Wave", "ORANGE", ("RED", "ORANGE")),
    ("Dense Fog", "YELLOW", ("YELLOW", "ORANGE")),
    ("Hailstorm", "ORANGE", ("ORANGE",)),
    ("Cold Wave", "YELLOW", ("YELLOW", "ORANGE")),
)


class IMDClient:
    """Fetches warnings and station observations."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client: httpx.AsyncClient | None = None
        self._rng = random.Random()
        self._sequence = 0

    async def __aenter__(self) -> "IMDClient":
        await self.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def start(self) -> None:
        if self._client is None and not self._settings.imd_mock_mode:
            self._client = httpx.AsyncClient(
                base_url=self._settings.imd_base_url,
                timeout=self._settings.imd_request_timeout_s,
                headers={"Accept": "application/json", "User-Agent": "sih26069/1.0"},
                follow_redirects=True,
            )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ----------------------------------------------------------- public API --
    async def fetch_warnings(self) -> list[dict[str, Any]]:
        if self._settings.imd_mock_mode:
            return self._mock_warnings()
        return await self._get("/api/warnings_district_api.php", record_type="warning")

    async def fetch_observations(self) -> list[dict[str, Any]]:
        if self._settings.imd_mock_mode:
            return self._mock_observations()
        return await self._get("/api/current_wx_api.php", record_type="observation")

    # --------------------------------------------------------------- remote --
    async def _get(self, path: str, *, record_type: str) -> list[dict[str, Any]]:
        if self._client is None:
            await self.start()
        if self._client is None:  # mock mode flipped underneath us
            return []

        try:
            response = await self._client.get(path)
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            raise UpstreamUnavailableError("IMD request failed", path=path, cause=str(exc)) from exc
        except httpx.HTTPError as exc:
            raise UpstreamUnavailableError("IMD transport error", path=path, cause=str(exc)) from exc

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            raise RateLimitedUpstreamError(
                "IMD rate limited",
                retry_after_s=float(retry_after) if retry_after else 60.0,
            )
        if response.status_code >= 500:
            raise UpstreamUnavailableError("IMD server error", status=response.status_code)
        if response.status_code != 200:
            logger.warning("IMD non-200", extra={"status": response.status_code, "path": path})
            return []

        try:
            body = response.json()
        except ValueError as exc:
            raise UpstreamUnavailableError("IMD returned non-JSON", cause=str(exc)) from exc

        records = body if isinstance(body, list) else body.get("data") or body.get("records") or []
        if not isinstance(records, list):
            logger.warning("Unexpected IMD body shape", extra={"type": type(records).__name__})
            return []

        for record in records:
            if isinstance(record, dict):
                record.setdefault("record_type", record_type)
        return [r for r in records if isinstance(r, dict)]

    # ----------------------------------------------------------------- mock --
    def _sample_places(self, count: int) -> list[Place]:
        places = get_gazetteer().places
        if not places:
            return []
        return self._rng.sample(list(places), k=min(count, len(places)))

    def _mock_warnings(self) -> list[dict[str, Any]]:
        """Generate 2-5 district warnings for random locations."""
        now = datetime.now(timezone.utc)
        records: list[dict[str, Any]] = []

        for place in self._sample_places(self._rng.randint(2, 5)):
            hazard, _default, colours = self._rng.choice(_HAZARDS)
            colour = self._rng.choice(colours)
            self._sequence += 1
            valid_hours = self._rng.choice([6, 12, 24, 48])

            record: dict[str, Any] = {
                "record_type": "warning",
                "bulletin_id": f"IMD-{now:%Y%m%d}-{self._sequence:05d}",
                "hazard_type": hazard,
                "colour_code": colour,
                "headline": f"{hazard} warning for {place.district}, {place.state}",
                "description": (
                    f"{hazard} very likely at isolated places over {place.district} "
                    f"district of {place.state} during the next {valid_hours} hours. "
                    "Citizens are advised to avoid low-lying areas and follow "
                    "district administration advisories."
                ),
                "district": place.district,
                "state": place.state,
                "area": place.name,
                "lat": round(place.lat + self._rng.uniform(-0.05, 0.05), 5),
                "lon": round(place.lon + self._rng.uniform(-0.05, 0.05), 5),
                "issue_time": now.isoformat(),
                "valid_from": now.isoformat(),
                "valid_until": (now + timedelta(hours=valid_hours)).isoformat(),
                "valid_hours": valid_hours,
                "issuing_office": f"RMC {place.state}",
            }

            if "Rainfall" in hazard:
                record["expected_rainfall_mm"] = round(
                    self._rng.uniform(65, 210) if colour in ("RED", "ORANGE")
                    else self._rng.uniform(20, 64), 1
                )
            if hazard in ("Squall", "Thunderstorm with Lightning"):
                record["wind_speed_kmh"] = round(self._rng.uniform(40, 70), 1)
                record["wind_gust_kmh"] = round(self._rng.uniform(70, 110), 1)

            records.append(record)

        return records

    def _mock_observations(self) -> list[dict[str, Any]]:
        """Generate AWS/ARG station readings."""
        now = datetime.now(timezone.utc)
        records: list[dict[str, Any]] = []

        for place in self._sample_places(self._rng.randint(4, 9)):
            # Occasionally emit a genuinely extreme reading so downstream
            # severity logic is exercised rather than always seeing calm data.
            extreme = self._rng.random() < 0.15
            rainfall = (
                round(self._rng.uniform(70, 180), 1) if extreme
                else round(max(0.0, self._rng.gauss(4, 8)), 1)
            )
            records.append(
                {
                    "record_type": "observation",
                    "station_id": f"AWS-{abs(hash(place.name)) % 90000 + 10000}",
                    "station_name": f"{place.name} AWS",
                    "station_type": self._rng.choice(["AWS", "ARG"]),
                    "district": place.district,
                    "state": place.state,
                    "lat": place.lat,
                    "lon": place.lon,
                    "observation_time": now.isoformat(),
                    "rainfall_mm": rainfall,
                    "rainfall_window_hours": 24,
                    "temperature_c": round(self._rng.uniform(18, 44), 1),
                    "humidity_pct": round(self._rng.uniform(35, 98), 1),
                    "wind_speed_kmh": round(self._rng.uniform(2, 35), 1),
                    "wind_gust_kmh": round(self._rng.uniform(10, 75), 1),
                    "wind_direction_deg": round(self._rng.uniform(0, 360), 1),
                    "pressure_hpa": round(self._rng.uniform(995, 1015), 1),
                    "visibility_m": round(
                        self._rng.uniform(50, 800) if self._rng.random() < 0.1
                        else self._rng.uniform(2000, 10000)
                    ),
                }
            )

        return records
