"""Location resolution with graded fallbacks.

A report without coordinates is not a report the platform can map, cluster or
corroborate — but discarding it is worse, because the reports most likely to
lack GPS are exactly the ones from people whose phones are old, whose battery
saver is on, or who are relaying a message on someone else's behalf.

So this module tries, in descending order of trustworthiness:

  1. Coordinates supplied in the payload            (GPS_PAYLOAD,     ~0.97)
  2. GPS block in an attached photo's EXIF          (EXIF_GPS,        ~0.95)
  3. A structured place/district/state field        (PLACE_NAME_LOOKUP, ~0.75)
  4. A place name mined from the free text          (GAZETTEER_TEXT_MATCH, ~0.60)
  5. An external geocoder, when explicitly enabled  (REMOTE_GEOCODER, ~0.70)
  6. The state centroid                             (ADMIN_CENTROID,  ~0.35)
  7. Nothing — flagged UNRESOLVED for manual triage

Each outcome records *which* strategy won, because Phase 2 must be able to
discount a report whose location was guessed from a hashtag.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.geo.exif import ExifResult
from app.geo.gazetteer import Gazetteer, get_gazetteer, haversine_km
from app.schemas.enums import GEO_METHOD_CONFIDENCE, GeoMethod
from app.schemas.incident import GeoContext, GeoPoint

logger = get_logger(__name__)

# State centroids for the last-resort fallback. Coarse by construction: the
# 0.35 confidence and the uncertainty radius are what stop Phase 3 from
# clustering two reports 200 km apart just because both said "Kerala".
_STATE_CENTROIDS: dict[str, tuple[float, float, float]] = {
    # state -> (lat, lon, approximate radius km)
    "andhra pradesh": (15.9129, 79.7400, 300.0),
    "arunachal pradesh": (28.2180, 94.7278, 250.0),
    "assam": (26.2006, 92.9376, 220.0),
    "bihar": (25.0961, 85.3131, 200.0),
    "chhattisgarh": (21.2787, 81.8661, 230.0),
    "delhi": (28.7041, 77.1025, 30.0),
    "goa": (15.2993, 74.1240, 50.0),
    "gujarat": (22.2587, 71.1924, 280.0),
    "haryana": (29.0588, 76.0856, 150.0),
    "himachal pradesh": (31.1048, 77.1734, 170.0),
    "jharkhand": (23.6102, 85.2799, 180.0),
    "karnataka": (15.3173, 75.7139, 300.0),
    "kerala": (10.8505, 76.2711, 200.0),
    "ladakh": (34.2268, 77.5619, 300.0),
    "madhya pradesh": (23.4733, 77.9470, 350.0),
    "maharashtra": (19.7515, 75.7139, 350.0),
    "manipur": (24.6637, 93.9063, 110.0),
    "meghalaya": (25.4670, 91.3662, 120.0),
    "mizoram": (23.1645, 92.9376, 120.0),
    "nagaland": (26.1584, 94.5624, 110.0),
    "odisha": (20.9517, 85.0985, 250.0),
    "punjab": (31.1471, 75.3412, 160.0),
    "rajasthan": (27.0238, 74.2179, 400.0),
    "sikkim": (27.5330, 88.5122, 70.0),
    "tamil nadu": (11.1271, 78.6569, 280.0),
    "telangana": (18.1124, 79.0193, 200.0),
    "tripura": (23.9408, 91.9882, 90.0),
    "uttar pradesh": (26.8467, 80.9462, 350.0),
    "uttarakhand": (30.0668, 79.0193, 170.0),
    "west bengal": (22.9868, 87.8550, 250.0),
    "jammu and kashmir": (33.7782, 76.5762, 250.0),
    "chandigarh": (30.7333, 76.7794, 15.0),
    "puducherry": (11.9416, 79.8083, 30.0),
    "andaman and nicobar islands": (11.7401, 92.6586, 250.0),
}

# Typical settlement extent, used as the uncertainty radius when a match came
# from a place name rather than a coordinate.
_PLACE_UNCERTAINTY_KM = 12.0
_TEXT_MATCH_UNCERTAINTY_KM = 25.0


@dataclass(slots=True)
class GeoResolutionInput:
    """Everything the resolver is allowed to look at."""

    lat: float | None = None
    lon: float | None = None
    accuracy_m: float | None = None
    exif: ExifResult | None = None
    place_name: str | None = None
    district: str | None = None
    state: str | None = None
    free_text: str | None = None


class GeoResolver:
    """Resolves a location and records how it did so."""

    def __init__(
        self,
        settings: Settings | None = None,
        gazetteer: Gazetteer | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._gazetteer = gazetteer or get_gazetteer()
        self._http = http_client
        self._owns_http = http_client is None
        # Nominatim's usage policy is one request per second. Even with the
        # remote path disabled by default, the lock keeps a burst of workers
        # from ever violating it.
        self._remote_lock = asyncio.Lock()

    async def aclose(self) -> None:
        if self._owns_http and self._http is not None:
            await self._http.aclose()
            self._http = None

    # ----------------------------------------------------------- public API --
    async def resolve(self, data: GeoResolutionInput) -> GeoContext:
        """Run the fallback chain and return an annotated :class:`GeoContext`."""
        for strategy in (
            self._from_payload,
            self._from_exif,
            self._from_structured_fields,
            self._from_free_text,
        ):
            context = strategy(data)
            if context is not None:
                return self._enrich_admin_labels(context)

        remote = await self._from_remote_geocoder(data)
        if remote is not None:
            return self._enrich_admin_labels(remote)

        centroid = self._from_state_centroid(data)
        if centroid is not None:
            return centroid

        logger.info(
            "Location unresolved",
            extra={
                "had_text": bool(data.free_text),
                "had_place_name": bool(data.place_name),
                "had_state": bool(data.state),
            },
        )
        return GeoContext(method=GeoMethod.UNRESOLVED, confidence=0.0)

    # ----------------------------------------------------------- strategies --
    def _from_payload(self, data: GeoResolutionInput) -> GeoContext | None:
        if data.lat is None or data.lon is None:
            return None
        point = self._safe_point(data.lat, data.lon)
        if point is None:
            return None

        # A GPS fix with a 5 km accuracy circle is a cell-tower estimate wearing
        # a GPS costume; downgrade confidence accordingly.
        confidence = GEO_METHOD_CONFIDENCE[GeoMethod.GPS_PAYLOAD]
        radius_km = 0.05
        if data.accuracy_m is not None:
            radius_km = max(0.02, data.accuracy_m / 1000.0)
            if data.accuracy_m > 2000:
                confidence = 0.55
            elif data.accuracy_m > 500:
                confidence = 0.80

        return GeoContext(
            point=point,
            method=GeoMethod.GPS_PAYLOAD,
            confidence=confidence,
            uncertainty_radius_km=radius_km,
            outside_india=not self._settings.is_within_india(point.lat, point.lon),
        )

    def _from_exif(self, data: GeoResolutionInput) -> GeoContext | None:
        exif = data.exif
        if exif is None or not exif.has_gps:
            return None
        point = self._safe_point(exif.lat, exif.lon)  # type: ignore[arg-type]
        if point is None:
            return None
        return GeoContext(
            point=point,
            method=GeoMethod.EXIF_GPS,
            confidence=GEO_METHOD_CONFIDENCE[GeoMethod.EXIF_GPS],
            uncertainty_radius_km=0.1,
            outside_india=not self._settings.is_within_india(point.lat, point.lon),
        )

    def _from_structured_fields(self, data: GeoResolutionInput) -> GeoContext | None:
        """Use an explicitly supplied place/district field."""
        for candidate in (data.place_name, data.district):
            if not candidate:
                continue
            match = self._gazetteer.lookup_fuzzy(
                candidate, min_similarity=self._settings.gazetteer_min_similarity
            )
            if match is None:
                continue
            # If a state was also given and disagrees, the pair is untrustworthy
            # — "Aurangabad, Bihar" must not silently resolve to Maharashtra.
            if data.state and not self._states_agree(data.state, match.place.state):
                logger.debug(
                    "Structured place/state mismatch; skipping",
                    extra={"place": candidate, "claimed_state": data.state,
                           "matched_state": match.place.state},
                )
                continue
            point = self._safe_point(match.place.lat, match.place.lon)
            if point is None:
                continue
            return GeoContext(
                point=point,
                method=GeoMethod.PLACE_NAME_LOOKUP,
                confidence=GEO_METHOD_CONFIDENCE[GeoMethod.PLACE_NAME_LOOKUP] * match.similarity,
                place_label=match.place.label,
                district=match.place.district,
                state=match.place.state,
                uncertainty_radius_km=_PLACE_UNCERTAINTY_KM,
            )
        return None

    def _from_free_text(self, data: GeoResolutionInput) -> GeoContext | None:
        """Mine a place name out of the report body."""
        if not data.free_text:
            return None
        matches = self._gazetteer.extract_from_text(
            data.free_text, min_similarity=self._settings.gazetteer_min_similarity
        )
        if not matches:
            return None

        best = matches[0]
        # Two well-separated places named in one message is ambiguous: a tweet
        # comparing Mumbai and Chennai belongs to neither. Prefer UNRESOLVED
        # over a coin flip.
        if len(matches) > 1 and matches[1].similarity >= 0.95 * best.similarity:
            separation = haversine_km(
                best.place.lat, best.place.lon, matches[1].place.lat, matches[1].place.lon
            )
            if separation > 100.0:
                logger.debug(
                    "Ambiguous place mention; deferring",
                    extra={"first": best.place.name, "second": matches[1].place.name,
                           "separation_km": round(separation, 1)},
                )
                return None

        point = self._safe_point(best.place.lat, best.place.lon)
        if point is None:
            return None
        return GeoContext(
            point=point,
            method=GeoMethod.GAZETTEER_TEXT_MATCH,
            confidence=GEO_METHOD_CONFIDENCE[GeoMethod.GAZETTEER_TEXT_MATCH] * best.similarity,
            place_label=best.place.label,
            district=best.place.district,
            state=best.place.state,
            uncertainty_radius_km=_TEXT_MATCH_UNCERTAINTY_KM,
        )

    async def _from_remote_geocoder(self, data: GeoResolutionInput) -> GeoContext | None:
        """Query Nominatim. Disabled by default; opt in with GEOCODER_REMOTE_ENABLED."""
        if not self._settings.geocoder_remote_enabled:
            return None
        query = data.place_name or data.district or data.state
        if not query:
            return None

        if self._http is None:
            self._http = httpx.AsyncClient(
                timeout=self._settings.geocoder_timeout_s,
                headers={"User-Agent": self._settings.nominatim_user_agent},
            )

        try:
            async with self._remote_lock:
                response = await self._http.get(
                    f"{self._settings.nominatim_base_url}/search",
                    params={
                        "q": query,
                        "countrycodes": "in",
                        "format": "jsonv2",
                        "limit": 1,
                        "addressdetails": 1,
                    },
                )
                await asyncio.sleep(1.0)  # honour the 1 req/s usage policy

            if response.status_code != 200:
                logger.warning(
                    "Remote geocoder non-200",
                    extra={"status": response.status_code, "query": query},
                )
                return None

            results = response.json()
            if not results:
                return None

            item = results[0]
            point = self._safe_point(float(item["lat"]), float(item["lon"]))
            if point is None:
                return None

            address = item.get("address", {}) or {}
            return GeoContext(
                point=point,
                method=GeoMethod.REMOTE_GEOCODER,
                confidence=GEO_METHOD_CONFIDENCE[GeoMethod.REMOTE_GEOCODER],
                place_label=item.get("display_name", query)[:256],
                district=address.get("state_district") or address.get("county"),
                state=address.get("state"),
                uncertainty_radius_km=_PLACE_UNCERTAINTY_KM,
            )
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            # Geocoding is a nice-to-have; never let it fail an ingest.
            logger.warning("Remote geocoder failed", extra={"error": str(exc), "query": query})
            return None

    def _from_state_centroid(self, data: GeoResolutionInput) -> GeoContext | None:
        if not data.state:
            return None
        entry = _STATE_CENTROIDS.get(data.state.strip().casefold())
        if entry is None:
            return None
        lat, lon, radius = entry
        point = self._safe_point(lat, lon)
        if point is None:
            return None
        return GeoContext(
            point=point,
            method=GeoMethod.ADMIN_CENTROID,
            confidence=GEO_METHOD_CONFIDENCE[GeoMethod.ADMIN_CENTROID],
            place_label=f"{data.state.strip().title()} (state centroid)",
            state=data.state.strip().title(),
            uncertainty_radius_km=radius,
        )

    # --------------------------------------------------------------- helpers --
    def _enrich_admin_labels(self, context: GeoContext) -> GeoContext:
        """Backfill district/state from the nearest gazetteer entry."""
        if context.point is None or (context.district and context.state):
            return context
        nearest = self._gazetteer.nearest(context.point.lat, context.point.lon)
        if nearest is None:
            return context
        place, distance_km = nearest
        return context.model_copy(
            update={
                "district": context.district or place.district,
                "state": context.state or place.state,
                "place_label": context.place_label
                or (place.label if distance_km < 25 else f"near {place.label}"),
            }
        )

    @staticmethod
    def _states_agree(claimed: str, matched: str) -> bool:
        a, b = claimed.strip().casefold(), matched.strip().casefold()
        return a == b or a in b or b in a

    @staticmethod
    def _safe_point(lat: float | None, lon: float | None) -> GeoPoint | None:
        """Build a GeoPoint, swallowing the validator's rejections."""
        if lat is None or lon is None:
            return None
        try:
            return GeoPoint(lat=lat, lon=lon)
        except ValueError:
            return None


_resolver: GeoResolver | None = None


def get_geo_resolver() -> GeoResolver:
    global _resolver
    if _resolver is None:
        _resolver = GeoResolver()
    return _resolver
