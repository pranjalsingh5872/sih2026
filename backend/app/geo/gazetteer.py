"""Offline India gazetteer.

Two jobs:

1. **Forward** — mine a place name out of free text ("भारी बारिश इंदौर में") and
   return coordinates. This is the fallback that rescues the ~35% of citizen and
   social reports that arrive with no GPS fix at all.
2. **Reverse** — given coordinates, name the district and state, so every
   located incident carries administrative labels for the Phase 4 regional
   risk indices without a network round-trip.

Everything runs in-process off a bundled JSON file. During a cyclone the
external geocoder is exactly the dependency that will rate-limit or time out,
so it is never on the critical path.
"""

from __future__ import annotations

import json
import math
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Final, Iterable

from app.core.logging import get_logger

logger = get_logger(__name__)

_DATA_FILE: Final[Path] = Path(__file__).parent / "data" / "india_gazetteer.json"

EARTH_RADIUS_KM: Final[float] = 6371.0088

# Tokens that show up adjacent to place names and must never be matched as one.
_STOPWORDS: Final[frozenset[str]] = frozenset(
    {
        "the", "in", "at", "near", "from", "and", "of", "to", "for", "on", "is",
        "heavy", "rain", "rains", "rainfall", "flood", "flooding", "flooded",
        "water", "storm", "alert", "warning", "weather", "road", "street",
        "city", "district", "area", "region", "today", "now", "update",
        "me", "mein", "se", "par", "aur", "hai", "ho", "raha", "bahut",
        "gaya", "gayi", "gayi", "गया", "गई", "में", "से", "पर", "और",
        "है", "बारिश", "पानी", "बाढ़", "भारी",
    }
)

_PUNCT_RE = re.compile(r"[^\w\u0900-\u0DFF\s]", re.UNICODE)
_WS_RE = re.compile(r"\s+")
# Case boundary inside a glued token: "MumbaiRains" -> "Mumbai Rains".
_CAMEL_RE = re.compile(r"([a-z\u0900-\u0DFF])([A-Z])")


def normalize_place_token(text: str) -> str:
    """Casefold, strip accents and punctuation, collapse whitespace.

    Indic scripts are left intact — NFKC only, never NFKD-then-strip, which
    would destroy Devanagari matras and turn 'इंदौर' into noise.
    """
    if not text:
        return ""
    folded = unicodedata.normalize("NFKC", text).casefold()
    # Decompose, drop only the Latin/Greek/Cyrillic combining block
    # (U+0300-U+036F), then recompose. Indic matras live in their own script
    # blocks and survive untouched, so 'Bengalūru' -> 'bengaluru' while
    # 'इंदौर' is preserved exactly.
    decomposed = unicodedata.normalize("NFD", folded)
    stripped = "".join(ch for ch in decomposed if not 0x0300 <= ord(ch) <= 0x036F)
    folded = unicodedata.normalize("NFC", stripped)
    folded = _PUNCT_RE.sub(" ", folded)
    return _WS_RE.sub(" ", folded).strip()


def split_compound_tokens(text: str) -> str:
    """Split camel/Pascal-case runs so hashtags surrender their place names.

    ``#MumbaiRains`` and ``#ChennaiFloods`` are among the most common ways a
    location appears in Indian weather chatter, and as a single glued token
    they match nothing. Splitting on the case boundary recovers them.
    """
    if not text:
        return ""
    return _CAMEL_RE.sub(r"\1 \2", text)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


@dataclass(frozen=True, slots=True)
class Place:
    name: str
    state: str
    district: str
    lat: float
    lon: float
    population_k: int
    aliases: tuple[str, ...]

    @property
    def label(self) -> str:
        return f"{self.name}, {self.state}"


@dataclass(frozen=True, slots=True)
class PlaceMatch:
    place: Place
    similarity: float
    matched_text: str
    exact: bool


class Gazetteer:
    """In-memory index over the bundled place list."""

    # Longest alias is three tokens ("Jammu and Kashmir"); scanning wider
    # n-grams only costs time.
    MAX_NGRAM: Final[int] = 3

    def __init__(self, data_file: Path | None = None) -> None:
        self._places: list[Place] = []
        self._exact: dict[str, Place] = {}
        self._by_first_token: dict[str, list[tuple[str, Place]]] = {}
        self._load(data_file or _DATA_FILE)

    # ------------------------------------------------------------- loading --
    def _load(self, path: Path) -> None:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            # A broken gazetteer degrades geocoding but must not stop ingestion.
            logger.error("Gazetteer failed to load", extra={"path": str(path), "error": str(exc)})
            return

        for entry in raw.get("places", []):
            try:
                place = Place(
                    name=entry["name"],
                    state=entry["state"],
                    district=entry.get("district") or entry["name"],
                    lat=float(entry["lat"]),
                    lon=float(entry["lon"]),
                    population_k=int(entry.get("population_k") or 0),
                    aliases=tuple(entry.get("aliases") or ()),
                )
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning("Skipping malformed gazetteer entry", extra={"error": str(exc)})
                continue

            self._places.append(place)
            for surface in (place.name, place.district, *place.aliases):
                key = normalize_place_token(surface)
                if not key:
                    continue
                # Denser settlements win a collision: "Aurangabad" unqualified
                # should land on the larger of the two.
                incumbent = self._exact.get(key)
                if incumbent is None or place.population_k > incumbent.population_k:
                    self._exact[key] = place
                head = key.split(" ", 1)[0]
                self._by_first_token.setdefault(head, []).append((key, place))

        logger.info(
            "Gazetteer loaded",
            extra={"place_count": len(self._places), "surface_forms": len(self._exact)},
        )

    # ------------------------------------------------------------ accessors --
    def __len__(self) -> int:
        return len(self._places)

    @property
    def places(self) -> tuple[Place, ...]:
        return tuple(self._places)

    # -------------------------------------------------------------- lookup --
    def lookup_exact(self, name: str) -> Place | None:
        return self._exact.get(normalize_place_token(name))

    def lookup_fuzzy(self, name: str, min_similarity: float = 0.82) -> PlaceMatch | None:
        """Resolve a single candidate name, tolerating transliteration drift.

        'Bengaluru' / 'Bangalore' / 'Banglore' should all land in the same
        place; a genuinely unknown token should land nowhere.
        """
        key = normalize_place_token(name)
        if not key:
            return None

        exact = self._exact.get(key)
        if exact is not None:
            return PlaceMatch(place=exact, similarity=1.0, matched_text=name, exact=True)

        # Only compare against surfaces sharing a first character — a full
        # 137-way SequenceMatcher sweep per token is wasteful at stream rates.
        best: PlaceMatch | None = None
        prefix = key[0]
        for surface, place in self._exact.items():
            if surface[0] != prefix or abs(len(surface) - len(key)) > 4:
                continue
            ratio = SequenceMatcher(None, key, surface).ratio()
            if ratio >= min_similarity and (best is None or ratio > best.similarity):
                best = PlaceMatch(
                    place=place, similarity=ratio, matched_text=name, exact=False
                )
        return best

    def extract_from_text(
        self, text: str, min_similarity: float = 0.82, max_candidates: int = 3
    ) -> list[PlaceMatch]:
        """Scan free text for place names, best match first.

        Returns several candidates so the caller can apply its own
        disambiguation — a tweet naming both a district and a landmark should
        not be silently collapsed to whichever appeared first.
        """
        if not text:
            return []

        # Split glued hashtag compounds before normalizing, or "#MumbaiRains"
        # arrives as one unmatchable token.
        tokens = [t for t in normalize_place_token(split_compound_tokens(text)).split(" ") if t]
        if not tokens:
            return []

        found: dict[str, PlaceMatch] = {}
        n = len(tokens)
        # Longest n-gram first: "New Delhi" must beat a bare "Delhi".
        for size in range(min(self.MAX_NGRAM, n), 0, -1):
            for i in range(n - size + 1):
                gram_tokens = tokens[i : i + size]
                if size == 1 and gram_tokens[0] in _STOPWORDS:
                    continue
                if size == 1 and len(gram_tokens[0]) < 4:
                    # Two- and three-letter tokens generate nothing but noise.
                    continue
                gram = " ".join(gram_tokens)
                match = self.lookup_fuzzy(gram, min_similarity=min_similarity)
                if match is None:
                    continue
                key = match.place.name
                incumbent = found.get(key)
                if incumbent is None or match.similarity > incumbent.similarity:
                    found[key] = match

        ranked = sorted(
            found.values(),
            key=lambda m: (m.exact, m.similarity, m.place.population_k),
            reverse=True,
        )
        return ranked[:max_candidates]

    def nearest(self, lat: float, lon: float, max_km: float = 150.0) -> tuple[Place, float] | None:
        """Nearest known place to a coordinate, for reverse labelling.

        Linear scan over 137 rows is ~microseconds and avoids pulling in a
        spatial index dependency. If the gazetteer grows past a few thousand
        entries, swap in a k-d tree here and nothing else changes.
        """
        best: tuple[Place, float] | None = None
        for place in self._places:
            distance = haversine_km(lat, lon, place.lat, place.lon)
            if distance <= max_km and (best is None or distance < best[1]):
                best = (place, distance)
        return best

    def iter_states(self) -> Iterable[str]:
        return sorted({p.state for p in self._places})


@lru_cache(maxsize=1)
def get_gazetteer() -> Gazetteer:
    """Process-wide singleton; the JSON is parsed exactly once per process."""
    return Gazetteer()
