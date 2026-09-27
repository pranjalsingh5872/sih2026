"""Simulated social/RSS feed generator.

Generates the messy stream Phase 2 has to survive. The mix is controlled by
configuration and is deliberately unkind:

* **Near-duplicates and verbatim reposts** — the same claim recirculating with
  slight rewording is the single most common pattern in disaster social media,
  and it is what inflates a two-street incident into an apparent city-wide
  catastrophe. Embedding-based dedup must catch these.
* **Missing coordinates** — the default for most posts. Forces the geo
  fallback chain to earn its place.
* **Misinformation** — old-footage reposts, fabricated dam breaches, wrong-city
  claims. Every synthetic fake carries a ``synthetic_label`` so Phase 2's
  credibility model can be measured against known ground truth instead of
  vibes. Production feeds simply do not have that key.

Nothing here is used as a data source in production; it stands in for the
scraper/ingestion connectors that would attach to real platform APIs.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from app.geo.gazetteer import Place, get_gazetteer

# (template, language). ``{place}`` is substituted at generation time.
_TEMPLATES: tuple[tuple[str, str], ...] = (
    ("Heavy rain lashing {place} since morning, roads completely waterlogged #{tag}", "en"),
    ("Water entered our building basement in {place}. Situation getting worse.", "en"),
    ("Traffic at a standstill in {place} due to waterlogging. Avoid the area.", "en"),
    ("Severe thunderstorm over {place} right now, lightning every few seconds", "en"),
    ("{place} mein bahut tez baarish ho rahi hai, sadak par paani bhar gaya", "en"),
    ("भारी बारिश के कारण {place} में जलभराव, यातायात प्रभावित #{tag}", "hi"),
    ("{place} में सड़कों पर पानी भर गया है, लोग फंसे हुए हैं", "hi"),
    ("{place} में तेज आंधी और बिजली गिरने की खबर", "hi"),
    ("{place} ನಲ್ಲಿ ಭಾರೀ ಮಳೆ, ರಸ್ತೆಗಳಲ್ಲಿ ನೀರು ನಿಂತಿದೆ", "kn"),
    ("{place} இல் கனமழை, சாலைகளில் வெள்ளம் #{tag}", "ta"),
    ("{place} এ ভারী বৃষ্টি, রাস্তায় জল জমেছে", "bn"),
    ("{place} లో భారీ వర్షం, రోడ్లపై నీరు నిలిచింది", "te"),
    ("{place} ൽ കനത്ത മഴ, ഗതാഗതം തടസ്സപ്പെട്ടു", "ml"),
    ("{place} માં ભારે વરસાદ, રસ્તાઓ પર પાણી", "gu"),
    ("Landslide reported near {place}, highway blocked both directions", "en"),
    ("Dense fog in {place}, visibility almost zero on the bypass", "en"),
    ("Hailstorm just hit {place}, crops damaged across nearby villages", "en"),
    ("Severe heatwave in {place}, temperature crossed 45 degrees today", "en"),
)

# Reposts that recirculate an earlier claim with cosmetic changes.
_REPOST_PREFIXES: tuple[str, ...] = (
    "RT @localnews: ", "Forwarded as received: ", "Breaking — ", "",
    "Please share widely: ", "देखिए: ",
)

# Misinformation patterns observed in real Indian disaster events. Each is
# tagged so it can be scored as a known negative downstream.
_FAKE_TEMPLATES: tuple[tuple[str, str], ...] = (
    ("BREAKING: {place} dam has broken, entire city will be submerged in 2 hours. "
     "Evacuate immediately! Forward to everyone!", "fabricated_dam_breach"),
    ("Army has taken over {place}, all roads sealed, no one allowed to move. "
     "Official announcement coming soon.", "fabricated_authority_claim"),
    ("{place} में 200 लोगों की मौत, सरकार छुपा रही है सच्चाई। शेयर करें!",
     "fabricated_casualty_figures"),
    ("Shocking visuals from {place} flood today - watch how the whole market "
     "washed away", "recycled_old_footage"),
    ("Government confirms {place} will receive 500mm rain in next 3 hours, "
     "all schools closed permanently", "fabricated_official_forecast"),
)

_HASHTAGS: tuple[str, ...] = (
    "Rains", "Floods", "Waterlogging", "WeatherAlert", "Monsoon", "HeavyRain",
)

_PLATFORMS: tuple[str, ...] = ("x", "facebook", "instagram", "rss", "whatsapp_public")


class SocialFeedGenerator:
    """Produces synthetic social posts with configurable pathology rates."""

    def __init__(
        self,
        *,
        duplicate_ratio: float = 0.22,
        missing_geo_ratio: float = 0.35,
        fake_ratio: float = 0.12,
        seed: int | None = None,
    ) -> None:
        self._duplicate_ratio = duplicate_ratio
        self._missing_geo_ratio = missing_geo_ratio
        self._fake_ratio = fake_ratio
        self._rng = random.Random(seed)
        self._recent: list[dict[str, Any]] = []
        # Weight toward larger cities: that is where the volume genuinely is.
        places = list(get_gazetteer().places)
        self._places = places
        self._weights = [max(1, p.population_k) for p in places]

    # ------------------------------------------------------------ generation --
    def generate(self) -> dict[str, Any]:
        """Produce one post."""
        roll = self._rng.random()
        if roll < self._duplicate_ratio and self._recent:
            return self._make_repost()
        if roll < self._duplicate_ratio + self._fake_ratio:
            return self._make_fake()
        return self._make_genuine()

    def generate_batch(self, count: int) -> list[dict[str, Any]]:
        return [self.generate() for _ in range(count)]

    # --------------------------------------------------------------- variants --
    def _make_genuine(self) -> dict[str, Any]:
        place = self._pick_place()
        template, language = self._rng.choice(_TEMPLATES)
        text = template.format(place=place.name, tag=f"{place.name}{self._rng.choice(_HASHTAGS)}")
        post = self._build(text, place, language)
        self._remember(post)
        return post

    def _make_repost(self) -> dict[str, Any]:
        """Recirculate a recent post with cosmetic mutation."""
        original = self._rng.choice(self._recent)
        text = original["text"]

        mutation = self._rng.random()
        if mutation < 0.45:
            text = self._rng.choice(_REPOST_PREFIXES) + text   # verbatim repost
        elif mutation < 0.75:
            text = text.replace("Heavy", "Very heavy").replace("भारी", "बहुत भारी")
            text += " " + self._rng.choice(("Stay safe!", "कृपया सावधान रहें", "Be careful"))
        else:
            # Word-order shuffle: defeats exact hashing, not embeddings.
            words = text.split()
            if len(words) > 6:
                pivot = len(words) // 2
                text = " ".join(words[pivot:] + words[:pivot])

        post = self._build(
            text,
            place=None,
            language=original.get("lang", "en"),
            lat=original.get("lat"),
            lon=original.get("lon"),
            place_name=(original.get("geo") or {}).get("place_name"),
        )
        post["is_repost"] = True
        post["reposted_from_id"] = original["post_id"]
        post["synthetic_label"] = "near_duplicate"
        return post

    def _make_fake(self) -> dict[str, Any]:
        place = self._pick_place()
        template, label = self._rng.choice(_FAKE_TEMPLATES)
        text = template.format(place=place.name)
        post = self._build(text, place, "hi" if any(c > "\u0900" for c in template) else "en")
        post["synthetic_label"] = label
        # Misinformation tends to come from young, low-follower, unverified
        # accounts and to spread further than the genuine reports it displaces.
        post["author"]["verified"] = False
        post["author"]["account_age_days"] = self._rng.randint(1, 45)
        post["author"]["follower_count"] = self._rng.randint(5, 400)
        post["repost_count"] = self._rng.randint(200, 5000)
        self._remember(post)
        return post

    # ---------------------------------------------------------------- helpers --
    def _pick_place(self) -> Place:
        return self._rng.choices(self._places, weights=self._weights, k=1)[0]

    def _build(
        self,
        text: str,
        place: Place | None,
        language: str,
        *,
        lat: float | None = None,
        lon: float | None = None,
        place_name: str | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        posted_at = now - timedelta(seconds=self._rng.randint(0, 900))
        has_geo = self._rng.random() >= self._missing_geo_ratio

        if place is not None and has_geo and lat is None:
            # Social geotags are coarse; jitter reflects that honestly.
            lat = round(place.lat + self._rng.uniform(-0.08, 0.08), 5)
            lon = round(place.lon + self._rng.uniform(-0.08, 0.08), 5)
            place_name = place.name
        elif not has_geo:
            lat = lon = None
            # Half the time the place name survives in the text only.
            place_name = place.name if (place and self._rng.random() < 0.5) else None

        age_days = self._rng.randint(1, 4000)
        return {
            "post_id": uuid.uuid4().hex[:18],
            "platform": self._rng.choice(_PLATFORMS),
            "text": text,
            "lang": language,
            "created_at": posted_at.isoformat(),
            "lat": lat,
            "lon": lon,
            "geo": {"lat": lat, "lon": lon, "place_name": place_name} if (lat or place_name) else {},
            "author": {
                "id": f"u{self._rng.randint(100000, 999999)}",
                "handle": f"user_{self._rng.randint(1000, 99999)}",
                "verified": self._rng.random() < 0.08,
                "account_age_days": age_days,
                "follower_count": self._rng.randint(0, 50000),
            },
            "repost_count": self._rng.randint(0, 250),
            "reply_count": self._rng.randint(0, 80),
            "like_count": self._rng.randint(0, 900),
            "media": (
                [{"url": f"https://cdn.example.invalid/{uuid.uuid4().hex[:12]}.jpg", "type": "photo"}]
                if self._rng.random() < 0.3 else []
            ),
            "url": f"https://social.example.invalid/p/{uuid.uuid4().hex[:10]}",
            "is_repost": False,
        }

    def _remember(self, post: dict[str, Any]) -> None:
        """Keep a rolling window of recent posts to draw reposts from."""
        self._recent.append(post)
        if len(self._recent) > 120:
            self._recent.pop(0)
