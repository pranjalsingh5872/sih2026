import math
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

# Hazard definitions aligned with IMD standard warnings
EVENT_CATEGORIES = [
    "FLASH_FLOOD",
    "HEAVY_RAINFALL",
    "THUNDERSTORM",
    "HEATWAVE",
    "FOG_DUST_STORM",
    "GENERAL_WEATHER"
]

CATEGORY_KEYWORDS = {
    "FLASH_FLOOD": ["flood", "waterlogging", "inundated", "submerged", "overflow", "drainage block", "water level"],
    "HEAVY_RAINFALL": ["downpour", "torrential", "heavy rain", "cloudburst", "deluge", "continuous rain"],
    "THUNDERSTORM": ["lightning", "thunder", "gusty winds", "hailstorm", "squall", "tree fallen"],
    "HEATWAVE": ["scorching", "heatwave", "loo", "extreme heat", "sunstroke", "high temperature"],
    "FOG_DUST_STORM": ["dust storm", "dense fog", "zero visibility", "smog", "sandstorm"]
}

SOURCE_WEIGHTS = {
    "IMD": 1.0,
    "SENSOR": 0.95,
    "OPENWEATHER": 0.85,
    "CITIZEN": 0.70,
    "SOCIAL": 0.40
}

class IncidentAIEngine:
    def __init__(self):
        self.active_window_records: List[Dict[str, Any]] = []
        self.similarity_threshold = 0.80
        self.window_seconds = 10800  # 3-hour deduplication window
        words = set([w for kws in CATEGORY_KEYWORDS.values() for w in kws])
        self.vocab = sorted(list(words))

    def classify_event(self, text: str) -> Tuple[str, float]:
        text_lower = (text or "").lower()
        matched_scores = {}

        for category, kws in CATEGORY_KEYWORDS.items():
            count = sum(1 for kw in kws if kw in text_lower)
            if count > 0:
                matched_scores[category] = min(0.60 + (count * 0.15), 0.98)

        if not matched_scores:
            return "GENERAL_WEATHER", 0.50

        best_category = max(matched_scores, key=matched_scores.get)
        return best_category, matched_scores[best_category]

    def _generate_bow_vector(self, text: str) -> List[float]:
        text_words = (text or "").lower().split()
        vec = [float(text_words.count(word)) for word in self.vocab]
        magnitude = math.sqrt(sum(x * x for x in vec))
        if magnitude == 0:
            return vec
        return [x / magnitude for x in vec]

    @staticmethod
    def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        return sum(a * b for a, b in zip(vec1, vec2))

    def check_duplicate(self, incident_id: str, text: str, lat: Optional[float], lon: Optional[float], timestamp: datetime) -> Tuple[bool, Optional[str]]:
        now = datetime.now(timezone.utc)
        self.active_window_records = [
            r for r in self.active_window_records 
            if (now - r["timestamp"]).total_seconds() <= self.window_seconds
        ]

        vec = self._generate_bow_vector(text)
        mag = math.sqrt(sum(x * x for x in vec))
        if mag == 0:
            self._cache_record(incident_id, vec, timestamp, lat, lon)
            return False, None

        for rec in self.active_window_records:
            if rec["id"] == incident_id:
                continue

            sim = self._cosine_similarity(vec, rec["vector"])
            if sim >= self.similarity_threshold:
                if lat is not None and lon is not None and rec["lat"] is not None and rec["lon"] is not None:
                    dist_km = self.haversine_distance(lat, lon, rec["lat"], rec["lon"])
                    if dist_km <= 15.0:
                        return True, rec["id"]
                else:
                    return True, rec["id"]

        self._cache_record(incident_id, vec, timestamp, lat, lon)
        return False, None

    def _cache_record(self, incident_id: str, vector: List[float], timestamp: datetime, lat: Optional[float], lon: Optional[float]):
        self.active_window_records.append({
            "id": incident_id,
            "vector": vector,
            "timestamp": timestamp,
            "lat": lat,
            "lon": lon
        })

    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2.0) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def compute_credibility(
        self,
        source_type: str,
        has_gps: bool,
        has_media: bool,
        is_duplicate: bool,
        nearby_corroborations: int = 0
    ) -> Tuple[float, str]:
        base_authority = SOURCE_WEIGHTS.get(source_type.upper(), 0.50)
        score = base_authority

        if has_gps:
            score += 0.10
        else:
            score -= 0.15

        if has_media:
            score += 0.10

        if is_duplicate:
            score -= 0.20

        if nearby_corroborations > 0:
            score += min(nearby_corroborations * 0.05, 0.15)

        credibility = max(0.05, min(0.99, round(score, 3)))

        if credibility >= 0.75:
            verification_status = "AUTO_VERIFIED"
        elif credibility >= 0.40:
            verification_status = "FLAGGED_FOR_REVIEW"
        else:
            verification_status = "SUSPICIOUS"

        return credibility, verification_status

ai_engine = IncidentAIEngine()