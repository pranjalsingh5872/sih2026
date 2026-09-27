"""Text cleaning, language identification and rule-based hazard tagging.

Indian weather chatter arrives in a dozen scripts and in Romanised transliteration
("bahut tez baarish"), interleaved with URLs, handles and emoji. This module
produces the cleaned string that Phase 2 will embed, and does *cheap* hazard
tagging on the way through.

The keyword rules here are deliberately high-precision and low-recall. Anything
they cannot label confidently stays ``UNKNOWN`` for the Phase 2 zero-shot
classifier. A rule engine that guesses is worse than one that abstains, because
a wrong category propagates into clustering and then into an alert.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Final

from app.schemas.enums import HazardCategory, SeverityHint

# --------------------------------------------------------------- cleaning ---
_URL_RE: Final = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_MENTION_RE: Final = re.compile(r"@[\w_]{2,30}")
_HASHTAG_RE: Final = re.compile(r"#(\w+)")
_RT_RE: Final = re.compile(r"^\s*RT\s+@?[\w_]*:?\s*", re.IGNORECASE)
_WS_RE: Final = re.compile(r"\s+")
_REPEAT_PUNCT_RE: Final = re.compile(r"([!?.,])\1{2,}")

# Emoji and pictographic blocks. Stripped because they add no semantic signal
# to the embedding but do add token noise.
_EMOJI_RE: Final = re.compile(
    "[" 
    "\U0001F300-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\U00002600-\U000027BF"
    "\U0001F1E6-\U0001F1FF"
    "\U00002190-\U000021FF"
    "\U0000FE00-\U0000FE0F"
    "]+",
    flags=re.UNICODE,
)

# Unicode script ranges, ordered so the first hit wins.
_SCRIPT_RANGES: Final[tuple[tuple[str, int, int], ...]] = (
    ("hi", 0x0900, 0x097F),  # Devanagari — Hindi/Marathi/Nepali
    ("bn", 0x0980, 0x09FF),  # Bengali/Assamese
    ("pa", 0x0A00, 0x0A7F),  # Gurmukhi
    ("gu", 0x0A80, 0x0AFF),  # Gujarati
    ("or", 0x0B00, 0x0B7F),  # Odia
    ("ta", 0x0B80, 0x0BFF),  # Tamil
    ("te", 0x0C00, 0x0C7F),  # Telugu
    ("kn", 0x0C80, 0x0CFF),  # Kannada
    ("ml", 0x0D00, 0x0D7F),  # Malayalam
    ("ur", 0x0600, 0x06FF),  # Arabic block — Urdu in this context
)


def clean_text(raw: str, *, keep_hashtag_words: bool = True) -> str:
    """Normalise a report body for embedding and display.

    Hashtag *words* are kept by default — ``#MumbaiRains`` carries both the
    hazard and the location — while the ``#`` itself is dropped.
    """
    if not raw:
        return ""

    text = unicodedata.normalize("NFKC", raw)
    text = _RT_RE.sub("", text)
    text = _URL_RE.sub(" ", text)
    text = _MENTION_RE.sub(" ", text)
    text = _HASHTAG_RE.sub(r"\1" if keep_hashtag_words else " ", text)
    text = _EMOJI_RE.sub(" ", text)
    text = _REPEAT_PUNCT_RE.sub(r"\1", text)
    # Strip zero-width joiners and other format characters that survive NFKC.
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Cf")
    return _WS_RE.sub(" ", text).strip()


def detect_language(text: str) -> str:
    """Identify the dominant script and map it to an ISO-639-1 code.

    Script detection, not language detection: Hindi and Marathi share
    Devanagari and both return ``hi``. That is sufficient for Phase 1 routing,
    and it never fails on short inputs the way statistical detectors do.
    Returns ``und`` when undeterminable.
    """
    if not text or not text.strip():
        return "und"

    counts: dict[str, int] = {}
    latin = 0
    for char in text:
        code = ord(char)
        if 0x0041 <= code <= 0x007A and char.isalpha():
            latin += 1
            continue
        for lang, low, high in _SCRIPT_RANGES:
            if low <= code <= high:
                counts[lang] = counts.get(lang, 0) + 1
                break

    if counts:
        dominant = max(counts, key=lambda k: counts[k])
        # Mixed-script posts are common; require the Indic script to be a real
        # presence rather than one stray character.
        if counts[dominant] >= max(3, 0.15 * (latin + sum(counts.values()))):
            return dominant

    return "en" if latin >= 3 else "und"


# ----------------------------------------------------------- hazard rules ---
# Each entry: category -> tuple of lowercase surface forms across languages and
# transliterations. Matching is substring-based on the cleaned, casefolded text.
_CATEGORY_KEYWORDS: Final[dict[HazardCategory, tuple[str, ...]]] = {
    HazardCategory.FLASH_FLOOD: (
        "flash flood", "cloudburst", "cloud burst", "badal phatna", "बादल फटा",
        "अचानक बाढ़", "flash-flood", "मेघ विस्फोट",
    ),
    HazardCategory.URBAN_FLOODING: (
        "waterlogging", "water logging", "waterlogged", "urban flood",
        "knee deep water", "submerged road", "जलभराव", "पानी भर", "सड़क पर पानी",
        "நீர் தேக்கம்", "জল জমে",
    ),
    HazardCategory.LANDSLIDE: (
        "landslide", "landslip", "mudslide", "rockfall", "भूस्खलन", "पहाड़ खिसक",
        "मलबा", "நிலச்சரிவு", "ধস",
    ),
    HazardCategory.CYCLONE: (
        "cyclone", "cyclonic storm", "depression intensif", "landfall",
        "चक्रवात", "तूफान आ रहा", "புயல்", "ঘূর্ণিঝড়",
    ),
    HazardCategory.HEATWAVE: (
        "heatwave", "heat wave", "loo chal", "severe heat", "लू", "गर्मी की लहर",
        "भीषण गर्मी", "வெப்ப அலை",
    ),
    HazardCategory.COLDWAVE: (
        "cold wave", "coldwave", "cold day", "शीत लहर", "शीतलहर", "कड़ाके की ठंड",
    ),
    HazardCategory.DENSE_FOG: (
        "dense fog", "thick fog", "zero visibility", "घना कोहरा", "कोहरा",
        "কুয়াশা",
    ),
    HazardCategory.DUST_STORM: (
        "dust storm", "duststorm", "sandstorm", "andhi", "धूल भरी आंधी", "आंधी",
        "धूल का तूफान",
    ),
    HazardCategory.HAILSTORM: (
        "hailstorm", "hailstone", "hail storm", "ओलावृष्टि", "ओले", "শিলাবৃষ্টি",
    ),
    HazardCategory.LIGHTNING: (
        "lightning strike", "lightning", "thunderbolt", "आकाशीय बिजली",
        "बिजली गिर", "வானத் தாக்கு",
    ),
    HazardCategory.THUNDERSTORM: (
        "thunderstorm", "thunder storm", "squall", "gusty wind", "आंधी तूफान",
        "गरज के साथ", "तेज हवा", "இடியுடன்",
    ),
    HazardCategory.SNOWFALL: (
        "snowfall", "heavy snow", "बर्फबारी", "हिमपात",
    ),
    HazardCategory.HEAVY_RAINFALL: (
        "heavy rain", "heavy rainfall", "very heavy rain", "torrential rain",
        "extremely heavy rain", "downpour", "भारी बारिश", "मूसलाधार", "तेज बारिश",
        "भारी वर्षा", "கனமழை", "ভারী বৃষ্টি", "ಭಾರೀ ಮಳೆ", "భారీ వర్షం",
        "കനത്ത മഴ", "baarish", "barish",
    ),
}

# Explicit precedence. Specific hazards outrank the generic rainfall bucket,
# because "flash flood" also contains no rain keyword but is far more severe.
_CATEGORY_PRECEDENCE: Final[tuple[HazardCategory, ...]] = (
    HazardCategory.FLASH_FLOOD,
    HazardCategory.LANDSLIDE,
    HazardCategory.CYCLONE,
    HazardCategory.URBAN_FLOODING,
    HazardCategory.HAILSTORM,
    HazardCategory.LIGHTNING,
    HazardCategory.DUST_STORM,
    HazardCategory.DENSE_FOG,
    HazardCategory.HEATWAVE,
    HazardCategory.COLDWAVE,
    HazardCategory.SNOWFALL,
    HazardCategory.THUNDERSTORM,
    HazardCategory.HEAVY_RAINFALL,
)

_GENERIC_FLOOD_TERMS: Final[tuple[str, ...]] = (
    "flood", "flooding", "flooded", "inundat", "बाढ़", "வெள்ளம்", "বন্যা",
    "ಪ್ರವಾಹ", "వరద", "വെള്ളപ്പൊക്കം",
)

_SEVERITY_KEYWORDS: Final[dict[SeverityHint, tuple[str, ...]]] = {
    SeverityHint.EXTREME: (
        "red alert", "extremely heavy", "catastrophic", "life threatening",
        "evacuate", "रेड अलर्ट", "अत्यंत भारी", "जानलेवा", "खाली करें",
    ),
    SeverityHint.SEVERE: (
        "orange alert", "very heavy", "severe", "emergency", "stranded", "rescue",
        "ऑरेंज अलर्ट", "अत्यधिक भारी", "गंभीर", "फंसे", "बचाव",
    ),
    SeverityHint.MODERATE: (
        "yellow alert", "moderate", "disrupted", "waterlogging",
        "येलो अलर्ट", "मध्यम", "बाधित",
    ),
    SeverityHint.MINOR: (
        "light rain", "drizzle", "minor", "हल्की बारिश", "बूंदाबांदी",
    ),
}


def classify_by_keywords(text: str) -> tuple[HazardCategory, float]:
    """Rule-based hazard tagging.

    Returns ``(category, confidence)``. Confidence is a coarse proxy — the real
    calibrated score comes from the Phase 2 classifier — but it lets the
    dashboard show *something* before the model has run.
    """
    if not text:
        return HazardCategory.UNKNOWN, 0.0

    lowered = text.casefold()

    for category in _CATEGORY_PRECEDENCE:
        for keyword in _CATEGORY_KEYWORDS.get(category, ()):
            if keyword in lowered:
                # Multi-word phrase matches are much less likely to be
                # coincidental than a single common word.
                confidence = 0.85 if " " in keyword else 0.70
                return category, confidence

    for term in _GENERIC_FLOOD_TERMS:
        if term in lowered:
            return HazardCategory.URBAN_FLOODING, 0.55

    return HazardCategory.UNKNOWN, 0.0


def severity_from_keywords(text: str) -> SeverityHint:
    """Extract the severity the *source* is claiming, most severe wins."""
    if not text:
        return SeverityHint.UNKNOWN
    lowered = text.casefold()
    for severity in (
        SeverityHint.EXTREME,
        SeverityHint.SEVERE,
        SeverityHint.MODERATE,
        SeverityHint.MINOR,
    ):
        if any(keyword in lowered for keyword in _SEVERITY_KEYWORDS[severity]):
            return severity
    return SeverityHint.UNKNOWN


def extract_hashtags(raw: str, limit: int = 10) -> list[str]:
    """Pull hashtags off the *uncleaned* text, lowercased and deduplicated."""
    seen: list[str] = []
    for match in _HASHTAG_RE.finditer(raw or ""):
        tag = match.group(1).casefold()
        if tag not in seen:
            seen.append(tag)
        if len(seen) >= limit:
            break
    return seen


def truncate(text: str | None, limit: int = 8192) -> str | None:
    """Clamp to the schema's field length without splitting a surrogate pair."""
    if text is None:
        return None
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"
