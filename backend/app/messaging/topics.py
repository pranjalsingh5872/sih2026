"""Kafka topic names and consumer-group identifiers.

Centralised so a topic rename is one edit rather than a grep across workers.
Names match ``infra/kafka/create-topics.sh``.
"""

from __future__ import annotations

from typing import Final


class Topics:
    """Topic name constants."""

    # Everything enters here verbatim, whatever the provider's payload shape.
    RAW_WEATHER: Final[str] = "raw-weather-stream"

    # The unified contract. Phase 2's AI engine consumes this.
    NORMALIZED_INCIDENTS: Final[str] = "normalized-incident-stream"

    # Messages the pipeline refused, kept for operator review and replay.
    DEAD_LETTER: Final[str] = "incident-dead-letter"

    # Located-but-not-locatable reports. Still real signal: an operator can
    # often place them by hand from the text, so they are fanned out here as
    # well as onto the main stream.
    GEO_UNRESOLVED: Final[str] = "geo-unresolved-incidents"

    @classmethod
    def all(cls) -> tuple[str, ...]:
        return (
            cls.RAW_WEATHER,
            cls.NORMALIZED_INCIDENTS,
            cls.DEAD_LETTER,
            cls.GEO_UNRESOLVED,
        )


class ConsumerGroups:
    """Consumer-group ids. One group per logical role, never per replica."""

    NORMALIZER: Final[str] = "normalizer-workers"
    AI_ENRICHMENT: Final[str] = "ai-enrichment-workers"       # Phase 2
    EVENT_FUSION: Final[str] = "event-fusion-workers"         # Phase 3
    POSTGRES_SINK: Final[str] = "postgres-sink-workers"       # Phase 3
    DEAD_LETTER_AUDIT: Final[str] = "dead-letter-audit"
