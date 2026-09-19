"""Data contracts and models for user intelligence ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class IntelligencePillar(StrEnum):
    """Pillars of intelligence classification."""

    MACRO_LIQUIDITY = "MACRO_LIQUIDITY"
    REGULATORY = "REGULATORY"
    SECURITY_EXPLOIT = "SECURITY_EXPLOIT"
    INSTITUTIONAL = "INSTITUTIONAL"
    GEOPOLITICAL = "GEOPOLITICAL"
    USER_THESIS = "USER_THESIS"


@dataclass(frozen=True)
class UserIntelligenceRecord:
    """A verified user-submitted market intelligence record."""

    intelligence_id: str
    source_url: str | None
    title: str
    user_thesis: str
    raw_content: str
    pillar: IntelligencePillar
    sentiment_bias: float  # -1.0 to +1.0
    confidence_score: float  # 0.0 to 1.0
    tags: list[str]
    created_at_utc: datetime
    is_active: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert intelligence record to dictionary."""
        return {
            "intelligence_id": self.intelligence_id,
            "source_url": self.source_url,
            "title": self.title,
            "user_thesis": self.user_thesis,
            "raw_content": self.raw_content,
            "pillar": self.pillar.value,
            "sentiment_bias": self.sentiment_bias,
            "confidence_score": self.confidence_score,
            "tags": list(self.tags),
            "created_at_utc": self.created_at_utc.isoformat(),
            "is_active": self.is_active,
        }
