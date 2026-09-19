"""Data models and type contracts for Phase 16 Macro & Narrative Intelligence Engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any


class MacroPillar(StrEnum):
    """Thematic news and analysis pillars."""

    REGULATORY = "REGULATORY"
    SECURITY_EXPLOIT = "SECURITY_EXPLOIT"
    INSTITUTIONAL = "INSTITUTIONAL"
    MACRO_LIQUIDITY = "MACRO_LIQUIDITY"
    GENERAL = "GENERAL"


class AlertSeverity(StrEnum):
    """Severity classification for news events and alerts."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class MacroRegime(StrEnum):
    """5-tier Macro-Narrative Index regime classification."""

    RISK_ON_EXPANSION = "RISK_ON_EXPANSION"
    CAUTIOUS_BULL = "CAUTIOUS_BULL"
    NEUTRAL_CHOP = "NEUTRAL_CHOP"
    RISK_OFF_DEFENSE = "RISK_OFF_DEFENSE"
    BLACK_SWAN_CRISIS = "BLACK_SWAN_CRISIS"


@dataclass(frozen=True)
class MacroArticle:
    """Ingested, classified, and deduplicated news article record."""

    article_id: str
    source: str
    title: str
    url: str
    published_utc: datetime
    summary: str = ""
    pillar: MacroPillar = MacroPillar.GENERAL
    severity: AlertSeverity = AlertSeverity.LOW
    polarity: float = 0.0
    matched_keywords: list[str] = field(default_factory=list)
    ingested_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Serialize article record to dictionary."""
        return {
            "article_id": self.article_id,
            "source": self.source,
            "title": self.title,
            "url": self.url,
            "published_utc": self.published_utc.isoformat(),
            "summary": self.summary,
            "pillar": self.pillar.value,
            "severity": self.severity.value,
            "polarity": round(self.polarity, 4),
            "matched_keywords": self.matched_keywords,
            "ingested_at_utc": self.ingested_at_utc.isoformat(),
        }


@dataclass(frozen=True)
class MacroEconomicRelease:
    """Parsed macroeconomic calendar event with calculated surprise delta and score."""

    release_id: str
    event_name: str
    country: str
    release_date: date
    release_time_utc: str
    impact: str
    actual_value: float | None = None
    forecast_value: float | None = None
    previous_value: float | None = None
    surprise_delta: float | None = None
    directional_score: float = 0.0
    raw_payload_json: str = "{}"
    ingested_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Serialize macroeconomic release record to dictionary."""
        return {
            "release_id": self.release_id,
            "event_name": self.event_name,
            "country": self.country,
            "release_date": self.release_date.isoformat(),
            "release_time_utc": self.release_time_utc,
            "impact": self.impact,
            "actual_value": self.actual_value,
            "forecast_value": self.forecast_value,
            "previous_value": self.previous_value,
            "surprise_delta": (
                round(self.surprise_delta, 4) if self.surprise_delta is not None else None
            ),
            "directional_score": round(self.directional_score, 4),
            "raw_payload_json": self.raw_payload_json,
            "ingested_at_utc": self.ingested_at_utc.isoformat(),
        }


@dataclass(frozen=True)
class DailyNarrativeReport:
    """Daily synthesized composite macro-narrative intelligence report."""

    intelligence_date: date
    synthesized_at_utc: datetime
    hard_macro_score: float
    sentiment_score: float
    narrative_score: float
    composite_mni: float
    regime: MacroRegime
    black_swan_flag: bool
    active_critical_alerts: int
    dominant_pillar: MacroPillar
    narrative_summary_id: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize daily narrative report to dictionary."""
        return {
            "intelligence_date": self.intelligence_date.isoformat(),
            "synthesized_at_utc": self.synthesized_at_utc.isoformat(),
            "hard_macro_score": round(self.hard_macro_score, 4),
            "sentiment_score": round(self.sentiment_score, 4),
            "narrative_score": round(self.narrative_score, 4),
            "composite_mni": round(self.composite_mni, 4),
            "regime": self.regime.value,
            "black_swan_flag": self.black_swan_flag,
            "active_critical_alerts": self.active_critical_alerts,
            "dominant_pillar": self.dominant_pillar.value,
            "narrative_summary_id": self.narrative_summary_id,
        }
