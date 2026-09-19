"""Data contracts and domain models for Phase 17 Investment Committee."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any


class CommitteePersona(StrEnum):
    """Personas participating in the investment committee."""

    MACRO_STRATEGIST = "MACRO_STRATEGIST"
    VALUATION_ANALYST = "VALUATION_ANALYST"
    RISK_OFFICER = "RISK_OFFICER"


class MemberStance(StrEnum):
    """Directional stance of a committee member."""

    BULLISH = "BULLISH"
    MODERATELY_BULLISH = "MODERATELY_BULLISH"
    NEUTRAL = "NEUTRAL"
    DEFENSIVE = "DEFENSIVE"
    CRISIS = "CRISIS"


class AllocationAction(StrEnum):
    """Action recommendation proposed by the committee."""

    AGGRESSIVE_ACCUMULATE = "AGGRESSIVE_ACCUMULATE"
    OPPORTUNISTIC_BUY = "OPPORTUNISTIC_BUY"
    STANDARD_DCA = "STANDARD_DCA"
    DEFENSIVE_HOLD = "DEFENSIVE_HOLD"
    EMERGENCY_HALT = "EMERGENCY_HALT"


@dataclass(frozen=True)
class PersonaVote:
    """Individual vote and rationale cast by a committee persona."""

    vote_id: str
    memo_id: str
    persona: CommitteePersona
    stance: MemberStance
    target_allocation_usd: float
    confidence: float  # 0.0 to 1.0
    rationale: str
    voted_at_utc: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert persona vote to dictionary representation."""
        return {
            "vote_id": self.vote_id,
            "memo_id": self.memo_id,
            "persona": self.persona.value,
            "stance": self.stance.value,
            "target_allocation_usd": self.target_allocation_usd,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "voted_at_utc": self.voted_at_utc.isoformat(),
        }


@dataclass(frozen=True)
class ClampingReceipt:
    """Audit receipt output from symbolic invariant solver."""

    proposed_allocation_usd: float
    clamped_allocation_usd: float
    is_clamped: bool
    triggered_rules: list[str] = field(default_factory=list)
    explanation: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert clamping receipt to dictionary representation."""
        return {
            "proposed_allocation_usd": self.proposed_allocation_usd,
            "clamped_allocation_usd": self.clamped_allocation_usd,
            "is_clamped": self.is_clamped,
            "triggered_rules": list(self.triggered_rules),
            "explanation": self.explanation,
        }


@dataclass(frozen=True)
class InvestmentMemorandum:
    """Complete institutional investment memorandum synthesized by the committee."""

    memo_id: str
    memo_date: date
    created_at_utc: datetime
    market_regime: str
    composite_mni: float
    consensus_score: float
    executive_summary_id: str
    macro_thesis: str
    valuation_thesis: str
    technical_thesis: str
    dissenting_opinions: str
    proposed_action: AllocationAction
    proposed_allocation_usd: float
    clamped_allocation_usd: float
    allocation_clamped: bool
    clamping_reason: str | None
    risk_guard_passed: bool
    memo_markdown: str
    votes: list[PersonaVote] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert memorandum to dictionary representation."""
        return {
            "memo_id": self.memo_id,
            "memo_date": self.memo_date.isoformat(),
            "created_at_utc": self.created_at_utc.isoformat(),
            "market_regime": self.market_regime,
            "composite_mni": self.composite_mni,
            "consensus_score": self.consensus_score,
            "executive_summary_id": self.executive_summary_id,
            "macro_thesis": self.macro_thesis,
            "valuation_thesis": self.valuation_thesis,
            "technical_thesis": self.technical_thesis,
            "dissenting_opinions": self.dissenting_opinions,
            "proposed_action": self.proposed_action.value,
            "proposed_allocation_usd": self.proposed_allocation_usd,
            "clamped_allocation_usd": self.clamped_allocation_usd,
            "allocation_clamped": self.allocation_clamped,
            "clamping_reason": self.clamping_reason,
            "risk_guard_passed": self.risk_guard_passed,
            "memo_markdown": self.memo_markdown,
            "votes": [v.to_dict() for v in self.votes],
        }
