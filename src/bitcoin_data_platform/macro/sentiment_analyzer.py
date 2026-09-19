"""Lexical polarity engine, thematic pillar classifier, and black swan regulatory sentinel."""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from bitcoin_data_platform.macro.models import (
    AlertSeverity,
    MacroArticle,
    MacroPillar,
)


@dataclass(frozen=True)
class Rule:
    """Keyword pattern rule with pillar, sentiment score, and severity."""

    pattern: re.Pattern[str]
    pillar: MacroPillar
    weight: float  # Positive = bullish, Negative = bearish
    severity: AlertSeverity
    token_label: str


def _compile_rule(
    raw_pattern: str,
    pillar: MacroPillar,
    weight: float,
    severity: AlertSeverity,
) -> Rule:
    """Compile a case-insensitive regex rule with word boundaries."""
    # Ensure regex matches on word boundaries
    pat_str = rf"\b{raw_pattern}\b"
    return Rule(
        pattern=re.compile(pat_str, re.IGNORECASE),
        pillar=pillar,
        weight=weight,
        severity=severity,
        token_label=raw_pattern.lower(),
    )


# Curated dictionary rules across 4 thematic pillars
RULES: list[Rule] = [
    # -------------------------------------------------------------
    # 1. REGULATORY PILLAR
    # -------------------------------------------------------------
    # Critical Black Swan (-3.0)
    _compile_rule(r"doj indictment", MacroPillar.REGULATORY, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"nationwide outright ban", MacroPillar.REGULATORY, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"outright ban", MacroPillar.REGULATORY, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"asset freezing order", MacroPillar.REGULATORY, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"criminal charges", MacroPillar.REGULATORY, -3.0, AlertSeverity.CRITICAL),
    # Bearish Regulatory (-1.0 to -2.0)
    _compile_rule(r"sec lawsuit", MacroPillar.REGULATORY, -2.0, AlertSeverity.HIGH),
    _compile_rule(r"sec sues", MacroPillar.REGULATORY, -2.0, AlertSeverity.HIGH),
    _compile_rule(r"regulatory clampdown", MacroPillar.REGULATORY, -2.0, AlertSeverity.HIGH),
    _compile_rule(r"enforcement action", MacroPillar.REGULATORY, -2.0, AlertSeverity.HIGH),
    _compile_rule(r"subpoena", MacroPillar.REGULATORY, -1.0, AlertSeverity.MEDIUM),
    _compile_rule(r"ban proposal", MacroPillar.REGULATORY, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"crackdown", MacroPillar.REGULATORY, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"investigation", MacroPillar.REGULATORY, -1.0, AlertSeverity.MEDIUM),
    _compile_rule(r"fine", MacroPillar.REGULATORY, -0.8, AlertSeverity.LOW),
    # Bullish Regulatory (+1.0 to +2.0)
    _compile_rule(r"etf approval", MacroPillar.REGULATORY, 2.0, AlertSeverity.HIGH),
    _compile_rule(r"etf approved", MacroPillar.REGULATORY, 2.0, AlertSeverity.HIGH),
    _compile_rule(r"approves spot bitcoin etf", MacroPillar.REGULATORY, 2.0, AlertSeverity.HIGH),
    _compile_rule(r"legal clarification", MacroPillar.REGULATORY, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"regulatory clarity", MacroPillar.REGULATORY, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"pro-crypto bill", MacroPillar.REGULATORY, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"license granted", MacroPillar.REGULATORY, 1.0, AlertSeverity.LOW),
    _compile_rule(r"sec dismisses", MacroPillar.REGULATORY, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"court victory", MacroPillar.REGULATORY, 1.5, AlertSeverity.MEDIUM),
    # -------------------------------------------------------------
    # 2. SECURITY & EXPLOIT PILLAR
    # -------------------------------------------------------------
    # Critical Black Swan (-3.0)
    _compile_rule(
        r"exchange insolvency", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL
    ),
    _compile_rule(r"insolvency", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"bankrupt", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"bankruptcy", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"chapter 11", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"halts withdrawals", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(
        r"halted withdrawals", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL
    ),
    _compile_rule(r"bank run", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"stablecoin depeg", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"major cex hack", MacroPillar.SECURITY_EXPLOIT, -3.0, AlertSeverity.CRITICAL),
    # Bearish Security (-1.0 to -2.0)
    _compile_rule(r"bridge vulnerability", MacroPillar.SECURITY_EXPLOIT, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"protocol bug", MacroPillar.SECURITY_EXPLOIT, -1.0, AlertSeverity.MEDIUM),
    _compile_rule(r"phishing surge", MacroPillar.SECURITY_EXPLOIT, -1.0, AlertSeverity.MEDIUM),
    _compile_rule(r"exploit", MacroPillar.SECURITY_EXPLOIT, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"security breach", MacroPillar.SECURITY_EXPLOIT, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"hacked", MacroPillar.SECURITY_EXPLOIT, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"flash loan attack", MacroPillar.SECURITY_EXPLOIT, -1.5, AlertSeverity.HIGH),
    # Bullish Security (+1.0 to +1.5)
    _compile_rule(r"recovery of funds", MacroPillar.SECURITY_EXPLOIT, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"funds recovered", MacroPillar.SECURITY_EXPLOIT, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"whitehat return", MacroPillar.SECURITY_EXPLOIT, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"audit passed", MacroPillar.SECURITY_EXPLOIT, 1.0, AlertSeverity.LOW),
    _compile_rule(r"vulnerability patched", MacroPillar.SECURITY_EXPLOIT, 1.0, AlertSeverity.LOW),
    # -------------------------------------------------------------
    # 3. INSTITUTIONAL ADOPTION PILLAR
    # -------------------------------------------------------------
    # Critical Black Swan (-3.0)
    _compile_rule(
        r"major custodian bankruptcy", MacroPillar.INSTITUTIONAL, -3.0, AlertSeverity.CRITICAL
    ),
    _compile_rule(r"custodian collapse", MacroPillar.INSTITUTIONAL, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(
        r"mega-whale forced liquidation", MacroPillar.INSTITUTIONAL, -3.0, AlertSeverity.CRITICAL
    ),
    # Bearish Institutional (-1.0 to -2.0)
    _compile_rule(r"fund liquidation", MacroPillar.INSTITUTIONAL, -2.0, AlertSeverity.HIGH),
    _compile_rule(r"miner capitulation", MacroPillar.INSTITUTIONAL, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"outflow streak", MacroPillar.INSTITUTIONAL, -1.2, AlertSeverity.MEDIUM),
    _compile_rule(r"etf outflow", MacroPillar.INSTITUTIONAL, -1.0, AlertSeverity.MEDIUM),
    _compile_rule(r"institutional dumping", MacroPillar.INSTITUTIONAL, -1.5, AlertSeverity.HIGH),
    # Bullish Institutional (+1.0 to +2.0)
    _compile_rule(r"treasury allocation", MacroPillar.INSTITUTIONAL, 2.0, AlertSeverity.HIGH),
    _compile_rule(r"sovereign adoption", MacroPillar.INSTITUTIONAL, 2.0, AlertSeverity.HIGH),
    _compile_rule(r"etf inflow record", MacroPillar.INSTITUTIONAL, 2.0, AlertSeverity.HIGH),
    _compile_rule(r"etf inflow", MacroPillar.INSTITUTIONAL, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"record inflows", MacroPillar.INSTITUTIONAL, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"institutional buy", MacroPillar.INSTITUTIONAL, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"microstrategy", MacroPillar.INSTITUTIONAL, 1.0, AlertSeverity.LOW),
    _compile_rule(r"accumulates bitcoin", MacroPillar.INSTITUTIONAL, 1.2, AlertSeverity.LOW),
    # -------------------------------------------------------------
    # 4. MACRO LIQUIDITY PILLAR
    # -------------------------------------------------------------
    # Critical Black Swan (-3.0)
    _compile_rule(
        r"global liquidity freeze", MacroPillar.MACRO_LIQUIDITY, -3.0, AlertSeverity.CRITICAL
    ),
    _compile_rule(
        r"systemic bank failure", MacroPillar.MACRO_LIQUIDITY, -3.0, AlertSeverity.CRITICAL
    ),
    _compile_rule(r"banking contagion", MacroPillar.MACRO_LIQUIDITY, -3.0, AlertSeverity.CRITICAL),
    _compile_rule(r"financial crisis", MacroPillar.MACRO_LIQUIDITY, -3.0, AlertSeverity.CRITICAL),
    # Bearish Macro Liquidity (-1.0 to -2.0)
    _compile_rule(r"fed rate hike", MacroPillar.MACRO_LIQUIDITY, -2.0, AlertSeverity.HIGH),
    _compile_rule(r"balance sheet runoff", MacroPillar.MACRO_LIQUIDITY, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"banking panic", MacroPillar.MACRO_LIQUIDITY, -2.0, AlertSeverity.HIGH),
    _compile_rule(r"rate hike", MacroPillar.MACRO_LIQUIDITY, -1.5, AlertSeverity.HIGH),
    _compile_rule(r"hawkish pause", MacroPillar.MACRO_LIQUIDITY, -1.0, AlertSeverity.MEDIUM),
    _compile_rule(
        r"quantitative tightening", MacroPillar.MACRO_LIQUIDITY, -1.5, AlertSeverity.HIGH
    ),
    _compile_rule(r"inflation surge", MacroPillar.MACRO_LIQUIDITY, -1.2, AlertSeverity.MEDIUM),
    # Bullish Macro Liquidity (+1.0 to +2.0)
    _compile_rule(r"fed rate cut", MacroPillar.MACRO_LIQUIDITY, 2.0, AlertSeverity.HIGH),
    _compile_rule(r"rate cut", MacroPillar.MACRO_LIQUIDITY, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"quantitative easing", MacroPillar.MACRO_LIQUIDITY, 2.0, AlertSeverity.HIGH),
    _compile_rule(r"liquidity stimulus", MacroPillar.MACRO_LIQUIDITY, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"debt relief", MacroPillar.MACRO_LIQUIDITY, 1.0, AlertSeverity.LOW),
    _compile_rule(r"monetary easing", MacroPillar.MACRO_LIQUIDITY, 1.5, AlertSeverity.MEDIUM),
    _compile_rule(r"dovish pivot", MacroPillar.MACRO_LIQUIDITY, 1.5, AlertSeverity.MEDIUM),
]


class SentimentAnalyzer:
    """Evaluates lexical polarity, classifies articles into pillars, and tags black swans."""

    def __init__(self, rules: list[Rule] | None = None) -> None:
        self.rules = rules if rules is not None else RULES

    def analyze_text(self, text: str) -> tuple[MacroPillar, AlertSeverity, float, list[str]]:
        """Analyze text and return (pillar, severity, polarity, matched_tokens).

        Polarity is bounded strictly within [-1.0, +1.0].
        """
        if not text or not text.strip():
            return MacroPillar.GENERAL, AlertSeverity.LOW, 0.0, []

        pillar_scores: dict[MacroPillar, float] = {
            MacroPillar.REGULATORY: 0.0,
            MacroPillar.SECURITY_EXPLOIT: 0.0,
            MacroPillar.INSTITUTIONAL: 0.0,
            MacroPillar.MACRO_LIQUIDITY: 0.0,
        }
        pillar_counts: dict[MacroPillar, int] = {
            MacroPillar.REGULATORY: 0,
            MacroPillar.SECURITY_EXPLOIT: 0,
            MacroPillar.INSTITUTIONAL: 0,
            MacroPillar.MACRO_LIQUIDITY: 0,
        }

        matched_tokens: list[str] = []
        highest_severity = AlertSeverity.LOW
        total_raw_score = 0.0

        for rule in self.rules:
            if rule.pattern.search(text):
                matched_tokens.append(rule.token_label)
                pillar_scores[rule.pillar] += rule.weight
                pillar_counts[rule.pillar] += 1
                total_raw_score += rule.weight

                # Severity ordering: CRITICAL > HIGH > MEDIUM > LOW
                if rule.severity == AlertSeverity.CRITICAL:
                    highest_severity = AlertSeverity.CRITICAL
                elif (
                    rule.severity == AlertSeverity.HIGH
                    and highest_severity != AlertSeverity.CRITICAL
                ):
                    highest_severity = AlertSeverity.HIGH
                elif rule.severity == AlertSeverity.MEDIUM and highest_severity not in (
                    AlertSeverity.CRITICAL,
                    AlertSeverity.HIGH,
                ):
                    highest_severity = AlertSeverity.MEDIUM

        if not matched_tokens:
            return MacroPillar.GENERAL, AlertSeverity.LOW, 0.0, []

        # Determine dominant pillar: highest absolute impact or match count
        dominant_pillar = max(
            pillar_counts.keys(),
            key=lambda p: (pillar_counts[p], abs(pillar_scores[p])),
        )
        if pillar_counts[dominant_pillar] == 0:
            dominant_pillar = MacroPillar.GENERAL

        # Saturation clamp to [-1.0, 1.0] using hyperbolic tangent
        polarity = math.tanh(total_raw_score / 2.0)
        polarity_clamped = max(-1.0, min(1.0, round(polarity, 4)))

        return dominant_pillar, highest_severity, polarity_clamped, matched_tokens

    def classify_article(self, article: MacroArticle) -> MacroArticle:
        """Classify and populate pillar, severity, polarity, and matched_keywords for an article."""
        combined_text = f"{article.title} {article.summary}"
        pillar, severity, polarity, keywords = self.analyze_text(combined_text)

        return MacroArticle(
            article_id=article.article_id,
            source=article.source,
            title=article.title,
            url=article.url,
            published_utc=article.published_utc,
            summary=article.summary,
            pillar=pillar,
            severity=severity,
            polarity=polarity,
            matched_keywords=keywords,
            ingested_at_utc=article.ingested_at_utc,
        )

    def compute_narrative_score(
        self,
        articles: Sequence[MacroArticle],
        now_utc: datetime | None = None,
    ) -> tuple[float, bool, int, MacroPillar]:
        """Compute aggregated Tier 3 Narrative Score, black swan flag, and dominant pillar.

        Returns:
            (narrative_score, black_swan_flag, active_critical_alerts, dominant_pillar)
        """
        if not articles:
            return 0.0, False, 0, MacroPillar.GENERAL

        current_time = now_utc or datetime.now(UTC)
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=UTC)

        total_score = 0.0
        black_swan_flag = False
        critical_alerts_count = 0
        pillar_counts: dict[MacroPillar, int] = dict.fromkeys(MacroPillar, 0)

        for a in articles:
            # Check articles published in the last 24 hours
            age_hours = (current_time - a.published_utc).total_seconds() / 3600.0
            if age_hours > 24.0 or age_hours < -1.0:
                continue

            total_score += a.polarity
            pillar_counts[a.pillar] += 1

            if a.severity == AlertSeverity.CRITICAL and a.polarity < 0.0:
                black_swan_flag = True
                critical_alerts_count += 1

        # S_narrative = tanh(sum(Score_j) / 10)
        narrative_score = math.tanh(total_score / 10.0)
        narrative_score = max(-1.0, min(1.0, round(narrative_score, 4)))

        dominant_pillar = max(pillar_counts.keys(), key=lambda p: pillar_counts[p])
        if pillar_counts[dominant_pillar] == 0:
            dominant_pillar = MacroPillar.GENERAL

        return narrative_score, black_swan_flag, critical_alerts_count, dominant_pillar
