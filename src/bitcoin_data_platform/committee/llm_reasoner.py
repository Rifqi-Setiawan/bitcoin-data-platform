"""Neural reasoning engine for Bitcoin Investment Committee using 9Router / Codex models."""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

from bitcoin_data_platform.committee.models import (
    CommitteePersona,
    MemberStance,
    PersonaVote,
)

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "cx/gpt-5.6-sol"
DEFAULT_BASE_URL = "http://127.0.0.1:20128/v1"


class CommitteeLLMReasoner:
    """Invokes frontier reasoning models (cx/gpt-5.6-sol) via 9Router for committee deliberation."""

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 30.0,
        enabled: bool = True,
    ) -> None:
        self.model = model or os.getenv("BDP_COMMITTEE_MODEL", DEFAULT_MODEL)
        base = base_url or os.getenv("BDP_9ROUTER_BASE_URL") or DEFAULT_BASE_URL
        self.base_url = base.rstrip("/")
        self.api_key = api_key or self._resolve_api_key()
        self.timeout = timeout
        self.enabled = enabled

    @staticmethod
    def _resolve_api_key() -> str:
        """Resolve 9Router API key from environment or known profile env files."""
        key = os.getenv("HERMES_9ROUTER_API_KEY") or os.getenv("NINEROUTER_API_KEY") or ""
        if key:
            return key

        search_paths = [
            "/srv/apps/hermes/profiles/vps-boss/.env",
            "/srv/apps/hermes/.env",
            "/srv/projects/bitcoin-data-platform/.env",
        ]
        for path in search_paths:
            if os.path.exists(path):
                try:
                    with open(path, encoding="utf-8") as f:
                        for line in f:
                            sline = line.strip()
                            if "9ROUTER" in sline and "=" in sline and not sline.startswith("#"):
                                key = sline.split("=", 1)[1].strip("\"'")
                                if key:
                                    return key
                except Exception:
                    continue
        return ""

    def evaluate_committee(
        self,
        snapshot: dict[str, Any],
        user_alpha: list[dict[str, Any]],
        portfolio_state: dict[str, Any],
        base_budget: float = 10.0,
        memo_id: str = "",
    ) -> list[PersonaVote] | None:
        """Deliberate with 3 personas using cx/gpt-5.6-sol and return validated votes."""
        if not self.enabled or not self.api_key:
            return None

        prompt = self._build_deliberation_prompt(
            snapshot=snapshot,
            user_alpha=user_alpha,
            portfolio_state=portfolio_state,
            base_budget=base_budget,
        )

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an elite quantitative Bitcoin Investment Committee. "
                        "You must deliberate across 3 distinct personas: Macro Strategist, "
                        "Valuation Analyst, and Risk Officer. Respond ONLY with valid JSON."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
            "stream": False,
        }

        try:
            req = urllib.request.Request(
                f"{self.base_url}/chat/completions",
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                    "User-Agent": "bitcoin-data-platform/0.1.0",
                },
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            content = data["choices"][0]["message"]["content"].strip()
            # Clean potential markdown fences
            if content.startswith("```"):
                content = content.split("\n", 1)[1]
            if content.endswith("```"):
                content = content.rsplit("\n", 1)[0]
            if content.startswith("json"):
                content = content[4:].strip()

            parsed = json.loads(content)
            return self._parse_votes(parsed, memo_id, base_budget)
        except Exception as exc:
            logger.warning(
                "Committee LLM reasoning with %s failed: %s. Falling back to heuristic.",
                self.model,
                exc,
            )
            return None

    def _build_deliberation_prompt(
        self,
        snapshot: dict[str, Any],
        user_alpha: list[dict[str, Any]],
        portfolio_state: dict[str, Any],
        base_budget: float,
    ) -> str:
        spot = snapshot.get("market_close_usd", 0.0)
        mni = snapshot.get("composite_mni", 0.0)
        regime = snapshot.get("macro_regime", "NEUTRAL_CHOP")
        mvrv = snapshot.get("mvrv_ratio", 1.5)
        mm = snapshot.get("mayer_multiple", 1.0)
        fng = snapshot.get("fng_value", 50)
        black_swan = snapshot.get("black_swan_flag", False)

        base_cash = portfolio_state.get("base_cash", 700.0)
        reserve_cash = portfolio_state.get("reserve_cash", 300.0)
        drawdown = portfolio_state.get("drawdown_pct", 0.0)

        user_notes = ""
        if user_alpha:
            user_notes = "\n".join(
                f"- [{a.get('pillar', 'GENERAL')}] {a.get('title', '')}: {a.get('body', '')}"
                for a in user_alpha[:3]
            )
        else:
            user_notes = "None submitted."

        return f"""
Analyze today's market conditions and deliberate for the Bitcoin Investment Committee.

## Market & Quantitative Snapshot:
- Spot Price: ${spot:,.2f} USD
- Macro-Narrative Index (MNI): {mni:+.2f} (Regime: {regime})
- Black Swan Sentinel: {"ALERT ACTIVE" if black_swan else "NORMAL / CLEAR"}
- On-chain MVRV Ratio: {mvrv} (Historical Benchmark: <1.0 Value, >2.4 Heated)
- Mayer Multiple (Price / SMA-200): {mm} (Benchmark: <0.8 Undervalued, >2.4 Bullish Peak)
- Fear & Greed Index: {fng}/100

## Portfolio Constraints:
- Baseline Daily DCA Budget: ${base_budget:,.2f} USD
- Liquid Base Cash: ${base_cash:,.2f} USD
- Tactical Reserve Cash: ${reserve_cash:,.2f} USD
- Current Drawdown from High: {drawdown:.1%}

## User Submitted Research / Intelligence:
{user_notes}

## Required Task:
Provide the evaluation from 3 personas:
1. Macro Strategist: Evaluates liquidity, geopolitical news, rates, and user intelligence.
2. Valuation Analyst: Evaluates on-chain cycle valuation (MVRV, Mayer Multiple, Fear & Greed).
3. Risk Officer: Evaluates drawdown, volatility, and capital preservation.

Valid stances: "BULLISH", "MODERATELY_BULLISH", "NEUTRAL", "DEFENSIVE", "CRISIS".
Target allocation: float between 0.0 and 15.0 USD (baseline is ${base_budget:.2f}).
Confidence: float between 0.0 and 1.0.
Rationale: 1-2 sentences in professional Indonesian explaining their thesis.

Return ONLY a JSON object matching this EXACT structure:
{{
  "macro_strategist": {{
    "stance": "BULLISH" | "MODERATELY_BULLISH" | "NEUTRAL" | "DEFENSIVE" | "CRISIS",
    "target_allocation_usd": <float>,
    "confidence": <float>,
    "rationale": "<string in Indonesian>"
  }},
  "valuation_analyst": {{
    "stance": "BULLISH" | "MODERATELY_BULLISH" | "NEUTRAL" | "DEFENSIVE" | "CRISIS",
    "target_allocation_usd": <float>,
    "confidence": <float>,
    "rationale": "<string in Indonesian>"
  }},
  "risk_officer": {{
    "stance": "BULLISH" | "MODERATELY_BULLISH" | "NEUTRAL" | "DEFENSIVE" | "CRISIS",
    "target_allocation_usd": <float>,
    "confidence": <float>,
    "rationale": "<string in Indonesian>"
  }}
}}
"""

    def _parse_votes(
        self, data: dict[str, Any], memo_id: str, base_budget: float
    ) -> list[PersonaVote]:
        now = datetime.now(UTC)
        votes: list[PersonaVote] = []

        mapping = [
            ("macro_strategist", CommitteePersona.MACRO_STRATEGIST),
            ("valuation_analyst", CommitteePersona.VALUATION_ANALYST),
            ("risk_officer", CommitteePersona.RISK_OFFICER),
        ]

        for key, persona in mapping:
            item = data.get(key, {})
            raw_stance = str(item.get("stance", "NEUTRAL")).upper().strip()
            try:
                stance = MemberStance(raw_stance)
            except ValueError:
                stance = MemberStance.NEUTRAL

            target_usd = float(item.get("target_allocation_usd", base_budget))
            target_usd = max(0.0, min(15.0, target_usd))
            confidence = float(item.get("confidence", 0.8))
            confidence = max(0.1, min(1.0, confidence))
            rationale = str(item.get("rationale", "Evaluasi berdasarkan analisis pasar terkini."))

            votes.append(
                PersonaVote(
                    vote_id=f"v_{os.urandom(4).hex()}",
                    memo_id=memo_id,
                    persona=persona,
                    stance=stance,
                    target_allocation_usd=round(target_usd, 2),
                    confidence=round(confidence, 2),
                    rationale=rationale,
                    voted_at_utc=now,
                )
            )

        return votes
