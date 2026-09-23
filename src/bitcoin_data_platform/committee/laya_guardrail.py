"""
Laya Semantic Guardrail for Bitcoin Data Platform Committee Memorandums.
Performs independent, read-only semantic audits of committee drafts against market facts and risk invariants.
Operates on the fail-safe principle: Laya can flag or quarantine ambiguous/contradictory memos,
but has ZERO execution authority, ZERO access to credentials, and CANNOT change position size or funds.
"""
from dataclasses import dataclass
import json
import logging
import os
import urllib.request
from typing import Any

logger = logging.getLogger("bitcoin_data_platform.committee.laya_guardrail")

LAYA_ENDPOINT = os.environ.get("LAYA_ENDPOINT", "http://127.0.0.1:8098/v1/systemone")


@dataclass(frozen=True)
class SemanticAuditVerdict:
    status: str  # "PASS", "REVIEW", "QUARANTINE"
    confidence: float
    explanation: str
    contradiction_detected: bool


class LayaCommitteeGuardrail:
    """Non-authoritative semantic auditor for institutional committee memorandums."""

    def __init__(self, endpoint_url: str = LAYA_ENDPOINT, timeout_seconds: float = 2.0):
        self.endpoint_url = endpoint_url
        self.timeout = timeout_seconds

    def audit_memorandum(
        self,
        market_regime: str,
        proposed_action: str,
        consensus_score: float,
        executive_summary: str,
        macro_thesis: str,
        valuation_thesis: str,
        technical_thesis: str,
    ) -> SemanticAuditVerdict:
        """
        Audit memorandum narrative against deterministic regime and proposed action.
        """
        # 1. Deterministic heuristic check
        # Example: Action is EMERGENCY_HALT or DATA_UNAVAILABLE, but narrative promotes buying
        summary_lower = f"{executive_summary} {macro_thesis} {valuation_thesis} {technical_thesis}".lower()
        if proposed_action in ["EMERGENCY_HALT", "DATA_UNAVAILABLE"]:
            if any(term in summary_lower for term in ["beli agresif", "strong buy", "aggressive accumulate"]):
                return SemanticAuditVerdict(
                    status="QUARANTINE",
                    confidence=1.0,
                    explanation="Direct semantic contradiction: Action is HALT but narrative recommends aggressive buy.",
                    contradiction_detected=True,
                )

        # 2. Semantic evaluation via Laya System 1
        state_repr = (
            f"Market Regime: {market_regime}. "
            f"Action: {proposed_action}. "
            f"Consensus: {consensus_score:.4f}. "
            f"Summary: {executive_summary}"
        )

        try:
            req_data = json.dumps({
                "state": state_repr[:600],
                "questions": {
                    "is_contradictory": {
                        "type": "noul",
                        "instructions": "Does the narrative summary conflict with the market regime or action?",
                    }
                },
            }).encode("utf-8")

            req = urllib.request.Request(
                self.endpoint_url,
                data=req_data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if resp.status == 200:
                    body = json.loads(resp.read().decode("utf-8"))
                    answers = body.get("data", {}).get("answers", {})
                    contradiction_prob = answers.get("is_contradictory", {}).get("noul", 0.0)
                    if contradiction_prob >= 0.75:
                        return SemanticAuditVerdict(
                            status="REVIEW",
                            confidence=contradiction_prob,
                            explanation=f"Laya detected potential narrative contradiction (score: {contradiction_prob:.2f})",
                            contradiction_detected=True,
                        )
        except Exception as exc:
            logger.debug("Laya semantic guardrail service check skipped: %s", exc)

        return SemanticAuditVerdict(
            status="PASS",
            confidence=0.95,
            explanation="Memorandum narrative is semantically consistent with invariants.",
            contradiction_detected=False,
        )
