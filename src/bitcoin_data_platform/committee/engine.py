"""Investment Committee Deliberation Engine.

Coordinates personas, weighted consensus, and symbolic invariant clamping.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from bitcoin_data_platform.committee.invariant_solver import InvariantSolver
from bitcoin_data_platform.committee.llm_reasoner import CommitteeLLMReasoner
from bitcoin_data_platform.committee.models import (
    AllocationAction,
    ClampingReceipt,
    CommitteePersona,
    InvestmentMemorandum,
    MemberStance,
    PersonaVote,
)
from bitcoin_data_platform.committee.personas import (
    MacroStrategistPersona,
    RiskOfficerPersona,
    ValuationAnalystPersona,
)
from bitcoin_data_platform.committee.prompt_templates import (
    extract_dissenting_opinions,
    generate_executive_summary_id,
    render_memorandum_markdown,
)
from bitcoin_data_platform.exceptions import MarketDataUnavailableError
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


class InvestmentCommitteeEngine:
    """Deliberation coordinator synthesizing multi-persona votes and invariant verification."""

    PERSONA_WEIGHTS = {
        "MACRO_STRATEGIST": 0.35,
        "VALUATION_ANALYST": 0.35,
        "RISK_OFFICER": 0.30,
    }

    STANCE_SCORES = {
        MemberStance.BULLISH: 1.0,
        MemberStance.MODERATELY_BULLISH: 0.5,
        MemberStance.NEUTRAL: 0.0,
        MemberStance.DEFENSIVE: -0.5,
        MemberStance.CRISIS: -1.0,
    }

    def __init__(
        self,
        db_manager: DuckDBManager,
        invariant_solver: InvariantSolver | None = None,
        llm_client: Any | None = None,
        use_llm: bool | None = None,
        allow_unpopulated: bool = False,
    ) -> None:
        self.db = db_manager
        self.solver = invariant_solver or InvariantSolver()
        self.allow_unpopulated = allow_unpopulated
        is_testing = bool(os.getenv("PYTEST_CURRENT_TEST"))
        env_val = os.getenv("BDP_USE_LLM", "true").lower()
        effective_use_llm = (
            use_llm if use_llm is not None else (not is_testing and env_val in ("true", "1", "yes"))
        )
        if llm_client is not None:
            self.llm = llm_client
        elif effective_use_llm:
            try:
                self.llm = CommitteeLLMReasoner()
            except Exception as exc:
                logger = logging.getLogger(__name__)
                logger.warning("Failed initializing CommitteeLLMReasoner: %s", exc)
                self.llm = None
        else:
            self.llm = None

    def deliberate(
        self,
        target_date: date,
        portfolio_state: dict[str, Any] | None = None,
        dry_run: bool = False,
        base_budget: float = 10.0,
        allow_unpopulated: bool | None = None,
        fail_closed_action: bool = False,
    ) -> InvestmentMemorandum:
        """Run daily investment committee deliberation session."""
        memo_id = str(uuid.uuid4())

        # 1. Fetch conformed snapshot & active user alpha
        effective_allow_unpopulated = (
            allow_unpopulated if allow_unpopulated is not None else self.allow_unpopulated
        )
        snapshot = self._load_daily_snapshot(target_date)
        if snapshot is None:
            if not effective_allow_unpopulated:
                if fail_closed_action:
                    memo = self._build_data_unavailable_memorandum(target_date, memo_id)
                    if not dry_run:
                        self.db.insert_investment_memo(memo)
                    return memo
                raise MarketDataUnavailableError(
                    f"Market data snapshot unavailable in DuckDB for date {target_date}. "
                    "Analytical marts are unpopulated (fail-closed)."
                )
            snapshot = self._fallback_snapshot(target_date)

        user_alpha = self._load_user_intelligence(target_date)
        port_state = portfolio_state or self._load_portfolio_state()

        # 2. Collect Persona Evaluations (Neural Reasoning via cx/gpt-5.6-sol or heuristic fallback)
        votes: list[PersonaVote] | None = None
        if self.llm is not None and hasattr(self.llm, "evaluate_committee"):
            try:
                votes = self.llm.evaluate_committee(
                    snapshot=snapshot,
                    user_alpha=user_alpha,
                    portfolio_state=port_state,
                    base_budget=base_budget,
                    memo_id=memo_id,
                )
            except Exception as llm_err:
                logging.getLogger(__name__).warning(
                    "LLM committee reasoning failed: %s. Falling back to quantitative heuristic.",
                    llm_err,
                )
                votes = None

        if not votes:
            votes = [
                MacroStrategistPersona.evaluate(snapshot, user_alpha, base_budget, memo_id),
                ValuationAnalystPersona.evaluate(snapshot, base_budget, memo_id),
                RiskOfficerPersona.evaluate(snapshot, port_state, base_budget, memo_id),
            ]

        # 3. Calculate Weighted Consensus Score
        consensus_score = self._calculate_consensus(votes)

        # 4. Derive Unconstrained Proposed Action & Allocation USD
        proposed_action, proposed_usd = self._derive_proposal(
            consensus_score, votes, port_state, snapshot, base_budget
        )

        # 5. Neuro-Symbolic Invariant Verification & Clamping
        base_cash = float(port_state.get("base_cash", 700.0))
        reserve_cash = float(port_state.get("reserve_cash", 300.0))
        drawdown_pct = float(port_state.get("drawdown_pct", 0.0))
        spot_price = float(snapshot.get("market_close_usd", 0.0))
        last_price = float(snapshot.get("last_close_usd", spot_price))
        macro_prox = snapshot.get("macro_proximity_minutes")

        receipt: ClampingReceipt = self.solver.solve_and_clamp(
            proposed_usd=proposed_usd,
            available_base_cash=base_cash,
            available_reserve_cash=reserve_cash,
            has_black_swan=bool(snapshot.get("black_swan_flag", False)),
            macro_proximity_minutes=int(macro_prox) if macro_prox is not None else None,
            portfolio_drawdown=drawdown_pct,
            spot_price=spot_price,
            last_validated_price=last_price,
        )

        # 6. Generate Indonesian Summary & Full Markdown Memorandum
        dissent = extract_dissenting_opinions(votes)
        summary_id = generate_executive_summary_id(
            target_date=target_date,
            market_regime=snapshot.get("macro_regime", "NEUTRAL_CHOP"),
            consensus_score=consensus_score,
            proposed_action=proposed_action,
            receipt=receipt,
            votes=votes,
            user_alpha_count=len(user_alpha),
        )
        memo_md = render_memorandum_markdown(
            target_date=target_date,
            market_regime=snapshot.get("macro_regime", "NEUTRAL_CHOP"),
            composite_mni=float(snapshot.get("composite_mni", 0.0)),
            consensus_score=consensus_score,
            proposed_action=proposed_action,
            receipt=receipt,
            votes=votes,
            executive_summary=summary_id,
            dissent=dissent,
            snapshot=snapshot,
        )

        memo = InvestmentMemorandum(
            memo_id=memo_id,
            memo_date=target_date,
            created_at_utc=datetime.now(UTC),
            market_regime=snapshot.get("macro_regime", "NEUTRAL_CHOP"),
            composite_mni=float(snapshot.get("composite_mni", 0.0)),
            consensus_score=round(consensus_score, 4),
            executive_summary_id=summary_id,
            macro_thesis=votes[0].rationale,
            valuation_thesis=votes[1].rationale,
            technical_thesis=votes[2].rationale,
            dissenting_opinions=dissent,
            proposed_action=proposed_action,
            proposed_allocation_usd=receipt.proposed_allocation_usd,
            clamped_allocation_usd=receipt.clamped_allocation_usd,
            allocation_clamped=receipt.is_clamped,
            clamping_reason=receipt.explanation if receipt.is_clamped else None,
            risk_guard_passed=bool(
                receipt.clamped_allocation_usd > 0 or not snapshot.get("black_swan_flag")
            ),
            memo_markdown=memo_md,
            votes=votes,
        )

        if not dry_run:
            self.db.insert_investment_memo(memo)

        return memo

    def _calculate_consensus(self, votes: list[PersonaVote]) -> float:
        """Compute weighted consensus score from persona votes."""
        weighted_sum = 0.0
        weight_total = 0.0

        for v in votes:
            p_name = v.persona.value if hasattr(v.persona, "value") else str(v.persona)
            weight = self.PERSONA_WEIGHTS.get(p_name, 0.33)
            score = self.STANCE_SCORES.get(v.stance, 0.0)
            eff_weight = weight * max(0.1, v.confidence)
            weighted_sum += eff_weight * score
            weight_total += eff_weight

        if weight_total <= 0.0:
            return 0.0

        raw_consensus = weighted_sum / weight_total
        return max(-1.0, min(1.0, raw_consensus))

    def _derive_proposal(
        self,
        consensus_score: float,
        votes: list[PersonaVote],
        portfolio_state: dict[str, Any],
        snapshot: dict[str, Any],
        base_budget: float,
    ) -> tuple[AllocationAction, float]:
        """Derive qualitative action and unconstrained dollar allocation."""
        if snapshot.get("black_swan_flag", False):
            return AllocationAction.EMERGENCY_HALT, 0.0

        if consensus_score >= 0.45:
            action = AllocationAction.AGGRESSIVE_ACCUMULATE
        elif consensus_score >= 0.15:
            action = AllocationAction.OPPORTUNISTIC_BUY
        elif consensus_score >= -0.20:
            action = AllocationAction.STANDARD_DCA
        elif consensus_score >= -0.55:
            action = AllocationAction.DEFENSIVE_HOLD
        else:
            action = AllocationAction.EMERGENCY_HALT

        # Unconstrained proposal blends persona allocations
        total_conf = sum(v.confidence for v in votes)
        if total_conf > 0:
            proposed_usd = sum(v.target_allocation_usd * v.confidence for v in votes) / total_conf
        else:
            proposed_usd = base_budget

        if action == AllocationAction.EMERGENCY_HALT:
            proposed_usd = 0.0
        elif action == AllocationAction.DEFENSIVE_HOLD:
            proposed_usd = min(proposed_usd, base_budget * 0.5)

        return action, round(proposed_usd, 2)

    def _load_daily_snapshot(self, target_date: date) -> dict[str, Any] | None:
        """Load conformed market & macro snapshot for target date from DuckDB."""
        self.db.initialize()
        try:
            sql = """
            SELECT
                m.trade_date_utc,
                m.market_close_usd,
                m.sma_200,
                m.mayer_multiple,
                m.mvrv_ratio,
                m.fng_value,
                COALESCE(m.composite_mni, 0.0) AS composite_mni,
                COALESCE(m.macro_regime, 'NEUTRAL_CHOP') AS macro_regime,
                COALESCE(m.black_swan_flag, FALSE) AS black_swan_flag
            FROM mart_macro_narrative_daily m
            WHERE CAST(m.trade_date_utc AS DATE) = ?
            LIMIT 1;
            """
            rows = self.db.execute_query(sql.replace("?", f"'{target_date.isoformat()}'"))
            if rows:
                res = dict(rows[0])
                res["last_close_usd"] = res.get("market_close_usd", 0.0)
                return res
        except Exception:
            pass

        # Fallback query to mart_btc_investment_signals_daily if view has no rows
        try:
            sql2 = """
            SELECT
                m.trade_date_utc,
                m.market_close_usd,
                m.sma_200,
                m.mayer_multiple,
                m.mvrv_ratio,
                m.fng_value,
                0.0 AS composite_mni,
                'NEUTRAL_CHOP' AS macro_regime,
                FALSE AS black_swan_flag
            FROM mart_btc_investment_signals_daily m
            WHERE CAST(m.trade_date_utc AS DATE) = ?
            LIMIT 1;
            """
            rows2 = self.db.execute_query(sql2.replace("?", f"'{target_date.isoformat()}'"))
            if rows2:
                res2 = dict(rows2[0])
                res2["last_close_usd"] = res2.get("market_close_usd", 0.0)
                return res2
        except Exception:
            pass

        return None

    @staticmethod
    def _fallback_snapshot(target_date: date) -> dict[str, Any]:
        """Synthetic fallback snapshot used ONLY when allow_unpopulated=True is explicitly set."""
        logging.getLogger(__name__).warning(
            "Marts unpopulated: using fallback snapshot for %s (allow_unpopulated=True)",
            target_date.isoformat(),
        )
        return {
            "trade_date_utc": target_date.isoformat(),
            "market_close_usd": 65000.0,
            "last_close_usd": 65000.0,
            "sma_200": 60000.0,
            "mayer_multiple": 1.08,
            "mvrv_ratio": 1.55,
            "fng_value": 52,
            "composite_mni": 0.0,
            "macro_regime": "NEUTRAL_CHOP",
            "black_swan_flag": False,
            "macro_proximity_minutes": None,
        }

    def _build_data_unavailable_memorandum(
        self, target_date: date, memo_id: str
    ) -> InvestmentMemorandum:
        """Create an emergency halt / DATA_UNAVAILABLE memorandum when marts are unpopulated."""
        now = datetime.now(UTC)
        votes = [
            PersonaVote(
                vote_id=uuid.uuid4().hex[:16],
                memo_id=memo_id,
                persona=CommitteePersona.MACRO_STRATEGIST,
                stance=MemberStance.CRISIS,
                target_allocation_usd=0.0,
                confidence=1.0,
                rationale=(
                    "Data pasar analitik tidak tersedia (Marts unpopulated). "
                    "Membekukan alokasi modal baru."
                ),
                voted_at_utc=now,
            ),
            PersonaVote(
                vote_id=uuid.uuid4().hex[:16],
                memo_id=memo_id,
                persona=CommitteePersona.VALUATION_ANALYST,
                stance=MemberStance.CRISIS,
                target_allocation_usd=0.0,
                confidence=1.0,
                rationale="Metrik on-chain dan valuasi tidak dapat dihitung tanpa conformed marts.",
                voted_at_utc=now,
            ),
            PersonaVote(
                vote_id=uuid.uuid4().hex[:16],
                memo_id=memo_id,
                persona=CommitteePersona.RISK_OFFICER,
                stance=MemberStance.CRISIS,
                target_allocation_usd=0.0,
                confidence=1.0,
                rationale=(
                    "RiskGuard Pre-Trade Fail-Closed: Operasi dihentikan karena ketiadaan data."
                ),
                voted_at_utc=now,
            ),
        ]
        return InvestmentMemorandum(
            memo_id=memo_id,
            memo_date=target_date,
            created_at_utc=now,
            market_regime="DATA_UNAVAILABLE",
            composite_mni=0.0,
            consensus_score=-1.0,
            executive_summary_id=(
                "HALT: Data pasar tidak tersedia di analytical marts. "
                "Eksekusi perdagangan dihentikan (fail-closed)."
            ),
            macro_thesis=votes[0].rationale,
            valuation_thesis=votes[1].rationale,
            technical_thesis=votes[2].rationale,
            dissenting_opinions="Persona sepakat membekukan alokasi karena data pasar kosong.",
            proposed_action=AllocationAction.DATA_UNAVAILABLE,
            proposed_allocation_usd=0.0,
            clamped_allocation_usd=0.0,
            allocation_clamped=True,
            clamping_reason="DATA_UNAVAILABLE: Marts unpopulated - fail-closed halt",
            risk_guard_passed=False,
            memo_markdown=(
                "# Institutional Investment Committee Memorandum\n\n"
                "**STATUS: DATA_UNAVAILABLE (FAIL-CLOSED)**\n"
            ),
            votes=votes,
        )

    def _load_user_intelligence(self, target_date: date) -> list[dict[str, Any]]:
        """Load active user intelligence records within 72h window of target date."""
        try:
            start_date = target_date - timedelta(days=3)
            return self.db.get_user_intelligence_window(start_date, target_date)
        except Exception:
            return []

    def _load_portfolio_state(self) -> dict[str, Any]:
        """Query current paper portfolio balance or supply default institutional state."""
        try:
            rows = self.db.execute_query(
                "SELECT base_cash, reserve_cash, btc_balance FROM paper_portfolio_balance LIMIT 1;"
            )
            snap_rows = self.db.execute_query(
                "SELECT portfolio_equity FROM paper_portfolio_snapshots_daily "
                "ORDER BY snapshot_date DESC LIMIT 30;"
            )
            if snap_rows:
                equities = [float(r["portfolio_equity"]) for r in snap_rows]
                peak = max(equities)
                curr = equities[0]
                dd = max(0.0, (peak - curr) / peak) if peak > 0.0 else 0.0
            else:
                dd = 0.0

            if rows:
                r = rows[0]
                return {
                    "base_cash": float(r.get("base_cash", 700.0)),
                    "reserve_cash": float(r.get("reserve_cash", 300.0)),
                    "drawdown_pct": dd,
                }
        except Exception:
            pass

        return {
            "base_cash": 700.0,
            "reserve_cash": 300.0,
            "drawdown_pct": 0.0,
        }
