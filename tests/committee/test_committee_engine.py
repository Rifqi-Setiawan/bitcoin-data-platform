"""Automated tests for Committee Personas, Deliberation Engine, and Consensus (Group B)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from bitcoin_data_platform.committee.engine import InvestmentCommitteeEngine
from bitcoin_data_platform.committee.models import (
    CommitteePersona,
    MemberStance,
    PersonaVote,
)
from bitcoin_data_platform.committee.personas import (
    MacroStrategistPersona,
    RiskOfficerPersona,
    ValuationAnalystPersona,
)
from bitcoin_data_platform.committee.prompt_templates import extract_dissenting_opinions
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def tmp_duckdb(tmp_path: Path) -> DuckDBManager:
    db_file = tmp_path / "test_committee.duckdb"
    mgr = DuckDBManager(db_file)
    with mgr:
        mgr.initialize()
    return mgr


# 9. test_macro_strategist_persona_evaluation
def test_macro_strategist_persona_evaluation() -> None:
    # Dovish / Expansion regime (high MNI)
    dovish_snapshot = {"composite_mni": 0.65}
    vote_bull = MacroStrategistPersona.evaluate(dovish_snapshot, base_budget=10.0)
    assert vote_bull.persona == CommitteePersona.MACRO_STRATEGIST
    assert vote_bull.stance == MemberStance.BULLISH
    assert vote_bull.target_allocation_usd > 10.0

    # Hawkish / Crisis regime (negative MNI)
    hawkish_snapshot = {"composite_mni": -0.75}
    vote_crisis = MacroStrategistPersona.evaluate(hawkish_snapshot, base_budget=10.0)
    assert vote_crisis.stance == MemberStance.CRISIS
    assert vote_crisis.target_allocation_usd == 0.0


# 10. test_valuation_analyst_persona_mvrv
def test_valuation_analyst_persona_mvrv() -> None:
    # Deep accumulation / undervaluation (MVRV < 1.0, MM < 0.8)
    cheap_snapshot = {"mvrv_ratio": 0.85, "mayer_multiple": 0.70}
    cheap_vote = ValuationAnalystPersona.evaluate(cheap_snapshot, base_budget=10.0)
    assert cheap_vote.stance == MemberStance.BULLISH
    assert cheap_vote.target_allocation_usd == 15.0

    # Cycle peak / bubble (MVRV > 3.0, MM > 2.5)
    peak_snapshot = {"mvrv_ratio": 3.40, "mayer_multiple": 2.80}
    peak_vote = ValuationAnalystPersona.evaluate(peak_snapshot, base_budget=10.0)
    assert peak_vote.stance == MemberStance.CRISIS
    assert peak_vote.target_allocation_usd == 0.0


# 11. test_risk_officer_persona_drawdown_response
def test_risk_officer_persona_drawdown_response() -> None:
    # High drawdown > 25%
    high_dd_portfolio = {"drawdown_pct": 0.32}
    snap = {"market_close_usd": 60000.0, "sma_200": 62000.0, "black_swan_flag": False}
    dd_vote = RiskOfficerPersona.evaluate(snap, portfolio_state=high_dd_portfolio, base_budget=10.0)
    assert dd_vote.stance == MemberStance.DEFENSIVE
    assert dd_vote.target_allocation_usd <= 5.0
    assert "Drawdown portofolio" in dd_vote.rationale

    # Black swan alert
    black_swan_snap = {"market_close_usd": 50000.0, "sma_200": 60000.0, "black_swan_flag": True}
    bs_vote = RiskOfficerPersona.evaluate(black_swan_snap, base_budget=10.0)
    assert bs_vote.stance == MemberStance.CRISIS
    assert bs_vote.target_allocation_usd == 0.0


# 12. test_committee_consensus_synthesis_math
def test_committee_consensus_synthesis_math(tmp_duckdb: DuckDBManager) -> None:
    engine = InvestmentCommitteeEngine(tmp_duckdb)
    now = datetime.now(UTC)
    ms = CommitteePersona.MACRO_STRATEGIST
    va = CommitteePersona.VALUATION_ANALYST
    ro = CommitteePersona.RISK_OFFICER
    bull = MemberStance.BULLISH

    # Bullish scenario across all 3
    votes_bull = [
        PersonaVote("1", "m", ms, bull, 15.0, 1.0, "ok", now),
        PersonaVote("2", "m", va, bull, 15.0, 1.0, "ok", now),
        PersonaVote("3", "m", ro, bull, 15.0, 1.0, "ok", now),
    ]
    score_bull = engine._calculate_consensus(votes_bull)
    assert score_bull == 1.0

    # Split: Macro bull (+1.0, w=0.35), Val neutral (0.0, w=0.35), Risk def (-0.5, w=0.30)
    # Total = 0.35*1.0 + 0.35*0.0 + 0.30*(-0.5) = 0.35 - 0.15 = 0.20
    votes_split = [
        PersonaVote("1", "m", ms, bull, 15.0, 1.0, "ok", now),
        PersonaVote("2", "m", va, MemberStance.NEUTRAL, 10.0, 1.0, "ok", now),
        PersonaVote("3", "m", ro, MemberStance.DEFENSIVE, 5.0, 1.0, "ok", now),
    ]
    score_split = engine._calculate_consensus(votes_split)
    assert abs(score_split - 0.20) < 1e-4


# 13. test_bahasa_indonesia_memo_generation
def test_bahasa_indonesia_memo_generation(tmp_duckdb: DuckDBManager) -> None:
    engine = InvestmentCommitteeEngine(tmp_duckdb)
    memo = engine.deliberate(target_date=date(2026, 9, 19), dry_run=True)

    assert "Komite Investasi merekomendasikan" in memo.executive_summary_id
    assert "RiskGuard" in memo.executive_summary_id
    assert memo.macro_thesis is not None
    assert memo.valuation_thesis is not None
    assert memo.technical_thesis is not None
    assert "Ringkasan Eksekutif" in memo.memo_markdown
    assert "Verifikasi Neuro-Simbolik" in memo.memo_markdown


# 14. test_dissenting_opinions_extraction
def test_dissenting_opinions_extraction() -> None:
    now = datetime.now(UTC)
    ms = CommitteePersona.MACRO_STRATEGIST
    va = CommitteePersona.VALUATION_ANALYST
    ro = CommitteePersona.RISK_OFFICER
    bull = MemberStance.BULLISH

    # Case 1: Unanimous
    unanimous = [
        PersonaVote("1", "m", ms, bull, 15.0, 1.0, "ok", now),
        PersonaVote("2", "m", va, bull, 15.0, 1.0, "ok", now),
    ]
    dissent_none = extract_dissenting_opinions(unanimous)
    assert "sepakat" in dissent_none

    # Case 2: Risk officer warns while others are bullish
    disagree = [
        PersonaVote("1", "m", ms, bull, 15.0, 1.0, "Likuiditas melimpah", now),
        PersonaVote("2", "m", va, bull, 15.0, 1.0, "Valuasi murah", now),
        PersonaVote("3", "m", ro, MemberStance.DEFENSIVE, 5.0, 0.9, "Drawdown tinggi", now),
    ]
    dissent_text = extract_dissenting_opinions(disagree)
    assert "Risk Officer" in dissent_text
    assert "DEFENSIVE" in dissent_text


# 15. test_mock_llm_provider_offline_reproducibility
def test_mock_llm_provider_offline_reproducibility(tmp_duckdb: DuckDBManager) -> None:
    engine1 = InvestmentCommitteeEngine(tmp_duckdb, llm_client=None)
    engine2 = InvestmentCommitteeEngine(tmp_duckdb, llm_client=None)

    target = date(2026, 9, 19)
    port = {"base_cash": 700.0, "reserve_cash": 300.0, "drawdown_pct": 0.05}

    memo1 = engine1.deliberate(target_date=target, portfolio_state=port, dry_run=True)
    memo2 = engine2.deliberate(target_date=target, portfolio_state=port, dry_run=True)

    assert memo1.consensus_score == memo2.consensus_score
    assert memo1.proposed_action == memo2.proposed_action
    assert memo1.clamped_allocation_usd == memo2.clamped_allocation_usd


# 16. test_committee_memo_duckdb_persistence
def test_committee_memo_duckdb_persistence(tmp_duckdb: DuckDBManager) -> None:
    engine = InvestmentCommitteeEngine(tmp_duckdb)
    target = date(2026, 9, 19)
    memo = engine.deliberate(target_date=target, dry_run=False)

    with tmp_duckdb:
        stored_memo = tmp_duckdb.get_investment_memo_by_date(target)
        assert stored_memo is not None
        assert stored_memo["memo_id"] == memo.memo_id
        assert stored_memo["proposed_action"] == memo.proposed_action.value
        assert len(stored_memo["votes"]) == 3

        history = tmp_duckdb.get_investment_memos_history(limit=5)
        assert len(history) == 1
        assert history[0]["memo_id"] == memo.memo_id
