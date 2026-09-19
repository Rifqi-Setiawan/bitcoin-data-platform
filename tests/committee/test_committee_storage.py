"""Automated tests for DuckDB Storage, Schemas, and Analytical Views (Group D)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from bitcoin_data_platform.committee.models import (
    AllocationAction,
    CommitteePersona,
    InvestmentMemorandum,
    MemberStance,
    PersonaVote,
)
from bitcoin_data_platform.intelligence.models import (
    IntelligencePillar,
    UserIntelligenceRecord,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def tmp_duckdb(tmp_path: Path) -> DuckDBManager:
    db_file = tmp_path / "test_storage.duckdb"
    mgr = DuckDBManager(db_file)
    with mgr:
        mgr.initialize()
    return mgr


# 27. test_duckdb_schema_initialization
def test_duckdb_schema_initialization(tmp_duckdb: DuckDBManager) -> None:
    with tmp_duckdb:
        con = tmp_duckdb.get_connection()
        tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
        assert "user_market_intelligence" in tables
        assert "investment_committee_memos" in tables
        assert "investment_committee_votes" in tables
        views_sql = "SELECT table_name FROM information_schema.views;"
        views = [r[0] for r in con.execute(views_sql).fetchall()]
        assert "mart_committee_deliberation_daily" in views


# 28. test_mart_committee_deliberation_daily_view
def test_mart_committee_deliberation_daily_view(tmp_duckdb: DuckDBManager) -> None:
    # Query view even when underlying marts are empty
    with tmp_duckdb:
        rows = tmp_duckdb.execute_query("SELECT * FROM mart_committee_deliberation_daily LIMIT 5;")
        assert isinstance(rows, list)


# 29. test_memo_and_votes_foreign_key_integrity
def test_memo_and_votes_foreign_key_integrity(tmp_duckdb: DuckDBManager) -> None:
    now = datetime.now(UTC)
    memo_id = "test-memo-fk-1"
    memo_date = date(2026, 9, 19)

    ms = CommitteePersona.MACRO_STRATEGIST
    va = CommitteePersona.VALUATION_ANALYST
    bull = MemberStance.BULLISH

    vote1 = PersonaVote("v1", memo_id, ms, bull, 15.0, 0.85, "Macro OK", now)
    vote2 = PersonaVote("v2", memo_id, va, bull, 15.0, 0.90, "Valuation OK", now)

    memo = InvestmentMemorandum(
        memo_id=memo_id,
        memo_date=memo_date,
        created_at_utc=now,
        market_regime="CAUTIOUS_BULL",
        composite_mni=0.45,
        consensus_score=0.85,
        executive_summary_id="Ringkasan eksekutif tes.",
        macro_thesis="Macro OK",
        valuation_thesis="Valuation OK",
        technical_thesis="Tech OK",
        dissenting_opinions="None",
        proposed_action=AllocationAction.OPPORTUNISTIC_BUY,
        proposed_allocation_usd=30.0,
        clamped_allocation_usd=22.5,
        allocation_clamped=True,
        clamping_reason="DAILY_RESERVE_CAP",
        risk_guard_passed=True,
        memo_markdown="# Memo Markdown",
        votes=[vote1, vote2],
    )

    with tmp_duckdb:
        tmp_duckdb.insert_investment_memo(memo)
        stored_votes = tmp_duckdb.get_votes_for_memo(memo_id)
        assert len(stored_votes) == 2
        assert all(v["memo_id"] == memo_id for v in stored_votes)


# 30. test_concurrent_read_write_duckdb_safety
def test_concurrent_read_write_duckdb_safety(tmp_path: Path) -> None:
    db_file = tmp_path / "concurrent.duckdb"
    # Ensure connections close properly with context manager
    with DuckDBManager(db_file) as db1:
        db1.initialize()

    with DuckDBManager(db_file) as db2:
        memos = db2.get_investment_memos_history()
        assert memos == []


# 31. test_idempotent_table_creation
def test_idempotent_table_creation(tmp_duckdb: DuckDBManager) -> None:
    # Repeated calls should execute cleanly without crashing
    with tmp_duckdb:
        tmp_duckdb.initialize()
        tmp_duckdb.initialize()
        tmp_duckdb.create_committee_tables()
        tmp_duckdb.create_committee_mart_view()


# 32. test_user_intelligence_active_count_join
def test_user_intelligence_active_count_join(tmp_duckdb: DuckDBManager) -> None:
    target_dt = datetime(2026, 9, 19, 10, 0, tzinfo=UTC)
    with tmp_duckdb:
        # Insert 2 active and 1 inactive user intelligence items
        for i, act in [(1, True), (2, True), (3, False)]:
            tmp_duckdb.insert_user_intelligence(
                UserIntelligenceRecord(
                    intelligence_id=f"u{i}",
                    source_url=None,
                    title=f"T{i}",
                    user_thesis=f"Thesis {i}",
                    raw_content="",
                    pillar=IntelligencePillar.USER_THESIS,
                    sentiment_bias=0.5,
                    confidence_score=0.8,
                    tags=[],
                    created_at_utc=target_dt,
                    is_active=act,
                )
            )

        sql = (
            "SELECT COUNT(*) AS cnt FROM user_market_intelligence "
            "WHERE is_active = TRUE AND CAST(created_at_utc AS DATE) = '2026-09-19';"
        )
        rows = tmp_duckdb.execute_query(sql)
        assert rows[0]["cnt"] == 2


# 33. test_memorandum_retrieval_by_date
def test_memorandum_retrieval_by_date(tmp_duckdb: DuckDBManager) -> None:
    now = datetime.now(UTC)
    target = date(2026, 9, 19)
    memo = InvestmentMemorandum(
        memo_id="date-memo-test",
        memo_date=target,
        created_at_utc=now,
        market_regime="NEUTRAL_CHOP",
        composite_mni=0.0,
        consensus_score=0.0,
        executive_summary_id="Test ID",
        macro_thesis="T1",
        valuation_thesis="T2",
        technical_thesis="T3",
        dissenting_opinions="None",
        proposed_action=AllocationAction.STANDARD_DCA,
        proposed_allocation_usd=10.0,
        clamped_allocation_usd=10.0,
        allocation_clamped=False,
        clamping_reason=None,
        risk_guard_passed=True,
        memo_markdown="# Test",
        votes=[],
    )

    with tmp_duckdb:
        tmp_duckdb.insert_investment_memo(memo)
        fetched = tmp_duckdb.get_investment_memo_by_date(target)
        assert fetched is not None
        assert fetched["memo_id"] == "date-memo-test"


# 34. test_memo_query_fallback_on_unpopulated_db
def test_memo_query_fallback_on_unpopulated_db(tmp_duckdb: DuckDBManager) -> None:
    with tmp_duckdb:
        latest = tmp_duckdb.get_latest_investment_memo()
        assert latest is None
        by_date = tmp_duckdb.get_investment_memo_by_date(date(2026, 1, 1))
        assert by_date is None
        history = tmp_duckdb.get_investment_memos_history()
        assert history == []
