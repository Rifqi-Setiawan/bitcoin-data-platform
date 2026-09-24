"""Unit tests for PipelineOrchestrator conditional pacing (Group E / Phase 18-5)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bitcoin_data_platform.committee.engine import InvestmentCommitteeEngine
from bitcoin_data_platform.committee.models import (
    AllocationAction,
    InvestmentMemorandum,
)
from bitcoin_data_platform.paper.engine import PaperTradingEngine
from bitcoin_data_platform.paper.models import PaperTradeRecord
from bitcoin_data_platform.pipeline.models import JobStatus, PipelineCadence
from bitcoin_data_platform.pipeline.orchestrator import PipelineOrchestrator
from bitcoin_data_platform.sources.sentiment_contract import SentimentRecord
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def tmp_duckdb(tmp_path: Path) -> DuckDBManager:
    """Create initialized temporary DuckDB instance."""
    db_file = tmp_path / "test_orchestrator_pacing.duckdb"
    mgr = DuckDBManager(db_file)
    with mgr:
        mgr.initialize()
    return mgr


@pytest.fixture
def orchestrator(tmp_path: Path, tmp_duckdb: DuckDBManager) -> PipelineOrchestrator:
    """Create PipelineOrchestrator with isolated paths."""
    return PipelineOrchestrator(
        repo_root=tmp_path,
        db_manager=tmp_duckdb,
        lock_dir=tmp_path / "locks",
    )


# =============================================================================
# 1. Tests for check_cadence_or_trigger logic
# =============================================================================


def test_check_cadence_or_trigger_chop_day(orchestrator: PipelineOrchestrator) -> None:
    """Verify check_cadence_or_trigger returns False on routine chop day."""
    tuesday = date(2026, 9, 22)  # Tuesday
    chop_row = {
        "product_id": "BTC-USD",
        "trade_date_utc": tuesday,
        "is_weekly_cadence_day": False,
        "is_drawdown_event": False,
        "is_regime_capitulation": False,
        "is_regime_froth": False,
        "return_24h": -0.01,
        "drawdown_7d": -0.02,
        "mvrv_ratio": 1.5,
        "mayer_multiple": 1.1,
        "fng_value": 50,
    }

    with patch.object(orchestrator.db, "get_event_triggers", return_value=[chop_row]):
        is_active, info = orchestrator.check_cadence_or_trigger(tuesday)

    assert is_active is False
    assert info["is_weekly_cadence_day"] is False
    assert info["is_drawdown_event"] is False
    assert info["is_regime_capitulation"] is False
    assert info["is_regime_froth"] is False
    assert info["trigger_reasons"] == []


def test_check_cadence_or_trigger_weekly_cadence(orchestrator: PipelineOrchestrator) -> None:
    """Verify check_cadence_or_trigger detects weekly cadence flag."""
    sunday = date(2026, 9, 20)  # Sunday
    weekly_row = {
        "product_id": "BTC-USD",
        "trade_date_utc": sunday,
        "is_weekly_cadence_day": True,
        "is_drawdown_event": False,
        "is_regime_capitulation": False,
        "is_regime_froth": False,
    }

    with patch.object(orchestrator.db, "get_event_triggers", return_value=[weekly_row]):
        is_active, info = orchestrator.check_cadence_or_trigger(sunday)

    assert is_active is True
    assert info["is_weekly_cadence_day"] is True
    assert "WEEKLY_CADENCE" in info["trigger_reasons"]


def test_check_cadence_or_trigger_drawdown_event(orchestrator: PipelineOrchestrator) -> None:
    """Verify check_cadence_or_trigger detects drawdown event."""
    wednesday = date(2026, 9, 23)
    dd_row = {
        "product_id": "BTC-USD",
        "trade_date_utc": wednesday,
        "is_weekly_cadence_day": False,
        "is_drawdown_event": True,
        "is_regime_capitulation": False,
        "is_regime_froth": False,
    }

    with patch.object(orchestrator.db, "get_event_triggers", return_value=[dd_row]):
        is_active, info = orchestrator.check_cadence_or_trigger(wednesday)

    assert is_active is True
    assert info["is_drawdown_event"] is True
    assert "DRAWDOWN_EVENT" in info["trigger_reasons"]


def test_check_cadence_or_trigger_drawdown_raw_threshold(
    orchestrator: PipelineOrchestrator,
) -> None:
    """Verify check_cadence_or_trigger detects drawdown via raw thresholds if flag is False."""
    day = date(2026, 9, 23)
    raw_dd_row = {
        "product_id": "BTC-USD",
        "trade_date_utc": day,
        "is_weekly_cadence_day": False,
        "is_drawdown_event": False,
        "return_24h": -0.06,  # <= -0.05 threshold
        "drawdown_7d": -0.04,
    }

    with patch.object(orchestrator.db, "get_event_triggers", return_value=[raw_dd_row]):
        is_active, info = orchestrator.check_cadence_or_trigger(day)

    assert is_active is True
    assert info["is_drawdown_event"] is True
    assert "DRAWDOWN_EVENT" in info["trigger_reasons"]


def test_check_cadence_or_trigger_capitulation_event(
    orchestrator: PipelineOrchestrator,
) -> None:
    """Verify check_cadence_or_trigger detects capitulation event."""
    thursday = date(2026, 9, 24)
    cap_row = {
        "product_id": "BTC-USD",
        "trade_date_utc": thursday,
        "is_weekly_cadence_day": False,
        "is_drawdown_event": False,
        "is_regime_capitulation": True,
        "is_regime_froth": False,
    }

    with patch.object(orchestrator.db, "get_event_triggers", return_value=[cap_row]):
        is_active, info = orchestrator.check_cadence_or_trigger(thursday)

    assert is_active is True
    assert info["is_regime_capitulation"] is True
    assert "REGIME_CAPITULATION" in info["trigger_reasons"]


def test_check_cadence_or_trigger_froth_event(orchestrator: PipelineOrchestrator) -> None:
    """Verify check_cadence_or_trigger detects froth / overheat event."""
    friday = date(2026, 9, 25)
    froth_row = {
        "product_id": "BTC-USD",
        "trade_date_utc": friday,
        "is_weekly_cadence_day": False,
        "is_drawdown_event": False,
        "is_regime_capitulation": False,
        "is_regime_froth": True,
    }

    with patch.object(orchestrator.db, "get_event_triggers", return_value=[froth_row]):
        is_active, info = orchestrator.check_cadence_or_trigger(friday)

    assert is_active is True
    assert info["is_regime_froth"] is True
    assert "REGIME_FROTH" in info["trigger_reasons"]


def test_check_cadence_or_trigger_black_swan_flag(orchestrator: PipelineOrchestrator) -> None:
    """Verify MNI black swan flag triggers committee deliberation."""
    day = date(2026, 9, 22)
    mock_mni = MagicMock()
    mock_mni.black_swan_flag = True

    with patch.object(orchestrator.db, "get_event_triggers", return_value=[]):
        is_active, info = orchestrator.check_cadence_or_trigger(day, mni_report=mock_mni)

    assert is_active is True
    assert info["black_swan_flag"] is True
    assert "BLACK_SWAN_ALERT" in info["trigger_reasons"]


def test_check_cadence_or_trigger_empty_table_fallback(
    orchestrator: PipelineOrchestrator,
) -> None:
    """Verify fallback to Sunday calendar check when event triggers table is empty."""
    sunday = date(2026, 9, 20)  # Sunday
    monday = date(2026, 9, 21)  # Monday

    with patch.object(orchestrator.db, "get_event_triggers", return_value=[]):
        is_active_sun, info_sun = orchestrator.check_cadence_or_trigger(sunday)
        is_active_mon, info_mon = orchestrator.check_cadence_or_trigger(monday)

    assert is_active_sun is True
    assert "WEEKLY_CADENCE_SUNDAY" in info_sun["trigger_reasons"]
    assert is_active_mon is False
    assert info_mon["trigger_reasons"] == []


# =============================================================================
# 2. End-to-End Daily Pipeline Execution Tests
# =============================================================================


def test_pipeline_chop_day_records_heartbeat_without_llm(
    orchestrator: PipelineOrchestrator,
) -> None:
    """Verify standard chop day bypasses LLM deliberation and records lightweight heartbeat."""
    target_date = date(2026, 9, 22)  # Tuesday, standard chop day
    orchestrator.db.set_watermark(datetime(2026, 9, 22, 0, 0, tzinfo=UTC), "run-test")

    chop_trigger = {
        "product_id": "BTC-USD",
        "trade_date_utc": target_date,
        "is_weekly_cadence_day": False,
        "is_drawdown_event": False,
        "is_regime_capitulation": False,
        "is_regime_froth": False,
        "return_24h": -0.01,
        "drawdown_7d": -0.02,
        "mvrv_ratio": 1.5,
        "mayer_multiple": 1.1,
        "fng_value": 50,
    }

    with (
        patch.object(orchestrator, "_sync_market_candles") as mock_sync,
        patch("bitcoin_data_platform.pipeline.orchestrator.SentimentClient") as mock_sent,
        patch("bitcoin_data_platform.pipeline.orchestrator.MacroCalendarClient") as mock_cal,
        patch(
            "bitcoin_data_platform.pipeline.orchestrator.MacroNarrativeSynthesizer"
        ) as mock_synth,
        patch.object(orchestrator.db, "get_event_triggers", return_value=[chop_trigger]),
        patch.object(InvestmentCommitteeEngine, "deliberate") as mock_deliberate,
    ):
        mock_sync.return_value = {"status": "fresh"}
        mock_sent_inst = MagicMock()
        mock_sent_inst.fetch_current.return_value = SentimentRecord(
            date_utc=datetime.now(UTC),
            value=50,
            classification="Neutral",
            ingested_at_utc=datetime.now(UTC),
        )
        mock_sent.return_value = mock_sent_inst
        mock_cal_inst = MagicMock()
        mock_cal_inst.fetch_week_events.return_value = []
        mock_cal.return_value = mock_cal_inst

        mock_synth_inst = MagicMock()
        mock_mni = MagicMock()
        mock_mni.composite_mni = 0.05
        mock_mni.regime.value = "NEUTRAL_CHOP"
        mock_mni.black_swan_flag = False
        mock_synth_inst.synthesize_from_db.return_value = mock_mni
        mock_synth.return_value = mock_synth_inst

        # Populate market signal row for 2026-09-22 so PaperTradingEngine can execute real step
        con = orchestrator.db.get_connection()
        con.execute(
            """
            DROP VIEW IF EXISTS mart_btc_investment_signals_daily;
            CREATE TABLE IF NOT EXISTS mart_btc_investment_signals_daily (
                trade_date_utc TIMESTAMPTZ PRIMARY KEY,
                market_close_usd DOUBLE,
                sma_200 DOUBLE,
                mayer_multiple DOUBLE,
                mvrv_ratio DOUBLE,
                fng_value INTEGER,
                fng_classification VARCHAR,
                has_high_impact_macro_event BOOLEAN,
                investment_signal VARCHAR
            );
            INSERT OR REPLACE INTO mart_btc_investment_signals_daily VALUES (
                '2026-09-22 00:00:00+00', 80000.0, 70000.0, 1.14, 1.8, 50,
                'Neutral', false, 'STANDARD_DCA'
            );
            """
        )

        # Initialize paper balance via PaperTradingEngine
        paper_eng = PaperTradingEngine(db_path=orchestrator.db.db_path_str)
        paper_eng.init_portfolio(initial_cash=1000.0)

        report = orchestrator.run_daily(target_date=target_date)

        assert report.cadence == PipelineCadence.DAILY
        assert report.overall_status == JobStatus.SUCCESS

        # Critical Assertion 1: LLM deliberate was NEVER called!
        mock_deliberate.assert_not_called()

        # Critical Assertion 2: Committee step succeeded with heartbeat metadata
        step_map = {s.step_name: s for s in report.steps}
        assert "investment_committee_deliberation" in step_map
        comm_step = step_map["investment_committee_deliberation"]
        assert comm_step.status == JobStatus.SUCCESS
        assert comm_step.metadata["deliberation_invoked"] is False
        assert comm_step.metadata["cadence_or_trigger"] is False
        assert comm_step.metadata["heartbeat"] is True
        assert comm_step.metadata["tokens_saved"] is True
        assert comm_step.metadata["proposed_action"] == AllocationAction.HEARTBEAT.value
        assert comm_step.metadata["clamped_usd"] == 0.0

        # Critical Assertion 3: Heartbeat memo was persisted in DuckDB
        memos = orchestrator.db.get_investment_memos_history(limit=5)
        assert len(memos) >= 1
        hb_memo = memos[0]
        assert hb_memo["proposed_action"] == AllocationAction.HEARTBEAT.value
        assert float(hb_memo["clamped_allocation_usd"]) == 0.0
        assert "HEARTBEAT" in hb_memo["executive_summary_id"]

        # Critical Assertion 4: Paper trading step executed NO_ACTION
        paper_step = step_map["paper_trading_execution_step"]
        assert paper_step.status == JobStatus.SUCCESS
        assert paper_step.metadata["side"] == "NO_ACTION"
        assert float(paper_step.metadata["gross_amount_usd"]) == 0.0


def test_pipeline_weekly_cadence_invokes_llm(orchestrator: PipelineOrchestrator) -> None:
    """Verify weekly cadence day invokes LLM investment committee deliberation."""
    target_date = date(2026, 9, 20)  # Sunday
    orchestrator.db.set_watermark(datetime(2026, 9, 20, 0, 0, tzinfo=UTC), "run-test")

    weekly_trigger = {
        "product_id": "BTC-USD",
        "trade_date_utc": target_date,
        "is_weekly_cadence_day": True,
        "is_drawdown_event": False,
        "is_regime_capitulation": False,
        "is_regime_froth": False,
    }

    mock_memo = InvestmentMemorandum(
        memo_id="memo_cadence_test",
        memo_date=target_date,
        created_at_utc=datetime.now(UTC),
        market_regime="STANDARD_REGIME",
        composite_mni=0.10,
        consensus_score=0.25,
        executive_summary_id="Weekly core routine accumulation.",
        macro_thesis="Macro stable.",
        valuation_thesis="Fair value.",
        technical_thesis="Support held.",
        dissenting_opinions="None",
        proposed_action=AllocationAction.STANDARD_DCA,
        proposed_allocation_usd=20.0,
        clamped_allocation_usd=20.0,
        allocation_clamped=False,
        clamping_reason=None,
        risk_guard_passed=True,
        memo_markdown="Markdown",
        votes=[],
    )

    with (
        patch.object(orchestrator, "_sync_market_candles") as mock_sync,
        patch("bitcoin_data_platform.pipeline.orchestrator.SentimentClient") as mock_sent,
        patch("bitcoin_data_platform.pipeline.orchestrator.MacroCalendarClient") as mock_cal,
        patch(
            "bitcoin_data_platform.pipeline.orchestrator.MacroNarrativeSynthesizer"
        ) as mock_synth,
        patch.object(orchestrator.db, "get_event_triggers", return_value=[weekly_trigger]),
        patch.object(
            InvestmentCommitteeEngine, "deliberate", return_value=mock_memo
        ) as mock_deliberate,
        patch("bitcoin_data_platform.pipeline.orchestrator.PaperTradingEngine") as mock_paper,
    ):
        mock_sync.return_value = {"status": "fresh"}
        mock_sent_inst = MagicMock()
        mock_sent_inst.fetch_current.return_value = SentimentRecord(
            date_utc=datetime.now(UTC),
            value=50,
            classification="Neutral",
            ingested_at_utc=datetime.now(UTC),
        )
        mock_sent.return_value = mock_sent_inst
        mock_cal_inst = MagicMock()
        mock_cal_inst.fetch_week_events.return_value = []
        mock_cal.return_value = mock_cal_inst

        mock_synth_inst = MagicMock()
        mock_mni = MagicMock()
        mock_mni.composite_mni = 0.10
        mock_mni.regime.value = "STANDARD_REGIME"
        mock_mni.black_swan_flag = False
        mock_synth_inst.synthesize_from_db.return_value = mock_mni
        mock_synth.return_value = mock_synth_inst

        mock_paper_inst = MagicMock()
        mock_paper_inst.step.return_value = PaperTradeRecord(
            trade_id="tr_cadence",
            portfolio_id="default",
            executed_at_utc=datetime.now(UTC),
            trade_date=target_date,
            side="BUY",
            signal_regime="STANDARD_DCA",
            spot_price=80000.0,
            gross_amount_usd=20.0,
            fee_usd=0.02,
            net_amount_usd=19.98,
            btc_amount=0.00025,
            narrative="Weekly buy",
        )
        mock_paper.return_value = mock_paper_inst

        report = orchestrator.run_daily(target_date=target_date)

        assert report.cadence == PipelineCadence.DAILY
        assert report.overall_status == JobStatus.SUCCESS

        # Committee deliberation MUST be invoked
        mock_deliberate.assert_called_once()

        step_map = {s.step_name: s for s in report.steps}
        comm_step = step_map["investment_committee_deliberation"]
        assert comm_step.status == JobStatus.SUCCESS
        assert comm_step.metadata["deliberation_invoked"] is True
        assert comm_step.metadata["cadence_or_trigger"] is True
        assert "WEEKLY_CADENCE" in comm_step.metadata["trigger_reasons"]
        assert comm_step.metadata["heartbeat"] is False
        assert comm_step.metadata["tokens_saved"] is False
        assert comm_step.metadata["proposed_action"] == AllocationAction.STANDARD_DCA.value


def test_pipeline_drawdown_trigger_invokes_llm(orchestrator: PipelineOrchestrator) -> None:
    """Verify drawdown event triggers LLM investment committee deliberation."""
    target_date = date(2026, 9, 23)  # Wednesday (drawdown trigger day)
    orchestrator.db.set_watermark(datetime(2026, 9, 23, 0, 0, tzinfo=UTC), "run-test")

    dd_trigger = {
        "product_id": "BTC-USD",
        "trade_date_utc": target_date,
        "is_weekly_cadence_day": False,
        "is_drawdown_event": True,
        "is_regime_capitulation": False,
        "is_regime_froth": False,
    }

    mock_memo = InvestmentMemorandum(
        memo_id="memo_dd_test",
        memo_date=target_date,
        created_at_utc=datetime.now(UTC),
        market_regime="DRAWDOWN_EPISODE",
        composite_mni=-0.25,
        consensus_score=0.40,
        executive_summary_id="Drawdown sniper execution.",
        macro_thesis="Flash crash observed.",
        valuation_thesis="Discount valuation.",
        technical_thesis="R24 <= -5%.",
        dissenting_opinions="None",
        proposed_action=AllocationAction.AGGRESSIVE_ACCUMULATE,
        proposed_allocation_usd=90.0,
        clamped_allocation_usd=90.0,
        allocation_clamped=False,
        clamping_reason=None,
        risk_guard_passed=True,
        memo_markdown="Markdown",
        votes=[],
    )

    with (
        patch.object(orchestrator, "_sync_market_candles") as mock_sync,
        patch("bitcoin_data_platform.pipeline.orchestrator.SentimentClient") as mock_sent,
        patch("bitcoin_data_platform.pipeline.orchestrator.MacroCalendarClient") as mock_cal,
        patch(
            "bitcoin_data_platform.pipeline.orchestrator.MacroNarrativeSynthesizer"
        ) as mock_synth,
        patch.object(orchestrator.db, "get_event_triggers", return_value=[dd_trigger]),
        patch.object(
            InvestmentCommitteeEngine, "deliberate", return_value=mock_memo
        ) as mock_deliberate,
        patch("bitcoin_data_platform.pipeline.orchestrator.PaperTradingEngine") as mock_paper,
    ):
        mock_sync.return_value = {"status": "fresh"}
        mock_sent_inst = MagicMock()
        mock_sent_inst.fetch_current.return_value = SentimentRecord(
            date_utc=datetime.now(UTC),
            value=30,
            classification="Fear",
            ingested_at_utc=datetime.now(UTC),
        )
        mock_sent.return_value = mock_sent_inst
        mock_cal_inst = MagicMock()
        mock_cal_inst.fetch_week_events.return_value = []
        mock_cal.return_value = mock_cal_inst

        mock_synth_inst = MagicMock()
        mock_mni = MagicMock()
        mock_mni.composite_mni = -0.25
        mock_mni.regime.value = "DRAWDOWN_EPISODE"
        mock_mni.black_swan_flag = False
        mock_synth_inst.synthesize_from_db.return_value = mock_mni
        mock_synth.return_value = mock_synth_inst

        mock_paper_inst = MagicMock()
        mock_paper_inst.step.return_value = PaperTradeRecord(
            trade_id="tr_sniper",
            portfolio_id="default",
            executed_at_utc=datetime.now(UTC),
            trade_date=target_date,
            side="BUY",
            signal_regime="AGGRESSIVE_ACCUMULATE",
            spot_price=76000.0,
            gross_amount_usd=90.0,
            fee_usd=0.09,
            net_amount_usd=89.91,
            btc_amount=0.00118,
            narrative="Sniper buy",
        )
        mock_paper.return_value = mock_paper_inst

        report = orchestrator.run_daily(target_date=target_date)

        assert report.overall_status == JobStatus.SUCCESS
        mock_deliberate.assert_called_once()

        step_map = {s.step_name: s for s in report.steps}
        comm_step = step_map["investment_committee_deliberation"]
        assert comm_step.metadata["deliberation_invoked"] is True
        assert comm_step.metadata["cadence_or_trigger"] is True
        assert "DRAWDOWN_EVENT" in comm_step.metadata["trigger_reasons"]
        assert comm_step.metadata["heartbeat"] is False
        assert comm_step.metadata["tokens_saved"] is False


def test_committee_record_heartbeat_duckdb_persistence(tmp_duckdb: DuckDBManager) -> None:
    """Verify InvestmentCommitteeEngine.record_heartbeat creates valid persisted record."""
    committee = InvestmentCommitteeEngine(tmp_duckdb, use_llm=False)
    test_date = date(2026, 9, 22)

    memo = committee.record_heartbeat(
        target_date=test_date,
        dry_run=False,
        reason="ROUTINE_CHOP_DAY",
    )

    assert memo.proposed_action == AllocationAction.HEARTBEAT
    assert memo.clamped_allocation_usd == 0.0
    assert memo.allocation_clamped is False
    assert memo.risk_guard_passed is True

    # Read back from DuckDB
    rows = tmp_duckdb.get_investment_memos_history(limit=5)
    assert len(rows) == 1
    r = rows[0]
    assert r["memo_id"] == memo.memo_id
    assert r["proposed_action"] == AllocationAction.HEARTBEAT.value
    assert float(r["clamped_allocation_usd"]) == 0.0
    assert r["clamping_reason"] == "PACING_HEARTBEAT: ROUTINE_CHOP_DAY"
