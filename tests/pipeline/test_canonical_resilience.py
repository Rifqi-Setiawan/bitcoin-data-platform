"""Tests for canonical resilience and fail-closed invariants from Professor's blueprint."""

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from bitcoin_data_platform.committee.engine import InvestmentCommitteeEngine
from bitcoin_data_platform.committee.models import AllocationAction
from bitcoin_data_platform.paper.engine import PaperTradingEngine
from bitcoin_data_platform.paper.models import PaperTradeRecord
from bitcoin_data_platform.pipeline.lock_manager import (
    ConcurrentRunLockError,
    LockManager,
)
from bitcoin_data_platform.pipeline.models import PipelineCadence
from bitcoin_data_platform.pipeline.orchestrator import PipelineOrchestrator
from bitcoin_data_platform.sources.sentiment_contract import SentimentRecord
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def test_lock_manager_global_writer_prevents_collision(tmp_path: Path) -> None:
    """Global writer lock prevents concurrent hourly and daily execution."""
    lock_dir = tmp_path / "locks"
    mgr1 = LockManager(lock_dir=lock_dir)
    mgr2 = LockManager(lock_dir=lock_dir)

    with (
        mgr1.acquire(PipelineCadence.HOURLY),
        pytest.raises(ConcurrentRunLockError) as exc_info,
        mgr2.acquire(PipelineCadence.DAILY),
    ):
        pass
    assert "writer.lock" in str(exc_info.value.lock_path)


def test_committee_persists_data_unavailable_memo(tmp_path: Path) -> None:
    """Emergency DATA_UNAVAILABLE memorandum is persisted to DuckDB (durable audit trail)."""
    db_path = tmp_path / "test_state.duckdb"
    db = DuckDBManager(str(db_path))
    db.initialize()

    committee = InvestmentCommitteeEngine(db_manager=db, use_llm=False)
    target_date = date(2026, 9, 21)

    memo = committee.deliberate(
        target_date=target_date,
        dry_run=False,
        fail_closed_action=True,
    )
    assert memo.proposed_action == AllocationAction.DATA_UNAVAILABLE
    assert memo.clamped_allocation_usd == 0.0

    # Verify memorandum is stored in DuckDB
    rows = db.execute_query(
        f"SELECT memo_id, proposed_action, clamped_allocation_usd "
        f"FROM investment_committee_memos WHERE memo_date = '{target_date}'"
    )
    assert len(rows) == 1
    assert rows[0]["proposed_action"] == "DATA_UNAVAILABLE"
    assert rows[0]["clamped_allocation_usd"] == 0.0


def test_paper_engine_decision_first_safe_hold(tmp_path: Path) -> None:
    """Decision-first paper step records safe HOLD without crashing or tripping kill switch."""
    db_path = tmp_path / "paper_test.duckdb"
    db = DuckDBManager(str(db_path))
    db.initialize()

    ks_path = tmp_path / "data" / "state" / "PAPER_KILL_SWITCH"
    paper = PaperTradingEngine(db_path=str(db_path), kill_switch_path=ks_path)
    target_date = date(2026, 9, 21)

    memo_dict = {
        "proposed_action": "DATA_UNAVAILABLE",
        "clamped_allocation_usd": 0.0,
        "executive_summary_id": "Data snapshot unavailable in analytical marts.",
    }

    # Step must succeed with side=HOLD and no exception
    record = paper.step(trade_date=target_date, committee_memo=memo_dict)
    assert record.side == "HOLD"
    assert record.gross_amount_usd == 0.0
    assert record.btc_amount == 0.0
    assert "KOMITE HALT" in record.narrative

    # Verify PAPER_KILL_SWITCH was NOT created
    assert not ks_path.exists()

    # Verify balance was NOT altered
    bal = paper.get_portfolio_balance()
    assert bal.total_cash == 1000.0
    assert bal.total_trades == 0


def test_pipeline_evaluates_t_minus_one_near_midnight(tmp_path: Path) -> None:
    """Pipeline evaluates T-1 date when running in 00:00-00:30 UTC window."""
    db_path = tmp_path / "pipe_test.duckdb"
    db = DuckDBManager(str(db_path))
    db.initialize()
    db.set_watermark(datetime(2026, 9, 20, 23, 0, tzinfo=UTC), "init")

    orchestrator = PipelineOrchestrator(
        db_manager=db,
        repo_root=tmp_path,
        lock_dir=tmp_path / "locks",
    )

    captured_dates: dict[str, date] = {}

    def _mock_synthesize(target_date: date) -> Any:
        captured_dates["synthesizer"] = target_date
        mock_m = MagicMock()
        mock_m.composite_mni = 0.0
        mock_m.regime.value = "NEUTRAL_CHOP"
        mock_m.black_swan_flag = False
        return mock_m

    def _mock_deliberate(target_date: date, **kwargs: Any) -> Any:
        captured_dates["committee"] = target_date
        mock_memo = MagicMock()
        mock_memo.proposed_action.value = "OPPORTUNISTIC_BUY"
        mock_memo.consensus_score = 0.16
        mock_memo.clamped_allocation_usd = 9.63
        mock_memo.allocation_clamped = False
        return mock_memo

    def _mock_step(trade_date: date, **kwargs: Any) -> PaperTradeRecord:
        captured_dates["paper"] = trade_date
        return PaperTradeRecord(
            trade_id="tr1",
            portfolio_id="paper_default",
            executed_at_utc=datetime.now(UTC),
            trade_date=trade_date,
            side="BUY",
            signal_regime="NEUTRAL_CHOP",
            spot_price=81000.0,
            gross_amount_usd=9.63,
            fee_usd=0.01,
            net_amount_usd=9.62,
            btc_amount=0.0001,
            narrative="test",
        )

    frozen_now = datetime(2026, 9, 21, 0, 5, 0, tzinfo=UTC)

    with (
        patch("bitcoin_data_platform.pipeline.orchestrator.datetime") as mock_dt,
        patch.object(orchestrator, "_sync_market_candles") as mock_sync,
        patch("bitcoin_data_platform.pipeline.orchestrator.SentimentClient") as mock_sent,
        patch("bitcoin_data_platform.pipeline.orchestrator.MacroCalendarClient") as mock_cal,
        patch(
            "bitcoin_data_platform.pipeline.orchestrator.MacroNarrativeSynthesizer"
        ) as mock_synth,
        patch("bitcoin_data_platform.pipeline.orchestrator.InvestmentCommitteeEngine") as mock_comm,
        patch("bitcoin_data_platform.pipeline.orchestrator.PaperTradingEngine") as mock_paper,
    ):
        mock_dt.now.return_value = frozen_now
        mock_sync.return_value = {"status": "fresh"}

        mock_sent_inst = MagicMock()
        mock_sent_inst.fetch_current.return_value = SentimentRecord(
            date_utc=frozen_now,
            value=50,
            classification="Neutral",
            ingested_at_utc=frozen_now,
        )
        mock_sent.return_value = mock_sent_inst

        mock_cal_inst = MagicMock()
        mock_cal_inst.fetch_week_events.return_value = []
        mock_cal.return_value = mock_cal_inst

        mock_synth_inst = MagicMock()
        mock_synth_inst.synthesize_from_db.side_effect = _mock_synthesize
        mock_synth.return_value = mock_synth_inst

        mock_comm_inst = MagicMock()
        mock_comm_inst.deliberate.side_effect = _mock_deliberate
        mock_comm.return_value = mock_comm_inst

        mock_paper_inst = MagicMock()
        mock_paper_inst.step.side_effect = _mock_step
        mock_paper.return_value = mock_paper_inst

        report = orchestrator.run_daily()
        assert report.overall_status.value == "SUCCESS"

        # Verify that T-1 (2026-09-20) was passed across all 3 components!
        expected_date = date(2026, 9, 20)
        assert captured_dates["synthesizer"] == expected_date
        assert captured_dates["committee"] == expected_date
        assert captured_dates["paper"] == expected_date
