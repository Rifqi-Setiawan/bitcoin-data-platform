"""Automated tests for Scheduling Pipeline DAGs, Concurrency Locks, and Systemd (Group E)."""

from __future__ import annotations

import configparser
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bitcoin_data_platform.pipeline.lock_manager import (
    ConcurrentRunLockError,
    LockManager,
)
from bitcoin_data_platform.pipeline.models import (
    JobStatus,
    PipelineCadence,
)
from bitcoin_data_platform.pipeline.orchestrator import PipelineOrchestrator
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def tmp_duckdb(tmp_path: Path) -> DuckDBManager:
    db_file = tmp_path / "test_pipeline.duckdb"
    mgr = DuckDBManager(db_file)
    with mgr:
        mgr.initialize()
    return mgr


# 35. test_pipeline_lock_acquisition_and_release
def test_pipeline_lock_acquisition_and_release(tmp_path: Path) -> None:
    lock_mgr = LockManager(tmp_path / "locks")
    assert lock_mgr.is_locked(PipelineCadence.HOURLY) is False

    with lock_mgr.acquire(PipelineCadence.HOURLY) as lock_file:
        assert lock_file.exists()
        assert lock_mgr.is_locked(PipelineCadence.HOURLY) is True

    # Lockfile automatically removed upon context manager exit
    assert lock_mgr.is_locked(PipelineCadence.HOURLY) is False
    assert not lock_file.exists()


# 36. test_pipeline_concurrent_run_lock_exit_6
def test_pipeline_concurrent_run_lock_exit_6(tmp_path: Path) -> None:
    lock_mgr = LockManager(tmp_path / "locks")

    with lock_mgr.acquire(PipelineCadence.DAILY):
        # Second attempt on same cadence while active must raise ConcurrentRunLockError
        with (
            pytest.raises(ConcurrentRunLockError) as excinfo,
            lock_mgr.acquire(PipelineCadence.DAILY),
        ):
            pass
        assert excinfo.value.cadence == PipelineCadence.DAILY


# 37. test_stale_lock_cleanup_on_dead_pid
def test_stale_lock_cleanup_on_dead_pid(tmp_path: Path) -> None:
    lock_dir = tmp_path / "locks"
    lock_mgr = LockManager(lock_dir)
    lock_path = lock_dir / "pipeline_hourly.lock"
    lock_dir.mkdir(parents=True, exist_ok=True)

    # Write lockfile with guaranteed dead PID (e.g. 99999999)
    dead_pid = 99999999
    payload = {"cadence": "hourly", "pid": dead_pid, "acquired_at_utc": "2026-09-19T00:00:00Z"}
    lock_path.write_text(json.dumps(payload), encoding="utf-8")

    # is_locked should detect dead PID, remove lockfile, and return False
    assert lock_mgr.is_locked(PipelineCadence.HOURLY) is False
    assert not lock_path.exists()


# 38. test_pipeline_run_hourly_dag
def test_pipeline_run_hourly_dag(tmp_path: Path, tmp_duckdb: DuckDBManager) -> None:
    orchestrator = PipelineOrchestrator(
        repo_root=tmp_path,
        db_manager=tmp_duckdb,
        lock_dir=tmp_path / "locks",
    )

    with patch("bitcoin_data_platform.pipeline.orchestrator.FeedIngester") as mock_ingester:
        mock_instance = MagicMock()
        mock_instance.fetch_all_feeds.return_value = []
        mock_ingester.return_value = mock_instance

        report = orchestrator.run_hourly()

        assert report.cadence == PipelineCadence.HOURLY
        assert report.overall_status == JobStatus.SUCCESS
        step_names = [s.step_name for s in report.steps]
        assert "fetch_crypto_rss_news" in step_names
        assert "sentinel_black_swan_scan" in step_names


# 39. test_pipeline_run_daily_dag
def test_pipeline_run_daily_dag(tmp_path: Path, tmp_duckdb: DuckDBManager) -> None:
    orchestrator = PipelineOrchestrator(
        repo_root=tmp_path,
        db_manager=tmp_duckdb,
        lock_dir=tmp_path / "locks",
    )

    with (
        patch("bitcoin_data_platform.pipeline.orchestrator.SentimentClient") as mock_sent,
        patch("bitcoin_data_platform.pipeline.orchestrator.MacroCalendarClient") as mock_cal,
        patch("bitcoin_data_platform.pipeline.orchestrator.PaperTradingEngine") as mock_paper,
    ):
        mock_sent_inst = MagicMock()
        from bitcoin_data_platform.sources.sentiment_contract import SentimentRecord

        mock_sent_inst.fetch_current.return_value = SentimentRecord(
            date_utc=datetime.now(UTC),
            value=55,
            classification="Greed",
            ingested_at_utc=datetime.now(UTC),
        )
        mock_sent.return_value = mock_sent_inst

        mock_cal_inst = MagicMock()
        mock_cal_inst.fetch_week_events.return_value = []
        mock_cal.return_value = mock_cal_inst

        mock_paper_inst = MagicMock()
        from bitcoin_data_platform.paper.models import PaperTradeRecord

        mock_paper_inst.step.return_value = PaperTradeRecord(
            trade_id="t-1",
            portfolio_id="default",
            executed_at_utc=datetime.now(UTC),
            trade_date=datetime.now(UTC).date(),
            side="BUY",
            signal_regime="STANDARD_DCA",
            spot_price=65000.0,
            gross_amount_usd=10.0,
            fee_usd=0.01,
            net_amount_usd=9.99,
            btc_amount=0.00015,
            narrative="Test Step",
        )
        mock_paper.return_value = mock_paper_inst

        report = orchestrator.run_daily()

        assert report.cadence == PipelineCadence.DAILY
        assert report.overall_status == JobStatus.SUCCESS
        step_names = [s.step_name for s in report.steps]
        assert "incremental_market_sync" in step_names
        assert "fetch_sentiment_fng" in step_names
        assert "fetch_macro_calendar" in step_names
        assert "synthesize_composite_mni" in step_names
        assert "investment_committee_deliberation" in step_names
        assert "paper_trading_execution_step" in step_names


# 40. test_pipeline_run_weekly_dag
def test_pipeline_run_weekly_dag(tmp_path: Path, tmp_duckdb: DuckDBManager) -> None:
    orchestrator = PipelineOrchestrator(
        repo_root=tmp_path,
        db_manager=tmp_duckdb,
        lock_dir=tmp_path / "locks",
    )

    report = orchestrator.run_weekly()
    assert report.cadence == PipelineCadence.WEEKLY
    assert report.overall_status == JobStatus.SUCCESS
    step_names = [s.step_name for s in report.steps]
    assert "portfolio_risk_audit" in step_names
    assert "tactical_reserve_audit" in step_names
    assert "weekly_retrospective_memo" in step_names


# 41. test_systemd_hourly_service_timer_config
def test_systemd_hourly_service_timer_config() -> None:
    repo_root = Path(__file__).resolve().parent.parent.parent
    svc_path = repo_root / "infra" / "systemd" / "bitcoin-data-hourly.service"
    tmr_path = repo_root / "infra" / "systemd" / "bitcoin-data-hourly.timer"

    assert svc_path.is_file()
    assert tmr_path.is_file()

    svc_ini = configparser.ConfigParser(interpolation=None)
    svc_ini.read_file(svc_path.open("r", encoding="utf-8"))
    assert svc_ini["Service"]["Type"] == "oneshot"
    assert svc_ini["Service"]["ProtectSystem"] == "strict"
    assert svc_ini["Service"]["ProtectHome"] == "true"
    assert svc_ini["Service"]["NoNewPrivileges"] == "true"
    assert "run-hourly" in svc_ini["Service"]["ExecStart"]

    tmr_ini = configparser.ConfigParser(interpolation=None)
    tmr_ini.read_file(tmr_path.open("r", encoding="utf-8"))
    assert tmr_ini["Timer"]["OnCalendar"] == "*-*-* *:05:00 UTC"


# 42. test_systemd_daily_weekly_service_timer_config
def test_systemd_daily_weekly_service_timer_config() -> None:
    repo_root = Path(__file__).resolve().parent.parent.parent

    # Daily
    daily_svc = repo_root / "infra" / "systemd" / "bitcoin-data-daily.service"
    daily_tmr = repo_root / "infra" / "systemd" / "bitcoin-data-daily.timer"
    d_svc_ini = configparser.ConfigParser(interpolation=None)
    d_svc_ini.read_file(daily_svc.open("r", encoding="utf-8"))
    assert "run-daily" in d_svc_ini["Service"]["ExecStart"]
    assert d_svc_ini["Service"]["MemoryMax"] == "1.5G"

    d_tmr_ini = configparser.ConfigParser(interpolation=None)
    d_tmr_ini.read_file(daily_tmr.open("r", encoding="utf-8"))
    assert d_tmr_ini["Timer"]["OnCalendar"] == "*-*-* 00:05:00 UTC"

    # Weekly
    weekly_svc = repo_root / "infra" / "systemd" / "bitcoin-data-weekly.service"
    weekly_tmr = repo_root / "infra" / "systemd" / "bitcoin-data-weekly.timer"
    w_svc_ini = configparser.ConfigParser(interpolation=None)
    w_svc_ini.read_file(weekly_svc.open("r", encoding="utf-8"))
    assert "run-weekly" in w_svc_ini["Service"]["ExecStart"]

    w_tmr_ini = configparser.ConfigParser(interpolation=None)
    w_tmr_ini.read_file(weekly_tmr.open("r", encoding="utf-8"))
    assert w_tmr_ini["Timer"]["OnCalendar"] == "Mon *-*-* 01:00:00 UTC"
