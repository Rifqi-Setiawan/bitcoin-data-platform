"""Integration tests for Committee, Intelligence, and Pipeline CLI commands (Group G)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.paper.models import PaperTradeRecord
from bitcoin_data_platform.sources.sentiment_contract import SentimentRecord


# 51. test_cli_committee_deliberate_dry_run
def test_cli_committee_deliberate_dry_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_file = tmp_path / "cli_comm.duckdb"
    code = main(
        [
            "committee",
            "deliberate",
            "--date",
            "2026-09-19",
            "--dry-run",
            "--db-path",
            str(db_file),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "INVESTMENT COMMITTEE DELIBERATION" in out
    assert "[DRY-RUN]" in out
    assert "Ringkasan Eksekutif" in out


# 52. test_cli_committee_memo_render
def test_cli_committee_memo_render(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_file = tmp_path / "cli_memo.duckdb"
    # First deliberate and save
    main(["committee", "deliberate", "--date", "2026-09-19", "--db-path", str(db_file)])
    capsys.readouterr()

    # Query memo in markdown format
    code = main(
        [
            "committee",
            "memo",
            "--date",
            "2026-09-19",
            "--format",
            "markdown",
            "--db-path",
            str(db_file),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "# Institutional Investment Committee Memorandum" in out
    assert "Verifikasi Neuro-Simbolik" in out


# 53. test_cli_committee_status
def test_cli_committee_status(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_file = tmp_path / "cli_status.duckdb"
    main(["committee", "deliberate", "--date", "2026-09-19", "--db-path", str(db_file)])
    capsys.readouterr()

    code = main(["committee", "status", "--db-path", str(db_file)])
    assert code == 0
    out = capsys.readouterr().out
    assert "INVESTMENT COMMITTEE OPERATIONAL STATUS" in out
    assert "Latest Deliberation:" in out
    assert "Consensus Score:" in out


# 54. test_cli_intelligence_ingest
def test_cli_intelligence_ingest(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_file = tmp_path / "cli_intel.duckdb"
    code = main(
        [
            "intelligence",
            "ingest",
            "--title",
            "Fed Slows Quantitative Tightening",
            "--thesis",
            "Pelonggaran laju QT oleh Fed berpotensi mendongkrak likuiditas USD",
            "--url",
            "https://example.com/fed-qt",
            "--pillar",
            "MACRO_LIQUIDITY",
            "--sentiment",
            "0.65",
            "--confidence",
            "0.85",
            "--tags",
            "fed,qt,liquidity",
            "--db-path",
            str(db_file),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "SUCCESS: Ingested intelligence item" in out
    assert "MACRO_LIQUIDITY" in out
    assert "+0.65" in out


# 55. test_cli_intelligence_list
def test_cli_intelligence_list(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_file = tmp_path / "cli_intel_list.duckdb"
    main(
        [
            "intelligence",
            "ingest",
            "--title",
            "Institutional ETF Flow",
            "--thesis",
            "Inflow harian mencapai rekor baru.",
            "--pillar",
            "INSTITUTIONAL",
            "--db-path",
            str(db_file),
        ]
    )
    capsys.readouterr()

    code = main(["intelligence", "list", "--format", "table", "--db-path", str(db_file)])
    assert code == 0
    out = capsys.readouterr().out
    assert "INSTITUTIONAL" in out
    assert "Institutional ETF Flow" in out


# 56. test_cli_pipeline_run_daily
def test_cli_pipeline_run_daily(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_file = tmp_path / "cli_pipeline.duckdb"

    with (
        patch("bitcoin_data_platform.pipeline.orchestrator.SentimentClient") as mock_sent,
        patch("bitcoin_data_platform.pipeline.orchestrator.MacroCalendarClient") as mock_cal,
        patch("bitcoin_data_platform.pipeline.orchestrator.PaperTradingEngine") as mock_paper,
    ):
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

        mock_paper_inst = MagicMock()
        mock_paper_inst.step.return_value = PaperTradeRecord(
            trade_id="trade-step-1",
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
            narrative="CLI Test",
        )
        mock_paper.return_value = mock_paper_inst

        code = main(
            [
                "pipeline",
                "run-daily",
                "--date",
                "2026-09-19",
                "--db-path",
                str(db_file),
            ]
        )

    assert code == 0
    out = capsys.readouterr().out
    assert "PIPELINE RUN REPORT [DAILY]" in out
    assert "SUCCESS" in out
