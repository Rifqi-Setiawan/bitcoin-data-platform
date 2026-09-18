"""Integration tests for the 'bitcoin-data diagnose' CLI command."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def test_diagnose_cli_text_format_healthy(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """1. diagnose --format text on healthy system outputs summary table and exits 0."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()

    exit_code = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--format",
            "text",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "BITCOIN DATA PLATFORM — OPERATIONAL DIAGNOSTICS" in captured.out
    assert "System State: HEALTHY" in captured.out
    assert "No active incidents detected" in captured.out


def test_diagnose_cli_json_format(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """2. diagnose --format json outputs machine-readable JSON."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()

    exit_code = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--format",
            "json",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["overall_status"] == "HEALTHY"
    assert "report_id" in data
    assert "telemetry_summary" in data
    assert "executed_queries" in data


def test_diagnose_cli_markdown_format(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """3. diagnose --format markdown outputs formatted Markdown."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()

    exit_code = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--format",
            "markdown",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "# Operational Diagnostics & Incident Report" in captured.out
    assert "## 3. Telemetry Snapshot" in captured.out


def test_diagnose_cli_output_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """4. diagnose --output persists report to specified path."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"
    report_file = tmp_path / "reports" / "audit.json"

    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()

    exit_code = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--output",
            str(report_file),
        ]
    )

    assert exit_code == 0
    assert report_file.exists()
    report_json = json.loads(report_file.read_text(encoding="utf-8"))
    assert "report_id" in report_json


def test_diagnose_cli_stale_lock_auto_heal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """5. diagnose --auto-heal detects stale lock, reports INC-04, and executes auto-heal."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    now = datetime(2026, 1, 15, 14, 0, tzinfo=UTC)
    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()
        # Acquire stale lock (2.5 hours old)
        mgr.acquire_lock(
            run_id="run-stale-cli",
            mode="incremental",
            started_at_utc=now - timedelta(hours=2, minutes=30),
        )
        assert mgr.check_lock() is True

    # 1. Run in dry-run mode (default)
    exit_code = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--format",
            "text",
        ],
        clock=lambda: now,
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "INC-04" in captured.out
    assert "System State: DEGRADED" in captured.out
    assert "Mode:         DRY-RUN" in captured.out
    assert "[DRY-RUN] ACTION_CLEAR_STALE_LOCK" in captured.out

    # Lock must still be held after dry-run
    with mgr:
        assert mgr.check_lock() is True

    # 2. Run with --auto-heal
    exit_code_heal = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--auto-heal",
            "--format",
            "text",
        ],
        clock=lambda: now,
    )
    assert exit_code_heal == 0
    captured_heal = capsys.readouterr()
    assert "Mode:         AUTO-HEAL (Active)" in captured_heal.out
    assert "[SUCCESS] ACTION_CLEAR_STALE_LOCK" in captured_heal.out

    # Postcondition: lock has now been cleared
    with mgr:
        assert mgr.check_lock() is False


def test_diagnose_cli_with_run_id_filter(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """6. diagnose --run-id filters diagnosis on specific run ID."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    now = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()
        mgr.record_run(
            run_id="run-rate-limited",
            mode="backfill",
            started_at_utc=now - timedelta(hours=1),
            completed_at_utc=now - timedelta(minutes=55),
            status="FAILED",
            error_message="Coinbase HTTP 429 Too Many Requests: CoinbaseHTTPError",
        )

    exit_code = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--run-id",
            "run-rate-limited",
            "--format",
            "json",
        ],
        clock=lambda: now,
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    report_data = json.loads(captured.out)
    assert report_data["target_run_id"] == "run-rate-limited"
    assert report_data["overall_status"] == "DEGRADED"
    assert len(report_data["incidents"]) == 1
    assert report_data["incidents"][0]["incident_code"] == "INC-01"


def test_diagnose_cli_output_directory_auto_name(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """7. diagnose --output pointing to a directory generates timestamped report."""
    db_path = tmp_path / "state" / "platform.duckdb"
    report_dir = tmp_path / "reports_dir"
    report_dir.mkdir(parents=True, exist_ok=True)

    mgr = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with mgr:
        mgr.initialize()

    exit_code = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--output",
            str(report_dir),
            "--format",
            "markdown",
        ]
    )

    assert exit_code == 0
    files = list(report_dir.glob("incident_report_*.md"))
    assert len(files) == 1
    assert "Operational Diagnostics" in files[0].read_text(encoding="utf-8")


def test_diagnose_cli_explicit_dry_run_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """8. diagnose with explicit --dry-run operates in simulation mode."""
    db_path = tmp_path / "state" / "platform.duckdb"
    mgr = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with mgr:
        mgr.initialize()

    exit_code = main(
        [
            "diagnose",
            "--db-path",
            str(db_path),
            "--dry-run",
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Mode:         DRY-RUN (Simulated)" in captured.out
