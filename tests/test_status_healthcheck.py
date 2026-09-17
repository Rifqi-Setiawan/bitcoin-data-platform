"""Tests for status healthcheck (--check) and formatted text output (--format text)."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.parquet_writer import write_parquet_partitions
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _make_candle(hour: int, date_str: str = "2026-01-01") -> NormalizedCandle:
    dt = datetime.fromisoformat(f"{date_str}T{hour:02d}:00:00+00:00")
    return NormalizedCandle(
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        candle_start_utc=dt,
        open=Decimal("95000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94500.00"),
        close=Decimal("95800.00"),
        volume_base=Decimal("10.0"),
        ingested_at_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        source_run_id="run-health-candle",
    )


def test_status_empty_db_is_degraded(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Empty database without watermark must evaluate to degraded (exit code 1 on --check)."""
    db_path = tmp_path / "state" / "empty.duckdb"
    curated_dir = tmp_path / "curated"

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--check",
        ]
    )
    assert exit_code == 1

    captured = capsys.readouterr()
    res = json.loads(captured.out)
    assert res["is_healthy"] is False
    assert res["watermark_utc"] is None


def test_status_fresh_watermark_without_gaps_is_healthy(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Fresh pipeline (watermark age <= 2h, 0 gaps, disk ok, no lock) evaluates to healthy."""
    db_path = tmp_path / "state" / "healthy.duckdb"
    curated_dir = tmp_path / "curated"

    # Write 5 consecutive hours of data: 00:00 to 04:00
    candles = [_make_candle(h) for h in range(5)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        # Set watermark to hour 4 (2026-01-01T04:00:00Z)
        latest_ts = datetime(2026, 1, 1, 4, 0, tzinfo=UTC)
        manager.set_watermark(latest_ts, run_id="run-init")

    # Current time is 1 hour after watermark: 2026-01-01T05:00:00Z
    def mock_clock() -> datetime:
        return datetime(2026, 1, 1, 5, 0, tzinfo=UTC)

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--check",
        ],
        clock=mock_clock,
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    res = json.loads(captured.out)
    assert res["is_healthy"] is True
    assert res["watermark_age_hours"] == 1.0


def test_status_stale_watermark_evaluates_degraded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Watermark age > 2.0 hours evaluates to degraded."""
    db_path = tmp_path / "state" / "stale.duckdb"
    curated_dir = tmp_path / "curated"

    candles = [_make_candle(0)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.set_watermark(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), run_id="run-init")

    # Current time is 3.5 hours later (2026-01-01T03:30:00Z > 2h threshold)
    def mock_clock() -> datetime:
        return datetime(2026, 1, 1, 3, 30, tzinfo=UTC)

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--check",
        ],
        clock=mock_clock,
    )
    assert exit_code == 1

    captured = capsys.readouterr()
    res = json.loads(captured.out)
    assert res["is_healthy"] is False
    assert res["watermark_age_hours"] == 3.5


def test_status_data_gaps_evaluates_degraded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Detected gaps cause health check degradation."""
    db_path = tmp_path / "state" / "gaps.duckdb"
    curated_dir = tmp_path / "curated"

    # Missing hour 1 and 2
    candles = [_make_candle(0), _make_candle(3)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.set_watermark(datetime(2026, 1, 1, 3, 0, tzinfo=UTC), run_id="run-init")

    def mock_clock() -> datetime:
        return datetime(2026, 1, 1, 3, 30, tzinfo=UTC)

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--check",
        ],
        clock=mock_clock,
    )
    assert exit_code == 1

    captured = capsys.readouterr()
    res = json.loads(captured.out)
    assert res["is_healthy"] is False
    assert len(res["gaps"]) == 1


def test_status_critical_disk_evaluates_degraded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Disk usage > 80% triggers critical warning and degraded health."""
    db_path = tmp_path / "state" / "disk.duckdb"
    curated_dir = tmp_path / "curated"

    candles = [_make_candle(0)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.set_watermark(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), run_id="run-init")

    def mock_clock() -> datetime:
        return datetime(2026, 1, 1, 0, 30, tzinfo=UTC)

    # Mock shutil.disk_usage to return 85% used
    mock_stat = MagicMock()
    mock_stat.total = 100 * (1024**3)
    mock_stat.used = 85 * (1024**3)
    mock_stat.free = 15 * (1024**3)

    with patch("shutil.disk_usage", return_value=mock_stat):
        exit_code = main(
            [
                "status",
                "--db-path",
                str(db_path),
                "--curated-dir",
                str(curated_dir),
                "--check",
            ],
            clock=mock_clock,
        )
        assert exit_code == 1

        captured = capsys.readouterr()
        res = json.loads(captured.out)
        assert res["is_healthy"] is False
        assert res["disk"]["disk_critical"] is True
        assert res["disk"]["percent_used"] == 85.0


def test_status_active_lock_evaluates_degraded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """An active run lock evaluates to degraded."""
    db_path = tmp_path / "state" / "locked.duckdb"
    curated_dir = tmp_path / "curated"

    candles = [_make_candle(0)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.set_watermark(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), run_id="run-init")
        manager.acquire_lock(
            run_id="run-active-lock",
            mode="incremental",
            started_at_utc=datetime(2026, 1, 1, 0, 15, tzinfo=UTC),
        )

    def mock_clock() -> datetime:
        return datetime(2026, 1, 1, 0, 30, tzinfo=UTC)

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--check",
        ],
        clock=mock_clock,
    )
    assert exit_code == 1

    captured = capsys.readouterr()
    res = json.loads(captured.out)
    assert res["is_healthy"] is False
    assert res["is_locked"] is True


def test_status_format_text_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Format text produces readable terminal summary with required sections."""
    db_path = tmp_path / "state" / "text.duckdb"
    curated_dir = tmp_path / "curated"

    candles = [_make_candle(0), _make_candle(1)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.set_watermark(datetime(2026, 1, 1, 1, 0, tzinfo=UTC), run_id="run-text")

    def mock_clock() -> datetime:
        return datetime(2026, 1, 1, 1, 30, tzinfo=UTC)

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
            "--format",
            "text",
        ],
        clock=mock_clock,
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    out = captured.out

    assert "Bitcoin Data Platform - System Status & Health" in out
    assert "Health Status      : HEALTHY" in out
    assert "[ Watermark & Freshness ]" in out
    assert "[ Curated Dataset Stats ]" in out
    assert "[ Storage & Disk Utilization ]" in out
    assert "2026-01-01T01:00:00Z" in out
