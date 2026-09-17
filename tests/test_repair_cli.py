"""CLI tests for repair command rebuilding curated layer and clearing stale locks."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.raw_writer import create_raw_envelope, write_raw_envelope


def _write_envelope(
    raw_dir: Path,
    run_id: str = "run-repair-env",
    payload: list[list[Any]] | None = None,
) -> Path:
    if payload is None:
        payload = [
            [1767225600, 94000, 96000, 95000, 95500, 10],  # 2026-01-01T00:00:00Z
            [1767229200, 95000, 97000, 95500, 96500, 12],  # 2026-01-01T01:00:00Z
        ]
    env = create_raw_envelope(
        run_id=run_id,
        product_id="BTC-USD",
        granularity_seconds=3600,
        start_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 2, 0, tzinfo=UTC),
        retrieved_at_utc=datetime(2026, 1, 1, 3, 0, tzinfo=UTC),
        http_status=200,
        payload=payload,
    )
    return write_raw_envelope(raw_dir, env)


def test_repair_rebuilds_from_raw_successfully(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """25. Repair rebuilds from raw successfully."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    _write_envelope(raw_dir, run_id="env-1")

    # Initial promote
    exit_promote = main(
        [
            "promote",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ]
    )
    assert exit_promote == 0
    capsys.readouterr()

    # Place an unwanted orphan file in curated directory
    orphan_file = (
        curated_dir
        / "market"
        / "candles_hourly"
        / "source=coinbase_exchange"
        / "year=2026"
        / "orphan.parquet"
    )
    orphan_file.write_text("fake corrupted data")
    assert orphan_file.exists()

    # Run repair
    exit_repair = main(
        [
            "repair",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ]
    )
    assert exit_repair == 0

    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["status"] == "success"
    assert summary["mode"] == "repair"
    assert summary["rows_promoted"] == 2
    assert summary["partitions_written"] == 1

    # Verify orphan was unlinked/cleaned up
    assert not orphan_file.exists()

    # Verify views work
    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        res = manager.execute_query("SELECT COUNT(*) AS cnt FROM fact_market_candle_hourly")
        assert res[0]["cnt"] == 2


def test_repair_does_not_lower_watermark(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """26. Repair does not lower watermark."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    # Seed data up to 2026-01-01T01:00:00Z
    _write_envelope(raw_dir, run_id="env-early")

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    high_watermark = datetime(2026, 1, 20, 0, 0, tzinfo=UTC)
    with manager:
        manager.initialize()
        manager.set_watermark(high_watermark, run_id="run-high-prior")

    # Run repair
    exit_repair = main(
        [
            "repair",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ]
    )
    assert exit_repair == 0
    capsys.readouterr()

    # Watermark must remain at high_watermark
    with manager:
        wm = manager.get_watermark()
        assert wm == high_watermark


def test_repair_with_force_clears_stale_lock(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """27. Repair with --force clears stale lock and proceeds."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    _write_envelope(raw_dir)

    now = datetime(2026, 1, 1, 15, 0, tzinfo=UTC)
    stale_time = now - timedelta(hours=2)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.acquire_lock(
            run_id="abandoned-stale-run",
            mode="incremental",
            started_at_utc=stale_time,
        )
        assert manager.check_lock() is True

    # Repair without --force should fail with exit 6
    code_blocked = main(
        [
            "repair",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ],
        clock=lambda: now,
    )
    assert code_blocked == 6
    captured_blocked = capsys.readouterr()
    assert "concurrent run detected" in captured_blocked.err.lower()

    # Repair with --force should succeed
    code_force = main(
        [
            "repair",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--force",
        ],
        clock=lambda: now,
    )
    assert code_force == 0
    captured_force = capsys.readouterr()
    summary = json.loads(captured_force.out)
    assert summary["status"] == "success"

    # Verify lock is freed
    with manager:
        assert manager.check_lock() is False


def test_repair_produces_same_result_as_fresh_promote(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """28. Repair produces same result as fresh promote."""
    # Setup Promote tree
    p_raw = tmp_path / "promote_tree" / "raw"
    p_curated = tmp_path / "promote_tree" / "curated"
    p_db = tmp_path / "promote_tree" / "platform.duckdb"
    _write_envelope(p_raw, run_id="env-p")

    # Setup Repair tree
    r_raw = tmp_path / "repair_tree" / "raw"
    r_curated = tmp_path / "repair_tree" / "curated"
    r_db = tmp_path / "repair_tree" / "platform.duckdb"
    _write_envelope(r_raw, run_id="env-p")

    # 1. Fresh promote
    assert (
        main(
            [
                "promote",
                "--raw-dir",
                str(p_raw),
                "--curated-dir",
                str(p_curated),
                "--db-path",
                str(p_db),
            ]
        )
        == 0
    )
    capsys.readouterr()

    # 2. Promote then repair
    assert (
        main(
            [
                "promote",
                "--raw-dir",
                str(r_raw),
                "--curated-dir",
                str(r_curated),
                "--db-path",
                str(r_db),
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert (
        main(
            [
                "repair",
                "--raw-dir",
                str(r_raw),
                "--curated-dir",
                str(r_curated),
                "--db-path",
                str(r_db),
            ]
        )
        == 0
    )
    capsys.readouterr()

    # Compare query outputs from both databases
    mgr_p = DuckDBManager(db_path=p_db, curated_dir=p_curated)
    mgr_r = DuckDBManager(db_path=r_db, curated_dir=r_curated)

    with mgr_p, mgr_r:
        query = (
            "SELECT candle_start_utc, open, high, low, close, volume_base "
            "FROM fact_market_candle_hourly ORDER BY candle_start_utc"
        )
        rows_promote = mgr_p.execute_query(query)
        rows_repair = mgr_r.execute_query(query)

        assert len(rows_promote) == 2
        assert rows_promote == rows_repair
