"""CLI tests for status command inspecting operational visibility and state."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.parquet_writer import write_parquet_partitions
from bitcoin_data_platform.storage.raw_writer import create_raw_envelope, write_raw_envelope
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _make_candle(hour: int, date_str: str = "2026-01-01") -> NormalizedCandle:
    dt = datetime.fromisoformat(f"{date_str}T{hour:02d}:00:00+00:00")
    return NormalizedCandle(
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        candle_start_utc=dt,
        open=Decimal("95000"),
        high=Decimal("96000"),
        low=Decimal("94500"),
        close=Decimal("95800"),
        volume_base=Decimal("10"),
        ingested_at_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        source_run_id="run-status-candle",
    )


def test_status_with_no_data_shows_empty_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """20. Status with no data shows empty state."""
    db_path = tmp_path / "state" / "empty.duckdb"
    curated_dir = tmp_path / "curated"

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    res = json.loads(captured.out)

    assert res["watermark_utc"] is None
    assert res["watermark_age_hours"] is None
    assert res["last_run"] is None
    assert res["last_success"] is None
    assert res["last_failure"] is None
    assert res["curated_stats"]["total_rows"] == 0
    assert res["curated_stats"]["partitions"] == 0
    assert res["curated_stats"]["total_size_bytes"] == 0
    assert res["curated_stats"]["min_candle_utc"] is None
    assert res["curated_stats"]["max_candle_utc"] is None
    assert res["gaps"] == []
    assert res["is_locked"] is False


def test_status_with_data_shows_correct_watermark_and_stats(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """21. Status with data shows correct watermark and stats."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    # Write 24 hours of raw data
    payload = []
    for h in range(24):
        epoch = int(datetime(2026, 1, 1, h, 0, tzinfo=UTC).timestamp())
        payload.append([epoch, 94000, 96000, 95000, 95500, 10])

    env = create_raw_envelope(
        run_id="run-day-1",
        product_id="BTC-USD",
        granularity_seconds=3600,
        start_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        retrieved_at_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        http_status=200,
        payload=payload,
    )
    write_raw_envelope(raw_dir, env)

    # Promote to populate Parquet, DuckDB, and watermark
    code = main(
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
    assert code == 0
    capsys.readouterr()

    # Now run status with clock 12 hours after latest candle:
    # (2026-01-01T23:00 + 12h = 2026-01-02T11:00:00Z)
    def fixed_clock() -> datetime:
        return datetime(2026, 1, 2, 11, 0, tzinfo=UTC)

    status_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
        ],
        clock=fixed_clock,
    )
    assert status_code == 0

    captured = capsys.readouterr()
    res = json.loads(captured.out)

    assert res["watermark_utc"] == "2026-01-01T23:00:00Z"
    assert res["watermark_age_hours"] == 12.0
    assert res["last_run"] is not None
    assert res["last_run"]["mode"] == "promote"
    assert res["last_run"]["rows_promoted"] == 24
    assert res["last_success"] is not None
    assert res["curated_stats"]["total_rows"] == 24
    assert res["curated_stats"]["partitions"] == 1
    assert res["curated_stats"]["total_size_bytes"] > 0
    assert res["curated_stats"]["min_candle_utc"] == "2026-01-01T00:00:00Z"
    assert res["curated_stats"]["max_candle_utc"] == "2026-01-01T23:00:00Z"
    assert res["gaps"] == []
    assert res["is_locked"] is False


def test_status_detects_gaps_in_hourly_data(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """22. Status detects gaps in hourly data."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    # Write candles for hour 0 and hour 3 (missing hours 1 and 2)
    candles = [_make_candle(0), _make_candle(3)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    res = json.loads(captured.out)

    gaps = res["gaps"]
    assert len(gaps) == 1
    assert gaps[0]["missing_hours"] == 2
    assert gaps[0]["start_utc"] == "2026-01-01T01:00:00Z"
    assert gaps[0]["end_utc"] == "2026-01-01T03:00:00Z"


def test_status_shows_last_failure(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """23. Status shows last failure."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.record_run(
            run_id="run-failed-status-check",
            mode="incremental",
            started_at_utc=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
            completed_at_utc=datetime(2026, 1, 1, 10, 1, tzinfo=UTC),
            status="FAILED",
            error_message="Coinbase HTTP 503",
        )

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    res = json.loads(captured.out)

    assert res["last_failure"] is not None
    assert res["last_failure"]["run_id"] == "run-failed-status-check"
    assert res["last_failure"]["completed_at_utc"] == "2026-01-01T10:01:00Z"


def test_status_shows_lock_state(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """24. Status shows lock state accurately when locked and unlocked."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.acquire_lock(
            run_id="run-locked-now",
            mode="incremental",
            started_at_utc=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
        )

    exit_code = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    res = json.loads(captured.out)
    assert res["is_locked"] is True

    # Release lock and check again
    with manager:
        manager.release_lock(run_id="run-locked-now", status="SUCCEEDED")

    exit_code2 = main(
        [
            "status",
            "--db-path",
            str(db_path),
            "--curated-dir",
            str(curated_dir),
        ]
    )
    assert exit_code2 == 0
    captured2 = capsys.readouterr()
    res2 = json.loads(captured2.out)
    assert res2["is_locked"] is False
