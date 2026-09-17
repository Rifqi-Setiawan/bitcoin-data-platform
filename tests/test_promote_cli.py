"""CLI tests for promote and query commands."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.storage.raw_writer import create_raw_envelope, write_raw_envelope


def _fixed_clock() -> datetime:
    return datetime(2026, 1, 2, 0, 0, tzinfo=UTC)


def _write_sample_raw_envelope(
    raw_dir: Path,
    run_id: str = "run-sample-1",
    payload: list | None = None,
) -> Path:
    if payload is None:
        payload = [
            [1767225600, 94800, 96000, 95200, 95800, 10.5],
            [1767229200, 95000, 96500, 95500, 96200, 15.0],
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


def test_full_promote_pipeline_succeeds(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """35. Full promote pipeline succeeds with valid raw data."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    _write_sample_raw_envelope(raw_dir, run_id="run-p35")

    exit_code = main(
        [
            "promote",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ],
        clock=_fixed_clock,
    )

    captured = capsys.readouterr()
    assert exit_code == 0

    summary = json.loads(captured.out)
    assert summary["status"] == "success"
    assert summary["raw_envelopes_read"] == 1
    assert summary["rows_promoted"] == 2
    assert summary["partitions_written"] == 1

    # Verify Parquet file was written
    parquet_path = (
        curated_dir
        / "market"
        / "candles_hourly"
        / "source=coinbase_exchange"
        / "year=2026"
        / "data.parquet"
    )
    assert parquet_path.exists()

    # Verify DuckDB views are queryable via CLI query command
    exit_query = main(
        [
            "query",
            "--db-path",
            str(db_path),
            "--sql",
            "SELECT COUNT(*) AS cnt FROM fact_market_candle_hourly",
        ]
    )
    assert exit_query == 0
    query_captured = capsys.readouterr()
    query_results = json.loads(query_captured.out)
    assert query_results[0]["cnt"] == 2


def test_empty_raw_directory_exits_appropriate_message(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """36. Empty raw directory exits with appropriate message."""
    raw_dir = tmp_path / "raw_empty"
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    exit_code = main(
        [
            "promote",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ],
        clock=_fixed_clock,
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "No raw envelopes found" in captured.err

    summary = json.loads(captured.out)
    assert summary["status"] == "success"
    assert summary["rows_promoted"] == 0
    assert summary["raw_envelopes_read"] == 0


def test_quality_failure_exits_4(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """37. Quality failure exits 4."""
    raw_dir = tmp_path / "raw_bad"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    # Candle with negative price
    bad_payload = [
        [1767225600, -500, 96000, 95200, 95800, 10.5],
    ]
    _write_sample_raw_envelope(raw_dir, run_id="run-bad", payload=bad_payload)

    exit_code = main(
        [
            "promote",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ],
        clock=_fixed_clock,
    )

    captured = capsys.readouterr()
    assert exit_code == 4
    assert "error: quality failure" in captured.err


def test_promote_summary_includes_correct_counts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """38. Promote summary includes correct counts."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    _write_sample_raw_envelope(raw_dir, run_id="run-counts")

    exit_code = main(
        [
            "promote",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ],
        clock=_fixed_clock,
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    summary = json.loads(captured.out)

    assert "run_id" in summary
    assert summary["status"] == "success"
    assert summary["raw_envelopes_read"] == 1
    assert summary["rows_promoted"] == 2
    assert summary["partitions_written"] == 1
    assert summary["curated_dir"] == str(curated_dir)
    assert summary["db_path"] == str(db_path)


def test_repromote_is_idempotent(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """39. Re-promote (idempotent) produces same result."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    _write_sample_raw_envelope(raw_dir, run_id="run-idem-1")

    # First promote
    code1 = main(
        [
            "promote",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ],
        clock=_fixed_clock,
    )
    assert code1 == 0
    captured1 = capsys.readouterr()
    summary1 = json.loads(captured1.out)

    # Second promote (idempotent rerun)
    code2 = main(
        [
            "promote",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ],
        clock=_fixed_clock,
    )
    assert code2 == 0
    captured2 = capsys.readouterr()
    summary2 = json.loads(captured2.out)

    assert summary1["rows_promoted"] == summary2["rows_promoted"]
    assert summary1["partitions_written"] == summary2["partitions_written"]

    # Query to verify row count did not double
    exit_query = main(
        [
            "query",
            "--db-path",
            str(db_path),
            "--sql",
            "SELECT COUNT(*) AS cnt FROM fact_market_candle_hourly",
        ]
    )
    assert exit_query == 0
    query_captured = capsys.readouterr()
    query_results = json.loads(query_captured.out)
    assert query_results[0]["cnt"] == 2
