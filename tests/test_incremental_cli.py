"""CLI tests for incremental command with watermark-based loading and recovery."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.sources.coinbase_client import CoinbaseClient
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.raw_writer import create_raw_envelope, write_raw_envelope


def _make_mock_client(
    payload: list[list[Any]] | None = None,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
) -> CoinbaseClient:
    candles = (
        payload
        if payload is not None
        else [
            [1767225600, 95000, 96000, 95200, 96000, 10],  # 2026-01-01T00:00:00Z
            [1767229200, 96000, 97000, 96000, 96800, 15],  # 2026-01-01T01:00:00Z
        ]
    )
    hdrs = headers or {"cb-request-id": "cb-test-inc-123"}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=candles, headers=hdrs)

    transport = httpx.MockTransport(handler)
    return CoinbaseClient(transport=transport, min_request_interval_seconds=0.0)


def _seed_initial_watermark(
    raw_dir: Path,
    curated_dir: Path,
    db_path: Path,
    watermark_ts: datetime,
) -> None:
    """Helper to seed an initial valid watermark and curated Parquet state."""
    epoch_sec = int(watermark_ts.timestamp())
    payload = [
        [epoch_sec, 95000, 96000, 95200, 96000, 10],
    ]
    env = create_raw_envelope(
        run_id="seed-run",
        product_id="BTC-USD",
        granularity_seconds=3600,
        start_utc=watermark_ts,
        end_utc=datetime.fromtimestamp(epoch_sec + 3600, tz=UTC),
        retrieved_at_utc=datetime.fromtimestamp(epoch_sec + 3600, tz=UTC),
        http_status=200,
        payload=payload,
    )
    write_raw_envelope(raw_dir, env)
    # Run promote to establish initial partition and watermark
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


def test_incremental_succeeds_with_valid_watermark(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """12. Incremental succeeds with valid watermark."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    seed_ts = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    _seed_initial_watermark(raw_dir, curated_dir, db_path, seed_ts)
    capsys.readouterr()  # Clear promote output

    # Mock new candles up to 02:00
    new_candles = [
        [1767225600, 94000, 96500, 95000, 96000, 10],  # 00:00
        [1767229200, 95500, 97000, 96000, 96800, 15],  # 01:00
        [1767232800, 96000, 97500, 96500, 97200, 20],  # 02:00
    ]
    client = _make_mock_client(payload=new_candles)

    def clock() -> datetime:
        return datetime(2026, 1, 1, 4, 0, tzinfo=UTC)

    code = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--overlap-hours",
            "1",
        ],
        clock=clock,
        client=client,
    )

    assert code == 0
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["status"] == "success"
    assert summary["mode"] == "incremental"
    assert summary["rows_promoted"] == 3


def test_incremental_fails_with_no_watermark_exit_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """13. Incremental fails with no watermark (exit 2)."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    code = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ]
    )

    assert code == 2
    captured = capsys.readouterr()
    assert "no watermark found" in captured.err.lower()

    # Lock must be released
    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    assert manager.check_lock() is False


def test_incremental_nothing_to_fetch_exits_0(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """14. Incremental with nothing to fetch (fresh data) exits 0."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    seed_ts = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    _seed_initial_watermark(raw_dir, curated_dir, db_path, seed_ts)
    capsys.readouterr()

    # Clock at exactly 10:15 UTC, with 0 overlap hours -> start (10:00) >= end (10:00)
    def clock() -> datetime:
        return datetime(2026, 1, 1, 10, 15, tzinfo=UTC)

    code = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--overlap-hours",
            "0",
        ],
        clock=clock,
    )

    assert code == 0
    captured = capsys.readouterr()
    assert "data is fresh" in (captured.out + captured.err).lower()
    summary = json.loads(captured.out)
    assert summary["status"] == "success"
    assert summary["windows_planned"] == 0


def test_incremental_advances_watermark_correctly(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """15. Incremental advances watermark correctly."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    t0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    _seed_initial_watermark(raw_dir, curated_dir, db_path, t0)
    capsys.readouterr()

    # Coinbase returns data up to 03:00
    later_candles = [
        [1767225600, 94000, 96500, 95000, 96000, 10],  # 00:00
        [1767229200, 95500, 97000, 96000, 96800, 15],  # 01:00
        [1767232800, 96000, 97500, 96500, 97200, 20],  # 02:00
        [1767236400, 96500, 98000, 97000, 97800, 25],  # 03:00
    ]
    client = _make_mock_client(payload=later_candles)

    def clock() -> datetime:
        return datetime(2026, 1, 1, 5, 0, tzinfo=UTC)

    code = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--overlap-hours",
            "1",
        ],
        clock=clock,
        client=client,
    )
    assert code == 0

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        wm = manager.get_watermark()
        assert wm == datetime(2026, 1, 1, 3, 0, tzinfo=UTC)


def test_incremental_with_overlap_fetches_correctly(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """16. Incremental with overlap fetches correctly."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    # Watermark is at 2026-01-03T00:00:00Z
    t_wm = datetime(2026, 1, 3, 0, 0, tzinfo=UTC)
    _seed_initial_watermark(raw_dir, curated_dir, db_path, t_wm)
    capsys.readouterr()

    captured_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(
            200,
            json=[[1767402000, 95000, 96000, 95200, 96000, 10]],  # 2026-01-03T01:00:00Z
            headers={"cb-request-id": "cb-overlap-test"},
        )

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(transport=transport, min_request_interval_seconds=0.0)

    def clock() -> datetime:
        return datetime(2026, 1, 3, 2, 0, tzinfo=UTC)

    code = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--overlap-hours",
            "48",
        ],
        clock=clock,
        client=client,
    )
    assert code == 0

    # Overlap 48 hours means window start should be 2026-01-01T00:00:00Z
    assert len(captured_requests) > 0
    first_url = str(captured_requests[0].url)
    assert (
        "start=2026-01-01T00%3A00%3A00Z" in first_url or "start=2026-01-01T00:00:00Z" in first_url
    )


def test_incremental_rerun_deduplicates_correctly(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """17. Incremental re-run deduplicates correctly without inflating row count."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    t0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    _seed_initial_watermark(raw_dir, curated_dir, db_path, t0)
    capsys.readouterr()

    candles = [
        [1767225600, 95000, 96000, 95200, 96000, 10],  # 00:00
        [1767229200, 96000, 97000, 96000, 96800, 15],  # 01:00
    ]
    client = _make_mock_client(payload=candles)

    def clock() -> datetime:
        return datetime(2026, 1, 1, 3, 0, tzinfo=UTC)

    # First incremental run
    code1 = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--overlap-hours",
            "1",
        ],
        clock=clock,
        client=client,
    )
    assert code1 == 0
    capsys.readouterr()

    # Second incremental run with same data
    code2 = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--overlap-hours",
            "1",
        ],
        clock=clock,
        client=client,
    )
    assert code2 == 0
    capsys.readouterr()

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        res = manager.execute_query("SELECT COUNT(*) AS cnt FROM fact_market_candle_hourly")
        assert res[0]["cnt"] == 2


def test_incremental_source_unavailable_exits_3(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """18. Source unavailable exits 3."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    t0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    _seed_initial_watermark(raw_dir, curated_dir, db_path, t0)
    capsys.readouterr()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="Service Unavailable")

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        max_retries=1,
        base_backoff_seconds=0.001,
        min_request_interval_seconds=0.0,
        jitter=False,
    )

    def clock() -> datetime:
        return datetime(2026, 1, 1, 5, 0, tzinfo=UTC)

    code = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--overlap-hours",
            "1",
        ],
        clock=clock,
        client=client,
    )

    assert code == 3
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["status"] == "failure"
    assert summary["windows_failed"] == 1

    # Lock must be released
    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    assert manager.check_lock() is False


def test_incremental_summary_includes_old_new_watermark(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """19. Run summary includes old/new watermark."""
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    t0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    _seed_initial_watermark(raw_dir, curated_dir, db_path, t0)
    capsys.readouterr()

    candles = [
        [1767225600, 94000, 96500, 95000, 96000, 10],  # 00:00
        [1767229200, 95500, 97000, 96000, 96800, 15],  # 01:00
        [1767232800, 96000, 97500, 96500, 97200, 20],  # 02:00
    ]
    client = _make_mock_client(payload=candles)

    def clock() -> datetime:
        return datetime(2026, 1, 1, 4, 0, tzinfo=UTC)

    code = main(
        [
            "incremental",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
            "--overlap-hours",
            "1",
        ],
        clock=clock,
        client=client,
    )
    assert code == 0

    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["old_watermark_utc"] == "2026-01-01T00:00:00Z"
    assert summary["new_watermark_utc"] == "2026-01-01T02:00:00Z"
