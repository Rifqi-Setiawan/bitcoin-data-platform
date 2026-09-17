"""Tests for DuckDB manager, views, and analytical modeling."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.parquet_writer import write_parquet_partitions
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _make_candle(
    year: int = 2026,
    month: int = 1,
    day: int = 1,
    hour: int = 0,
    open_p: str = "95000",
    high_p: str = "96000",
    low_p: str = "94000",
    close_p: str = "95500",
    vol: str = "10.0",
) -> NormalizedCandle:
    ts = datetime(year, month, day, hour, 0, tzinfo=UTC)
    return NormalizedCandle(
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        candle_start_utc=ts,
        open=Decimal(open_p),
        high=Decimal(high_p),
        low=Decimal(low_p),
        close=Decimal(close_p),
        volume_base=Decimal(vol),
        ingested_at_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        source_run_id="run-duckdb-test",
    )


def test_initialize_creates_views_and_run_metadata(tmp_path: Path) -> None:
    """29. Initialize creates views and run_metadata table."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()

        # Verify run_metadata table exists
        meta_res = manager.execute_query("SELECT COUNT(*) AS cnt FROM run_metadata")
        assert meta_res[0]["cnt"] == 0

        # Verify hourly fact view exists
        fact_res = manager.execute_query("SELECT COUNT(*) AS cnt FROM fact_market_candle_hourly")
        assert fact_res[0]["cnt"] == 0

        # Verify daily mart view exists
        mart_res = manager.execute_query("SELECT COUNT(*) AS cnt FROM mart_btc_usd_daily")
        assert mart_res[0]["cnt"] == 0


def test_fact_hourly_view_reads_parquet_correctly(tmp_path: Path) -> None:
    """30. fact_market_candle_hourly view reads Parquet correctly."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    candles = [
        _make_candle(
            hour=0, open_p="95000", high_p="96000", low_p="94500", close_p="95800", vol="12.5"
        ),
        _make_candle(
            hour=1, open_p="95800", high_p="96500", low_p="95200", close_p="96100", vol="15.0"
        ),
    ]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        rows = manager.execute_query(
            "SELECT source, product_id, candle_start_utc, open, close, volume_base "
            "FROM fact_market_candle_hourly ORDER BY candle_start_utc"
        )
        assert len(rows) == 2
        assert rows[0]["source"] == "coinbase_exchange"
        assert rows[0]["product_id"] == "BTC-USD"
        assert rows[0]["open"] == Decimal("95000.000000000000000000")
        assert rows[0]["close"] == Decimal("95800.000000000000000000")
        assert rows[0]["volume_base"] == Decimal("12.500000000000000000")


def test_mart_btc_usd_daily_aggregation(tmp_path: Path) -> None:
    """31. mart_btc_usd_daily aggregation produces correct OHLCV."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    # Full 24 hours
    candles = []
    for h in range(24):
        # Open of h=0 is 90000, Close of h=23 is 98000
        # High reaches 100000 at h=12, Low reaches 89000 at h=3
        open_val = "90000" if h == 0 else "91000"
        close_val = "98000" if h == 23 else "92000"
        high_val = "100000" if h == 12 else "93000"
        low_val = "89000" if h == 3 else "89500"
        vol_val = "10.0"
        candles.append(
            _make_candle(
                hour=h,
                open_p=open_val,
                high_p=high_val,
                low_p=low_val,
                close_p=close_val,
                vol=vol_val,
            )
        )

    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        daily_rows = manager.execute_query("SELECT * FROM mart_btc_usd_daily")
        assert len(daily_rows) == 1
        d = daily_rows[0]

        assert d["source"] == "coinbase_exchange"
        assert d["product_id"] == "BTC-USD"
        assert d["open"] == Decimal("90000.000000000000000000")
        assert d["high"] == Decimal("100000.000000000000000000")
        assert d["low"] == Decimal("89000.000000000000000000")
        assert d["close"] == Decimal("98000.000000000000000000")
        assert d["volume_base"] == Decimal("240.000000000000000000")
        assert d["observed_hour_count"] == 24
        assert d["is_complete"] is True


def test_partial_day_has_is_complete_false(tmp_path: Path) -> None:
    """32. Partial day has is_complete=false."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    # Only 10 hours
    candles = [_make_candle(hour=h) for h in range(10)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        daily_rows = manager.execute_query("SELECT * FROM mart_btc_usd_daily")
        assert len(daily_rows) == 1
        d = daily_rows[0]
        assert d["observed_hour_count"] == 10
        assert d["is_complete"] is False


def test_record_run_metadata_and_query_back(tmp_path: Path) -> None:
    """33. Record run metadata and query it back."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    start_t = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    end_t = datetime(2026, 1, 1, 10, 5, tzinfo=UTC)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.record_run(
            run_id="run-meta-123",
            mode="backfill_promote",
            started_at_utc=start_t,
            completed_at_utc=end_t,
            status="success",
            rows_promoted=100,
            partitions_written=1,
            raw_envelopes_read=4,
            error_message=None,
        )

        records = manager.execute_query("SELECT * FROM run_metadata WHERE run_id = 'run-meta-123'")
        assert len(records) == 1
        rec = records[0]
        assert rec["run_id"] == "run-meta-123"
        assert rec["mode"] == "backfill_promote"
        assert rec["status"] == "success"
        assert rec["rows_promoted"] == 100
        assert rec["partitions_written"] == 1
        assert rec["raw_envelopes_read"] == 4
        assert rec["error_message"] is None


def test_query_command_returns_correct_json(tmp_path: Path) -> None:
    """34. Query command returns correct JSON."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    candles = [_make_candle(hour=0)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        results = manager.execute_query(
            "SELECT source, product_id, granularity_seconds FROM fact_market_candle_hourly"
        )
        # Ensure serialization to JSON succeeds
        json_output = json.dumps(results, default=str)
        parsed = json.loads(json_output)
        assert len(parsed) == 1
        assert parsed[0]["source"] == "coinbase_exchange"
        assert parsed[0]["product_id"] == "BTC-USD"
        assert parsed[0]["granularity_seconds"] == 3600
