"""Tests for cross-domain conformed modeling and DuckDB views."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.network_parquet_writer import (
    NETWORK_PARQUET_SCHEMA,
    read_network_partition_metrics,
    write_network_parquet_partitions,
)
from bitcoin_data_platform.storage.parquet_writer import write_parquet_partitions
from bitcoin_data_platform.transforms.network_normalizer import NormalizedNetworkMetric
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _make_metric(
    day: int,
    year: int = 2026,
    tx: int = 350000,
    adr: int = 850000,
    run_id: str = "run-1",
) -> NormalizedNetworkMetric:
    return NormalizedNetworkMetric(
        source="coin_metrics",
        asset="btc",
        metric_date_utc=datetime(year, 1, day, 0, 0, tzinfo=UTC),
        transaction_count=tx,
        active_addresses_count=adr,
        ingested_at_utc=datetime(year, 1, day + 1, 0, 0, tzinfo=UTC),
        source_run_id=run_id,
    )


def _make_candle(
    day: int,
    hour: int,
    price: Decimal = Decimal("95000.0"),
    year: int = 2026,
) -> NormalizedCandle:
    return NormalizedCandle(
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        candle_start_utc=datetime(year, 1, day, hour, 0, tzinfo=UTC),
        open=price,
        high=price + Decimal("100.0"),
        low=price - Decimal("50.0"),
        close=price + Decimal("25.0"),
        volume_base=Decimal("10.5"),
        ingested_at_utc=datetime(year, 1, day, hour, 5, tzinfo=UTC),
        source_run_id="cb-run-1",
    )


def test_write_and_merge_network_parquet_partitions(tmp_path: Path) -> None:
    """1. Write network metrics to annual Parquet partitions and verify atomic merge."""
    curated_dir = tmp_path / "curated"

    m1 = _make_metric(1, tx=300000)
    m2 = _make_metric(2, tx=320000)

    results = write_network_parquet_partitions([m1, m2], curated_dir=curated_dir)
    assert len(results) == 1
    assert results[0].year == 2026
    assert results[0].row_count == 2
    assert results[0].is_new is True

    target_file = (
        curated_dir
        / "onchain"
        / "network_metrics_daily"
        / "source=coin_metrics"
        / "year=2026"
        / "data.parquet"
    )
    assert target_file.exists()

    # Verify PyArrow schema
    table = pq.read_table(target_file)
    assert table.schema.equals(NETWORK_PARQUET_SCHEMA)

    # Merge an updated record for day 2 with higher run_id, and a new record for day 3
    m2_updated = _make_metric(2, tx=325000, run_id="run-2")
    m3 = _make_metric(3, tx=340000)

    merge_results = write_network_parquet_partitions([m2_updated, m3], curated_dir=curated_dir)
    assert len(merge_results) == 1
    assert merge_results[0].row_count == 3
    assert merge_results[0].is_new is False

    read_back = read_network_partition_metrics(target_file)
    assert len(read_back) == 3
    assert read_back[1].transaction_count == 325000
    assert read_back[1].source_run_id == "run-2"


def test_fact_network_metrics_daily_view(tmp_path: Path) -> None:
    """2. Verify fact_network_metrics_daily view over Parquet partitions."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "test.duckdb"

    # With no files present, fallback view should exist with 0 rows
    db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    db_manager.initialize()

    rows = db_manager.execute_query("SELECT COUNT(*) AS c FROM fact_network_metrics_daily;")
    assert rows[0]["c"] == 0

    # Write Parquet and refresh view
    write_network_parquet_partitions([_make_metric(1), _make_metric(2)], curated_dir=curated_dir)
    db_manager.create_network_fact_view()

    res = db_manager.execute_query(
        "SELECT metric_date_utc, transaction_count, active_addresses_count "
        "FROM fact_network_metrics_daily ORDER BY metric_date_utc;"
    )
    assert len(res) == 2
    assert res[0]["transaction_count"] == 350000
    assert res[1]["active_addresses_count"] == 850000


def test_cross_domain_mart_join_and_metrics(tmp_path: Path) -> None:
    """3. Verify mart_btc_market_and_network_daily conformed join and tx_per_active_address."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "test.duckdb"

    # Populate 24 hours of market candles for 2026-01-01 (complete day)
    market_candles_day1 = [_make_candle(1, h) for h in range(24)]
    # Populate only 12 hours for 2026-01-02 (incomplete day)
    market_candles_day2 = [_make_candle(2, h) for h in range(12)]

    write_parquet_partitions(market_candles_day1 + market_candles_day2, curated_dir=curated_dir)

    # Populate network metrics for 2026-01-01 and 2026-01-03 (day 3 has network but no market)
    network_metrics = [
        _make_metric(1, tx=350000, adr=700000),  # tx_per_address = 0.5
        _make_metric(3, tx=420000, adr=600000),  # tx_per_address = 0.7
    ]
    write_network_parquet_partitions(network_metrics, curated_dir=curated_dir)

    db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    db_manager.initialize()

    query = """
    SELECT
        STRFTIME(trade_date_utc, '%Y-%m-%d') AS trade_date,
        market_open_usd,
        market_close_usd,
        market_observed_hour_count,
        is_market_day_complete,
        transaction_count,
        active_addresses_count,
        tx_per_active_address
    FROM mart_btc_market_and_network_daily
    ORDER BY trade_date_utc;
    """
    rows = db_manager.execute_query(query)
    assert len(rows) == 3

    # Day 1: Both market & network exist
    r1 = rows[0]
    assert r1["trade_date"] == "2026-01-01"
    assert r1["market_observed_hour_count"] == 24
    assert r1["is_market_day_complete"] is True
    assert r1["transaction_count"] == 350000
    assert r1["active_addresses_count"] == 700000
    assert r1["tx_per_active_address"] == 0.5

    # Day 2: Market exists (incomplete), network missing (NULL)
    r2 = rows[1]
    assert r2["trade_date"] == "2026-01-02"
    assert r2["market_observed_hour_count"] == 12
    assert r2["is_market_day_complete"] is False
    assert r2["transaction_count"] is None
    assert r2["tx_per_active_address"] is None

    # Day 3: Network exists, market missing (NULL)
    r3 = rows[2]
    assert r3["trade_date"] == "2026-01-03"
    assert r3["market_observed_hour_count"] is None
    assert r3["is_market_day_complete"] is None
    assert r3["transaction_count"] == 420000
    assert r3["tx_per_active_address"] == 0.7


def test_independent_watermarks(tmp_path: Path) -> None:
    """4. Watermarks for btc_usd_hourly and coin_metrics_daily operate independently."""
    db_path = tmp_path / "state" / "test.duckdb"
    db_manager = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    db_manager.initialize()

    t_market = datetime(2026, 1, 1, 18, 0, tzinfo=UTC)
    t_network = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)

    # Set market watermark
    db_manager.set_watermark(t_market, run_id="run-m1", pipeline_id="btc_usd_hourly")
    # Set network watermark
    db_manager.set_watermark(t_network, run_id="run-n1", pipeline_id="coin_metrics_daily")

    assert db_manager.get_watermark("btc_usd_hourly") == t_market
    assert db_manager.get_watermark("coin_metrics_daily") == t_network
