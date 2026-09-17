"""Tests for version-controlled query catalog in queries/*.sql."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from bitcoin_data_platform.serving.query_service import QueryService
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager, DuckDBManagerError
from bitcoin_data_platform.storage.network_parquet_writer import (
    NormalizedNetworkMetric,
    write_network_parquet_partitions,
)
from bitcoin_data_platform.storage.parquet_writer import (
    NormalizedCandle,
    write_parquet_partitions,
)

QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"


def test_query_catalog_files_exist() -> None:
    """1. Verify queries directory and required SQL files exist."""
    assert QUERIES_DIR.is_dir()

    expected_files = [
        "daily_market_summary.sql",
        "onchain_network_activity.sql",
        "cross_domain_market_network.sql",
    ]
    for filename in expected_files:
        sql_file = QUERIES_DIR / filename
        assert sql_file.is_file(), f"Missing query file: {filename}"
        content = sql_file.read_text(encoding="utf-8").strip()
        assert len(content) > 0, f"Query file is empty: {filename}"
        assert "SELECT" in content.upper()


@pytest.fixture
def populated_duckdb_manager(tmp_path: Path) -> DuckDBManager:
    """Create a DuckDBManager populated with sample market and on-chain data."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "test.duckdb"

    # Write 24 hourly candles for 2026-01-01 (complete day) and 24 for 2026-01-02
    candles: list[NormalizedCandle] = []
    for day in [1, 2]:
        for h in range(24):
            candles.append(
                NormalizedCandle(
                    source="coinbase_exchange",
                    product_id="BTC-USD",
                    granularity_seconds=3600,
                    candle_start_utc=datetime(2026, 1, day, h, 0, tzinfo=UTC),
                    open=Decimal("95000.00") + Decimal(str(h * 10)),
                    high=Decimal("95500.00") + Decimal(str(h * 10)),
                    low=Decimal("94500.00") + Decimal(str(h * 10)),
                    close=Decimal("95100.00") + Decimal(str(h * 10)),
                    volume_base=Decimal("150.50"),
                    ingested_at_utc=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
                    source_run_id="test-run-1",
                )
            )
    write_parquet_partitions(candles, curated_dir=curated_dir)

    # Write 2 days of on-chain metrics
    net_metrics = [
        NormalizedNetworkMetric(
            source="coin_metrics",
            asset="BTC",
            metric_date_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            transaction_count=350000,
            active_addresses_count=850000,
            ingested_at_utc=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
            source_run_id="test-run-1",
        ),
        NormalizedNetworkMetric(
            source="coin_metrics",
            asset="BTC",
            metric_date_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
            transaction_count=380000,
            active_addresses_count=900000,
            ingested_at_utc=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
            source_run_id="test-run-1",
        ),
    ]
    write_network_parquet_partitions(net_metrics, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()

    return manager


def test_daily_market_summary_query(populated_duckdb_manager: DuckDBManager) -> None:
    """2. Verify daily_market_summary.sql executes cleanly and computes metrics."""
    query_file = QUERIES_DIR / "daily_market_summary.sql"
    service = QueryService(populated_duckdb_manager)

    with populated_duckdb_manager:
        table = service.execute_file(query_file)

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    columns = table.column_names
    expected_cols = [
        "trade_date_utc",
        "product_id",
        "open",
        "high",
        "low",
        "close",
        "volume_base",
        "observed_hour_count",
        "is_complete",
        "daily_return_pct",
        "high_low_spread_pct",
    ]
    for col in expected_cols:
        assert col in columns, f"Missing column {col} in daily_market_summary result"

    # Check complete flag
    assert table.column("is_complete").to_pylist() == [True, True]
    assert table.column("observed_hour_count").to_pylist() == [24, 24]


def test_onchain_network_activity_query(populated_duckdb_manager: DuckDBManager) -> None:
    """3. Verify onchain_network_activity.sql executes and computes velocity/deltas."""
    query_file = QUERIES_DIR / "onchain_network_activity.sql"
    service = QueryService(populated_duckdb_manager)

    with populated_duckdb_manager:
        table = service.execute_file(query_file)

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    expected_cols = [
        "metric_date_utc",
        "asset",
        "transaction_count",
        "active_addresses_count",
        "tx_per_active_address",
        "dod_tx_delta",
        "dod_active_addresses_delta",
    ]
    for col in expected_cols:
        assert col in table.column_names

    # Check deltas: day 1 has None, day 2 has 380000 - 350000 = 30000
    tx_deltas = table.column("dod_tx_delta").to_pylist()
    assert tx_deltas[0] is None
    assert tx_deltas[1] == 30000


def test_cross_domain_market_network_query(populated_duckdb_manager: DuckDBManager) -> None:
    """4. Verify cross_domain_market_network.sql executes with date parameters."""
    query_file = QUERIES_DIR / "cross_domain_market_network.sql"
    service = QueryService(populated_duckdb_manager)

    params = {
        "start_date": "2026-01-01T00:00:00Z",
        "end_date": "2026-01-02T23:59:59Z",
    }

    with populated_duckdb_manager:
        table = service.execute_file(query_file, params)

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    expected_cols = [
        "trade_date_utc",
        "asset",
        "market_open_usd",
        "market_high_usd",
        "market_low_usd",
        "market_close_usd",
        "market_volume_btc",
        "market_observed_hour_count",
        "is_market_day_complete",
        "transaction_count",
        "active_addresses_count",
        "tx_per_active_address",
        "daily_return_pct",
        "high_low_spread_pct",
    ]
    for col in expected_cols:
        assert col in table.column_names

    # Test parameter filtering: narrow range to single day
    single_day_params = {
        "start_date": "2026-01-01T00:00:00Z",
        "end_date": "2026-01-01T23:59:59Z",
    }
    with populated_duckdb_manager:
        single_table = service.execute_file(query_file, single_day_params)
    assert single_table.num_rows == 1


def test_catalog_queries_on_empty_database(tmp_path: Path) -> None:
    """5. Catalog queries execute without errors on freshly initialized empty views."""
    db_path = tmp_path / "empty.duckdb"
    manager = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "empty_curated")
    with manager:
        manager.initialize()

    service = QueryService(manager)
    with manager:
        t1 = service.execute_file(QUERIES_DIR / "daily_market_summary.sql")
        assert t1.num_rows == 0

        t2 = service.execute_file(QUERIES_DIR / "onchain_network_activity.sql")
        assert t2.num_rows == 0

        t3 = service.execute_file(
            QUERIES_DIR / "cross_domain_market_network.sql",
            {"start_date": "2026-01-01", "end_date": "2026-01-02"},
        )
        assert t3.num_rows == 0


def test_cross_domain_query_missing_param_error(populated_duckdb_manager: DuckDBManager) -> None:
    """6. Omitting required parameters raises DuckDBManagerError."""
    query_file = QUERIES_DIR / "cross_domain_market_network.sql"
    service = QueryService(populated_duckdb_manager)

    with populated_duckdb_manager, pytest.raises(DuckDBManagerError):
        service.execute_file(query_file, {"start_date": "2026-01-01"})
