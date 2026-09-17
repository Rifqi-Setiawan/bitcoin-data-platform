"""Unit and integration tests for historical time-travel querying."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.models import (
    DEFAULT_PARTITION_KEYS,
    DEFAULT_TRADES_SCHEMA,
    SnapshotNotFoundError,
)
from bitcoin_data_platform.lakehouse.table import LakehouseTable
from bitcoin_data_platform.lakehouse.writer import LakehouseWriter
from bitcoin_data_platform.streaming.models import StreamTrade


@pytest.fixture
def catalog_dir(tmp_path: Path) -> Path:
    """Provide temporary catalog directory."""
    return tmp_path / "catalog"


@pytest.fixture
def catalog(catalog_dir: Path) -> LakehouseCatalog:
    """Provide LakehouseCatalog fixture."""
    return LakehouseCatalog(catalog_dir)


@pytest.fixture
def trades_table(catalog: LakehouseCatalog) -> LakehouseTable:
    """Initialize a trades table."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA, partition_spec=DEFAULT_PARTITION_KEYS)
    return LakehouseTable("trades", catalog)


def _make_trade(trade_id: int, product_id: str = "BTC-USD") -> StreamTrade:
    """Helper to generate a single trade."""
    now = datetime.now(UTC)
    return StreamTrade(
        source="coinbase_exchange",
        product_id=product_id,
        trade_id=trade_id,
        sequence=1000 + trade_id,
        price=Decimal("67000.00"),
        size=Decimal("1.00"),
        side="buy",
        time_utc=now,
        received_at_utc=now,
        received_monotonic_ns=1000000 + trade_id,
    )


def test_time_travel_read_by_snapshot_id(trades_table: LakehouseTable) -> None:
    """Verify reading table states across different historical snapshot IDs."""
    writer = LakehouseWriter(trades_table)

    # Commit snapshot 1: 5 trades
    writer.write([_make_trade(i) for i in range(1, 6)])
    # Commit snapshot 2: 3 more trades (total 8)
    writer.write([_make_trade(i) for i in range(6, 9)])
    # Commit snapshot 3: 2 more trades (total 10)
    writer.write([_make_trade(i) for i in range(9, 11)])

    # Snapshot 1 should have exactly 5 rows
    t1 = trades_table.read_snapshot(1)
    assert len(t1) == 5
    assert sorted(t1.column("trade_id").to_pylist()) == list(range(1, 6))

    # Snapshot 2 should have exactly 8 rows
    t2 = trades_table.read_snapshot(2)
    assert len(t2) == 8
    assert sorted(t2.column("trade_id").to_pylist()) == list(range(1, 9))

    # Snapshot 3 / current should have 10 rows
    t3 = trades_table.read_snapshot(3)
    t_curr = trades_table.read_current()
    assert len(t3) == 10
    assert len(t_curr) == 10


def test_time_travel_read_as_of_timestamp(
    trades_table: LakehouseTable,
    catalog: LakehouseCatalog,
) -> None:
    """Verify reading table state as-of past timestamps."""
    writer = LakehouseWriter(trades_table)

    writer.write([_make_trade(1)])
    s1 = catalog.get_snapshot("trades", 1)
    assert s1 is not None

    writer.write([_make_trade(2)])
    s2 = catalog.get_snapshot("trades", 2)
    assert s2 is not None

    # Querying as-of s1 created timestamp should return 1 row
    t_as_of_s1 = trades_table.read_as_of(s1.created_at_utc)
    assert len(t_as_of_s1) == 1

    # Querying as-of s2 created timestamp should return 2 rows
    t_as_of_s2 = trades_table.read_as_of(s2.created_at_utc)
    assert len(t_as_of_s2) == 2


def test_time_travel_invalid_snapshot_raises(trades_table: LakehouseTable) -> None:
    """Verify requesting a non-existent snapshot ID raises SnapshotNotFoundError."""
    writer = LakehouseWriter(trades_table)
    writer.write([_make_trade(1)])

    with pytest.raises(SnapshotNotFoundError, match="Snapshot 999 not found"):
        trades_table.read_snapshot(999)


def test_time_travel_before_first_snapshot_raises(trades_table: LakehouseTable) -> None:
    """Verify querying as-of before table's first commit raises SnapshotNotFoundError."""
    writer = LakehouseWriter(trades_table)
    writer.write([_make_trade(1)])

    way_back = datetime.now(UTC) - timedelta(days=365)
    with pytest.raises(SnapshotNotFoundError, match="No snapshot found as of"):
        trades_table.read_as_of(way_back)


def test_time_travel_duckdb_integration(trades_table: LakehouseTable) -> None:
    """Verify querying historical snapshots through DuckDB SQL views."""
    writer = LakehouseWriter(trades_table)
    writer.write([_make_trade(1), _make_trade(2)])  # Snap 1: 2 rows
    writer.write([_make_trade(3)])  # Snap 2: 3 rows

    # Query current snapshot
    res_curr = trades_table.query("SELECT COUNT(*) AS cnt, MAX(trade_id) AS max_id FROM trades")
    assert res_curr.column("cnt")[0].as_py() == 3
    assert res_curr.column("max_id")[0].as_py() == 3

    # Query historical snapshot 1
    res_s1 = trades_table.query(
        "SELECT COUNT(*) AS cnt, MAX(trade_id) AS max_id FROM trades",
        snapshot_id=1,
    )
    assert res_s1.column("cnt")[0].as_py() == 2
    assert res_s1.column("max_id")[0].as_py() == 2

    # Register into existing DuckDB connection
    conn = duckdb.connect()
    trades_table.to_duckdb(conn=conn, snapshot_id=1, view_name="historical_trades")
    duck_res = conn.execute("SELECT COUNT(*) FROM historical_trades").fetchone()
    assert duck_res is not None
    assert duck_res[0] == 2
