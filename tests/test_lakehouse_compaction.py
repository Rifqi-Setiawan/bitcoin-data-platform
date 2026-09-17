"""Unit and integration tests for the bin-packing CompactionEngine."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.compaction import CompactionEngine
from bitcoin_data_platform.lakehouse.models import (
    DEFAULT_PARTITION_KEYS,
    DEFAULT_TRADES_SCHEMA,
    TableNotFoundError,
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
    """Initialize a trades Lakehouse table."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA, partition_spec=DEFAULT_PARTITION_KEYS)
    return LakehouseTable("trades", catalog)


def _make_trades(product_id: str, count: int, offset: int) -> list[StreamTrade]:
    """Helper to generate sequential trades."""
    now = datetime.now(UTC)
    return [
        StreamTrade(
            source="coinbase_exchange",
            product_id=product_id,
            trade_id=offset + i,
            sequence=10000 + offset + i,
            price=Decimal(f"65000.{i:02d}"),
            size=Decimal("0.500000000000000000"),
            side="buy" if i % 2 == 0 else "sell",
            time_utc=now,
            received_at_utc=now,
            received_monotonic_ns=2000000 + offset + i,
        )
        for i in range(count)
    ]


def test_compaction_micro_batches_preserves_row_count_and_values(
    trades_table: LakehouseTable,
    catalog: LakehouseCatalog,
) -> None:
    """Verify compaction merges multiple micro-batches into 1 file without modifying data."""
    writer = LakehouseWriter(trades_table)

    # Ingest 4 micro-batches
    writer.write(_make_trades("BTC-USD", 5, offset=10))
    writer.write(_make_trades("BTC-USD", 5, offset=20))
    writer.write(_make_trades("BTC-USD", 5, offset=30))
    s4 = writer.write(_make_trades("BTC-USD", 5, offset=40))

    assert s4.snapshot_id == 4
    assert len(s4.manifest_files) == 4
    assert s4.summary["total_records"] == 20

    # Read data before compaction
    table_before = trades_table.read_current()
    assert len(table_before) == 20
    trade_ids_before = sorted(table_before.column("trade_id").to_pylist())

    # Run compaction
    engine = CompactionEngine(catalog)
    res = engine.compact("trades", target_size_bytes=100 * 1024 * 1024)

    assert res.compacted_files_count == 4
    assert res.new_files_count == 1
    assert res.rows_processed == 20
    assert res.new_snapshot_id == 5

    # Verify head snapshot
    s5 = trades_table.current_snapshot
    assert s5 is not None
    assert s5.snapshot_id == 5
    assert s5.parent_snapshot_id == 4
    assert s5.summary["operation"] == "compaction"
    assert len(s5.manifest_files) == 1
    assert s5.manifest_files[0].startswith("product_id=BTC-USD/compacted_")

    # Read data after compaction
    table_after = trades_table.read_current()
    assert len(table_after) == 20
    trade_ids_after = sorted(table_after.column("trade_id").to_pylist())
    assert trade_ids_before == trade_ids_after

    # Verify historical data files are still intact on disk
    for old_file in s4.manifest_files:
        assert (trades_table.location / old_file).exists()


def test_compaction_multi_asset_maintains_partition_boundaries(
    trades_table: LakehouseTable,
    catalog: LakehouseCatalog,
) -> None:
    """Verify compaction packs files per partition without mixing assets."""
    writer = LakehouseWriter(trades_table)

    # Ingest 2 micro-batches for BTC and 2 for ETH
    writer.write(_make_trades("BTC-USD", 4, offset=10))
    writer.write(_make_trades("BTC-USD", 6, offset=20))
    writer.write(_make_trades("ETH-USD", 3, offset=30))
    s4 = writer.write(_make_trades("ETH-USD", 7, offset=40))

    assert s4.snapshot_id == 4
    assert len(s4.manifest_files) == 4

    engine = CompactionEngine(catalog)
    res = engine.compact("trades", target_size_bytes=50 * 1024 * 1024)

    assert res.compacted_files_count == 4
    assert res.new_files_count == 2
    assert res.rows_processed == 20
    assert res.new_snapshot_id == 5

    s5 = trades_table.current_snapshot
    assert s5 is not None
    assert len(s5.manifest_files) == 2

    btc_compacted = [f for f in s5.manifest_files if f.startswith("product_id=BTC-USD/")]
    eth_compacted = [f for f in s5.manifest_files if f.startswith("product_id=ETH-USD/")]
    assert len(btc_compacted) == 1
    assert len(eth_compacted) == 1

    tbl_btc = pq.read_table(trades_table.location / btc_compacted[0])
    tbl_eth = pq.read_table(trades_table.location / eth_compacted[0])
    assert len(tbl_btc) == 10
    assert len(tbl_eth) == 10


def test_compaction_noop_when_insufficient_small_files(
    trades_table: LakehouseTable,
    catalog: LakehouseCatalog,
) -> None:
    """Verify compaction is a no-op if there are fewer than 2 small files per partition."""
    writer = LakehouseWriter(trades_table)
    writer.write(_make_trades("BTC-USD", 10, offset=1))

    engine = CompactionEngine(catalog)
    res = engine.compact("trades", target_size_bytes=100 * 1024 * 1024)

    assert res.compacted_files_count == 0
    assert res.new_files_count == 0
    assert res.new_snapshot_id == 1  # Head snapshot unchanged


def test_compaction_empty_table(
    trades_table: LakehouseTable,
    catalog: LakehouseCatalog,
) -> None:
    """Verify compaction on empty table returns cleanly without creating snapshot."""
    engine = CompactionEngine(catalog)
    res = engine.compact("trades")
    assert res.compacted_files_count == 0
    assert res.new_snapshot_id is None


def test_compaction_missing_table_raises(catalog: LakehouseCatalog) -> None:
    """Verify compaction raises TableNotFoundError for unknown table."""
    engine = CompactionEngine(catalog)
    with pytest.raises(TableNotFoundError, match="does not exist"):
        engine.compact("nonexistent_table")
