"""Unit and integration tests for the transactional LakehouseWriter."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
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
    """Create and return a LakehouseTable instance for testing."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA, partition_spec=DEFAULT_PARTITION_KEYS)
    return LakehouseTable("trades", catalog)


def _make_stream_trades(
    product_id: str,
    count: int,
    start_trade_id: int = 1000,
    start_seq: int = 5000,
) -> list[StreamTrade]:
    """Helper to create sample StreamTrade instances."""
    trades = []
    now = datetime.now(UTC)
    for i in range(count):
        trades.append(
            StreamTrade(
                source="coinbase_exchange",
                product_id=product_id,
                trade_id=start_trade_id + i,
                sequence=start_seq + i,
                price=Decimal(f"65000.{i:02d}"),
                size=Decimal(f"0.1{i:02d}"),
                side="buy" if i % 2 == 0 else "sell",
                time_utc=now,
                received_at_utc=now,
                received_monotonic_ns=1000000 + i,
            )
        )
    return trades


def test_write_stream_trades_single_asset(trades_table: LakehouseTable) -> None:
    """Verify writing StreamTrade objects creates partitioned parquet and commits snapshot."""
    writer = LakehouseWriter(trades_table)
    trades = _make_stream_trades("BTC-USD", 10)

    snap = writer.write(trades)
    assert snap.snapshot_id == 1
    assert snap.summary["operation"] == "append"
    assert snap.summary["added_records"] == 10
    assert snap.summary["total_records"] == 10
    assert len(snap.manifest_files) == 1
    assert snap.manifest_files[0].startswith("product_id=BTC-USD/")

    # Check file exists and has no leftover .tmp files
    data_file = trades_table.location / snap.manifest_files[0]
    assert data_file.exists()
    assert len(list(trades_table.location.glob("**/*.tmp"))) == 0

    # Verify Parquet content
    tbl = pq.read_table(data_file)
    assert len(tbl) == 10
    assert tbl.column("product_id").to_pylist() == ["BTC-USD"] * 10


def test_write_multi_asset_partitioning(trades_table: LakehouseTable) -> None:
    """Verify multi-asset dataset is partitioned into separate product directories."""
    writer = LakehouseWriter(trades_table)
    btc_trades = _make_stream_trades("BTC-USD", 5, start_trade_id=100)
    eth_trades = _make_stream_trades("ETH-USD", 7, start_trade_id=200)
    combined = btc_trades + eth_trades

    snap = writer.write(combined)
    assert snap.snapshot_id == 1
    assert snap.summary["added_records"] == 12
    assert snap.summary["total_records"] == 12
    assert len(snap.manifest_files) == 2

    # Check partitions exist
    btc_files = [f for f in snap.manifest_files if f.startswith("product_id=BTC-USD/")]
    eth_files = [f for f in snap.manifest_files if f.startswith("product_id=ETH-USD/")]
    assert len(btc_files) == 1
    assert len(eth_files) == 1

    # Verify row counts per partition
    tbl_btc = pq.read_table(trades_table.location / btc_files[0])
    tbl_eth = pq.read_table(trades_table.location / eth_files[0])
    assert len(tbl_btc) == 5
    assert len(tbl_eth) == 7


def test_successive_appends_accumulate_manifest_and_records(
    trades_table: LakehouseTable,
) -> None:
    """Verify multiple write calls advance snapshot ID and accumulate total_records."""
    writer = LakehouseWriter(trades_table)

    s1 = writer.write(_make_stream_trades("BTC-USD", 5, start_trade_id=10))
    assert s1.snapshot_id == 1
    assert s1.summary["total_records"] == 5
    assert len(s1.manifest_files) == 1

    s2 = writer.write(_make_stream_trades("BTC-USD", 10, start_trade_id=20))
    assert s2.snapshot_id == 2
    assert s2.summary["added_records"] == 10
    assert s2.summary["total_records"] == 15
    assert len(s2.manifest_files) == 2

    s3 = writer.write(_make_stream_trades("ETH-USD", 8, start_trade_id=50))
    assert s3.snapshot_id == 3
    assert s3.summary["added_records"] == 8
    assert s3.summary["total_records"] == 23
    assert len(s3.manifest_files) == 3


def test_write_pyarrow_table_directly(trades_table: LakehouseTable) -> None:
    """Verify writing directly from a pa.Table instance."""
    writer = LakehouseWriter(trades_table)
    now = datetime.now(UTC)

    arrow_data = pa.table(
        {
            "source": ["coinbase_exchange", "coinbase_exchange"],
            "product_id": ["BTC-USD", "BTC-USD"],
            "trade_id": [1, 2],
            "sequence": [10, 11],
            "price": pa.array(
                [Decimal("68000.00"), Decimal("68100.50")],
                type=pa.decimal128(38, 18),
            ),
            "size": pa.array([Decimal("0.5"), Decimal("1.25")], type=pa.decimal128(38, 18)),
            "side": ["buy", "sell"],
            "time_utc": pa.array([now, now], type=pa.timestamp("us", tz="UTC")),
            "ingested_at_utc": pa.array([now, now], type=pa.timestamp("us", tz="UTC")),
        },
        schema=DEFAULT_TRADES_SCHEMA,
    )

    snap = writer.write(arrow_data)
    assert snap.snapshot_id == 1
    assert snap.summary["added_records"] == 2
    assert snap.summary["total_records"] == 2


def test_write_dict_records(trades_table: LakehouseTable) -> None:
    """Verify writing records passed as a list of dictionaries."""
    writer = LakehouseWriter(trades_table)
    now = datetime.now(UTC)
    records = [
        {
            "source": "coinbase_exchange",
            "product_id": "SOL-USD",
            "trade_id": 999,
            "sequence": 4321,
            "price": "145.25",
            "size": "10.0",
            "side": "buy",
            "time_utc": now.isoformat(),
            "ingested_at_utc": now.isoformat(),
        }
    ]

    snap = writer.write(records)
    assert snap.snapshot_id == 1
    assert snap.summary["added_records"] == 1
    assert len(snap.manifest_files) == 1
    assert snap.manifest_files[0].startswith("product_id=SOL-USD/")


def test_write_empty_batch(trades_table: LakehouseTable) -> None:
    """Verify writing empty sequence creates 0-record commit preserving state."""
    writer = LakehouseWriter(trades_table)
    # First commit 5 records
    s1 = writer.write(_make_stream_trades("BTC-USD", 5))
    assert s1.snapshot_id == 1

    # Empty write
    s2 = writer.write([])
    assert s2.snapshot_id == 2
    assert s2.summary["added_records"] == 0
    assert s2.summary["total_records"] == 5
    assert s2.manifest_files == s1.manifest_files


def test_writer_init_by_table_name_and_missing_table(catalog: LakehouseCatalog) -> None:
    """Verify initializing LakehouseWriter with string table name."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    writer = LakehouseWriter("trades", catalog)
    assert writer.table.table_name == "trades"

    with pytest.raises(TableNotFoundError, match="is not registered"):
        missing_writer = LakehouseWriter("nonexistent", catalog)
        _ = missing_writer.table.metadata
