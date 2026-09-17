"""Unit and integration tests for snapshot retention and vacuuming."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.compaction import CompactionEngine
from bitcoin_data_platform.lakehouse.models import (
    DEFAULT_PARTITION_KEYS,
    DEFAULT_TRADES_SCHEMA,
    TableNotFoundError,
)
from bitcoin_data_platform.lakehouse.retention import RetentionManager
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


def _make_trades(count: int, offset: int) -> list[StreamTrade]:
    """Helper to create test trades."""
    now = datetime.now(UTC)
    return [
        StreamTrade(
            source="coinbase_exchange",
            product_id="BTC-USD",
            trade_id=offset + i,
            sequence=20000 + offset + i,
            price=Decimal("66000.00"),
            size=Decimal("0.25"),
            side="buy",
            time_utc=now,
            received_at_utc=now,
            received_monotonic_ns=3000000 + offset + i,
        )
        for i in range(count)
    ]


def test_expire_snapshots_deletes_older_metadata(
    trades_table: LakehouseTable,
    catalog: LakehouseCatalog,
) -> None:
    """Verify expire_snapshots purges historical commits while keeping head."""
    writer = LakehouseWriter(trades_table)
    writer.write(_make_trades(2, offset=1))
    writer.write(_make_trades(2, offset=3))
    s3 = writer.write(_make_trades(2, offset=5))

    retention_mgr = RetentionManager(catalog)
    # Expire everything up to far future
    cutoff = datetime.now(UTC) + timedelta(days=365)
    expired = retention_mgr.expire_snapshots("trades", cutoff)

    assert expired == 2
    remaining = catalog.list_snapshots("trades")
    assert len(remaining) == 1
    assert remaining[0].snapshot_id == s3.snapshot_id


def test_vacuum_orphan_files_lifecycle(
    trades_table: LakehouseTable,
    catalog: LakehouseCatalog,
) -> None:
    """Verify vacuum detects and purges unreferenced files only after snapshot expiration."""
    writer = LakehouseWriter(trades_table)
    # Step 1: Write two micro-batches (2 files on disk)
    s1 = writer.write(_make_trades(2, offset=1))
    s2 = writer.write(_make_trades(2, offset=3))
    assert len(s2.manifest_files) == 2

    # Step 2: Compact into 1 new file (now 3 files exist on disk: 2 old, 1 new)
    engine = CompactionEngine(catalog)
    comp_res = engine.compact("trades", target_size_bytes=50 * 1024 * 1024)
    assert comp_res.compacted_files_count == 2
    assert comp_res.new_files_count == 1

    retention_mgr = RetentionManager(catalog)

    # Step 3: Vacuum before snapshot expiration should NOT delete anything
    # because s1 and s2 still reference the old files
    vac_before = retention_mgr.vacuum_orphan_files("trades", dry_run=False, retain_days=0)
    assert vac_before.deleted_files_count == 0
    for f in s2.manifest_files:
        assert (trades_table.location / f).exists()

    # Step 4: Expire historical snapshots s1 and s2
    cutoff = datetime.now(UTC) + timedelta(days=365)
    expired_count = retention_mgr.expire_snapshots("trades", cutoff)
    assert expired_count == 2

    # Step 5: Dry-run vacuum reports the 2 orphan files without deleting them
    vac_dry = retention_mgr.vacuum_orphan_files("trades", dry_run=True, retain_days=0)
    assert vac_dry.deleted_files_count == 2
    assert vac_dry.dry_run is True
    for f in s1.manifest_files:
        assert (trades_table.location / f).exists()

    # Step 6: Real vacuum deletes the 2 orphan files
    vac_real = retention_mgr.vacuum_orphan_files("trades", dry_run=False, retain_days=0)
    assert vac_real.deleted_files_count == 2
    assert vac_real.dry_run is False

    # Check orphan files are deleted
    for f in s1.manifest_files:
        assert not (trades_table.location / f).exists()

    # Current snapshot is still fully intact and readable
    curr_data = trades_table.read_current()
    assert len(curr_data) == 4


def test_vacuum_retention_window_protects_recent_files(
    trades_table: LakehouseTable,
    catalog: LakehouseCatalog,
) -> None:
    """Verify retain_days > 0 protects files created recently."""
    writer = LakehouseWriter(trades_table)
    writer.write(_make_trades(2, offset=1))

    # Manually drop an unreferenced file in the table folder
    orphan_file = trades_table.location / "uncommitted_orphan.parquet"
    orphan_file.write_bytes(b"dummy")

    retention_mgr = RetentionManager(catalog)

    # retain_days=1 should protect this file created seconds ago
    vac_protected = retention_mgr.vacuum_orphan_files("trades", dry_run=False, retain_days=1)
    assert vac_protected.deleted_files_count == 0
    assert orphan_file.exists()

    # retain_days=0 should immediately delete it
    vac_purge = retention_mgr.vacuum_orphan_files("trades", dry_run=False, retain_days=0)
    assert vac_purge.deleted_files_count == 1
    assert not orphan_file.exists()


def test_vacuum_missing_table_raises(catalog: LakehouseCatalog) -> None:
    """Verify vacuum raises TableNotFoundError for unknown table."""
    retention_mgr = RetentionManager(catalog)
    with pytest.raises(TableNotFoundError, match="does not exist"):
        retention_mgr.vacuum_orphan_files("ghost_table")
