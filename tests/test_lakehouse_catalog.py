"""Unit tests for the Lakehouse transactional metadata catalog."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.models import (
    DEFAULT_PARTITION_KEYS,
    DEFAULT_TRADES_SCHEMA,
    ConcurrentModificationError,
    TableAlreadyExistsError,
    TableNotFoundError,
)


@pytest.fixture
def catalog_dir(tmp_path: Path) -> Path:
    """Provide a clean temporary catalog directory."""
    cat_dir = tmp_path / "lakehouse_catalog"
    return cat_dir


@pytest.fixture
def catalog(catalog_dir: Path) -> LakehouseCatalog:
    """Initialize a LakehouseCatalog instance."""
    return LakehouseCatalog(catalog_dir)


def test_catalog_init_creates_database(catalog_dir: Path, catalog: LakehouseCatalog) -> None:
    """Verify that catalog initialization creates the SQLite file."""
    assert catalog_dir.exists()
    assert catalog.db_path.exists()
    assert catalog.list_tables() == []


def test_create_table_success(catalog: LakehouseCatalog) -> None:
    """Verify successful table registration with schema and partition spec."""
    meta = catalog.create_table(
        table_name="trades",
        schema=DEFAULT_TRADES_SCHEMA,
        partition_spec=DEFAULT_PARTITION_KEYS,
    )
    assert meta.table_name == "trades"
    assert meta.table_uuid is not None
    assert meta.partition_spec == ["product_id"]
    assert len(meta.schema) == len(DEFAULT_TRADES_SCHEMA)
    assert Path(meta.location).exists()

    fetched = catalog.get_table("trades")
    assert fetched is not None
    assert fetched.table_name == "trades"
    assert fetched.table_uuid == meta.table_uuid
    assert fetched.partition_spec == ["product_id"]


def test_create_table_duplicate_raises_error(catalog: LakehouseCatalog) -> None:
    """Verify registering a table twice raises TableAlreadyExistsError."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    with pytest.raises(TableAlreadyExistsError, match="already exists"):
        catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)


def test_create_table_empty_name_raises(catalog: LakehouseCatalog) -> None:
    """Verify empty table name raises ValueError."""
    with pytest.raises(ValueError, match="cannot be empty"):
        catalog.create_table("   ", DEFAULT_TRADES_SCHEMA)


def test_get_table_nonexistent(catalog: LakehouseCatalog) -> None:
    """Verify querying an unregistered table returns None."""
    assert catalog.get_table("unknown_table") is None


def test_list_tables(catalog: LakehouseCatalog) -> None:
    """Verify list_tables returns all registered tables sorted."""
    catalog.create_table("zeta_trades", DEFAULT_TRADES_SCHEMA)
    catalog.create_table("alpha_trades", DEFAULT_TRADES_SCHEMA)
    tables = catalog.list_tables()
    assert tables == ["alpha_trades", "zeta_trades"]


def test_initial_snapshot_is_none(catalog: LakehouseCatalog) -> None:
    """Verify head snapshot is None for a freshly initialized table."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    assert catalog.get_current_snapshot("trades") is None


def test_commit_initial_snapshot(catalog: LakehouseCatalog) -> None:
    """Verify committing the first snapshot assigns ID 1 with parent None."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    snap = catalog.commit_snapshot(
        table_name="trades",
        manifest_files=["product_id=BTC-USD/f1.parquet"],
        summary={"operation": "append", "added_records": 10},
    )
    assert snap.snapshot_id == 1
    assert snap.table_name == "trades"
    assert snap.parent_snapshot_id is None
    assert snap.manifest_files == ["product_id=BTC-USD/f1.parquet"]
    assert snap.summary["added_records"] == 10

    current = catalog.get_current_snapshot("trades")
    assert current is not None
    assert current.snapshot_id == 1


def test_monotonic_snapshot_ids_and_parent_lineage(catalog: LakehouseCatalog) -> None:
    """Verify successive commits produce strictly monotonic IDs and correct parent IDs."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    s1 = catalog.commit_snapshot("trades", ["f1.parquet"], summary={"op": "append"})
    s2 = catalog.commit_snapshot("trades", ["f1.parquet", "f2.parquet"], summary={"op": "append"})
    s3 = catalog.commit_snapshot("trades", ["f3.parquet"], summary={"op": "compaction"})

    assert s1.snapshot_id == 1
    assert s1.parent_snapshot_id is None

    assert s2.snapshot_id == 2
    assert s2.parent_snapshot_id == 1

    assert s3.snapshot_id == 3
    assert s3.parent_snapshot_id == 2

    current = catalog.get_current_snapshot("trades")
    assert current is not None
    assert current.snapshot_id == 3


def test_optimistic_concurrency_conflict(catalog: LakehouseCatalog) -> None:
    """Verify ConcurrentModificationError when committing with stale parent_snapshot_id."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    s1 = catalog.commit_snapshot("trades", ["f1.parquet"])
    assert s1.snapshot_id == 1

    # Writer A wants to commit with parent 1
    # But Writer B commits first advancing head to 2
    s2 = catalog.commit_snapshot("trades", ["f1.parquet", "f2.parquet"])
    assert s2.snapshot_id == 2

    # Writer A now attempts to commit expecting parent 1
    with pytest.raises(
        ConcurrentModificationError,
        match="expected parent snapshot 1, but current head is 2",
    ):
        catalog.commit_snapshot(
            table_name="trades",
            manifest_files=["f1.parquet", "f3.parquet"],
            parent_snapshot_id=1,
        )


def test_commit_to_nonexistent_table_raises(catalog: LakehouseCatalog) -> None:
    """Verify committing snapshot to non-existent table raises TableNotFoundError."""
    with pytest.raises(TableNotFoundError, match="does not exist"):
        catalog.commit_snapshot("ghost_table", ["f1.parquet"])


def test_get_snapshot_by_id(catalog: LakehouseCatalog) -> None:
    """Verify retrieving snapshots by snapshot_id."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    s1 = catalog.commit_snapshot("trades", ["f1.parquet"])
    s2 = catalog.commit_snapshot("trades", ["f1.parquet", "f2.parquet"])

    assert catalog.get_snapshot("trades", 1) == s1
    assert catalog.get_snapshot("trades", 2) == s2
    assert catalog.get_snapshot("trades", 999) is None


def test_list_snapshots_order(catalog: LakehouseCatalog) -> None:
    """Verify list_snapshots returns all snapshots in ascending ID order."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    catalog.commit_snapshot("trades", ["f1.parquet"])
    catalog.commit_snapshot("trades", ["f2.parquet"])
    catalog.commit_snapshot("trades", ["f3.parquet"])

    snaps = catalog.list_snapshots("trades")
    assert len(snaps) == 3
    assert [s.snapshot_id for s in snaps] == [1, 2, 3]


def test_get_snapshot_as_of_timestamp(catalog: LakehouseCatalog) -> None:
    """Verify retrieving latest snapshot committed at or before a given timestamp."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    s1 = catalog.commit_snapshot("trades", ["f1.parquet"])

    before_s1 = s1.created_at_utc - timedelta(seconds=10)
    assert catalog.get_snapshot_as_of("trades", before_s1) is None

    exact_s1 = catalog.get_snapshot_as_of("trades", s1.created_at_utc)
    assert exact_s1 is not None
    assert exact_s1.snapshot_id == 1

    after_s1 = s1.created_at_utc + timedelta(seconds=10)
    found_after = catalog.get_snapshot_as_of("trades", after_s1)
    assert found_after is not None
    assert found_after.snapshot_id == 1


def test_expire_snapshots_protects_current_head(catalog: LakehouseCatalog) -> None:
    """Verify expire_snapshots deletes historical snapshots but preserves head."""
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA)
    catalog.commit_snapshot("trades", ["f1.parquet"])
    catalog.commit_snapshot("trades", ["f2.parquet"])
    s3 = catalog.commit_snapshot("trades", ["f3.parquet"])

    far_future = datetime.now(UTC) + timedelta(days=365)
    # Expiring with cutoff in the far future should delete snapshots 1 and 2, but NOT 3 (head)
    deleted_count = catalog.expire_snapshots("trades", far_future)
    assert deleted_count == 2

    remaining = catalog.list_snapshots("trades")
    assert len(remaining) == 1
    assert remaining[0].snapshot_id == s3.snapshot_id

    current = catalog.get_current_snapshot("trades")
    assert current is not None
    assert current.snapshot_id == 3
