"""Table abstraction for querying current and historical Lakehouse snapshots."""

from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.models import (
    LakehouseError,
    LakehouseTableMetadata,
    SnapshotNotFoundError,
    SnapshotRecord,
    TableNotFoundError,
)


class LakehouseTable:
    """Analytical table interface supporting current and time-travel reads."""

    def __init__(self, table_name: str, catalog: LakehouseCatalog) -> None:
        self.table_name = table_name.strip()
        self.catalog = catalog

    @property
    def metadata(self) -> LakehouseTableMetadata:
        """Fetch current table metadata from the catalog."""
        meta = self.catalog.get_table(self.table_name)
        if meta is None:
            raise TableNotFoundError(f"Table '{self.table_name}' is not registered in catalog.")
        return meta

    @property
    def schema(self) -> pa.Schema:
        """Table PyArrow schema."""
        return self.metadata.schema

    @property
    def location(self) -> Path:
        """Filesystem root directory for table data files."""
        return Path(self.metadata.location)

    @property
    def current_snapshot(self) -> SnapshotRecord | None:
        """Head snapshot of the table's main branch."""
        return self.catalog.get_current_snapshot(self.table_name)

    def _resolve_file_path(self, manifest_file: str) -> Path:
        """Resolve manifest file string to an absolute filesystem Path."""
        p = Path(manifest_file)
        if not p.is_absolute():
            p = self.location / p
        return p

    def _read_manifest_files(self, manifest_files: list[str]) -> pa.Table:
        """Read and concatenate parquet files listed in a snapshot manifest."""
        if not manifest_files:
            return pa.Table.from_batches([], schema=self.schema)

        tables: list[pa.Table] = []
        for file_str in manifest_files:
            file_path = self._resolve_file_path(file_str)
            if not file_path.exists():
                raise LakehouseError(
                    f"Data file referenced in snapshot manifest not found on disk: {file_path}"
                )
            tables.append(pq.read_table(file_path))

        if not tables:
            return pa.Table.from_batches([], schema=self.schema)
        return pa.concat_tables(tables)

    def read_current(self) -> pa.Table:
        """Read table data at the current head snapshot."""
        snapshot = self.current_snapshot
        if snapshot is None:
            return pa.Table.from_batches([], schema=self.schema)
        return self._read_manifest_files(snapshot.manifest_files)

    def read_snapshot(self, snapshot_id: int) -> pa.Table:
        """Read table data as of a specific historical snapshot ID."""
        snapshot = self.catalog.get_snapshot(self.table_name, snapshot_id)
        if snapshot is None:
            raise SnapshotNotFoundError(
                f"Snapshot {snapshot_id} not found for table '{self.table_name}'."
            )
        return self._read_manifest_files(snapshot.manifest_files)

    def read_as_of(self, timestamp_utc: datetime) -> pa.Table:
        """Read table data as-of a specific historical UTC timestamp."""
        snapshot = self.catalog.get_snapshot_as_of(self.table_name, timestamp_utc)
        if snapshot is None:
            raise SnapshotNotFoundError(
                f"No snapshot found as of {timestamp_utc.isoformat()} "
                f"for table '{self.table_name}'."
            )
        return self._read_manifest_files(snapshot.manifest_files)

    def to_duckdb(
        self,
        conn: duckdb.DuckDBPyConnection | None = None,
        snapshot_id: int | None = None,
        as_of: datetime | None = None,
        view_name: str | None = None,
    ) -> duckdb.DuckDBPyConnection:
        """Register the table snapshot as a view in a DuckDB connection for OLAP querying."""
        if snapshot_id is not None:
            arrow_table = self.read_snapshot(snapshot_id)
        elif as_of is not None:
            arrow_table = self.read_as_of(as_of)
        else:
            arrow_table = self.read_current()

        target_conn = conn if conn is not None else duckdb.connect(":memory:")
        target_name = view_name or self.table_name
        target_conn.register(target_name, arrow_table)
        return target_conn

    def query(
        self,
        sql: str,
        snapshot_id: int | None = None,
        as_of: datetime | None = None,
        view_name: str | None = None,
    ) -> pa.Table:
        """Execute SQL query over a table snapshot using DuckDB and return PyArrow Table."""
        target_name = view_name or self.table_name
        conn = self.to_duckdb(
            conn=None,
            snapshot_id=snapshot_id,
            as_of=as_of,
            view_name=target_name,
        )
        raw_result = conn.execute(sql).arrow()
        if hasattr(raw_result, "read_all"):
            return raw_result.read_all()
        return raw_result
