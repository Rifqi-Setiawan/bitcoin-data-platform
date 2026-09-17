"""SQLite-backed transactional metadata catalog for Lakehouse tables."""

import json
import sqlite3
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from bitcoin_data_platform.lakehouse.models import (
    ConcurrentModificationError,
    LakehouseTableMetadata,
    SnapshotRecord,
    TableAlreadyExistsError,
    TableNotFoundError,
    deserialize_arrow_schema,
    serialize_arrow_schema,
)


class LakehouseCatalog:
    """Zero-daemon metadata catalog managing table schemas, snapshots, and branches.

    Uses SQLite with Write-Ahead Logging (WAL) and optimistic concurrency control
    to guarantee ACID transactions across concurrent processes without external daemons.
    """

    def __init__(self, catalog_dir: Path | str) -> None:
        self.catalog_dir = Path(catalog_dir).resolve()
        self.catalog_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.catalog_dir / "platform_catalog.sqlite"
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Create a connection with WAL and timeout configured."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        """Initialize catalog schema tables if not present."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tables (
                    table_name TEXT PRIMARY KEY,
                    table_uuid TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    partition_spec_json TEXT NOT NULL,
                    location TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    table_name TEXT NOT NULL,
                    snapshot_id INTEGER NOT NULL,
                    parent_snapshot_id INTEGER,
                    manifest_files_json TEXT NOT NULL,
                    summary_json TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    PRIMARY KEY (table_name, snapshot_id),
                    FOREIGN KEY (table_name) REFERENCES tables(table_name) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS table_branches (
                    table_name TEXT NOT NULL,
                    branch_name TEXT NOT NULL,
                    current_snapshot_id INTEGER,
                    updated_at_utc TEXT NOT NULL,
                    PRIMARY KEY (table_name, branch_name),
                    FOREIGN KEY (table_name) REFERENCES tables(table_name) ON DELETE CASCADE
                );
                """
            )

    def create_table(
        self,
        table_name: str,
        schema: pa.Schema,
        partition_spec: Sequence[str] | None = None,
        location: Path | str | None = None,
    ) -> LakehouseTableMetadata:
        """Register a new Lakehouse table in the catalog."""
        table_name = table_name.strip()
        if not table_name:
            raise ValueError("Table name cannot be empty.")

        p_spec = list(partition_spec) if partition_spec is not None else []
        now_utc = datetime.now(UTC)

        if location is not None:
            table_location = Path(location).resolve()
        else:
            table_location = self.catalog_dir.parent / "tables" / table_name

        table_location.mkdir(parents=True, exist_ok=True)
        table_uuid = str(uuid.uuid4())
        schema_json = serialize_arrow_schema(schema)
        p_spec_json = json.dumps(p_spec)

        with self._get_connection() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO tables (
                        table_name, table_uuid, schema_json,
                        partition_spec_json, location, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        table_name,
                        table_uuid,
                        schema_json,
                        p_spec_json,
                        str(table_location),
                        now_utc.isoformat(),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO table_branches (
                        table_name, branch_name, current_snapshot_id, updated_at_utc
                    ) VALUES (?, 'main', NULL, ?);
                    """,
                    (table_name, now_utc.isoformat()),
                )
            except sqlite3.IntegrityError as exc:
                raise TableAlreadyExistsError(f"Table '{table_name}' already exists.") from exc

        return LakehouseTableMetadata(
            table_name=table_name,
            table_uuid=table_uuid,
            schema=schema,
            partition_spec=p_spec,
            location=str(table_location),
            created_at_utc=now_utc,
        )

    def get_table(self, table_name: str) -> LakehouseTableMetadata | None:
        """Retrieve table metadata by name."""
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                SELECT table_name, table_uuid, schema_json,
                       partition_spec_json, location, created_at_utc
                FROM tables WHERE table_name = ?;
                """,
                (table_name,),
            )
            row = cur.fetchone()
            if row is None:
                return None

            schema = deserialize_arrow_schema(row["schema_json"])
            partition_spec = json.loads(row["partition_spec_json"])
            created_at_utc = datetime.fromisoformat(row["created_at_utc"])
            if created_at_utc.tzinfo is None:
                created_at_utc = created_at_utc.replace(tzinfo=UTC)

            return LakehouseTableMetadata(
                table_name=row["table_name"],
                table_uuid=row["table_uuid"],
                schema=schema,
                partition_spec=partition_spec,
                location=row["location"],
                created_at_utc=created_at_utc,
            )

    def list_tables(self) -> list[str]:
        """List all table names registered in the catalog."""
        with self._get_connection() as conn:
            cur = conn.execute("SELECT table_name FROM tables ORDER BY table_name ASC;")
            return [row["table_name"] for row in cur.fetchall()]

    def get_current_snapshot(self, table_name: str, branch: str = "main") -> SnapshotRecord | None:
        """Get the current head snapshot for a table branch."""
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                SELECT current_snapshot_id FROM table_branches
                WHERE table_name = ? AND branch_name = ?;
                """,
                (table_name, branch),
            )
            row = cur.fetchone()
            if row is None or row["current_snapshot_id"] is None:
                return None
            snapshot_id = int(row["current_snapshot_id"])

        return self.get_snapshot(table_name, snapshot_id)

    def get_snapshot(self, table_name: str, snapshot_id: int) -> SnapshotRecord | None:
        """Retrieve a specific snapshot by ID."""
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                SELECT snapshot_id, table_name, parent_snapshot_id,
                       manifest_files_json, summary_json, created_at_utc
                FROM snapshots WHERE table_name = ? AND snapshot_id = ?;
                """,
                (table_name, snapshot_id),
            )
            row = cur.fetchone()
            if row is None:
                return None

            manifest_files = json.loads(row["manifest_files_json"])
            summary = json.loads(row["summary_json"])
            created_at = datetime.fromisoformat(row["created_at_utc"])
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)

            return SnapshotRecord(
                snapshot_id=int(row["snapshot_id"]),
                table_name=row["table_name"],
                parent_snapshot_id=(
                    int(row["parent_snapshot_id"])
                    if row["parent_snapshot_id"] is not None
                    else None
                ),
                manifest_files=manifest_files,
                summary=summary,
                created_at_utc=created_at,
            )

    def list_snapshots(self, table_name: str) -> list[SnapshotRecord]:
        """List all historical snapshots for a table ordered chronologically."""
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                SELECT snapshot_id, table_name, parent_snapshot_id,
                       manifest_files_json, summary_json, created_at_utc
                FROM snapshots WHERE table_name = ? ORDER BY snapshot_id ASC;
                """,
                (table_name,),
            )
            records: list[SnapshotRecord] = []
            for row in cur.fetchall():
                created_at = datetime.fromisoformat(row["created_at_utc"])
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=UTC)
                records.append(
                    SnapshotRecord(
                        snapshot_id=int(row["snapshot_id"]),
                        table_name=row["table_name"],
                        parent_snapshot_id=(
                            int(row["parent_snapshot_id"])
                            if row["parent_snapshot_id"] is not None
                            else None
                        ),
                        manifest_files=json.loads(row["manifest_files_json"]),
                        summary=json.loads(row["summary_json"]),
                        created_at_utc=created_at,
                    )
                )
            return records

    def get_snapshot_as_of(self, table_name: str, as_of_utc: datetime) -> SnapshotRecord | None:
        """Find the latest snapshot committed at or before a given UTC timestamp."""
        as_of_iso = as_of_utc.isoformat()
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                SELECT snapshot_id, table_name, parent_snapshot_id,
                       manifest_files_json, summary_json, created_at_utc
                FROM snapshots
                WHERE table_name = ? AND created_at_utc <= ?
                ORDER BY snapshot_id DESC LIMIT 1;
                """,
                (table_name, as_of_iso),
            )
            row = cur.fetchone()
            if row is None:
                return None

            created_at = datetime.fromisoformat(row["created_at_utc"])
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)

            return SnapshotRecord(
                snapshot_id=int(row["snapshot_id"]),
                table_name=row["table_name"],
                parent_snapshot_id=(
                    int(row["parent_snapshot_id"])
                    if row["parent_snapshot_id"] is not None
                    else None
                ),
                manifest_files=json.loads(row["manifest_files_json"]),
                summary=json.loads(row["summary_json"]),
                created_at_utc=created_at,
            )

    def commit_snapshot(
        self,
        table_name: str,
        manifest_files: Sequence[str],
        summary: dict[str, Any] | None = None,
        parent_snapshot_id: int | None = None,
        branch: str = "main",
    ) -> SnapshotRecord:
        """Commit a new snapshot to the table with optimistic concurrency control.

        Verifies that parent_snapshot_id matches the current branch head.
        If parent_snapshot_id is omitted (None) and the table has no commits, parent is None.
        If parent_snapshot_id is omitted (None) and the table has existing commits,
        parent is auto-resolved to current head.
        """
        now_utc = datetime.now(UTC)
        summary_payload = dict(summary) if summary is not None else {}
        manifest_payload = list(manifest_files)

        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")

            # 1. Verify table exists
            cur = conn.execute("SELECT table_name FROM tables WHERE table_name = ?;", (table_name,))
            if cur.fetchone() is None:
                raise TableNotFoundError(f"Table '{table_name}' does not exist.")

            # 2. Check current branch head
            cur = conn.execute(
                """
                SELECT current_snapshot_id FROM table_branches
                WHERE table_name = ? AND branch_name = ?;
                """,
                (table_name, branch),
            )
            branch_row = cur.fetchone()
            if branch_row is None:
                raise TableNotFoundError(
                    f"Branch '{branch}' for table '{table_name}' does not exist."
                )

            current_head = (
                int(branch_row["current_snapshot_id"])
                if branch_row["current_snapshot_id"] is not None
                else None
            )

            # 3. Optimistic Concurrency Control
            if parent_snapshot_id is not None and parent_snapshot_id != current_head:
                raise ConcurrentModificationError(
                    f"Conflict on branch '{branch}': expected parent snapshot "
                    f"{parent_snapshot_id}, but current head is {current_head}."
                )

            effective_parent = (
                parent_snapshot_id if parent_snapshot_id is not None else current_head
            )

            # 4. Monotonic Snapshot ID allocation
            cur = conn.execute(
                """
                SELECT COALESCE(MAX(snapshot_id), 0) + 1 AS next_id
                FROM snapshots WHERE table_name = ?;
                """,
                (table_name,),
            )
            next_id_row = cur.fetchone()
            next_snapshot_id = int(next_id_row["next_id"])

            # 5. Insert new snapshot
            conn.execute(
                """
                INSERT INTO snapshots (
                    table_name, snapshot_id, parent_snapshot_id,
                    manifest_files_json, summary_json, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    table_name,
                    next_snapshot_id,
                    effective_parent,
                    json.dumps(manifest_payload),
                    json.dumps(summary_payload),
                    now_utc.isoformat(),
                ),
            )

            # 6. Advance branch pointer
            conn.execute(
                """
                UPDATE table_branches
                SET current_snapshot_id = ?, updated_at_utc = ?
                WHERE table_name = ? AND branch_name = ?;
                """,
                (next_snapshot_id, now_utc.isoformat(), table_name, branch),
            )

            return SnapshotRecord(
                snapshot_id=next_snapshot_id,
                table_name=table_name,
                parent_snapshot_id=effective_parent,
                manifest_files=manifest_payload,
                summary=summary_payload,
                created_at_utc=now_utc,
            )

    def expire_snapshots(self, table_name: str, older_than_utc: datetime) -> int:
        """Expire snapshots older than cutoff while strictly protecting current branch heads."""
        older_than_iso = older_than_utc.isoformat()
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")

            # Query candidate snapshots
            cur = conn.execute(
                """
                SELECT snapshot_id FROM snapshots
                WHERE table_name = ? AND created_at_utc < ?;
                """,
                (table_name, older_than_iso),
            )
            candidates = [int(r["snapshot_id"]) for r in cur.fetchall()]
            if not candidates:
                return 0

            # Protect any snapshot currently pointed to by any branch
            cur = conn.execute(
                """
                SELECT current_snapshot_id FROM table_branches
                WHERE table_name = ? AND current_snapshot_id IS NOT NULL;
                """,
                (table_name,),
            )
            protected = {int(r["current_snapshot_id"]) for r in cur.fetchall()}

            to_delete = [s_id for s_id in candidates if s_id not in protected]
            if not to_delete:
                return 0

            placeholders = ",".join("?" for _ in to_delete)
            conn.execute(
                f"DELETE FROM snapshots WHERE table_name = ? AND snapshot_id IN ({placeholders});",
                [table_name, *to_delete],
            )
            return len(to_delete)
