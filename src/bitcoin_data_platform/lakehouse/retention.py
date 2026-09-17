"""Snapshot expiration and orphan file vacuuming for Lakehouse tables."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.models import TableNotFoundError, VacuumResult


class RetentionManager:
    """Manages metadata snapshot lifecycle and safe storage garbage collection."""

    def __init__(self, catalog: LakehouseCatalog) -> None:
        self.catalog = catalog

    def expire_snapshots(self, table_name: str, older_than_utc: datetime) -> int:
        """Expire metadata snapshots older than cutoff while protecting branch heads."""
        return self.catalog.expire_snapshots(table_name, older_than_utc)

    def vacuum_orphan_files(
        self,
        table_name: str,
        dry_run: bool = False,
        retain_days: int = 0,
        clock: datetime | None = None,
    ) -> VacuumResult:
        """Purge data files on disk that are not referenced by any retained snapshot.

        If retain_days > 0, files modified within that time window are protected
        to prevent deleting files written by active concurrent transactions.
        """
        table_meta = self.catalog.get_table(table_name)
        if table_meta is None:
            raise TableNotFoundError(f"Table '{table_name}' does not exist in catalog.")

        table_location = Path(table_meta.location).resolve()
        if not table_location.exists():
            return VacuumResult(
                table_name=table_name,
                deleted_files_count=0,
                deleted_bytes=0,
                expired_snapshots_count=0,
                dry_run=dry_run,
            )

        # 1. Collect all files referenced in any valid snapshot
        snapshots = self.catalog.list_snapshots(table_name)
        referenced_files: set[Path] = set()
        for snap in snapshots:
            for manifest_entry in snap.manifest_files:
                p = Path(manifest_entry)
                if not p.is_absolute():
                    p = table_location / p
                referenced_files.add(p.resolve())

        # 2. Scan disk for all Parquet files under table location
        disk_files = list(table_location.glob("**/*.parquet"))

        now_utc = clock if clock is not None else datetime.now(UTC)
        cutoff_timestamp = (
            (now_utc - timedelta(days=retain_days)).timestamp() if retain_days > 0 else None
        )

        orphan_files: list[Path] = []
        deleted_bytes = 0

        for disk_file in disk_files:
            resolved_disk_file = disk_file.resolve()
            if resolved_disk_file in referenced_files:
                continue

            # Retention window safety guard
            if cutoff_timestamp is not None:
                mtime = resolved_disk_file.stat().st_mtime
                if mtime > cutoff_timestamp:
                    # Recently created file within safety window
                    continue

            orphan_files.append(resolved_disk_file)
            deleted_bytes += resolved_disk_file.stat().st_size

        # 3. Purge orphan files if not in dry-run mode
        if not dry_run:
            for orphan in orphan_files:
                if orphan.exists():
                    orphan.unlink()
                # Clean up empty parent directory if inside table_location
                parent = orphan.parent
                while parent != table_location:
                    try:
                        parent.rmdir()
                        parent = parent.parent
                    except OSError:
                        break

        return VacuumResult(
            table_name=table_name,
            deleted_files_count=len(orphan_files),
            deleted_bytes=deleted_bytes,
            expired_snapshots_count=0,
            dry_run=dry_run,
        )
