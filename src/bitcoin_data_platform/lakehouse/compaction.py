"""Bin-packing compaction engine for merging streaming micro-batch Parquet files."""

import os
import time
import uuid
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.models import (
    CompactionResult,
    LakehouseError,
    TableNotFoundError,
)
from bitcoin_data_platform.lakehouse.table import LakehouseTable


class CompactionEngine:
    """Detects fragmented small Parquet files and merges them into optimal sized files."""

    def __init__(self, catalog: LakehouseCatalog) -> None:
        self.catalog = catalog

    def compact(
        self,
        table_name: str,
        target_size_bytes: int = 128 * 1024 * 1024,
    ) -> CompactionResult:
        """Merge micro-batch Parquet files smaller than target_size_bytes per partition.

        Preserves total row count and data values identically, committing an atomic
        snapshot that replaces the fragmented files with compacted files in the manifest.
        """
        table_meta = self.catalog.get_table(table_name)
        if table_meta is None:
            raise TableNotFoundError(f"Table '{table_name}' does not exist in catalog.")

        table = LakehouseTable(table_name, self.catalog)
        current_snap = table.current_snapshot

        if current_snap is None or not current_snap.manifest_files:
            return CompactionResult(
                table_name=table_name,
                compacted_files_count=0,
                new_files_count=0,
                bytes_before=0,
                bytes_after=0,
                new_snapshot_id=None,
                rows_processed=0,
            )

        table_location = Path(table_meta.location)

        # 1. Group active manifest files by partition directory
        partition_files: dict[str, list[tuple[str, Path, int]]] = defaultdict(list)
        for manifest_rel in current_snap.manifest_files:
            file_path = Path(manifest_rel)
            if not file_path.is_absolute():
                file_path = table_location / file_path

            if not file_path.exists():
                raise LakehouseError(
                    f"Data file referenced in manifest does not exist: {file_path}"
                )

            file_size = file_path.stat().st_size
            parent_rel = str(file_path.parent.relative_to(table_location))
            partition_key = "" if parent_rel == "." else parent_rel
            partition_files[partition_key].append((manifest_rel, file_path, file_size))

        compacted_old_manifest_files: list[str] = []
        new_manifest_files: list[str] = []
        total_bytes_before = 0
        total_bytes_after = 0
        total_rows_processed = 0

        # 2. Iterate per partition and bin-pack small files
        for part_key, files in partition_files.items():
            # Small files under threshold
            small_files = [item for item in files if item[2] < target_size_bytes]
            if len(small_files) < 2:
                # Need at least 2 files to compact
                continue

            # Bin-packing into batches up to target_size_bytes
            bins: list[list[tuple[str, Path, int]]] = []
            current_bin: list[tuple[str, Path, int]] = []
            current_bin_size = 0

            for item in small_files:
                manifest_rel, path, size = item
                if current_bin and (current_bin_size + size > target_size_bytes):
                    bins.append(current_bin)
                    current_bin = [item]
                    current_bin_size = size
                else:
                    current_bin.append(item)
                    current_bin_size += size

            if current_bin:
                bins.append(current_bin)

            # Process each bin containing 2+ files
            for bin_items in bins:
                if len(bin_items) < 2:
                    continue

                bin_tables: list[pa.Table] = []
                bin_bytes_before = sum(item[2] for item in bin_items)

                for item in bin_items:
                    bin_tables.append(pq.read_table(item[1]))

                merged = pa.concat_tables(bin_tables)
                row_count = len(merged)

                # Deterministic ordering by sequence or time_utc
                sort_columns = [
                    (col, "ascending")
                    for col in ["sequence", "trade_id", "time_utc"]
                    if col in merged.column_names
                ]
                if sort_columns:
                    indices = pc.sort_indices(merged, sort_keys=sort_columns)
                    merged = merged.take(indices)

                # Determine target directory
                target_dir = table_location / part_key if part_key else table_location
                target_dir.mkdir(parents=True, exist_ok=True)

                new_filename = f"compacted_{int(time.time())}_{uuid.uuid4().hex[:8]}.parquet"
                tmp_path = target_dir / f"{new_filename}.tmp"
                final_path = target_dir / new_filename

                pq.write_table(merged, tmp_path, compression="snappy")
                os.replace(tmp_path, final_path)

                bytes_after = final_path.stat().st_size
                rel_new_path = str(final_path.relative_to(table_location))

                for item in bin_items:
                    compacted_old_manifest_files.append(item[0])
                new_manifest_files.append(rel_new_path)

                total_bytes_before += bin_bytes_before
                total_bytes_after += bytes_after
                total_rows_processed += row_count

        # 3. Commit new snapshot if compaction was performed
        if not compacted_old_manifest_files:
            return CompactionResult(
                table_name=table_name,
                compacted_files_count=0,
                new_files_count=0,
                bytes_before=0,
                bytes_after=0,
                new_snapshot_id=current_snap.snapshot_id,
                rows_processed=0,
            )

        compacted_set = set(compacted_old_manifest_files)
        surviving_manifest = [f for f in current_snap.manifest_files if f not in compacted_set]
        new_manifest = surviving_manifest + new_manifest_files

        summary = {
            "operation": "compaction",
            "compacted_files": len(compacted_old_manifest_files),
            "new_files": len(new_manifest_files),
            "bytes_before": total_bytes_before,
            "bytes_after": total_bytes_after,
            "rows_processed": total_rows_processed,
            "total_records": current_snap.summary.get("total_records", total_rows_processed),
            "total_files": len(new_manifest),
        }

        new_snapshot = self.catalog.commit_snapshot(
            table_name=table_name,
            manifest_files=new_manifest,
            summary=summary,
            parent_snapshot_id=current_snap.snapshot_id,
        )

        return CompactionResult(
            table_name=table_name,
            compacted_files_count=len(compacted_old_manifest_files),
            new_files_count=len(new_manifest_files),
            bytes_before=total_bytes_before,
            bytes_after=total_bytes_after,
            new_snapshot_id=new_snapshot.snapshot_id,
            rows_processed=total_rows_processed,
        )
