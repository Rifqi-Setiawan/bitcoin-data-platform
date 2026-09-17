"""CLI commands and argument parser for the transactional Lakehouse engine."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.compaction import CompactionEngine
from bitcoin_data_platform.lakehouse.models import (
    DEFAULT_PARTITION_KEYS,
    DEFAULT_TRADES_SCHEMA,
    LakehouseError,
    SnapshotNotFoundError,
    TableAlreadyExistsError,
    TableNotFoundError,
    VacuumResult,
)
from bitcoin_data_platform.lakehouse.retention import RetentionManager
from bitcoin_data_platform.lakehouse.table import LakehouseTable
from bitcoin_data_platform.lakehouse.writer import (
    LakehouseWriter,
    load_input_file_to_arrow,
)
from bitcoin_data_platform.time_range import TimeRangeError, parse_iso_utc


def register_lakehouse_cli(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the 'lakehouse' top-level subcommand and nested operations."""
    lakehouse_parser = subparsers.add_parser(
        "lakehouse",
        help="Transactional multi-asset Lakehouse table operations.",
        description=(
            "Manage Lakehouse tables, atomic writes, compaction, time-travel, and vacuuming."
        ),
    )
    lakehouse_subparsers = lakehouse_parser.add_subparsers(
        dest="lakehouse_command",
        title="lakehouse commands",
        metavar="<subcommand>",
    )

    # 1. init
    init_parser = lakehouse_subparsers.add_parser(
        "init",
        help="Initialize a new Lakehouse table in the catalog.",
        description="Register a new multi-asset table with schema and partition specification.",
    )
    init_parser.add_argument("--table", required=True, help="Table name to register (e.g. trades).")
    init_parser.add_argument(
        "--catalog-dir",
        default="./data/lakehouse/catalog",
        help="Directory where SQLite catalog is stored (default: ./data/lakehouse/catalog).",
    )
    init_parser.add_argument(
        "--location",
        default=None,
        help="Custom filesystem directory for table data files.",
    )

    # 2. write
    write_parser = lakehouse_subparsers.add_parser(
        "write",
        help="Atomically write and commit a batch of trades to a Lakehouse table.",
        description="Ingest JSON Lines, JSON, or Parquet batch files into partitioned storage.",
    )
    write_parser.add_argument("--table", required=True, help="Target Lakehouse table name.")
    write_parser.add_argument(
        "--input-file",
        required=True,
        help="Path to input data file (.jsonl, .json, or .parquet).",
    )
    write_parser.add_argument(
        "--catalog-dir",
        default="./data/lakehouse/catalog",
        help="Directory where SQLite catalog is stored (default: ./data/lakehouse/catalog).",
    )

    # 3. compact
    compact_parser = lakehouse_subparsers.add_parser(
        "compact",
        help="Merge small micro-batch Parquet files into optimal sized files.",
        description="Bin-pack small files per partition and commit new replacement snapshot.",
    )
    compact_parser.add_argument("--table", required=True, help="Target Lakehouse table name.")
    compact_parser.add_argument(
        "--target-size-mb",
        type=int,
        default=128,
        help="Target file size threshold in MiB (default: 128).",
    )
    compact_parser.add_argument(
        "--catalog-dir",
        default="./data/lakehouse/catalog",
        help="Directory where SQLite catalog is stored (default: ./data/lakehouse/catalog).",
    )

    # 4. time-travel
    time_travel_parser = lakehouse_subparsers.add_parser(
        "time-travel",
        help="Query historical table snapshot by snapshot ID or timestamp.",
        description=(
            "Access immutable historical states of the dataset for backtesting and auditing."
        ),
    )
    time_travel_parser.add_argument("--table", required=True, help="Target Lakehouse table name.")
    time_travel_parser.add_argument(
        "--as-of-snapshot",
        type=int,
        default=None,
        help="Specific snapshot ID to query.",
    )
    time_travel_parser.add_argument(
        "--as-of-time",
        default=None,
        help="Historical UTC timestamp in ISO-8601 format (e.g. 2026-09-18T00:00:00Z).",
    )
    time_travel_parser.add_argument(
        "--catalog-dir",
        default="./data/lakehouse/catalog",
        help="Directory where SQLite catalog is stored (default: ./data/lakehouse/catalog).",
    )

    # 5. vacuum
    vacuum_parser = lakehouse_subparsers.add_parser(
        "vacuum",
        help="Purge unreferenced data files and expire obsolete metadata snapshots.",
        description="Safely garbage-collect orphan Parquet files and remove expired snapshots.",
    )
    vacuum_parser.add_argument("--table", required=True, help="Target Lakehouse table name.")
    vacuum_parser.add_argument(
        "--retain-days",
        type=int,
        default=7,
        help="Retention window in days for snapshots and safety grace period (default: 7).",
    )
    vacuum_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Scan and report orphan files without deleting them.",
    )
    vacuum_parser.add_argument(
        "--catalog-dir",
        default="./data/lakehouse/catalog",
        help="Directory where SQLite catalog is stored (default: ./data/lakehouse/catalog).",
    )


def handle_lakehouse_cli(args: argparse.Namespace) -> int:
    """Execute parsed lakehouse subcommands."""
    subcmd = getattr(args, "lakehouse_command", None)
    if not subcmd:
        sys.stderr.write(
            "error: lakehouse command requires a subcommand "
            "(init, write, compact, time-travel, vacuum)\n"
        )
        return 2

    catalog_dir = Path(getattr(args, "catalog_dir", "./data/lakehouse/catalog"))
    table_name = getattr(args, "table", "").strip()

    if not table_name:
        sys.stderr.write("error: --table must not be empty\n")
        return 2

    catalog = LakehouseCatalog(catalog_dir)

    try:
        if subcmd == "init":
            loc = getattr(args, "location", None)
            meta = catalog.create_table(
                table_name=table_name,
                schema=DEFAULT_TRADES_SCHEMA,
                partition_spec=DEFAULT_PARTITION_KEYS,
                location=loc,
            )
            init_out: dict[str, Any] = {
                "status": "created",
                "table_name": meta.table_name,
                "table_uuid": meta.table_uuid,
                "location": meta.location,
                "partition_spec": meta.partition_spec,
            }
            sys.stdout.write(json.dumps(init_out, indent=2) + "\n")
            return 0

        # For non-init subcommands, verify table exists
        table_meta = catalog.get_table(table_name)
        if table_meta is None:
            sys.stderr.write(f"error: table '{table_name}' does not exist in catalog\n")
            return 2

        table = LakehouseTable(table_name, catalog)

        if subcmd == "write":
            input_file = Path(args.input_file)
            if not input_file.exists():
                sys.stderr.write(f"error: input file does not exist: {input_file}\n")
                return 2

            arrow_data = load_input_file_to_arrow(input_file, schema=table.schema)
            writer = LakehouseWriter(table, catalog)
            snap = writer.write(arrow_data)
            write_out: dict[str, Any] = {
                "status": "committed",
                "table_name": table_name,
                "snapshot_id": snap.snapshot_id,
                "operation": snap.summary.get("operation"),
                "added_records": snap.summary.get("added_records", 0),
                "total_records": snap.summary.get("total_records", 0),
                "manifest_files_count": len(snap.manifest_files),
            }
            sys.stdout.write(json.dumps(write_out, indent=2) + "\n")
            return 0

        if subcmd == "compact":
            target_size_mb = int(args.target_size_mb)
            if target_size_mb <= 0:
                sys.stderr.write("error: --target-size-mb must be a positive integer\n")
                return 2

            engine = CompactionEngine(catalog)
            res = engine.compact(
                table_name=table_name,
                target_size_bytes=target_size_mb * 1024 * 1024,
            )
            sys.stdout.write(json.dumps(res.to_dict(), indent=2) + "\n")
            return 0

        if subcmd == "time-travel":
            as_of_snap = getattr(args, "as_of_snapshot", None)
            as_of_time_str = getattr(args, "as_of_time", None)
            effective_snap_id: int | None = None

            if as_of_snap is not None:
                arrow_tbl = table.read_snapshot(int(as_of_snap))
                snap_rec = catalog.get_snapshot(table_name, int(as_of_snap))
                effective_snap_id = int(as_of_snap)
            elif as_of_time_str is not None:
                as_of_utc = parse_iso_utc(as_of_time_str, "as-of-time")
                arrow_tbl = table.read_as_of(as_of_utc)
                snap_rec = catalog.get_snapshot_as_of(table_name, as_of_utc)
                effective_snap_id = snap_rec.snapshot_id if snap_rec else None
            else:
                arrow_tbl = table.read_current()
                snap_rec = table.current_snapshot
                effective_snap_id = snap_rec.snapshot_id if snap_rec else None

            travel_out: dict[str, Any] = {
                "table_name": table_name,
                "snapshot_id": effective_snap_id,
                "rows_count": len(arrow_tbl),
                "columns": arrow_tbl.column_names,
                "created_at_utc": snap_rec.created_at_utc.isoformat() if snap_rec else None,
            }
            sys.stdout.write(json.dumps(travel_out, indent=2) + "\n")
            return 0

        if subcmd == "vacuum":
            retain_days = int(args.retain_days)
            if retain_days < 0:
                sys.stderr.write("error: --retain-days must be non-negative\n")
                return 2

            dry_run = bool(args.dry_run)
            retention_mgr = RetentionManager(catalog)
            now_utc = datetime.now(UTC)
            cutoff = now_utc - timedelta(days=retain_days)

            # Expire historical snapshots older than retention threshold
            expired_snapshots = retention_mgr.expire_snapshots(table_name, cutoff)

            # Purge orphan files from disk
            vac_res = retention_mgr.vacuum_orphan_files(
                table_name=table_name,
                dry_run=dry_run,
                retain_days=retain_days,
            )

            result = VacuumResult(
                table_name=table_name,
                deleted_files_count=vac_res.deleted_files_count,
                deleted_bytes=vac_res.deleted_bytes,
                expired_snapshots_count=expired_snapshots,
                dry_run=dry_run,
            )
            sys.stdout.write(json.dumps(result.to_dict(), indent=2) + "\n")
            return 0

        sys.stderr.write(f"error: unrecognized lakehouse subcommand '{subcmd}'\n")
        return 2

    except TableAlreadyExistsError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except TableNotFoundError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except SnapshotNotFoundError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except TimeRangeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except LakehouseError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(f"error: lakehouse operation failed: {exc}\n")
        return 2
