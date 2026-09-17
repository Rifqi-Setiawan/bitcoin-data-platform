"""Transactional ACID writer for Lakehouse tables."""

import json
import os
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.models import (
    DEFAULT_TRADES_SCHEMA,
    SnapshotRecord,
)
from bitcoin_data_platform.lakehouse.table import LakehouseTable
from bitcoin_data_platform.streaming.models import StreamTrade


def stream_trades_to_arrow(
    trades: Sequence[StreamTrade],
    schema: pa.Schema | None = None,
) -> pa.Table:
    """Convert a sequence of StreamTrade dataclasses to a conforming PyArrow Table."""
    target_schema = schema if schema is not None else DEFAULT_TRADES_SCHEMA
    if not trades:
        return pa.Table.from_batches([], schema=target_schema)

    sources = [t.source for t in trades]
    product_ids = [t.product_id for t in trades]
    trade_ids = [t.trade_id for t in trades]
    sequences = [t.sequence for t in trades]
    prices = [t.price for t in trades]
    sizes = [t.size for t in trades]
    sides = [t.side for t in trades]
    times = [t.time_utc for t in trades]
    ingested_times = [t.received_at_utc for t in trades]

    p_type = target_schema.field("price").type
    s_type = target_schema.field("size").type
    t_type = target_schema.field("time_utc").type
    ing_type = target_schema.field("ingested_at_utc").type

    return pa.table(
        {
            "source": pa.array(sources, type=pa.string()),
            "product_id": pa.array(product_ids, type=pa.string()),
            "trade_id": pa.array(trade_ids, type=pa.int64()),
            "sequence": pa.array(sequences, type=pa.int64()),
            "price": pa.array(prices, type=p_type),
            "size": pa.array(sizes, type=s_type),
            "side": pa.array(sides, type=pa.string()),
            "time_utc": pa.array(times, type=t_type),
            "ingested_at_utc": pa.array(ingested_times, type=ing_type),
        },
        schema=target_schema,
    )


def dict_records_to_arrow(
    records: Sequence[dict[str, Any]],
    schema: pa.Schema | None = None,
) -> pa.Table:
    """Convert a sequence of dictionary records to a conforming PyArrow Table."""
    target_schema = schema if schema is not None else DEFAULT_TRADES_SCHEMA
    if not records:
        return pa.Table.from_batches([], schema=target_schema)

    sources = [str(r.get("source", "coinbase_exchange")) for r in records]
    product_ids = [str(r["product_id"]) for r in records]
    trade_ids = [int(r["trade_id"]) for r in records]
    sequences = [int(r.get("sequence", 0)) for r in records]
    prices = [Decimal(str(r["price"])) for r in records]
    sizes = [Decimal(str(r["size"])) for r in records]
    sides = [str(r["side"]) for r in records]

    times = []
    for r in records:
        val = r["time_utc"]
        dt = datetime.fromisoformat(str(val)) if isinstance(val, str) else val
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        times.append(dt)

    now_utc = datetime.now(UTC)
    ingested = []
    for r in records:
        val = r.get("ingested_at_utc") or r.get("received_at_utc")
        if val is not None:
            dt = datetime.fromisoformat(str(val)) if isinstance(val, str) else val
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
        else:
            dt = now_utc
        ingested.append(dt)

    p_type = target_schema.field("price").type
    s_type = target_schema.field("size").type
    t_type = target_schema.field("time_utc").type
    ing_type = target_schema.field("ingested_at_utc").type

    return pa.table(
        {
            "source": pa.array(sources, type=pa.string()),
            "product_id": pa.array(product_ids, type=pa.string()),
            "trade_id": pa.array(trade_ids, type=pa.int64()),
            "sequence": pa.array(sequences, type=pa.int64()),
            "price": pa.array(prices, type=p_type),
            "size": pa.array(sizes, type=s_type),
            "side": pa.array(sides, type=pa.string()),
            "time_utc": pa.array(times, type=t_type),
            "ingested_at_utc": pa.array(ingested, type=ing_type),
        },
        schema=target_schema,
    )


def load_input_file_to_arrow(
    file_path: Path | str,
    schema: pa.Schema | None = None,
) -> pa.Table:
    """Load JSON, JSON Lines, or Parquet file into a PyArrow Table."""
    path = Path(file_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist: {path}")

    if path.suffix == ".parquet":
        tbl = pq.read_table(path)
        return tbl

    if path.suffix in (".jsonl", ".ndjson"):
        records = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    records.append(json.loads(line_str))
        return dict_records_to_arrow(records, schema=schema)

    if path.suffix == ".json":
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return dict_records_to_arrow(data, schema=schema)
            if isinstance(data, dict):
                return dict_records_to_arrow([data], schema=schema)
            raise ValueError("JSON file must contain an array or object of records.")

    # Fallback attempt
    try:
        return pq.read_table(path)
    except Exception:
        records = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    records.append(json.loads(line_str))
        return dict_records_to_arrow(records, schema=schema)


class LakehouseWriter:
    """Atomically writes partitioned Parquet data and commits ACID snapshots."""

    def __init__(
        self,
        table: LakehouseTable | str,
        catalog: LakehouseCatalog | None = None,
    ) -> None:
        if isinstance(table, str):
            if catalog is None:
                raise ValueError("Catalog must be provided when table is specified by name.")
            self.catalog = catalog
            self.table = LakehouseTable(table, catalog)
        else:
            self.table = table
            self.catalog = table.catalog

    def write(
        self,
        data: pa.Table | Sequence[StreamTrade] | Sequence[dict[str, Any]],
        partition_keys: Sequence[str] | None = None,
        summary_extra: dict[str, Any] | None = None,
        expected_parent_snapshot_id: int | None = None,
    ) -> SnapshotRecord:
        """Write trade data partitioned by product_id and commit snapshot atomically."""
        arrow_table: pa.Table
        if isinstance(data, pa.Table):
            arrow_table = data
        elif isinstance(data, Sequence) and len(data) > 0:
            first = data[0]
            if isinstance(first, StreamTrade):
                trade_list = [x for x in data if isinstance(x, StreamTrade)]
                arrow_table = stream_trades_to_arrow(trade_list, schema=self.table.schema)
            elif isinstance(first, dict):
                dict_list = [x for x in data if isinstance(x, dict)]
                arrow_table = dict_records_to_arrow(dict_list, schema=self.table.schema)
            else:
                arrow_table = pa.Table.from_batches([], schema=self.table.schema)
        else:
            arrow_table = pa.Table.from_batches([], schema=self.table.schema)

        current_snap = self.table.current_snapshot
        parent_id = (
            expected_parent_snapshot_id
            if expected_parent_snapshot_id is not None
            else (current_snap.snapshot_id if current_snap is not None else None)
        )

        p_keys = (
            list(partition_keys)
            if partition_keys is not None
            else self.table.metadata.partition_spec
        )

        new_relative_files: list[str] = []

        if len(arrow_table) == 0:
            # Empty write preserves current manifest
            prev_manifest = list(current_snap.manifest_files) if current_snap else []
            prev_records = current_snap.summary.get("total_records", 0) if current_snap else 0
            summary = {
                "operation": "append",
                "added_records": 0,
                "total_records": prev_records,
                "added_files": 0,
                "total_files": len(prev_manifest),
            }
            if summary_extra:
                summary.update(summary_extra)
            return self.catalog.commit_snapshot(
                table_name=self.table.table_name,
                manifest_files=prev_manifest,
                summary=summary,
                parent_snapshot_id=parent_id,
            )

        if p_keys and all(k in arrow_table.column_names for k in p_keys):
            # Partitioned write (e.g. product_id=BTC-USD)
            p_col = p_keys[0]  # Standard primary partition
            unique_vals = pc.unique(arrow_table.column(p_col)).to_pylist()
            for val in unique_vals:
                mask = pc.equal(arrow_table.column(p_col), val)
                part_table = arrow_table.filter(mask)
                if len(part_table) == 0:
                    continue

                part_dir = self.table.location / f"{p_col}={val}"
                part_dir.mkdir(parents=True, exist_ok=True)

                filename = f"trade_{int(time.time())}_{uuid.uuid4().hex[:8]}.parquet"
                tmp_path = part_dir / f"{filename}.tmp"
                final_path = part_dir / filename

                pq.write_table(part_table, tmp_path, compression="snappy")
                os.replace(tmp_path, final_path)

                rel_path = str(final_path.relative_to(self.table.location))
                new_relative_files.append(rel_path)
        else:
            # Unpartitioned write
            target_dir = self.table.location
            target_dir.mkdir(parents=True, exist_ok=True)
            filename = f"trade_{int(time.time())}_{uuid.uuid4().hex[:8]}.parquet"
            tmp_path = target_dir / f"{filename}.tmp"
            final_path = target_dir / filename

            pq.write_table(arrow_table, tmp_path, compression="snappy")
            os.replace(tmp_path, final_path)

            rel_path = str(final_path.relative_to(self.table.location))
            new_relative_files.append(rel_path)

        prev_manifest = list(current_snap.manifest_files) if current_snap else []
        new_manifest = prev_manifest + new_relative_files
        added_records = len(arrow_table)
        prev_records = current_snap.summary.get("total_records", 0) if current_snap else 0
        total_records = prev_records + added_records

        summary = {
            "operation": "append",
            "added_records": added_records,
            "total_records": total_records,
            "added_files": len(new_relative_files),
            "total_files": len(new_manifest),
        }
        if summary_extra:
            summary.update(summary_extra)

        return self.catalog.commit_snapshot(
            table_name=self.table.table_name,
            manifest_files=new_manifest,
            summary=summary,
            parent_snapshot_id=parent_id,
        )
