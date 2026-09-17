"""Data models and schemas for the transactional Lakehouse engine."""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

# Standard analytical schema for multi-asset trade executions.
DEFAULT_TRADES_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("trade_id", pa.int64(), nullable=False),
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("price", pa.decimal128(38, 18), nullable=False),
        pa.field("size", pa.decimal128(38, 18), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("time_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

DEFAULT_PARTITION_KEYS = ["product_id"]


class LakehouseError(Exception):
    """Base exception for lakehouse storage and catalog operations."""


class TableNotFoundError(LakehouseError):
    """Table is not registered in the catalog."""


class TableAlreadyExistsError(LakehouseError):
    """Table is already registered in the catalog."""


class SnapshotNotFoundError(LakehouseError):
    """Requested snapshot ID or timestamp does not exist."""


class ConcurrentModificationError(LakehouseError):
    """Optimistic concurrency check failed due to conflicting concurrent writes."""


def serialize_arrow_schema(schema: pa.Schema) -> str:
    """Serialize PyArrow schema to JSON preserving exact types via IPC hex buffer."""
    buffer = schema.serialize()
    hex_payload = buffer.to_pybytes().hex()
    fields = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in schema]
    return json.dumps({"fields": fields, "arrow_ipc_hex": hex_payload})


def deserialize_arrow_schema(schema_json: str) -> pa.Schema:
    """Reconstruct PyArrow schema from serialized JSON payload."""
    data = json.loads(schema_json)
    if "arrow_ipc_hex" in data:
        buf = pa.py_buffer(bytes.fromhex(data["arrow_ipc_hex"]))
        return pa.ipc.read_schema(buf)

    # Reconstruct from fields if IPC hex is absent
    type_map = {
        "string": pa.string(),
        "int64": pa.int64(),
        "int32": pa.int32(),
        "float64": pa.float64(),
        "bool": pa.bool_(),
    }
    arrow_fields = []
    for f in data.get("fields", []):
        t_str = f["type"]
        if t_str.startswith("decimal128"):
            # e.g. decimal128(38, 18)
            inner = t_str[len("decimal128(") : -1]
            p_str, s_str = inner.split(",")
            t = pa.decimal128(int(p_str.strip()), int(s_str.strip()))
        elif t_str.startswith("timestamp"):
            t = pa.timestamp("us", tz="UTC")
        else:
            t = type_map.get(t_str, pa.string())
        arrow_fields.append(pa.field(f["name"], t, nullable=f.get("nullable", True)))
    return pa.schema(arrow_fields)


@dataclass(frozen=True)
class LakehouseTableMetadata:
    """Metadata describing a table registered in the lakehouse catalog."""

    table_name: str
    table_uuid: str
    schema: pa.Schema
    partition_spec: list[str]
    location: str
    created_at_utc: datetime

    def to_dict(self) -> dict[str, Any]:
        """Serialize metadata to dictionary."""
        return {
            "table_name": self.table_name,
            "table_uuid": self.table_uuid,
            "schema_fields": [f.name for f in self.schema],
            "partition_spec": self.partition_spec,
            "location": self.location,
            "created_at_utc": self.created_at_utc.isoformat(),
        }


@dataclass(frozen=True)
class SnapshotRecord:
    """Immutable record of an ACID commit in the table snapshot lineage."""

    snapshot_id: int
    table_name: str
    parent_snapshot_id: int | None
    manifest_files: list[str]
    summary: dict[str, Any]
    created_at_utc: datetime

    def to_dict(self) -> dict[str, Any]:
        """Serialize snapshot record to dictionary."""
        return {
            "snapshot_id": self.snapshot_id,
            "table_name": self.table_name,
            "parent_snapshot_id": self.parent_snapshot_id,
            "manifest_files": list(self.manifest_files),
            "summary": dict(self.summary),
            "created_at_utc": self.created_at_utc.isoformat(),
        }


@dataclass(frozen=True)
class CompactionPlan:
    """Plan for compacting fragmented micro-batch parquet files."""

    table_name: str
    partition_values: dict[str, str]
    source_files: list[str]
    target_file: str
    total_source_bytes: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize compaction plan to dictionary."""
        return {
            "table_name": self.table_name,
            "partition_values": dict(self.partition_values),
            "source_files": list(self.source_files),
            "target_file": self.target_file,
            "total_source_bytes": self.total_source_bytes,
        }


@dataclass(frozen=True)
class CompactionResult:
    """Summary of a table compaction execution."""

    table_name: str
    compacted_files_count: int
    new_files_count: int
    bytes_before: int
    bytes_after: int
    new_snapshot_id: int | None
    rows_processed: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize compaction result to dictionary."""
        return {
            "table_name": self.table_name,
            "compacted_files_count": self.compacted_files_count,
            "new_files_count": self.new_files_count,
            "bytes_before": self.bytes_before,
            "bytes_after": self.bytes_after,
            "new_snapshot_id": self.new_snapshot_id,
            "rows_processed": self.rows_processed,
        }


@dataclass(frozen=True)
class VacuumResult:
    """Summary of snapshot expiration and orphan file vacuuming."""

    table_name: str
    deleted_files_count: int
    deleted_bytes: int
    expired_snapshots_count: int
    dry_run: bool

    def to_dict(self) -> dict[str, Any]:
        """Serialize vacuum result to dictionary."""
        return {
            "table_name": self.table_name,
            "deleted_files_count": self.deleted_files_count,
            "deleted_bytes": self.deleted_bytes,
            "expired_snapshots_count": self.expired_snapshots_count,
            "dry_run": self.dry_run,
        }
