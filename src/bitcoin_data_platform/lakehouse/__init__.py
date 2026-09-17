"""Transactional Lakehouse engine for multi-asset analytics."""

from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.compaction import CompactionEngine
from bitcoin_data_platform.lakehouse.models import (
    DEFAULT_PARTITION_KEYS,
    DEFAULT_TRADES_SCHEMA,
    CompactionPlan,
    CompactionResult,
    ConcurrentModificationError,
    LakehouseError,
    LakehouseTableMetadata,
    SnapshotNotFoundError,
    SnapshotRecord,
    TableAlreadyExistsError,
    TableNotFoundError,
    VacuumResult,
    deserialize_arrow_schema,
    serialize_arrow_schema,
)
from bitcoin_data_platform.lakehouse.retention import RetentionManager
from bitcoin_data_platform.lakehouse.table import LakehouseTable
from bitcoin_data_platform.lakehouse.writer import (
    LakehouseWriter,
    dict_records_to_arrow,
    load_input_file_to_arrow,
    stream_trades_to_arrow,
)

__all__ = [
    "LakehouseCatalog",
    "LakehouseTable",
    "LakehouseWriter",
    "CompactionEngine",
    "RetentionManager",
    "LakehouseTableMetadata",
    "SnapshotRecord",
    "CompactionPlan",
    "CompactionResult",
    "VacuumResult",
    "DEFAULT_TRADES_SCHEMA",
    "DEFAULT_PARTITION_KEYS",
    "LakehouseError",
    "TableNotFoundError",
    "TableAlreadyExistsError",
    "SnapshotNotFoundError",
    "ConcurrentModificationError",
    "deserialize_arrow_schema",
    "serialize_arrow_schema",
    "dict_records_to_arrow",
    "load_input_file_to_arrow",
    "stream_trades_to_arrow",
]
