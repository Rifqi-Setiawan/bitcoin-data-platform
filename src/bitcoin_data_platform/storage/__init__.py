"""Storage layer and partition management."""

from bitcoin_data_platform.storage.duckdb_manager import (
    DuckDBManager,
    DuckDBManagerError,
)
from bitcoin_data_platform.storage.network_parquet_writer import (
    NETWORK_PARQUET_SCHEMA,
    NetworkParquetStorageError,
    NetworkPartitionResult,
    network_metrics_to_table,
    read_network_partition_metrics,
    write_network_parquet_partitions,
)
from bitcoin_data_platform.storage.parquet_writer import (
    PARQUET_SCHEMA,
    ParquetStorageError,
    PartitionResult,
    candles_to_table,
    read_partition_candles,
    write_parquet_partitions,
)
from bitcoin_data_platform.storage.raw_writer import (
    ENDPOINT_NAME,
    SCHEMA_VERSION,
    SOURCE_NAME,
    StorageError,
    compute_payload_sha256,
    create_raw_envelope,
    format_envelope_filename,
    read_raw_envelope,
    verify_raw_envelope,
    write_raw_envelope,
)

__all__ = [
    "ENDPOINT_NAME",
    "NETWORK_PARQUET_SCHEMA",
    "PARQUET_SCHEMA",
    "SCHEMA_VERSION",
    "SOURCE_NAME",
    "DuckDBManager",
    "DuckDBManagerError",
    "NetworkParquetStorageError",
    "NetworkPartitionResult",
    "ParquetStorageError",
    "PartitionResult",
    "StorageError",
    "candles_to_table",
    "compute_payload_sha256",
    "create_raw_envelope",
    "format_envelope_filename",
    "network_metrics_to_table",
    "read_network_partition_metrics",
    "read_partition_candles",
    "read_raw_envelope",
    "verify_raw_envelope",
    "write_network_parquet_partitions",
    "write_parquet_partitions",
    "write_raw_envelope",
]
