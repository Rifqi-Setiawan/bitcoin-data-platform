# Phase 10 Implementation Specification — Lakehouse & Distributed Compute Evolution

Status: approved for implementation
Parent phase: Phase 10 — Lakehouse and distributed compute evolution
Task ID: `P10-lakehouse-evolution`
Recommended branch: `feature/P10-lakehouse-evolution`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Implement an evidence-led, zero-daemon transactional Lakehouse table engine for the Bitcoin Data Platform.
Address the single-writer locking constraints of embedded DuckDB, solve the small-file accumulation problem
from real-time trade streaming (Phase 9) via bounded bin-packing compaction, enable historical time-travel
reproducibility and in-place schema evolution via atomic ACID snapshot commits, and establish multi-asset
readiness (supporting `BTC-USD`, `ETH-USD`, etc.) while strictly operating within single-host VPS resource bounds.

## 2. User Stories

### Transactional ACID Appends
As a data engineer, I can execute:
```bash
bitcoin-data lakehouse write \
  --table trades \
  --input-file data/raw/streaming/events/part-001.jsonl \
  --catalog-dir ./data/lakehouse/catalog
```
to transactionally commit new trade batches into a versioned Lakehouse table, producing an atomic snapshot
with cryptographic checksums and optimistic concurrency control without locking readers.

### Small-File Compaction
As a platform operator, I can execute:
```bash
bitcoin-data lakehouse compact \
  --table trades \
  --target-size-mb 128 \
  --catalog-dir ./data/lakehouse/catalog
```
to scan fragmented micro-batch Parquet files, merge them into optimal columnar files (128–512 MiB), and atomically
publish a new snapshot that preserves logical row counts and data values identically.

### As-Of Time-Travel Querying
As a quantitative researcher, I can execute:
```bash
bitcoin-data lakehouse time-travel \
  --table trades \
  --as-of-snapshot 2 \
  --catalog-dir ./data/lakehouse/catalog
```
or specify `--as-of-time "2026-09-18T00:00:00Z"` to query exact historical states of the dataset for backtesting
and audit verification without maintaining duplicated table copies.

### Snapshot Retention & Vacuum
As an administrator, I can execute:
```bash
bitcoin-data lakehouse vacuum \
  --table trades \
  --retain-days 7 \
  --catalog-dir ./data/lakehouse/catalog
```
to purge unreferenced historical data files and expire obsolete metadata snapshots safely.

## 3. Architectural Boundaries & Ponytail Principles

1. **Zero External Daemon**: Runs entirely in-process using Python, PyArrow, SQLite, and DuckDB. Rejects Spark,
   Trino, MinIO, and Kafka (YAGNI / Ponytail) to eliminate JVM overhead and daemon maintenance on the VPS.
2. **Strict Multi-Asset Design**: The table schema natively models `product_id` (`BTC-USD`, `ETH-USD`, `SOL-USD`)
   and `asset` dimensions as first-class partitioning keys.
3. **Storage Decoupling**: Data files are stored as standard Apache Parquet, managed by an ACID metadata catalog
   persisted in local SQLite (`platform_catalog.sqlite`).
4. **Safety & Monotonicity**: Table snapshots form an immutable, append-only DAG. A failed write or crash
   never corrupts active reader snapshots.

## 4. Technical Specifications

### 4.1 Module Structure
```text
src/bitcoin_data_platform/lakehouse/
├── __init__.py
├── models.py           # LakehouseTableMetadata, SnapshotRecord, CompactionPlan, VacuumResult
├── catalog.py          # SQLite-backed transactional metadata catalog (zero-daemon)
├── table.py            # LakehouseTable abstraction (schema, partitions, read current / as-of)
├── writer.py           # ACID snapshot writer, Arrow/Parquet serialization, OCC retry
├── compaction.py       # Bin-packing compaction engine for streaming micro-batches
├── retention.py        # Snapshot expiration and orphan file vacuuming
└── cli.py              # CLI subcommands: init, write, compact, time-travel, vacuum, status
```

### 4.2 Metadata Catalog Schema (`platform_catalog.sqlite`)
- `tables`: `table_name`, `table_uuid`, `schema_json`, `partition_spec_json`, `location`, `created_at_utc`
- `snapshots`: `snapshot_id`, `table_name`, `parent_snapshot_id`, `manifest_files_json`, `summary_json`, `created_at_utc`
- `table_branches`: `table_name`, `branch_name` (default `main`), `current_snapshot_id`, `updated_at_utc`

### 4.3 Multi-Asset Lakehouse Table Schema
- `source`: String (e.g. `coinbase_exchange`)
- `product_id`: String (e.g. `BTC-USD`, `ETH-USD`)
- `trade_id`: Int64 (Unique transaction ID)
- `sequence`: Int64 (Exchange sequence number)
- `price`: Decimal128(38, 18) (Execution price)
- `size`: Decimal128(38, 18) (Executed volume)
- `side`: String (`buy` or `sell`)
- `time_utc`: Timestamp(us, tz="UTC") (Execution timestamp)
- `ingested_at_utc`: Timestamp(us, tz="UTC") (Pipeline ingestion timestamp)

## 5. Acceptance Criteria

- **AC-1**: `LakehouseCatalog` provides atomic snapshot commit with parent tracking and optimistic concurrency.
- **AC-2**: `LakehouseWriter` writes valid columnar Parquet files and commits snapshots with exact row counts.
- **AC-3**: Multi-asset support verified across `BTC-USD` and `ETH-USD` datasets with correct partition pruning.
- **AC-4**: `CompactionEngine` combines micro-batches into target file sizes without data loss or value mutation.
- **AC-5**: Time-travel querying succeeds by both `snapshot_id` and timestamp `as_of`.
- **AC-6**: `RetentionManager` cleans up expired snapshots and deletes orphan files while protecting active snapshots.
- **AC-7**: CLI subcommand `bitcoin-data lakehouse` provides `init`, `write`, `compact`, `time-travel`, and `vacuum`.
- **AC-8**: Comprehensive unit test suite added to `tests/test_lakehouse_*.py` with > 410 total tests passing.
- **AC-9**: Strict code quality gates pass cleanly (`ruff check`, `ruff format --check`, `mypy src`).
