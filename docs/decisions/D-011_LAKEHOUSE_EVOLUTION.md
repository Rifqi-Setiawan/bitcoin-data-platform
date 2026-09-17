# D-011: Conditional Local Lakehouse Engine with Evidence-Gated Distributed Compute

## Status
Accepted

## Context
The Bitcoin Data Platform's foundation (Phases 1–9) relies on Hive-partitioned Parquet files and an embedded DuckDB
instance for local OLAP transformations. This architecture delivers sub-second query performance for up to tens
of millions of rows with minimal operational complexity.

However, scaling beyond single-writer constraints introduces distinct challenges:
1. **Concurrency**: DuckDB's embedded file lock prevents concurrent writers (e.g. streaming trade ingestion running
   simultaneously alongside batch promotion and analytical queries).
2. **Small-File Degradation**: Real-time WebSocket trade streaming (Phase 9) creates hundreds of micro-batch Parquet
   files, which increases metadata overhead and degrades query latency unless compacted.
3. **Reproducibility & Time-Travel**: Quantitative research workflows demand querying historical table snapshots as-of
   past timestamps without storing duplicate dataset copies.
4. **Schema Evolution**: Adding new market attributes (e.g. order funding rates, liquidations, multi-asset dimensions)
   currently requires table recreation or full partition rewrites.

## Decision
We implement a lightweight, zero-daemon transactional Lakehouse engine in `src/bitcoin_data_platform/lakehouse/`
governed by an SQLite metadata catalog (`platform_catalog.sqlite`) and Apache Parquet columnar storage.

Key architectural choices:
1. **Zero-Daemon Local Catalog**: We use an embedded SQLite database to track table namespaces, schemas, snapshot IDs,
   and commit manifests. This avoids deploying or running separate catalog services (e.g. Nessie, AWS Glue, Polaris)
   or storage engines (MinIO, Ceph).
2. **Multi-Asset Architecture**: Tables natively support multi-asset partitioning (`BTC-USD`, `ETH-USD`, `SOL-USD`)
   ensuring long-term horizontal extensibility.
3. **Atomic Snapshot Commits (ACID)**: Every append or compaction creates an immutable snapshot record. Readers always
   query an immutable snapshot state, allowing concurrent, non-blocking reads during active writes.
4. **Bounded Bin-Packing Compaction**: A dedicated compaction utility merges small micro-batch files into optimal
   sizes (128–512 MiB) in a single atomic snapshot transaction.
5. **Declining Spark and Trino (Evidence-Gated)**: We explicitly decline distributed compute engines (Apache Spark,
   Trino, Ray) because single-node vectorized execution in DuckDB and Polars processes up to 100M rows with lower
   latency, zero network hop overhead, and no JVM memory bloat on our VPS.

## Consequences

### Positive
- Readers never lock writers; writers never corrupt readers.
- Eliminates the small-file problem from streaming ingestion via deterministic compaction.
- Native as-of time-travel query capability for reproducible quantitative research.
- Multi-asset scaling without format mutation or schema rebuilds.
- Zero licensing cost, zero external daemons, and low memory footprint.

### Negative
- Snapshot history consumes metadata disk space until expired via vacuum operations.
- Slightly higher write latency due to catalog transaction commitment and manifest recording.

## Rollback & Migration Strategy
The lakehouse tables store data as standard Apache Parquet files. If the catalog or snapshot engine is decommissioned,
underlying Parquet data remains directly readable via standard DuckDB / PyArrow queries (`read_parquet`) without data loss.
