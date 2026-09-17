# Decision Log

This log is intentionally lightweight. Promote a decision to a numbered ADR when it changes a public contract, introduces a long-lived service, is hard to reverse, or has meaningful security/operational cost.

## D-001 — Start with hourly Coinbase BTC-USD candles

- **Decision:** Use Coinbase Exchange public REST candles at one-hour grain.
- **Reason:** Clear official market-data contract, no credential, useful pagination/backfill exercise, manageable data, and immediate research value.
- **Alternatives:** Coin Metrics network metrics, Kraken OHLCVT, Alternative.me sentiment, FRED macro series, self-hosted Bitcoin Core.
- **Trade-offs:** One exchange is not the global market; historical buckets may be incomplete; current candles may change.
- **Reconsider when:** Source terms/access degrade, gaps become material, or cross-venue analysis becomes a Phase 6 requirement.

## D-002 — Preserve raw responses before transformation

- **Decision:** Store checksummed gzip JSON envelopes for successful requests.
- **Reason:** Enables replay, auditing, contract debugging, and deterministic rebuilds.
- **Alternatives:** Load directly into a database; store only curated rows.
- **Trade-offs:** Duplicate storage and retention management.
- **Reconsider when:** Raw volume materially pressures disk; then add tiering/lifecycle rules without sacrificing lineage.

## D-003 — Use Parquet and DuckDB, not a database server

- **Decision:** Curated facts live in Parquet; DuckDB provides SQL models, views, and small operational tables.
- **Reason:** Columnar analytics and portable files with almost no service overhead.
- **Alternatives:** PostgreSQL, SQLite, ClickHouse.
- **Trade-offs:** Serialized writes and no always-on multi-user endpoint.
- **Reconsider when:** Concurrent writers/readers, a serving API, or measured interactive latency requires a service.

## D-004 — Run manually before using a systemd timer

- **Decision:** Manual CLI is the first operating mode; systemd scheduling follows demonstrated idempotency and recovery.
- **Reason:** Automation should repeat a proven process.
- **Alternatives:** cron, Dagster, Airflow, Prefect.
- **Trade-offs:** No early dashboard/DAG UI.
- **Reconsider when:** More than roughly five interdependent pipelines make native timers hard to operate.

## D-005 — No container in the first implementation slice

- **Decision:** Develop with a locked Python environment; add an OCI image in the delivery phase.
- **Reason:** Avoid hiding filesystem permissions, state, and debugging behind packaging while the contract is still changing.
- **Alternatives:** Docker from first commit.
- **Trade-offs:** Host Python setup must be documented; initial deployment is less portable.
- **Reconsider when:** The CLI and filesystem contract are stable enough to test container parity.

## D-006 — No public service endpoint in V1

- **Decision:** Query through local DuckDB/SSH; bind no new port.
- **Reason:** Minimal attack surface and no demonstrated API/dashboard consumer.
- **Alternatives:** PostgreSQL port, dashboard, REST API.
- **Trade-offs:** Remote consumers need SSH or exported artifacts.
- **Reconsider when:** A specific consumer and authentication/TLS design exist.

## D-007 — Separate code and persistent data

- **Decision:** Code under `/srv/apps/services/bitcoin-data-platform`; data/state under `/srv/data/bitcoin-data-platform`.
- **Reason:** Deployments must not overwrite runtime state, and Git must never track data.
- **Alternatives:** Store `data/` inside the repository; use `/mnt`.
- **Trade-offs:** Deployment needs ownership and path configuration.
- **Reconsider when:** Object storage replaces local persistent data.

## D-008 — AI is optional and read-only first

- **Decision:** The core pipeline has no AI dependency. Later agents start with read-only metadata, SQL, and incident explanation.
- **Reason:** Deterministic data operations must work without model availability; least privilege limits harm.
- **Alternatives:** AI-directed pipeline control or autonomous remediation.
- **Trade-offs:** Less automation spectacle; more trustworthy operations.
- **Reconsider when:** Evaluation proves a narrow workflow safe, useful, auditable, and reversible.

## D-009 — Managed Native DuckDB SQL Views over dbt-core

- **Decision:** Use native declarative SQL views in DuckDB rather than adopting dbt-core with dbt-duckdb.
- **Reason:** Minimizes dependency footprint (avoids 45+ transitive dependencies), eliminates CLI invocation latency, and fits the current 4-model conformed DAG.
- **Alternatives:** Full dbt-core adoption, lightweight Jinja2 template preprocessor.
- **Trade-offs:** No automated interactive lineage graph generator; view dependencies managed explicitly in code.
- **Reconsider when:** Model DAG grows beyond 15 interdependent models or migration to a distributed cloud data warehouse occurs. (See `D-009_DBT_EVALUATION.md` for full evaluation).

## D-010 — Isolated Bounded WebSocket Streaming Experiment

- **Decision:** Implement an isolated, bounded real-time trade streaming collector for `BTC-USD` on Coinbase Exchange WebSocket without external streaming brokers (Kafka/Redis) or coupling to production batch storage.
- **Reason:** Empirically evaluate latency advantages and operational failure modes (disconnections, out-of-order sequencing, buffer bloat) while preserving batch as the single source of truth.
- **Alternatives:** Apache Kafka/Redpanda cluster, Redis Streams, direct write into batch Parquet partitions.
- **Trade-offs:** Volatile uncommitted events lost on abrupt process termination; raw JSON Lines consume uncompressed disk space during experiment.
- **Reconsider when:** Low-latency streaming is required continuously in production or cross-venue trade consolidation becomes necessary. (See `D-010_STREAMING_EXPERIMENT.md` for full evaluation).

## D-011 — Conditional Local Lakehouse Engine with Evidence-Gated Distributed Compute

- **Decision:** Implement a lightweight, zero-daemon transactional Lakehouse engine in `src/bitcoin_data_platform/lakehouse/` governed by an embedded SQLite metadata catalog (`platform_catalog.sqlite`), bin-packing compaction, snapshot time-travel, and Apache Parquet columnar storage.
- **Reason:** Solves single-writer locking constraints of embedded DuckDB, eliminates small-file degradation from streaming ingestion via deterministic compaction, enables reproducible as-of time-travel queries, and provides multi-asset scaling (`BTC-USD`, `ETH-USD`) without JVM daemon bloat (rejecting Spark/Trino).
- **Alternatives:** Apache Iceberg with Nessie / AWS Glue catalog, Delta Lake Standalone, Apache Spark / Trino cluster.
- **Trade-offs:** Snapshot metadata history consumes SQLite disk space until vacuumed; slight write latency overhead for ACID transaction commits.
- **Reconsider when:** Dataset volume crosses 100M rows on distributed object storage or multi-node distributed compute becomes cost-effective. (See `D-011_LAKEHOUSE_EVOLUTION.md` for full evaluation).
