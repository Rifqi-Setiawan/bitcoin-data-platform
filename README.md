# Bitcoin Data Engineering Platform

This repository is the architecture and implementation workspace for a production-style,
single-host Bitcoin data platform and public data-engineering portfolio project.

## Phase 1A: Repository Bootstrap and Window Planner

Phase 1A provides the executable foundation for deterministic batch ingestion: an installable
Python 3.12 package and the `bitcoin-data` CLI with the `plan-backfill` command.

This slice is strictly offline: it performs no network requests and writes no runtime data.
It validates an operator-specified half-open UTC interval and deterministically partitions it
into Coinbase-compatible request windows of at most 300 hourly candles.

### Core semantics

- **UTC timestamps**: All timestamps must be explicit UTC, formatted as ISO-8601 (e.g.
  `2026-01-01T00:00:00Z` or `2026-01-01T00:00:00+00:00`). Output is normalized to canonical
  `YYYY-MM-DDTHH:00:00Z`. Naive datetimes and non-UTC offsets are strictly rejected.
- **Half-open interval `[start, end)`**: The start timestamp is included; the end timestamp is
  excluded.
- **Hourly boundary alignment**: Both `start` and `end` must align to an exact hour
  (`minute=0`, `second=0`, `microsecond=0`).
- **Coinbase window limit**: Each request window contains between 1 and 300 expected hourly candles
  (granularity: 3600 seconds, product: `BTC-USD`, source: `coinbase_exchange`).
- **No open/future candles**: The `end` boundary must not exceed the start of the current UTC hour.
- **Deterministic output**: Identical inputs always produce byte-equivalent JSON output with
  stable key ordering.
- **Safe failure**: Invalid input prints a concise error message to stderr, outputs nothing to stdout,
  and exits with status `2`.

## Environment setup

Target Python version: **Python 3.12**.

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install development dependencies and editable package
make install

# Alternatively, using pip directly:
pip install -r requirements-dev.txt
pip install -e .
```

## CLI usage

Run `bitcoin-data --help` to view available commands:

```bash
bitcoin-data --help
```

### Plan backfill

To generate a deterministic backfill plan for a UTC interval:

```bash
bitcoin-data plan-backfill \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-26T01:00:00Z
```

Example JSON output (601 hours partitioned into two 300-hour windows and one 1-hour window):

```json
{
  "schema_version": 1,
  "source": "coinbase_exchange",
  "product_id": "BTC-USD",
  "granularity_seconds": 3600,
  "requested_start_utc": "2026-01-01T00:00:00Z",
  "requested_end_utc": "2026-01-26T01:00:00Z",
  "expected_candle_count": 601,
  "window_count": 3,
  "windows": [
    {
      "index": 0,
      "start_utc": "2026-01-01T00:00:00Z",
      "end_utc": "2026-01-13T12:00:00Z",
      "expected_candle_count": 300
    },
    {
      "index": 1,
      "start_utc": "2026-01-13T12:00:00Z",
      "end_utc": "2026-01-26T00:00:00Z",
      "expected_candle_count": 300
    },
    {
      "index": 2,
      "start_utc": "2026-01-26T00:00:00Z",
      "end_utc": "2026-01-26T01:00:00Z",
      "expected_candle_count": 1
    }
  ]
}
```

### Execute backfill (Phase 1B)

To execute a full backfill pipeline (plan windows, fetch from Coinbase Exchange with retries/rate-limiting, validate against contract, and write atomic raw envelopes):

```bash
bitcoin-data backfill \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-02T00:00:00Z \
  --output-dir ./data/raw
```

The command outputs a structured JSON run summary to stdout and logs structured JSON events to stderr.

Example run summary:

```json
{
  "run_id": "843195da-79aa-4df7-8094-0cfc3b7a58ad",
  "status": "success",
  "requested_start_utc": "2026-01-01T00:00:00Z",
  "requested_end_utc": "2026-01-02T00:00:00Z",
  "windows_planned": 1,
  "windows_succeeded": 1,
  "windows_failed": 0,
  "candles_ingested": 24,
  "output_dir": "./data/raw",
  "files_written": [
    "data/raw/843195da-79aa-4df7-8094-0cfc3b7a58ad_20260101T00Z_20260102T00Z.json.gz"
  ]
}
```

### Promote to curated Parquet and DuckDB (Phase 2)

To normalize and promote raw envelopes into typed, annual Parquet partitions and register DuckDB analytical views:

```bash
bitcoin-data promote \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```

The command reads raw gzip JSON envelopes, normalizes them into typed records with Decimal price/volume fields, applies blocking quality invariants, deduplicates by natural key (`source`, `product_id`, `granularity_seconds`, `candle_start_utc`), writes annual Parquet partitions atomically, and initializes DuckDB views (`fact_market_candle_hourly`, `mart_btc_usd_daily`) and pipeline `run_metadata`.

Example run summary:

```json
{
  "run_id": "4b5f4d89-4fa2-4bf1-bfd0-4bf69d671c6d",
  "status": "success",
  "raw_envelopes_read": 1,
  "rows_promoted": 24,
  "partitions_written": 1,
  "curated_dir": "./data/curated",
  "db_path": "./data/state/platform.duckdb"
}
```

### Query curated views (Phase 2)

To execute SQL queries directly against the DuckDB analytical tables and views:

```bash
bitcoin-data query \
  --db-path ./data/state/platform.duckdb \
  --sql "SELECT * FROM fact_market_candle_hourly LIMIT 5"
```

Or query the daily mart aggregation:

```bash
bitcoin-data query \
  --db-path ./data/state/platform.duckdb \
  --sql "SELECT trade_date_utc, open, high, low, close, volume_base, observed_hour_count, is_complete FROM mart_btc_usd_daily"
```

Results are printed as JSON arrays to stdout.

### Incremental load (Phase 3)

To execute a watermark-based incremental update (fetching from `watermark - overlap` to the current completed UTC hour, promoting to Parquet, and advancing the watermark):

```bash
bitcoin-data incremental \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb \
  --overlap-hours 48
```

The watermark only advances forward monotonically after successful quality checks and Parquet promotion. If no watermark exists yet, the command cleanly exits with code `2`. Concurrency is protected by run locking (exit code `6` if a run is already active).

Example incremental run summary:

```json
{
  "run_id": "9b12e345-6789-4def-9012-3456789abcde",
  "status": "success",
  "mode": "incremental",
  "old_watermark_utc": "2026-01-01T00:00:00Z",
  "new_watermark_utc": "2026-01-02T12:00:00Z",
  "windows_planned": 1,
  "windows_succeeded": 1,
  "windows_failed": 0,
  "candles_ingested": 36,
  "raw_envelopes_read": 3,
  "rows_promoted": 60,
  "partitions_written": 1,
  "curated_dir": "./data/curated",
  "db_path": "./data/state/platform.duckdb"
}
```

### Operational status (Phase 3)

To inspect platform watermark freshness, run history, curated data statistics, gap detection, and run lock state:

```bash
bitcoin-data status \
  --db-path ./data/state/platform.duckdb \
  --curated-dir ./data/curated
```

Example status JSON output:

```json
{
  "watermark_utc": "2026-01-02T12:00:00Z",
  "watermark_age_hours": 1.5,
  "last_run": {
    "run_id": "9b12e345-6789-4def-9012-3456789abcde",
    "mode": "incremental",
    "status": "SUCCEEDED",
    "completed_at_utc": "2026-01-02T13:30:00Z",
    "rows_promoted": 60
  },
  "last_success": {
    "run_id": "9b12e345-6789-4def-9012-3456789abcde",
    "completed_at_utc": "2026-01-02T13:30:00Z"
  },
  "last_failure": null,
  "curated_stats": {
    "total_rows": 60,
    "min_candle_utc": "2026-01-01T00:00:00Z",
    "max_candle_utc": "2026-01-02T12:00:00Z",
    "partitions": 1,
    "total_size_bytes": 12800
  },
  "gaps": [],
  "is_locked": false
}
```

### Repair curated layer (Phase 3)

To rebuild the curated Parquet layer completely from all raw envelopes without lowering the pipeline watermark:

```bash
bitcoin-data repair \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```

Use `--force` to clear stale locks older than 1 hour if an earlier run was abandoned:

```bash
bitcoin-data repair \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb \
  --force
```

#### Exit codes

- `0`: Success (operation completed successfully, or nothing to fetch).
- `2`: Invalid input parameters, missing watermark for incremental, or query execution error.
- `3`: Source unavailable (Coinbase API unavailable after exhausting all retry attempts).
- `4`: Contract or quality check failure (malformed payload or invariant violation).
- `5`: Storage failure (disk, Parquet, or database write failure).
- `6`: Concurrent run detected (run lock actively held).

## Phase 5: Observability, Data Quality Rules & Failure Alerting

Phase 5 establishes multi-layer operational observability, automated dataset quality gates, storage monitoring, and low-noise failure alerting:

- **Dataset Quality Rules (`quality/dataset_checks.py`)**:
  - **Natural Key Uniqueness (`BLOCK`)**: Prevents duplicate records across `(source, product_id, granularity_seconds, candle_start_utc)`.
  - **Boundary Reconciliation (`BLOCK`)**: Reconciles candle timestamps against planned time intervals, catching out-of-bounds outliers.
  - **Price Return Anomaly (`WARN`)**: Detects anomalous hourly price moves exceeding 15% (both intrabar and interbar).
  - **Volume Spike Anomaly (`WARN`)**: Identifies volume spikes exceeding 5x rolling/batch baseline.
- **DuckDB Quality Audit Table (`quality_check_results`)**: Persists check outcomes, metric values, evaluated thresholds, and severity classifications.
- **Platform Health Check (`bitcoin-data status --check`)**:
  - Evaluates four health pillars: watermark freshness $\le 2.0$ hours, zero data gaps, disk headroom ($< 80\%$), and zero active error locks.
  - Returns exit code `0` when healthy and exit code `1` when degraded.
  - Terminal-friendly formatted text mode via `--format text`.
- **Low-Noise Failure Alerting**:
  - Automatically triggered via systemd `OnFailure=bitcoin-data-failure@%n.service`.
  - Sub-command `bitcoin-data alert --failed-unit %I` scrubs sensitive credentials/paths and throttles repeated alerts within a 2-hour window to prevent alert fatigue.

For incident remediation playbooks and data quality triage, see the **[Data Quality Runbook](docs/runbooks/DATA_QUALITY_RUNBOOK.md)**.

## Production Deployment & Scheduling (systemd)

Phase 4 operationalizes the pipeline on a single-host Linux VPS using native `systemd` service and timer units:

- **Timer Unit (`infra/systemd/bitcoin-data.timer`):** Triggers incremental ingestion hourly at 10 minutes past the hour (`OnCalendar=*-*-* *:10:00`) with randomized jitter (`RandomizedDelaySec=120`) and catch-up on reboot (`Persistent=true`).
- **Service Unit (`infra/systemd/bitcoin-data.service`):** Executes `bitcoin-data incremental` as a supervised `oneshot` service under a dedicated unprivileged `bitcoin-data` system account.
- **Security & Sandboxing:** Hardened with `ProtectSystem=strict`, `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectHome=true`, resource limits (`MemoryMax=1G`, `CPUQuota=100%`), and filesystem write boundaries confined strictly to `/srv/data/bitcoin-data-platform` and `/tmp`.
- **Structured Journald Logging:** Streams JSON logs directly to systemd's journal.

For step-by-step user provisioning, permission setup, unit installation, failure recovery, and rollback instructions, see the **[Deployment & Operations Runbook](docs/runbooks/DEPLOYMENT_RUNBOOK.md)**.

## Phase 6: Continuous Integration, Reproducible Packaging & Release

Phase 6 establishes automated CI/CD pipelines, dependency vulnerability scanning, deterministic packaging, and an operational release runbook:

- **Continuous Integration (`.github/workflows/ci.yml`)**:
  - Automatically triggers on every push and pull request targeting `main`.
  - Executes linting (`ruff check`), formatting verification (`ruff format --check`), strict static type checking (`mypy src`), offline unit and regression tests (`pytest -m "not integration"`), and package build validation (`python -m build`).
- **Security & Dependency Audit (`.github/workflows/security.yml`)**:
  - Scans dependencies against known vulnerability databases (`pip-audit`) on pushes, PRs, and a weekly scheduled cron (`0 4 * * 1`).
- **Deterministic Packaging & Dependency Locking**:
  - `requirements.lock`: Fully pinned runtime dependency tree guaranteeing reproducible execution across environments.
  - `make build`: Produces distribution wheel (`.whl`) and source distribution (`.tar.gz`) via `hatchling`.
  - `make audit`: Runs local dependency security audit via `pip-audit`.
  - `make clean-dist`: Cleans build directories and distribution artifacts.
- **Container Packaging & Parity Evaluation (`Dockerfile`, `.dockerignore`)**:
  - Multi-stage container build (`python:3.12-slim`) producing an isolated runtime environment with non-root user `bitcoin-data` (UID 1001) and volume boundary `/srv/data/bitcoin-data-platform`.
  - Architectural comparison documented in **[ADR D-008: Container Evaluation](docs/decisions/D-008_CONTAINER_EVALUATION.md)**.
- **Release Standard Operating Procedure**:
  - Documented release tagging (`v0.1.0`), clean-room virtualenv verification, zero-downtime deployment, and rollback procedures in the **[Release & Deployment Runbook](docs/runbooks/RELEASE_RUNBOOK.md)**.

## Phase 7: Second Data Domain (Coin Metrics On-Chain) & Conformed Modeling

Phase 7 incorporates a second, heterogeneous data domain: on-chain daily network activity from the Coin Metrics Community API v4 (`TxCnt` - Transaction Count, and `AdrActCnt` - Active Address Count). It establishes conformed dimensional modeling in DuckDB to bridge the grain mismatch (hourly market trading vs. daily network activity) while maintaining zero source coupling and independent watermarks:

- **Coin Metrics Client & Contract (`sources/coin_metrics_client.py`, `sources/coin_metrics_contract.py`)**:
  - Unauthenticated HTTP client targeting `https://community-api.coinmetrics.io/v4/timeseries/asset-metrics`.
  - Enforces 600ms request spacing (10 req / 6s rate limit), exponential backoff with jitter on 429/5xx, and pagination handling.
  - Strict contract validation: required fields, midnight UTC boundary alignment, and non-negative counts.
- **On-Chain Curated Parquet Storage (`storage/network_parquet_writer.py`)**:
  - Annual partitions at `curated/onchain/network_metrics_daily/source=coin_metrics/year=YYYY/data.parquet`.
  - PyArrow typed schema (`int64` transaction and active address counts).
  - Atomic temporary-file replacement and idempotent deduplication on `(source, asset, metric_date_utc)`.
- **Conformed DuckDB Views (`storage/duckdb_manager.py`)**:
  - `fact_network_metrics_daily`: Fact view reading on-chain Parquet files via Hive partitioning with typed empty fallback.
  - `mart_btc_market_and_network_daily`: Conformed cross-domain mart performing a `FULL OUTER JOIN` between `mart_btc_usd_daily` and `fact_network_metrics_daily` on UTC date (`trade_date_utc`), calculating `tx_per_active_address` and preserving market completeness flags.
  - Independent watermark tracking for `coin_metrics_daily`.
- **Architecture Decision Record**:
  - **[ADR D-009: dbt-core Evaluation](docs/decisions/D-009_DBT_EVALUATION.md)**: Objective architectural evaluation comparing `dbt-core` adoption vs. managed native DuckDB SQL views for single-host pipelines.

### Fetch on-chain network metrics

```bash
bitcoin-data fetch-network \
  --start 2026-01-01 \
  --end 2026-01-07 \
  --output-dir ./data/raw/coin_metrics
```

### Promote on-chain network metrics

```bash
bitcoin-data promote-network \
  --raw-dir ./data/raw/coin_metrics \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```

### Query conformed cross-domain mart

```bash
bitcoin-data query --db-path ./data/state/platform.duckdb \
  --sql "SELECT trade_date_utc, market_close_usd, market_volume_btc, transaction_count, active_addresses_count, tx_per_active_address FROM mart_btc_market_and_network_daily ORDER BY trade_date_utc DESC LIMIT 7;"
```

## Phase 8: Research Serving Layer, Multi-Format Exporter & Query Catalog

Phase 8 establishes an ergonomic, decoupled research serving layer designed for reproducible quantitative analysis and downstream research consumption:

- **Parameterized Query Service (`serving/query_service.py`)**:
  - Secure parameterized SQL execution supporting named parameter bindings (`:start_date`, `:end_date`, `:asset`) translated safely to native DuckDB bindings.
  - Parameter validation and injection defense preserving PostgreSQL/DuckDB type casts (`::TYPE`).
  - Query file loader supporting version-controlled analytical models.
- **Multi-Format Analytical Exporter (`serving/exporter.py`)**:
  - Serializes PyArrow tables into `parquet` (Snappy compression), `arrow` (IPC stream), `csv` (RFC 4180), and `json` formats.
  - Atomic filesystem write operations (`.tmp` write followed by `os.replace`) with failure cleanup.
  - In-memory serialization returning byte payloads for headless execution and programmatic consumption.
- **Version-Controlled Query Catalog (`queries/`)**:
  - `queries/daily_market_summary.sql`: Daily BTC-USD OHLCV summary, inter-day return, and intraday high-low volatility spread.
  - `queries/onchain_network_activity.sql`: On-chain transaction counts, active address metrics, network velocity ratios, and day-over-day deltas.
  - `queries/cross_domain_market_network.sql`: Parameterized cross-domain query joining market price action and network throughput with completeness filtering.
- **Official Data Dictionary (`docs/data_dictionary/DATA_DICTIONARY.md`)**:
  - Formal documentation of granularities, primary keys, nullability, data types, and semantic definitions for `fact_market_candle_hourly`, `mart_btc_usd_daily`, `fact_network_metrics_daily`, and `mart_btc_market_and_network_daily`.
- **Reproducible Research Notebooks (`notebooks/`)**:
  - `notebooks/README.md`: Step-by-step clean-room research setup guide.
  - `notebooks/bitcoin_research_baseline.ipynb`: Valid Jupyter notebook analyzing market vs. on-chain interactions with zero embedded ETL code or hardcoded credentials.

### Query CLI with Multi-Format Export

Execute parameterized queries and export to diverse formats:

```bash
# Export parameterized cross-domain query to Parquet
bitcoin-data query \
  --db-path ./data/state/platform.duckdb \
  --file queries/cross_domain_market_network.sql \
  --param start_date=2026-01-01T00:00:00Z \
  --param end_date=2026-01-31T23:59:59Z \
  --format parquet \
  --output ./data/exports/cross_domain_jan2026.parquet

# Query daily market summary and output CSV to stdout
bitcoin-data query \
  --db-path ./data/state/platform.duckdb \
  --file queries/daily_market_summary.sql \
  --format csv

# Ad-hoc SQL query with Arrow IPC export
bitcoin-data query \
  --db-path ./data/state/platform.duckdb \
  --sql "SELECT * FROM mart_btc_market_and_network_daily LIMIT 10;" \
  --format arrow \
  --output ./data/exports/sample.arrow
```

## Phase 9: Real-Time WebSocket Trade Streaming Experiment & Reconciliation

Phase 9 implements an isolated, bounded real-time trade capture collector and latency benchmarking experiment using the Coinbase Exchange WebSocket feed (`wss://ws-feed.exchange.coinbase.com`):

- **Strict Experimental Isolation (`streaming/`)**:
  - Operates completely outside the production batch pipeline; never writes to curated Parquet partitions (`curated/market/`, `curated/onchain/`) or alters batch watermarks.
  - Production batch remains the single source of truth for historical candles and OLAP models.
- **Async WebSocket Connection Supervisor (`streaming/connection.py`)**:
  - Connects to public WebSocket feed with automatic channel subscription (`matches`, `heartbeat`).
  - Exponential backoff reconnection with full jitter (`min(30s, base * 2^attempt)`).
- **Bounded In-Memory Buffer (`streaming/buffer.py`)**:
  - Strictly limited to 20,000 events or 32 MiB to prevent memory pressure on single-host VPS.
  - Applies `drop-newest` overflow policy with explicit loss accounting counters.
- **Atomic Micro-Batch Persistence (`streaming/writer.py`)**:
  - Persists raw trade micro-batches to immutable JSON Lines segments (`part-<batch_id>.jsonl`) using atomic rename (`.partial` -> `.jsonl`) and `os.fsync()`.
  - Generates atomic commit receipts (`commits/<batch_id>.json`) with SHA-256 checksums and sequence bounds.
  - Enforces minimum free disk headroom guard threshold (default: 50 MiB).
- **Synthetic Candle Replay Engine (`streaming/candles.py`)**:
  - Deterministic replay of committed trade segments to generate synthetic 1-minute and 1-hour OHLCV candles exported to Parquet (`derived/candles_1m.parquet`, `derived/candles_1h.parquet`).
  - Automatic deduplication on `trade_id` and tie-breaker sorting on timestamp and sequence.
- **Post-Capture REST Reconciliation (`streaming/reconcile.py`)**:
  - Evaluates synthetic streaming candles against official Coinbase REST API candles for settled windows.
  - Computes $\Delta\text{Open}, \Delta\text{High}, \Delta\text{Low}, \Delta\text{Close}$, and $\Delta\text{Volume}$ and classifies windows as `matched`, `mismatched`, or `partial_capture`.

### Stream CLI Command

Run a bounded trade streaming capture session:

```bash
# Capture 60 seconds of live trades with post-capture REST reconciliation
bitcoin-data stream \
  --duration 60 \
  --output-dir ./data/raw/streaming \
  --reconcile
```

Each run generates an isolated artifact directory under `<output-dir>/runs/<run_id>/`:
```text
<output-dir>/runs/<run_id>/
├── manifest.json               # Run parameters, start/end timestamps, exit code
├── events/part-<batch_id>.jsonl # Immutable raw trade segments
├── commits/<batch_id>.json     # Commit receipts with checksums and row counts
├── derived/candles_1m.parquet  # Synthetic 1-minute candles
├── derived/candles_1h.parquet  # Synthetic 1-hour candles
└── reports/summary.json        # Latency percentiles (p50/p95/p99), loss metrics, reconciliation deltas
```

## Phase 10: Lakehouse and Distributed Compute Evolution

Phase 10 implements an evidence-led, zero-daemon transactional Lakehouse table engine in `src/bitcoin_data_platform/lakehouse/`:
- **SQLite Transactional Metadata Catalog (`lakehouse/catalog.py`)**:
  - Embedded zero-daemon catalog in `platform_catalog.sqlite` operating with WAL mode and optimistic concurrency control (OCC).
  - Tracks table namespaces, monotonic snapshot IDs, parent commit lineage, and active manifest files.
- **Multi-Asset Analytical Table (`lakehouse/table.py` & `lakehouse/writer.py`)**:
  - Native multi-asset partitioning supporting `BTC-USD`, `ETH-USD`, and other trading pairs in Hive-style directories (`product_id=BTC-USD/`).
  - High-precision decimal columns (`Decimal128(38, 18)`) for prices and sizes with timezone-aware UTC timestamps.
  - Non-blocking concurrent reads: readers always access an immutable snapshot state while active writes commit new snapshots atomically.
- **Bin-Packing Compaction Engine (`lakehouse/compaction.py`)**:
  - Solves the small-file fragmentation problem from real-time streaming micro-batches (Phase 9).
  - Deterministically bin-packs and merges files below target size (default 128 MiB) per partition into optimal columnar files without data loss or value mutation.
- **Historical Time-Travel Querying (`lakehouse/table.py`)**:
  - Direct as-of querying by snapshot ID (`table.read_snapshot(id)`) or historical UTC timestamp (`table.read_as_of(time)`).
  - In-memory DuckDB integration (`table.to_duckdb()` / `table.query()`) for high-performance SQL analytical backtesting.
- **Snapshot Retention & Safe Vacuum (`lakehouse/retention.py`)**:
  - Metadata expiration for obsolete snapshots while strictly protecting branch heads.
  - Safe garbage collection of unreferenced orphan Parquet files with retention window safety guards.

### Lakehouse CLI Subcommands

```bash
# 1. Initialize a multi-asset Lakehouse table
bitcoin-data lakehouse init \
  --table trades \
  --catalog-dir ./data/lakehouse/catalog

# 2. Transactionally append a batch of trades
bitcoin-data lakehouse write \
  --table trades \
  --input-file data/raw/streaming/events/part-001.jsonl \
  --catalog-dir ./data/lakehouse/catalog

# 3. Compact fragmented micro-batches per partition
bitcoin-data lakehouse compact \
  --table trades \
  --target-size-mb 128 \
  --catalog-dir ./data/lakehouse/catalog

# 4. Query historical dataset state as of a snapshot or timestamp
bitcoin-data lakehouse time-travel \
  --table trades \
  --as-of-snapshot 2 \
  --catalog-dir ./data/lakehouse/catalog

# 5. Purge unreferenced orphan files and expire historical snapshots
bitcoin-data lakehouse vacuum \
  --table trades \
  --retain-days 7 \
  --catalog-dir ./data/lakehouse/catalog
```

### Operational Diagnostics & Self-Healing (Phase 11)

Phase 11 provides an auditable, out-of-band operational diagnostics engine and bounded self-healing framework (`src/bitcoin_data_platform/diagnostics/`).
It inspects telemetry across DuckDB run metadata, quality checks, Lakehouse SQLite catalog, disk usage, and alert state strictly using 100% read-only connections. It classifies incidents against a 6-part deterministic failure taxonomy (INC-01..INC-06), generates audit reports, and simulates or executes safe, bounded self-healing actions.

```bash
# 1. Diagnose system health and print human-readable triage summary
bitcoin-data diagnose --format text

# 2. Output machine-readable JSON telemetry and incident report
bitcoin-data diagnose --format json

# 3. Target root-cause investigation for a specific failed run ID and persist report
bitcoin-data diagnose \
  --run-id 843195da-79aa-4df7-8094-0cfc3b7a58ad \
  --output ./reports/incidents/report.json

# 4. Simulate bounded remediation plans (default dry-run mode)
bitcoin-data diagnose --dry-run

# 5. Execute bounded self-healing actions (clear stale lock, gap backfill, compaction)
bitcoin-data diagnose --auto-heal
```

### Investment Data Expansion (Phase 12)

Phase 12 expands the data platform with on-chain valuation, market sentiment, and macroeconomic calendar data required for evidence-based investment signals:
1. **Coin Metrics MVRV Extension**: Ingests `CapMVRVCur` (Market Value to Realized Value) into curated Parquet partitions and `fact_network_metrics_daily`.
2. **Crypto Fear & Greed Index (`fetch-sentiment`)**: Daily sentiment tracking (0–100) from Alternative.me persisted to `raw_crypto_sentiment_daily`.
3. **ForexFactory Macro Calendar (`fetch-macro-calendar`)**: Scheduled high-impact US economic events (FOMC, CPI, NFP) persisted to `raw_macro_economic_events`.
4. **Investment Signals Mart (`mart_btc_investment_signals_daily`)**: Conformed view joining market OHLCV, 200-day rolling SMA, Mayer Multiple, MVRV ratio, sentiment, and macro event flags into deterministic allocation signals (`AGGRESSIVE_ACCUMULATE`, `OPPORTUNISTIC_ACCUMULATE`, `STANDARD_DCA`, `DEFENSIVE_RESERVE`, `HARD_FREEZE`).

```bash
# 1. Fetch current Crypto Fear & Greed Index
bitcoin-data fetch-sentiment --limit 1

# 2. Fetch last 7 days of sentiment history
bitcoin-data fetch-sentiment --limit 7

# 3. Fetch this week's scheduled USD High-impact economic calendar events
bitcoin-data fetch-macro-calendar
```

### Investment Signal Engine (Phase 13)

Phase 13 operationalizes tactical asset allocation with deterministic daily signal generation, automated breaking news event detection, and Telegram alert delivery:
1. **Daily Signal Generator (`generate-signal`)**: Evaluates `mart_btc_investment_signals_daily` to compute regime signals (`AGGRESSIVE_ACCUMULATE`, `OPPORTUNISTIC_ACCUMULATE`, `STANDARD_DCA`, `DEFENSIVE_RESERVE`, `HARD_FREEZE`), multi-indicator agreement strength (`STRONG`, `MODERATE`, `WEAK`), and localized narratives in Bahasa Indonesia, with audit trail persistence to `signal_history`.
2. **CoinDesk News Sentinel (`news-sentinel`)**: Scans CoinDesk RSS feed with regex keyword rules for critical events (hacks, exploits, insolvency, SEC enforcement) and warning events (ETF decisions, FOMC, CPI, stablecoin depegs), with SHA-256 deduplication in `news_sentinel_alerts`.
3. **Telegram Alert Dispatcher (`send-alert`)**: Formats and dispatches investment signals and emergency alerts via Telegram Bot API with dry-run support, transient retry backoff, and credential redaction.

```bash
# 1. Generate latest investment signal (human-readable)
bitcoin-data generate-signal

# 2. Generate signal as JSON and persist to DuckDB signal_history
bitcoin-data generate-signal --json --save

# 3. Generate signal for a specific historical date
bitcoin-data generate-signal --date 2026-09-18

# 4. Scan CoinDesk RSS feed for market-moving events
bitcoin-data news-sentinel

# 5. Preview investment signal Telegram message (dry-run)
bitcoin-data send-alert --type signal --dry-run

# 6. Preview emergency news alert Telegram message (dry-run)
bitcoin-data send-alert --type news --dry-run
```

### Quantitative Backtest & Validation Engine (Phase 14)

Phase 14 delivers an institutional-grade, event-driven backtesting and quantitative validation engine (`src/bitcoin_data_platform/backtest/`) to simulate systematic investment strategies across multi-year market cycles without lookahead bias:
1. **Dynamic Reserve DCA + Macro Regime Overlay**: 70% Base DCA Pool + 30% Tactical Reserve Pool with dynamic accumulation multipliers (0.0x to 2.0x + 25% tactical reserve draw) driven by Mayer Multiple, MVRV, Fear & Greed sentiment, and high-impact macro circuit breakers.
2. **Blind DCA**: Naive periodic dollar-cost averaging executed unconditionally on schedule.
3. **Lump Sum Buy & Hold**: 100% initial capital deployed at inception ($T_0$) with zero subsequent contributions.
4. **Institutional Metrics**: Total Return (%), CAGR (365-day basis), Maximum Drawdown (MDD %), continuous annualized Sharpe Ratio, Sortino Ratio (downside semivariance), Calmar Ratio, and BTC Acquisition Cost Discount (%).

```bash
# 1. Benchmark all three strategies side-by-side (ASCII table)
bitcoin-data backtest

# 2. Run simulation over specific historical date range with weekly DCA injections
bitcoin-data backtest \
  --start 2024-01-01 \
  --end 2026-01-01 \
  --frequency weekly \
  --periodic-amount 250.0

# 3. Simulate single strategy and output as JSON
bitcoin-data backtest \
  --strategy dynamic-reserve \
  --format json

# 4. Generate GitHub-flavored Markdown benchmark report to file
bitcoin-data backtest \
  --format markdown \
  --output ./reports/backtest_benchmark.md
```

### Web UI Dashboard & API Server

The platform includes a zero-dependency, lightweight web dashboard ("Bitcoin Market Hub") served using Python's standard library `ThreadingHTTPServer` (`src/bitcoin_data_platform/dashboard/`).
It renders a responsive dark-mode technical UI featuring 3 balanced KPI cards (Spot Price, 24h Volume, Blockchain Network Activity), interactive Chart.js price & volume history, recent trade tape, conformed daily ledger table with search & pagination, and direct CSV export endpoints.

```bash
# 1. Launch the Web UI Dashboard on default port (http://127.0.0.1:8080)
bitcoin-data dashboard --port 8080

# 2. Bind to a specific host and custom DuckDB database path
bitcoin-data dashboard \
  --host 127.0.0.1 \
  --port 8080 \
  --db-path ./data/state/platform.duckdb
```

#### API Endpoints

- `GET /`: Serves the production dashboard single-page web interface.
- `GET /api/kpi?asset=BTC`: Returns real-time 3-card KPI metrics from DuckDB `mart_btc_market_and_network_daily`.
- `GET /api/chart?asset=BTC&range=30D`: Returns historical timeseries (OHLCV) from `mart_btc_usd_daily` / `fact_market_candle_hourly`.
- `GET /api/trades?asset=BTC`: Returns recent trade executions.
- `GET /api/ledger?asset=BTC&limit=30`: Returns daily conformed cross-domain ledger rows.
- `GET /api/export?format=csv&asset=BTC`: Serves direct CSV file download with `Content-Disposition` header.
- `GET /api/portfolio`: Returns JSON consolidated summary metrics for forward paper trading.
- `GET /api/portfolio/equity?limit=90`: Returns daily equity curve history comparing Dynamic Reserve DCA vs Buy & Hold benchmark.
- `GET /api/portfolio/trades?limit=50`: Returns executed order blotter records from `paper_trade_ledger`.
- `GET /api/macro/radar`: Returns latest synthesized Macro Radar metrics, Composite MNI gauge, 3-tier subscores, and regime.
- `GET /api/macro/news?limit=20`: Returns curated multi-source news items with verified clickable original hyperlinks (`target="_blank"`).
- `GET /api/macro/calendar?days=7`: Returns macroeconomic announcements with evaluated economic surprise deltas and liquidity directional biases.

### Forward Paper Trading Simulation (Phase 15)

Forward-testing paper trading engine initialized with **$1,000 USD virtual capital** (partitioned into $700 Base Cash and $300 Tactical Reserve). Evaluates daily systematic accumulation according to `DynamicReserveDCAStrategy` against live market data, guarded by institutional `RiskGuard` pre-trade validation controls and a filesystem kill-switch (`data/state/PAPER_KILL_SWITCH`).

```bash
# 1. Initialize paper portfolio with $1,000 virtual capital
bitcoin-data paper init --initial-cash 1000.0

# 2. Advance simulation by one day executing systematic DCA logic
bitcoin-data paper step --daily-budget 10.0

# 3. Step on a specific historical date with custom spot price
bitcoin-data paper step --date 2026-09-18 --daily-budget 15.0 --force-price 85000.0

# 4. View consolidated Fincept Terminal-style portfolio summary
bitcoin-data paper status --format table

# 5. Export portfolio metrics as JSON
bitcoin-data paper status --format json

# 6. Reset portfolio to initial capital (requires explicit --force)
bitcoin-data paper reset --initial-cash 1000.0 --force
```

### Macro & Narrative Intelligence Engine (Phase 16)

A 3-tier institutional intelligence engine synthesizing Hard Macro surprises (ForexFactory CPI/NFP/FOMC), on-chain valuation sentiment (Fear & Greed, MVRV, Mayer Multiple), and 4-pillar narrative polarity (CoinDesk, Cointelegraph, Decrypt, Bitcoin Magazine) into a **Composite Macro-Narrative Index (MNI)** with 5-regime classification and automated AutoHedge-inspired `RiskGuard` circuit breakers.

```bash
# 1. Ingest and classify latest articles from curated RSS feeds
bitcoin-data macro fetch-news

# 2. Fetch high-impact macroeconomic calendar events and compute surprises
bitcoin-data macro fetch-calendar

# 3. Execute daily 3-tier synthesis and persist MNI report to DuckDB
bitcoin-data macro synthesize

# 4. Render terminal Macro Radar status and sentiment report
bitcoin-data macro radar --format text

# 5. Output Macro Radar state as JSON
bitcoin-data macro radar --format json

# 6. Check health and statistics of the macro intelligence pipeline
bitcoin-data macro status
```

## Quality gates

Run the documented quality gates:

```bash
# Run all quality gates (linter, type checker, tests)
make check

# Or run individual checks:
make lint       # ruff check and ruff format --check
make format     # ruff auto-formatting
make typecheck  # mypy strict static typing
make test       # pytest test suite

# Packaging and security audits:
make build      # build distribution packages (wheel and sdist)
make audit      # audit dependencies for CVE vulnerabilities
make clean-dist # remove build and packaging artifacts
```

## Current documents

- [Master plan](docs/MASTER_PLAN.md)
- [Architecture V1](docs/architecture/ARCHITECTURE_V1.md)
- [Decision log](docs/decisions/README.md)
- [Roadmap](docs/roadmap/ROADMAP.md)
- [Phase 1A Specification](docs/specs/PHASE_1A_BOOTSTRAP_WINDOW_PLANNER.md)
- [Phase 1B Specification](docs/specs/PHASE_1B_COINBASE_CLIENT_RAW_INGESTION.md)
- [Phase 2 Specification](docs/specs/PHASE_2_CURATED_PARQUET_DUCKDB.md)
- [Phase 3 Specification](docs/specs/PHASE_3_INCREMENTAL_WATERMARK.md)
- [Phase 4 Specification](docs/specs/PHASE_4_SINGLE_HOST_ORCHESTRATION.md)
- [Phase 5 Specification](docs/specs/PHASE_5_OBSERVABILITY_DATA_QUALITY.md)
- [Phase 6 Specification](docs/specs/PHASE_6_REPRODUCIBLE_DELIVERY_CICD.md)
- [Phase 7 Specification](docs/specs/PHASE_7_SECOND_DOMAIN_CONFORMED_MODELING.md)
- [Phase 8 Specification](docs/specs/PHASE_8_RESEARCH_SERVING_LAYER.md)
- [Phase 9 Specification](docs/specs/PHASE_9_WEBSOCKET_STREAMING.md)
- [Phase 10 Specification](docs/specs/PHASE_10_LAKEHOUSE_EVOLUTION.md)
- [Phase 11 Specification](docs/specs/PHASE_11_OPERATIONAL_DIAGNOSTICS.md)
- [Phase 12 Specification](docs/specs/PHASE_12_INVESTMENT_DATA_EXPANSION.md)
- [Phase 13 Specification](docs/specs/PHASE_13_INVESTMENT_SIGNAL_ENGINE.md)
- [Phase 14 Specification](docs/specs/PHASE_14_BACKTEST_VALIDATION.md)
- [Phase 15 Specification](docs/specs/PHASE_15_FORWARD_PAPER_TRADING_DASHBOARD.md)
- [Phase 16 Specification](docs/specs/PHASE_16_MACRO_NARRATIVE_INTELLIGENCE.md)
- [Official Data Dictionary](docs/data_dictionary/DATA_DICTIONARY.md)
- [ADR D-008: Container Evaluation](docs/decisions/D-008_CONTAINER_EVALUATION.md)
- [ADR D-009: dbt-core Evaluation](docs/decisions/D-009_DBT_EVALUATION.md)
- [ADR D-010: Streaming Experiment](docs/decisions/D-010_STREAMING_EXPERIMENT.md)
- [ADR D-011: Lakehouse Evolution](docs/decisions/D-011_LAKEHOUSE_EVOLUTION.md)
- [ADR D-012: Operational Diagnostics & Self-Healing](docs/decisions/D-012_DIAGNOSTICS_SELF_HEALING.md)
- [Operational Runbook (Phase 3)](docs/runbooks/OPERATIONAL_RUNBOOK.md)
- [Deployment Runbook (Phase 4)](docs/runbooks/DEPLOYMENT_RUNBOOK.md)
- [Data Quality Runbook (Phase 5)](docs/runbooks/DATA_QUALITY_RUNBOOK.md)
- [Release Runbook (Phase 6)](docs/runbooks/RELEASE_RUNBOOK.md)
- [Source evaluation](docs/sources/SOURCE_EVALUATION.md)

## Safety boundary

This project is for data engineering and Bitcoin research. It does not place trades, provide automated buy/sell decisions, expose a database publicly, or depend on external heuristic services for pipeline correctness.
