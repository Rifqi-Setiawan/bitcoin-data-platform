# Operational Runbook: Incremental Loading & Incident Recovery

**System:** Bitcoin Data Engineering Platform  
**Phase:** Phase 3 — Incremental Loads and Recovery  
**Target Grain:** Hourly (`3600s`), Product `BTC-USD`, Source `coinbase_exchange`  
**Storage Architecture:** Immutable Raw Envelopes (`.json.gz`) → Curated Annual Parquet → DuckDB Views  

---

## 1. Overview & Architecture Grain

The Bitcoin Data Engineering Platform is designed for deterministic, repeatable, and idempotent ingestion and analytical modeling of Bitcoin market data. The operational pipeline transitions data across three distinct architectural layers:

```text
┌────────────────────────────────────────────────────────┐
│               Coinbase Exchange REST API               │
└───────────────────────────┬────────────────────────────┘
                            │ (Deterministic Windows <= 300h)
                            ▼
┌────────────────────────────────────────────────────────┐
│             Raw Ingestion Layer (Immutable)            │
│         Path: ./data/raw/<run_id>_<range>.json.gz       │
│     Envelope: payload + provider request ID + SHA-256   │
└───────────────────────────┬────────────────────────────┘
                            │ (Normalization & Quality Invariants)
                            ▼
┌────────────────────────────────────────────────────────┐
│           Curated Analytical Layer (Parquet)           │
│   Path: ./data/curated/market/candles_hourly/...        │
│   Partitioning: source=coinbase_exchange/year=YYYY/    │
└───────────────────────────┬────────────────────────────┘
                            │ (Zero-Copy Columnar Scan)
                            ▼
┌────────────────────────────────────────────────────────┐
│             DuckDB Engine (Analytical Views)           │
│   - fact_market_candle_hourly                          │
│   - mart_btc_usd_daily                                 │
│   - pipeline_watermark & run_metadata                  │
└────────────────────────────────────────────────────────┘
```

### 1.1 Natural Key & Table Grain
Every candle in the curated layer represents a single completed hourly market interval:
- **Natural Key:** `(source, product_id, granularity_seconds, candle_start_utc)`
- **Granularity:** `3600` seconds (1 hour).
- **Product ID:** `BTC-USD`.
- **Source:** `coinbase_exchange`.
- **Interval Semantics:** Half-open interval `[start, end)`. The `candle_start_utc` timestamp represents the inclusive start boundary of the candle.

### 1.2 Idempotency and Deduplication
The platform enforces strict idempotency across all operations:
- When processing raw envelopes, records are deduplicated by their natural key.
- If duplicate candles appear across overlapping ingestions or re-runs, the newest ingested record takes precedence deterministically.
- Repetitive executions over the same time range converge to identical Parquet files and identical DuckDB view states.

### 1.3 Partitioning and Analytical Views
- **Parquet Storage:** Stored under Hive-partitioned directory layout:  
  `./data/curated/market/candles_hourly/source=coinbase_exchange/year=YYYY/*.parquet`
- **DuckDB Views:**  
  - `fact_market_candle_hourly`: Pointed directly to the Parquet file glob using `read_parquet(..., hive_partitioning=true)`.
  - `mart_btc_usd_daily`: Daily aggregation summarizing `open`, `high`, `low`, `close`, `volume_base`, `observed_hour_count`, and `is_complete` (boolean confirming whether all 24 hours are present).
  - `pipeline_watermark`: Tracks the high-water mark for incremental synchronization.
  - `run_metadata`: Detailed operational log tracking execution mode, timestamps, rows promoted, partitions written, and lock states.

---

## 2. Routine Operations

### 2.1 Initial Bootstrap & Backfill
When initializing the platform on a clean environment or ingesting long historical periods, execute a two-step bootstrap:

#### Step 1: Execute Raw Backfill
Plan windows and ingest raw envelopes from Coinbase Exchange:

```bash
bitcoin-data backfill \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-31T00:00:00Z \
  --output-dir ./data/raw \
  --db-path ./data/state/platform.duckdb
```

#### Step 2: Promote Raw Envelopes to Curated Storage
Normalize raw payloads, evaluate quality invariants, write Parquet partitions, and initialize DuckDB analytical views:

```bash
bitcoin-data promote \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```

*Note:* If no watermark exists prior to promotion, the `promote` command automatically sets the initial `watermark_utc` in `pipeline_watermark` to `MAX(candle_start_utc)`.

---

### 2.2 Scheduled Incremental Runs
The `incremental` command is the standard operational job designed for recurring execution (e.g. via hourly cron or systemd timer):

```bash
bitcoin-data incremental \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb \
  --overlap-hours 48
```

#### Execution Lifecycle:
1. **Lock Acquisition:** Checks `run_metadata` for active runs; registers a `RUNNING` record.
2. **Watermark Check:** Reads `watermark_utc` from `pipeline_watermark`. If absent, aborts with exit code `2`.
3. **Window Calculation:** Defines request interval `[start_utc, end_utc)`:
   - `start_utc = watermark_utc - overlap_hours` (default: 48 hours).
   - `end_utc = current_completed_hour_utc` (floored to `minute=0`, `second=0`, `microsecond=0`).
4. **Freshness Guard:** If `start_utc >= end_utc`, exits cleanly with code `0` (`"Nothing to fetch, data is fresh."`).
5. **Coinbase Fetch:** Downloads raw candle windows with exponential backoff retry.
6. **Envelope Storage:** Writes atomic `.json.gz` envelopes with SHA-256 validation.
7. **Full Promotion:** Reads all envelopes in `./data/raw`, applies schema and quality checks, updates Parquet partitions, and refreshes DuckDB views.
8. **Watermark Advancement:** Updates `watermark_utc` to the newest candle start timestamp.
9. **Lock Release:** Marks the run as `SUCCEEDED` in `run_metadata`.

---

### 2.3 Health & Status Monitoring
Monitor platform health, watermark freshness, storage metrics, and gaps using the `status` command:

```bash
bitcoin-data status \
  --db-path ./data/state/platform.duckdb \
  --curated-dir ./data/curated
```

#### Interpreting Status Output
The command returns a structured JSON payload:

```json
{
  "watermark_utc": "2026-01-30T23:00:00Z",
  "watermark_age_hours": 1.25,
  "last_run": {
    "run_id": "31b4028d-19df-416b-b461-460c5a08901b",
    "mode": "incremental",
    "status": "SUCCEEDED",
    "completed_at_utc": "2026-01-31T00:15:00Z",
    "rows_promoted": 720
  },
  "last_success": {
    "run_id": "31b4028d-19df-416b-b461-460c5a08901b",
    "completed_at_utc": "2026-01-31T00:15:00Z"
  },
  "last_failure": null,
  "curated_stats": {
    "total_rows": 720,
    "min_candle_utc": "2026-01-01T00:00:00Z",
    "max_candle_utc": "2026-01-30T23:00:00Z",
    "partitions": 1,
    "total_size_bytes": 45280
  },
  "gaps": [],
  "is_locked": false
}
```

#### Key Operational Thresholds:
- **`watermark_age_hours`**: Should normally be `<= 2.0` hours under hourly schedules. An age `> 3.0` indicates missed runs, source outages, or persistent lock contention.
- **`is_locked`**: Must be `false` when no job is running. If `true` for extended periods, check for hung processes.
- **`gaps`**: Must be empty (`[]`). Any entry indicates missing hours in the analytical layer.
- **`last_failure`**: Inspect immediately if not `null` or if `last_run.status == "FAILED"`.

---

## 3. Watermark Semantics

### 3.1 Watermark Definition
The watermark (`watermark_utc` in `pipeline_watermark`) represents the **upper boundary timestamp of completed, verified candles** successfully committed to the curated analytical storage.
- It is NOT the timestamp of the last executed request.
- It is NOT updated upon raw envelope fetch alone.
- It is updated **only after** raw records are normalized, validated against all quality assertions, written to Parquet, and exposed in DuckDB views.

### 3.2 Monotonicity Rule
The watermark is **strictly monotonic non-decreasing**.
- It moves forward during successful `incremental` or `promote` runs when newer candles are processed.
- It **never decreases**, even when running `repair` or historical backfills covering older time windows.
- This ensures downstream consumers and future incremental runs never lose track of pipeline progress.

### 3.3 48-Hour Overlap Policy
Incremental runs default to `--overlap-hours 48` for two reasons:
1. **Source Revisions:** Cryptocurrency exchanges (including Coinbase) occasionally post late-settled trades or restate recent hourly candle aggregations. Re-fetching the trailing 48 hours ensures recent adjustments are ingested.
2. **Missing Windows:** If an intermediate scheduled job failed or was skipped during network degradation, the overlap interval acts as an automated rolling catch-up window without requiring manual operator intervention.
3. **Idempotent Merge:** Because the curated promotion upserts data based on the candle natural key, overlapping data seamlessly refreshes existing records without creating duplicates.

---

## 4. Incident Response & Recovery Procedures

### Scenario 1: Coinbase API Outage or Network Degradation (Exit Code 3)
*Symptom:* Incremental or backfill run terminates with exit code `3` and logs `Coinbase source unavailable`.

#### Diagnostic Steps:
1. Check last failure details:
   ```bash
   bitcoin-data status --db-path ./data/state/platform.duckdb --curated-dir ./data/curated
   ```
2. Inspect structured error events on stderr:
   ```json
   {"level": "ERROR", "event": "coinbase_request_error", "status_code": 503, "retry_count": 3}
   ```
3. Verify external connectivity to Coinbase public API:
   ```bash
   curl -I https://api.exchange.coinbase.com/products/BTC-USD/candles
   ```

#### Fail-Safe Behavior:
- In-flight batches are aborted.
- No partial or corrupted envelopes are written.
- The pipeline lock is released with status `FAILED`.
- The watermark **does not advance**.

#### Recovery Procedure:
Once network connectivity or API availability is restored:
```bash
# Re-run incremental ingestion; it will resume from the existing watermark
bitcoin-data incremental \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb \
  --overlap-hours 48
```

---

### Scenario 2: Data Contract or Quality Check Violation (Exit Code 4)
*Symptom:* Pipeline terminates with exit code `4`. Stderr reports contract violations or quality invariant failures.

#### Common Root Causes:
- **Contract Violation:** Upstream Coinbase API returned non-conforming JSON, missing columns, or invalid candle tuple formats.
- **Quality Invariant Violation:**
  - `low > min(open, close)` or `high < max(open, close)`.
  - Non-positive or negative prices / negative volume.
  - Timestamp ordering or duplicate natural keys within a single batch.

#### Diagnostic Steps:
1. Read the exact failure reason from stderr:
   ```text
   error: quality failure: Invariant violation: high must be >= max(open, close)
   ```
2. Inspect the latest failed run in DuckDB:
   ```bash
   bitcoin-data query \
     --db-path ./data/state/platform.duckdb \
     --sql "SELECT run_id, mode, status, error_message FROM run_metadata ORDER BY started_at_utc DESC LIMIT 1"
   ```

#### Fail-Safe Behavior:
- The entire promotion batch is rejected.
- Curated Parquet files remain untouched.
- Watermark is frozen at the last known valid state.

#### Recovery Procedure:
1. If the violation is due to corrupted raw envelopes on disk, identify and isolate the affected file:
   ```bash
   # Move suspicious envelope out of raw directory for offline inspection
   mv ./data/raw/<corrupted_file>.json.gz /tmp/quarantine/
   ```
2. Re-run `promote` or `repair` to verify curated consistency.

---

### Scenario 3: Concurrent Run Lock or Stale Lock (Exit Code 6)
*Symptom:* Job exits immediately with exit code `6` (`error: concurrent run detected`).

#### Root Causes:
1. **Active Overlap:** Another scheduled cron or manual command is currently running.
2. **Stale Lock:** A previous run was terminated abruptly (e.g. OS OOM-killer, power disruption, or unhandled `SIGKILL`) before releasing the lock in `run_metadata`.

#### Diagnostic Steps:
1. Check if a pipeline process is actively executing:
   ```bash
   ps aux | grep "[b]itcoin-data"
   ```
2. Check lock status and timestamp:
   ```bash
   bitcoin-data query \
     --db-path ./data/state/platform.duckdb \
     --sql "SELECT run_id, mode, started_at_utc, status FROM run_metadata WHERE status = 'RUNNING'"
   ```

#### Recovery Procedure:
- **If an active process is legitimately running:** Wait for it to complete. Do not intervene.
- **If no process is running (stale lock):**
  Use the `--force` option of the `repair` command to clear any stale locks older than 1 hour (3600s) and restore curated consistency:
  ```bash
  bitcoin-data repair \
    --raw-dir ./data/raw \
    --curated-dir ./data/curated \
    --db-path ./data/state/platform.duckdb \
    --force
  ```
  The `--force` option automatically transitions stale `RUNNING` rows to `FAILED` with `error_message = 'Cleared by force_clear_lock'`.

---

### Scenario 4: Curated Storage Corruption or Desynchronization
*Symptom:* Parquet files in `./data/curated` are accidentally deleted, partially overwritten, or DuckDB views report file-not-found errors.

#### Diagnostic Steps:
1. Test DuckDB curated queries:
   ```bash
   bitcoin-data query \
     --db-path ./data/state/platform.duckdb \
     --sql "SELECT COUNT(*) FROM fact_market_candle_hourly"
   ```
2. Inspect directory integrity:
   ```bash
   find ./data/curated/market/candles_hourly -type f -name "*.parquet"
   ```

#### Recovery Procedure (Full Rebuild):
The raw layer (`./data/raw`) is the immutable single source of truth. The curated layer is entirely disposable and reproducible.
Execute `repair` to perform a clean rebuild from all raw envelopes:

```bash
bitcoin-data repair \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```

#### Under the Hood:
- Acquires run lock.
- Scans and reads every `.json.gz` envelope in `./data/raw`.
- Normalizes and runs analytical quality invariants on all candles.
- Completely deletes old Parquet files under `./data/curated/market/candles_hourly/source=*/year=*/*.parquet`.
- Atomically writes fresh Parquet partitions.
- Re-registers DuckDB views (`fact_market_candle_hourly`, `mart_btc_usd_daily`).
- **Watermark Preservation:** Does **not** lower or clear `pipeline_watermark`.
- Emits a summary JSON upon completion.

---

### Scenario 5: Detecting and Resolving Data Gaps
*Symptom:* The `status` command reports non-empty `gaps`:

```json
"gaps": [
  {
    "start_utc": "2026-01-10T00:00:00Z",
    "gap_start_utc": "2026-01-10T00:00:00Z",
    "end_utc": "2026-01-12T00:00:00Z",
    "gap_end_utc": "2026-01-12T00:00:00Z",
    "missing_hours": 47
  }
]
```

#### Root Causes:
- Outage or paused pipeline exceeding the 48-hour overlap window.
- Targeted downtime on Coinbase API during past intervals.

#### Recovery Procedure:
1. **Targeted Backfill:** Run a backfill covering the exact missing interval:
   ```bash
   bitcoin-data backfill \
     --start 2026-01-10T00:00:00Z \
     --end 2026-01-12T00:00:00Z \
     --output-dir ./data/raw \
     --db-path ./data/state/platform.duckdb
   ```
2. **Promote New Envelopes:**
   ```bash
   bitcoin-data promote \
     --raw-dir ./data/raw \
     --curated-dir ./data/curated \
     --db-path ./data/state/platform.duckdb
   ```
3. **Verify Resolution:**
   ```bash
   bitcoin-data status \
     --db-path ./data/state/platform.duckdb \
     --curated-dir ./data/curated
   ```
   Confirm that `"gaps": []` and `total_rows` increased by the missing candle count.

---

## 5. Command Reference & Exit Codes

### 5.1 CLI Command Reference

| Command | Purpose | Primary Flags |
|---|---|---|
| `plan-backfill` | Offline window planning | `--start`, `--end` |
| `backfill` | Fetch raw Coinbase data | `--start`, `--end`, `--output-dir`, `--db-path` |
| `promote` | Raw to Curated Parquet & DuckDB | `--raw-dir`, `--curated-dir`, `--db-path` |
| `query` | Run SQL query on DuckDB | `--db-path`, `--sql` |
| `incremental` | Watermark-based recurring ingest | `--raw-dir`, `--curated-dir`, `--db-path`, `--overlap-hours` |
| `status` | System health, watermark & gaps | `--db-path`, `--curated-dir` |
| `repair` | Full Parquet rebuild from raw | `--raw-dir`, `--curated-dir`, `--db-path`, `--force` |

### 5.2 Exit Code Standards

All commands adhere to standardized, distinct exit codes to facilitate automated orchestration and alerting:

| Exit Code | Classification | Description & Action |
|---|---|---|
| **`0`** | **Success** | Operation completed successfully. For `incremental`, also indicates data is already fresh (`nothing to fetch`). |
| **`2`** | **Invalid Input / Precondition** | Syntax error, unaligned hourly boundaries, inverted range, or **no watermark found** for `incremental` (run backfill & promote first). |
| **`3`** | **Source Unavailable** | Coinbase API network failure, 5xx server errors, or HTTP 429 rate limit exhausted after exponential backoff. Safe to retry. |
| **`4`** | **Contract / Quality Violation** | Coinbase payload schema mismatch or candle financial invariant failure (OHLC violation, negative volume). Fails safe without persisting bad data. |
| **`5`** | **Storage Failure** | Filesystem I/O error, disk full, corrupted gzip envelope read error, atomic file rename failure, or Parquet write error. |
| **`6`** | **Concurrency Conflict** | Active execution lock detected in `run_metadata`. Another run is active, or a stale lock must be resolved with `repair --force`. |

---

## 6. Verification & Acceptance Checklist

Before closing an operational maintenance window, run this verification suite:

```bash
# 1. Inspect status
bitcoin-data status --db-path ./data/state/platform.duckdb --curated-dir ./data/curated

# 2. Check for zero gaps and no active lock
# Ensure "gaps": [] and "is_locked": false

# 3. Query sample hourly candle
bitcoin-data query \
  --db-path ./data/state/platform.duckdb \
  --sql "SELECT candle_start_utc, open, high, low, close, volume_base FROM fact_market_candle_hourly ORDER BY candle_start_utc DESC LIMIT 3"

# 4. Check daily mart completeness
bitcoin-data query \
  --db-path ./data/state/platform.duckdb \
  --sql "SELECT trade_date_utc, observed_hour_count, is_complete FROM mart_btc_usd_daily ORDER BY trade_date_utc DESC LIMIT 3"
```
