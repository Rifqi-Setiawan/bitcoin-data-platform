# Phase 3 Implementation Specification — Incremental Loading and Recovery

Status: approved for implementation
Parent phase: Phase 3 — Incremental loads and recovery
Task ID: `P3-incremental-watermark`
Recommended branch: `hermes/P3-incremental-watermark`
Implementation profile: `btc-coder`
Verification profile: `btc-verifier`

## 1. Objective

Add incremental loading mode that fetches only recent completed candles using a watermark-based
overlap strategy, a status command for operational visibility, a repair command for recovery,
and run-level concurrency protection. After Phase 3, the platform can run repeatedly and safely
without reprocessing all historical data.

## 2. User stories

### Incremental load
```bash
bitcoin-data incremental \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```
Fetches from `(watermark - 48h overlap)` to `(current completed UTC hour)`, promotes to Parquet,
advances the watermark. Fails cleanly if no watermark exists yet.

### Status check
```bash
bitcoin-data status --db-path ./data/state/platform.duckdb --curated-dir ./data/curated
```
Shows last run, watermark, freshness, row counts, gaps, and disk usage as JSON.

### Repair
```bash
bitcoin-data repair \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```
Rebuilds curated Parquet from all raw envelopes. Does NOT lower watermark.

## 3. Scope

### In scope

- Watermark table in DuckDB (`pipeline_watermark`).
- Incremental CLI command with 48-hour configurable overlap.
- Watermark advances only after successful full promotion.
- No-watermark guard: incremental fails with clear message if no watermark exists.
- Run lock: prevent concurrent runs (DuckDB-based advisory lock via run_metadata RUNNING check).
- Status CLI command (JSON output).
- Repair CLI command (full rebuild from raw, does not lower watermark).
- Update run_metadata schema: add watermark columns, requested interval.
- Update promote command to set initial watermark after successful promotion.
- Comprehensive offline tests.

### Out of scope

- systemd timer scheduling (Phase 4).
- Alerting and observability beyond status command (Phase 5).
- CI/CD (Phase 6).

## 4. Functional contract

### 4.1 Watermark Table

Add to DuckDB via `duckdb_manager.py`:

```sql
CREATE TABLE IF NOT EXISTS pipeline_watermark (
    pipeline_id VARCHAR PRIMARY KEY DEFAULT 'btc_usd_hourly',
    watermark_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL,
    updated_by_run_id VARCHAR NOT NULL
);
```

**Semantics:**
- Watermark = latest completed candle timestamp successfully promoted with all quality checks passed.
- Moves monotonically forward in incremental mode.
- Repair/backfill runs do NOT lower it.
- Only one row for pipeline_id='btc_usd_hourly' in V1.

### 4.2 Run Lock

Before any run (backfill, incremental, promote, repair), check:
```sql
SELECT COUNT(*) FROM run_metadata WHERE status = 'RUNNING';
```
If > 0, exit with clear message and non-zero code (exit 6). This prevents concurrent corruption.

On run start, insert `RUNNING` row. On completion (success or failure), update to `SUCCEEDED` or `FAILED`.

**Stale lock recovery:** If a RUNNING row exists for > 1 hour, the repair command can force-clear it with `--force` flag.

### 4.3 Updated run_metadata Schema

```sql
CREATE TABLE IF NOT EXISTS run_metadata (
    run_id VARCHAR PRIMARY KEY,
    mode VARCHAR,
    started_at_utc TIMESTAMPTZ,
    completed_at_utc TIMESTAMPTZ,
    status VARCHAR,
    rows_promoted INTEGER,
    partitions_written INTEGER,
    raw_envelopes_read INTEGER,
    requested_start_utc TIMESTAMPTZ,
    requested_end_utc TIMESTAMPTZ,
    old_watermark_utc TIMESTAMPTZ,
    new_watermark_utc TIMESTAMPTZ,
    error_message VARCHAR
);
```

### 4.4 Incremental Command

Module updates: `cli.py`, `duckdb_manager.py`

**Flow:**
1. Acquire run lock (check no RUNNING runs, insert RUNNING row).
2. Read current watermark from `pipeline_watermark`.
3. If no watermark exists → exit 2 with message: "No watermark found. Run a backfill first."
4. Calculate interval: `start = watermark - overlap (default 48h)`, `end = current completed UTC hour`.
5. If start >= end → exit 0 with message: "Nothing to fetch, data is fresh."
6. Plan windows using existing planner.
7. Fetch each window from Coinbase (reuse Phase 1B client).
8. Write raw envelopes (reuse Phase 1B writer).
9. Read ALL raw envelopes from raw-dir (not just new ones).
10. Normalize, deduplicate, quality check (reuse Phase 2).
11. Write/merge Parquet partitions (reuse Phase 2).
12. Update DuckDB views (reuse Phase 2).
13. Advance watermark to max(promoted candle timestamps).
14. Update run_metadata with SUCCEEDED, old/new watermark.
15. Release run lock.
16. Print JSON summary.

**Exit codes:**
- 0: success (or nothing to do)
- 2: invalid input / no watermark
- 3: source unavailable
- 4: quality failure
- 5: storage failure
- 6: concurrent run detected

**Overlap config:** `--overlap-hours 48` (default 48, configurable).

### 4.5 Status Command

**Output JSON:**
```json
{
  "watermark_utc": "2026-01-02T00:00:00Z",
  "watermark_age_hours": 12.5,
  "last_run": {
    "run_id": "...",
    "mode": "incremental",
    "status": "SUCCEEDED",
    "completed_at_utc": "...",
    "rows_promoted": 24
  },
  "last_success": {
    "run_id": "...",
    "completed_at_utc": "..."
  },
  "last_failure": null,
  "curated_stats": {
    "total_rows": 720,
    "min_candle_utc": "2026-01-01T00:00:00Z",
    "max_candle_utc": "2026-01-30T23:00:00Z",
    "partitions": 1,
    "total_size_bytes": 45000
  },
  "gaps": [],
  "is_locked": false
}
```

**Implementation:**
- Read watermark from `pipeline_watermark`.
- Calculate age from current time.
- Query run_metadata for last run, last success, last failure.
- Query fact_market_candle_hourly for stats (count, min/max timestamp).
- Detect gaps: find missing hours between min and max candle.
- Check curated directory for partition count and size.
- Check for RUNNING locks.

### 4.6 Repair Command

**Flow:**
1. Check run lock. With `--force`: clear stale RUNNING rows first.
2. Read ALL raw envelopes from raw-dir.
3. Normalize, deduplicate, quality check.
4. Rebuild ALL Parquet partitions from scratch (not merge — full replace).
5. Update DuckDB views.
6. Do NOT change watermark (preserve monotonicity).
7. Record run in run_metadata with mode='repair'.
8. Print summary.

### 4.7 Update Promote Command

After successful promotion, if no watermark exists yet, set initial watermark to max promoted candle timestamp. If watermark exists, advance it if new max > current watermark.

## 5. Module layout changes

```text
src/bitcoin_data_platform/
├── storage/
│   └── duckdb_manager.py           # UPDATED: watermark table, run lock, status queries
├── cli.py                          # UPDATED: incremental, status, repair commands
└── (all other files unchanged)
tests/
├── test_incremental_cli.py         # NEW
├── test_status_cli.py              # NEW
├── test_repair_cli.py              # NEW
├── test_watermark.py               # NEW
├── test_run_lock.py                # NEW
└── (all existing tests unchanged)
```

## 6. Required tests

### Watermark (6)
1. Initial state: no watermark exists.
2. Set watermark after first promote.
3. Watermark advances after incremental.
4. Watermark does not lower on backfill/repair.
5. Watermark monotonically increases.
6. Read watermark returns correct value.

### Run Lock (5)
7. Acquire lock succeeds when no RUNNING runs.
8. Acquire lock fails when RUNNING run exists (exit 6).
9. Lock released on successful completion.
10. Lock released on failure.
11. Force clear stale lock in repair.

### Incremental CLI (8)
12. Incremental succeeds with valid watermark.
13. Incremental fails with no watermark (exit 2).
14. Incremental with nothing to fetch (fresh data) exits 0.
15. Incremental advances watermark correctly.
16. Incremental with overlap fetches correctly.
17. Incremental re-run deduplicates correctly.
18. Source unavailable exits 3.
19. Run summary includes old/new watermark.

### Status CLI (5)
20. Status with no data shows empty state.
21. Status with data shows correct watermark and stats.
22. Status detects gaps in hourly data.
23. Status shows last failure.
24. Status shows lock state.

### Repair CLI (4)
25. Repair rebuilds from raw successfully.
26. Repair does not lower watermark.
27. Repair with --force clears stale lock.
28. Repair produces same result as fresh promote.

### Total: ~28 new tests

## 7. Acceptance criteria

- AC-1: Incremental fetches only overlap window, not full history.
- AC-2: Watermark advances only after successful promotion.
- AC-3: No-watermark incremental fails with clear message.
- AC-4: Concurrent runs blocked (exit 6).
- AC-5: Status reports accurate watermark, freshness, gaps, lock state.
- AC-6: Repair rebuilds without lowering watermark.
- AC-7: All existing tests (157) still pass.
- AC-8: pytest + ruff + mypy all pass.
- AC-9: README updated with incremental, status, repair commands.

## 8. Dependencies

- Python 3.12, httpx, pyarrow, duckdb (all existing)
- No new dependencies needed.

## 9. Implementation sequence

1. Update duckdb_manager.py: watermark table, run lock methods, status queries
2. Implement watermark tests
3. Implement run lock tests
4. Update cli.py: incremental command + tests
5. Update cli.py: status command + tests
6. Update cli.py: repair command + tests
7. Update promote to set initial watermark
8. Full test suite
9. Quality gate
10. Update README
11. Commit
