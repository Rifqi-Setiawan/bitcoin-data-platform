# Phase 2 Implementation Specification — Curated Parquet and DuckDB Modeling

Status: Approved
Parent phase: Phase 2 — Curated Parquet and analytical modeling
Task ID: `P2-curated-parquet-duckdb`
Recommended branch: `feature/P2-curated-parquet-duckdb`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Transform raw gzip JSON envelopes (from Phase 1B) into typed, deduplicated Parquet files
partitioned by year, expose them via DuckDB views as `fact_market_candle_hourly`, derive a
`mart_btc_usd_daily` aggregation, and provide a CLI command `promote` that executes the full
raw→curated pipeline. After Phase 2, raw data becomes queryable analytical tables.

## 2. User story

As an operator, I can run:

```bash
bitcoin-data promote \
  --raw-dir ./data/raw \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```

and the tool will:
1. Read all raw gzip JSON envelopes from `raw-dir`
2. Validate and normalize candles into typed records
3. Deduplicate by natural key (source, product_id, granularity_seconds, candle_start_utc)
4. Write/merge annual Parquet partitions atomically
5. Register/update DuckDB views for hourly fact and daily mart
6. Print a promotion summary

I can then query:
```bash
bitcoin-data query --db-path ./data/state/platform.duckdb \
  --sql "SELECT * FROM fact_market_candle_hourly LIMIT 5"
```

## 3. Scope

### In scope

- Normalizer: map raw envelope payload arrays to typed CoinbaseCandle records with Decimal prices.
- Deduplication by natural key (most recent run_id wins on conflict).
- Annual Parquet partitions: `curated/market/candles_hourly/source=coinbase_exchange/year=YYYY/*.parquet`
- Atomic partition replacement (write temp → os.replace).
- DuckDB database with:
  - View `fact_market_candle_hourly` over Parquet files.
  - View `mart_btc_usd_daily` derived from hourly fact.
  - Table `run_metadata` tracking promotion runs.
- Quality gate checks (blocking): OHLC invariants, positive prices, non-negative volume, unique keys, hour-aligned UTC.
- CLI `promote` command.
- CLI `query` command (simple SQL executor against DuckDB).
- Comprehensive offline tests.

### Out of scope

- Incremental mode and watermarks (Phase 3).
- systemd scheduling (Phase 4).
- Observability and alerting (Phase 5).
- Quarantine directory management.
- Any modification to Phase 1A/1B files.

## 4. Functional contract

### 4.1 Raw Envelope Reader

Module: `src/bitcoin_data_platform/transforms/raw_reader.py`

```python
@dataclass
class RawEnvelope:
    """Parsed raw gzip JSON envelope."""
    schema_version: int
    run_id: str
    source: str
    endpoint: str
    product_id: str
    granularity_seconds: int
    start_utc: datetime
    end_utc: datetime
    retrieved_at_utc: datetime
    http_status: int
    payload_sha256: str
    candle_count: int
    payload: list

def read_raw_envelopes(raw_dir: Path) -> list[RawEnvelope]:
    """Read and parse all .json.gz envelopes from raw_dir. Returns sorted by start_utc."""
    ...
```

### 4.2 Normalizer and Deduplicator

Module: `src/bitcoin_data_platform/transforms/normalizer.py`

```python
@dataclass
class NormalizedCandle:
    """Typed, validated candle ready for Parquet."""
    source: str
    product_id: str
    granularity_seconds: int
    candle_start_utc: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_base: Decimal
    ingested_at_utc: datetime
    source_run_id: str

def normalize_envelopes(envelopes: list[RawEnvelope]) -> list[NormalizedCandle]:
    """Normalize raw payloads to typed candles. Applies blocking quality checks.
    Deduplicates by natural key (latest run_id wins). Returns sorted by candle_start_utc."""
    ...
```

**Blocking quality checks during normalization:**
- Tuple length == 6
- Positive prices (open, high, low, close > 0)
- Non-negative volume (>= 0)
- high >= max(open, close, low) and low <= min(open, close, high)
- Timestamp is positive integer, converts to hour-aligned UTC
- Natural key uniqueness after dedup

### 4.3 Parquet Writer

Module: `src/bitcoin_data_platform/storage/parquet_writer.py`

```python
@dataclass
class PartitionResult:
    """Result of writing one annual partition."""
    year: int
    file_path: Path
    row_count: int
    is_new: bool  # True if partition didn't exist before

def write_parquet_partitions(
    candles: list[NormalizedCandle],
    curated_dir: Path,
    existing_dir: Path | None = None,
) -> list[PartitionResult]:
    """Write annual Parquet partitions. Merges with existing partitions if present.
    Atomic write: temp file → os.replace to final path.
    Returns list of partition results."""
    ...
```

**Directory structure:**
```
curated/market/candles_hourly/source=coinbase_exchange/year=2026/data.parquet
```

**Parquet schema (pyarrow):**
- source: string
- product_id: string
- granularity_seconds: int32
- candle_start_utc: timestamp[us, tz=UTC]
- open: decimal128(38,18)
- high: decimal128(38,18)
- low: decimal128(38,18)
- close: decimal128(38,18)
- volume_base: decimal128(38,18)
- ingested_at_utc: timestamp[us, tz=UTC]
- source_run_id: string

**Merge behavior:**
- Read existing partition if present.
- Combine new + existing rows.
- Deduplicate by natural key (latest ingested_at_utc wins).
- Sort by candle_start_utc.
- Write atomically (temp → replace).

### 4.4 DuckDB Manager

Module: `src/bitcoin_data_platform/storage/duckdb_manager.py`

```python
class DuckDBManager:
    """Manages DuckDB database with views and metadata."""

    def __init__(self, db_path: Path, curated_dir: Path): ...

    def initialize(self) -> None:
        """Create/update views and metadata tables."""
        ...

    def create_hourly_view(self) -> None:
        """Create/replace view fact_market_candle_hourly from Parquet files."""
        ...

    def create_daily_mart_view(self) -> None:
        """Create/replace view mart_btc_usd_daily derived from hourly."""
        ...

    def record_run(self, run_id: str, mode: str, rows_promoted: int, ...) -> None:
        """Record promotion run metadata."""
        ...

    def execute_query(self, sql: str) -> list[dict]:
        """Execute arbitrary SQL and return results as list of dicts."""
        ...
```

**`fact_market_candle_hourly` view:**
```sql
CREATE OR REPLACE VIEW fact_market_candle_hourly AS
SELECT * FROM read_parquet('curated/market/candles_hourly/source=*/year=*/*.parquet',
    hive_partitioning=true);
```

**`mart_btc_usd_daily` view:**
```sql
CREATE OR REPLACE VIEW mart_btc_usd_daily AS
SELECT
    source,
    product_id,
    DATE_TRUNC('day', candle_start_utc) AS trade_date_utc,
    FIRST(open ORDER BY candle_start_utc) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close ORDER BY candle_start_utc) AS close,
    SUM(volume_base) AS volume_base,
    COUNT(*) AS observed_hour_count,
    COUNT(*) = 24 AS is_complete
FROM fact_market_candle_hourly
GROUP BY source, product_id, DATE_TRUNC('day', candle_start_utc);
```

**`run_metadata` table:**
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
    error_message VARCHAR
);
```

### 4.5 CLI Commands

**`promote` command:**
```
bitcoin-data promote --raw-dir <path> --curated-dir <path> --db-path <path>
```

Flow:
1. Read raw envelopes from raw-dir.
2. Normalize and deduplicate.
3. Run blocking quality checks.
4. Write/merge Parquet partitions.
5. Initialize/update DuckDB views.
6. Record run metadata.
7. Print JSON summary.

Exit codes: 0=success, 2=invalid input, 4=quality failure, 5=storage failure.

**`query` command:**
```
bitcoin-data query --db-path <path> --sql "SELECT ..."
```

Simple SQL executor. Prints results as JSON array to stdout.

## 5. Module layout after Phase 2

```text
src/bitcoin_data_platform/
├── transforms/
│   ├── __init__.py                 # UPDATED: exports
│   ├── raw_reader.py               # NEW
│   └── normalizer.py               # NEW
├── storage/
│   ├── __init__.py                 # UPDATED: exports
│   ├── raw_writer.py               # unchanged from 1B
│   ├── parquet_writer.py           # NEW
│   └── duckdb_manager.py           # NEW
├── quality/
│   ├── __init__.py                 # UPDATED: exports
│   └── checks.py                   # NEW
├── cli.py                          # UPDATED: add promote & query commands
└── (all Phase 1A/1B files unchanged)
tests/
├── test_raw_reader.py              # NEW
├── test_normalizer.py              # NEW
├── test_parquet_writer.py          # NEW
├── test_duckdb_manager.py          # NEW
├── test_quality_checks.py          # NEW
├── test_promote_cli.py             # NEW
└── (all Phase 1A/1B tests unchanged)
```

## 6. Required tests

### Raw Reader (5)
1. Read single valid envelope.
2. Read multiple envelopes, sorted by start_utc.
3. Skip non-.json.gz files gracefully.
4. Handle empty directory (returns empty list).
5. Validate envelope schema_version.

### Normalizer (10)
6. Normalize valid candle tuple to NormalizedCandle with Decimal fields.
7. Reject candle with wrong tuple length.
8. Reject negative price.
9. Reject high < low.
10. Accept zero volume.
11. Reject negative volume.
12. Deduplicate by natural key (latest run_id wins).
13. Multiple envelopes with overlapping candles dedup correctly.
14. Output sorted by candle_start_utc.
15. Attach correct lineage (source_run_id, ingested_at_utc).

### Quality Checks (6)
16. Valid candle passes all blocking checks.
17. OHLC invariant violation caught.
18. Non-hour-aligned timestamp caught.
19. Future timestamp caught.
20. Null/missing field caught.
21. Batch with mix of valid/invalid: only valid promoted.

### Parquet Writer (7)
22. Write new partition creates correct directory structure.
23. Parquet schema matches spec (decimal types, timestamps).
24. Merge with existing partition deduplicates correctly.
25. Atomic write (temp → replace).
26. Verify row count matches input.
27. Partition by year correctly (multi-year data splits).
28. Read back with pyarrow matches written data.

### DuckDB Manager (6)
29. Initialize creates views and run_metadata table.
30. fact_market_candle_hourly view reads Parquet correctly.
31. mart_btc_usd_daily aggregation produces correct OHLCV.
32. Partial day has is_complete=false.
33. Record run metadata and query it back.
34. Query command returns correct JSON.

### Promote CLI (5)
35. Full promote pipeline succeeds with valid raw data.
36. Empty raw directory exits with appropriate message.
37. Quality failure exits 4.
38. Promote summary includes correct counts.
39. Re-promote (idempotent) produces same result.

### Total: ~39 new tests

## 7. Acceptance criteria

- AC-1: promote command reads raw envelopes, writes Parquet, creates DuckDB views.
- AC-2: fact_market_candle_hourly queryable via DuckDB with correct types.
- AC-3: mart_btc_usd_daily correctly aggregates hourly to daily with is_complete flag.
- AC-4: Deduplication by natural key works across multiple runs.
- AC-5: Parquet partitions are annual and written atomically.
- AC-6: Blocking quality checks reject bad data.
- AC-7: All Phase 1A (81) + 1B (38) tests still pass.
- AC-8: pytest + ruff + mypy all pass.
- AC-9: README updated with promote and query commands.

## 8. Dependencies

- Python 3.12 (existing)
- `httpx` (existing from 1B)
- `pyarrow` — add to pyproject.toml (Parquet read/write)
- `duckdb` — add to pyproject.toml (OLAP engine)

## 9. Implementation sequence

1. Add pyarrow and duckdb to pyproject.toml, pip install
2. Implement quality/checks.py + tests
3. Implement transforms/raw_reader.py + tests
4. Implement transforms/normalizer.py + tests
5. Implement storage/parquet_writer.py + tests
6. Implement storage/duckdb_manager.py + tests
7. Update cli.py with promote & query commands + tests
8. Full test suite
9. Quality gate
10. Update README
11. Commit
