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

#### Exit codes

- `0`: Success (operation completed successfully).
- `2`: Invalid input parameters or query execution error.
- `3`: Source unavailable (Coinbase API unavailable after exhausting all retry attempts).
- `4`: Contract or quality check failure (malformed payload or invariant violation).
- `5`: Storage failure (disk, Parquet, or database write failure).

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
```

## Current documents

- [Master plan](docs/MASTER_PLAN.md)
- [Architecture V1](docs/architecture/ARCHITECTURE_V1.md)
- [Decision log](docs/decisions/README.md)
- [Roadmap](docs/roadmap/ROADMAP.md)
- [Phase 1A Specification](docs/specs/PHASE_1A_BOOTSTRAP_WINDOW_PLANNER.md)
- [Phase 1B Specification](docs/specs/PHASE_1B_COINBASE_CLIENT_RAW_INGESTION.md)
- [Phase 2 Specification](docs/specs/PHASE_2_CURATED_PARQUET_DUCKDB.md)
- [Data Engineering concept map](docs/learning/DE_CONCEPT_MAP.md)
- [Source evaluation](docs/sources/SOURCE_EVALUATION.md)
- [Hermes implementation workflow](docs/IMPLEMENTATION_WORKFLOW.md)

## Safety boundary

This project is for data engineering and Bitcoin research. It does not place trades, provide automated buy/sell decisions, expose a database publicly, or depend on an AI agent for pipeline correctness.
