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
- [Data Engineering concept map](docs/learning/DE_CONCEPT_MAP.md)
- [Source evaluation](docs/sources/SOURCE_EVALUATION.md)
- [Hermes implementation workflow](docs/IMPLEMENTATION_WORKFLOW.md)

## Safety boundary

This project is for data engineering and Bitcoin research. It does not place trades, provide automated buy/sell decisions, expose a database publicly, or depend on an AI agent for pipeline correctness.
