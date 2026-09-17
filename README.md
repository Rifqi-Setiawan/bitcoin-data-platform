# Bitcoin Data Engineering Platform

A production-style, single-host Bitcoin data platform built as a data engineering portfolio project. Demonstrates intentional system design — from source contracts and deterministic ingestion to analytical modeling — without unnecessary enterprise complexity.

## Current Status: Phase 1A ✅

**Repository Bootstrap and Deterministic Window Planner** — the executable foundation for batch ingestion.

An installable Python 3.12 package with the `bitcoin-data` CLI and `plan-backfill` command. Strictly offline: no network requests, no runtime data. Validates an operator-specified half-open UTC interval and deterministically partitions it into Coinbase-compatible request windows of at most 300 hourly candles.

### Core Semantics

- **UTC timestamps** — All timestamps must be explicit UTC (`2026-01-01T00:00:00Z` or `+00:00`). Naive datetimes and non-UTC offsets are rejected.
- **Half-open interval `[start, end)`** — Start is included, end is excluded.
- **Hourly boundary alignment** — Both boundaries must align to an exact hour.
- **Coinbase window limit** — Each window contains 1–300 expected hourly candles (granularity: 3600s, product: BTC-USD).
- **No open/future candles** — End boundary must not exceed the current UTC hour.
- **Deterministic output** — Identical inputs always produce byte-equivalent JSON.
- **Safe failure** — Invalid input prints a concise error to stderr, outputs nothing to stdout, exits with status `2`.

## Quick Start

**Requirements:** Python 3.12+

```bash
python3 -m venv .venv
source .venv/bin/activate

# Install with dev dependencies
make install

# Or manually:
pip install -r requirements-dev.txt
pip install -e .
```

## CLI Usage

```bash
bitcoin-data plan-backfill \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-26T01:00:00Z
```

Output (601 hours → 3 windows):

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

## Quality Gates

```bash
make check       # All gates: lint + typecheck + test
make lint        # ruff check + format verification
make typecheck   # mypy strict static typing
make test        # pytest (81 offline tests)
```

## Project Structure

```
src/bitcoin_data_platform/
├── cli.py              # CLI entry point and argument parsing
├── time_range.py       # UTC timestamp parsing and validation
├── window_planner.py   # Deterministic window planning algorithm
├── ingestion/          # Ingestion module
├── sources/            # Source adapters (planned)
├── storage/            # Storage layer (planned)
├── transforms/         # Data transformations (planned)
└── quality/            # Data quality checks (planned)

tests/
├── test_cli.py
├── test_time_range.py
└── test_window_planner.py
```

## Documentation

- [Master Plan](docs/MASTER_PLAN.md) — Project scope, constraints, architecture, and technology decisions
- [Architecture V1](docs/architecture/ARCHITECTURE_V1.md) — Single-host batch platform design
- [Roadmap](docs/roadmap/ROADMAP.md) — Progressive 11-phase development plan
- [Decision Log](docs/decisions/README.md) — Architecture decisions with trade-offs
- [Source Evaluation](docs/sources/SOURCE_EVALUATION.md) — Data source comparison and selection rationale
- [Phase 1A Spec](docs/specs/PHASE_1A_BOOTSTRAP_WINDOW_PLANNER.md) — Window planner specification

## Roadmap Overview

| Phase | Focus | Status |
|-------|-------|--------|
| 0 | Architecture and documentation | ✅ Done |
| 1A | Repository bootstrap and window planner | ✅ Done |
| 1B | Coinbase API client and raw ingestion | 🔜 Next |
| 2 | Curated Parquet and DuckDB modeling | Planned |
| 3 | Incremental loads and recovery | Planned |
| 4 | Single-host orchestration (systemd) | Planned |
| 5 | Data quality and observability | Planned |
| 6 | CI/CD and reproducible delivery | Planned |
| 7+ | Second domain, research serving, streaming, AI ops | Planned |

## Safety Boundary

This project is for **data engineering and Bitcoin market research**. It does not place trades, provide buy/sell decisions, expose a database publicly, or depend on AI for pipeline correctness.

## License

[MIT](LICENSE)
