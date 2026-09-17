# Phase 1A Implementation Specification — Repository Bootstrap and Window Planner

Status: complete
Parent phase: Phase 1 — Repository bootstrap and deterministic raw ingestion

## 1. Objective

Create the smallest executable foundation of the Bitcoin Data Engineering Platform: an installable
Python 3.12 package with a CLI that validates an explicit UTC half-open interval and deterministically
splits it into Coinbase-compatible request windows containing no more than 300 hourly candles.

This slice does not call Coinbase and does not write runtime data. Its user-visible outcome is a
machine-readable request plan that later Phase 1 work can pass to the Coinbase client.

## 2. User story

As an operator, I can run:

```bash
bitcoin-data plan-backfill \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-26T01:00:00Z
```

and receive a deterministic JSON plan for the half-open interval `[start, end)`, split into
contiguous windows of at most 300 hours.

## 3. Scope

### In scope

- Bootstrap a `src/bitcoin_data_platform/` Python package targeting Python 3.12.
- Add one console entry point named `bitcoin-data`.
- Add the `plan-backfill` command.
- Parse and validate explicit UTC timestamps.
- Model all ranges as half-open intervals `[start, end)`.
- Require both boundaries to be aligned to an exact hour.
- Reject empty, reversed, open-current-hour, and future ranges.
- Split valid ranges into consecutive windows of at most 300 expected hourly candles.
- Emit a stable JSON document to stdout.
- Return documented non-zero exit status for invalid operator input.
- Add offline unit and CLI tests.
- Configure and document formatter/linter, type checker, and test commands.
- Update the root README with setup and example usage for this slice.

### Out of scope

- HTTP requests or any live Coinbase access.
- Retries, rate limiting, or response parsing.
- Raw gzip envelopes, checksums, quarantine, or runtime data directories.
- Parquet, DuckDB, transformations, watermarks, incremental mode, or status mode.
- Docker, systemd, cron, long-running processes, or deployment.
- Any modification to system services, infrastructure, or data directories outside the project.
- Any public listener, credential, trading, wallet, signal, or financial-advice behavior.

## 4. Functional contract

### Inputs

- `--start`: required ISO-8601 timestamp in UTC, expressed with `Z` or `+00:00`.
- `--end`: required ISO-8601 timestamp in UTC, expressed with `Z` or `+00:00`.
- The interval is `[start, end)`; `end` is not included.

Reject the request when any of these is true:

- either value is missing, malformed, timezone-naive, or has a non-UTC offset;
- either boundary includes non-zero minutes, seconds, or microseconds;
- `start >= end`;
- `end` is later than the start of the current UTC hour supplied by an injectable clock.

The clock must be injectable at the Python API boundary so tests never depend on wall-clock timing.

### Planning rules

- Granularity is fixed at `3600` seconds.
- Product is fixed at `BTC-USD`.
- Source is fixed at `coinbase_exchange`.
- Each window is half-open and contains between 1 and 300 expected candles.
- The first window starts exactly at the requested `start`.
- The final window ends exactly at the requested `end`.
- Adjacent windows meet exactly: `window[n].end == window[n+1].start`.
- Windows never overlap and never leave a gap.
- The same validated input always produces byte-equivalent JSON output.

### Output

Successful execution writes one JSON document to stdout using UTC timestamps normalized to
`YYYY-MM-DDTHH:00:00Z`:

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
    }
  ]
}
```

The example abbreviates the `windows` array; the real output must contain every window. JSON key
order and formatting must be stable and covered by a CLI test.

Invalid input writes a concise, secret-free diagnostic to stderr, writes no plan to stdout, and
exits with status `2`.

## 5. Design boundaries

- Keep timestamp parsing/validation, window planning, and CLI presentation in separate modules.
- Public Python functions and returned domain objects must be type annotated.
- Use timezone-aware `datetime` values internally; do not use naive datetimes.
- Use integer arithmetic for expected candle counts.
- Core planning code must not read environment variables, the network, or the filesystem.
- CLI code may serialize output but must not duplicate planning rules.
- Do not introduce an application framework, database, HTTP client, or service process.
- Prefer the Python standard library for runtime code. Development-only quality tools are allowed.
- Do not add a dependency solely for ISO-8601 parsing when Python 3.12 standard-library behavior
  satisfies this contract.

Suggested module boundary (exact filenames may vary if the same separation is preserved):

```text
src/bitcoin_data_platform/
├── __init__.py
├── cli.py
├── time_range.py
└── window_planner.py
tests/
├── test_time_range.py
├── test_window_planner.py
└── test_cli.py
```

## 6. Required test cases

All tests are offline and deterministic.

1. One-hour range produces one one-hour window.
2. A 300-hour range produces exactly one 300-hour window.
3. A 301-hour range produces windows of 300 and 1 hour.
4. A 601-hour range produces windows of 300, 300, and 1 hour.
5. Every generated plan has exact start/end coverage with no gaps or overlaps.
6. The same input produces byte-equivalent CLI JSON on repeated calls.
7. `Z` and `+00:00` inputs normalize to the same canonical output.
8. Timezone-naive and non-UTC-offset inputs are rejected.
9. Minute-, second-, and microsecond-misaligned boundaries are rejected.
10. Equal and reversed ranges are rejected.
11. An end boundary after the injected current-hour boundary is rejected.
12. An end boundary equal to the injected current-hour boundary is accepted.
13. A range crossing 29 February remains continuous and correctly counted.
14. A range spanning a daylight-saving transition remains unchanged because all internal time is UTC.
15. Invalid CLI input returns exit status `2`, emits stderr, and leaves stdout empty.
16. Planning performs no network or runtime-data filesystem access.

## 7. Acceptance criteria

The implementation is ready for review only when all criteria below are satisfied.

### AC-1 — Clean bootstrap

From a clean Python 3.12 environment, the documented install command succeeds without manual file
edits. The package imports successfully, and `bitcoin-data --help` exits `0`.

### AC-2 — Exact 601-hour plan

Given an injected current time later than `2026-01-26T01:00:00Z`, running the user-story command
exits `0` and emits a schema-version-1 plan containing exactly three windows with counts
`[300, 300, 1]`, total count `601`, exact requested boundaries, and no gaps or overlaps.

### AC-3 — Coinbase window limit

For every valid tested range, no generated window has `expected_candle_count > 300`. Property-style
coverage or a parameterized suite must exercise multiple ranges beyond the three named examples.

### AC-4 — UTC and half-open semantics

The required boundary and clock tests pass, proving exact-hour UTC validation and `[start, end)`
behavior. No naive datetime enters the planner API.

### AC-5 — Deterministic output

Two executions with the same inputs and injected clock produce byte-equivalent stdout. Output has
canonical `Z` timestamps and stable JSON formatting.

### AC-6 — Safe failure

Every invalid-input case returns status `2`, produces no stdout plan, emits a concise stderr message,
and creates or modifies no runtime data file.

### AC-7 — Quality gate

The repository-documented formatter check, linter, type checker, and complete offline test suite all
exit `0`. The documented formatter check, linter, type checker, and complete offline test suite must all
pass with exact commands and summarized results reported.

### AC-8 — Documentation and repository hygiene

README documents environment setup, `plan-backfill`, UTC/half-open semantics, the 300-candle limit,
and the fact that this slice performs no network request. `git diff --check` passes; no secret,
credential, generated data, virtual environment, cache, or runtime output is committed.

### AC-9 — Scoped commit

Implementation is committed on a dedicated feature branch with a coherent commit. The handoff names
the task and branch, lists changed files, includes validation evidence, identifies assumptions and
deferred Phase 1 work, and supplies the commit hash.

## 8. Required validation evidence

The following validation commands must be run and reported. At
minimum, evidence must cover equivalents of:

```bash
python --version
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy src
git diff --check
git status --short
```

Additionally, capture the successful CLI output for the 601-hour example and one invalid-input
example. Do not use a live network call as evidence.

## 9. Dependencies and sequencing

- Depends on the approved Phase 0 architecture documents already in the repository.
- Has no dependency on any external inference service at runtime and requires no API key.
- Must complete review before a separate Phase 1B card adds the Coinbase HTTP client,
  retries, rate limiting, source tuple validation, or raw-envelope persistence.

## 10. Handoff checklist

The completed implementation must return:

- task ID and branch;
- concise implementation summary;
- files changed and why;
- exact validation commands and results;
- sample valid and invalid CLI outcomes;
- assumptions and residual risks;
- intentionally deferred Phase 1B work;
- commit hash;
- confirmation that no network request, infrastructure change, or secret was introduced.
