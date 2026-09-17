# Phase 1B Implementation Specification — Coinbase HTTP Client and Raw Ingestion

Status: approved for implementation
Parent phase: Phase 1 — Repository bootstrap and deterministic raw ingestion
Task ID: `P1B-coinbase-raw-ingestion`
Recommended branch: `hermes/P1B-coinbase-raw-ingestion`
Implementation profile: `btc-coder`
Verification profile: `btc-verifier`

## 1. Objective

Add the ability to actually fetch BTC-USD hourly candle data from the Coinbase Exchange REST API,
validate source responses against a strict contract, persist each successful response as an immutable
gzip JSON envelope with checksums, and expose a `backfill` CLI command that executes the full
plan→fetch→validate→persist pipeline for an explicit UTC time range.

This builds on Phase 1A's window planner. After Phase 1B, the operator can run a single command and
get real historical data saved to disk in a reproducible, auditable format.

## 2. User story

As an operator, I can run:

```bash
bitcoin-data backfill \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-02T00:00:00Z \
  --output-dir ./data/raw
```

and the tool will:
1. Plan request windows (reusing Phase 1A planner)
2. Fetch each window from Coinbase with retry/backoff
3. Validate each response against the source contract
4. Write each valid response as an atomic gzip JSON envelope
5. Print a run summary with success/failure counts

## 3. Scope

### In scope

- HTTP client for Coinbase Exchange product candles endpoint.
- Configurable connect, read, and total request timeouts.
- Rate limiting: maximum 2 requests per second.
- Retry logic: up to 5 attempts for transient failures (connection error, timeout, HTTP 408/429/5xx).
- Exponential backoff with jitter, capped delay, and Retry-After header support.
- No retry for non-transient 4xx errors.
- Source contract validation for candle tuple `[time, low, high, open, close, volume]`.
- Raw gzip JSON envelope writer with SHA-256 checksum.
- Atomic file writes (temp file → os.replace).
- CLI `backfill` command integrating planner → client → validator → writer.
- Structured JSON logging.
- Distinct exit codes per error class.
- Comprehensive offline tests with mocked HTTP.
- User-Agent: `bitcoin-data-platform/0.1.0 (+https://github.com/Rifqi-Setiawan/bitcoin-data-platform)`.

### Out of scope

- Incremental mode, watermarks, or status command.
- Parquet, DuckDB, transforms, or curated layer.
- Quarantine directory (flag only).
- Docker, systemd, cron, or deployment.
- API key management (public endpoint, no key needed).

## 4. Functional contract

### 4.1 Coinbase HTTP Client

Module: `src/bitcoin_data_platform/sources/coinbase_client.py`

**Endpoint:** `GET https://api.exchange.coinbase.com/products/BTC-USD/candles`
- Query params: `start` (ISO-8601), `end` (ISO-8601), `granularity` (seconds)
- Response: JSON array of arrays `[[time, low, high, open, close, volume], ...]`

**Data classes:**
- `CoinbaseCandle`: timestamp_utc, low, high, open, close, volume (Decimal).
- `CoinbaseResponse`: window boundaries, candles list, raw_payload, http_status, retrieved_at_utc.
- `CoinbaseClient`: configurable product_id, granularity, timeouts, rate limit, retry params.

**Rate limiting:** minimum 500ms between requests (2 req/s).

**Retry policy:**
- Retryable: ConnectionError, Timeout, HTTP 408/429/500/502/503/504.
- Non-retryable: HTTP 400/401/403/404 → raise immediately.
- Backoff: `min(base_backoff * 2^attempt + jitter, max_backoff)`.
- Honor `Retry-After` header.
- After max_retries (5): raise `SourceUnavailableError`.

**Dependency injection:** client must accept an injectable HTTP transport for testing.

### 4.2 Source Contract Validation

Module: `src/bitcoin_data_platform/sources/coinbase_contract.py`

**Validation rules:**
- Each candle: list/tuple of exactly 6 elements.
- `time` (idx 0): positive integer (unix epoch).
- `low` (idx 1): positive number.
- `high` (idx 2): positive number, >= low.
- `open` (idx 3): positive number.
- `close` (idx 4): positive number.
- `volume` (idx 5): non-negative number.

Returns `ValidationResult` with valid_candles, violations list, and is_valid flag.

### 4.3 Raw Envelope Writer

Module: `src/bitcoin_data_platform/storage/raw_writer.py`

**Envelope schema:**
```json
{
  "schema_version": 1,
  "run_id": "uuid",
  "source": "coinbase_exchange",
  "endpoint": "product_candles",
  "request": {
    "product_id": "BTC-USD",
    "granularity_seconds": 3600,
    "start_utc": "...",
    "end_utc": "..."
  },
  "retrieved_at_utc": "...",
  "http": { "status": 200, "provider_request_id": "..." },
  "payload_sha256": "hex-digest",
  "candle_count": 12,
  "payload": [...]
}
```

**Atomicity:** write to temp `.tmp` → `os.replace()` to final path.
**Filename:** `{run_id}_{YYYYMMDDTHHZ}_{YYYYMMDDTHHZ}.json.gz`
**Checksum:** SHA-256 of canonical JSON payload (sorted keys, compact).

### 4.4 CLI backfill command

Update `cli.py`:
```
bitcoin-data backfill --start <ISO> --end <ISO> [--output-dir ./data/raw]
```

**Flow:** validate range → plan windows → for each: fetch → validate → write envelope → print summary.

**Exit codes:**
- 0: success
- 2: invalid input
- 3: source unavailable
- 4: contract violation
- 5: storage failure

## 5. Module layout after Phase 1B

```text
src/bitcoin_data_platform/
├── sources/
│   ├── coinbase_client.py          # NEW
│   └── coinbase_contract.py        # NEW
├── storage/
│   └── raw_writer.py               # NEW
├── cli.py                          # UPDATED: add backfill
└── (all Phase 1A files unchanged)
tests/
├── test_coinbase_client.py         # NEW
├── test_coinbase_contract.py       # NEW
├── test_raw_writer.py              # NEW
├── test_backfill_cli.py            # NEW
└── (all Phase 1A tests unchanged)
```

## 6. Required tests (37 total)

### Client (10): fetch success, rate limit, retry 429/5xx/timeout, no retry 4xx, Retry-After, max retries, user-agent, params.
### Contract (11): valid candle, wrong length, non-numeric, negative price/volume, zero volume ok, high<low, bad timestamp, mixed batch, empty payload.
### Writer (8): valid gzip, checksum match, atomic success/failure, no in-place edit, filename format, mkdir, schema fields.
### CLI (8): success exit 0, invalid input exit 2, source unavail exit 3, contract fail exit 4, partial failure, output-dir, summary counts.
### Integration (1): optional real Coinbase fetch (@pytest.mark.integration, skipped default).

## 7. Acceptance criteria

- AC-1: Successful backfill fetches real data and writes valid envelopes.
- AC-2: Retry tests prove backoff on transient, immediate raise on permanent errors.
- AC-3: Rate limiter maintains ≥500ms spacing.
- AC-4: Invalid candle data caught and reported.
- AC-5: Atomic writes (temp→rename), no partial files.
- AC-6: SHA-256 checksum integrity verified.
- AC-7: All 81 Phase 1A tests still pass.
- AC-8: pytest + ruff + mypy all pass.
- AC-9: README updated.

## 8. Dependencies

- Python 3.12 (existing)
- `httpx` — add to pyproject.toml
- Phase 1A (existing, unchanged)

## 9. Implementation sequence

1. `coinbase_contract.py` + tests (no network)
2. `coinbase_client.py` + tests (mocked HTTP)
3. `raw_writer.py` + tests (filesystem)
4. Update `cli.py` + backfill tests
5. Full test suite (1A + 1B)
6. Update README
7. Quality gate
