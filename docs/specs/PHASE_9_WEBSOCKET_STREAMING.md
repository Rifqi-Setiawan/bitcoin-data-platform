# Phase 9 Implementation Specification — Real-time WebSocket Trade Streaming Experiment

Status: approved for implementation
Parent phase: Phase 9 — WebSocket trade streaming experiment
Task ID: `P9-websocket-streaming`
Recommended branch: `feature/P9-websocket-streaming`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Implement a bounded, isolated, real-time trade streaming collector for Bitcoin (`BTC-USD`) using the
Coinbase Exchange WebSocket feed (`wss://ws-feed.exchange.coinbase.com`).
Compare the latency, network failure modes, out-of-order sequencing, and trade-to-candle reconciliation
against the existing production REST batch pipeline without coupling or modifying the batch pipeline.
Batch remains the single source of truth for historical candles.

## 2. User Stories

### Bounded Stream Capture
As a quantitative developer, I can execute:
```bash
bitcoin-data stream \
  --duration 60 \
  --output-dir ./data/raw/streaming \
  --reconcile
```
to connect to the Coinbase WebSocket `matches` and `heartbeat` channels for 60 seconds, stream trades into
a bounded in-memory buffer, persist raw micro-batches atomically to JSON Lines, construct synthetic 1-minute
and 1-hour candles, and output a reconciliation report against Coinbase REST API candles.

### Latency & Reliability Benchmarking
As a system architect, I can inspect the generated `reports/summary.json` to review:
- Exchange-to-application latency percentiles (p50, p95, p99)
- Application-to-durable persistence latency
- Reconnection frequency and sequence gap events
- Absolute and percentage differences between streaming-aggregated candles and official REST candles

## 3. Architectural Constraints & Isolation Rules

1. **Strict Isolation**: Streaming code lives under `src/bitcoin_data_platform/streaming/`. It MUST NOT write
   to curated batch Parquet directories (`curated/market/`, `curated/onchain/`), must not modify batch watermarks
   in `pipeline_watermark`, and must not interfere with `bitcoin-data.service` or timer.
2. **Resource Bounds**: Bounded queue strictly limited to 20,000 events or 32 MiB. If the writer lags, apply
   `drop-newest` policy and record explicit drop counters.
3. **High Precision**: Prices and sizes must be parsed directly from JSON strings into `Decimal` without binary
   float conversion.
4. **Resilience**: Auto-reconnect with exponential backoff and jitter (`min(30s, base * 2^attempt)`). Heartbeat
   monitoring distinguishes quiet markets from dropped connections.
5. **Reproducibility**: Synthetic candles and reconciliation metrics are derived from committed raw micro-batches.

## 4. Technical Specifications

### 4.1 Module Layout
```text
src/bitcoin_data_platform/streaming/
├── __init__.py
├── models.py           # Dataclasses: StreamTrade, HeartbeatEvent, MicroBatchReceipt, StreamMetrics
├── protocol.py         # Decoder & contract validation for matches, last_match, heartbeat
├── connection.py       # Async WebSocket connection manager, ping/pong, backoff reconnect
├── buffer.py           # Bounded FIFO ring buffer with drop-newest overflow tracking
├── writer.py           # Atomic micro-batch JSON Lines writer with fsync and disk guard
├── candles.py          # Deterministic 1m and 1h synthetic candle aggregation
├── reconcile.py        # Comparator against Coinbase REST candle reference
└── runner.py           # Lifecycle supervisor, duration countdown, graceful drain, reporting
```

### 4.2 Stream Trade Data Model (`models.py`)
```python
@dataclass(frozen=True)
class StreamTrade:
    source: str                 # "coinbase_exchange"
    product_id: str             # "BTC-USD"
    trade_id: int               # Unique integer trade identifier
    sequence: int               # Feed sequence number
    price: Decimal              # Execution price
    size: Decimal               # Executed volume
    side: str                   # "buy" or "sell" (maker side)
    time_utc: datetime          # Exchange execution timestamp
    received_at_utc: datetime   # Local arrival wall-clock timestamp
    received_monotonic_ns: int  # High-resolution monotonic timestamp
```

### 4.3 Output Layout
```text
<output-dir>/runs/<run_id>/
├── manifest.json               # Run parameters, start/end timestamps, exit status
├── events/part-<batch_id>.jsonl # Immutable raw trade segments
├── commits/<batch_id>.json     # Commit receipts with checksums and row counts
├── derived/candles_1m.parquet  # Synthetic 1-minute candles
├── derived/candles_1h.parquet  # Synthetic 1-hour candles
└── reports/summary.json        # Latency percentiles, loss accounting, reconciliation deltas
```

### 4.4 Reconciliation Logic (`reconcile.py`)
For settled windows, fetch Coinbase REST candles and compute:
- $\Delta\text{Open}, \Delta\text{High}, \Delta\text{Low}, \Delta\text{Close} = \text{Stream} - \text{REST}$
- $\Delta\text{Volume} = \text{Stream Volume} - \text{REST Volume}$
- Status: `matched` (exact match), `mismatched` (delta detected), or `partial_capture` (window partially covered).

## 5. Acceptance Criteria

- **AC-1**: Streaming package operates under `streaming/` with complete isolation from batch data and state.
- **AC-2**: Trade messages validate precision `Decimal` types, ISO UTC timestamps, and maker side semantics.
- **AC-3**: Connection supervisor reliably performs subscription, ping/pong, and exponential backoff reconnection.
- **AC-4**: Bounded buffer strictly enforces capacity caps and logs dropped events when writer is throttled.
- **AC-5**: Raw micro-batches write atomically (`.partial` -> `.jsonl`) with commit receipts.
- **AC-6**: Replay engine builds deterministic synthetic 1-minute and 1-hour candles.
- **AC-7**: Reconciliation engine compares settled windows against Coinbase REST and outputs structured report.
- **AC-8**: CLI subcommand `stream` supports `--duration`, `--output-dir`, and `--reconcile`.
- **AC-9**: Full test suite passes (>345 tests), quality gates pass (`ruff`, `mypy`), and ADR D-010 is recorded.
