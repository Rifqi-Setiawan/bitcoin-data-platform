# D-010: Isolated, Bounded WebSocket Trade Capture for an Empirical Streaming Experiment

## Status
Accepted

## Context
The Bitcoin Data Platform has successfully proven its batch architecture (Phase 1–8) using Coinbase hourly
REST candles and Coin Metrics daily on-chain metrics, backed by annual Parquet partitions and DuckDB local OLAP.
While batch processing provides high reproducibility, auditability, and monotonic watermarks, it introduces
an inherent trade-off: price discovery is delayed until the hourly candle window has closed and the batch
pipeline has polled, validated, and promoted the data (up to ~60–70 minutes lag).

We require an empirical evaluation to answer:
1. What is the quantitative latency advantage of real-time trade capture?
2. What are the operational failure modes of a live stream on a single-host VPS (disconnects, out-of-order data, message loss, buffer bloat)?
3. How closely does synthetic candle aggregation from trade streams reconcile against exchange-published REST candles?

## Decision
We implement a bounded, isolated real-time trade streaming collector for `BTC-USD` on Coinbase Exchange
WebSocket (`wss://ws-feed.exchange.coinbase.com`) using `matches` and `heartbeat` channels.

Key architectural constraints:
1. **Strict Experimental Isolation**: Streaming is an isolated research experiment. It does NOT replace or write to
   production batch Parquet partitions, does NOT alter batch watermarks, and does NOT open inbound ports.
   The batch pipeline remains the Single Source of Truth (SSOT) for historical analytical data.
2. **Single Async Event Loop with Dedicated Disk Writer**: Single-process architecture using `asyncio` and `websockets`
   to avoid multi-process complexity and inter-process locking.
3. **Bounded Buffer & Drop-Newest Overflow**: Maximum queue limit of 20,000 events (32 MiB) with explicit loss accounting
   to strictly prevent Out-of-Memory (OOM) conditions on the VPS host.
4. **Immutable Micro-Batch JSON Lines**: Raw trades are written as atomic `.jsonl` segments without hot-path compression
   to maintain low CPU overhead.
5. **Deterministic Replay & Post-Capture Reconciliation**: Synthetic candles (1m and 1h) and reconciliation against
   Coinbase REST reference candles are computed post-capture from committed raw segments.
6. **No External Streaming Broker**: We deliberately decline Kafka, Redpanda, or Redis (YAGNI / Ponytail) because
   broker daemons add unnecessary memory, storage, and operational overhead for a single-host node experiment.

## Consequences

### Positive
- Zero risk to historical batch data integrity.
- Provides hard empirical metrics comparing stream latency vs batch polling freshness.
- Low operational weight: runs entirely within unprivileged process boundaries on the existing host.
- Fully reproducible: all metrics and synthetic candles can be re-derived from raw JSON Lines segments.

### Negative
- Uncommitted volatile events in buffer may be lost during abrupt process termination (best-effort capture).
- JSON Lines raw files consume uncompressed disk space during execution.
- Reconciliation differences may occur due to exchange boundary tick filtering and latency anomalies.

## Evaluation Criteria Post-Experiment
- **Keep**: If freshness advantage is high, network reliability is acceptable, and resource impact remains minimal.
- **Evolve**: If specific I/O or serialization bottlenecks require transition to compressed micro-batches.
- **Remove**: If network variance is excessive and batch polling remains the only cost-effective architecture.
