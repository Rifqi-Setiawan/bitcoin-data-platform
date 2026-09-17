"""Streaming session lifecycle supervisor and orchestrator."""

import asyncio
import json
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.sources.coinbase_client import CoinbaseClient
from bitcoin_data_platform.streaming.buffer import (
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_ITEMS,
    StreamBuffer,
)
from bitcoin_data_platform.streaming.candles import aggregate_synthetic_candles
from bitcoin_data_platform.streaming.connection import (
    DEFAULT_WS_URL,
    WebSocketConnectionError,
    WebSocketConnectionManager,
)
from bitcoin_data_platform.streaming.models import (
    HeartbeatEvent,
    ReconciliationReport,
    StreamMetrics,
    StreamTrade,
)
from bitcoin_data_platform.streaming.protocol import (
    LastMatchMarker,
    StreamProtocolError,
    decode_message,
)
from bitcoin_data_platform.streaming.reconcile import reconcile_with_rest
from bitcoin_data_platform.streaming.writer import (
    DiskFullError,
    MicroBatchWriter,
    StorageError,
)


class StreamingSessionError(Exception):
    """Raised when the streaming session fails fatally."""

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


async def run_streaming_session(
    duration_seconds: int = 60,
    output_dir: Path | str = "./data/raw/streaming",
    *,
    product_id: str = "BTC-USD",
    reconcile: bool = False,
    ws_url: str = DEFAULT_WS_URL,
    batch_interval_seconds: float = 1.0,
    batch_max_size: int = 500,
    max_buffer_items: int = DEFAULT_MAX_ITEMS,
    max_buffer_bytes: int = DEFAULT_MAX_BYTES,
    connect_factory: Any = None,
    coinbase_client: CoinbaseClient | None = None,
    clock: Callable[[], datetime] | None = None,
    sleeper: Callable[[float], Any] = asyncio.sleep,
) -> dict[str, Any]:
    """Supervise a bounded real-time WebSocket capture experiment run.

    Lifecycle:
    1. Initialize output directories and write initial manifest.json.
    2. Start async WebSocket consumer and periodic disk writer.
    3. Countdown duration timer.
    4. Graceful shutdown: stop connection, drain in-flight buffer, flush final micro-batch.
    5. Replay committed trades to build synthetic 1m and 1h candles in derived/.
    6. Post-capture reconciliation against Coinbase REST API if enabled.
    7. Generate reports/summary.json and finalize manifest.json.
    """
    if duration_seconds <= 0:
        raise StreamingSessionError("duration_seconds must be a positive integer", exit_code=2)

    wall_clock = clock or (lambda: datetime.now(UTC))
    started_at = wall_clock()

    run_id = f"{started_at.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    run_dir = Path(output_dir) / "runs" / run_id

    events_dir = run_dir / "events"
    commits_dir = run_dir / "commits"
    derived_dir = run_dir / "derived"
    reports_dir = run_dir / "reports"

    for d in (events_dir, commits_dir, derived_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    manifest_path = run_dir / "manifest.json"
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "status": "running",
        "product_id": product_id,
        "duration_seconds": duration_seconds,
        "reconcile_enabled": reconcile,
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": None,
        "exit_code": None,
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    buffer = StreamBuffer(max_items=max_buffer_items, max_bytes=max_buffer_bytes)
    writer = MicroBatchWriter(run_dir, clock=wall_clock)
    metrics = StreamMetrics()

    connection_manager = WebSocketConnectionManager(
        url=ws_url,
        product_ids=[product_id],
        channels=["matches", "heartbeat"],
        connect_factory=connect_factory,
        sleeper=sleeper,
    )

    stop_event = asyncio.Event()

    async def consumer_loop() -> None:
        try:
            async for raw_msg in connection_manager.stream_messages():
                if stop_event.is_set():
                    break

                metrics.total_messages_received += 1
                arrival_wall = wall_clock()
                arrival_mono = time.monotonic_ns()

                try:
                    decoded = decode_message(
                        raw_msg,
                        received_at_utc=arrival_wall,
                        received_monotonic_ns=arrival_mono,
                    )
                except StreamProtocolError:
                    continue

                if isinstance(decoded, StreamTrade):
                    if (
                        metrics.last_sequence is not None
                        and decoded.sequence > metrics.last_sequence + 1
                    ):
                        metrics.sequence_gaps += 1
                    metrics.last_sequence = decoded.sequence

                    latency_ms = (arrival_wall - decoded.time_utc).total_seconds() * 1000.0
                    metrics.record_latency(latency_ms)

                    metrics.total_trades_captured += 1
                    if not buffer.push(decoded):
                        metrics.dropped_events += 1

                elif isinstance(decoded, LastMatchMarker):
                    metrics.total_trades_captured += 1
                    if not buffer.push(decoded.trade):
                        metrics.dropped_events += 1

                elif isinstance(decoded, HeartbeatEvent):
                    metrics.total_heartbeats += 1

        except WebSocketConnectionError as exc:
            if not stop_event.is_set():
                raise StreamingSessionError(f"Connection failure: {exc}", exit_code=3) from exc
        except Exception as exc:
            if not stop_event.is_set():
                raise StreamingSessionError(f"Consumer loop error: {exc}", exit_code=1) from exc

    async def writer_loop() -> None:
        while not stop_event.is_set():
            await sleeper(batch_interval_seconds)
            batch = buffer.pop_batch(max_batch_size=batch_max_size)
            if batch:
                t0 = time.monotonic()
                try:
                    writer.write_batch(batch)
                except DiskFullError as exc:
                    raise StreamingSessionError(
                        f"Disk storage critical: {exc}", exit_code=5
                    ) from exc
                except StorageError as exc:
                    raise StreamingSessionError(
                        f"Micro-batch storage failure: {exc}", exit_code=5
                    ) from exc
                t1 = time.monotonic()
                metrics.record_persistence_latency((t1 - t0) * 1000.0)

    async def timer_loop() -> None:
        await sleeper(duration_seconds)
        stop_event.set()
        connection_manager.stop()

    try:
        writer_task = asyncio.create_task(writer_loop())
        consumer_task = asyncio.create_task(consumer_loop())
        timer_task = asyncio.create_task(timer_loop())

        done, pending = await asyncio.wait(
            [writer_task, consumer_task, timer_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in done:
            exc = task.exception()
            if exc:
                stop_event.set()
                connection_manager.stop()
                for p in pending:
                    p.cancel()
                if isinstance(exc, StreamingSessionError):
                    raise exc
                raise StreamingSessionError(
                    f"Streaming execution failure: {exc}", exit_code=1
                ) from exc

        # Timer completed normally
        stop_event.set()
        connection_manager.stop()
        consumer_task.cancel()
        writer_task.cancel()
        await asyncio.gather(consumer_task, writer_task, return_exceptions=True)

    except StreamingSessionError as exc:
        manifest["status"] = "failed"
        manifest["completed_at_utc"] = wall_clock().isoformat()
        manifest["exit_code"] = exc.exit_code
        manifest["error"] = str(exc)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        raise

    except Exception as exc:
        manifest["status"] = "failed"
        manifest["completed_at_utc"] = wall_clock().isoformat()
        manifest["exit_code"] = 1
        manifest["error"] = str(exc)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        raise StreamingSessionError(
            f"Unexpected streaming session failure: {exc}", exit_code=1
        ) from exc

    # Final buffer drain
    final_batch = buffer.drain()
    if final_batch:
        t0 = time.monotonic()
        writer.write_batch(final_batch)
        t1 = time.monotonic()
        metrics.record_persistence_latency((t1 - t0) * 1000.0)

    metrics.reconnect_count = connection_manager.reconnect_count
    metrics.dropped_events += buffer.dropped_events

    # Replay and build synthetic candles
    candle_result = aggregate_synthetic_candles(run_dir=run_dir, product_id=product_id)

    completed_at = wall_clock()

    # Reconcile if requested
    reconcile_report: ReconciliationReport | None = None
    if reconcile:
        reconcile_report = reconcile_with_rest(
            synthetic_candles=candle_result.candles_1m,
            session_start_utc=started_at,
            session_end_utc=completed_at,
            coinbase_client=coinbase_client,
            granularity_seconds=60,
            product_id=product_id,
        )

    summary: dict[str, Any] = {
        "run_id": run_id,
        "status": "success",
        "product_id": product_id,
        "duration_seconds": duration_seconds,
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": completed_at.isoformat(),
        "metrics": metrics.to_dict(),
        "derived": {
            "candles_1m_count": len(candle_result.candles_1m),
            "candles_1h_count": len(candle_result.candles_1h),
            "candles_1m_path": str(candle_result.path_1m) if candle_result.path_1m else None,
            "candles_1h_path": str(candle_result.path_1h) if candle_result.path_1h else None,
        },
        "reconciliation": reconcile_report.to_dict() if reconcile_report else None,
    }

    summary_path = reports_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")

    manifest["status"] = "completed"
    manifest["completed_at_utc"] = completed_at.isoformat()
    manifest["exit_code"] = 0
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    return summary
