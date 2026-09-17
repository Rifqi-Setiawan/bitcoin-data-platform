"""Data models for real-time WebSocket trade streaming and reconciliation."""

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class StreamTrade:
    """Represents a validated executed trade received from the WebSocket feed.

    All prices and sizes are stored as high-precision Decimal values.
    Timestamps are timezone-aware UTC.
    """

    source: str
    product_id: str
    trade_id: int
    sequence: int
    price: Decimal
    size: Decimal
    side: str
    time_utc: datetime
    received_at_utc: datetime
    received_monotonic_ns: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize StreamTrade to JSON-compatible dictionary."""
        return {
            "source": self.source,
            "product_id": self.product_id,
            "trade_id": self.trade_id,
            "sequence": self.sequence,
            "price": str(self.price),
            "size": str(self.size),
            "side": self.side,
            "time_utc": self.time_utc.isoformat(),
            "received_at_utc": self.received_at_utc.isoformat(),
            "received_monotonic_ns": self.received_monotonic_ns,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StreamTrade":
        """Reconstruct StreamTrade from dictionary."""
        time_utc = datetime.fromisoformat(data["time_utc"])
        if time_utc.tzinfo is None:
            time_utc = time_utc.replace(tzinfo=UTC)

        received_at_utc = datetime.fromisoformat(data["received_at_utc"])
        if received_at_utc.tzinfo is None:
            received_at_utc = received_at_utc.replace(tzinfo=UTC)

        return cls(
            source=str(data["source"]),
            product_id=str(data["product_id"]),
            trade_id=int(data["trade_id"]),
            sequence=int(data["sequence"]),
            price=Decimal(str(data["price"])),
            size=Decimal(str(data["size"])),
            side=str(data["side"]),
            time_utc=time_utc,
            received_at_utc=received_at_utc,
            received_monotonic_ns=int(data["received_monotonic_ns"]),
        )


@dataclass(frozen=True)
class HeartbeatEvent:
    """Represents a periodic heartbeat pulse from the exchange feed."""

    sequence: int
    last_trade_id: int
    product_id: str
    time_utc: datetime
    received_at_utc: datetime

    def to_dict(self) -> dict[str, Any]:
        """Serialize HeartbeatEvent to JSON-compatible dictionary."""
        return {
            "sequence": self.sequence,
            "last_trade_id": self.last_trade_id,
            "product_id": self.product_id,
            "time_utc": self.time_utc.isoformat(),
            "received_at_utc": self.received_at_utc.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HeartbeatEvent":
        """Reconstruct HeartbeatEvent from dictionary."""
        time_utc = datetime.fromisoformat(data["time_utc"])
        if time_utc.tzinfo is None:
            time_utc = time_utc.replace(tzinfo=UTC)

        received_at_utc = datetime.fromisoformat(data["received_at_utc"])
        if received_at_utc.tzinfo is None:
            received_at_utc = received_at_utc.replace(tzinfo=UTC)

        return cls(
            sequence=int(data["sequence"]),
            last_trade_id=int(data["last_trade_id"]),
            product_id=str(data["product_id"]),
            time_utc=time_utc,
            received_at_utc=received_at_utc,
        )


@dataclass(frozen=True)
class MicroBatchReceipt:
    """Durable commit receipt for a flushed micro-batch JSON Lines segment."""

    batch_id: str
    segment_path: str
    trade_count: int
    byte_size: int
    sha256_checksum: str
    min_sequence: int | None
    max_sequence: int | None
    min_trade_id: int | None
    max_trade_id: int | None
    start_time_utc: datetime | None
    end_time_utc: datetime | None
    committed_at_utc: datetime

    def to_dict(self) -> dict[str, Any]:
        """Serialize MicroBatchReceipt to JSON-compatible dictionary."""
        return {
            "batch_id": self.batch_id,
            "segment_path": self.segment_path,
            "trade_count": self.trade_count,
            "byte_size": self.byte_size,
            "sha256_checksum": self.sha256_checksum,
            "min_sequence": self.min_sequence,
            "max_sequence": self.max_sequence,
            "min_trade_id": self.min_trade_id,
            "max_trade_id": self.max_trade_id,
            "start_time_utc": self.start_time_utc.isoformat() if self.start_time_utc else None,
            "end_time_utc": self.end_time_utc.isoformat() if self.end_time_utc else None,
            "committed_at_utc": self.committed_at_utc.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MicroBatchReceipt":
        """Reconstruct MicroBatchReceipt from dictionary."""
        start_time_utc = None
        if data.get("start_time_utc"):
            st = datetime.fromisoformat(data["start_time_utc"])
            start_time_utc = st if st.tzinfo is not None else st.replace(tzinfo=UTC)

        end_time_utc = None
        if data.get("end_time_utc"):
            et = datetime.fromisoformat(data["end_time_utc"])
            end_time_utc = et if et.tzinfo is not None else et.replace(tzinfo=UTC)

        comm = datetime.fromisoformat(data["committed_at_utc"])
        committed_at_utc = comm if comm.tzinfo is not None else comm.replace(tzinfo=UTC)

        return cls(
            batch_id=str(data["batch_id"]),
            segment_path=str(data["segment_path"]),
            trade_count=int(data["trade_count"]),
            byte_size=int(data["byte_size"]),
            sha256_checksum=str(data["sha256_checksum"]),
            min_sequence=int(data["min_sequence"])
            if data.get("min_sequence") is not None
            else None,
            max_sequence=int(data["max_sequence"])
            if data.get("max_sequence") is not None
            else None,
            min_trade_id=int(data["min_trade_id"])
            if data.get("min_trade_id") is not None
            else None,
            max_trade_id=int(data["max_trade_id"])
            if data.get("max_trade_id") is not None
            else None,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            committed_at_utc=committed_at_utc,
        )


def _calc_percentile(values: list[float], percentile: float) -> float | None:
    """Calculate percentile from sorted list of numbers."""
    if not values:
        return None
    k = (len(values) - 1) * percentile
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return round(values[int(k)], 3)
    d0 = values[int(f)] * (c - k)
    d1 = values[int(c)] * (k - f)
    return round(d0 + d1, 3)


@dataclass
class StreamMetrics:
    """Tracks latency, volume, and failure statistics during streaming."""

    total_messages_received: int = 0
    total_trades_captured: int = 0
    total_heartbeats: int = 0
    dropped_events: int = 0
    reconnect_count: int = 0
    sequence_gaps: int = 0
    last_sequence: int | None = None
    latencies_ms: list[float] = field(default_factory=list)
    persistence_latencies_ms: list[float] = field(default_factory=list)

    def record_latency(self, latency_ms: float) -> None:
        """Record an exchange-to-app latency sample in milliseconds."""
        self.latencies_ms.append(latency_ms)

    def record_persistence_latency(self, latency_ms: float) -> None:
        """Record an app-to-disk write latency sample in milliseconds."""
        self.persistence_latencies_ms.append(latency_ms)

    def compute_percentiles(self) -> dict[str, dict[str, float | None]]:
        """Compute p50, p95, and p99 percentiles for latencies."""
        exchange_sorted = sorted(self.latencies_ms)
        persist_sorted = sorted(self.persistence_latencies_ms)

        return {
            "exchange_to_app_latency_ms": {
                "p50": _calc_percentile(exchange_sorted, 0.50),
                "p95": _calc_percentile(exchange_sorted, 0.95),
                "p99": _calc_percentile(exchange_sorted, 0.99),
            },
            "persistence_latency_ms": {
                "p50": _calc_percentile(persist_sorted, 0.50),
                "p95": _calc_percentile(persist_sorted, 0.95),
                "p99": _calc_percentile(persist_sorted, 0.99),
            },
        }

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        percentiles = self.compute_percentiles()
        return {
            "total_messages_received": self.total_messages_received,
            "total_trades_captured": self.total_trades_captured,
            "total_heartbeats": self.total_heartbeats,
            "dropped_events": self.dropped_events,
            "reconnect_count": self.reconnect_count,
            "sequence_gaps": self.sequence_gaps,
            **percentiles,
        }


@dataclass(frozen=True)
class ReconciliationReport:
    """Structured report comparing synthetic streaming candles against REST reference."""

    granularity_seconds: int
    windows_evaluated: int
    matched_windows: int
    mismatched_windows: int
    partial_windows: int
    deltas: list[dict[str, Any]]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Serialize ReconciliationReport to JSON-compatible dictionary."""
        return {
            "granularity_seconds": self.granularity_seconds,
            "windows_evaluated": self.windows_evaluated,
            "matched_windows": self.matched_windows,
            "mismatched_windows": self.mismatched_windows,
            "partial_windows": self.partial_windows,
            "deltas": self.deltas,
            "summary": self.summary,
        }
