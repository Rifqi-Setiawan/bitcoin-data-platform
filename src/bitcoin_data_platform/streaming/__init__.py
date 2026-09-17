"""Real-time WebSocket trade streaming experiment module for Bitcoin Data Platform."""

from bitcoin_data_platform.streaming.buffer import StreamBuffer
from bitcoin_data_platform.streaming.candles import (
    CandleAggregationResult,
    SyntheticCandle,
    aggregate_synthetic_candles,
    build_candles_from_trades,
    load_committed_trades,
)
from bitcoin_data_platform.streaming.connection import (
    ConnectionState,
    WebSocketConnectionError,
    WebSocketConnectionManager,
)
from bitcoin_data_platform.streaming.models import (
    HeartbeatEvent,
    MicroBatchReceipt,
    ReconciliationReport,
    StreamMetrics,
    StreamTrade,
)
from bitcoin_data_platform.streaming.protocol import (
    LastMatchMarker,
    StreamProtocolError,
    SubscriptionAck,
    decode_message,
)
from bitcoin_data_platform.streaming.reconcile import (
    ReconciliationError,
    WindowReconciliationDelta,
    reconcile_with_rest,
)
from bitcoin_data_platform.streaming.runner import (
    StreamingSessionError,
    run_streaming_session,
)
from bitcoin_data_platform.streaming.writer import (
    DiskFullError,
    MicroBatchWriter,
    StorageError,
)

__all__ = [
    "CandleAggregationResult",
    "ConnectionState",
    "DiskFullError",
    "HeartbeatEvent",
    "LastMatchMarker",
    "MicroBatchReceipt",
    "MicroBatchWriter",
    "ReconciliationError",
    "ReconciliationReport",
    "StorageError",
    "StreamBuffer",
    "StreamMetrics",
    "StreamProtocolError",
    "StreamTrade",
    "StreamingSessionError",
    "SubscriptionAck",
    "SyntheticCandle",
    "WebSocketConnectionError",
    "WebSocketConnectionManager",
    "WindowReconciliationDelta",
    "aggregate_synthetic_candles",
    "build_candles_from_trades",
    "decode_message",
    "load_committed_trades",
    "reconcile_with_rest",
    "run_streaming_session",
]
