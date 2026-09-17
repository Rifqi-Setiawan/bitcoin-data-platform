"""Coinbase Exchange WebSocket protocol decoder and message validator."""

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from bitcoin_data_platform.streaming.models import HeartbeatEvent, StreamTrade


class StreamProtocolError(ValueError):
    """Raised when a WebSocket message fails schema or protocol validation."""


@dataclass(frozen=True)
class SubscriptionAck:
    """Represents a subscription confirmation message from Coinbase."""

    channels: list[dict[str, Any]]


@dataclass(frozen=True)
class LastMatchMarker:
    """Represents the initial snapshot trade marker (last_match)."""

    trade: StreamTrade


def _parse_decimal(value: Any, field_name: str) -> Decimal:
    """Safely parse a Decimal from string or int, strictly rejecting float."""
    if isinstance(value, float):
        raise StreamProtocolError(
            f"Invalid {field_name}: binary float conversion is prohibited; value must be str or int"
        )
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise StreamProtocolError(
            f"Invalid {field_name}: cannot parse '{value}' as Decimal"
        ) from exc

    if not dec.is_finite() or dec <= 0:
        raise StreamProtocolError(
            f"Invalid {field_name}: value must be positive finite number, got {dec}"
        )
    return dec


def _parse_iso_utc(val: str, field_name: str) -> datetime:
    """Parse an ISO-8601 timestamp string into timezone-aware UTC datetime."""
    try:
        raw = val.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
    except Exception as exc:
        raise StreamProtocolError(f"Invalid {field_name}: malformed ISO timestamp '{val}'") from exc

    if dt.tzinfo is None:
        raise StreamProtocolError(
            f"Invalid {field_name}: naive datetimes are rejected; must have UTC offset"
        )
    return dt.astimezone(UTC)


def decode_message(
    raw_msg: str | bytes,
    *,
    received_at_utc: datetime | None = None,
    received_monotonic_ns: int | None = None,
    default_source: str = "coinbase_exchange",
) -> StreamTrade | HeartbeatEvent | SubscriptionAck | LastMatchMarker | None:
    """Decode and validate a Coinbase WebSocket feed message.

    Returns:
    - StreamTrade: for 'match' messages
    - LastMatchMarker: for 'last_match' snapshot markers
    - HeartbeatEvent: for 'heartbeat' messages
    - SubscriptionAck: for 'subscriptions' messages
    - None: for recognized but non-actionable message types

    Raises:
    - StreamProtocolError: if message violates protocol contract or type is 'error'
    """
    if isinstance(raw_msg, bytes):
        try:
            raw_msg = raw_msg.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise StreamProtocolError("Malformed message: UTF-8 decoding error") from exc

    try:
        data = json.loads(raw_msg)
    except Exception as exc:
        raise StreamProtocolError(f"Malformed message: invalid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise StreamProtocolError(
            f"Invalid message shape: expected JSON object, got {type(data).__name__}"
        )

    msg_type = data.get("type")
    if not msg_type:
        raise StreamProtocolError("Missing required 'type' field in WebSocket message")

    if msg_type == "error":
        reason = data.get("message") or data.get("reason") or "Unknown feed error"
        raise StreamProtocolError(f"Coinbase feed returned error: {reason}")

    if msg_type == "subscriptions":
        channels = data.get("channels")
        if not isinstance(channels, list):
            channels = []
        return SubscriptionAck(channels=channels)

    now_wall = received_at_utc or datetime.now(UTC)
    now_mono = received_monotonic_ns if received_monotonic_ns is not None else time.monotonic_ns()

    if msg_type in ("match", "last_match"):
        product_id = data.get("product_id")
        if not product_id or not isinstance(product_id, str):
            raise StreamProtocolError("Missing or invalid 'product_id' in match message")

        if "trade_id" not in data:
            raise StreamProtocolError("Missing 'trade_id' in match message")
        try:
            trade_id = int(data["trade_id"])
        except (ValueError, TypeError) as exc:
            raise StreamProtocolError(
                f"Invalid 'trade_id' in match message: {data.get('trade_id')}"
            ) from exc

        if "sequence" not in data:
            raise StreamProtocolError("Missing 'sequence' in match message")
        try:
            sequence = int(data["sequence"])
        except (ValueError, TypeError) as exc:
            raise StreamProtocolError(
                f"Invalid 'sequence' in match message: {data.get('sequence')}"
            ) from exc

        price = _parse_decimal(data.get("price"), "price")
        size = _parse_decimal(data.get("size"), "size")

        side = data.get("side")
        if side not in ("buy", "sell"):
            raise StreamProtocolError(f"Invalid 'side': must be 'buy' or 'sell', got '{side}'")

        time_raw = data.get("time")
        if not time_raw:
            raise StreamProtocolError("Missing 'time' in match message")
        time_utc = _parse_iso_utc(str(time_raw), "time")

        trade = StreamTrade(
            source=default_source,
            product_id=product_id,
            trade_id=trade_id,
            sequence=sequence,
            price=price,
            size=size,
            side=side,
            time_utc=time_utc,
            received_at_utc=now_wall,
            received_monotonic_ns=now_mono,
        )
        if msg_type == "last_match":
            return LastMatchMarker(trade=trade)
        return trade

    if msg_type == "heartbeat":
        product_id = data.get("product_id")
        if not product_id or not isinstance(product_id, str):
            raise StreamProtocolError("Missing or invalid 'product_id' in heartbeat message")

        if "sequence" not in data:
            raise StreamProtocolError("Missing 'sequence' in heartbeat message")
        try:
            sequence = int(data["sequence"])
        except (ValueError, TypeError) as exc:
            raise StreamProtocolError("Invalid 'sequence' in heartbeat message") from exc

        if "last_trade_id" not in data:
            raise StreamProtocolError("Missing 'last_trade_id' in heartbeat message")
        try:
            last_trade_id = int(data["last_trade_id"])
        except (ValueError, TypeError) as exc:
            raise StreamProtocolError("Invalid 'last_trade_id' in heartbeat message") from exc

        time_raw = data.get("time")
        if not time_raw:
            raise StreamProtocolError("Missing 'time' in heartbeat message")
        time_utc = _parse_iso_utc(str(time_raw), "time")

        return HeartbeatEvent(
            sequence=sequence,
            last_trade_id=last_trade_id,
            product_id=product_id,
            time_utc=time_utc,
            received_at_utc=now_wall,
        )

    # Unhandled or informational message type (e.g. status)
    return None
