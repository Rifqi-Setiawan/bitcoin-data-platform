"""Unit tests for WebSocket protocol decoding and contract validation."""

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from bitcoin_data_platform.streaming.models import (
    HeartbeatEvent,
    StreamTrade,
)
from bitcoin_data_platform.streaming.protocol import (
    LastMatchMarker,
    StreamProtocolError,
    SubscriptionAck,
    decode_message,
)


def test_decode_match_message_success() -> None:
    raw = json.dumps(
        {
            "type": "match",
            "trade_id": 1001,
            "sequence": 50001,
            "product_id": "BTC-USD",
            "price": "65123.45",
            "size": "0.12500000",
            "side": "buy",
            "time": "2026-09-18T10:00:00.123456Z",
        }
    )
    result = decode_message(raw)
    assert isinstance(result, StreamTrade)
    assert result.source == "coinbase_exchange"
    assert result.product_id == "BTC-USD"
    assert result.trade_id == 1001
    assert result.sequence == 50001
    assert result.price == Decimal("65123.45")
    assert result.size == Decimal("0.12500000")
    assert result.side == "buy"
    assert result.time_utc == datetime(2026, 9, 18, 10, 0, 0, 123456, tzinfo=UTC)


def test_decode_last_match_message() -> None:
    raw = json.dumps(
        {
            "type": "last_match",
            "trade_id": 999,
            "sequence": 50000,
            "product_id": "BTC-USD",
            "price": "65100.00",
            "size": "1.00000000",
            "side": "sell",
            "time": "2026-09-18T09:59:59.000000Z",
        }
    )
    result = decode_message(raw)
    assert isinstance(result, LastMatchMarker)
    assert isinstance(result.trade, StreamTrade)
    assert result.trade.trade_id == 999
    assert result.trade.side == "sell"
    assert result.trade.price == Decimal("65100.00")


def test_decode_heartbeat_message() -> None:
    raw = json.dumps(
        {
            "type": "heartbeat",
            "sequence": 50002,
            "last_trade_id": 1001,
            "product_id": "BTC-USD",
            "time": "2026-09-18T10:00:01.000000Z",
        }
    )
    result = decode_message(raw)
    assert isinstance(result, HeartbeatEvent)
    assert result.sequence == 50002
    assert result.last_trade_id == 1001
    assert result.product_id == "BTC-USD"
    assert result.time_utc == datetime(2026, 9, 18, 10, 0, 1, tzinfo=UTC)


def test_decode_subscriptions_ack() -> None:
    raw = json.dumps(
        {
            "type": "subscriptions",
            "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}],
        }
    )
    result = decode_message(raw)
    assert isinstance(result, SubscriptionAck)
    assert len(result.channels) == 1
    assert result.channels[0]["name"] == "matches"


def test_decode_ignored_type() -> None:
    raw = json.dumps({"type": "status", "products": []})
    result = decode_message(raw)
    assert result is None


def test_decode_feed_error_message() -> None:
    raw = json.dumps({"type": "error", "message": "Failed to subscribe"})
    with pytest.raises(StreamProtocolError, match="Coinbase feed returned error"):
        decode_message(raw)


def test_decode_invalid_json() -> None:
    with pytest.raises(StreamProtocolError, match="invalid JSON"):
        decode_message("not valid json {")


def test_decode_non_dict_json() -> None:
    with pytest.raises(StreamProtocolError, match="expected JSON object"):
        decode_message("[1, 2, 3]")


def test_decode_missing_type() -> None:
    with pytest.raises(StreamProtocolError, match="Missing required 'type' field"):
        decode_message(json.dumps({"trade_id": 1}))


def test_decode_prohibits_binary_float() -> None:
    raw = json.dumps(
        {
            "type": "match",
            "trade_id": 1,
            "sequence": 1,
            "product_id": "BTC-USD",
            "price": 65000.5,
            "size": "0.1",
            "side": "buy",
            "time": "2026-09-18T10:00:00Z",
        }
    )
    with pytest.raises(StreamProtocolError, match="binary float conversion is prohibited"):
        decode_message(raw)


def test_decode_rejects_negative_or_zero_values() -> None:
    for bad_price in ("-100", "0"):
        raw = json.dumps(
            {
                "type": "match",
                "trade_id": 1,
                "sequence": 1,
                "product_id": "BTC-USD",
                "price": bad_price,
                "size": "0.1",
                "side": "buy",
                "time": "2026-09-18T10:00:00Z",
            }
        )
        with pytest.raises(StreamProtocolError, match="must be positive finite number"):
            decode_message(raw)


def test_decode_rejects_invalid_side() -> None:
    raw = json.dumps(
        {
            "type": "match",
            "trade_id": 1,
            "sequence": 1,
            "product_id": "BTC-USD",
            "price": "65000.00",
            "size": "0.1",
            "side": "hold",
            "time": "2026-09-18T10:00:00Z",
        }
    )
    with pytest.raises(StreamProtocolError, match="must be 'buy' or 'sell'"):
        decode_message(raw)


def test_decode_rejects_naive_timestamp() -> None:
    raw = json.dumps(
        {
            "type": "match",
            "trade_id": 1,
            "sequence": 1,
            "product_id": "BTC-USD",
            "price": "65000.00",
            "size": "0.1",
            "side": "buy",
            "time": "2026-09-18T10:00:00",
        }
    )
    with pytest.raises(StreamProtocolError, match="naive datetimes are rejected"):
        decode_message(raw)


def test_decode_bytes_input() -> None:
    payload = json.dumps(
        {
            "type": "match",
            "trade_id": 2,
            "sequence": 2,
            "product_id": "BTC-USD",
            "price": "65000.00",
            "size": "0.5",
            "side": "sell",
            "time": "2026-09-18T10:00:00Z",
        }
    ).encode("utf-8")
    result = decode_message(payload)
    assert isinstance(result, StreamTrade)
    assert result.trade_id == 2


def test_decode_invalid_utf8_bytes() -> None:
    with pytest.raises(StreamProtocolError, match="UTF-8 decoding error"):
        decode_message(b"\xff\xfe\xfd")


def test_stream_trade_roundtrip_dict() -> None:
    now = datetime.now(UTC)
    trade = StreamTrade(
        source="coinbase_exchange",
        product_id="BTC-USD",
        trade_id=10,
        sequence=20,
        price=Decimal("65000.25"),
        size=Decimal("0.50000000"),
        side="buy",
        time_utc=now,
        received_at_utc=now,
        received_monotonic_ns=123456789,
    )
    d = trade.to_dict()
    restored = StreamTrade.from_dict(d)
    assert restored == trade


def test_heartbeat_roundtrip_dict() -> None:
    now = datetime.now(UTC)
    hb = HeartbeatEvent(
        sequence=55,
        last_trade_id=12,
        product_id="BTC-USD",
        time_utc=now,
        received_at_utc=now,
    )
    d = hb.to_dict()
    restored = HeartbeatEvent.from_dict(d)
    assert restored == hb
