"""Unit tests for bounded FIFO streaming buffer."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from bitcoin_data_platform.streaming.buffer import StreamBuffer
from bitcoin_data_platform.streaming.models import StreamTrade


def _make_trade(trade_id: int) -> StreamTrade:
    now = datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    return StreamTrade(
        source="coinbase_exchange",
        product_id="BTC-USD",
        trade_id=trade_id,
        sequence=trade_id * 10,
        price=Decimal("65000.00"),
        size=Decimal("0.1"),
        side="buy",
        time_utc=now,
        received_at_utc=now,
        received_monotonic_ns=1000 + trade_id,
    )


def test_buffer_initialization_and_validation() -> None:
    buf = StreamBuffer(max_items=100, max_bytes=1024)
    assert buf.size == 0
    assert buf.bytes_size == 0
    assert buf.dropped_events == 0
    assert buf.total_pushed == 0
    assert buf.is_empty is True
    assert buf.is_full is False

    with pytest.raises(ValueError, match="max_items must be a positive integer"):
        StreamBuffer(max_items=0)

    with pytest.raises(ValueError, match="max_bytes must be a positive integer"):
        StreamBuffer(max_bytes=-1)


def test_buffer_fifo_ordering() -> None:
    buf = StreamBuffer(max_items=10)
    t1 = _make_trade(1)
    t2 = _make_trade(2)
    t3 = _make_trade(3)

    assert buf.push(t1) is True
    assert buf.push(t2) is True
    assert buf.push(t3) is True

    assert buf.size == 3
    assert buf.is_empty is False

    batch = buf.pop_batch(max_batch_size=2)
    assert len(batch) == 2
    assert batch[0].trade_id == 1
    assert batch[1].trade_id == 2
    assert buf.size == 1

    remaining = buf.pop_batch(max_batch_size=2)
    assert len(remaining) == 1
    assert remaining[0].trade_id == 3
    assert buf.size == 0
    assert buf.is_empty is True


def test_buffer_bounded_size_limit_and_drop_newest() -> None:
    buf = StreamBuffer(max_items=3)
    t1 = _make_trade(1)
    t2 = _make_trade(2)
    t3 = _make_trade(3)
    t4 = _make_trade(4)

    assert buf.push(t1) is True
    assert buf.push(t2) is True
    assert buf.push(t3) is True
    assert buf.is_full is True

    # 4th trade should be dropped (drop-newest policy)
    assert buf.push(t4) is False
    assert buf.dropped_events == 1
    assert buf.total_pushed == 3
    assert buf.size == 3

    # Queue contents must remain intact (t1, t2, t3)
    batch = buf.pop_batch(max_batch_size=10)
    assert [t.trade_id for t in batch] == [1, 2, 3]


def test_buffer_byte_limit_overflow() -> None:
    # 256 bytes per trade, limit to 600 bytes -> can only hold 2 trades (512 bytes)
    buf = StreamBuffer(max_items=100, max_bytes=600, trade_byte_estimate=256)
    t1 = _make_trade(1)
    t2 = _make_trade(2)
    t3 = _make_trade(3)

    assert buf.push(t1) is True
    assert buf.bytes_size == 256

    assert buf.push(t2) is True
    assert buf.bytes_size == 512
    assert buf.is_full is True  # 512 + 256 > 600

    assert buf.push(t3) is False
    assert buf.dropped_events == 1
    assert buf.size == 2


def test_buffer_push_many() -> None:
    buf = StreamBuffer(max_items=2)
    trades = [_make_trade(i) for i in range(5)]
    accepted = buf.push_many(trades)
    assert accepted == 2
    assert buf.dropped_events == 3
    assert buf.size == 2


def test_buffer_drain() -> None:
    buf = StreamBuffer(max_items=10)
    trades = [_make_trade(i) for i in range(4)]
    buf.push_many(trades)

    drained = buf.drain()
    assert len(drained) == 4
    assert [t.trade_id for t in drained] == [0, 1, 2, 3]
    assert buf.size == 0
    assert buf.bytes_size == 0
    assert buf.is_empty is True


def test_buffer_pop_batch_zero_or_empty() -> None:
    buf = StreamBuffer(max_items=5)
    assert buf.pop_batch(0) == []
    assert buf.pop_batch(-1) == []
    assert buf.pop_batch(5) == []
