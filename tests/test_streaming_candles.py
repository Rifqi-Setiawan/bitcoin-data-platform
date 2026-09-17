"""Unit tests for synthetic candle aggregation from trade stream replay."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from bitcoin_data_platform.streaming.candles import (
    aggregate_synthetic_candles,
    build_candles_from_trades,
    load_committed_trades,
)
from bitcoin_data_platform.streaming.models import StreamTrade
from bitcoin_data_platform.streaming.writer import MicroBatchWriter


def _make_trade(
    trade_id: int,
    ts: datetime,
    price: str,
    size: str,
    sequence: int = 1,
) -> StreamTrade:
    return StreamTrade(
        source="coinbase_exchange",
        product_id="BTC-USD",
        trade_id=trade_id,
        sequence=sequence,
        price=Decimal(price),
        size=Decimal(size),
        side="buy",
        time_utc=ts,
        received_at_utc=ts,
        received_monotonic_ns=1000,
    )


def test_build_candles_empty_trades() -> None:
    assert build_candles_from_trades([], 60) == []
    assert build_candles_from_trades([], 3600) == []


def test_candle_deduplication_and_tie_breaker() -> None:
    t0 = datetime(2026, 9, 18, 10, 0, 10, tzinfo=UTC)
    # Duplicate trade_id 101 with different prices: first seen should be retained
    trade_first = _make_trade(101, t0, "65000.00", "0.5", sequence=1)
    trade_dup = _make_trade(101, t0, "66000.00", "1.0", sequence=2)
    # Trade with same timestamp but higher sequence
    trade_next = _make_trade(102, t0, "65200.00", "0.2", sequence=3)

    candles = build_candles_from_trades([trade_first, trade_dup, trade_next], 60)
    assert len(candles) == 1
    candle = candles[0]
    assert candle.trade_count == 2
    assert candle.open == Decimal("65000.00")
    assert candle.close == Decimal("65200.00")
    assert candle.volume == Decimal("0.7")  # 0.5 + 0.2


def test_ohlcv_computation_1m_and_1h() -> None:
    # 3 trades in minute 10:00, 1 trade in minute 10:01
    t1 = datetime(2026, 9, 18, 10, 0, 5, tzinfo=UTC)
    t2 = datetime(2026, 9, 18, 10, 0, 20, tzinfo=UTC)
    t3 = datetime(2026, 9, 18, 10, 0, 45, tzinfo=UTC)
    t4 = datetime(2026, 9, 18, 10, 1, 10, tzinfo=UTC)

    trades = [
        _make_trade(1, t1, "65000.00", "1.0", sequence=1),
        _make_trade(2, t2, "65500.00", "0.5", sequence=2),
        _make_trade(3, t3, "64800.00", "0.2", sequence=3),
        _make_trade(4, t4, "65100.00", "0.3", sequence=4),
    ]

    # 1-minute candles
    candles_1m = build_candles_from_trades(trades, 60)
    assert len(candles_1m) == 2

    c0 = candles_1m[0]
    assert c0.candle_start_utc == datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    assert c0.candle_end_utc == datetime(2026, 9, 18, 10, 1, 0, tzinfo=UTC)
    assert c0.open == Decimal("65000.00")
    assert c0.high == Decimal("65500.00")
    assert c0.low == Decimal("64800.00")
    assert c0.close == Decimal("64800.00")
    assert c0.volume == Decimal("1.7")
    assert c0.trade_count == 3

    c1 = candles_1m[1]
    assert c1.candle_start_utc == datetime(2026, 9, 18, 10, 1, 0, tzinfo=UTC)
    assert c1.candle_end_utc == datetime(2026, 9, 18, 10, 2, 0, tzinfo=UTC)
    assert c1.open == Decimal("65100.00")
    assert c1.high == Decimal("65100.00")
    assert c1.low == Decimal("65100.00")
    assert c1.close == Decimal("65100.00")
    assert c1.volume == Decimal("0.3")
    assert c1.trade_count == 1

    # 1-hour candles: all 4 trades fall into 10:00:00 - 11:00:00
    candles_1h = build_candles_from_trades(trades, 3600)
    assert len(candles_1h) == 1
    ch = candles_1h[0]
    assert ch.candle_start_utc == datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    assert ch.candle_end_utc == datetime(2026, 9, 18, 11, 0, 0, tzinfo=UTC)
    assert ch.open == Decimal("65000.00")
    assert ch.high == Decimal("65500.00")
    assert ch.low == Decimal("64800.00")
    assert ch.close == Decimal("65100.00")
    assert ch.volume == Decimal("2.0")
    assert ch.trade_count == 4


def test_aggregate_synthetic_candles_and_parquet(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_test"
    writer = MicroBatchWriter(run_dir)

    t1 = datetime(2026, 9, 18, 10, 0, 10, tzinfo=UTC)
    t2 = datetime(2026, 9, 18, 10, 0, 30, tzinfo=UTC)
    trades = [
        _make_trade(1, t1, "65000.00", "1.0", sequence=1),
        _make_trade(2, t2, "65200.00", "1.5", sequence=2),
    ]
    writer.write_batch(trades, batch_id="000001")

    # Load from committed run_dir
    replayed = load_committed_trades(run_dir)
    assert len(replayed) == 2

    # Aggregate and write parquet
    result = aggregate_synthetic_candles(run_dir=run_dir)
    assert len(result.candles_1m) == 1
    assert len(result.candles_1h) == 1
    assert result.path_1m is not None and result.path_1m.exists()
    assert result.path_1h is not None and result.path_1h.exists()

    # Read back Parquet and verify contents
    table_1m = pq.read_table(result.path_1m)
    assert table_1m.num_rows == 1
    row_1m = table_1m.to_pylist()[0]
    assert row_1m["product_id"] == "BTC-USD"
    assert row_1m["granularity_seconds"] == 60
    assert row_1m["trade_count"] == 2
    assert row_1m["open"] == Decimal("65000.00")
    assert row_1m["close"] == Decimal("65200.00")
