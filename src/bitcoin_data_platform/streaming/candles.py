"""Deterministic synthetic candle aggregation (1m and 1h) from trade stream replay."""

import json
import os
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from bitcoin_data_platform.streaming.models import MicroBatchReceipt, StreamTrade

CANDLE_PARQUET_SCHEMA = pa.schema(
    [
        ("product_id", pa.string()),
        ("granularity_seconds", pa.int32()),
        ("candle_start_utc", pa.timestamp("us", tz="UTC")),
        ("candle_end_utc", pa.timestamp("us", tz="UTC")),
        ("open", pa.decimal128(38, 18)),
        ("high", pa.decimal128(38, 18)),
        ("low", pa.decimal128(38, 18)),
        ("close", pa.decimal128(38, 18)),
        ("volume", pa.decimal128(38, 18)),
        ("trade_count", pa.int64()),
    ]
)


@dataclass(frozen=True)
class SyntheticCandle:
    """Represents an aggregated synthetic candle from raw trade replay."""

    product_id: str
    granularity_seconds: int
    candle_start_utc: datetime
    candle_end_utc: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int

    def to_dict(self) -> dict[str, Any]:
        """Convert candle to dictionary."""
        return {
            "product_id": self.product_id,
            "granularity_seconds": self.granularity_seconds,
            "candle_start_utc": self.candle_start_utc.isoformat(),
            "candle_end_utc": self.candle_end_utc.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "trade_count": self.trade_count,
        }


@dataclass(frozen=True)
class CandleAggregationResult:
    """Result of synthetic candle aggregation."""

    candles_1m: list[SyntheticCandle]
    candles_1h: list[SyntheticCandle]
    path_1m: Path | None
    path_1h: Path | None


def _write_candles_parquet(candles: list[SyntheticCandle], output_path: Path) -> None:
    """Write synthetic candles to Parquet file atomically."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(".parquet.tmp")

    table = pa.Table.from_arrays(
        [
            pa.array([c.product_id for c in candles], type=pa.string()),
            pa.array([c.granularity_seconds for c in candles], type=pa.int32()),
            pa.array([c.candle_start_utc for c in candles], type=pa.timestamp("us", tz="UTC")),
            pa.array([c.candle_end_utc for c in candles], type=pa.timestamp("us", tz="UTC")),
            pa.array([c.open for c in candles], type=pa.decimal128(38, 18)),
            pa.array([c.high for c in candles], type=pa.decimal128(38, 18)),
            pa.array([c.low for c in candles], type=pa.decimal128(38, 18)),
            pa.array([c.close for c in candles], type=pa.decimal128(38, 18)),
            pa.array([c.volume for c in candles], type=pa.decimal128(38, 18)),
            pa.array([c.trade_count for c in candles], type=pa.int64()),
        ],
        schema=CANDLE_PARQUET_SCHEMA,
    )

    pq.write_table(table, tmp_path)
    os.replace(tmp_path, output_path)


def load_committed_trades(run_dir: Path | str) -> list[StreamTrade]:
    """Replay committed trades from all batch receipts in a run directory."""
    run_path = Path(run_dir)
    commits_dir = run_path / "commits"
    if not commits_dir.exists():
        return []

    receipt_files = sorted(commits_dir.glob("*.json"))
    raw_trades: list[StreamTrade] = []

    for receipt_file in receipt_files:
        with open(receipt_file, encoding="utf-8") as f:
            receipt_data = json.load(f)
        receipt = MicroBatchReceipt.from_dict(receipt_data)

        segment_file = Path(receipt.segment_path)
        if not segment_file.is_absolute():
            segment_file = run_path / segment_file

        if not segment_file.exists():
            continue

        with open(segment_file, encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    trade_dict = json.loads(line_str)
                    raw_trades.append(StreamTrade.from_dict(trade_dict))

    return raw_trades


def build_candles_from_trades(
    trades: Sequence[StreamTrade],
    granularity_seconds: int,
    product_id: str = "BTC-USD",
) -> list[SyntheticCandle]:
    """Group trades into synthetic candles of specified granularity."""
    if not trades:
        return []

    # Deduplicate by trade_id, keeping first seen
    deduped: dict[int, StreamTrade] = {}
    for t in trades:
        if t.trade_id not in deduped:
            deduped[t.trade_id] = t

    # Deterministic sort
    sorted_trades = sorted(deduped.values(), key=lambda t: (t.time_utc, t.sequence, t.trade_id))

    buckets: dict[datetime, list[StreamTrade]] = defaultdict(list)
    for t in sorted_trades:
        ts = t.time_utc
        if granularity_seconds == 60:
            bucket_start = ts.replace(second=0, microsecond=0)
        elif granularity_seconds == 3600:
            bucket_start = ts.replace(minute=0, second=0, microsecond=0)
        else:
            epoch = int(ts.timestamp())
            b_epoch = (epoch // granularity_seconds) * granularity_seconds
            bucket_start = datetime.fromtimestamp(b_epoch, tz=UTC)
        buckets[bucket_start].append(t)

    candles: list[SyntheticCandle] = []
    for bucket_start in sorted(buckets.keys()):
        bucket_trades = buckets[bucket_start]
        candle_end = bucket_start + timedelta(seconds=granularity_seconds)
        open_price = bucket_trades[0].price
        high_price = max(t.price for t in bucket_trades)
        low_price = min(t.price for t in bucket_trades)
        close_price = bucket_trades[-1].price
        volume = sum((t.size for t in bucket_trades), Decimal("0"))
        trade_count = len(bucket_trades)

        candles.append(
            SyntheticCandle(
                product_id=product_id,
                granularity_seconds=granularity_seconds,
                candle_start_utc=bucket_start,
                candle_end_utc=candle_end,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
                trade_count=trade_count,
            )
        )

    return candles


def aggregate_synthetic_candles(
    run_dir: Path | str | None = None,
    *,
    trades: Sequence[StreamTrade] | None = None,
    derived_dir: Path | str | None = None,
    product_id: str = "BTC-USD",
) -> CandleAggregationResult:
    """Aggregate 1-minute and 1-hour synthetic candles from trades or run replay."""
    all_trades: list[StreamTrade] = []
    if trades is not None:
        all_trades.extend(trades)
    elif run_dir is not None:
        all_trades = load_committed_trades(run_dir)

    candles_1m = build_candles_from_trades(all_trades, 60, product_id=product_id)
    candles_1h = build_candles_from_trades(all_trades, 3600, product_id=product_id)

    path_1m: Path | None = None
    path_1h: Path | None = None

    if derived_dir is None and run_dir is not None:
        derived_dir = Path(run_dir) / "derived"

    if derived_dir is not None:
        target_dir = Path(derived_dir)
        path_1m = target_dir / "candles_1m.parquet"
        path_1h = target_dir / "candles_1h.parquet"
        _write_candles_parquet(candles_1m, path_1m)
        _write_candles_parquet(candles_1h, path_1h)

    return CandleAggregationResult(
        candles_1m=candles_1m,
        candles_1h=candles_1h,
        path_1m=path_1m,
        path_1h=path_1h,
    )
