"""Reconciliation engine comparing synthetic streaming candles against Coinbase REST reference."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from bitcoin_data_platform.sources.coinbase_client import CoinbaseClient
from bitcoin_data_platform.sources.coinbase_contract import CoinbaseCandle
from bitcoin_data_platform.streaming.candles import SyntheticCandle
from bitcoin_data_platform.streaming.models import ReconciliationReport


class ReconciliationError(Exception):
    """Raised when reconciliation against reference source fails."""


@dataclass(frozen=True)
class WindowReconciliationDelta:
    """Detailed reconciliation comparison for a single candle window."""

    window_start_utc: datetime
    window_end_utc: datetime
    granularity_seconds: int
    status: str  # "matched", "mismatched", "partial_capture"
    stream_open: Decimal | None
    stream_high: Decimal | None
    stream_low: Decimal | None
    stream_close: Decimal | None
    stream_volume: Decimal | None
    stream_trade_count: int
    rest_open: Decimal | None
    rest_high: Decimal | None
    rest_low: Decimal | None
    rest_close: Decimal | None
    rest_volume: Decimal | None
    delta_open: Decimal | None
    delta_high: Decimal | None
    delta_low: Decimal | None
    delta_close: Decimal | None
    delta_volume: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        """Serialize delta to dictionary."""
        return {
            "window_start_utc": self.window_start_utc.isoformat(),
            "window_end_utc": self.window_end_utc.isoformat(),
            "granularity_seconds": self.granularity_seconds,
            "status": self.status,
            "stream": {
                "open": str(self.stream_open) if self.stream_open is not None else None,
                "high": str(self.stream_high) if self.stream_high is not None else None,
                "low": str(self.stream_low) if self.stream_low is not None else None,
                "close": str(self.stream_close) if self.stream_close is not None else None,
                "volume": str(self.stream_volume) if self.stream_volume is not None else None,
                "trade_count": self.stream_trade_count,
            },
            "rest": {
                "open": str(self.rest_open) if self.rest_open is not None else None,
                "high": str(self.rest_high) if self.rest_high is not None else None,
                "low": str(self.rest_low) if self.rest_low is not None else None,
                "close": str(self.rest_close) if self.rest_close is not None else None,
                "volume": str(self.rest_volume) if self.rest_volume is not None else None,
            },
            "deltas": {
                "delta_open": str(self.delta_open) if self.delta_open is not None else None,
                "delta_high": str(self.delta_high) if self.delta_high is not None else None,
                "delta_low": str(self.delta_low) if self.delta_low is not None else None,
                "delta_close": str(self.delta_close) if self.delta_close is not None else None,
                "delta_volume": str(self.delta_volume) if self.delta_volume is not None else None,
            },
        }


def reconcile_with_rest(
    synthetic_candles: Sequence[SyntheticCandle],
    session_start_utc: datetime,
    session_end_utc: datetime,
    *,
    coinbase_client: CoinbaseClient | None = None,
    granularity_seconds: int = 60,
    product_id: str = "BTC-USD",
    rest_candles_override: list[CoinbaseCandle] | None = None,
) -> ReconciliationReport:
    """Compare synthetic streaming candles with official Coinbase REST candles.

    Computes deltas for OHLCV:
    delta = stream_value - rest_value

    Status values:
    - 'matched': full-coverage window with identical OHLCV
    - 'mismatched': full-coverage window with delta discrepancies
    - 'partial_capture': window only partially covered by the streaming session
    """
    if not synthetic_candles and rest_candles_override is None:
        return ReconciliationReport(
            granularity_seconds=granularity_seconds,
            windows_evaluated=0,
            matched_windows=0,
            mismatched_windows=0,
            partial_windows=0,
            deltas=[],
            summary={
                "message": "No synthetic candles to reconcile",
                "match_rate": 1.0,
            },
        )

    # Filter candles matching granularity
    matching_stream_candles = [
        c for c in synthetic_candles if c.granularity_seconds == granularity_seconds
    ]
    stream_map = {c.candle_start_utc: c for c in matching_stream_candles}

    # Determine reference window bounds
    rest_candles: list[CoinbaseCandle] = []
    if rest_candles_override is not None:
        rest_candles = rest_candles_override
    else:
        client = coinbase_client or CoinbaseClient(
            granularity_seconds=granularity_seconds,
            product_id=product_id,
        )
        if matching_stream_candles:
            fetch_start = min(c.candle_start_utc for c in matching_stream_candles)
            fetch_end = max(c.candle_end_utc for c in matching_stream_candles)
        else:
            fetch_start = session_start_utc
            fetch_end = session_end_utc

        try:
            resp = client.fetch_candles(start_utc=fetch_start, end_utc=fetch_end)
            rest_candles = resp.candles
        except Exception as exc:
            raise ReconciliationError(
                f"Failed to fetch REST candles for reconciliation: {exc}"
            ) from exc

    rest_map = {c.timestamp_utc: c for c in rest_candles}

    # All unique window start timestamps
    all_starts = sorted(set(stream_map.keys()) | set(rest_map.keys()))

    deltas: list[WindowReconciliationDelta] = []
    matched_count = 0
    mismatched_count = 0
    partial_count = 0

    for w_start in all_starts:
        w_end = w_start + (
            stream_map[w_start].candle_end_utc - w_start
            if w_start in stream_map
            else (
                matching_stream_candles[0].candle_end_utc
                - matching_stream_candles[0].candle_start_utc
                if matching_stream_candles
                else session_end_utc - session_start_utc
            )
        )

        stream_c = stream_map.get(w_start)
        rest_c = rest_map.get(w_start)

        # Check if streaming run fully covered the window interval [w_start, w_end]
        is_full_coverage = session_start_utc <= w_start and session_end_utc >= w_end

        delta_open: Decimal | None = None
        delta_high: Decimal | None = None
        delta_low: Decimal | None = None
        delta_close: Decimal | None = None
        delta_volume: Decimal | None = None

        if stream_c is not None and rest_c is not None:
            delta_open = stream_c.open - rest_c.open
            delta_high = stream_c.high - rest_c.high
            delta_low = stream_c.low - rest_c.low
            delta_close = stream_c.close - rest_c.close
            delta_volume = stream_c.volume - rest_c.volume

        if not is_full_coverage:
            status = "partial_capture"
            partial_count += 1
        elif (
            delta_open is not None
            and delta_high is not None
            and delta_low is not None
            and delta_close is not None
            and delta_volume is not None
        ):
            if (
                delta_open == 0
                and delta_high == 0
                and delta_low == 0
                and delta_close == 0
                and delta_volume == 0
            ):
                status = "matched"
                matched_count += 1
            else:
                status = "mismatched"
                mismatched_count += 1
        else:
            status = "mismatched"
            mismatched_count += 1

        deltas.append(
            WindowReconciliationDelta(
                window_start_utc=w_start,
                window_end_utc=w_end,
                granularity_seconds=granularity_seconds,
                status=status,
                stream_open=stream_c.open if stream_c else None,
                stream_high=stream_c.high if stream_c else None,
                stream_low=stream_c.low if stream_c else None,
                stream_close=stream_c.close if stream_c else None,
                stream_volume=stream_c.volume if stream_c else None,
                stream_trade_count=stream_c.trade_count if stream_c else 0,
                rest_open=rest_c.open if rest_c else None,
                rest_high=rest_c.high if rest_c else None,
                rest_low=rest_c.low if rest_c else None,
                rest_close=rest_c.close if rest_c else None,
                rest_volume=rest_c.volume if rest_c else None,
                delta_open=delta_open,
                delta_high=delta_high,
                delta_low=delta_low,
                delta_close=delta_close,
                delta_volume=delta_volume,
            )
        )

    total_evaluated = len(deltas)
    match_rate = matched_count / total_evaluated if total_evaluated > 0 else 1.0

    return ReconciliationReport(
        granularity_seconds=granularity_seconds,
        windows_evaluated=total_evaluated,
        matched_windows=matched_count,
        mismatched_windows=mismatched_count,
        partial_windows=partial_count,
        deltas=[d.to_dict() for d in deltas],
        summary={
            "total_windows": total_evaluated,
            "matched_windows": matched_count,
            "mismatched_windows": mismatched_count,
            "partial_windows": partial_count,
            "match_rate": round(match_rate, 4),
        },
    )
