"""Unit tests for reconciliation engine comparing streaming candles with REST."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from bitcoin_data_platform.sources.coinbase_contract import CoinbaseCandle
from bitcoin_data_platform.streaming.candles import SyntheticCandle
from bitcoin_data_platform.streaming.reconcile import (
    ReconciliationError,
    reconcile_with_rest,
)


def _make_synthetic_candle(
    start_utc: datetime,
    granularity: int = 60,
    open_price: str = "65000.00",
    high_price: str = "65500.00",
    low_price: str = "64900.00",
    close_price: str = "65200.00",
    volume: str = "10.5",
    trade_count: int = 25,
) -> SyntheticCandle:
    return SyntheticCandle(
        product_id="BTC-USD",
        granularity_seconds=granularity,
        candle_start_utc=start_utc,
        candle_end_utc=start_utc + timedelta(seconds=granularity),
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=Decimal(volume),
        trade_count=trade_count,
    )


def _make_rest_candle(
    start_utc: datetime,
    open_price: str = "65000.00",
    high_price: str = "65500.00",
    low_price: str = "64900.00",
    close_price: str = "65200.00",
    volume: str = "10.5",
) -> CoinbaseCandle:
    return CoinbaseCandle(
        timestamp_utc=start_utc,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=Decimal(volume),
    )


def test_reconcile_empty_synthetic_candles() -> None:
    t0 = datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 9, 18, 10, 1, 0, tzinfo=UTC)
    report = reconcile_with_rest([], t0, t1)
    assert report.windows_evaluated == 0
    assert report.summary["match_rate"] == 1.0


def test_reconcile_exact_match() -> None:
    w_start = datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 18, 10, 1, 0, tzinfo=UTC)

    synth = _make_synthetic_candle(w_start)
    rest = _make_rest_candle(w_start)

    # Session covers the full window [10:00:00, 10:01:00]
    report = reconcile_with_rest(
        [synth],
        session_start_utc=w_start,
        session_end_utc=w_end,
        rest_candles_override=[rest],
    )

    assert report.windows_evaluated == 1
    assert report.matched_windows == 1
    assert report.mismatched_windows == 0
    assert report.partial_windows == 0
    assert report.summary["match_rate"] == 1.0

    delta = report.deltas[0]
    assert delta["status"] == "matched"
    assert Decimal(delta["deltas"]["delta_open"]) == Decimal(0)
    assert Decimal(delta["deltas"]["delta_volume"]) == Decimal(0)


def test_reconcile_mismatch_detected() -> None:
    w_start = datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 18, 10, 1, 0, tzinfo=UTC)

    synth = _make_synthetic_candle(w_start, close_price="65205.00", volume="11.0")
    rest = _make_rest_candle(w_start, close_price="65200.00", volume="10.5")

    report = reconcile_with_rest(
        [synth],
        session_start_utc=w_start,
        session_end_utc=w_end,
        rest_candles_override=[rest],
    )

    assert report.windows_evaluated == 1
    assert report.matched_windows == 0
    assert report.mismatched_windows == 1
    assert report.partial_windows == 0
    assert report.summary["match_rate"] == 0.0

    delta = report.deltas[0]
    assert delta["status"] == "mismatched"
    assert delta["deltas"]["delta_close"] == "5.00"
    assert delta["deltas"]["delta_volume"] == "0.5"


def test_reconcile_partial_capture() -> None:
    w_start = datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 18, 10, 1, 0, tzinfo=UTC)

    synth = _make_synthetic_candle(w_start)
    rest = _make_rest_candle(w_start)

    # Session started after window start and ended before window end
    session_start = w_start + timedelta(seconds=15)
    session_end = w_end - timedelta(seconds=10)

    report = reconcile_with_rest(
        [synth],
        session_start_utc=session_start,
        session_end_utc=session_end,
        rest_candles_override=[rest],
    )

    assert report.windows_evaluated == 1
    assert report.partial_windows == 1
    assert report.deltas[0]["status"] == "partial_capture"


def test_reconcile_missing_counterpart() -> None:
    w_start = datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 18, 10, 1, 0, tzinfo=UTC)

    synth = _make_synthetic_candle(w_start)
    # REST has no candle for this window
    report = reconcile_with_rest(
        [synth],
        session_start_utc=w_start,
        session_end_utc=w_end,
        rest_candles_override=[],
    )

    assert report.windows_evaluated == 1
    assert report.mismatched_windows == 1
    assert report.deltas[0]["rest"]["open"] is None


def test_reconcile_client_error_raises() -> None:
    mock_client = MagicMock()
    mock_client.fetch_candles.side_effect = RuntimeError("REST API connection timeout")

    w_start = datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    synth = _make_synthetic_candle(w_start)

    with pytest.raises(ReconciliationError, match="Failed to fetch REST candles"):
        reconcile_with_rest(
            [synth],
            session_start_utc=w_start,
            session_end_utc=w_start + timedelta(seconds=60),
            coinbase_client=mock_client,
        )
