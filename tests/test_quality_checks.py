"""Tests for data quality checks and blocking assertions."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from bitcoin_data_platform.quality.checks import (
    QualityCheckError,
    check_candle,
    filter_valid_candles,
    validate_candle_quality,
)


@dataclass
class DummyCandle:
    candle_start_utc: datetime | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume_base: Decimal | None


def test_valid_candle_passes_checks() -> None:
    """16. Valid candle passes all blocking checks."""
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    candle = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        open=Decimal("95000.00"),
        high=Decimal("96500.00"),
        low=Decimal("94800.00"),
        close=Decimal("96000.00"),
        volume_base=Decimal("123.456"),
    )
    violations = check_candle(candle, now_utc=now)
    assert violations == []
    # validate_candle_quality should not raise
    validate_candle_quality(candle, now_utc=now)


def test_ohlc_invariant_violation_caught() -> None:
    """17. OHLC invariant violation caught."""
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    # high < low
    candle_high_lt_low = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        open=Decimal("95000.00"),
        high=Decimal("94000.00"),
        low=Decimal("96000.00"),
        close=Decimal("95000.00"),
        volume_base=Decimal("10"),
    )
    v1 = check_candle(candle_high_lt_low, now_utc=now)
    assert any("high" in v and "low" in v for v in v1)

    # high < open
    candle_high_lt_open = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        open=Decimal("97000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94000.00"),
        close=Decimal("95000.00"),
        volume_base=Decimal("10"),
    )
    v2 = check_candle(candle_high_lt_open, now_utc=now)
    assert any("high" in v and "open" in v for v in v2)

    # low > close
    candle_low_gt_close = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        open=Decimal("95000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94500.00"),
        close=Decimal("94000.00"),
        volume_base=Decimal("10"),
    )
    v3 = check_candle(candle_low_gt_close, now_utc=now)
    assert any("low" in v and "close" in v for v in v3)

    with pytest.raises(QualityCheckError):
        validate_candle_quality(candle_high_lt_low, now_utc=now)


def test_non_hour_aligned_timestamp_caught() -> None:
    """18. Non-hour-aligned timestamp caught."""
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    candle_minute = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 1, 15, 0, tzinfo=UTC),
        open=Decimal("95000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94000.00"),
        close=Decimal("95500.00"),
        volume_base=Decimal("10"),
    )
    violations = check_candle(candle_minute, now_utc=now)
    assert any("hour-aligned" in v for v in violations)

    candle_second = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 1, 0, 45, tzinfo=UTC),
        open=Decimal("95000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94000.00"),
        close=Decimal("95500.00"),
        volume_base=Decimal("10"),
    )
    violations_sec = check_candle(candle_second, now_utc=now)
    assert any("hour-aligned" in v for v in violations_sec)


def test_future_timestamp_caught() -> None:
    """19. Future timestamp caught."""
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    future_candle = DummyCandle(
        candle_start_utc=now + timedelta(hours=1),
        open=Decimal("95000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94000.00"),
        close=Decimal("95500.00"),
        volume_base=Decimal("10"),
    )
    violations = check_candle(future_candle, now_utc=now)
    assert any("future" in v for v in violations)


def test_null_missing_field_caught() -> None:
    """20. Null/missing field caught."""
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    candle_null_open = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        open=None,
        high=Decimal("96000.00"),
        low=Decimal("94000.00"),
        close=Decimal("95500.00"),
        volume_base=Decimal("10"),
    )
    v1 = check_candle(candle_null_open, now_utc=now)
    assert any("open" in v for v in v1)

    candle_null_vol = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        open=Decimal("95000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94000.00"),
        close=Decimal("95500.00"),
        volume_base=None,
    )
    v2 = check_candle(candle_null_vol, now_utc=now)
    assert any("volume" in v for v in v2)

    candle_null_ts = DummyCandle(
        candle_start_utc=None,
        open=Decimal("95000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94000.00"),
        close=Decimal("95500.00"),
        volume_base=Decimal("10"),
    )
    v3 = check_candle(candle_null_ts, now_utc=now)
    assert any("timestamp" in v for v in v3)


def test_batch_mixed_valid_invalid_promotes_only_valid() -> None:
    """21. Batch with mix of valid/invalid: only valid promoted."""
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    valid1 = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        open=Decimal("95000.00"),
        high=Decimal("96000.00"),
        low=Decimal("94000.00"),
        close=Decimal("95500.00"),
        volume_base=Decimal("10"),
    )
    invalid_ohlc = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 1, 0, 0, tzinfo=UTC),
        open=Decimal("95000.00"),
        high=Decimal("93000.00"),  # high < low
        low=Decimal("94000.00"),
        close=Decimal("93500.00"),
        volume_base=Decimal("10"),
    )
    valid2 = DummyCandle(
        candle_start_utc=datetime(2026, 1, 1, 2, 0, 0, tzinfo=UTC),
        open=Decimal("95500.00"),
        high=Decimal("97000.00"),
        low=Decimal("95000.00"),
        close=Decimal("96500.00"),
        volume_base=Decimal("20"),
    )

    batch = [valid1, invalid_ohlc, valid2]
    promoted, violations = filter_valid_candles(batch, now_utc=now)

    assert len(promoted) == 2
    assert promoted == [valid1, valid2]
    assert len(violations) >= 1
    assert any("index 1" in v for v in violations)
