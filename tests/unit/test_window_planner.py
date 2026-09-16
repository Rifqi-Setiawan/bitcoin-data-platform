"""Unit tests for the Window Planner module."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from bitcoin_data_platform.ingestion.window_planner import (
    InvalidIntervalError,
    InvalidTimezoneError,
    MisalignedBoundaryError,
    OpenCandleError,
    PlannedWindow,
    SafetyLimitExceededError,
    plan_windows,
)


def test_rejects_naive_start_datetime() -> None:
    start = datetime(2026, 1, 1, 0, 0)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    with pytest.raises(InvalidTimezoneError, match="must be explicit UTC"):
        plan_windows(start, end)


def test_rejects_naive_end_datetime() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0)
    with pytest.raises(InvalidTimezoneError, match="must be explicit UTC"):
        plan_windows(start, end)


def test_rejects_non_utc_timezone() -> None:
    non_utc = timezone(timedelta(hours=7))
    start = datetime(2026, 1, 1, 7, 0, tzinfo=non_utc)
    end = datetime(2026, 1, 1, 8, 0, tzinfo=UTC)
    with pytest.raises(InvalidTimezoneError, match="must be explicit UTC"):
        plan_windows(start, end)


def test_rejects_misaligned_start_minute() -> None:
    start = datetime(2026, 1, 1, 0, 15, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    with pytest.raises(MisalignedBoundaryError, match="aligned to hourly boundary"):
        plan_windows(start, end)


def test_rejects_misaligned_end_minute() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 30, tzinfo=UTC)
    with pytest.raises(MisalignedBoundaryError, match="aligned to hourly boundary"):
        plan_windows(start, end)


def test_rejects_misaligned_second_or_microsecond() -> None:
    start = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    with pytest.raises(MisalignedBoundaryError, match="aligned to hourly boundary"):
        plan_windows(start, end)

    start = datetime(2026, 1, 1, 0, 0, 0, 500, tzinfo=UTC)
    with pytest.raises(MisalignedBoundaryError, match="aligned to hourly boundary"):
        plan_windows(start, end)


def test_rejects_equal_start_and_end() -> None:
    dt = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    with pytest.raises(InvalidIntervalError, match="strictly before end"):
        plan_windows(dt, dt)


def test_rejects_reversed_interval() -> None:
    start = datetime(2026, 1, 1, 5, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 2, 0, tzinfo=UTC)
    with pytest.raises(InvalidIntervalError, match="strictly before end"):
        plan_windows(start, end)


def test_rejects_open_current_hour_candle() -> None:
    now_utc = datetime(2026, 9, 17, 14, 35, tzinfo=UTC)
    start = datetime(2026, 9, 17, 10, 0, tzinfo=UTC)
    end = datetime(2026, 9, 17, 15, 0, tzinfo=UTC)
    with pytest.raises(OpenCandleError, match="open or future candle"):
        plan_windows(start, end, now_utc=now_utc)


def test_allows_up_to_latest_completed_hour() -> None:
    now_utc = datetime(2026, 9, 17, 14, 35, tzinfo=UTC)
    start = datetime(2026, 9, 17, 10, 0, tzinfo=UTC)
    end = datetime(2026, 9, 17, 14, 0, tzinfo=UTC)
    windows = plan_windows(start, end, now_utc=now_utc)
    assert len(windows) == 1
    assert windows[0].expected_candles == 4


def test_allow_open_candle_override() -> None:
    now_utc = datetime(2026, 9, 17, 14, 35, tzinfo=UTC)
    start = datetime(2026, 9, 17, 10, 0, tzinfo=UTC)
    end = datetime(2026, 9, 17, 15, 0, tzinfo=UTC)
    windows = plan_windows(start, end, now_utc=now_utc, allow_open_candle=True)
    assert len(windows) == 1
    assert windows[0].expected_candles == 5


def test_single_hour_window() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 1
    w = windows[0]
    assert w.start_utc == start
    assert w.end_utc == end
    assert w.expected_candles == 1
    assert w.granularity_seconds == 3600
    assert w.product_id == "BTC-USD"
    assert w.start_iso == "2026-01-01T00:00:00+00:00"
    assert w.end_iso == "2026-01-01T01:00:00+00:00"


def test_exactly_300_hours_produces_single_window() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=300)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 1
    assert windows[0].expected_candles == 300
    assert windows[0].start_utc == start
    assert windows[0].end_utc == end


def test_301_hours_splits_into_300_and_1() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=301)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 2
    assert windows[0].expected_candles == 300
    assert windows[0].start_utc == start
    assert windows[0].end_utc == start + timedelta(hours=300)

    assert windows[1].expected_candles == 1
    assert windows[1].start_utc == start + timedelta(hours=300)
    assert windows[1].end_utc == end


def test_600_hours_splits_into_two_equal_windows() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=600)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 2
    assert windows[0].expected_candles == 300
    assert windows[1].expected_candles == 300
    assert windows[0].end_utc == windows[1].start_utc


def test_large_interval_partitions_deterministically_without_gaps() -> None:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    total_hours = 8760  # 1 non-leap year
    end = start + timedelta(hours=total_hours)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 30
    assert windows[-1].expected_candles == 60

    assert windows[0].start_utc == start
    assert windows[-1].end_utc == end

    for i in range(len(windows)):
        assert windows[i].expected_candles <= 300
        assert windows[i].start_utc < windows[i].end_utc
        if i > 0:
            assert windows[i].start_utc == windows[i - 1].end_utc

    total_candles = sum(w.expected_candles for w in windows)
    assert total_candles == total_hours


def test_safety_limit_enforced() -> None:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=100)
    with pytest.raises(SafetyLimitExceededError, match="exceeds safety limit"):
        plan_windows(start, end, max_total_hours=50, allow_open_candle=True)


def test_safety_limit_respected_when_within_limit() -> None:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=50)
    windows = plan_windows(start, end, max_total_hours=50, allow_open_candle=True)
    assert len(windows) == 1
    assert windows[0].expected_candles == 50


def test_leap_year_handling() -> None:
    # 2024 is a leap year: Feb 28 00:00 to Mar 1 00:00 is 48 hours (28th 24h + 29th 24h)
    start = datetime(2024, 2, 28, 0, 0, tzinfo=UTC)
    end = datetime(2024, 3, 1, 0, 0, tzinfo=UTC)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 1
    assert windows[0].expected_candles == 48
    assert (end - start).total_seconds() == 48 * 3600


def test_planned_window_to_dict() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 5, 0, tzinfo=UTC)
    w = PlannedWindow(
        start_utc=start,
        end_utc=end,
        expected_candles=5,
        granularity_seconds=3600,
        product_id="BTC-USD",
    )
    d = w.to_dict()
    assert d == {
        "start_utc": "2026-01-01T00:00:00+00:00",
        "end_utc": "2026-01-01T05:00:00+00:00",
        "expected_candles": 5,
        "granularity_seconds": 3600,
        "product_id": "BTC-USD",
    }
