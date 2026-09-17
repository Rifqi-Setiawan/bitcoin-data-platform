"""Tests for timestamp parsing, UTC validation, and half-open time range modeling."""

from datetime import UTC, datetime, timedelta

import pytest

from bitcoin_data_platform.time_range import (
    InvalidIntervalError,
    InvalidTimezoneError,
    MalformedTimestampError,
    MisalignedBoundaryError,
    OpenCandleError,
    TimeRange,
    TimeRangeError,
    format_canonical_utc,
    parse_iso_utc,
    validate_half_open_interval,
    validate_hourly_boundary,
)


def test_parse_iso_utc_with_z() -> None:
    dt = parse_iso_utc("2026-01-01T00:00:00Z", "--start")
    assert dt == datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    assert dt.tzinfo == UTC


def test_parse_iso_utc_with_plus_zero_offset() -> None:
    dt = parse_iso_utc("2026-01-01T00:00:00+00:00", "--start")
    assert dt == datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    assert dt.utcoffset() == timedelta(0)


def test_format_canonical_utc_normalizes_to_z() -> None:
    dt_z = parse_iso_utc("2026-01-01T00:00:00Z", "--start")
    dt_offset = parse_iso_utc("2026-01-01T00:00:00+00:00", "--start")
    assert format_canonical_utc(dt_z) == "2026-01-01T00:00:00Z"
    assert format_canonical_utc(dt_offset) == "2026-01-01T00:00:00Z"


def test_rejects_timezone_naive_input() -> None:
    with pytest.raises(InvalidTimezoneError, match="explicit UTC"):
        parse_iso_utc("2026-01-01T00:00:00", "--start")


def test_rejects_non_utc_offset_input() -> None:
    with pytest.raises(InvalidTimezoneError, match="explicit UTC"):
        parse_iso_utc("2026-01-01T00:00:00+07:00", "--start")

    with pytest.raises(InvalidTimezoneError, match="explicit UTC"):
        parse_iso_utc("2026-01-01T00:00:00-05:00", "--end")


def test_rejects_malformed_timestamp() -> None:
    with pytest.raises(MalformedTimestampError, match="Invalid ISO-8601"):
        parse_iso_utc("not-a-timestamp", "--start")

    with pytest.raises(MalformedTimestampError, match="Invalid ISO-8601"):
        parse_iso_utc("2026-13-45T00:00:00Z", "--start")


def test_rejects_minute_misaligned_boundary() -> None:
    dt = datetime(2026, 1, 1, 0, 15, 0, tzinfo=UTC)
    with pytest.raises(MisalignedBoundaryError, match="exact hour"):
        validate_hourly_boundary(dt, "--start")


def test_rejects_second_misaligned_boundary() -> None:
    dt = datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)
    with pytest.raises(MisalignedBoundaryError, match="exact hour"):
        validate_hourly_boundary(dt, "--end")


def test_rejects_microsecond_misaligned_boundary() -> None:
    dt = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    with pytest.raises(MisalignedBoundaryError, match="exact hour"):
        validate_hourly_boundary(dt, "--start")


def test_accepts_exact_hourly_boundary() -> None:
    dt = datetime(2026, 1, 1, 15, 0, 0, 0, tzinfo=UTC)
    # Should not raise
    validate_hourly_boundary(dt, "--start")


def test_rejects_equal_start_and_end() -> None:
    dt = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    with pytest.raises(InvalidIntervalError, match="strictly before"):
        validate_half_open_interval(dt, dt)


def test_rejects_reversed_interval() -> None:
    start = datetime(2026, 1, 2, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    with pytest.raises(InvalidIntervalError, match="strictly before"):
        validate_half_open_interval(start, end)


def test_rejects_end_after_current_hour_boundary() -> None:
    clock_now = datetime(2026, 9, 17, 14, 35, 12, tzinfo=UTC)
    start = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 17, 15, 0, 0, tzinfo=UTC)
    with pytest.raises(OpenCandleError, match="start of the current UTC hour"):
        validate_half_open_interval(start, end, now_utc=clock_now)


def test_accepts_end_equal_to_current_hour_boundary() -> None:
    clock_now = datetime(2026, 9, 17, 14, 35, 12, tzinfo=UTC)
    start = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 17, 14, 0, 0, tzinfo=UTC)
    # Should not raise
    validate_half_open_interval(start, end, now_utc=clock_now)


def test_time_range_dataclass() -> None:
    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 2, 0, 0, 0, tzinfo=UTC)
    tr = TimeRange(start_utc=start, end_utc=end)
    assert tr.total_hours == 24
    assert tr.start_iso == "2026-01-01T00:00:00Z"
    assert tr.end_iso == "2026-01-02T00:00:00Z"


def test_leap_year_feb_29_continuous_and_correctly_counted() -> None:
    # 2024 is a leap year: Feb 28 00:00Z to Mar 01 00:00Z is 48 hours
    start = datetime(2024, 2, 28, 0, 0, 0, tzinfo=UTC)
    end = datetime(2024, 3, 1, 0, 0, 0, tzinfo=UTC)
    validate_half_open_interval(start, end, allow_open_candle=True)
    tr = TimeRange(start_utc=start, end_utc=end)
    assert tr.total_hours == 48


def test_rejects_empty_or_whitespace_timestamp() -> None:
    with pytest.raises(MalformedTimestampError, match="non-empty string"):
        parse_iso_utc("", "--start")
    with pytest.raises(MalformedTimestampError, match="non-empty string"):
        parse_iso_utc("   ", "--end")


def test_rejects_naive_now_utc_in_validation() -> None:
    naive_now = datetime(2026, 9, 17, 14, 0, 0)
    start = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 17, 12, 0, 0, tzinfo=UTC)
    with pytest.raises(InvalidTimezoneError, match="explicit UTC"):
        validate_half_open_interval(start, end, now_utc=naive_now)


def test_time_range_error_hierarchy() -> None:
    assert issubclass(MalformedTimestampError, TimeRangeError)
    assert issubclass(InvalidTimezoneError, TimeRangeError)
    assert issubclass(MisalignedBoundaryError, TimeRangeError)
    assert issubclass(InvalidIntervalError, TimeRangeError)
    assert issubclass(OpenCandleError, TimeRangeError)


def test_dst_transition_unchanged_in_utc() -> None:
    # US spring forward transition date: 2024-03-10
    start = datetime(2024, 3, 10, 0, 0, 0, tzinfo=UTC)
    end = datetime(2024, 3, 11, 0, 0, 0, tzinfo=UTC)
    validate_half_open_interval(start, end, allow_open_candle=True)
    tr = TimeRange(start_utc=start, end_utc=end)
    assert tr.total_hours == 24
