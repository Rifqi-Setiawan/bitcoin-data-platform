"""Timestamp parsing, UTC validation, and half-open time interval modeling."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


class TimeRangeError(ValueError):
    """Base exception for all time range and timestamp validation errors."""


class MalformedTimestampError(TimeRangeError):
    """Raised when a timestamp string cannot be parsed as an ISO-8601 datetime."""


class InvalidTimezoneError(TimeRangeError):
    """Raised when a timestamp is timezone-naive or has a non-UTC offset."""


class MisalignedBoundaryError(TimeRangeError):
    """Raised when a timestamp is not aligned to an exact hour."""


class InvalidIntervalError(TimeRangeError):
    """Raised when start timestamp is greater than or equal to end timestamp."""


class OpenCandleError(TimeRangeError):
    """Raised when an interval end exceeds the current completed hourly candle boundary."""


def parse_iso_utc(val: str, name: str = "timestamp") -> datetime:
    """Parse an ISO-8601 string ensuring explicit UTC timezone (Z or +00:00)."""
    if not isinstance(val, str) or not val.strip():
        raise MalformedTimestampError(f"{name} must be a non-empty string")

    raw = val.strip()
    try:
        dt = datetime.fromisoformat(raw)
    except Exception as exc:
        raise MalformedTimestampError(f"{name}: Invalid ISO-8601 timestamp: {raw}") from exc

    if dt.tzinfo is None:
        raise InvalidTimezoneError(
            f"{name} must be explicit UTC (ending in 'Z' or '+00:00'), got naive timestamp: {raw}"
        )

    offset = dt.utcoffset()
    if offset != timedelta(0):
        raise InvalidTimezoneError(
            f"{name} must be explicit UTC (offset 0), got non-UTC offset {offset}: {raw}"
        )

    if not (raw.endswith("Z") or raw.endswith("+00:00")):
        raise InvalidTimezoneError(
            f"{name} must be explicit UTC expressed exactly with 'Z' or '+00:00', got: {raw}"
        )

    return dt.astimezone(UTC)


def validate_hourly_boundary(dt: datetime, name: str = "timestamp") -> None:
    """Ensure datetime is aligned to an exact hourly boundary (00:00.000000)."""
    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise MisalignedBoundaryError(
            f"{name} must be aligned to an exact hour (minute=0, second=0, microsecond=0), "
            f"got {dt.isoformat()}"
        )


def validate_half_open_interval(
    start_utc: datetime,
    end_utc: datetime,
    *,
    now_utc: datetime | None = None,
) -> None:
    """Validate interval [start, end) ordering and ensure no open candle inclusion."""
    if start_utc.tzinfo is None or start_utc.utcoffset() != timedelta(0):
        raise InvalidTimezoneError(f"start_utc must be explicit UTC, got {start_utc}")
    if end_utc.tzinfo is None or end_utc.utcoffset() != timedelta(0):
        raise InvalidTimezoneError(f"end_utc must be explicit UTC, got {end_utc}")

    if start_utc >= end_utc:
        raise InvalidIntervalError(
            f"start ({format_canonical_utc(start_utc)}) must be strictly before "
            f"end ({format_canonical_utc(end_utc)})"
        )

    now = now_utc if now_utc is not None else datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise InvalidTimezoneError(f"now_utc must be explicit UTC, got {now}")
    current_hour_boundary = now.replace(minute=0, second=0, microsecond=0)
    if end_utc > current_hour_boundary:
        start_hour_str = format_canonical_utc(current_hour_boundary)
        raise OpenCandleError(
            f"Requested end ({format_canonical_utc(end_utc)}) is later than "
            f"the start of the current UTC hour ({start_hour_str}). "
            "Open or future candles cannot be planned."
        )


def format_canonical_utc(dt: datetime) -> str:
    """Format a UTC datetime into canonical YYYY-MM-DDTHH:MM:SSZ string."""
    utc_dt = dt.astimezone(UTC)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class TimeRange:
    """A validated half-open UTC time range [start_utc, end_utc)."""

    start_utc: datetime
    end_utc: datetime

    @property
    def start_iso(self) -> str:
        return format_canonical_utc(self.start_utc)

    @property
    def end_iso(self) -> str:
        return format_canonical_utc(self.end_utc)

    @property
    def total_hours(self) -> int:
        return int((self.end_utc - self.start_utc).total_seconds() // 3600)
