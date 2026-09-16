"""Deterministic window planning for historical and incremental candle ingestion."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


class WindowPlanningError(Exception):
    """Base exception for all window planning failures."""


class InvalidTimezoneError(WindowPlanningError):
    """Raised when a datetime is naive or not explicit UTC."""


class MisalignedBoundaryError(WindowPlanningError):
    """Raised when a timestamp is not aligned to the required granularity boundary."""


class InvalidIntervalError(WindowPlanningError):
    """Raised when start timestamp is greater than or equal to end timestamp."""


class OpenCandleError(WindowPlanningError):
    """Raised when an interval includes current or future unclosed candles."""


class SafetyLimitExceededError(WindowPlanningError):
    """Raised when a requested window exceeds the configured safety limit."""


@dataclass(frozen=True)
class PlannedWindow:
    """A bounded, contiguous request window for candle acquisition."""

    start_utc: datetime
    end_utc: datetime
    expected_candles: int
    granularity_seconds: int = 3600
    product_id: str = "BTC-USD"

    @property
    def start_iso(self) -> str:
        """ISO 8601 representation of start timestamp."""
        return self.start_utc.isoformat()

    @property
    def end_iso(self) -> str:
        """ISO 8601 representation of end timestamp."""
        return self.end_utc.isoformat()

    def to_dict(self) -> dict[str, str | int]:
        """Convert planned window to a serializable dictionary."""
        return {
            "start_utc": self.start_iso,
            "end_utc": self.end_iso,
            "expected_candles": self.expected_candles,
            "granularity_seconds": self.granularity_seconds,
            "product_id": self.product_id,
        }


def _validate_utc(dt: datetime, name: str) -> None:
    """Ensure datetime is timezone-aware and set to UTC."""
    if dt.tzinfo is None:
        raise InvalidTimezoneError(f"{name} must be explicit UTC, got naive datetime")
    offset = dt.utcoffset()
    if offset != timedelta(0):
        raise InvalidTimezoneError(f"{name} must be explicit UTC, got offset {offset}")


def _validate_hourly_boundary(dt: datetime, name: str) -> None:
    """Ensure datetime is aligned to an hourly boundary."""
    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise MisalignedBoundaryError(
            f"{name} must be aligned to hourly boundary (minute=0, second=0, microsecond=0), "
            f"got {dt.isoformat()}"
        )


def plan_windows(
    start_utc: datetime,
    end_utc: datetime,
    *,
    granularity_seconds: int = 3600,
    max_candles_per_window: int = 300,
    now_utc: datetime | None = None,
    allow_open_candle: bool = False,
    max_total_hours: int | None = None,
    product_id: str = "BTC-USD",
) -> list[PlannedWindow]:
    """Plan consecutive, gapless request windows."""
    _validate_utc(start_utc, "start_utc")
    _validate_utc(end_utc, "end_utc")
    _validate_hourly_boundary(start_utc, "start_utc")
    _validate_hourly_boundary(end_utc, "end_utc")
    if start_utc >= end_utc:
        raise InvalidIntervalError(
            f"start_utc ({start_utc.isoformat()}) must be strictly before "
            f"end_utc ({end_utc.isoformat()})"
        )

    if max_total_hours is not None:
        total_duration = end_utc - start_utc
        if total_duration > timedelta(hours=max_total_hours):
            raise SafetyLimitExceededError(
                f"Requested interval of {int(total_duration.total_seconds() // 3600)} hours "
                f"exceeds safety limit of {max_total_hours} hours."
            )

    if not allow_open_candle:
        now = now_utc if now_utc is not None else datetime.now(UTC)
        _validate_utc(now, "now_utc")
        current_completed_hour = now.replace(minute=0, second=0, microsecond=0)
        if end_utc > current_completed_hour:
            raise OpenCandleError(
                f"Requested end_utc ({end_utc.isoformat()}) includes open or future candle. "
                f"Latest completed hourly candle boundary is {current_completed_hour.isoformat()}."
            )

    window_span = timedelta(seconds=max_candles_per_window * granularity_seconds)
    windows: list[PlannedWindow] = []
    current = start_utc
    while current < end_utc:
        window_end = min(current + window_span, end_utc)
        candle_count = int((window_end - current).total_seconds() // granularity_seconds)
        windows.append(
            PlannedWindow(
                start_utc=current,
                end_utc=window_end,
                expected_candles=candle_count,
                granularity_seconds=granularity_seconds,
                product_id=product_id,
            )
        )
        current = window_end

    return windows
