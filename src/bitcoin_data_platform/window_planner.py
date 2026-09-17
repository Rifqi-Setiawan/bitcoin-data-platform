"""Deterministic window planning for historical and incremental candle ingestion."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

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

WindowPlanningError = TimeRangeError

__all__ = [
    "BackfillPlan",
    "InvalidIntervalError",
    "InvalidTimezoneError",
    "MalformedTimestampError",
    "MisalignedBoundaryError",
    "OpenCandleError",
    "PlannedWindow",
    "SafetyLimitExceededError",
    "TimeRange",
    "TimeRangeError",
    "WindowPlanningError",
    "format_canonical_utc",
    "parse_iso_utc",
    "plan_backfill",
    "plan_windows",
    "validate_half_open_interval",
    "validate_hourly_boundary",
]


class SafetyLimitExceededError(TimeRangeError):
    """Raised when a requested window exceeds the configured safety limit."""


@dataclass(frozen=True)
class PlannedWindow:
    """A bounded, contiguous request window for candle acquisition."""

    index: int
    start_utc: datetime
    end_utc: datetime
    expected_candle_count: int
    granularity_seconds: int = 3600
    product_id: str = "BTC-USD"

    @property
    def start_iso(self) -> str:
        """ISO 8601 canonical representation of start timestamp."""
        return format_canonical_utc(self.start_utc)

    @property
    def end_iso(self) -> str:
        """ISO 8601 canonical representation of end timestamp."""
        return format_canonical_utc(self.end_utc)

    @property
    def expected_candles(self) -> int:
        """Alias for backward compatibility."""
        return self.expected_candle_count

    def to_dict(self) -> dict[str, Any]:
        """Convert planned window to a serializable dictionary."""
        return {
            "index": self.index,
            "start_utc": self.start_iso,
            "end_utc": self.end_iso,
            "expected_candle_count": self.expected_candle_count,
        }


@dataclass(frozen=True)
class BackfillPlan:
    """A complete, deterministic backfill plan."""

    requested_start_utc: datetime
    requested_end_utc: datetime
    expected_candle_count: int
    window_count: int
    windows: list[PlannedWindow]
    schema_version: int = 1
    source: str = "coinbase_exchange"
    product_id: str = "BTC-USD"
    granularity_seconds: int = 3600

    def to_dict(self) -> dict[str, Any]:
        """Convert backfill plan to dictionary with stable key ordering."""
        return {
            "schema_version": self.schema_version,
            "source": self.source,
            "product_id": self.product_id,
            "granularity_seconds": self.granularity_seconds,
            "requested_start_utc": format_canonical_utc(self.requested_start_utc),
            "requested_end_utc": format_canonical_utc(self.requested_end_utc),
            "expected_candle_count": self.expected_candle_count,
            "window_count": self.window_count,
            "windows": [w.to_dict() for w in self.windows],
        }

    def to_json(self) -> str:
        """Serialize plan to stable formatted JSON string with trailing newline."""
        return json.dumps(self.to_dict(), indent=2) + "\n"


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
    if start_utc.tzinfo is None or start_utc.utcoffset() != timedelta(0):
        raise InvalidTimezoneError(f"start_utc must be explicit UTC, got {start_utc}")
    if end_utc.tzinfo is None or end_utc.utcoffset() != timedelta(0):
        raise InvalidTimezoneError(f"end_utc must be explicit UTC, got {end_utc}")

    validate_hourly_boundary(start_utc, "start_utc")
    validate_hourly_boundary(end_utc, "end_utc")
    validate_half_open_interval(
        start_utc, end_utc, now_utc=now_utc, allow_open_candle=allow_open_candle
    )

    if max_total_hours is not None:
        total_duration = end_utc - start_utc
        if total_duration > timedelta(hours=max_total_hours):
            raise SafetyLimitExceededError(
                f"Requested interval of {int(total_duration.total_seconds() // 3600)} hours "
                f"exceeds safety limit of {max_total_hours} hours."
            )

    window_span = timedelta(seconds=max_candles_per_window * granularity_seconds)
    windows: list[PlannedWindow] = []
    current = start_utc
    idx = 0
    while current < end_utc:
        window_end = min(current + window_span, end_utc)
        candle_count = int((window_end - current).total_seconds() // granularity_seconds)
        windows.append(
            PlannedWindow(
                index=idx,
                start_utc=current,
                end_utc=window_end,
                expected_candle_count=candle_count,
                granularity_seconds=granularity_seconds,
                product_id=product_id,
            )
        )
        current = window_end
        idx += 1

    return windows


def plan_backfill(
    start_utc: datetime,
    end_utc: datetime,
    *,
    now_utc: datetime | None = None,
    allow_open_candle: bool = False,
    clock: Callable[[], datetime] | None = None,
) -> BackfillPlan:
    """Create a complete BackfillPlan for Coinbase BTC-USD hourly candles."""
    resolved_now = clock() if clock is not None else now_utc
    windows = plan_windows(
        start_utc,
        end_utc,
        granularity_seconds=3600,
        max_candles_per_window=300,
        now_utc=resolved_now,
        allow_open_candle=allow_open_candle,
        product_id="BTC-USD",
    )
    total_expected = sum(w.expected_candle_count for w in windows)
    return BackfillPlan(
        requested_start_utc=start_utc,
        requested_end_utc=end_utc,
        expected_candle_count=total_expected,
        window_count=len(windows),
        windows=windows,
    )
