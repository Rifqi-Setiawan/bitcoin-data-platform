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


SCHEMA_VERSION: int = 1
SOURCE: str = "coinbase_exchange"
PRODUCT_ID: str = "BTC-USD"
GRANULARITY_SECONDS: int = 3600
MAX_CANDLES_PER_WINDOW: int = 300


class SafetyLimitExceededError(TimeRangeError):
    """Deprecated: safety limits were removed to enforce fixed planner invariants."""


@dataclass(frozen=True)
class PlannedWindow:
    """A bounded, contiguous request window for candle acquisition."""

    index: int
    start_utc: datetime
    end_utc: datetime
    expected_candle_count: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError(f"index must be non-negative, got {self.index}")
        if self.start_utc.tzinfo is None or self.start_utc.utcoffset() != timedelta(0):
            raise InvalidTimezoneError(f"start_utc must be explicit UTC, got {self.start_utc}")
        if self.end_utc.tzinfo is None or self.end_utc.utcoffset() != timedelta(0):
            raise InvalidTimezoneError(f"end_utc must be explicit UTC, got {self.end_utc}")
        validate_hourly_boundary(self.start_utc, "start_utc")
        validate_hourly_boundary(self.end_utc, "end_utc")
        if self.start_utc >= self.end_utc:
            raise InvalidIntervalError(
                f"start_utc ({format_canonical_utc(self.start_utc)}) must be strictly before "
                f"end_utc ({format_canonical_utc(self.end_utc)})"
            )
        duration_seconds = int((self.end_utc - self.start_utc).total_seconds())
        if duration_seconds % GRANULARITY_SECONDS != 0:
            raise MisalignedBoundaryError("Window duration must be an integer number of hours")
        hours = duration_seconds // GRANULARITY_SECONDS
        if not (1 <= self.expected_candle_count <= MAX_CANDLES_PER_WINDOW):
            raise ValueError(
                f"expected_candle_count must be between 1 and {MAX_CANDLES_PER_WINDOW}, "
                f"got {self.expected_candle_count}"
            )
        if self.expected_candle_count != hours:
            raise ValueError(
                f"expected_candle_count ({self.expected_candle_count}) does not match "
                f"window duration ({hours} hours)"
            )

    @property
    def granularity_seconds(self) -> int:
        """Fixed granularity in seconds (3600 for Phase 1A)."""
        return GRANULARITY_SECONDS

    @property
    def product_id(self) -> str:
        """Fixed product identifier ('BTC-USD' for Phase 1A)."""
        return PRODUCT_ID

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

    def __post_init__(self) -> None:
        if (
            self.requested_start_utc.tzinfo is None
            or self.requested_start_utc.utcoffset() != timedelta(0)
        ):
            raise InvalidTimezoneError(
                f"requested_start_utc must be explicit UTC, got {self.requested_start_utc}"
            )
        if self.requested_end_utc.tzinfo is None or self.requested_end_utc.utcoffset() != timedelta(
            0
        ):
            raise InvalidTimezoneError(
                f"requested_end_utc must be explicit UTC, got {self.requested_end_utc}"
            )
        validate_hourly_boundary(self.requested_start_utc, "requested_start_utc")
        validate_hourly_boundary(self.requested_end_utc, "requested_end_utc")
        if self.requested_start_utc >= self.requested_end_utc:
            start_str = format_canonical_utc(self.requested_start_utc)
            end_str = format_canonical_utc(self.requested_end_utc)
            raise InvalidIntervalError(
                f"requested_start_utc ({start_str}) must be strictly before "
                f"requested_end_utc ({end_str})"
            )
        if self.window_count != len(self.windows):
            raise ValueError(
                f"window_count ({self.window_count}) does not match "
                f"len(windows) ({len(self.windows)})"
            )
        actual_total = sum(w.expected_candle_count for w in self.windows)
        if self.expected_candle_count != actual_total:
            raise ValueError(
                f"expected_candle_count ({self.expected_candle_count}) does not match "
                f"sum of window counts ({actual_total})"
            )

    @property
    def schema_version(self) -> int:
        """Fixed schema version (1 for Phase 1A)."""
        return SCHEMA_VERSION

    @property
    def source(self) -> str:
        """Fixed source ('coinbase_exchange' for Phase 1A)."""
        return SOURCE

    @property
    def product_id(self) -> str:
        """Fixed product identifier ('BTC-USD' for Phase 1A)."""
        return PRODUCT_ID

    @property
    def granularity_seconds(self) -> int:
        """Fixed granularity in seconds (3600 for Phase 1A)."""
        return GRANULARITY_SECONDS

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
    now_utc: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> list[PlannedWindow]:
    """Plan consecutive, gapless request windows."""
    resolved_now = clock() if clock is not None else now_utc

    if start_utc.tzinfo is None or start_utc.utcoffset() != timedelta(0):
        raise InvalidTimezoneError(f"start_utc must be explicit UTC, got {start_utc}")
    if end_utc.tzinfo is None or end_utc.utcoffset() != timedelta(0):
        raise InvalidTimezoneError(f"end_utc must be explicit UTC, got {end_utc}")

    validate_hourly_boundary(start_utc, "start_utc")
    validate_hourly_boundary(end_utc, "end_utc")
    validate_half_open_interval(start_utc, end_utc, now_utc=resolved_now)

    window_span = timedelta(seconds=MAX_CANDLES_PER_WINDOW * GRANULARITY_SECONDS)
    windows: list[PlannedWindow] = []
    current = start_utc
    idx = 0
    while current < end_utc:
        window_end = min(current + window_span, end_utc)
        if window_end <= current:
            raise WindowPlanningError(
                f"Invariant violation: window_end ({window_end}) <= current ({current})"
            )
        candle_count = int((window_end - current).total_seconds() // GRANULARITY_SECONDS)
        if not (1 <= candle_count <= MAX_CANDLES_PER_WINDOW):
            msg = (
                f"Invariant violation: candle_count ({candle_count}) "
                f"not in 1..{MAX_CANDLES_PER_WINDOW}"
            )
            raise WindowPlanningError(msg)
        windows.append(
            PlannedWindow(
                index=idx,
                start_utc=current,
                end_utc=window_end,
                expected_candle_count=candle_count,
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
    clock: Callable[[], datetime] | None = None,
) -> BackfillPlan:
    """Create a complete BackfillPlan for Coinbase BTC-USD hourly candles."""
    resolved_now = clock() if clock is not None else now_utc
    windows = plan_windows(
        start_utc,
        end_utc,
        now_utc=resolved_now,
    )
    total_expected = sum(w.expected_candle_count for w in windows)
    return BackfillPlan(
        requested_start_utc=start_utc,
        requested_end_utc=end_utc,
        expected_candle_count=total_expected,
        window_count=len(windows),
        windows=windows,
    )
