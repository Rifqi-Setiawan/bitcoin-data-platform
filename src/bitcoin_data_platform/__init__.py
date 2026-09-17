"""Bitcoin Data Engineering Platform."""

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
from bitcoin_data_platform.window_planner import (
    BackfillPlan,
    PlannedWindow,
    SafetyLimitExceededError,
    WindowPlanningError,
    plan_backfill,
    plan_windows,
)

__version__ = "0.1.0"

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
    "__version__",
    "format_canonical_utc",
    "parse_iso_utc",
    "plan_backfill",
    "plan_windows",
    "validate_half_open_interval",
    "validate_hourly_boundary",
]
