"""Ingestion window planner re-exports for backward compatibility."""

from bitcoin_data_platform.window_planner import (
    BackfillPlan,
    InvalidIntervalError,
    InvalidTimezoneError,
    MisalignedBoundaryError,
    OpenCandleError,
    PlannedWindow,
    SafetyLimitExceededError,
    TimeRangeError,
    WindowPlanningError,
    plan_backfill,
    plan_windows,
)

__all__ = [
    "BackfillPlan",
    "InvalidIntervalError",
    "InvalidTimezoneError",
    "MisalignedBoundaryError",
    "OpenCandleError",
    "PlannedWindow",
    "SafetyLimitExceededError",
    "TimeRangeError",
    "WindowPlanningError",
    "plan_backfill",
    "plan_windows",
]
