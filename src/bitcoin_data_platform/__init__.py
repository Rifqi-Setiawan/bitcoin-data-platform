"""Bitcoin Data Engineering Platform."""

from bitcoin_data_platform.sources.coinbase_client import (
    CoinbaseClient,
    CoinbaseClientError,
    CoinbaseHTTPError,
    CoinbaseResponse,
    SourceUnavailableError,
)
from bitcoin_data_platform.sources.coinbase_contract import (
    CoinbaseCandle,
    ContractViolationError,
    ValidationResult,
    validate_candle,
    validate_candle_payload,
)
from bitcoin_data_platform.storage.raw_writer import (
    StorageError,
    compute_payload_sha256,
    create_raw_envelope,
    format_envelope_filename,
    read_raw_envelope,
    verify_raw_envelope,
    write_raw_envelope,
)
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
    "CoinbaseCandle",
    "CoinbaseClient",
    "CoinbaseClientError",
    "CoinbaseHTTPError",
    "CoinbaseResponse",
    "ContractViolationError",
    "InvalidIntervalError",
    "InvalidTimezoneError",
    "MalformedTimestampError",
    "MisalignedBoundaryError",
    "OpenCandleError",
    "PlannedWindow",
    "SafetyLimitExceededError",
    "SourceUnavailableError",
    "StorageError",
    "TimeRange",
    "TimeRangeError",
    "ValidationResult",
    "WindowPlanningError",
    "__version__",
    "compute_payload_sha256",
    "create_raw_envelope",
    "format_canonical_utc",
    "format_envelope_filename",
    "parse_iso_utc",
    "plan_backfill",
    "plan_windows",
    "read_raw_envelope",
    "validate_candle",
    "validate_candle_payload",
    "validate_half_open_interval",
    "validate_hourly_boundary",
    "verify_raw_envelope",
    "write_raw_envelope",
]
