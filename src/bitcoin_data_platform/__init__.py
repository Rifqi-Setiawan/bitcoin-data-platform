"""Bitcoin Data Engineering Platform."""

from bitcoin_data_platform.alerts.telegram_dispatcher import (
    TelegramDispatcher,
    format_news_alert_message,
    format_signal_message,
)
from bitcoin_data_platform.quality.checks import (
    QualityCheckError,
    check_candle,
    filter_valid_candles,
    validate_candle_quality,
)
from bitcoin_data_platform.signals.generator import (
    InvestmentSignal,
    SignalGenerator,
    compute_signal_strength,
    generate_narrative,
)
from bitcoin_data_platform.signals.news_sentinel import (
    NewsAlert,
    NewsSentinel,
    NewsSentinelError,
    compute_alert_id,
)
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
from bitcoin_data_platform.storage.duckdb_manager import (
    DuckDBManager,
    DuckDBManagerError,
)
from bitcoin_data_platform.storage.parquet_writer import (
    PARQUET_SCHEMA,
    ParquetStorageError,
    PartitionResult,
    candles_to_table,
    read_partition_candles,
    write_parquet_partitions,
)
from bitcoin_data_platform.storage.raw_writer import (
    ENDPOINT_NAME,
    SCHEMA_VERSION,
    SOURCE_NAME,
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
from bitcoin_data_platform.transforms.normalizer import (
    NormalizedCandle,
    normalize_candle,
    normalize_envelopes,
)
from bitcoin_data_platform.transforms.raw_reader import (
    RawEnvelope,
    RawReaderError,
    parse_raw_envelope_dict,
    read_raw_envelopes,
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
    "DuckDBManager",
    "DuckDBManagerError",
    "ENDPOINT_NAME",
    "InvalidIntervalError",
    "InvalidTimezoneError",
    "InvestmentSignal",
    "MalformedTimestampError",
    "MisalignedBoundaryError",
    "NewsAlert",
    "NewsSentinel",
    "NewsSentinelError",
    "NormalizedCandle",
    "OpenCandleError",
    "PARQUET_SCHEMA",
    "ParquetStorageError",
    "PartitionResult",
    "PlannedWindow",
    "QualityCheckError",
    "RawEnvelope",
    "RawReaderError",
    "SCHEMA_VERSION",
    "SOURCE_NAME",
    "SafetyLimitExceededError",
    "SignalGenerator",
    "SourceUnavailableError",
    "StorageError",
    "TelegramDispatcher",
    "TimeRange",
    "TimeRangeError",
    "ValidationResult",
    "WindowPlanningError",
    "__version__",
    "candles_to_table",
    "check_candle",
    "compute_alert_id",
    "compute_payload_sha256",
    "compute_signal_strength",
    "create_raw_envelope",
    "filter_valid_candles",
    "format_canonical_utc",
    "format_envelope_filename",
    "format_news_alert_message",
    "format_signal_message",
    "generate_narrative",
    "normalize_candle",
    "normalize_envelopes",
    "parse_iso_utc",
    "parse_raw_envelope_dict",
    "plan_backfill",
    "plan_windows",
    "read_partition_candles",
    "read_raw_envelope",
    "read_raw_envelopes",
    "validate_candle",
    "validate_candle_payload",
    "validate_candle_quality",
    "validate_half_open_interval",
    "validate_hourly_boundary",
    "verify_raw_envelope",
    "write_parquet_partitions",
    "write_raw_envelope",
]
