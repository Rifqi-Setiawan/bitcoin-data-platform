"""External source clients and adapters."""

from bitcoin_data_platform.sources.coin_metrics_client import (
    CoinMetricsClient,
    CoinMetricsClientError,
    CoinMetricsHTTPError,
    CoinMetricsResponse,
)
from bitcoin_data_platform.sources.coin_metrics_contract import (
    CoinMetricsContractViolationError,
    CoinMetricsRecord,
    CoinMetricsValidationResult,
    validate_coin_metrics_payload,
    validate_record,
)
from bitcoin_data_platform.sources.coinbase_client import (
    DEFAULT_BASE_URL,
    DEFAULT_GRANULARITY_SECONDS,
    DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
    DEFAULT_PRODUCT_ID,
    DEFAULT_USER_AGENT,
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

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_GRANULARITY_SECONDS",
    "DEFAULT_MIN_REQUEST_INTERVAL_SECONDS",
    "DEFAULT_PRODUCT_ID",
    "DEFAULT_USER_AGENT",
    "CoinMetricsClient",
    "CoinMetricsClientError",
    "CoinMetricsContractViolationError",
    "CoinMetricsHTTPError",
    "CoinMetricsRecord",
    "CoinMetricsResponse",
    "CoinMetricsValidationResult",
    "CoinbaseCandle",
    "CoinbaseClient",
    "CoinbaseClientError",
    "CoinbaseHTTPError",
    "CoinbaseResponse",
    "ContractViolationError",
    "SourceUnavailableError",
    "ValidationResult",
    "validate_candle",
    "validate_candle_payload",
    "validate_coin_metrics_payload",
    "validate_record",
]
