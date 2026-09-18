"""Coin Metrics Community API v4 client for Bitcoin on-chain network metrics."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from bitcoin_data_platform.sources.coin_metrics_contract import (
    CoinMetricsRecord,
    validate_coin_metrics_payload,
)
from bitcoin_data_platform.sources.http_helpers import apply_rate_limit, compute_backoff_delay

DEFAULT_USER_AGENT = (
    "bitcoin-data-platform/0.1.0 (+https://github.com/Rifqi-Setiawan/bitcoin-data-platform)"
)
DEFAULT_BASE_URL = "https://community-api.coinmetrics.io/v4"
DEFAULT_ASSET = "btc"
DEFAULT_METRICS = "TxCnt,AdrActCnt,CapMVRVCur"
DEFAULT_FREQUENCY = "1d"
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.6  # 10 req / 6s = 1 req per 600ms
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class CoinMetricsClientError(Exception):
    """Base exception for Coin Metrics client errors."""


class SourceUnavailableError(CoinMetricsClientError):
    """Raised when Coin Metrics API is unavailable after exhausting retry attempts."""


class CoinMetricsHTTPError(CoinMetricsClientError):
    """Raised when Coin Metrics API returns a non-retryable HTTP error."""

    def __init__(self, status_code: int, message: str, body: str = "") -> None:
        super().__init__(f"Coin Metrics HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message
        self.body = body


@dataclass(frozen=True)
class CoinMetricsResponse:
    """Encapsulates a validated response from Coin Metrics API."""

    start_time: str
    end_time: str
    records: list[CoinMetricsRecord]
    raw_payload: dict[str, Any]
    http_status: int
    retrieved_at_utc: datetime
    provider_request_id: str | None = None


class CoinMetricsClient:
    """HTTP client for fetching on-chain network metrics from Coin Metrics Community API v4.

    Features:
    - 600ms minimum request spacing (10 req / 6s rate limit).
    - Exponential backoff with jitter and Retry-After header parsing.
    - Automatic retries on transient network errors and 408/429/5xx status codes.
    - Immediate failure on non-retryable 4xx client errors.
    - Injectable transport, sleeper, and clock for deterministic offline testing.
    - Automatic pagination handling via next_page_url.
    """

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        asset: str = DEFAULT_ASSET,
        metrics: str = DEFAULT_METRICS,
        frequency: str = DEFAULT_FREQUENCY,
        max_retries: int = 5,
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 30.0,
        min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
        timeout: float | httpx.Timeout | None = None,
        user_agent: str = DEFAULT_USER_AGENT,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        jitter: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.asset = asset
        self.metrics = metrics
        self.frequency = frequency
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.min_request_interval_seconds = min_request_interval_seconds
        self.user_agent = user_agent
        self._sleeper = sleeper
        self._clock = clock
        self.jitter = jitter

        effective_timeout = (
            timeout if timeout is not None else httpx.Timeout(60.0, connect=10.0, read=30.0)
        )

        self._last_request_started_at: float | None = None

        if http_client is not None:
            self._client = http_client
            self._owns_client = False
        else:
            self._client = httpx.Client(
                timeout=effective_timeout,
                transport=transport,
                headers={
                    "User-Agent": self.user_agent,
                    "Accept": "application/json",
                },
            )
            self._owns_client = True

    def close(self) -> None:
        """Close the underlying HTTP client if owned."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "CoinMetricsClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _apply_rate_limit(self) -> None:
        """Enforce minimum spacing between outbound requests."""
        self._last_request_started_at = apply_rate_limit(
            self._last_request_started_at,
            self.min_request_interval_seconds,
            self._clock,
            self._sleeper,
        )

    def _compute_backoff_delay(self, attempt: int, response: httpx.Response | None) -> float:
        """Calculate backoff duration, respecting Retry-After when present."""
        return compute_backoff_delay(
            attempt,
            response,
            self.base_backoff_seconds,
            self.max_backoff_seconds,
            self.jitter,
        )

    def _sleep_backoff(self, attempt: int, response: httpx.Response | None = None) -> None:
        delay = max(
            self._compute_backoff_delay(attempt, response),
            self.min_request_interval_seconds,
        )
        self._sleeper(delay)
        self._last_request_started_at = self._clock()

    def _execute_request(
        self, url: str, params: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], int, str | None]:
        """Execute a single HTTP GET request with retry handling."""
        last_error_message = "Unknown error"

        for attempt in range(self.max_retries):
            if attempt == 0:
                self._apply_rate_limit()

            try:
                response = self._client.get(url, params=params)
            except (httpx.RequestError, ConnectionError, TimeoutError) as exc:
                last_error_message = f"Network failure ({type(exc).__name__}): {exc}"
                if attempt < self.max_retries - 1:
                    self._sleep_backoff(attempt)
                    continue
                break

            status = response.status_code
            if status == 200:
                self._last_request_started_at = self._clock()
                try:
                    raw_payload = response.json()
                except Exception as exc:
                    last_error_message = f"Invalid JSON response: {exc}"
                    if attempt < self.max_retries - 1:
                        self._sleep_backoff(attempt, response)
                        continue
                    break

                req_id = response.headers.get("x-request-id") or response.headers.get(
                    "cm-request-id"
                )
                if not isinstance(raw_payload, dict):
                    raw_payload = {"data": raw_payload}
                return raw_payload, status, req_id

            if status in RETRYABLE_STATUS_CODES:
                reason = response.reason_phrase or ""
                body_snippet = response.text[:200]
                last_error_message = f"HTTP {status} {reason}: {body_snippet}"
                if attempt < self.max_retries - 1:
                    self._sleep_backoff(attempt, response)
                    continue
                break

            self._last_request_started_at = self._clock()
            raise CoinMetricsHTTPError(
                status_code=status,
                message=response.reason_phrase or "Client error",
                body=response.text,
            )

        self._last_request_started_at = self._clock()
        raise SourceUnavailableError(
            f"Coin Metrics API unavailable after {self.max_retries} attempts: {last_error_message}"
        )

    def fetch_asset_metrics(
        self,
        start_time: str | datetime,
        end_time: str | datetime,
        *,
        assets: str | None = None,
        metrics: str | None = None,
        frequency: str | None = None,
    ) -> CoinMetricsResponse:
        """Fetch daily Bitcoin on-chain metrics for the interval [start_time, end_time].

        Args:
            start_time: Start date/timestamp (ISO-8601 string or datetime).
            end_time: End date/timestamp (ISO-8601 string or datetime).
            assets: Asset symbol (defaults to instance asset, e.g. "btc").
            metrics: Comma-separated metrics (defaults to "TxCnt,AdrActCnt").
            frequency: Frequency string (defaults to "1d").

        Returns:
            CoinMetricsResponse with validated records and raw payload.

        Raises:
            CoinMetricsHTTPError: On non-retryable 4xx HTTP responses.
            SourceUnavailableError: If max_retries attempts are exhausted.
        """
        start_str = (
            start_time.astimezone(UTC).strftime("%Y-%m-%d")
            if isinstance(start_time, datetime)
            else str(start_time).strip()
        )
        end_str = (
            end_time.astimezone(UTC).strftime("%Y-%m-%d")
            if isinstance(end_time, datetime)
            else str(end_time).strip()
        )

        url = f"{self.base_url}/timeseries/asset-metrics"
        params: dict[str, Any] | None = {
            "assets": assets or self.asset,
            "metrics": metrics or self.metrics,
            "frequency": frequency or self.frequency,
            "start_time": start_str,
            "end_time": end_str,
        }

        retrieved_at_utc = datetime.now(UTC)
        all_data: list[Any] = []
        last_req_id: str | None = None
        last_status = 200
        page_count = 0

        current_url: str | None = url
        current_params = params

        while current_url:
            page_payload, last_status, last_req_id = self._execute_request(
                current_url, current_params
            )
            page_count += 1
            data = page_payload.get("data", [])
            if isinstance(data, list):
                all_data.extend(data)

            next_page_url = page_payload.get("next_page_url")
            if next_page_url:
                current_url = str(next_page_url)
                current_params = None  # URL already includes query params
            else:
                current_url = None

        combined_payload: dict[str, Any] = {"data": all_data}
        if page_count > 1:
            combined_payload["pages_fetched"] = page_count

        validation = validate_coin_metrics_payload(combined_payload)

        return CoinMetricsResponse(
            start_time=start_str,
            end_time=end_str,
            records=validation.valid_records,
            raw_payload=combined_payload,
            http_status=last_status,
            retrieved_at_utc=retrieved_at_utc,
            provider_request_id=last_req_id,
        )
