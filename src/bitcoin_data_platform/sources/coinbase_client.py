"""Coinbase Exchange HTTP client for BTC-USD hourly candle retrieval."""

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from bitcoin_data_platform.sources.coinbase_contract import (
    CoinbaseCandle,
    validate_candle_payload,
)

DEFAULT_USER_AGENT = (
    "bitcoin-data-platform/0.1.0 (+https://github.com/Rifqi-Setiawan/bitcoin-data-platform)"
)
DEFAULT_BASE_URL = "https://api.exchange.coinbase.com"
DEFAULT_PRODUCT_ID = "BTC-USD"
DEFAULT_GRANULARITY_SECONDS = 3600
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.5  # Max 2 req/s
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class CoinbaseClientError(Exception):
    """Base exception for Coinbase client errors."""


class SourceUnavailableError(CoinbaseClientError):
    """Raised when Coinbase Exchange is unavailable after exhausting retry attempts."""


class CoinbaseHTTPError(CoinbaseClientError):
    """Raised when Coinbase Exchange returns a non-retryable HTTP error."""

    def __init__(self, status_code: int, message: str, body: str = "") -> None:
        super().__init__(f"Coinbase HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message
        self.body = body


@dataclass(frozen=True)
class CoinbaseResponse:
    """Encapsulates a successful or validated HTTP response from Coinbase Exchange."""

    start_utc: datetime
    end_utc: datetime
    candles: list[CoinbaseCandle]
    raw_payload: list[Any]
    http_status: int
    retrieved_at_utc: datetime
    provider_request_id: str | None = None


class CoinbaseClient:
    """HTTP client for fetching historical candles from Coinbase Exchange REST API.

    Features:
    - Half-second minimum request spacing (2 req/s rate limit).
    - Exponential backoff with jitter and Retry-After header parsing.
    - Automatic retries on transient network errors and 408/429/5xx status codes.
    - Immediate failure on non-retryable 4xx client errors.
    - Dependency injection for transport, sleeper, and clock to enable deterministic testing.
    """

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        product_id: str = DEFAULT_PRODUCT_ID,
        granularity_seconds: int = DEFAULT_GRANULARITY_SECONDS,
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
        self.product_id = product_id
        self.granularity_seconds = granularity_seconds
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

    def __enter__(self) -> "CoinbaseClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _apply_rate_limit(self) -> None:
        """Enforce minimum spacing between outbound requests."""
        if self._last_request_started_at is not None:
            elapsed = self._clock() - self._last_request_started_at
            if elapsed < self.min_request_interval_seconds:
                sleep_duration = self.min_request_interval_seconds - elapsed
                self._sleeper(sleep_duration)
        self._last_request_started_at = self._clock()

    def _compute_backoff_delay(self, attempt: int, response: httpx.Response | None) -> float:
        """Calculate backoff duration, respecting Retry-After when present."""
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after.strip())
                    if delay >= 0:
                        return delay
                except ValueError:
                    pass

        # Exponential backoff: min(base * 2^attempt + jitter, max_backoff)
        backoff = self.base_backoff_seconds * (2**attempt)
        if self.jitter:
            backoff += random.uniform(0.0, 0.5)
        return float(min(backoff, self.max_backoff_seconds))

    def fetch_candles(
        self,
        start_utc: datetime,
        end_utc: datetime,
    ) -> CoinbaseResponse:
        """Fetch candles from Coinbase Exchange for the half-open window [start_utc, end_utc).

        Args:
            start_utc: Window start datetime (must be UTC).
            end_utc: Window end datetime (must be UTC).

        Returns:
            CoinbaseResponse with raw payload, parsed candles, status, and metadata.

        Raises:
            CoinbaseHTTPError: On non-retryable 4xx HTTP responses.
            SourceUnavailableError: If max_retries attempts are exhausted.
        """
        start_str = start_utc.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_utc.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = f"{self.base_url}/products/{self.product_id}/candles"
        params = {
            "start": start_str,
            "end": end_str,
            "granularity": str(self.granularity_seconds),
        }

        last_error_message = "Unknown error"

        for attempt in range(self.max_retries):
            if attempt == 0:
                self._apply_rate_limit()

            retrieved_at_utc = datetime.now(UTC)

            try:
                response = self._client.get(url, params=params)
            except (httpx.RequestError, ConnectionError, TimeoutError) as exc:
                last_error_message = f"Network failure ({type(exc).__name__}): {exc}"
                if attempt < self.max_retries - 1:
                    delay = max(
                        self._compute_backoff_delay(attempt, None),
                        self.min_request_interval_seconds,
                    )
                    self._sleeper(delay)
                    self._last_request_started_at = self._clock()
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
                        delay = max(
                            self._compute_backoff_delay(attempt, response),
                            self.min_request_interval_seconds,
                        )
                        self._sleeper(delay)
                        self._last_request_started_at = self._clock()
                        continue
                    break

                req_id = response.headers.get("cb-request-id") or response.headers.get(
                    "x-request-id"
                )
                validation = validate_candle_payload(raw_payload)

                return CoinbaseResponse(
                    start_utc=start_utc,
                    end_utc=end_utc,
                    candles=validation.valid_candles,
                    raw_payload=raw_payload if isinstance(raw_payload, list) else [],
                    http_status=status,
                    retrieved_at_utc=retrieved_at_utc,
                    provider_request_id=req_id,
                )

            if status in RETRYABLE_STATUS_CODES:
                reason = response.reason_phrase or ""
                body_snippet = response.text[:200]
                last_error_message = f"HTTP {status} {reason}: {body_snippet}"
                if attempt < self.max_retries - 1:
                    delay = max(
                        self._compute_backoff_delay(attempt, response),
                        self.min_request_interval_seconds,
                    )
                    self._sleeper(delay)
                    self._last_request_started_at = self._clock()
                    continue
                break

            self._last_request_started_at = self._clock()
            # Non-retryable 4xx
            raise CoinbaseHTTPError(
                status_code=status,
                message=response.reason_phrase or "Client error",
                body=response.text,
            )

        self._last_request_started_at = self._clock()
        raise SourceUnavailableError(
            f"Coinbase Exchange unavailable after {self.max_retries} attempts: {last_error_message}"
        )
