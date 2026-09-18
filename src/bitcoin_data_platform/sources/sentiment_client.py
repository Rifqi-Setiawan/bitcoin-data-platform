"""Alternative.me Crypto Fear & Greed Index API client."""

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from bitcoin_data_platform.sources.http_helpers import apply_rate_limit, compute_backoff_delay
from bitcoin_data_platform.sources.sentiment_contract import (
    SentimentContractViolationError,
    SentimentRecord,
    validate_sentiment_payload,
)

DEFAULT_BASE_URL = "https://api.alternative.me/fng"
DEFAULT_USER_AGENT = "bitcoin-data-platform/0.1.0"
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 1.0
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class SentimentClientError(Exception):
    """Base exception for sentiment client errors."""


class SourceUnavailableError(SentimentClientError):
    """Raised when sentiment API is unavailable after exhausting retry attempts."""


class SentimentHTTPError(SentimentClientError):
    """Raised when sentiment API returns a non-retryable HTTP error."""

    def __init__(self, status_code: int, message: str, body: str = "") -> None:
        super().__init__(f"Sentiment HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message
        self.body = body


class SentimentClient:
    """HTTP client for fetching Fear & Greed sentiment index from Alternative.me."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        max_retries: int = 3,
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 10.0,
        min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
        timeout: float | httpx.Timeout | None = None,
        user_agent: str = DEFAULT_USER_AGENT,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        time_provider: Callable[[], datetime] | None = None,
        jitter: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.min_request_interval_seconds = min_request_interval_seconds
        self.user_agent = user_agent
        self._sleeper = sleeper
        self._clock = clock
        self._time_provider = time_provider or (lambda: datetime.now(UTC))
        self.jitter = jitter

        effective_timeout = (
            timeout if timeout is not None else httpx.Timeout(30.0, connect=10.0, read=20.0)
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

    def __enter__(self) -> "SentimentClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _execute_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute HTTP GET with rate limiting and retry handling."""
        attempt = 0
        last_error: Exception | None = None

        while attempt <= self.max_retries:
            self._last_request_started_at = apply_rate_limit(
                last_request_started_at=self._last_request_started_at,
                min_interval=self.min_request_interval_seconds,
                clock=self._clock,
                sleeper=self._sleeper,
            )

            try:
                url = f"{self.base_url}/"
                response = self._client.get(url, params=params)

                if response.status_code in RETRYABLE_STATUS_CODES:
                    attempt += 1
                    if attempt > self.max_retries:
                        raise SourceUnavailableError(
                            f"Sentiment API unavailable after {self.max_retries} retries "
                            f"(status {response.status_code}): {response.text[:200]}"
                        )
                    delay = compute_backoff_delay(
                        attempt=attempt,
                        response=response,
                        base_backoff=self.base_backoff_seconds,
                        max_backoff=self.max_backoff_seconds,
                        jitter=self.jitter,
                    )
                    self._sleeper(delay)
                    continue

                if response.is_error:
                    raise SentimentHTTPError(
                        status_code=response.status_code,
                        message=response.reason_phrase or "HTTP Error",
                        body=response.text[:500],
                    )

                return response.json()  # type: ignore[no-any-return]

            except (httpx.TransportError, httpx.RequestError) as exc:
                last_error = exc
                attempt += 1
                if attempt > self.max_retries:
                    raise SourceUnavailableError(
                        f"Sentiment API request failed after {self.max_retries} retries: {exc}"
                    ) from exc

                delay = compute_backoff_delay(
                    attempt=attempt,
                    response=None,
                    base_backoff=self.base_backoff_seconds,
                    max_backoff=self.max_backoff_seconds,
                    jitter=self.jitter,
                )
                self._sleeper(delay)

        raise SourceUnavailableError(
            f"Sentiment API unavailable after {self.max_retries} retries"
        ) from last_error

    def fetch_history(self, limit: int = 30) -> list[SentimentRecord]:
        """Fetch historical Fear & Greed records up to limit (1..30)."""
        if limit < 1 or limit > 30:
            raise ValueError(f"Limit must be between 1 and 30, got {limit}")

        raw_payload = self._execute_request(params={"limit": limit, "format": "json"})
        now_utc = self._time_provider()
        validation = validate_sentiment_payload(raw_payload, ingested_at_utc=now_utc)
        if not validation.is_valid:
            raise SentimentContractViolationError("; ".join(validation.violations))

        return validation.valid_records

    def fetch_current(self) -> SentimentRecord:
        """Fetch the most recent Fear & Greed sentiment record."""
        records = self.fetch_history(limit=1)
        if not records:
            raise SourceUnavailableError("Sentiment API returned empty record set")
        return records[0]
