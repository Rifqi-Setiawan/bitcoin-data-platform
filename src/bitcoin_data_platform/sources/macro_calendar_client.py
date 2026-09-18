"""ForexFactory macroeconomic calendar feed client."""

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from bitcoin_data_platform.sources.http_helpers import apply_rate_limit, compute_backoff_delay
from bitcoin_data_platform.sources.macro_calendar_contract import (
    MacroContractViolationError,
    MacroEvent,
    validate_macro_payload,
)

DEFAULT_FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
DEFAULT_USER_AGENT = "bitcoin-data-platform/0.1.0"
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 1.0
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class MacroCalendarClientError(Exception):
    """Base exception for Macro Calendar client errors."""


class SourceUnavailableError(MacroCalendarClientError):
    """Raised when Macro Calendar feed is unavailable after exhausting retries."""


class MacroHTTPError(MacroCalendarClientError):
    """Raised when Macro Calendar feed returns a non-retryable HTTP error."""

    def __init__(self, status_code: int, message: str, body: str = "") -> None:
        super().__init__(f"Macro Calendar HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message
        self.body = body


class MacroCalendarClient:
    """HTTP client for fetching high-impact macroeconomic events from ForexFactory feed."""

    def __init__(
        self,
        *,
        feed_url: str = DEFAULT_FEED_URL,
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
        self.feed_url = feed_url
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

    def __enter__(self) -> "MacroCalendarClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _execute_request(self) -> Any:
        """Execute HTTP GET on feed URL with rate limiting and retry handling."""
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
                response = self._client.get(self.feed_url)

                if response.status_code in RETRYABLE_STATUS_CODES:
                    attempt += 1
                    if attempt > self.max_retries:
                        raise SourceUnavailableError(
                            f"Macro calendar feed unavailable after {self.max_retries} retries "
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
                    raise MacroHTTPError(
                        status_code=response.status_code,
                        message=response.reason_phrase or "HTTP Error",
                        body=response.text[:500],
                    )

                return response.json()

            except (httpx.TransportError, httpx.RequestError) as exc:
                last_error = exc
                attempt += 1
                if attempt > self.max_retries:
                    raise SourceUnavailableError(
                        f"Macro calendar feed failed after {self.max_retries} retries: {exc}"
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
            f"Macro calendar feed unavailable after {self.max_retries} retries"
        ) from last_error

    def fetch_week_events(
        self,
        *,
        country_filter: str | None = "USD",
        impact_filter: str | None = "High",
    ) -> list[MacroEvent]:
        """Fetch this week's scheduled macroeconomic events with filtering and dedup."""
        raw_payload = self._execute_request()
        now_utc = self._time_provider()

        validation = validate_macro_payload(raw_payload, ingested_at_utc=now_utc)
        if not validation.is_valid:
            raise MacroContractViolationError("; ".join(validation.violations))

        deduped: dict[str, MacroEvent] = {}
        for event in validation.valid_events:
            if country_filter and event.country.upper() != country_filter.upper():
                continue
            if impact_filter and event.impact.capitalize() != impact_filter.capitalize():
                continue
            if event.event_id not in deduped:
                deduped[event.event_id] = event

        return sorted(deduped.values(), key=lambda e: (e.scheduled_utc, e.title))
