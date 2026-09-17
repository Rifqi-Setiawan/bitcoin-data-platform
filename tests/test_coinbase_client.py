"""Tests for Coinbase HTTP client and retry/rate-limiting logic."""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from bitcoin_data_platform.sources.coinbase_client import (
    DEFAULT_USER_AGENT,
    CoinbaseClient,
    CoinbaseHTTPError,
    SourceUnavailableError,
)


def _make_sample_payload() -> list[list[Any]]:
    return [
        [1767225600, 95000, 96000, 95200, 96000, 10],
        [1767229200, 96000, 97000, 96000, 96800, 15],
    ]


def test_fetch_success() -> None:
    """1. Successful fetch returns valid CoinbaseResponse with candles and metadata."""
    payload = _make_sample_payload()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=payload,
            headers={"cb-request-id": "req-12345"},
        )

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(transport=transport)

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 2, 0, tzinfo=UTC)

    response = client.fetch_candles(start, end)
    assert response.http_status == 200
    assert response.provider_request_id == "req-12345"
    assert response.raw_payload == payload
    assert len(response.candles) == 2
    assert response.start_utc == start
    assert response.end_utc == end
    assert response.retrieved_at_utc.tzinfo == UTC


def test_rate_limit_spacing() -> None:
    """2. Rate limiter enforces minimum 500ms spacing between outbound requests."""
    fake_now = [1000.0]
    sleep_calls: list[float] = []

    def mock_clock() -> float:
        return fake_now[0]

    def mock_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        fake_now[0] += seconds

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        min_request_interval_seconds=0.5,
        clock=mock_clock,
        sleeper=mock_sleep,
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)

    # First request: no prior request, zero sleep
    client.fetch_candles(start, end)
    assert sleep_calls == []

    # Second request immediately after (only 0.1s simulated elapsed)
    fake_now[0] += 0.1
    client.fetch_candles(start, end)

    # Must sleep for remaining 0.4s to reach 0.5s interval
    assert len(sleep_calls) == 1
    assert pytest.approx(sleep_calls[0], rel=1e-3) == 0.4


def test_retry_429() -> None:
    """3. HTTP 429 triggers retry and succeeds on subsequent request."""
    call_count = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(429, text="Rate limit exceeded")
        return httpx.Response(200, json=_make_sample_payload())

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        base_backoff_seconds=0.1,
        jitter=False,
        sleeper=lambda s: sleeps.append(s),
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 2, 0, tzinfo=UTC)
    response = client.fetch_candles(start, end)

    assert response.http_status == 200
    assert call_count == 3
    assert len(sleeps) == 2


def test_retry_5xx() -> None:
    """4. Server errors (500, 502, 503, 504) trigger retries."""
    status_sequence = [500, 502, 503, 504, 200]
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        status = status_sequence[call_count]
        call_count += 1
        if status == 200:
            return httpx.Response(200, json=[])
        return httpx.Response(status, text=f"Error {status}")

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        max_retries=6,
        base_backoff_seconds=0.01,
        jitter=False,
        sleeper=lambda s: None,
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    response = client.fetch_candles(start, end)

    assert response.http_status == 200
    assert call_count == 5


def test_retry_timeout_and_connection_error() -> None:
    """5. Network exceptions (connect error, read timeout) trigger retry."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise httpx.ConnectError("Connection refused", request=request)
        if call_count == 2:
            raise httpx.ReadTimeout("Read timed out", request=request)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        base_backoff_seconds=0.01,
        jitter=False,
        sleeper=lambda s: None,
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    response = client.fetch_candles(start, end)

    assert response.http_status == 200
    assert call_count == 3


def test_no_retry_4xx() -> None:
    """6. Non-retryable 4xx errors (400, 401, 403, 404) raise CoinbaseHTTPError immediately."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(400, text="Bad Request: invalid granularity")

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        max_retries=5,
        sleeper=lambda s: None,
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)

    with pytest.raises(CoinbaseHTTPError) as exc_info:
        client.fetch_candles(start, end)

    assert exc_info.value.status_code == 400
    assert "Bad Request" in exc_info.value.body
    # Must NOT retry: call_count should be exactly 1
    assert call_count == 1


def test_retry_after_header() -> None:
    """7. Retry-After header duration is respected on 429 response."""
    call_count = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(429, headers={"Retry-After": "4.5"}, text="Slow down")
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        sleeper=lambda s: sleeps.append(s),
        jitter=False,
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    response = client.fetch_candles(start, end)

    assert response.http_status == 200
    assert len(sleeps) == 1
    assert pytest.approx(sleeps[0], rel=1e-3) == 4.5


def test_max_retries_raises_source_unavailable() -> None:
    """8. Exhausting all retries raises SourceUnavailableError."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(503, text="Service Unavailable")

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        max_retries=5,
        base_backoff_seconds=0.01,
        jitter=False,
        sleeper=lambda s: None,
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)

    with pytest.raises(SourceUnavailableError) as exc_info:
        client.fetch_candles(start, end)

    assert "Coinbase Exchange unavailable after 5 attempts" in str(exc_info.value)
    assert call_count == 5


def test_user_agent_header() -> None:
    """9. Outbound request includes required User-Agent header."""
    captured_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_headers
        captured_headers = dict(request.headers)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(transport=transport)

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    client.fetch_candles(start, end)

    assert "user-agent" in captured_headers
    assert captured_headers["user-agent"] == DEFAULT_USER_AGENT


def test_query_params() -> None:
    """10. Endpoint path and query parameters (start, end, granularity) match expectations."""
    captured_url: str = ""
    captured_params: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_url, captured_params
        captured_url = str(request.url)
        captured_params = dict(request.url.params)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        product_id="BTC-USD",
        granularity_seconds=3600,
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
    client.fetch_candles(start, end)

    assert "/products/BTC-USD/candles" in captured_url
    assert captured_params["start"] == "2026-01-01T00:00:00Z"
    assert captured_params["end"] == "2026-01-02T00:00:00Z"
    assert captured_params["granularity"] == "3600"


@pytest.mark.integration
def test_real_coinbase_fetch() -> None:
    """Integration: actual live fetch from public Coinbase endpoint (skipped by default)."""
    client = CoinbaseClient(product_id="BTC-USD", granularity_seconds=3600)
    # 2 hours well in the past
    start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 2, 0, tzinfo=UTC)

    response = client.fetch_candles(start, end)
    assert response.http_status == 200
    assert len(response.candles) > 0
