"""Tests for Coin Metrics Community API v4 client."""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from bitcoin_data_platform.sources.coin_metrics_client import (
    DEFAULT_USER_AGENT,
    CoinMetricsClient,
    CoinMetricsHTTPError,
    SourceUnavailableError,
)


def _sample_page(page: int = 1, has_next: bool = False) -> dict[str, Any]:
    date_str = f"2026-01-0{page}"
    data = [
        {
            "asset": "btc",
            "time": f"{date_str}T00:00:00.000000000Z",
            "TxCnt": f"{300000 + page * 1000}",
            "AdrActCnt": f"{800000 + page * 1000}",
        }
    ]
    resp: dict[str, Any] = {"data": data}
    if has_next:
        resp["next_page_url"] = (
            "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?page=2"
        )
    return resp


def test_fetch_asset_metrics_success() -> None:
    """1. Successful fetch returns valid CoinMetricsResponse with records and metadata."""
    payload = _sample_page(1, has_next=False)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["assets"] == "btc"
        assert request.url.params["metrics"] == "TxCnt,AdrActCnt"
        assert request.url.params["frequency"] == "1d"
        assert request.url.params["start_time"] == "2026-01-01"
        assert request.url.params["end_time"] == "2026-01-07"
        return httpx.Response(200, json=payload, headers={"x-request-id": "cm-req-123"})

    transport = httpx.MockTransport(handler)
    client = CoinMetricsClient(transport=transport)

    resp = client.fetch_asset_metrics("2026-01-01", "2026-01-07")
    assert resp.http_status == 200
    assert resp.provider_request_id == "cm-req-123"
    assert len(resp.records) == 1
    assert resp.records[0].tx_count == 301000
    assert resp.records[0].active_addresses == 801000
    assert resp.retrieved_at_utc.tzinfo == UTC


def test_rate_limit_spacing() -> None:
    """2. Rate limiter enforces minimum 600ms spacing between requests."""
    fake_now = [100.0]
    sleep_calls: list[float] = []

    def mock_clock() -> float:
        return fake_now[0]

    def mock_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        fake_now[0] += seconds

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": []})

    transport = httpx.MockTransport(handler)
    client = CoinMetricsClient(
        transport=transport,
        min_request_interval_seconds=0.6,
        clock=mock_clock,
        sleeper=mock_sleep,
    )

    # First request sets timestamp
    client.fetch_asset_metrics("2026-01-01", "2026-01-02")
    assert len(sleep_calls) == 0

    # Advance fake time by only 0.2s (< 0.6s)
    fake_now[0] += 0.2
    client.fetch_asset_metrics("2026-01-02", "2026-01-03")

    assert len(sleep_calls) == 1
    assert abs(sleep_calls[0] - 0.4) < 1e-6


def test_retry_on_429_with_retry_after() -> None:
    """3. Rate-limited 429 response respects Retry-After header and succeeds on retry."""
    attempts = [0]
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts[0] += 1
        if attempts[0] == 1:
            return httpx.Response(
                429,
                headers={"Retry-After": "2"},
                text="Rate limited",
            )
        return httpx.Response(200, json=_sample_page(1))

    transport = httpx.MockTransport(handler)
    client = CoinMetricsClient(
        transport=transport,
        sleeper=sleep_calls.append,
        jitter=False,
    )

    resp = client.fetch_asset_metrics("2026-01-01", "2026-01-02")
    assert resp.http_status == 200
    assert attempts[0] == 2
    assert any(s >= 2.0 for s in sleep_calls)


def test_retry_on_5xx_transient_error() -> None:
    """4. Transient 502/503 errors retry and succeed."""
    attempts = [0]
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts[0] += 1
        if attempts[0] == 1:
            return httpx.Response(502, text="Bad Gateway")
        if attempts[0] == 2:
            return httpx.Response(503, text="Service Unavailable")
        return httpx.Response(200, json=_sample_page(1))

    transport = httpx.MockTransport(handler)
    client = CoinMetricsClient(
        transport=transport,
        sleeper=sleep_calls.append,
        jitter=False,
    )

    resp = client.fetch_asset_metrics("2026-01-01", "2026-01-02")
    assert resp.http_status == 200
    assert attempts[0] == 3
    assert len(sleep_calls) == 2


def test_source_unavailable_when_retries_exhausted() -> None:
    """5. SourceUnavailableError raised when max retries are exhausted."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(handler)
    client = CoinMetricsClient(
        transport=transport,
        max_retries=3,
        sleeper=lambda _: None,
        jitter=False,
    )

    with pytest.raises(SourceUnavailableError) as exc_info:
        client.fetch_asset_metrics("2026-01-01", "2026-01-02")

    assert "unavailable after 3 attempts" in str(exc_info.value)


def test_non_retryable_4xx_raises_http_error() -> None:
    """6. 400 Client Error fails immediately without retry."""
    attempts = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        attempts[0] += 1
        return httpx.Response(400, text="Bad Request: Unknown Metric")

    transport = httpx.MockTransport(handler)
    client = CoinMetricsClient(transport=transport)

    with pytest.raises(CoinMetricsHTTPError) as exc_info:
        client.fetch_asset_metrics("2026-01-01", "2026-01-02")

    assert attempts[0] == 1
    assert exc_info.value.status_code == 400


def test_pagination_handling() -> None:
    """7. Follows next_page_url to accumulate all records across pages."""
    call_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        call_urls.append(url_str)
        if "page=2" in url_str:
            return httpx.Response(200, json=_sample_page(page=2, has_next=False))
        return httpx.Response(200, json=_sample_page(page=1, has_next=True))

    transport = httpx.MockTransport(handler)
    client = CoinMetricsClient(transport=transport, sleeper=lambda _: None)

    resp = client.fetch_asset_metrics("2026-01-01", "2026-01-02")
    assert len(call_urls) == 2
    assert len(resp.records) == 2
    assert resp.records[0].time_utc == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert resp.records[1].time_utc == datetime(2026, 1, 2, 0, 0, tzinfo=UTC)


def test_user_agent_and_context_manager() -> None:
    """8. Verifies custom User-Agent header and context manager close."""
    seen_ua = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_ua.append(request.headers.get("User-Agent"))
        return httpx.Response(200, json={"data": []})

    transport = httpx.MockTransport(handler)
    with CoinMetricsClient(transport=transport) as client:
        client.fetch_asset_metrics("2026-01-01", "2026-01-02")

    assert len(seen_ua) == 1
    assert seen_ua[0] == DEFAULT_USER_AGENT
