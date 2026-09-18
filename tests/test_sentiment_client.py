"""Tests for Alternative.me Fear & Greed sentiment client, contract, and CLI."""

from datetime import UTC
from pathlib import Path
from typing import Any

import httpx
import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.sources.sentiment_client import (
    SentimentClient,
    SourceUnavailableError,
)
from bitcoin_data_platform.sources.sentiment_contract import (
    SentimentRecord,
    validate_sentiment_payload,
    validate_sentiment_record,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def _sample_payload(limit: int = 1) -> dict[str, Any]:
    base_ts = 1789689600  # 2026-09-18 00:00:00 UTC
    data = []
    for i in range(limit):
        data.append(
            {
                "value": str(45 + i),
                "value_classification": "Fear" if (45 + i) < 50 else "Neutral",
                "timestamp": str(base_ts - i * 86400),
                "time_until_update": "54785",
            }
        )
    return {
        "name": "Fear and Greed Index",
        "data": data,
        "metadata": {"error": None},
    }


def test_fetch_current_success() -> None:
    """1. Successful fetch of current sentiment parses valid SentimentRecord."""
    payload = _sample_payload(1)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["limit"] == "1"
        assert request.url.params["format"] == "json"
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = SentimentClient(transport=transport)

    record = client.fetch_current()
    assert isinstance(record, SentimentRecord)
    assert record.value == 45
    assert record.classification == "Fear"
    assert record.date_utc.tzinfo == UTC
    assert record.date_utc.hour == 0
    assert record.date_utc.minute == 0


def test_fetch_current_retries_on_transient_error() -> None:
    """2. Client retries on 500/503 status codes and succeeds on recovery."""
    payload = _sample_payload(1)
    call_count = [0]
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count[0] += 1
        if call_count[0] == 1:
            return httpx.Response(503, text="Service Unavailable")
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = SentimentClient(
        transport=transport,
        sleeper=sleeps.append,
        max_retries=3,
        base_backoff_seconds=0.1,
        jitter=False,
    )

    record = client.fetch_current()
    assert call_count[0] == 2
    assert record.value == 45
    assert len(sleeps) >= 1


def test_fetch_current_fails_after_max_retries() -> None:
    """3. Client raises SourceUnavailableError after exhausting retry attempts."""
    call_count = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        call_count[0] += 1
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(handler)
    client = SentimentClient(
        transport=transport,
        sleeper=lambda s: None,
        max_retries=2,
        base_backoff_seconds=0.01,
        jitter=False,
    )

    with pytest.raises(SourceUnavailableError, match="unavailable after 2 retries"):
        client.fetch_current()
    assert call_count[0] == 3  # initial + 2 retries


def test_fetch_history_returns_multiple_records() -> None:
    """4. Fetching history with limit=7 returns 7 ordered records."""
    payload = _sample_payload(7)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["limit"] == "7"
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = SentimentClient(transport=transport)

    records = client.fetch_history(limit=7)
    assert len(records) == 7
    assert records[0].value == 45
    assert records[6].value == 51


def test_contract_validation_rejects_invalid_value() -> None:
    """5. Contract validator rejects values > 100 or < 0."""
    rec_high, violations_high = validate_sentiment_record(
        {"value": "101", "value_classification": "Greed", "timestamp": "1789689600"}
    )
    assert rec_high is None
    assert any("between 0 and 100" in v for v in violations_high)

    rec_low, violations_low = validate_sentiment_record(
        {"value": "-5", "value_classification": "Extreme Fear", "timestamp": "1789689600"}
    )
    assert rec_low is None
    assert any("between 0 and 100" in v for v in violations_low)


def test_contract_validation_rejects_missing_fields() -> None:
    """6. Contract validator rejects payloads with missing or invalid fields."""
    res = validate_sentiment_payload({"error": "invalid"})
    assert res.is_valid is False
    assert any("missing required 'data' field" in v for v in res.violations)

    rec, violations = validate_sentiment_record(
        {"value": "50", "timestamp": "1789689600"}  # missing value_classification
    )
    assert rec is None
    assert any("value_classification" in v for v in violations)


def test_rate_limiting_enforced() -> None:
    """7. Rate limiter enforces minimum interval between calls."""
    payload = _sample_payload(1)
    fake_time = [100.0]
    sleeps: list[float] = []

    def clock() -> float:
        return fake_time[0]

    def sleeper(s: float) -> None:
        sleeps.append(s)
        fake_time[0] += s

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = SentimentClient(
        transport=transport,
        clock=clock,
        sleeper=sleeper,
        min_request_interval_seconds=1.0,
    )

    client.fetch_current()
    # Immediate second call
    client.fetch_current()
    assert len(sleeps) == 1
    assert sleeps[0] == pytest.approx(1.0)


def test_cli_fetch_sentiment_persists_to_duckdb(tmp_path: Path) -> None:
    """CLI subcommand fetch-sentiment fetches and persists records into DuckDB."""
    db_path = tmp_path / "state" / "platform.duckdb"
    payload = _sample_payload(3)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = SentimentClient(transport=transport)

    exit_code = main(
        ["fetch-sentiment", "--limit", "3", "--db-path", str(db_path)],
        sentiment_client=client,
    )
    assert exit_code == 0

    mgr = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    rows = mgr.execute_query(
        "SELECT sentiment_date_utc, fng_value, fng_classification "
        "FROM raw_crypto_sentiment_daily ORDER BY sentiment_date_utc DESC;"
    )
    assert len(rows) == 3
    assert rows[0]["fng_value"] == 45


def test_cli_fetch_sentiment_invalid_limit(tmp_path: Path) -> None:
    """CLI subcommand fetch-sentiment rejects limits outside 1..30."""
    db_path = tmp_path / "state" / "platform.duckdb"
    exit_code = main(
        ["fetch-sentiment", "--limit", "0", "--db-path", str(db_path)],
    )
    assert exit_code == 2

    exit_code_high = main(
        ["fetch-sentiment", "--limit", "35", "--db-path", str(db_path)],
    )
    assert exit_code_high == 2
