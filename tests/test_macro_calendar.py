"""Tests for ForexFactory macroeconomic calendar feed client, contract, and CLI."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.sources.macro_calendar_client import (
    MacroCalendarClient,
)
from bitcoin_data_platform.sources.macro_calendar_contract import (
    validate_macro_event,
    validate_macro_payload,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def _sample_feed() -> list[dict[str, Any]]:
    return [
        {
            "title": "FOMC Statement",
            "country": "USD",
            "date": "2026-09-18T14:00:00-04:00",
            "impact": "High",
            "forecast": "",
            "previous": "",
        },
        {
            "title": "Fed Interest Rate Decision",
            "country": "USD",
            "date": "2026-09-18T14:00:00-04:00",
            "impact": "High",
            "forecast": "5.25%",
            "previous": "5.50%",
        },
        {
            "title": "Existing Home Sales",
            "country": "USD",
            "date": "2026-09-18T10:00:00-04:00",
            "impact": "Low",
            "forecast": "3.85M",
            "previous": "3.95M",
        },
        {
            "title": "ECB Monetary Policy Statement",
            "country": "EUR",
            "date": "2026-09-18T08:45:00+02:00",
            "impact": "High",
            "forecast": "",
            "previous": "",
        },
    ]


def test_fetch_week_events_filters_usd_high_impact() -> None:
    """8. Client filters out non-USD and non-High impact events by default."""
    feed = _sample_feed()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=feed)

    transport = httpx.MockTransport(handler)
    client = MacroCalendarClient(transport=transport)

    events = client.fetch_week_events()
    assert len(events) == 2
    assert all(e.country == "USD" and e.impact == "High" for e in events)
    titles = [e.title for e in events]
    assert "FOMC Statement" in titles
    assert "Fed Interest Rate Decision" in titles


def test_fetch_week_events_handles_empty_week() -> None:
    """9. Client handles empty feed array cleanly returning empty list."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = MacroCalendarClient(transport=transport)

    events = client.fetch_week_events()
    assert events == []


def test_dedup_by_event_id() -> None:
    """10. Duplicate events with same title and timestamp are deduplicated."""
    feed = [
        {
            "title": "FOMC Statement",
            "country": "USD",
            "date": "2026-09-18T14:00:00-04:00",
            "impact": "High",
            "forecast": "",
            "previous": "",
        },
        {
            "title": "FOMC Statement",
            "country": "USD",
            "date": "2026-09-18T14:00:00-04:00",
            "impact": "High",
            "forecast": "Updated",
            "previous": "",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=feed)

    transport = httpx.MockTransport(handler)
    client = MacroCalendarClient(transport=transport)

    events = client.fetch_week_events()
    assert len(events) == 1
    assert events[0].title == "FOMC Statement"


def test_retries_on_transient_error() -> None:
    """11. Client retries on network failures / 503 and raises on exhaustion."""
    call_count = [0]
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_count[0] += 1
        if call_count[0] <= 2:
            return httpx.Response(502, text="Bad Gateway")
        return httpx.Response(200, json=_sample_feed())

    transport = httpx.MockTransport(handler)
    client = MacroCalendarClient(
        transport=transport,
        sleeper=sleeps.append,
        max_retries=3,
        base_backoff_seconds=0.01,
        min_request_interval_seconds=0.0,
        jitter=False,
    )

    events = client.fetch_week_events()
    assert len(events) == 2
    assert call_count[0] == 3
    assert len(sleeps) == 2


def test_contract_validation_rejects_malformed_event() -> None:
    """12. Contract validator rejects events missing required fields or invalid types."""
    res = validate_macro_payload({"not_a_list": True})
    assert res.is_valid is False
    assert any("must be a list" in v for v in res.violations)

    evt, violations = validate_macro_event(
        {"title": "", "country": "USD", "date": "2026-09-18T14:00:00-04:00"}
    )
    assert evt is None
    assert any("title must be a non-empty string" in v for v in violations)


def test_timezone_parsing_to_utc() -> None:
    """13. ForexFactory date with -04:00 EDT offset correctly converts to UTC."""
    evt, violations = validate_macro_event(
        {
            "title": "CPI Release",
            "country": "USD",
            "date": "2026-09-18T08:30:00-04:00",
            "impact": "High",
            "forecast": "0.2%",
            "previous": "0.1%",
        }
    )
    assert len(violations) == 0
    assert evt is not None
    assert evt.scheduled_utc == datetime(2026, 9, 18, 12, 30, 0, tzinfo=UTC)
    assert evt.scheduled_utc.tzinfo == UTC


def test_cli_fetch_macro_calendar_persists_to_duckdb(tmp_path: Path) -> None:
    """CLI subcommand fetch-macro-calendar fetches and persists events to DuckDB."""
    db_path = tmp_path / "state" / "platform.duckdb"
    feed = _sample_feed()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=feed)

    transport = httpx.MockTransport(handler)
    client = MacroCalendarClient(transport=transport)

    exit_code = main(
        ["fetch-macro-calendar", "--db-path", str(db_path)],
        macro_client=client,
    )
    assert exit_code == 0

    mgr = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    rows = mgr.execute_query(
        "SELECT event_id, country, title, impact, scheduled_utc, forecast, previous "
        "FROM raw_macro_economic_events ORDER BY title;"
    )
    assert len(rows) == 2
    assert rows[0]["country"] == "USD"
    assert rows[0]["impact"] == "High"
