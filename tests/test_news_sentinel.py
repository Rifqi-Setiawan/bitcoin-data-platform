"""Tests for NewsSentinel RSS scanning, keyword detection, deduplication, and retries."""

from pathlib import Path

import httpx

from bitcoin_data_platform.signals.news_sentinel import (
    NewsSentinel,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def _make_rss_xml(items: list[dict[str, str]]) -> str:
    """Helper to build RSS 2.0 XML string."""
    item_blocks = []
    for it in items:
        title = it.get("title", "")
        link = it.get("link", "https://example.com/news")
        pub_date = it.get("pubDate", "Fri, 18 Sep 2026 10:00:00 GMT")
        desc = it.get("description", "")
        item_blocks.append(
            f"<item>\n"
            f"  <title>{title}</title>\n"
            f"  <link>{link}</link>\n"
            f"  <pubDate>{pub_date}</pubDate>\n"
            f"  <description>{desc}</description>\n"
            f"</item>"
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0">\n'
        "  <channel>\n"
        "    <title>CoinDesk</title>\n" + "\n".join(item_blocks) + "\n  </channel>\n</rss>"
    )


def test_scan_detects_critical_keyword(tmp_path: Path) -> None:
    """11. test_scan_detects_critical_keyword: 'hack' in title triggers CRITICAL."""
    xml_content = _make_rss_xml(
        [
            {
                "title": "Major Crypto Exchange Suffers Massive Hack",
                "link": "https://www.coindesk.com/hack-1",
                "pubDate": "Fri, 18 Sep 2026 10:00:00 GMT",
                "description": "Over $100M stolen in recent security breach.",
            }
        ]
    )

    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=xml_content))
    sentinel = NewsSentinel(transport=transport)
    alerts = sentinel.scan()

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.severity == "CRITICAL"
    assert "hack" in alert.matched_keywords
    assert alert.title == "Major Crypto Exchange Suffers Massive Hack"
    assert alert.link == "https://www.coindesk.com/hack-1"


def test_scan_detects_warning_keyword(tmp_path: Path) -> None:
    """12. test_scan_detects_warning_keyword: 'ETF approved' triggers WARNING."""
    xml_content = _make_rss_xml(
        [
            {
                "title": "First Solana ETF Approved by Global Regulators",
                "link": "https://www.coindesk.com/etf-sol",
                "pubDate": "Fri, 18 Sep 2026 11:00:00 GMT",
                "description": "Trading begins next Monday.",
            }
        ]
    )

    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=xml_content))
    sentinel = NewsSentinel(transport=transport)
    alerts = sentinel.scan()

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.severity == "WARNING"
    assert "ETF approv/reject/deny" in alert.matched_keywords


def test_scan_dedup_skips_seen_alerts(tmp_path: Path) -> None:
    """13. test_scan_dedup_skips_seen_alerts: already-ingested alert_ids filtered out."""
    db_path = tmp_path / "test.duckdb"
    db_mgr = DuckDBManager(db_path=db_path)
    db_mgr.create_news_sentinel_alerts_table()

    xml_content = _make_rss_xml(
        [
            {
                "title": "Protocol Exploit Drains Liquidity Pool",
                "link": "https://www.coindesk.com/exploit-1",
                "pubDate": "Fri, 18 Sep 2026 12:00:00 GMT",
                "description": "Smart contract vulnerability exploited.",
            }
        ]
    )

    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=xml_content))
    sentinel = NewsSentinel(transport=transport, db_manager=db_mgr)

    # First scan: returns 1 alert and persists it
    alerts_1 = sentinel.scan()
    assert len(alerts_1) == 1
    assert alerts_1[0].severity == "CRITICAL"

    # Second scan: same alert already stored in DB, returns 0 alerts
    alerts_2 = sentinel.scan()
    assert len(alerts_2) == 0


def test_scan_handles_empty_feed(tmp_path: Path) -> None:
    """14. test_scan_handles_empty_feed: no items returns empty list."""
    xml_content = _make_rss_xml([])
    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=xml_content))
    sentinel = NewsSentinel(transport=transport)
    alerts = sentinel.scan()

    assert alerts == []


def test_scan_handles_malformed_rss(tmp_path: Path) -> None:
    """15. test_scan_handles_malformed_rss: graceful degradation on XML parse error."""
    malformed_xml = "<html><body>Not valid RSS XML <<>>><!/</body>"
    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=malformed_xml))
    sentinel = NewsSentinel(transport=transport)
    alerts = sentinel.scan()

    assert alerts == []


def test_keyword_regex_case_insensitive(tmp_path: Path) -> None:
    """16. test_keyword_regex_case_insensitive: 'HACKED' matches 'hack'."""
    xml_content = _make_rss_xml(
        [
            {
                "title": "BRIDGE PROTOCOL HACKED FOR $30M",
                "link": "https://www.coindesk.com/hacked",
                "pubDate": "Fri, 18 Sep 2026 13:00:00 GMT",
                "description": "Security team investigating.",
            }
        ]
    )

    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=xml_content))
    sentinel = NewsSentinel(transport=transport)
    alerts = sentinel.scan()

    assert len(alerts) == 1
    assert alerts[0].severity == "CRITICAL"
    assert "hack" in alerts[0].matched_keywords


def test_multiple_keywords_matched(tmp_path: Path) -> None:
    """17. test_multiple_keywords_matched: alert lists all matched keywords."""
    xml_content = _make_rss_xml(
        [
            {
                "title": "Lender Declares Bankrupt as Insolvency Threatens Collapse",
                "link": "https://www.coindesk.com/collapse",
                "pubDate": "Fri, 18 Sep 2026 14:00:00 GMT",
                "description": "Emergency liquidity withdrawal halted.",
            }
        ]
    )

    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=xml_content))
    sentinel = NewsSentinel(transport=transport)
    alerts = sentinel.scan()

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.severity == "CRITICAL"
    # Matches: bankrupt, insolvency, collapse, emergency
    assert "bankrupt" in alert.matched_keywords
    assert "insolvency" in alert.matched_keywords
    assert "collapse" in alert.matched_keywords
    assert "emergency" in alert.matched_keywords
    assert len(alert.matched_keywords) >= 4


def test_retries_on_transient_error(tmp_path: Path) -> None:
    """18. test_retries_on_transient_error: retries on network error and succeeds."""
    attempt_count = 0
    valid_xml = _make_rss_xml(
        [
            {
                "title": "SEC Sue Top Trading Firm",
                "link": "https://www.coindesk.com/sec",
                "pubDate": "Fri, 18 Sep 2026 15:00:00 GMT",
                "description": "Regulatory enforcement action.",
            }
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            return httpx.Response(503, text="Service Unavailable")
        return httpx.Response(200, text=valid_xml)

    sleep_calls: list[float] = []
    transport = httpx.MockTransport(handler)
    sentinel = NewsSentinel(
        transport=transport,
        sleeper=sleep_calls.append,
        max_retries=3,
    )

    alerts = sentinel.scan()
    assert attempt_count == 2
    assert len(sleep_calls) == 1
    assert len(alerts) == 1
    assert alerts[0].severity == "CRITICAL"
    assert "SEC sue/charge" in alerts[0].matched_keywords
