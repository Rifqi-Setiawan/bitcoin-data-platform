"""Unit tests for multi-source RSS feed ingestion engine (Group A)."""

import httpx
import pytest

from bitcoin_data_platform.macro.feed_ingester import (
    FeedIngester,
    FeedSourceUnavailableError,
    compute_article_id,
    parse_rss_datetime,
)

SAMPLE_COINDESK_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>CoinDesk: Bitcoin, Ethereum and Crypto News</title>
    <link>https://www.coindesk.com</link>
    <description>News and analysis</description>
    <item>
      <title>SEC Approves In-Kind Creation for Spot Bitcoin ETFs</title>
      <link>https://www.coindesk.com/policy/2026/09/19/sec-etf-ruling/</link>
      <description>
        <![CDATA[The SEC has finalized rules allowing physical Bitcoin redemption.]]>
      </description>
      <pubDate>Sat, 19 Sep 2026 07:15:00 GMT</pubDate>
      <guid>https://www.coindesk.com/policy/2026/09/19/sec-etf-ruling/</guid>
    </item>
  </channel>
</rss>
"""

SAMPLE_COINTELEGRAPH_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Cointelegraph RSS Feed</title>
    <link>https://cointelegraph.com</link>
    <item>
      <title>Bitcoin Hashrate Hits New All-Time High</title>
      <link>https://cointelegraph.com/news/bitcoin-hashrate-ath-2026</link>
      <description>Network security strengthens as mining rigs deploy globally.</description>
      <pubDate>Sat, 19 Sep 2026 06:30:00 +0000</pubDate>
    </item>
  </channel>
</rss>
"""

SAMPLE_DECRYPT_XML = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Decrypt</title>
  <link href="https://decrypt.co" />
  <entry>
    <title>Federal Reserve Signals Neutral Rate Stance</title>
    <link href="https://decrypt.co/fed-rate-stance-2026" />
    <summary>Chair Powell reiterated data-dependent stance in Jackson Hole speech.</summary>
    <published>2026-09-19T05:00:00Z</published>
  </entry>
</feed>
"""


def test_rss_feed_parsing_coindesk() -> None:
    """1. Validates XML parsing of CoinDesk RSS feed format."""
    ingester = FeedIngester()
    articles = ingester.parse_xml_feed(SAMPLE_COINDESK_XML, source="CoinDesk")

    assert len(articles) == 1
    art = articles[0]
    assert art.source == "CoinDesk"
    assert art.title == "SEC Approves In-Kind Creation for Spot Bitcoin ETFs"
    assert art.url == "https://www.coindesk.com/policy/2026/09/19/sec-etf-ruling/"
    assert "allowing physical Bitcoin redemption" in art.summary
    assert art.published_utc.year == 2026
    assert art.published_utc.month == 9
    assert art.published_utc.day == 19
    assert art.published_utc.hour == 7


def test_rss_feed_parsing_cointelegraph() -> None:
    """2. Validates Cointelegraph feed format."""
    ingester = FeedIngester()
    articles = ingester.parse_xml_feed(SAMPLE_COINTELEGRAPH_XML, source="Cointelegraph")

    assert len(articles) == 1
    art = articles[0]
    assert art.source == "Cointelegraph"
    assert "Bitcoin Hashrate" in art.title
    assert art.url == "https://cointelegraph.com/news/bitcoin-hashrate-ath-2026"
    assert art.published_utc.hour == 6


def test_rss_feed_parsing_decrypt() -> None:
    """3. Validates Decrypt Atom feed format."""
    ingester = FeedIngester()
    articles = ingester.parse_xml_feed(SAMPLE_DECRYPT_XML, source="Decrypt")

    assert len(articles) == 1
    art = articles[0]
    assert art.source == "Decrypt"
    assert "Federal Reserve" in art.title
    assert art.url == "https://decrypt.co/fed-rate-stance-2026"
    assert art.published_utc.hour == 5


def test_rss_deduplication_hash() -> None:
    """4. Confirms identical title + URL yields identical article_id."""
    id1 = compute_article_id("CoinDesk", "https://example.com/a", "Bitcoin Surge")
    id2 = compute_article_id("CoinDesk", "https://example.com/a", "Bitcoin Surge")
    id3 = compute_article_id("CoinDesk", "https://example.com/b", "Bitcoin Surge")

    assert id1 == id2
    assert id1 != id3
    assert len(id1) == 64  # SHA-256 hex string


def test_rss_retry_backoff_transient_error() -> None:
    """5. Verifies HTTP 503 retry backoff."""
    call_count = 0
    delays: list[float] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return httpx.Response(503, text="Service Unavailable")
        return httpx.Response(200, text=SAMPLE_COINDESK_XML)

    transport = httpx.MockTransport(mock_handler)
    ingester = FeedIngester(
        max_retries=3,
        base_backoff_seconds=0.1,
        min_request_interval_seconds=0.0,
        transport=transport,
        sleeper=delays.append,
    )
    with ingester:
        articles = ingester.fetch_feed("CoinDesk", "https://mock.com/rss")

    assert len(articles) == 1
    assert call_count == 3
    assert len(delays) == 2


def test_rss_graceful_degradation_malformed_xml() -> None:
    """6. Handles invalid XML without crash, returning empty list."""
    ingester = FeedIngester()
    articles = ingester.parse_xml_feed("<invalid>xml", source="BrokenSource")
    assert articles == []


def test_rss_partial_feed_failure() -> None:
    """7. Ensure failure in 1 source does not block other 3 sources."""

    def mock_handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "coindesk" in url_str:
            return httpx.Response(500, text="Internal Server Error")
        if "cointelegraph" in url_str:
            return httpx.Response(200, text=SAMPLE_COINTELEGRAPH_XML)
        if "decrypt" in url_str:
            return httpx.Response(200, text=SAMPLE_DECRYPT_XML)
        return httpx.Response(200, text=SAMPLE_COINDESK_XML)

    transport = httpx.MockTransport(mock_handler)
    feeds = {
        "CoinDesk": "https://mock.com/coindesk",
        "Cointelegraph": "https://mock.com/cointelegraph",
        "Decrypt": "https://mock.com/decrypt",
    }
    ingester = FeedIngester(
        feeds=feeds,
        transport=transport,
        max_retries=1,
        base_backoff_seconds=0.01,
    )
    with ingester:
        articles = ingester.fetch_all_feeds()

    assert len(articles) == 2
    sources = {a.source for a in articles}
    assert "Cointelegraph" in sources
    assert "Decrypt" in sources
    assert "CoinDesk" not in sources


def test_rss_timeout_handling() -> None:
    """8. Validates connect/read timeout isolation."""

    def mock_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Read timed out")

    transport = httpx.MockTransport(mock_handler)
    ingester = FeedIngester(transport=transport, max_retries=1, base_backoff_seconds=0.01)
    with ingester, pytest.raises(FeedSourceUnavailableError):
        ingester.fetch_feed("CoinDesk", "https://timeout.com/rss")


def test_rss_empty_feed() -> None:
    """9. Returns empty list without exceptions when feed has no items."""
    empty_xml = (
        '<?xml version="1.0"?><rss version="2.0"><channel><title>Empty</title></channel></rss>'
    )
    ingester = FeedIngester()
    articles = ingester.parse_xml_feed(empty_xml, source="EmptyFeed")
    assert articles == []


def test_rss_pubdate_parsing() -> None:
    """10. Tests RFC 822 and ISO-8601 parsing into UTC."""
    rfc_dt = parse_rss_datetime("Sat, 19 Sep 2026 14:30:00 +0700")
    # 14:30 +0700 is 07:30 UTC
    assert rfc_dt.hour == 7
    assert rfc_dt.minute == 30

    iso_dt = parse_rss_datetime("2026-09-19T10:15:00Z")
    assert iso_dt.hour == 10
    assert iso_dt.minute == 15
