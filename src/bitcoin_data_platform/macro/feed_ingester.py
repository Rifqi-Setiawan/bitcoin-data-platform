"""Multi-source RSS feed ingestion engine with SHA-256 deduplication and error isolation."""

from __future__ import annotations

import email.utils
import hashlib
import logging
import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from bitcoin_data_platform.macro.models import (
    AlertSeverity,
    MacroArticle,
    MacroPillar,
)
from bitcoin_data_platform.sources.http_helpers import apply_rate_limit, compute_backoff_delay

logger = logging.getLogger(__name__)

DEFAULT_FEEDS: dict[str, str] = {
    "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "Cointelegraph": "https://cointelegraph.com/rss",
    "Decrypt": "https://decrypt.co/feed",
    "BitcoinMagazine": "https://bitcoinmagazine.com/.rss/full/",
}

DEFAULT_USER_AGENT = "bitcoin-data-platform/0.1.0"
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class FeedIngesterError(Exception):
    """Base exception for feed ingestion failures."""


class FeedSourceUnavailableError(FeedIngesterError):
    """Raised when an RSS feed cannot be retrieved after retries."""


def compute_article_id(source: str, url: str, title: str) -> str:
    """Compute deterministic SHA-256 hash from source, url, and title."""
    raw = f"{source}:{url.strip()}:{title.strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_rss_datetime(raw_val: str) -> datetime:
    """Parse RFC 822 / RFC 2822 or ISO-8601 date string to timezone-aware UTC datetime."""
    val = raw_val.strip()
    # Try RFC 2822 / 822 first (standard RSS 2.0)
    try:
        dt = email.utils.parsedate_to_datetime(val)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        pass

    # Try ISO-8601 (Atom / Dublin Core)
    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        pass

    # Fallback to current UTC
    return datetime.now(UTC)


def clean_html(text: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", " ", text)
    return " ".join(clean.split())


class FeedIngester:
    """Multi-source RSS feed fetcher with resilient retry and deduplication."""

    def __init__(
        self,
        *,
        feeds: dict[str, str] | None = None,
        max_retries: int = 3,
        base_backoff_seconds: float = 0.5,
        max_backoff_seconds: float = 5.0,
        min_request_interval_seconds: float = 0.1,
        timeout: float | httpx.Timeout | None = None,
        user_agent: str = DEFAULT_USER_AGENT,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        time_provider: Callable[[], datetime] | None = None,
        jitter: bool = False,
    ) -> None:
        self.feeds = feeds if feeds is not None else DEFAULT_FEEDS.copy()
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
            timeout if timeout is not None else httpx.Timeout(20.0, connect=5.0, read=15.0)
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
                    "Accept": "application/rss+xml, application/xml, text/xml, */*",
                },
            )
            self._owns_client = True

    def close(self) -> None:
        """Close HTTP client if owned."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> FeedIngester:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _fetch_url(self, url: str) -> str:
        """Fetch XML content from feed URL with rate limiting and retry handling."""
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
                response = self._client.get(url)

                if response.status_code in RETRYABLE_STATUS_CODES:
                    attempt += 1
                    if attempt > self.max_retries:
                        raise FeedSourceUnavailableError(
                            f"Feed URL {url} returned HTTP {response.status_code} "
                            f"after {self.max_retries} retries."
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
                    raise FeedIngesterError(
                        f"Feed URL {url} failed with HTTP {response.status_code}: "
                        f"{response.reason_phrase}"
                    )

                return response.text

            except (httpx.TransportError, httpx.RequestError) as exc:
                last_error = exc
                attempt += 1
                if attempt > self.max_retries:
                    raise FeedSourceUnavailableError(
                        f"Feed URL {url} network failure after {self.max_retries} retries: {exc}"
                    ) from exc

                delay = compute_backoff_delay(
                    attempt=attempt,
                    response=None,
                    base_backoff=self.base_backoff_seconds,
                    max_backoff=self.max_backoff_seconds,
                    jitter=self.jitter,
                )
                self._sleeper(delay)

        raise FeedSourceUnavailableError(
            f"Feed URL {url} unavailable after {self.max_retries} retries."
        ) from last_error

    def parse_xml_feed(
        self, xml_content: str, source: str, ingested_at: datetime | None = None
    ) -> list[MacroArticle]:
        """Parse raw RSS/Atom XML text into typed MacroArticle objects."""
        if not xml_content or not xml_content.strip():
            return []

        try:
            root = ET.fromstring(xml_content.strip())
        except ET.ParseError as exc:
            logger.warning("Malformed XML from source %s: %s", source, exc)
            return []

        now_utc = ingested_at or self._time_provider()
        articles: list[MacroArticle] = []

        # Find items across RSS <channel><item> or Atom <entry>
        items = root.findall(".//item")
        if not items:
            # Check for Atom namespace
            items = root.findall(".//{http://www.w3.org/2005/Atom}entry")
            if not items:
                items = root.findall(".//entry")

        for item in items:
            # 1. Title
            title_elem = item.find("title")
            if title_elem is None:
                title_elem = item.find("{http://www.w3.org/2005/Atom}title")
            title = clean_html(
                title_elem.text if title_elem is not None and title_elem.text else ""
            )
            if not title:
                continue

            # 2. Link / URL
            url = ""
            link_elem = item.find("link")
            if link_elem is None:
                link_elem = item.find("{http://www.w3.org/2005/Atom}link")

            if link_elem is not None:
                # Standard RSS <link>http...</link>
                if link_elem.text and link_elem.text.strip():
                    url = link_elem.text.strip()
                # Atom <link href="http..." />
                elif "href" in link_elem.attrib:
                    url = link_elem.attrib["href"].strip()

            if not url:
                # Look for guid isPermaLink
                guid_elem = item.find("guid")
                if guid_elem is not None and guid_elem.text and guid_elem.text.startswith("http"):
                    url = guid_elem.text.strip()

            if not url:
                url = f"urn:{source.lower()}:{hashlib.sha256(title.encode()).hexdigest()[:16]}"

            # 3. Publication Date
            pub_date_str = ""
            for tag in (
                "pubDate",
                "{http://purl.org/dc/elements/1.1/}date",
                "{http://www.w3.org/2005/Atom}published",
                "{http://www.w3.org/2005/Atom}updated",
            ):
                elem = item.find(tag)
                if elem is not None and elem.text and elem.text.strip():
                    pub_date_str = elem.text.strip()
                    break

            pub_dt = parse_rss_datetime(pub_date_str) if pub_date_str else now_utc

            # 4. Summary / Description
            summary = ""
            for tag in (
                "description",
                "{http://purl.org/rss/1.0/modules/content/}encoded",
                "{http://www.w3.org/2005/Atom}summary",
                "{http://www.w3.org/2005/Atom}content",
            ):
                elem = item.find(tag)
                if elem is not None and elem.text and elem.text.strip():
                    summary = clean_html(elem.text.strip())
                    break

            article_id = compute_article_id(source, url, title)

            articles.append(
                MacroArticle(
                    article_id=article_id,
                    source=source,
                    title=title,
                    url=url,
                    published_utc=pub_dt,
                    summary=summary[:1000],  # Bounded summary length
                    pillar=MacroPillar.GENERAL,
                    severity=AlertSeverity.LOW,
                    polarity=0.0,
                    matched_keywords=[],
                    ingested_at_utc=now_utc,
                )
            )

        return articles

    def fetch_feed(self, source: str, url: str) -> list[MacroArticle]:
        """Fetch and parse a single RSS feed."""
        xml_content = self._fetch_url(url)
        return self.parse_xml_feed(xml_content, source=source)

    def fetch_all_feeds(self, sources: list[str] | None = None) -> list[MacroArticle]:
        """Fetch and aggregate articles across all configured feeds with error isolation."""
        target_sources = (
            {s: self.feeds[s] for s in sources if s in self.feeds} if sources else self.feeds
        )

        all_articles: list[MacroArticle] = []
        deduped: dict[str, MacroArticle] = {}

        for source_name, feed_url in target_sources.items():
            try:
                feed_articles = self.fetch_feed(source_name, feed_url)
                for a in feed_articles:
                    if a.article_id not in deduped:
                        deduped[a.article_id] = a
                        all_articles.append(a)
            except Exception as exc:
                logger.warning("Isolated feed failure for %s (%s): %s", source_name, feed_url, exc)

        all_articles.sort(key=lambda a: a.published_utc, reverse=True)
        return all_articles
