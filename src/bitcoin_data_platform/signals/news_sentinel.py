"""News Sentinel for scanning CoinDesk RSS feed and detecting critical market events."""

import hashlib
import logging
import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import httpx

from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager

logger = logging.getLogger(__name__)

DEFAULT_FEED_URL = "https://www.coindesk.com/arc/outboundfeeds/rss/"
DEFAULT_USER_AGENT = "bitcoin-data-platform/0.1.0"
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

# CRITICAL and WARNING keyword rules
CRITICAL_KEYWORD_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("hack", re.compile(r"\bhack(?:ed|ing)?\b", re.IGNORECASE)),
    ("exploit", re.compile(r"\bexploit(?:ed|ing|s)?\b", re.IGNORECASE)),
    ("insolvency", re.compile(r"\binsolvency\b", re.IGNORECASE)),
    ("insolvent", re.compile(r"\binsolvent\b", re.IGNORECASE)),
    ("bankrupt", re.compile(r"\bbankrupt(?:cy)?\b", re.IGNORECASE)),
    (
        "SEC sue/charge",
        re.compile(r"\bsec\s+(?:sue[ds]?|suing|charge[ds]?|charging|enforcement)\b", re.IGNORECASE),
    ),
    ("ban crypto", re.compile(r"\bban(?:ned|ning)?\b.*?\bcrypto\b", re.IGNORECASE)),
    ("emergency", re.compile(r"\bemergency\b", re.IGNORECASE)),
    ("collapse", re.compile(r"\bcollapse[ds]?\b", re.IGNORECASE)),
]

WARNING_KEYWORD_RULES: list[tuple[str, re.Pattern[str]]] = [
    (
        "ETF approv/reject/deny",
        re.compile(r"\betf\s+(?:approv\w*|reject\w*|den\w*)\b", re.IGNORECASE),
    ),
    ("FOMC", re.compile(r"\bfomc\b", re.IGNORECASE)),
    ("rate hike/cut", re.compile(r"\brate\s+(?:hike[s]?|cut[s]?)\b", re.IGNORECASE)),
    ("CPI", re.compile(r"\bcpi\b", re.IGNORECASE)),
    ("regulation", re.compile(r"\bregulation[s]?\b", re.IGNORECASE)),
    (
        "stablecoin depeg/crash",
        re.compile(r"\bstablecoin\s+(?:depeg\w*|crash\w*)\b", re.IGNORECASE),
    ),
]


class NewsSentinelError(Exception):
    """Base exception for news sentinel errors."""


@dataclass(frozen=True)
class NewsAlert:
    """Represents a breaking news alert triggered by keyword matching."""

    alert_id: str  # SHA-256(title + published)
    title: str
    link: str
    published_utc: datetime
    matched_keywords: list[str]
    severity: str  # CRITICAL | WARNING
    ingested_at_utc: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "alert_id": self.alert_id,
            "title": self.title,
            "link": self.link,
            "published_utc": self.published_utc.isoformat(),
            "matched_keywords": list(self.matched_keywords),
            "severity": self.severity,
            "ingested_at_utc": self.ingested_at_utc.isoformat(),
        }


def parse_pub_date(pub_date_str: str) -> datetime:
    """Parse RSS pubDate string to UTC datetime."""
    pub_clean = pub_date_str.strip()
    if not pub_clean:
        return datetime.now(UTC)
    try:
        dt = parsedate_to_datetime(pub_clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(pub_clean.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return datetime.now(UTC)


def compute_alert_id(title: str, published: str | datetime) -> str:
    """Compute deterministic SHA-256 alert ID from title and published timestamp/string."""
    pub_str = published.isoformat() if isinstance(published, datetime) else str(published)
    return hashlib.sha256(f"{title}{pub_str}".encode()).hexdigest()


class NewsSentinel:
    """Scans RSS feeds for critical market keywords and produces deduplicated alerts."""

    def __init__(
        self,
        *,
        feed_url: str = DEFAULT_FEED_URL,
        max_retries: int = 3,
        base_backoff_seconds: float = 0.5,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        time_provider: Callable[[], datetime] | None = None,
        db_path: Path | str | None = None,
        db_manager: DuckDBManager | None = None,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self.feed_url = feed_url
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self._sleeper = sleeper
        self._clock = clock
        self._time_provider = time_provider or (lambda: datetime.now(UTC))
        self._seen_alert_ids: set[str] = set()

        if db_manager is not None:
            self.db_manager: DuckDBManager | None = db_manager
        elif db_path is not None:
            self.db_manager = DuckDBManager(db_path=Path(db_path))
        else:
            self.db_manager = None

        if http_client is not None:
            self._client = http_client
            self._owns_client = False
        else:
            self._client = httpx.Client(
                transport=transport,
                timeout=httpx.Timeout(20.0, connect=10.0, read=15.0),
                headers={"User-Agent": user_agent},
            )
            self._owns_client = True

    def close(self) -> None:
        """Close HTTP client if owned."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "NewsSentinel":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _fetch_feed_xml(self) -> str:
        """Fetch RSS XML with retry handling on transient network/HTTP errors."""
        attempt = 0
        last_error: Exception | None = None

        while attempt <= self.max_retries:
            attempt += 1
            try:
                response = self._client.get(self.feed_url)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt <= self.max_retries:
                        delay = self.base_backoff_seconds * (2 ** (attempt - 1))
                        self._sleeper(delay)
                        continue
                    raise NewsSentinelError(
                        f"Feed returned HTTP {response.status_code} "
                        f"after {self.max_retries} retries"
                    )

                response.raise_for_status()
                return response.text

            except httpx.TransportError as exc:
                last_error = exc
                if attempt <= self.max_retries:
                    delay = self.base_backoff_seconds * (2 ** (attempt - 1))
                    self._sleeper(delay)
                    continue
                raise NewsSentinelError(
                    f"Feed request failed after {self.max_retries} retries: {exc}"
                ) from exc
            except httpx.HTTPStatusError as exc:
                raise NewsSentinelError(f"Feed returned HTTP error: {exc}") from exc

        raise NewsSentinelError(
            f"Feed fetch failed after {self.max_retries} attempts: {last_error}"
        )

    def scan(self) -> list[NewsAlert]:
        """Fetch feed, match keywords, deduplicate against DuckDB/in-memory, and return alerts."""
        now_utc = self._time_provider()
        xml_content = self._fetch_feed_xml()

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:
            logger.warning("Failed to parse RSS XML; graceful degradation returning empty list")
            return []

        items = root.findall(".//item")
        if not items:
            return []

        candidates: list[NewsAlert] = []
        for item in items:
            title_node = item.find("title")
            link_node = item.find("link")
            pub_date_node = item.find("pubDate")
            desc_node = item.find("description")

            title = title_node.text.strip() if title_node is not None and title_node.text else ""
            link = link_node.text.strip() if link_node is not None and link_node.text else ""
            pub_date_str = (
                pub_date_node.text.strip()
                if pub_date_node is not None and pub_date_node.text
                else ""
            )
            desc = desc_node.text.strip() if desc_node is not None and desc_node.text else ""

            if not title:
                continue

            text_to_check = f"{title} {desc}"

            matched_critical: list[str] = []
            for name, pattern in CRITICAL_KEYWORD_RULES:
                if pattern.search(text_to_check):
                    matched_critical.append(name)

            matched_warning: list[str] = []
            for name, pattern in WARNING_KEYWORD_RULES:
                if pattern.search(text_to_check):
                    matched_warning.append(name)

            if not matched_critical and not matched_warning:
                continue

            if matched_critical:
                severity = "CRITICAL"
                all_keywords = matched_critical + matched_warning
            else:
                severity = "WARNING"
                all_keywords = matched_warning

            published_utc = parse_pub_date(pub_date_str)
            # Use raw pub_date_str if present for exact SHA-256(title + published)
            alert_id = compute_alert_id(title, pub_date_str if pub_date_str else published_utc)

            alert = NewsAlert(
                alert_id=alert_id,
                title=title,
                link=link,
                published_utc=published_utc,
                matched_keywords=all_keywords,
                severity=severity,
                ingested_at_utc=now_utc,
            )
            candidates.append(alert)

        # Deduplication
        if self.db_manager is not None:
            seen_ids = self.db_manager.get_seen_alert_ids()
            new_alerts = [a for a in candidates if a.alert_id not in seen_ids]
            if new_alerts:
                self.db_manager.insert_news_alerts(new_alerts)
            return new_alerts
        else:
            new_alerts = [a for a in candidates if a.alert_id not in self._seen_alert_ids]
            for a in new_alerts:
                self._seen_alert_ids.add(a.alert_id)
            return new_alerts
