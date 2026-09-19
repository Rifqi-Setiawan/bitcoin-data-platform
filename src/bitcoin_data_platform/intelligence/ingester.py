"""User intelligence ingestion engine with lightweight text extraction and SHA-256 deduplication."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence
from datetime import UTC, datetime
from html.parser import HTMLParser
from typing import Any

import httpx

from bitcoin_data_platform.intelligence.models import (
    IntelligencePillar,
    UserIntelligenceRecord,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


class SimpleHTMLTextExtractor(HTMLParser):
    """Lightweight stdlib HTML parser extracting title and paragraph content."""

    def __init__(self) -> None:
        super().__init__()
        self._title_tokens: list[str] = []
        self._body_tokens: list[str] = []
        self._in_title = False
        self._in_paragraph = False
        self._ignored_tags = {"script", "style", "nav", "footer", "header", "noscript"}
        self._in_ignored = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_lower = tag.lower()
        if tag_lower in self._ignored_tags:
            self._in_ignored += 1
        elif tag_lower == "title":
            self._in_title = True
        elif tag_lower == "p":
            self._in_paragraph = True

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if tag_lower in self._ignored_tags:
            self._in_ignored = max(0, self._in_ignored - 1)
        elif tag_lower == "title":
            self._in_title = False
        elif tag_lower == "p":
            self._in_paragraph = False

    def handle_data(self, data: str) -> None:
        if self._in_ignored > 0:
            return
        clean = data.strip()
        if not clean:
            return
        if self._in_title:
            self._title_tokens.append(clean)
        elif self._in_paragraph:
            self._body_tokens.append(clean)

    @property
    def title(self) -> str:
        return " ".join(self._title_tokens).strip()

    @property
    def content(self) -> str:
        return "\n\n".join(self._body_tokens).strip()


class IntelligenceIngester:
    """Ingests user research notes, macro hypotheses, and extracted web intelligence."""

    def __init__(
        self,
        db_manager: DuckDBManager,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.db = db_manager
        self.http_client = http_client or httpx.Client(
            timeout=10.0,
            follow_redirects=True,
            headers={"User-Agent": "BitcoinDataPlatform/1.0"},
        )

    def extract_article_content(self, url: str) -> tuple[str, str]:
        """Fetch article HTML and extract clean title and paragraph text."""
        try:
            resp = self.http_client.get(url)
            resp.raise_for_status()
            parser = SimpleHTMLTextExtractor()
            parser.feed(resp.text)
            return parser.title, parser.content[:4000]
        except Exception:
            return "", ""

    def ingest(
        self,
        *,
        title: str,
        user_thesis: str,
        source_url: str | None = None,
        pillar: IntelligencePillar | str = IntelligencePillar.USER_THESIS,
        sentiment_bias: float = 0.0,
        confidence_score: float = 0.8,
        tags: list[str] | None = None,
        created_at_utc: datetime | None = None,
    ) -> UserIntelligenceRecord:
        """Ingest, validate, deduplicate, and persist user market intelligence."""
        raw_now = created_at_utc or datetime.now(UTC)
        now = raw_now.replace(tzinfo=UTC) if raw_now.tzinfo is None else raw_now.astimezone(UTC)

        clean_tags = list(tags) if tags else []

        extracted_title = ""
        extracted_content = ""
        if source_url:
            extracted_title, extracted_content = self.extract_article_content(source_url)

        fallback_title = extracted_title or "User Market Intelligence Note"
        final_title = title.strip() if title and title.strip() else fallback_title

        resolved_pillar = (
            pillar
            if isinstance(pillar, IntelligencePillar)
            else IntelligencePillar(str(pillar).upper())
        )

        # Deterministic SHA-256 deduplication ID
        raw_key = f"{source_url or ''}:{user_thesis.strip()}:{now.date().isoformat()}"
        intel_id = hashlib.sha256(raw_key.encode()).hexdigest()[:16]

        clamped_sentiment = max(-1.0, min(1.0, float(sentiment_bias)))
        clamped_confidence = max(0.0, min(1.0, float(confidence_score)))

        record = UserIntelligenceRecord(
            intelligence_id=intel_id,
            source_url=source_url,
            title=final_title,
            user_thesis=user_thesis.strip(),
            raw_content=extracted_content,
            pillar=resolved_pillar,
            sentiment_bias=clamped_sentiment,
            confidence_score=clamped_confidence,
            tags=clean_tags,
            created_at_utc=now,
            is_active=True,
        )

        self.db.insert_user_intelligence(record)
        return record

    def list_intelligence(
        self,
        limit: int = 20,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """List ingested user market intelligence records."""
        return self.db.list_user_intelligence(limit=limit, active_only=active_only)

    @staticmethod
    def compute_decayed_user_score(
        records: Sequence[UserIntelligenceRecord | dict[str, Any]],
        as_of_utc: datetime | None = None,
    ) -> float:
        """Compute aggregate time-decayed User Intelligence Score S_user (24-hour half-life).

        Formula: S_user = sum(c_i * s_i * decay) / sum(c_i * decay + eps).
        where lambda = ln(2) / 24.
        """
        if not records:
            return 0.0

        ref_time = as_of_utc or datetime.now(UTC)
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=UTC)

        lam = math.log(2) / 24.0
        numerator = 0.0
        denominator = 0.0

        for r in records:
            if isinstance(r, UserIntelligenceRecord):
                sentiment = r.sentiment_bias
                confidence = r.confidence_score
                created = r.created_at_utc
                is_active = r.is_active
            else:
                sentiment = float(r.get("sentiment_bias", 0.0))
                confidence = float(r.get("confidence_score", 0.8))
                created_raw = r.get("created_at_utc")
                if isinstance(created_raw, datetime):
                    created = created_raw
                elif isinstance(created_raw, str):
                    created = datetime.fromisoformat(created_raw)
                else:
                    created = ref_time
                is_active = bool(r.get("is_active", True))

            if not is_active:
                continue

            if created.tzinfo is None:
                created = created.replace(tzinfo=UTC)

            age_hours = max(0.0, (ref_time - created).total_seconds() / 3600.0)
            decay_weight = confidence * math.exp(-lam * age_hours)

            numerator += decay_weight * sentiment
            denominator += decay_weight

        if denominator < 1e-9:
            return 0.0

        return round(numerator / denominator, 4)
