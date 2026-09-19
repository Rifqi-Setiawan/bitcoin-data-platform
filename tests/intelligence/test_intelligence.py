"""Automated tests for User Intelligence Ingestion Engine (Group A)."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

from bitcoin_data_platform.intelligence.ingester import IntelligenceIngester
from bitcoin_data_platform.intelligence.models import (
    IntelligencePillar,
    UserIntelligenceRecord,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def tmp_duckdb(tmp_path: Path) -> DuckDBManager:
    """Fixture providing a clean temporary DuckDB instance."""
    db_file = tmp_path / "test_intel.duckdb"
    mgr = DuckDBManager(db_file)
    with mgr:
        mgr.initialize()
    return mgr


# 1. test_user_intelligence_creation
def test_user_intelligence_creation(tmp_duckdb: DuckDBManager) -> None:
    ingester = IntelligenceIngester(tmp_duckdb)
    record = ingester.ingest(
        title="Fed Policy Pivot",
        user_thesis="Fed memperlambat QT, meningkatkan likuiditas pasar modal.",
        pillar=IntelligencePillar.MACRO_LIQUIDITY,
        sentiment_bias=0.65,
        confidence_score=0.85,
        tags=["fed", "liquidity"],
    )
    assert record.intelligence_id is not None
    assert record.title == "Fed Policy Pivot"
    assert record.pillar == IntelligencePillar.MACRO_LIQUIDITY
    assert record.sentiment_bias == 0.65
    assert record.confidence_score == 0.85
    assert "fed" in record.tags
    assert record.is_active is True


# 2. test_intelligence_sha256_deduplication
def test_intelligence_sha256_deduplication(tmp_duckdb: DuckDBManager) -> None:
    ingester = IntelligenceIngester(tmp_duckdb)
    fixed_time = datetime(2026, 9, 19, 10, 0, tzinfo=UTC)

    rec1 = ingester.ingest(
        title="First Title",
        user_thesis="Identical thesis content for deduplication test",
        source_url="https://example.com/source1",
        created_at_utc=fixed_time,
    )
    rec2 = ingester.ingest(
        title="Updated Title",
        user_thesis="Identical thesis content for deduplication test",
        source_url="https://example.com/source1",
        created_at_utc=fixed_time,
    )

    assert rec1.intelligence_id == rec2.intelligence_id
    items = ingester.list_intelligence()
    assert len(items) == 1
    assert items[0]["title"] == "Updated Title"


# 3. test_article_text_extraction_mocked
def test_article_text_extraction_mocked(tmp_duckdb: DuckDBManager) -> None:
    mock_html = """
    <!DOCTYPE html>
    <html>
      <head><title>Central Bank Liquidity Inflow</title></head>
      <body>
        <nav>Menu item</nav>
        <p>The Federal Reserve has officially announced a moderation in quantitative tightening.</p>
        <p>Market participants expect positive liquidity transmission into Bitcoin markets.</p>
        <script>console.log('ignore me');</script>
      </body>
    </html>
    """
    mock_client = MagicMock(spec=httpx.Client)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = mock_html
    mock_resp.raise_for_status.return_value = None
    mock_client.get.return_value = mock_resp

    ingester = IntelligenceIngester(tmp_duckdb, http_client=mock_client)
    record = ingester.ingest(
        title="",  # Empty to trigger auto-extraction
        user_thesis="Riset pelonggaran likuiditas bank sentral.",
        source_url="https://example.com/liquidity",
    )

    assert "Central Bank Liquidity Inflow" in record.title
    assert "Federal Reserve has officially announced" in record.raw_content
    assert "Bitcoin markets" in record.raw_content
    assert "console.log" not in record.raw_content


# 4. test_article_fetch_failure_graceful_fallback
def test_article_fetch_failure_graceful_fallback(tmp_duckdb: DuckDBManager) -> None:
    mock_client = MagicMock(spec=httpx.Client)
    mock_client.get.side_effect = httpx.HTTPError("404 Not Found")

    ingester = IntelligenceIngester(tmp_duckdb, http_client=mock_client)
    record = ingester.ingest(
        title="Custom User Title",
        user_thesis="Catatan analisis pribadi saat tautan offline.",
        source_url="https://example.com/broken-link",
    )

    assert record.title == "Custom User Title"
    assert record.raw_content == ""
    assert record.user_thesis == "Catatan analisis pribadi saat tautan offline."


# 5. test_user_intelligence_sentiment_bounds
def test_user_intelligence_sentiment_bounds(tmp_duckdb: DuckDBManager) -> None:
    ingester = IntelligenceIngester(tmp_duckdb)

    # Exceeding upper bound
    rec_high = ingester.ingest(
        title="Overly Bullish",
        user_thesis="Sentimen melampaui batas atas.",
        sentiment_bias=3.5,
        confidence_score=1.8,
    )
    assert rec_high.sentiment_bias == 1.0
    assert rec_high.confidence_score == 1.0

    # Exceeding lower bound
    rec_low = ingester.ingest(
        title="Overly Bearish",
        user_thesis="Sentimen melampaui batas bawah.",
        sentiment_bias=-2.7,
        confidence_score=-0.5,
    )
    assert rec_low.sentiment_bias == -1.0
    assert rec_low.confidence_score == 0.0


# 6. test_user_intelligence_duckdb_persistence
def test_user_intelligence_duckdb_persistence(tmp_duckdb: DuckDBManager) -> None:
    ingester = IntelligenceIngester(tmp_duckdb)
    record = ingester.ingest(
        title="Regulatory Clarity in EU",
        user_thesis="Implementasi MiCA berjalan mulus.",
        pillar=IntelligencePillar.REGULATORY,
        sentiment_bias=0.40,
        confidence_score=0.90,
        tags=["mica", "eu", "regulation"],
    )

    with tmp_duckdb:
        rows = tmp_duckdb.execute_query(
            f"SELECT title, pillar, sentiment_bias, tags FROM user_market_intelligence "
            f"WHERE intelligence_id = '{record.intelligence_id}'"
        )
    assert len(rows) == 1
    assert rows[0]["title"] == "Regulatory Clarity in EU"
    assert rows[0]["pillar"] == "REGULATORY"
    assert rows[0]["sentiment_bias"] == 0.40
    assert "mica" in rows[0]["tags"]


# 7. test_intelligence_time_decay_weighting
def test_intelligence_time_decay_weighting() -> None:
    now = datetime(2026, 9, 19, 12, 0, tzinfo=UTC)

    # Item 1: Brand new (0h old), bullish +1.0, confidence 1.0 -> weight = 1.0
    rec_fresh = UserIntelligenceRecord(
        intelligence_id="fresh",
        source_url=None,
        title="Fresh Bullish News",
        user_thesis="Baru dirilis",
        raw_content="",
        pillar=IntelligencePillar.INSTITUTIONAL,
        sentiment_bias=1.0,
        confidence_score=1.0,
        tags=[],
        created_at_utc=now,
    )

    # Item 2: 24h old (half-life), bearish -1.0, confidence 1.0 -> weight = 0.5
    rec_old = UserIntelligenceRecord(
        intelligence_id="old",
        source_url=None,
        title="Old Bearish News",
        user_thesis="Dirilis kemarin",
        raw_content="",
        pillar=IntelligencePillar.MACRO_LIQUIDITY,
        sentiment_bias=-1.0,
        confidence_score=1.0,
        tags=[],
        created_at_utc=now - timedelta(hours=24),
    )

    score = IntelligenceIngester.compute_decayed_user_score([rec_fresh, rec_old], as_of_utc=now)
    # Expected: (1.0*1.0 + 0.5*(-1.0)) / (1.0 + 0.5) = 0.5 / 1.5 = +0.3333
    expected = (1.0 * 1.0 + 0.5 * (-1.0)) / (1.0 + 0.5)
    assert math.isclose(score, expected, abs_tol=0.01)


# 8. test_intelligence_listing_filtering
def test_intelligence_listing_filtering(tmp_duckdb: DuckDBManager) -> None:
    ingester = IntelligenceIngester(tmp_duckdb)
    for i in range(5):
        ingester.ingest(
            title=f"Note {i}",
            user_thesis=f"User thesis iteration {i}",
            confidence_score=0.7,
        )

    all_items = ingester.list_intelligence(limit=10)
    assert len(all_items) == 5

    capped_items = ingester.list_intelligence(limit=3)
    assert len(capped_items) == 3
