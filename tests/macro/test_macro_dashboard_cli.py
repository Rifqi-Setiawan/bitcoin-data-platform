"""Integration tests for Macro Radar Dashboard API endpoints and CLI commands (Group F)."""

import json
import threading
import urllib.request
from collections.abc import Generator
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.dashboard.server import DashboardServer, create_dashboard_server
from bitcoin_data_platform.macro.models import (
    DailyNarrativeReport,
    MacroArticle,
    MacroEconomicRelease,
    MacroPillar,
    MacroRegime,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def populated_dashboard(tmp_path: Path) -> Generator[tuple[DashboardServer, str, Path], None, None]:
    """Start an ephemeral DashboardServer with populated Macro tables."""
    db_file = tmp_path / "test_populated_macro.duckdb"
    db_mgr = DuckDBManager(db_path=db_file)
    with db_mgr:
        db_mgr.create_macro_tables()

        # 1. Insert report
        report = DailyNarrativeReport(
            intelligence_date=date(2026, 9, 19),
            synthesized_at_utc=datetime(2026, 9, 19, 10, 0, 0, tzinfo=UTC),
            hard_macro_score=0.25,
            sentiment_score=0.58,
            narrative_score=0.40,
            composite_mni=0.42,
            regime=MacroRegime.CAUTIOUS_BULL,
            black_swan_flag=False,
            active_critical_alerts=0,
            dominant_pillar=MacroPillar.INSTITUTIONAL,
            narrative_summary_id="Pasar kondusif pasca rilis CPI moderat.",
        )
        db_mgr.insert_daily_narrative_intelligence(report)

        # 2. Insert news article with verifiable url
        article = MacroArticle(
            article_id="art_12345",
            source="CoinDesk",
            title="SEC Approves In-Kind Creation for Spot Bitcoin ETFs",
            url="https://www.coindesk.com/policy/2026/09/19/sec-etf-in-kind-ruling",
            published_utc=datetime(2026, 9, 19, 7, 15, 0, tzinfo=UTC),
            summary="The SEC has finalized rules allowing physical Bitcoin redemption.",
            pillar=MacroPillar.REGULATORY,
            polarity=0.85,
        )
        db_mgr.insert_macro_articles([article])

        # 3. Insert calendar release
        release = MacroEconomicRelease(
            release_id="cpi_2026_09",
            event_name="Core CPI m/m",
            country="USD",
            release_date=date(2026, 9, 18),
            release_time_utc="12:30",
            impact="High",
            actual_value=0.2,
            forecast_value=0.3,
            previous_value=0.2,
            surprise_delta=-0.1,
            directional_score=0.5,
        )
        db_mgr.insert_macro_economic_releases([release])

    server = create_dashboard_server(host="127.0.0.1", port=0, db_path=db_file)
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield server, base_url, db_file

    server.shutdown()
    server.server_close()
    thread.join(timeout=2.0)


@pytest.fixture
def empty_dashboard(tmp_path: Path) -> Generator[tuple[DashboardServer, str, Path], None, None]:
    """Start ephemeral server with empty unpopulated database."""
    db_file = tmp_path / "test_empty_macro.duckdb"
    server = create_dashboard_server(host="127.0.0.1", port=0, db_path=db_file)
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield server, base_url, db_file

    server.shutdown()
    server.server_close()
    thread.join(timeout=2.0)


def test_api_macro_radar_endpoint(
    populated_dashboard: tuple[DashboardServer, str, Path],
) -> None:
    """41. Returns valid JSON matching schema from /api/macro/radar."""
    _, base_url, _ = populated_dashboard
    req = urllib.request.Request(f"{base_url}/api/macro/radar")
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))

    assert data["composite_mni"] == 0.42
    assert data["regime"] == "CAUTIOUS_BULL"
    assert "Cautious Bull" in data["regime_label"]
    assert data["black_swan_flag"] is False
    assert data["scores"]["hard_macro"] == 0.25
    assert data["scores"]["sentiment"] == 0.58
    assert data["scores"]["narrative"] == 0.40
    assert data["dominant_pillar"] == "INSTITUTIONAL"
    assert "Pasar kondusif" in data["narrative_summary"]


def test_api_macro_news_endpoint_with_urls(
    populated_dashboard: tuple[DashboardServer, str, Path],
) -> None:
    """42. Verifies all returned news items have valid original URLs."""
    _, base_url, _ = populated_dashboard
    req = urllib.request.Request(f"{base_url}/api/macro/news?limit=10")
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        assert resp.status == 200
        items = json.loads(resp.read().decode("utf-8"))

    assert len(items) == 1
    item = items[0]
    assert item["source"] == "CoinDesk"
    assert item["url"].startswith("https://www.coindesk.com")
    assert item["pillar"] == "REGULATORY"
    assert item["polarity"] == 0.85


def test_api_macro_calendar_endpoint(
    populated_dashboard: tuple[DashboardServer, str, Path],
) -> None:
    """43. Returns list of economic events with surprise evaluations."""
    _, base_url, _ = populated_dashboard
    req = urllib.request.Request(f"{base_url}/api/macro/calendar?days=7")
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        assert resp.status == 200
        events = json.loads(resp.read().decode("utf-8"))

    assert len(events) == 1
    evt = events[0]
    assert evt["event_name"] == "Core CPI m/m"
    assert evt["country"] == "USD"
    assert evt["impact"] == "High"
    assert evt["surprise"] == -0.1
    assert evt["directional_bias"] == "DOVISH"


def test_api_fallback_when_db_empty(
    empty_dashboard: tuple[DashboardServer, str, Path],
) -> None:
    """44. Endpoints return resilient fallbacks if DB unpopulated."""
    _, base_url, _ = empty_dashboard

    # Radar returns fallback 200
    with urllib.request.urlopen(f"{base_url}/api/macro/radar", timeout=3.0) as resp:
        assert resp.status == 200
        radar = json.loads(resp.read().decode("utf-8"))
        assert radar["regime"] == "NEUTRAL_CHOP"
        assert radar["composite_mni"] == 0.0

    # News returns empty list 200
    with urllib.request.urlopen(f"{base_url}/api/macro/news", timeout=3.0) as resp:
        assert resp.status == 200
        news = json.loads(resp.read().decode("utf-8"))
        assert news == []

    # Calendar returns empty list 200
    with urllib.request.urlopen(f"{base_url}/api/macro/calendar", timeout=3.0) as resp:
        assert resp.status == 200
        cal = json.loads(resp.read().decode("utf-8"))
        assert cal == []


def test_cli_macro_fetch_news(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """45. Executes fetch-news command successfully."""
    db_file = tmp_path / "cli_news.duckdb"
    with patch(
        "bitcoin_data_platform.macro.feed_ingester.FeedIngester.fetch_all_feeds"
    ) as mock_fetch:
        mock_fetch.return_value = [
            MacroArticle(
                article_id="art_mock_1",
                source="CoinDesk",
                title="Spot ETF Volume Reaches New High",
                url="https://coindesk.com/etf-high",
                published_utc=datetime(2026, 9, 19, 8, 0, 0, tzinfo=UTC),
            )
        ]
        code = main(["macro", "fetch-news", "--db-path", str(db_file)])

    assert code == 0
    out = capsys.readouterr().out
    assert "Successfully ingested and classified 1 articles" in out


def test_cli_macro_synthesize(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """46. Executes daily synthesis from CLI and prints summary."""
    db_file = tmp_path / "cli_synth.duckdb"
    code = main(["macro", "synthesize", "--date", "2026-09-19", "--db-path", str(db_file)])
    assert code == 0
    out = capsys.readouterr().out
    assert "Synthesis Complete for 2026-09-19" in out
    assert "Composite MNI:" in out
    assert "Regime:" in out


def test_cli_macro_radar_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """47. Renders tabular terminal radar output."""
    db_file = tmp_path / "cli_radar.duckdb"
    # Run text radar
    code_text = main(["macro", "radar", "--db-path", str(db_file)])
    assert code_text == 0
    out_text = capsys.readouterr().out
    assert "MACRO RADAR INTELLIGENCE REPORT" in out_text
    assert "Composite MNI:" in out_text
    assert "Black Swan Alert:" in out_text

    # Run json radar
    code_json = main(["macro", "radar", "--format", "json", "--db-path", str(db_file)])
    assert code_json == 0
    out_json = capsys.readouterr().out
    parsed = json.loads(out_json)
    assert "composite_mni" in parsed
    assert "regime" in parsed
