"""Automated tests for Committee, User Intelligence, and Scheduling Dashboard APIs (Group F)."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Generator
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from bitcoin_data_platform.committee.models import (
    AllocationAction,
    CommitteePersona,
    InvestmentMemorandum,
    MemberStance,
    PersonaVote,
)
from bitcoin_data_platform.dashboard.server import (
    DashboardServer,
    create_dashboard_server,
)
from bitcoin_data_platform.intelligence.models import (
    IntelligencePillar,
    UserIntelligenceRecord,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def test_dashboard(
    tmp_path: Path,
) -> Generator[tuple[DashboardServer, str, DuckDBManager], None, None]:
    """Start an ephemeral DashboardServer with temporary DuckDB database."""
    db_file = tmp_path / "test_comm_dash.duckdb"
    db_mgr = DuckDBManager(db_file)
    with db_mgr:
        db_mgr.initialize()

    server = create_dashboard_server(
        host="127.0.0.1",
        port=0,
        db_path=db_file,
    )
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield server, base_url, db_mgr

    server.shutdown()
    server.server_close()
    thread.join(timeout=2.0)


# 43. test_api_committee_latest_endpoint
def test_api_committee_latest_endpoint(
    test_dashboard: tuple[DashboardServer, str, DuckDBManager],
) -> None:
    _, base_url, db_mgr = test_dashboard
    now = datetime.now(UTC)
    memo_id = "latest-memo-1"

    ms = CommitteePersona.MACRO_STRATEGIST
    bull = MemberStance.BULLISH
    vote = PersonaVote("v-1", memo_id, ms, bull, 25.0, 0.85, "Macro", now)
    memo = InvestmentMemorandum(
        memo_id=memo_id,
        memo_date=date(2026, 9, 19),
        created_at_utc=now,
        market_regime="CAUTIOUS_BULL",
        composite_mni=0.42,
        consensus_score=0.38,
        executive_summary_id="Ringkasan eksekutif tes.",
        macro_thesis="Macro OK",
        valuation_thesis="Valuation OK",
        technical_thesis="Tech OK",
        dissenting_opinions="None",
        proposed_action=AllocationAction.OPPORTUNISTIC_BUY,
        proposed_allocation_usd=35.0,
        clamped_allocation_usd=22.5,
        allocation_clamped=True,
        clamping_reason="DAILY_RESERVE_CAP: Clamped to 15%",
        risk_guard_passed=True,
        memo_markdown="# Memo",
        votes=[vote],
    )
    with db_mgr:
        db_mgr.insert_investment_memo(memo)

    url = f"{base_url}/api/committee/latest"
    with urllib.request.urlopen(url, timeout=3.0) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))

    assert data["memo_id"] == memo_id
    assert data["market_regime"] == "CAUTIOUS_BULL"
    assert data["proposed_action"] == "OPPORTUNISTIC_BUY"
    assert data["allocation_clamped"] is True
    assert len(data["votes"]) == 1
    assert "invariants_checked" in data


# 44. test_api_committee_history_endpoint
def test_api_committee_history_endpoint(
    test_dashboard: tuple[DashboardServer, str, DuckDBManager],
) -> None:
    _, base_url, db_mgr = test_dashboard
    now = datetime.now(UTC)

    with db_mgr:
        for i in range(3):
            memo = InvestmentMemorandum(
                memo_id=f"history-memo-{i}",
                memo_date=date(2026, 9, 17 + i),
                created_at_utc=now,
                market_regime="NEUTRAL_CHOP",
                composite_mni=0.0,
                consensus_score=0.0,
                executive_summary_id="Summary",
                macro_thesis="M",
                valuation_thesis="V",
                technical_thesis="T",
                dissenting_opinions="None",
                proposed_action=AllocationAction.STANDARD_DCA,
                proposed_allocation_usd=10.0,
                clamped_allocation_usd=10.0,
                allocation_clamped=False,
                clamping_reason=None,
                risk_guard_passed=True,
                memo_markdown="# Markdown",
                votes=[],
            )
            db_mgr.insert_investment_memo(memo)

    url = f"{base_url}/api/committee/history?limit=2"
    with urllib.request.urlopen(url, timeout=3.0) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))

    assert len(data) == 2
    assert data[0]["memo_id"] == "history-memo-2"


# 45. test_api_intelligence_ingest_endpoint
def test_api_intelligence_ingest_endpoint(
    test_dashboard: tuple[DashboardServer, str, DuckDBManager],
) -> None:
    _, base_url, _ = test_dashboard
    url = f"{base_url}/api/intelligence/ingest"
    payload = {
        "title": "Fed QT Slowdown",
        "user_thesis": "Federal Reserve memperlambat laju quantitative tightening.",
        "source_url": "https://example.com/fed-qt",
        "pillar": "MACRO_LIQUIDITY",
        "sentiment_bias": 0.65,
        "confidence_score": 0.85,
        "tags": ["fed", "qt", "liquidity"],
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=3.0) as resp:
        assert resp.status == 201
        data = json.loads(resp.read().decode("utf-8"))

    assert data["status"] == "SUCCESS"
    assert "intelligence_id" in data
    assert "recorded" in data["message"]


# 46. test_api_intelligence_ingest_validation
def test_api_intelligence_ingest_validation(
    test_dashboard: tuple[DashboardServer, str, DuckDBManager],
) -> None:
    _, base_url, _ = test_dashboard
    url = f"{base_url}/api/intelligence/ingest"
    # Missing required 'user_thesis'
    payload = {
        "title": "Invalid Payload Without Thesis",
        "pillar": "MACRO_LIQUIDITY",
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(req, timeout=3.0)

    assert excinfo.value.code == 400


# 47. test_api_intelligence_list_endpoint
def test_api_intelligence_list_endpoint(
    test_dashboard: tuple[DashboardServer, str, DuckDBManager],
) -> None:
    _, base_url, db_mgr = test_dashboard
    target_dt = datetime.now(UTC)

    with db_mgr:
        db_mgr.insert_user_intelligence(
            UserIntelligenceRecord(
                intelligence_id="intel-item-1",
                source_url="https://example.com/source",
                title="Institutional Inflow",
                user_thesis="ETF inflows meningkat.",
                raw_content="",
                pillar=IntelligencePillar.INSTITUTIONAL,
                sentiment_bias=0.75,
                confidence_score=0.90,
                tags=["etf", "inflow"],
                created_at_utc=target_dt,
                is_active=True,
            )
        )

    url = f"{base_url}/api/intelligence/list?limit=10"
    with urllib.request.urlopen(url, timeout=3.0) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))

    assert len(data) == 1
    assert data[0]["intelligence_id"] == "intel-item-1"
    assert data[0]["source_url"] == "https://example.com/source"


# 48. test_api_pipeline_schedule_endpoint
def test_api_pipeline_schedule_endpoint(
    test_dashboard: tuple[DashboardServer, str, DuckDBManager],
) -> None:
    _, base_url, _ = test_dashboard
    url = f"{base_url}/api/pipeline/schedule"
    with urllib.request.urlopen(url, timeout=3.0) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))

    assert "cadences" in data
    assert "hourly" in data["cadences"]
    assert "daily" in data["cadences"]
    assert "weekly" in data["cadences"]
    assert "locks" in data


# 49. test_api_resilient_fallback_empty_db
def test_api_resilient_fallback_empty_db(
    test_dashboard: tuple[DashboardServer, str, DuckDBManager],
) -> None:
    _, base_url, _ = test_dashboard
    # With unpopulated DB, /api/committee/latest should return 200 with safe defaults
    url = f"{base_url}/api/committee/latest"
    with urllib.request.urlopen(url, timeout=3.0) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))

    assert data["memo_id"] == "default-memo"
    assert data["proposed_action"] == "STANDARD_DCA"
    assert data["allocation_clamped"] is False
    assert len(data["invariants_checked"]) >= 5


# 50. test_dashboard_index_html_contains_committee_tab
def test_dashboard_index_html_contains_committee_tab() -> None:
    repo_root = Path(__file__).resolve().parent.parent.parent
    html_path = repo_root / "src" / "bitcoin_data_platform" / "dashboard" / "assets" / "index.html"
    assert html_path.is_file()

    content = html_path.read_text(encoding="utf-8")
    assert 'id="tabCommitteeBtn"' in content
    assert 'id="committeeView"' in content
    assert "Komite Investasi" in content
    assert "tabular" in content
    assert 'target="_blank"' in content
