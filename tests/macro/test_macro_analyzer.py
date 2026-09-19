"""Unit tests for macroeconomic calendar analysis and surprise engine (Group B)."""

from datetime import UTC, date, datetime
from typing import Any

from bitcoin_data_platform.macro.macro_analyzer import MacroAnalyzer
from bitcoin_data_platform.macro.models import MacroEconomicRelease
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def test_cpi_surprise_calculation() -> None:
    """11. Actual > Forecast yields Hawkish negative score (inflation higher than expected)."""
    analyzer = MacroAnalyzer(scale_cpi=0.4)
    # Actual 0.5%, Forecast 0.3% -> surprise = +0.2% -> Hawkish negative score
    release = analyzer.analyze_event(
        {
            "title": "CPI m/m",
            "country": "USD",
            "impact": "High",
            "scheduled_utc": "2026-09-18T12:30:00Z",
            "forecast": "0.3%",
            "actual": "0.5%",
            "previous": "0.2%",
        }
    )

    assert release.surprise_delta == 0.2
    assert release.directional_score < 0.0
    # -(0.2 / 0.4) = -0.5
    assert release.directional_score == -0.5


def test_cpi_dovish_surprise() -> None:
    """12. Actual < Forecast yields Dovish positive score (disinflation)."""
    analyzer = MacroAnalyzer(scale_cpi=0.4)
    # Actual 0.1%, Forecast 0.3% -> surprise = -0.2% -> Dovish positive score
    release = analyzer.analyze_event(
        {
            "title": "Core CPI m/m",
            "country": "USD",
            "impact": "High",
            "scheduled_utc": "2026-09-18T12:30:00Z",
            "forecast": "0.3%",
            "actual": "0.1%",
            "previous": "0.3%",
        }
    )

    assert release.surprise_delta == -0.2
    assert release.directional_score > 0.0
    assert release.directional_score == 0.5


def test_nfp_surprise_mapping() -> None:
    """13. Tests job addition surprises to liquidity impact."""
    analyzer = MacroAnalyzer(scale_nfp=100.0)
    # Strong NFP (250K actual vs 150K forecast) -> delays rate cuts -> negative score
    rel_hawkish = analyzer.analyze_event(
        {
            "title": "Non-Farm Employment Change",
            "country": "USD",
            "impact": "High",
            "scheduled_utc": "2026-09-10T12:30:00Z",
            "forecast": "150K",
            "actual": "250K",
            "previous": "140K",
        }
    )
    assert rel_hawkish.surprise_delta == 100.0
    assert rel_hawkish.directional_score == -1.0

    # Weak NFP (50K actual vs 150K forecast) -> Dovish stimulus -> positive score
    rel_dovish = analyzer.analyze_event(
        {
            "title": "Non-Farm Employment Change",
            "country": "USD",
            "impact": "High",
            "scheduled_utc": "2026-09-10T12:30:00Z",
            "forecast": "150K",
            "actual": "50K",
            "previous": "140K",
        }
    )
    assert rel_dovish.surprise_delta == -100.0
    assert rel_dovish.directional_score == 1.0


def test_fomc_rate_hike_cut() -> None:
    """14. Tests direct directional score for FOMC rate decision."""
    analyzer = MacroAnalyzer()

    # Rate cut -> Dovish (+1.0)
    rel_cut = analyzer.analyze_event(
        {
            "title": "Fed Funds Rate Decision",
            "country": "USD",
            "impact": "High",
            "scheduled_utc": "2026-09-15T18:00:00Z",
            "forecast": "5.25%",
            "actual": "5.00%",
            "previous": "5.25%",
        }
    )
    assert rel_cut.directional_score == 1.0

    # Rate hike -> Hawkish (-1.0)
    rel_hike = analyzer.analyze_event(
        {
            "title": "Fed Funds Rate",
            "country": "USD",
            "impact": "High",
            "scheduled_utc": "2026-09-15T18:00:00Z",
            "forecast": "5.25%",
            "actual": "5.50%",
            "previous": "5.25%",
        }
    )
    assert rel_hike.directional_score == -1.0


def test_macro_time_decay_72h() -> None:
    """15. Verifies older events decay exponentially over 72h window."""
    analyzer = MacroAnalyzer(rolling_window_hours=72.0)
    now_utc = datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC)

    # Event 1 hour ago: strong weight
    fresh_release = MacroEconomicRelease(
        release_id="rel_fresh",
        event_name="CPI",
        country="USD",
        release_date=date(2026, 9, 19),
        release_time_utc="11:00",
        impact="High",
        actual_value=0.5,
        forecast_value=0.3,
        surprise_delta=0.2,
        directional_score=-1.0,
    )

    # Event 48 hours ago: decayed weight (48h is two 24h half-lives, weight ~ 0.25)
    old_release = MacroEconomicRelease(
        release_id="rel_old",
        event_name="PPI",
        country="USD",
        release_date=date(2026, 9, 17),
        release_time_utc="11:00",
        impact="High",
        actual_value=0.1,
        forecast_value=0.3,
        surprise_delta=-0.2,
        directional_score=+1.0,
    )

    score = analyzer.compute_hard_macro_score([fresh_release, old_release], now_utc=now_utc)
    # The fresh negative event should heavily dominate the older positive event
    assert score < 0.0


def test_macro_missing_actual_value() -> None:
    """16. Scheduled upcoming events produce zero surprise and zero score."""
    analyzer = MacroAnalyzer()
    release = analyzer.analyze_event(
        {
            "title": "Upcoming FOMC Statement",
            "country": "USD",
            "impact": "High",
            "scheduled_utc": "2026-09-25T18:00:00Z",
            "forecast": "5.00%",
            "actual": None,
            "previous": "5.25%",
        }
    )

    assert release.actual_value is None
    assert release.surprise_delta is None
    assert release.directional_score == 0.0


def test_macro_calendar_empty_graceful() -> None:
    """17. Handles empty calendar gracefully returning 0.0."""
    analyzer = MacroAnalyzer()
    assert analyzer.compute_hard_macro_score([]) == 0.0


def test_macro_release_deduplication(tmp_path: Any) -> None:
    """18. Confirms idempotent database insertion for identical release_id."""
    db_file = tmp_path / "test_macro_dedup.duckdb"
    db_mgr = DuckDBManager(db_path=db_file)

    release = MacroEconomicRelease(
        release_id="cpi_unique_id",
        event_name="Core CPI m/m",
        country="USD",
        release_date=date(2026, 9, 18),
        release_time_utc="12:30",
        impact="High",
        actual_value=0.3,
        forecast_value=0.2,
        previous_value=0.2,
        surprise_delta=0.1,
        directional_score=-0.5,
    )

    with db_mgr:
        db_mgr.create_macro_tables()
        count1 = db_mgr.insert_macro_economic_releases([release])
        count2 = db_mgr.insert_macro_economic_releases([release])
        releases = db_mgr.get_macro_economic_releases(days=30)

    assert count1 == 1
    assert count2 == 1
    assert len(releases) == 1
    assert releases[0]["release_id"] == "cpi_unique_id"
