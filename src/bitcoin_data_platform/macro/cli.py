"""CLI subcommands and handlers for Phase 16 Macro & Narrative Intelligence Engine."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.macro.feed_ingester import FeedIngester, FeedSourceUnavailableError
from bitcoin_data_platform.macro.macro_analyzer import MacroAnalyzer
from bitcoin_data_platform.macro.models import DailyNarrativeReport, MacroPillar, MacroRegime
from bitcoin_data_platform.macro.sentiment_analyzer import SentimentAnalyzer
from bitcoin_data_platform.macro.synthesizer import MacroNarrativeSynthesizer
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def register_macro_cli(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the 'macro' top-level subcommand and nested operations."""
    macro_parser = subparsers.add_parser(
        "macro",
        help="Macro & Narrative Intelligence Engine and Radar commands.",
        description=(
            "Ingest multi-source crypto RSS feeds, evaluate macroeconomic calendar surprises, "
            "synthesize 3-tier Macro-Narrative Index (MNI), and monitor black swan sentinels."
        ),
    )
    macro_subparsers = macro_parser.add_subparsers(
        dest="macro_command",
        title="macro intelligence subcommands",
        metavar="<subcommand>",
    )

    # 1. fetch-news
    news_parser = macro_subparsers.add_parser(
        "fetch-news",
        help="Ingest and classify articles from curated RSS feeds.",
    )
    news_parser.add_argument(
        "--sources",
        default=None,
        help="Comma-separated feed sources (e.g. CoinDesk,Cointelegraph,Decrypt).",
    )
    news_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 2. fetch-calendar
    cal_parser = macro_subparsers.add_parser(
        "fetch-calendar",
        help="Fetch economic calendar events and compute surprise deltas.",
    )
    cal_parser.add_argument(
        "--impact",
        default=None,
        help="Filter impact level (e.g. High,Medium).",
    )
    cal_parser.add_argument(
        "--country",
        default="USD",
        help="Currency/country code filter (default: USD).",
    )
    cal_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 3. synthesize
    synth_parser = macro_subparsers.add_parser(
        "synthesize",
        help="Execute 3-tier macro narrative synthesis and persist MNI report.",
    )
    synth_parser.add_argument(
        "--date",
        dest="target_date",
        default=None,
        help="Intelligence target date YYYY-MM-DD (default: current UTC date).",
    )
    synth_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 4. radar
    radar_parser = macro_subparsers.add_parser(
        "radar",
        help="Render terminal Macro Radar status and sentiment report.",
    )
    radar_parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    radar_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 5. status
    status_parser = macro_subparsers.add_parser(
        "status",
        help="Check health and table status of Macro & Narrative engine.",
    )
    status_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )


def handle_macro_cli(args: argparse.Namespace) -> int:
    """Route and execute 'macro' subcommands with distinct exit codes."""
    command = getattr(args, "macro_command", None)
    if not command:
        print("Error: Subcommand required. Run 'bitcoin-data macro --help'.", file=sys.stderr)
        return 2

    db_path = getattr(args, "db_path", "./data/state/platform.duckdb")

    try:
        if command == "fetch-news":
            return _cmd_fetch_news(args, db_path)
        if command == "fetch-calendar":
            return _cmd_fetch_calendar(args, db_path)
        if command == "synthesize":
            return _cmd_synthesize(args, db_path)
        if command == "radar":
            return _cmd_radar(args, db_path)
        if command == "status":
            return _cmd_status(args, db_path)
        print(f"Error: Unknown subcommand '{command}'", file=sys.stderr)
        return 2
    except FeedSourceUnavailableError as exc:
        print(f"Network error fetching feeds: {exc}", file=sys.stderr)
        return 3
    except ValueError as exc:
        print(f"Invalid input/configuration error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Database/execution error: {exc}", file=sys.stderr)
        return 5


def _cmd_fetch_news(args: argparse.Namespace, db_path: str) -> int:
    sources = args.sources.split(",") if getattr(args, "sources", None) else None
    ingester = FeedIngester()
    analyzer = SentimentAnalyzer()

    try:
        raw_articles = ingester.fetch_all_feeds(sources=sources)
    finally:
        ingester.close()

    classified = [analyzer.classify_article(a) for a in raw_articles]

    db_mgr = DuckDBManager(db_path=Path(db_path))
    with db_mgr:
        db_mgr.create_macro_tables()
        saved = db_mgr.insert_macro_articles(classified)

    print(f"Successfully ingested and classified {saved} articles from {len(classified)} items.")
    return 0


def _cmd_fetch_calendar(args: argparse.Namespace, db_path: str) -> int:
    country = getattr(args, "country", "USD")
    impact = getattr(args, "impact", None)
    analyzer = MacroAnalyzer()

    releases = analyzer.fetch_and_analyze_calendar(country_filter=country, impact_filter=impact)

    db_mgr = DuckDBManager(db_path=Path(db_path))
    with db_mgr:
        db_mgr.create_macro_tables()
        saved = db_mgr.insert_macro_economic_releases(releases)

    print(f"Successfully fetched and parsed {saved} macroeconomic releases.")
    return 0


def _cmd_synthesize(args: argparse.Namespace, db_path: str) -> int:
    target_date_str = getattr(args, "target_date", None)
    target_date: date | None = None
    if target_date_str:
        try:
            target_date = date.fromisoformat(target_date_str)
        except ValueError:
            print(
                f"Error: Invalid date format '{target_date_str}'. Expected YYYY-MM-DD.",
                file=sys.stderr,
            )
            return 2

    db_mgr = DuckDBManager(db_path=Path(db_path))
    with db_mgr:
        synthesizer = MacroNarrativeSynthesizer(db_manager=db_mgr)
        report = synthesizer.synthesize_from_db(target_date=target_date)

    print(f"Synthesis Complete for {report.intelligence_date}:")
    print(f"  Regime:          {report.regime.value}")
    print(f"  Composite MNI:   {report.composite_mni:+.2f}")
    print(f"  Hard Macro:      {report.hard_macro_score:+.2f}")
    print(f"  Sentiment Score: {report.sentiment_score:+.2f}")
    print(f"  Narrative Score: {report.narrative_score:+.2f}")
    print(f"  Black Swan Flag: {report.black_swan_flag}")
    print(f"  Narrative:       {report.narrative_summary_id}")
    return 0


def _cmd_radar(args: argparse.Namespace, db_path: str) -> int:
    out_format = getattr(args, "format", "text")
    db_mgr = DuckDBManager(db_path=Path(db_path))

    with db_mgr:
        report = db_mgr.get_latest_narrative_intelligence()

    if report is None:
        # Resilient default fallback
        now = datetime.now(UTC)
        report = DailyNarrativeReport(
            intelligence_date=now.date(),
            synthesized_at_utc=now,
            hard_macro_score=0.0,
            sentiment_score=0.0,
            narrative_score=0.0,
            composite_mni=0.0,
            regime=MacroRegime.NEUTRAL_CHOP,
            black_swan_flag=False,
            active_critical_alerts=0,
            dominant_pillar=MacroPillar.GENERAL,
            narrative_summary_id="Pasar konsolidasi netral tanpa katalis makro dominan.",
        )

    if out_format == "json":
        payload: dict[str, Any] = {
            "date": report.intelligence_date.isoformat(),
            "composite_mni": report.composite_mni,
            "regime": report.regime.value,
            "black_swan_flag": report.black_swan_flag,
            "scores": {
                "hard_macro": report.hard_macro_score,
                "sentiment": report.sentiment_score,
                "narrative": report.narrative_score,
            },
            "dominant_pillar": report.dominant_pillar.value,
            "critical_alerts_count": report.active_critical_alerts,
            "narrative_summary": report.narrative_summary_id,
            "last_updated_utc": report.synthesized_at_utc.isoformat(),
        }
        print(json.dumps(payload, indent=2))
        return 0

    # Text format
    print("=" * 68)
    print(f" MACRO RADAR INTELLIGENCE REPORT — {report.intelligence_date}")
    print("=" * 68)
    print(f" Regime:           {report.regime.value}")
    print(f" Composite MNI:    {report.composite_mni:+.2f} [-1.0 to +1.0]")
    print(f" Hard Macro:       {report.hard_macro_score:+.2f}")
    print(f" Market Sentiment: {report.sentiment_score:+.2f}")
    print(f" Narrative Score:  {report.narrative_score:+.2f}")
    print(f" Dominant Pillar:  {report.dominant_pillar.value}")
    print(f" Black Swan Alert: {'🚨 ACTIVE' if report.black_swan_flag else '✅ CLEAR'}")
    print("-" * 68)
    print(f" Narrative: {report.narrative_summary_id}")
    print("=" * 68)
    return 0


def _cmd_status(args: argparse.Namespace, db_path: str) -> int:
    db_mgr = DuckDBManager(db_path=Path(db_path))
    with db_mgr:
        db_mgr.create_macro_tables()
        articles = db_mgr.get_macro_articles(limit=5)
        releases = db_mgr.get_macro_economic_releases(days=7)
        report = db_mgr.get_latest_narrative_intelligence()

    print("Macro & Narrative Engine Status:")
    print(f"  Database Path:         {db_path}")
    print(f"  Ingested Articles:     {len(articles)} recent items queryable")
    print(f"  Economic Releases:     {len(releases)} events recorded")
    if report:
        print(f"  Latest Report Date:    {report.intelligence_date}")
        print(f"  Latest Regime:         {report.regime.value} (MNI {report.composite_mni:+.2f})")
        print(f"  Black Swan Sentinel:   {'TRIPPED' if report.black_swan_flag else 'PASS'}")
    else:
        print("  Latest Report:         None (Run 'bitcoin-data macro synthesize')")
    return 0
