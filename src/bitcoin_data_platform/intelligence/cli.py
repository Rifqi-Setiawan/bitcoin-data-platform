"""CLI subcommands and handlers for User Intelligence Ingestion."""

from __future__ import annotations

import argparse
import json
import sys

from bitcoin_data_platform.intelligence.ingester import IntelligenceIngester
from bitcoin_data_platform.intelligence.models import IntelligencePillar
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def register_intelligence_cli(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register 'intelligence' top-level subcommand and sub-operations."""
    intel_parser = subparsers.add_parser(
        "intelligence",
        help="User Intelligence Ingestion and Alpha Blotter commands.",
        description=(
            "Ingest user-provided research notes, macro hypotheses, external article URLs, "
            "and list active user alpha injected into the Investment Committee."
        ),
    )
    intel_subparsers = intel_parser.add_subparsers(
        dest="intelligence_command",
        title="intelligence subcommands",
        metavar="<subcommand>",
    )

    # 1. ingest
    ingest_parser = intel_subparsers.add_parser(
        "ingest",
        help="Ingest a research note, hypothesis, or external article URL.",
    )
    ingest_parser.add_argument(
        "--title",
        default="",
        help="Title or headline summary of the intelligence item.",
    )
    ingest_parser.add_argument(
        "--thesis",
        required=True,
        help="User analytical thesis or rationale (required).",
    )
    ingest_parser.add_argument(
        "--url",
        default=None,
        help="Optional external source URL to scrape and extract text from.",
    )
    ingest_parser.add_argument(
        "--pillar",
        default="USER_THESIS",
        choices=[p.value for p in IntelligencePillar],
        help="Intelligence pillar category (default: USER_THESIS).",
    )
    ingest_parser.add_argument(
        "--sentiment",
        type=float,
        default=0.0,
        help="Directional sentiment bias from -1.0 (bearish) to +1.0 (bullish).",
    )
    ingest_parser.add_argument(
        "--confidence",
        type=float,
        default=0.8,
        help="Confidence score from 0.0 to 1.0 (default: 0.8).",
    )
    ingest_parser.add_argument(
        "--tags",
        default="",
        help="Comma-separated tags (e.g. 'fed,rates,liquidity').",
    )
    ingest_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 2. list
    list_parser = intel_subparsers.add_parser(
        "list",
        help="List ingested user intelligence items.",
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum items to return (default: 20).",
    )
    list_parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format: table or json.",
    )
    list_parser.add_argument(
        "--all",
        action="store_true",
        help="Include inactive intelligence items.",
    )
    list_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )


def handle_intelligence_command(args: argparse.Namespace) -> int:
    """Dispatch intelligence CLI commands to appropriate handlers."""
    subcommand = getattr(args, "intelligence_command", None)
    if not subcommand:
        print("Error: missing intelligence subcommand. Use --help.", file=sys.stderr)
        return 2

    db_path = getattr(args, "db_path", "./data/state/platform.duckdb")

    try:
        if subcommand == "ingest":
            return _handle_ingest(args, db_path)
        if subcommand == "list":
            return _handle_list(args, db_path)
        print(f"Unknown subcommand: {subcommand}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Intelligence operation error: {exc}", file=sys.stderr)
        return 5


def _handle_ingest(args: argparse.Namespace, db_path: str) -> int:
    tags = [t.strip() for t in args.tags.split(",") if t.strip()] if args.tags else []
    try:
        with DuckDBManager(db_path) as db:
            db.initialize()
            ingester = IntelligenceIngester(db)
            record = ingester.ingest(
                title=args.title,
                user_thesis=args.thesis,
                source_url=args.url,
                pillar=args.pillar,
                sentiment_bias=args.sentiment,
                confidence_score=args.confidence,
                tags=tags,
            )
            print(f"SUCCESS: Ingested intelligence item [{record.intelligence_id}]")
            print(f"  Title:      {record.title}")
            print(f"  Pillar:     {record.pillar}")
            bias = record.sentiment_bias
            conf = record.confidence_score
            print(f"  Sentiment:  {bias:+.2f} (Confidence: {conf:.2f})")
            if record.source_url:
                print(f"  Source URL: {record.source_url}")
            print(f"  Thesis:     {record.user_thesis}")
            return 0
    except Exception as exc:
        print(f"Failed ingesting intelligence: {exc}", file=sys.stderr)
        return 5


def _handle_list(args: argparse.Namespace, db_path: str) -> int:
    try:
        with DuckDBManager(db_path) as db:
            db.initialize()
            ingester = IntelligenceIngester(db)
            items = ingester.list_intelligence(limit=args.limit, active_only=not args.all)

            if args.format == "json":
                print(json.dumps(items, indent=2))
                return 0

            if not items:
                print("No user intelligence items found.")
                return 0

            print(f"{'ID':<16} {'PILLAR':<18} {'SENT':<6} {'CONF':<6} {'DATE':<12} {'TITLE'}")
            print("-" * 80)
            for it in items:
                sent = f"{it.get('sentiment_bias', 0.0):+.2f}"
                conf = f"{it.get('confidence_score', 0.0):.2f}"
                date_str = str(it.get("created_at_utc", ""))[:10]
                title = str(it.get("title", ""))[:32]
                print(
                    f"{it.get('intelligence_id', ''):<16} "
                    f"{it.get('pillar', ''):<18} "
                    f"{sent:<6} {conf:<6} {date_str:<12} {title}"
                )
            return 0
    except Exception as exc:
        print(f"Failed listing intelligence: {exc}", file=sys.stderr)
        return 5
