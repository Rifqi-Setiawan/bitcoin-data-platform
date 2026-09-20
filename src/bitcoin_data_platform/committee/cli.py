"""CLI subcommands and handlers for Phase 17 Investment Committee."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime

from bitcoin_data_platform.committee.engine import InvestmentCommitteeEngine
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def register_committee_cli(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register 'committee' top-level subcommand and operations."""
    comm_parser = subparsers.add_parser(
        "committee",
        help="Agentic Investment Committee deliberation and memorandum commands.",
        description=(
            "Coordinate multi-persona AI investment deliberations, enforce neuro-symbolic "
            "pre-trade invariants, render institutional memorandums, and monitor consensus."
        ),
    )
    comm_subparsers = comm_parser.add_subparsers(
        dest="committee_command",
        title="committee subcommands",
        metavar="<subcommand>",
    )

    is_testing = bool(os.getenv("PYTEST_CURRENT_TEST"))
    default_provider = "mock" if is_testing else "9router"

    # 1. deliberate
    delib_parser = comm_subparsers.add_parser(
        "deliberate",
        help="Deliberate daily investment session and synthesize memorandum.",
    )
    delib_parser.add_argument(
        "--date",
        default=None,
        help="Target trade date (YYYY-MM-DD). Defaults to today UTC.",
    )
    delib_parser.add_argument(
        "--provider",
        choices=["mock", "hermes", "openai", "9router"],
        default=default_provider,
        help="LLM provider architecture (default: 9router).",
    )
    delib_parser.add_argument(
        "--model",
        default="cx/gpt-5.6-sol",
        help="Model name for neural reasoning (default: cx/gpt-5.6-sol).",
    )
    delib_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute deliberation session without writing memorandum to DuckDB.",
    )
    delib_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 2. memo
    memo_parser = comm_subparsers.add_parser(
        "memo",
        help="Display investment memorandum for a target date or latest.",
    )
    memo_parser.add_argument(
        "--date",
        default=None,
        help="Target trade date (YYYY-MM-DD). Defaults to latest memorandum.",
    )
    memo_parser.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Output format: text, json, or markdown.",
    )
    memo_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 3. status
    status_parser = comm_subparsers.add_parser(
        "status",
        help="Audit committee operational status, latest consensus, and active invariants.",
    )
    status_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )


def handle_committee_command(args: argparse.Namespace) -> int:
    """Dispatch committee CLI commands to appropriate handlers."""
    subcommand = getattr(args, "committee_command", None)
    if not subcommand:
        print("Error: missing committee subcommand. Use --help.", file=sys.stderr)
        return 2

    db_path = getattr(args, "db_path", "./data/state/platform.duckdb")

    try:
        if subcommand == "deliberate":
            return _handle_deliberate(args, db_path)
        if subcommand == "memo":
            return _handle_memo(args, db_path)
        if subcommand == "status":
            return _handle_status(args, db_path)
        print(f"Unknown subcommand: {subcommand}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Committee operational error: {exc}", file=sys.stderr)
        return 5


def _handle_deliberate(args: argparse.Namespace, db_path: str) -> int:
    target_date = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()

    with DuckDBManager(db_path) as db:
        db.initialize()
        use_llm = getattr(args, "provider", "9router") != "mock"
        from bitcoin_data_platform.committee.llm_reasoner import CommitteeLLMReasoner

        llm = (
            CommitteeLLMReasoner(model=getattr(args, "model", "cx/gpt-5.6-sol"), enabled=use_llm)
            if use_llm
            else None
        )
        engine = InvestmentCommitteeEngine(db, llm_client=llm, use_llm=use_llm)
        memo = engine.deliberate(target_date=target_date, dry_run=args.dry_run)

        print("=" * 70)
        print(f"🏛️  INVESTMENT COMMITTEE DELIBERATION — {memo.memo_date.isoformat()}")
        print("=" * 70)
        print(f"Status Aksi:      {memo.proposed_action.value}")
        print(f"Skor Konsensus:   {memo.consensus_score:+.2f}")
        clamp_tag = "[CLAMPED]" if memo.allocation_clamped else "[PASSED]"
        print(f"Alokasi Neural:   ${memo.proposed_allocation_usd:,.2f}")
        print(f"Alokasi Simbolik: ${memo.clamped_allocation_usd:,.2f} {clamp_tag}")
        if memo.allocation_clamped:
            print(f"Alasan Clamping:  {memo.clamping_reason}")
        print("\nRingkasan Eksekutif (Bahasa Indonesia):")
        print(f"> {memo.executive_summary_id}\n")
        print("Suara Persona Komite:")
        for v in memo.votes:
            p_val = v.persona.value
            s_val = v.stance.value
            target_usd = v.target_allocation_usd
            conf_pct = v.confidence
            print(f"  • {p_val:<18} {s_val:<14} ${target_usd:,.2f} (Conf: {conf_pct:.0%})")
        if args.dry_run:
            print("\n[DRY-RUN]: Memorandum tidak disimpan ke database.")
        else:
            print(f"\nSUCCESS: Memorandum [{memo.memo_id[:16]}] disimpan ke DuckDB.")
        return 0


def _handle_memo(args: argparse.Namespace, db_path: str) -> int:
    with DuckDBManager(db_path) as db:
        db.initialize()
        if args.date:
            target_date = date.fromisoformat(args.date)
            memo_dict = db.get_investment_memo_by_date(target_date)
        else:
            memo_dict = db.get_latest_investment_memo()

        if not memo_dict:
            target_desc = f"date {args.date}" if args.date else "latest"
            print(f"No memorandum found for {target_desc}.", file=sys.stderr)
            return 2

        if args.format == "json":
            print(json.dumps(memo_dict, indent=2))
            return 0
        if args.format == "markdown":
            print(memo_dict.get("memo_markdown", ""))
            return 0

        # Text format default
        print(f"Memorandum ID:    {memo_dict.get('memo_id')}")
        print(f"Tanggal:          {memo_dict.get('memo_date')}")
        print(f"Regime Makro:     {memo_dict.get('market_regime')}")
        print(f"Rekomendasi Aksi: {memo_dict.get('proposed_action')}")
        print(f"Skor Konsensus:   {memo_dict.get('consensus_score')}")
        print(f"Alokasi Diusulkan: ${memo_dict.get('proposed_allocation_usd', 0.0):,.2f}")
        print(f"Alokasi Eksekusi:  ${memo_dict.get('clamped_allocation_usd', 0.0):,.2f}")
        print(f"Clamped:          {memo_dict.get('allocation_clamped')}")
        print("\nRingkasan Eksekutif:")
        print(f"  {memo_dict.get('executive_summary_id')}\n")
        print("Tesis Analis:")
        print(f"  Makro:     {memo_dict.get('macro_thesis')}")
        print(f"  Valuasi:   {memo_dict.get('valuation_thesis')}")
        print(f"  Teknikal:  {memo_dict.get('technical_thesis')}")
        return 0


def _handle_status(args: argparse.Namespace, db_path: str) -> int:
    with DuckDBManager(db_path) as db:
        db.initialize()
        memo_dict = db.get_latest_investment_memo()
        print("=" * 60)
        print("🏛️  INVESTMENT COMMITTEE OPERATIONAL STATUS")
        print("=" * 60)
        if not memo_dict:
            print("Status: Database initialized. No deliberations recorded yet.")
            return 0

        print(f"Latest Deliberation:  {memo_dict.get('memo_date')}")
        print(f"Current Regime:       {memo_dict.get('market_regime')}")
        print(f"Latest Action:        {memo_dict.get('proposed_action')}")
        print(f"Consensus Score:      {memo_dict.get('consensus_score')}")
        print(f"Execution Allocation: ${memo_dict.get('clamped_allocation_usd', 0.0):,.2f}")
        print(f"RiskGuard Passed:     {memo_dict.get('risk_guard_passed')}")
        votes = memo_dict.get("votes", [])
        print(f"Active Personas:      {len(votes)}")
        for v in votes:
            print(f"  - {v.get('persona')}: {v.get('stance')} (Conf: {v.get('confidence')})")
        return 0
