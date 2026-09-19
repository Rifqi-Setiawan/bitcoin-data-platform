"""CLI subcommands and handlers for Phase 17 Automated Scheduling Pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

from bitcoin_data_platform.pipeline.lock_manager import ConcurrentRunLockError
from bitcoin_data_platform.pipeline.orchestrator import PipelineOrchestrator
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def register_pipeline_cli(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register 'pipeline' top-level subcommand and operations."""
    pipe_parser = subparsers.add_parser(
        "pipeline",
        help="Automated Scheduling Pipeline orchestration and run monitoring.",
        description=(
            "Execute and monitor hourly, daily, and weekly automated pipelines on single-host VPS, "
            "supervised by POSIX file locks."
        ),
    )
    pipe_subparsers = pipe_parser.add_subparsers(
        dest="pipeline_command",
        title="pipeline subcommands",
        metavar="<subcommand>",
    )

    # 1. run-hourly
    hourly_parser = pipe_subparsers.add_parser(
        "run-hourly",
        help="Execute hourly news ingestion and black swan sentinel scan (*:05 UTC).",
    )
    hourly_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 2. run-daily
    daily_parser = pipe_subparsers.add_parser(
        "run-daily",
        help="Execute daily pipeline: MNI, committee deliberation, paper step, memo (00:05 UTC).",
    )
    daily_parser.add_argument(
        "--date",
        default=None,
        help="Target trade date (YYYY-MM-DD). Defaults to current UTC date.",
    )
    daily_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 3. run-weekly
    weekly_parser = pipe_subparsers.add_parser(
        "run-weekly",
        help="Execute weekly portfolio drawdown and retrospective audit (Mon 01:00 UTC).",
    )
    weekly_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )

    # 4. status
    status_parser = pipe_subparsers.add_parser(
        "status",
        help="Inspect pipeline concurrency locks, timer states, and execution health.",
    )
    status_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file.",
    )


def handle_pipeline_command(args: argparse.Namespace) -> int:
    """Dispatch pipeline CLI commands with strict exit codes."""
    subcommand = getattr(args, "pipeline_command", None)
    if not subcommand:
        print("Error: missing pipeline subcommand. Use --help.", file=sys.stderr)
        return 2

    db_path = getattr(args, "db_path", "./data/state/platform.duckdb")
    repo_root = Path(".").resolve()

    try:
        with DuckDBManager(db_path) as db:
            db_p = Path(db_path)
            lock_dir = db_p.parent / "locks" if str(db_path) != ":memory:" else None
            orchestrator = PipelineOrchestrator(
                repo_root=repo_root, db_manager=db, lock_dir=lock_dir
            )

            if subcommand == "run-hourly":
                report = orchestrator.run_hourly()
                _print_report(report)
                return 0 if report.overall_status.value == "SUCCESS" else 1

            if subcommand == "run-daily":
                target_date = date.fromisoformat(args.date) if getattr(args, "date", None) else None
                report = orchestrator.run_daily(target_date=target_date)
                _print_report(report)
                return 0 if report.overall_status.value == "SUCCESS" else 1

            if subcommand == "run-weekly":
                report = orchestrator.run_weekly()
                _print_report(report)
                return 0 if report.overall_status.value == "SUCCESS" else 1

            if subcommand == "status":
                status = orchestrator.lock_mgr.get_status()
                print("=" * 60)
                print("⚙️  PIPELINE CONCURRENCY LOCKS & HEALTH STATUS")
                print("=" * 60)
                print(json.dumps(status, indent=2))
                return 0

            print(f"Unknown subcommand: {subcommand}", file=sys.stderr)
            return 2
    except ConcurrentRunLockError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 6
    except Exception as exc:
        print(f"Pipeline execution error: {exc}", file=sys.stderr)
        return 5


def _print_report(report: Any) -> None:
    print("=" * 70)
    print(f"⚡ PIPELINE RUN REPORT [{report.cadence.upper()}] — {report.overall_status.value}")
    print(f"Run ID:   {report.run_id}")
    print(f"Duration: {report.total_duration_seconds:.3f}s")
    print("-" * 70)
    for s in report.steps:
        is_succ = s.status.value == "SUCCESS"
        status_icon = "✅" if is_succ else ("❌" if s.status.value == "FAILED" else "⚠️")
        print(f"  {status_icon} {s.step_name:<36} {s.status.value:<10} ({s.duration_seconds:.3f}s)")
        if s.error_message:
            print(f"     Error: {s.error_message}")
        if s.metadata:
            meta_str = ", ".join(f"{k}={v}" for k, v in s.metadata.items())
            print(f"     Meta:  {meta_str}")
    print("=" * 70)
