"""Command-line interface for platform operational diagnostics and self-healing."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.diagnostics.collector import TelemetryCollector
from bitcoin_data_platform.diagnostics.healer import RemediationRunner
from bitcoin_data_platform.diagnostics.report import IncidentReportGenerator
from bitcoin_data_platform.diagnostics.triage import TriageEngine


def register_diagnostics_cli(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the 'diagnose' command in the top-level argument parser."""
    diag_parser = subparsers.add_parser(
        "diagnose",
        help="Perform automated operational diagnostics, incident triage, and self-healing.",
        description=(
            "Inspect system telemetry (DuckDB, Lakehouse catalog, disk, logs) using 100% "
            "read-only connections, classify incidents across standard taxonomy (INC-01..INC-06), "
            "and simulate or execute bounded remediation workflows."
        ),
    )
    diag_parser.add_argument(
        "--run-id",
        default=None,
        help="Target specific pipeline run ID for root-cause diagnosis.",
    )
    diag_parser.add_argument(
        "--auto-heal",
        action="store_true",
        default=False,
        help="Execute bounded self-healing remediation actions (default: False, dry-run only).",
    )
    diag_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Simulate remediation actions without state mutation (default: True).",
    )
    diag_parser.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Diagnostic output format (choices: text, json, markdown; default: text).",
    )
    diag_parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path to export structured incident report file or directory.",
    )
    diag_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    diag_parser.add_argument(
        "--curated-dir",
        default="./data/curated",
        help="Directory for curated Parquet partitions (default: ./data/curated).",
    )
    diag_parser.add_argument(
        "--raw-dir",
        default="./data/raw",
        help="Directory containing raw gzip JSON envelopes (default: ./data/raw).",
    )
    diag_parser.add_argument(
        "--catalog-dir",
        default="./data/lakehouse/catalog",
        help="Directory where Lakehouse SQLite catalog is stored.",
    )
    diag_parser.add_argument(
        "--state-file",
        default=None,
        help="Path to alert deduplication state file.",
    )


def handle_diagnostics_cli(
    args: argparse.Namespace,
    *,
    clock: Callable[[], datetime] | None = None,
) -> int:
    """Execute diagnostics workflow: collect -> triage -> heal -> report."""
    now_utc = clock() if clock is not None else datetime.now(UTC)
    now_utc = now_utc.replace(tzinfo=UTC) if now_utc.tzinfo is None else now_utc.astimezone(UTC)

    # Resolve dry-run vs auto-heal: auto-heal explicitly disables dry_run
    dry_run = not getattr(args, "auto_heal", False)

    # 1. Collect Telemetry (strictly read-only)
    collector = TelemetryCollector(
        db_path=args.db_path,
        curated_dir=args.curated_dir,
        lakehouse_catalog_dir=args.catalog_dir,
        alert_state_path=args.state_file,
        target_run_id=args.run_id,
        now_utc=now_utc,
    )
    bundle = collector.collect()

    # 2. Incident Triage
    triage_engine = TriageEngine(bundle, target_run_id=args.run_id, now_utc=now_utc)
    incidents, plans = triage_engine.triage()

    # 3. Remediation Execution / Simulation
    healer = RemediationRunner(
        db_path=args.db_path,
        curated_dir=args.curated_dir,
        raw_dir=args.raw_dir,
        lakehouse_catalog_dir=args.catalog_dir,
        dry_run=dry_run,
        now_utc=now_utc,
    )
    results = healer.execute_all(plans)

    # 4. Report Generation
    report_gen = IncidentReportGenerator(
        telemetry=bundle,
        incidents=incidents,
        remediation_plans=plans,
        remediation_results=results,
        target_run_id=args.run_id,
        now_utc=now_utc,
    )
    report = report_gen.generate()

    # 5. Persist Report if --output provided
    if args.output:
        fmt_for_file = (
            "markdown"
            if str(args.output).endswith((".md", ".markdown"))
            else ("json" if str(args.output).endswith(".json") else args.format)
        )
        saved_path = IncidentReportGenerator.write_report(report, args.output, fmt=fmt_for_file)
        sys.stderr.write(f"Incident report saved to: {saved_path}\n")

    # 6. Format and Output
    if args.format == "json":
        sys.stdout.write(report.to_json() + "\n")
    elif args.format == "markdown":
        sys.stdout.write(report.to_markdown() + "\n")
    else:
        # Default human-readable text format
        _render_text_summary(report, dry_run=dry_run)

    return 0


def _render_text_summary(report: Any, *, dry_run: bool) -> None:
    """Render structured terminal text summary."""
    status_str = "DEGRADED" if report.incidents else "HEALTHY"
    mode_str = "DRY-RUN (Simulated)" if dry_run else "AUTO-HEAL (Active)"

    lines: list[str] = [
        "=" * 80,
        "BITCOIN DATA PLATFORM — OPERATIONAL DIAGNOSTICS & TRIAGE",
        "=" * 80,
        f"Timestamp:    {report.generated_at_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"System State: {status_str}",
        f"Mode:         {mode_str}",
        f"Target Run:   {report.target_run_id or 'ALL'}",
        "",
        "[TELEMETRY SNAPSHOT]",
        f"  Watermark:    {report.telemetry_summary.get('watermark_utc') or 'None'}",
    ]

    wm_age = report.telemetry_summary.get("watermark_age_hours")
    lines.append(f"  Watermark Lag:{wm_age} hours" if wm_age is not None else "  Watermark Lag:N/A")
    is_locked = report.telemetry_summary.get("is_locked")
    lines.append(f"  Lock Status:  {'LOCKED' if is_locked else 'UNLOCKED'}")

    disk = report.telemetry_summary.get("disk", {})
    lines.append(
        f"  Disk Usage:   {disk.get('percent_used', 0)}% "
        f"({disk.get('free_gb', 0)} GB free of {disk.get('total_gb', 0)} GB)"
    )

    gaps = report.telemetry_summary.get("gaps", [])
    lines.append(f"  Hourly Gaps:  {len(gaps)} detected")
    lines.append(f"  Query Audit:  {len(report.executed_queries)} read-only SQL queries executed")
    lines.append("")

    lines.append("[ACTIVE INCIDENTS & TRIAGE]")
    if not report.incidents:
        lines.append("  No active incidents detected. All subsystems healthy.")
    else:
        lines.append(f"  Total Incidents: {len(report.incidents)}")
        for idx, inc in enumerate(report.incidents, 1):
            lines.append(f"  {idx}. [{inc.incident_code}] [{inc.severity}] {inc.title}")
            lines.append(f"     Scope:       {inc.affected_scope}")
            lines.append(f"     Root Cause:  {inc.root_cause}")
            lines.append(f"     Action:      {inc.recommended_action}")
            lines.append(f"     Runbook:     {inc.runbook_ref}")
    lines.append("")

    if report.remediation_plans:
        lines.append("[REMEDIATION WORKFLOWS]")
        for plan in report.remediation_plans:
            prefix = "[DRY-RUN]" if plan.dry_run else "[AUTO-HEAL]"
            lines.append(f"  - {prefix} {plan.action_type}: {plan.description}")

        if report.remediation_results:
            lines.append("  Execution Outcomes:")
            for res in report.remediation_results:
                st = "SUCCESS" if res.success else "FAILED"
                lines.append(f"    * [{st}] {res.action_type}: {res.message}")
        lines.append("")

    lines.append("=" * 80)
    sys.stdout.write("\n".join(lines) + "\n")
