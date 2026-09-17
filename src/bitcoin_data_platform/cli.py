"""Console entry point and CLI commands for Bitcoin Data Engineering Platform."""

import argparse
import contextlib
import json
import os
import sys
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from bitcoin_data_platform.infra import dispatch_failure_alert
from bitcoin_data_platform.quality import run_dataset_quality_checks
from bitcoin_data_platform.quality.checks import QualityCheckError
from bitcoin_data_platform.sources.coinbase_client import (
    CoinbaseClient,
    CoinbaseClientError,
    SourceUnavailableError,
)
from bitcoin_data_platform.sources.coinbase_contract import validate_candle_payload
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager, DuckDBManagerError
from bitcoin_data_platform.storage.parquet_writer import (
    ParquetStorageError,
    write_parquet_partitions,
)
from bitcoin_data_platform.storage.raw_writer import (
    StorageError,
    create_raw_envelope,
    write_raw_envelope,
)
from bitcoin_data_platform.time_range import (
    TimeRangeError,
    format_canonical_utc,
    parse_iso_utc,
    validate_hourly_boundary,
)
from bitcoin_data_platform.transforms.normalizer import normalize_envelopes
from bitcoin_data_platform.transforms.raw_reader import read_raw_envelopes
from bitcoin_data_platform.window_planner import WindowPlanningError, plan_backfill


def _log_event(level: str, event: str, **kwargs: Any) -> None:
    """Emit a structured JSON log message to stderr."""
    payload = {
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "level": level.upper(),
        "event": event,
        **kwargs,
    }
    sys.stderr.write(json.dumps(payload) + "\n")


def _resolve_clock(clock: Callable[[], datetime] | None) -> datetime:
    if clock is not None:
        return clock()
    override = os.environ.get("BITCOIN_DATA_OVERRIDE_NOW_UTC")
    if override:
        return parse_iso_utc(override, "BITCOIN_DATA_OVERRIDE_NOW_UTC")
    return datetime.now(UTC)


def _format_status_text(data: dict[str, Any]) -> str:
    """Format status dictionary as human-readable terminal table summary."""
    is_healthy = data.get("is_healthy", False)
    health_str = "HEALTHY" if is_healthy else "DEGRADED"
    is_locked = data.get("is_locked", False)
    lock_str = "LOCKED" if is_locked else "UNLOCKED"

    wm = data.get("watermark_utc") or "None"
    wm_age = data.get("watermark_age_hours")
    wm_age_str = f"{wm_age:.2f} hours" if wm_age is not None else "N/A"

    c_stats = data.get("curated_stats") or {}
    total_rows = c_stats.get("total_rows", 0)
    partitions = c_stats.get("partitions", 0)
    total_bytes = c_stats.get("total_size_bytes", 0)
    size_kb = round(total_bytes / 1024.0, 2)
    min_c = c_stats.get("min_candle_utc") or "N/A"
    max_c = c_stats.get("max_candle_utc") or "N/A"

    gaps = data.get("gaps") or []
    gap_count = len(gaps)

    disk = data.get("disk") or {}
    total_gb = disk.get("total_gb", 0.0)
    used_gb = disk.get("used_gb", 0.0)
    free_gb = disk.get("free_gb", 0.0)
    pct_used = disk.get("percent_used", 0.0)
    if disk.get("disk_critical"):
        disk_status = "CRITICAL (>80%)"
    elif disk.get("disk_warning"):
        disk_status = "WARNING (>70%)"
    else:
        disk_status = "OK"

    last_run = data.get("last_run") or {}
    last_run_str = (
        f"{last_run.get('run_id', 'N/A')} ({last_run.get('mode', 'N/A')}) - "
        f"{last_run.get('status', 'N/A')}"
        if last_run
        else "None"
    )

    recent_qc = data.get("quality_checks") or []
    qc_count = len(recent_qc)
    qc_failed = sum(1 for q in recent_qc if q.get("status") == "FAILED")
    qc_passed = sum(1 for q in recent_qc if q.get("status") == "PASSED")

    lines = [
        "=" * 60,
        "Bitcoin Data Platform - System Status & Health",
        "=" * 60,
        f"Health Status      : {health_str}",
        f"Lock Status        : {lock_str}",
        "-" * 60,
        "[ Watermark & Freshness ]",
        f"  Watermark UTC    : {wm}",
        f"  Watermark Age    : {wm_age_str}",
        "-" * 60,
        "[ Curated Dataset Stats ]",
        f"  Total Rows       : {total_rows}",
        f"  Partitions       : {partitions}",
        f"  Total Size       : {size_kb} KB",
        f"  Min Candle UTC   : {min_c}",
        f"  Max Candle UTC   : {max_c}",
        f"  Gaps Detected    : {gap_count}",
        "-" * 60,
        "[ Storage & Disk Utilization ]",
        f"  Total Space      : {total_gb:.2f} GB",
        f"  Used Space       : {used_gb:.2f} GB ({pct_used:.1f}%)",
        f"  Free Space       : {free_gb:.2f} GB",
        f"  Disk Status      : {disk_status}",
        "-" * 60,
        "[ Execution & Quality Summary ]",
        f"  Last Run         : {last_run_str}",
        f"  Quality Checks   : {qc_count} logged ({qc_passed} passed, {qc_failed} failed)",
        "=" * 60,
    ]
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bitcoin-data",
        description="Bitcoin Data Engineering Platform CLI",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        metavar="<command>",
    )

    # 1. plan-backfill command
    plan_parser = subparsers.add_parser(
        "plan-backfill",
        help="Plan deterministic backfill request windows for Coinbase Exchange BTC-USD.",
        description="Plan deterministic backfill request windows for Coinbase Exchange BTC-USD.",
    )
    plan_parser.add_argument(
        "--start",
        required=True,
        help="Start timestamp in ISO-8601 UTC (e.g. 2026-01-01T00:00:00Z).",
    )
    plan_parser.add_argument(
        "--end",
        required=True,
        help="End timestamp in ISO-8601 UTC (e.g. 2026-01-26T01:00:00Z).",
    )

    # 2. backfill command
    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Execute backfill: plan windows, fetch, validate, and write envelopes.",
        description="Execute backfill: plan windows, fetch, validate, and write envelopes.",
    )
    backfill_parser.add_argument(
        "--start",
        required=True,
        help="Start timestamp in ISO-8601 UTC (e.g. 2026-01-01T00:00:00Z).",
    )
    backfill_parser.add_argument(
        "--end",
        required=True,
        help="End timestamp in ISO-8601 UTC (e.g. 2026-01-02T00:00:00Z).",
    )
    backfill_parser.add_argument(
        "--output-dir",
        default="./data/raw",
        help="Directory where raw gzip JSON envelopes will be saved (default: ./data/raw).",
    )
    backfill_parser.add_argument(
        "--db-path",
        default=None,
        help=(
            "Path to DuckDB database file for run locking "
            "(default: <output-dir>/../state/platform.duckdb)."
        ),
    )

    # 3. promote command
    promote_parser = subparsers.add_parser(
        "promote",
        help="Promote raw envelopes to curated Parquet partitions and DuckDB analytical views.",
        description=(
            "Promote raw envelopes to curated Parquet partitions and DuckDB analytical views."
        ),
    )
    promote_parser.add_argument(
        "--raw-dir",
        default="./data/raw",
        help="Directory containing raw gzip JSON envelopes (default: ./data/raw).",
    )
    promote_parser.add_argument(
        "--curated-dir",
        default="./data/curated",
        help="Directory for curated Parquet partitions (default: ./data/curated).",
    )
    promote_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to DuckDB database file (default: ./data/state/platform.duckdb).",
    )

    # 4. query command
    query_parser = subparsers.add_parser(
        "query",
        help="Execute SQL query against DuckDB database and print JSON results.",
        description="Execute SQL query against DuckDB database and print JSON results.",
    )
    query_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    query_parser.add_argument(
        "--sql",
        required=True,
        help="SQL query to execute.",
    )

    # 5. incremental command
    incremental_parser = subparsers.add_parser(
        "incremental",
        help="Execute watermark-based incremental ingestion, promotion, and analytical update.",
        description=(
            "Execute watermark-based incremental ingestion, promotion, and analytical update."
        ),
    )
    incremental_parser.add_argument(
        "--raw-dir",
        default="./data/raw",
        help="Directory containing raw gzip JSON envelopes (default: ./data/raw).",
    )
    incremental_parser.add_argument(
        "--curated-dir",
        default="./data/curated",
        help="Directory for curated Parquet partitions (default: ./data/curated).",
    )
    incremental_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    incremental_parser.add_argument(
        "--overlap-hours",
        type=int,
        default=48,
        help="Overlap window in hours behind watermark (default: 48).",
    )

    # 6. status command
    status_parser = subparsers.add_parser(
        "status",
        help="Inspect platform state, watermark freshness, run history, and data gaps.",
        description="Inspect platform state, watermark freshness, run history, and data gaps.",
    )
    status_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    status_parser.add_argument(
        "--curated-dir",
        default="./data/curated",
        help="Directory for curated Parquet partitions (default: ./data/curated).",
    )
    status_parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="json",
        help="Output format: json (default) or text.",
    )
    status_parser.add_argument(
        "--check",
        action="store_true",
        default=False,
        help="Health check mode: exit 0 if healthy, exit 1 if degraded.",
    )

    # 7. repair command
    repair_parser = subparsers.add_parser(
        "repair",
        help="Rebuild curated Parquet layer from scratch without altering watermark.",
        description=(
            "Rebuild curated Parquet layer from scratch from raw envelopes without "
            "altering watermark."
        ),
    )
    repair_parser.add_argument(
        "--raw-dir",
        default="./data/raw",
        help="Directory containing raw gzip JSON envelopes (default: ./data/raw).",
    )
    repair_parser.add_argument(
        "--curated-dir",
        default="./data/curated",
        help="Directory for curated Parquet partitions (default: ./data/curated).",
    )
    repair_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    repair_parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force clear stale run lock older than 1 hour before repair execution.",
    )

    # 8. alert command
    alert_parser = subparsers.add_parser(
        "alert",
        help="Dispatch scrubbed, deduplicated failure alert for a systemd unit.",
        description="Dispatch scrubbed, deduplicated failure alert for a systemd unit.",
    )
    alert_parser.add_argument(
        "--failed-unit",
        required=True,
        help="Name of the failed systemd unit (e.g. bitcoin-data.service).",
    )
    alert_parser.add_argument(
        "--state-file",
        default=None,
        help=(
            "Path to alert deduplication state file "
            "(default: /srv/data/bitcoin-data-platform/state/alert_state.json)."
        ),
    )
    alert_parser.add_argument(
        "--message",
        default=None,
        help="Optional failure message text to scrub and alert.",
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
    client: CoinbaseClient | None = None,
) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]

    if not argv:
        parser.print_usage(sys.stderr)
        sys.stderr.write("error: a command is required\n")
        return 2

    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2

    if args.command == "plan-backfill":
        try:
            now_utc = _resolve_clock(clock)
            start_utc = parse_iso_utc(args.start, "--start")
            end_utc = parse_iso_utc(args.end, "--end")
            validate_hourly_boundary(start_utc, "--start")
            validate_hourly_boundary(end_utc, "--end")

            plan = plan_backfill(start_utc, end_utc, now_utc=now_utc)
            sys.stdout.write(plan.to_json())
            return 0
        except (TimeRangeError, WindowPlanningError) as exc:
            sys.stderr.write(f"error: {exc}\n")
            return 2
        except Exception as exc:
            sys.stderr.write(f"error: unexpected failure: {exc}\n")
            return 2

    if args.command == "backfill":
        try:
            now_utc = _resolve_clock(clock)
            start_utc = parse_iso_utc(args.start, "--start")
            end_utc = parse_iso_utc(args.end, "--end")
            validate_hourly_boundary(start_utc, "--start")
            validate_hourly_boundary(end_utc, "--end")

            plan = plan_backfill(start_utc, end_utc, now_utc=now_utc)
        except (TimeRangeError, WindowPlanningError) as exc:
            sys.stderr.write(f"error: {exc}\n")
            return 2
        except Exception as exc:
            sys.stderr.write(f"error: unexpected failure: {exc}\n")
            return 2

        run_id = str(uuid.uuid4())
        output_dir = Path(args.output_dir)
        db_path = (
            Path(args.db_path)
            if args.db_path is not None
            else output_dir.parent / "state" / "platform.duckdb"
        )
        db_manager = DuckDBManager(db_path=db_path, curated_dir=db_path.parent)
        db_manager.initialize()

        if not db_manager.acquire_lock(
            run_id=run_id,
            mode="backfill",
            started_at_utc=now_utc,
            requested_start_utc=start_utc,
            requested_end_utc=end_utc,
        ):
            _log_event("error", "concurrent_run_detected", run_id=run_id)
            sys.stderr.write("error: concurrent run detected\n")
            return 6

        windows_succeeded = 0
        windows_failed = 0
        candles_ingested = 0
        files_written: list[str] = []

        cb_client = client if client is not None else CoinbaseClient()
        owns_client = client is None

        _log_event(
            "info",
            "backfill_started",
            run_id=run_id,
            windows_planned=len(plan.windows),
            output_dir=str(output_dir),
        )

        exit_code = 0
        error_msg: str | None = None

        try:
            for window in plan.windows:
                _log_event(
                    "info",
                    "window_started",
                    run_id=run_id,
                    window_index=window.index,
                    start_utc=format_canonical_utc(window.start_utc),
                    end_utc=format_canonical_utc(window.end_utc),
                )
                try:
                    response = cb_client.fetch_candles(window.start_utc, window.end_utc)
                except SourceUnavailableError as exc:
                    windows_failed += 1
                    error_msg = f"Coinbase source unavailable: {exc}"
                    _log_event(
                        "error",
                        "source_unavailable",
                        run_id=run_id,
                        window_index=window.index,
                        error=str(exc),
                    )
                    exit_code = 3
                    break
                except CoinbaseClientError as exc:
                    windows_failed += 1
                    error_msg = f"Coinbase client error: {exc}"
                    _log_event(
                        "error",
                        "coinbase_error",
                        run_id=run_id,
                        window_index=window.index,
                        error=str(exc),
                    )
                    exit_code = 3
                    break

                validation = validate_candle_payload(response.raw_payload)
                if not validation.is_valid:
                    windows_failed += 1
                    error_msg = f"Contract violation: {'; '.join(validation.violations)}"
                    _log_event(
                        "error",
                        "contract_violation",
                        run_id=run_id,
                        window_index=window.index,
                        violations=validation.violations,
                    )
                    exit_code = 4
                    break

                try:
                    envelope = create_raw_envelope(
                        run_id=run_id,
                        product_id="BTC-USD",
                        granularity_seconds=3600,
                        start_utc=window.start_utc,
                        end_utc=window.end_utc,
                        retrieved_at_utc=response.retrieved_at_utc,
                        http_status=response.http_status,
                        payload=response.raw_payload,
                        provider_request_id=response.provider_request_id,
                    )
                    file_path = write_raw_envelope(output_dir, envelope)
                    files_written.append(str(file_path))
                    windows_succeeded += 1
                    candles_ingested += len(validation.valid_candles)
                    _log_event(
                        "info",
                        "window_persisted",
                        run_id=run_id,
                        window_index=window.index,
                        path=str(file_path),
                        candles=len(validation.valid_candles),
                    )
                except (StorageError, OSError) as exc:
                    windows_failed += 1
                    error_msg = f"Storage failure: {exc}"
                    _log_event(
                        "error",
                        "storage_failure",
                        run_id=run_id,
                        window_index=window.index,
                        error=str(exc),
                    )
                    exit_code = 5
                    break
        finally:
            if owns_client:
                cb_client.close()
            status_str = "SUCCEEDED" if exit_code == 0 else "FAILED"
            completed_t = _resolve_clock(clock)
            db_manager.release_lock(
                run_id=run_id,
                status=status_str,
                completed_at_utc=completed_t,
                error_message=error_msg,
            )

        summary = {
            "run_id": run_id,
            "status": "success" if exit_code == 0 else "failure",
            "requested_start_utc": format_canonical_utc(start_utc),
            "requested_end_utc": format_canonical_utc(end_utc),
            "windows_planned": len(plan.windows),
            "windows_succeeded": windows_succeeded,
            "windows_failed": windows_failed,
            "candles_ingested": candles_ingested,
            "output_dir": str(output_dir),
            "files_written": files_written,
        }
        sys.stdout.write(json.dumps(summary, indent=2) + "\n")

        if error_msg is not None:
            sys.stderr.write(f"error: {error_msg}\n")

        return exit_code

    if args.command == "promote":
        now_utc = _resolve_clock(clock)
        run_id = str(uuid.uuid4())
        raw_dir = Path(args.raw_dir)
        curated_dir = Path(args.curated_dir)
        db_path = Path(args.db_path)

        db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
        db_manager.initialize()

        if not db_manager.acquire_lock(run_id=run_id, mode="promote", started_at_utc=now_utc):
            _log_event("error", "concurrent_run_detected", run_id=run_id)
            sys.stderr.write("error: concurrent run detected\n")
            return 6

        _log_event(
            "info",
            "promote_started",
            run_id=run_id,
            raw_dir=str(raw_dir),
            curated_dir=str(curated_dir),
            db_path=str(db_path),
        )

        try:
            envelopes = read_raw_envelopes(raw_dir)
        except Exception as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            _log_event("error", "raw_read_failed", run_id=run_id, error=str(exc))
            sys.stderr.write(f"error: failed to read raw envelopes: {exc}\n")
            return 2

        if not envelopes:
            db_manager.release_lock(
                run_id=run_id,
                status="SUCCEEDED",
                completed_at_utc=_resolve_clock(clock),
            )
            sys.stderr.write(f"No raw envelopes found in {raw_dir}\n")
            summary = {
                "run_id": run_id,
                "status": "success",
                "raw_envelopes_read": 0,
                "rows_promoted": 0,
                "partitions_written": 0,
                "curated_dir": str(curated_dir),
                "db_path": str(db_path),
            }
            sys.stdout.write(json.dumps(summary, indent=2) + "\n")
            return 0

        # Step 2 & 3: Normalize and validate quality checks
        try:
            candles = normalize_envelopes(envelopes, now_utc=now_utc)
        except QualityCheckError as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            _log_event("error", "quality_failure", run_id=run_id, error=str(exc))
            sys.stderr.write(f"error: quality failure: {exc}\n")
            return 4
        except Exception as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            _log_event("error", "normalization_failure", run_id=run_id, error=str(exc))
            sys.stderr.write(f"error: normalization failure: {exc}\n")
            return 4

        # Step 3b: Run dataset quality checks
        req_start = min((e.start_utc for e in envelopes), default=None)
        req_end = max((e.end_utc for e in envelopes), default=None)
        quality_records = run_dataset_quality_checks(
            candles,
            run_id=run_id,
            requested_start=req_start,
            requested_end=req_end,
            evaluated_at_utc=now_utc,
        )
        try:
            db_manager.record_quality_checks(quality_records)
        except Exception as exc:
            _log_event("warn", "quality_checks_persist_failed", error=str(exc))

        blocking_failures = [
            r for r in quality_records if r.severity == "BLOCK" and r.status == "FAILED"
        ]
        if blocking_failures:
            failure_details = "; ".join(f"{r.rule_name}: {r.details}" for r in blocking_failures)
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=f"Dataset quality check violation: {failure_details}",
            )
            _log_event("error", "quality_check_blocked", run_id=run_id, details=failure_details)
            sys.stderr.write(f"error: quality check violation: {failure_details}\n")
            return 4

        warn_failures = [
            r for r in quality_records if r.severity == "WARN" and r.status == "FAILED"
        ]
        if warn_failures:
            warn_details = "; ".join(f"{r.rule_name}: {r.details}" for r in warn_failures)
            _log_event("warn", "quality_check_warning", run_id=run_id, details=warn_details)

        # Step 4: Write/merge Parquet partitions
        try:
            partitions = write_parquet_partitions(candles, curated_dir=curated_dir)
        except (ParquetStorageError, OSError) as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            _log_event("error", "storage_failure", run_id=run_id, error=str(exc))
            sys.stderr.write(f"error: storage failure: {exc}\n")
            return 5

        # Step 5 & 6: Initialize DuckDB views & update watermark & release lock
        try:
            db_manager.initialize()
            completed_at_utc = _resolve_clock(clock)

            # Set initial watermark or advance watermark
            if candles:
                max_candle_ts = max(c.candle_start_utc for c in candles)
                current_wm = db_manager.get_watermark()
                if current_wm is None or max_candle_ts > current_wm:
                    db_manager.set_watermark(max_candle_ts, run_id=run_id, now_utc=completed_at_utc)

            new_wm = db_manager.get_watermark()
            db_manager.release_lock(
                run_id=run_id,
                status="SUCCEEDED",
                completed_at_utc=completed_at_utc,
                rows_promoted=len(candles),
                partitions_written=len(partitions),
                raw_envelopes_read=len(envelopes),
                new_watermark_utc=new_wm,
                error_message=None,
            )
        except Exception as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            _log_event("error", "database_failure", run_id=run_id, error=str(exc))
            sys.stderr.write(f"error: database failure: {exc}\n")
            return 5

        _log_event(
            "info",
            "promote_completed",
            run_id=run_id,
            rows_promoted=len(candles),
            partitions_written=len(partitions),
        )

        summary = {
            "run_id": run_id,
            "status": "success",
            "raw_envelopes_read": len(envelopes),
            "rows_promoted": len(candles),
            "partitions_written": len(partitions),
            "curated_dir": str(curated_dir),
            "db_path": str(db_path),
        }
        sys.stdout.write(json.dumps(summary, indent=2) + "\n")
        return 0

    if args.command == "incremental":
        now_utc = _resolve_clock(clock)
        run_id = str(uuid.uuid4())
        raw_dir = Path(args.raw_dir)
        curated_dir = Path(args.curated_dir)
        db_path = Path(args.db_path)
        overlap_hours = int(args.overlap_hours)

        db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
        db_manager.initialize()

        # Step 1: Acquire run lock
        if not db_manager.acquire_lock(run_id=run_id, mode="incremental", started_at_utc=now_utc):
            _log_event("error", "concurrent_run_detected", run_id=run_id)
            sys.stderr.write("error: concurrent run detected\n")
            return 6

        # Step 2: Read current watermark
        watermark = db_manager.get_watermark()
        if watermark is None:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message="No watermark found. Run a backfill first.",
            )
            sys.stderr.write("error: No watermark found. Run a backfill first.\n")
            return 2

        # Step 3 & 4: Calculate interval
        start_utc = watermark - timedelta(hours=overlap_hours)
        end_utc = now_utc.replace(minute=0, second=0, microsecond=0)

        # Step 5: Check if nothing to fetch
        if start_utc >= end_utc:
            db_manager.release_lock(
                run_id=run_id,
                status="SUCCEEDED",
                completed_at_utc=_resolve_clock(clock),
                old_watermark_utc=watermark,
                new_watermark_utc=watermark,
            )
            sys.stderr.write("Nothing to fetch, data is fresh.\n")
            summary = {
                "run_id": run_id,
                "status": "success",
                "mode": "incremental",
                "message": "Nothing to fetch, data is fresh.",
                "old_watermark_utc": format_canonical_utc(watermark),
                "new_watermark_utc": format_canonical_utc(watermark),
                "windows_planned": 0,
                "windows_succeeded": 0,
                "windows_failed": 0,
                "candles_ingested": 0,
                "rows_promoted": 0,
                "partitions_written": 0,
                "raw_envelopes_read": 0,
            }
            sys.stdout.write(json.dumps(summary, indent=2) + "\n")
            return 0

        # Step 6: Plan windows
        try:
            plan = plan_backfill(start_utc, end_utc, now_utc=now_utc)
        except Exception as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: window planning failed: {exc}\n")
            return 2

        cb_client = client if client is not None else CoinbaseClient()
        owns_client = client is None
        windows_succeeded = 0
        windows_failed = 0
        candles_ingested = 0
        files_written = []
        exit_code = 0
        error_msg = None

        try:
            for window in plan.windows:
                try:
                    response = cb_client.fetch_candles(window.start_utc, window.end_utc)
                except (SourceUnavailableError, CoinbaseClientError) as exc:
                    windows_failed += 1
                    error_msg = f"Coinbase source unavailable: {exc}"
                    exit_code = 3
                    break

                validation = validate_candle_payload(response.raw_payload)
                if not validation.is_valid:
                    windows_failed += 1
                    error_msg = f"Contract violation: {'; '.join(validation.violations)}"
                    exit_code = 4
                    break

                try:
                    envelope = create_raw_envelope(
                        run_id=run_id,
                        product_id="BTC-USD",
                        granularity_seconds=3600,
                        start_utc=window.start_utc,
                        end_utc=window.end_utc,
                        retrieved_at_utc=response.retrieved_at_utc,
                        http_status=response.http_status,
                        payload=response.raw_payload,
                        provider_request_id=response.provider_request_id,
                    )
                    file_path = write_raw_envelope(raw_dir, envelope)
                    files_written.append(str(file_path))
                    windows_succeeded += 1
                    candles_ingested += len(validation.valid_candles)
                except (StorageError, OSError) as exc:
                    windows_failed += 1
                    error_msg = f"Storage failure: {exc}"
                    exit_code = 5
                    break
        finally:
            if owns_client:
                cb_client.close()

        if exit_code != 0:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                old_watermark_utc=watermark,
                new_watermark_utc=watermark,
                error_message=error_msg,
            )
            summary = {
                "run_id": run_id,
                "status": "failure",
                "mode": "incremental",
                "old_watermark_utc": format_canonical_utc(watermark),
                "new_watermark_utc": format_canonical_utc(watermark),
                "windows_planned": len(plan.windows),
                "windows_succeeded": windows_succeeded,
                "windows_failed": windows_failed,
                "candles_ingested": candles_ingested,
                "output_dir": str(raw_dir),
                "files_written": files_written,
            }
            sys.stdout.write(json.dumps(summary, indent=2) + "\n")
            if error_msg:
                sys.stderr.write(f"error: {error_msg}\n")
            return exit_code

        # Step 7: Read ALL raw envelopes from raw_dir
        try:
            envelopes = read_raw_envelopes(raw_dir)
        except Exception as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: failed to read raw envelopes: {exc}\n")
            return 2

        # Step 8: Normalize & quality check
        try:
            candles = normalize_envelopes(envelopes, now_utc=now_utc)
        except QualityCheckError as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: quality failure: {exc}\n")
            return 4
        except Exception as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: normalization failure: {exc}\n")
            return 4

        # Step 8b: Run dataset quality checks
        req_start = min((e.start_utc for e in envelopes), default=None)
        req_end = max((e.end_utc for e in envelopes), default=None)
        quality_records = run_dataset_quality_checks(
            candles,
            run_id=run_id,
            requested_start=req_start,
            requested_end=req_end,
            evaluated_at_utc=now_utc,
        )
        try:
            db_manager.record_quality_checks(quality_records)
        except Exception as exc:
            _log_event("warn", "quality_checks_persist_failed", error=str(exc))

        blocking_failures = [
            r for r in quality_records if r.severity == "BLOCK" and r.status == "FAILED"
        ]
        if blocking_failures:
            failure_details = "; ".join(f"{r.rule_name}: {r.details}" for r in blocking_failures)
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                old_watermark_utc=watermark,
                new_watermark_utc=watermark,
                error_message=f"Quality check violation: {failure_details}",
            )
            _log_event("error", "quality_check_blocked", run_id=run_id, details=failure_details)
            sys.stderr.write(f"error: quality check violation: {failure_details}\n")
            return 4

        warn_failures = [
            r for r in quality_records if r.severity == "WARN" and r.status == "FAILED"
        ]
        if warn_failures:
            warn_details = "; ".join(f"{r.rule_name}: {r.details}" for r in warn_failures)
            _log_event("warn", "quality_check_warning", run_id=run_id, details=warn_details)

        # Step 9: Write/merge Parquet partitions
        try:
            partitions = write_parquet_partitions(candles, curated_dir=curated_dir)
        except (ParquetStorageError, OSError) as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: storage failure: {exc}\n")
            return 5

        # Step 10: Update DuckDB views
        db_manager.initialize()

        # Step 11: Advance watermark
        max_ts = max(c.candle_start_utc for c in candles) if candles else watermark
        db_manager.set_watermark(max_ts, run_id=run_id, now_utc=_resolve_clock(clock))
        new_watermark = db_manager.get_watermark() or max_ts

        # Step 12: Release lock
        completed_at = _resolve_clock(clock)
        db_manager.release_lock(
            run_id=run_id,
            status="SUCCEEDED",
            completed_at_utc=completed_at,
            rows_promoted=len(candles),
            partitions_written=len(partitions),
            raw_envelopes_read=len(envelopes),
            new_watermark_utc=new_watermark,
        )

        summary = {
            "run_id": run_id,
            "status": "success",
            "mode": "incremental",
            "old_watermark_utc": format_canonical_utc(watermark),
            "new_watermark_utc": format_canonical_utc(new_watermark),
            "windows_planned": len(plan.windows),
            "windows_succeeded": windows_succeeded,
            "windows_failed": 0,
            "candles_ingested": candles_ingested,
            "raw_envelopes_read": len(envelopes),
            "rows_promoted": len(candles),
            "partitions_written": len(partitions),
            "curated_dir": str(curated_dir),
            "db_path": str(db_path),
        }
        sys.stdout.write(json.dumps(summary, indent=2) + "\n")
        return 0

    if args.command == "status":
        db_path = Path(args.db_path)
        curated_dir = Path(args.curated_dir)
        db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
        now_utc = _resolve_clock(clock)
        status_data = db_manager.get_status(now_utc=now_utc)

        output_format = getattr(args, "format", "json")
        if output_format == "text":
            sys.stdout.write(_format_status_text(status_data))
        else:
            sys.stdout.write(json.dumps(status_data, indent=2) + "\n")

        if getattr(args, "check", False):
            return 0 if status_data.get("is_healthy", False) else 1
        return 0

    if args.command == "alert":
        now_utc = _resolve_clock(clock)
        dispatch_failure_alert(
            failed_unit=args.failed_unit,
            state_file=args.state_file,
            message=args.message,
            now_utc=now_utc,
            output_stream=sys.stderr,
        )
        return 0

    if args.command == "repair":
        now_utc = _resolve_clock(clock)
        run_id = str(uuid.uuid4())
        raw_dir = Path(args.raw_dir)
        curated_dir = Path(args.curated_dir)
        db_path = Path(args.db_path)

        db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
        db_manager.initialize()

        if args.force:
            db_manager.force_clear_lock(stale_threshold_seconds=3600, now_utc=now_utc)

        if not db_manager.acquire_lock(run_id=run_id, mode="repair", started_at_utc=now_utc):
            _log_event("error", "concurrent_run_detected", run_id=run_id)
            sys.stderr.write("error: concurrent run detected\n")
            return 6

        try:
            envelopes = read_raw_envelopes(raw_dir)
        except Exception as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: failed to read raw envelopes: {exc}\n")
            return 2

        if not envelopes:
            db_manager.release_lock(
                run_id=run_id,
                status="SUCCEEDED",
                completed_at_utc=_resolve_clock(clock),
            )
            sys.stderr.write(f"No raw envelopes found in {raw_dir}\n")
            summary = {
                "run_id": run_id,
                "status": "success",
                "mode": "repair",
                "raw_envelopes_read": 0,
                "rows_promoted": 0,
                "partitions_written": 0,
                "curated_dir": str(curated_dir),
                "db_path": str(db_path),
            }
            sys.stdout.write(json.dumps(summary, indent=2) + "\n")
            return 0

        try:
            candles = normalize_envelopes(envelopes, now_utc=now_utc)
        except QualityCheckError as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: quality failure: {exc}\n")
            return 4
        except Exception as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: normalization failure: {exc}\n")
            return 4

        # Full rebuild: remove existing parquet files under curated_dir
        for p_file in curated_dir.glob("market/candles_hourly/source=*/year=*/*.parquet"):
            with contextlib.suppress(OSError):
                p_file.unlink()

        try:
            partitions = write_parquet_partitions(candles, curated_dir=curated_dir)
        except (ParquetStorageError, OSError) as exc:
            db_manager.release_lock(
                run_id=run_id,
                status="FAILED",
                completed_at_utc=_resolve_clock(clock),
                error_message=str(exc),
            )
            sys.stderr.write(f"error: storage failure: {exc}\n")
            return 5

        db_manager.initialize()

        # Do NOT change watermark (preserve monotonicity)
        completed_at = _resolve_clock(clock)
        db_manager.release_lock(
            run_id=run_id,
            status="SUCCEEDED",
            completed_at_utc=completed_at,
            rows_promoted=len(candles),
            partitions_written=len(partitions),
            raw_envelopes_read=len(envelopes),
        )

        summary = {
            "run_id": run_id,
            "status": "success",
            "mode": "repair",
            "raw_envelopes_read": len(envelopes),
            "rows_promoted": len(candles),
            "partitions_written": len(partitions),
            "curated_dir": str(curated_dir),
            "db_path": str(db_path),
        }
        sys.stdout.write(json.dumps(summary, indent=2) + "\n")
        return 0

    if args.command == "query":
        db_path = Path(args.db_path)
        if not db_path.exists():
            sys.stderr.write(f"error: database file not found at {db_path}\n")
            return 2

        db_manager = DuckDBManager(db_path=db_path, curated_dir=db_path.parent)
        try:
            with db_manager:
                results = db_manager.execute_query(args.sql)
                sys.stdout.write(json.dumps(results, default=str, indent=2) + "\n")
                return 0
        except DuckDBManagerError as exc:
            sys.stderr.write(f"error: {exc}\n")
            return 2
        except Exception as exc:
            sys.stderr.write(f"error: query failed: {exc}\n")
            return 2

    sys.stderr.write(f"error: unrecognized command: {args.command}\n")
    return 2


if __name__ == "__main__":
    sys.exit(main())
