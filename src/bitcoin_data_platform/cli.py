"""Console entry point and CLI commands for Bitcoin Data Engineering Platform."""

import argparse
import contextlib
import json
import os
import sys
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from bitcoin_data_platform.infra import dispatch_failure_alert
from bitcoin_data_platform.quality import run_dataset_quality_checks
from bitcoin_data_platform.quality.checks import QualityCheckError
from bitcoin_data_platform.serving import (
    execute_parameterized_query,
    export_arrow_table,
    load_query_file,
)
from bitcoin_data_platform.sources.coin_metrics_client import (
    CoinMetricsClient,
    CoinMetricsClientError,
    CoinMetricsHTTPError,
)
from bitcoin_data_platform.sources.coin_metrics_client import (
    SourceUnavailableError as CoinMetricsSourceUnavailableError,
)
from bitcoin_data_platform.sources.coin_metrics_contract import (
    CoinMetricsContractViolationError,
    validate_coin_metrics_payload,
)
from bitcoin_data_platform.sources.coinbase_client import (
    CoinbaseClient,
    CoinbaseClientError,
    SourceUnavailableError,
)
from bitcoin_data_platform.sources.coinbase_contract import validate_candle_payload
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager, DuckDBManagerError
from bitcoin_data_platform.storage.network_parquet_writer import (
    NetworkParquetStorageError,
    write_network_parquet_partitions,
)
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
from bitcoin_data_platform.transforms.network_normalizer import (
    create_network_raw_envelope,
    normalize_network_envelopes,
    read_network_raw_envelopes,
    write_network_raw_envelope,
)
from bitcoin_data_platform.transforms.normalizer import normalize_envelopes
from bitcoin_data_platform.transforms.raw_reader import read_raw_envelopes
from bitcoin_data_platform.window_planner import (
    PlannedWindow,
    WindowPlanningError,
    plan_backfill,
)


def _parse_cli_date(val: str, field_name: str) -> datetime:
    """Parse date or timestamp string into UTC datetime."""
    raw = val.strip()
    if len(raw) == 10 and raw.count("-") == 2:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError as exc:
            raise TimeRangeError(f"Invalid date format for {field_name}: {raw}") from exc
    return parse_iso_utc(raw, field_name)


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


@dataclass
class RunLockContext:
    """Context holding active pipeline run state for automatic lock management."""

    db_manager: DuckDBManager
    run_id: str
    clock: Callable[[], datetime] | None = None
    status: str = "SUCCEEDED"
    rows_promoted: int = 0
    partitions_written: int = 0
    raw_envelopes_read: int = 0
    old_watermark_utc: datetime | None = None
    new_watermark_utc: datetime | None = None
    error_message: str | None = None
    released: bool = False

    def release(
        self,
        *,
        status: str | None = None,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> None:
        if self.released:
            return
        if status is not None:
            self.status = status
        if error_message is not None:
            self.error_message = error_message
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.db_manager.release_lock(
            run_id=self.run_id,
            status=self.status,
            completed_at_utc=_resolve_clock(self.clock),
            rows_promoted=self.rows_promoted,
            partitions_written=self.partitions_written,
            raw_envelopes_read=self.raw_envelopes_read,
            old_watermark_utc=self.old_watermark_utc,
            new_watermark_utc=self.new_watermark_utc,
            error_message=self.error_message,
        )
        self.released = True


@contextlib.contextmanager
def _managed_run_lock(
    db_manager: DuckDBManager,
    run_id: str,
    mode: str,
    started_at_utc: datetime,
    *,
    clock: Callable[[], datetime] | None = None,
    requested_start_utc: datetime | None = None,
    requested_end_utc: datetime | None = None,
    old_watermark_utc: datetime | None = None,
) -> Iterator[RunLockContext | None]:
    """Context manager acquiring run lock on enter and reliably releasing on exit."""
    if not db_manager.acquire_lock(
        run_id=run_id,
        mode=mode,
        started_at_utc=started_at_utc,
        requested_start_utc=requested_start_utc,
        requested_end_utc=requested_end_utc,
        old_watermark_utc=old_watermark_utc,
    ):
        _log_event("error", "concurrent_run_detected", run_id=run_id)
        sys.stderr.write("error: concurrent run detected\n")
        yield None
        return

    ctx = RunLockContext(
        db_manager=db_manager,
        run_id=run_id,
        clock=clock,
        old_watermark_utc=old_watermark_utc,
    )
    try:
        yield ctx
    except Exception as exc:
        ctx.status = "FAILED"
        ctx.error_message = str(exc)
        raise
    finally:
        ctx.release()


def _ingest_windows(
    cb_client: CoinbaseClient,
    windows: Sequence[PlannedWindow],
    run_id: str,
    output_dir: Path,
) -> tuple[int, int, int, int, list[str], str | None]:
    """Ingest candle windows sequentially, writing raw envelopes.

    Returns:
        (exit_code, windows_succeeded, windows_failed, candles_ingested, files_written, error_msg)
    """
    windows_succeeded = 0
    windows_failed = 0
    candles_ingested = 0
    files_written: list[str] = []
    exit_code = 0
    error_msg: str | None = None

    for window in windows:
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
        except (SourceUnavailableError, CoinbaseClientError) as exc:
            windows_failed += 1
            is_unavailable = isinstance(exc, SourceUnavailableError)
            prefix = "Coinbase source unavailable" if is_unavailable else "Coinbase client error"
            event = "source_unavailable" if is_unavailable else "coinbase_error"
            error_msg = f"{prefix}: {exc}"
            _log_event("error", event, run_id=run_id, window_index=window.index, error=str(exc))
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
                "error", "storage_failure", run_id=run_id, window_index=window.index, error=str(exc)
            )
            exit_code = 5
            break

    return (
        exit_code,
        windows_succeeded,
        windows_failed,
        candles_ingested,
        files_written,
        error_msg,
    )


def _promote_market_data(
    raw_dir: Path,
    curated_dir: Path,
    db_manager: DuckDBManager,
    run_id: str,
    now_utc: datetime,
    *,
    clock: Callable[[], datetime] | None = None,
    update_watermark: bool = True,
    clear_existing: bool = False,
) -> tuple[int, str | None, int, int, int]:
    """Promote raw envelopes to curated Parquet partitions and refresh DuckDB views.

    Returns:
        (exit_code, error_msg, envelopes_count, rows_promoted, partitions_count)
    """
    try:
        envelopes = read_raw_envelopes(raw_dir)
    except Exception as exc:
        _log_event("error", "raw_read_failed", run_id=run_id, error=str(exc))
        sys.stderr.write(f"error: failed to read raw envelopes: {exc}\n")
        return 2, str(exc), 0, 0, 0

    if not envelopes:
        return 0, None, 0, 0, 0

    try:
        candles = normalize_envelopes(envelopes, now_utc=now_utc)
    except QualityCheckError as exc:
        _log_event("error", "quality_failure", run_id=run_id, error=str(exc))
        sys.stderr.write(f"error: quality failure: {exc}\n")
        return 4, str(exc), len(envelopes), 0, 0
    except Exception as exc:
        _log_event("error", "normalization_failure", run_id=run_id, error=str(exc))
        sys.stderr.write(f"error: normalization failure: {exc}\n")
        return 4, str(exc), len(envelopes), 0, 0

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
        _log_event("error", "quality_check_blocked", run_id=run_id, details=failure_details)
        sys.stderr.write(f"error: quality check violation: {failure_details}\n")
        return 4, f"Dataset quality check violation: {failure_details}", len(envelopes), 0, 0

    warn_failures = [r for r in quality_records if r.severity == "WARN" and r.status == "FAILED"]
    if warn_failures:
        warn_details = "; ".join(f"{r.rule_name}: {r.details}" for r in warn_failures)
        _log_event("warn", "quality_check_warning", run_id=run_id, details=warn_details)

    if clear_existing:
        for p_file in curated_dir.glob("market/candles_hourly/source=*/year=*/*.parquet"):
            with contextlib.suppress(OSError):
                p_file.unlink()

    try:
        partitions = write_parquet_partitions(candles, curated_dir=curated_dir)
    except (ParquetStorageError, OSError) as exc:
        _log_event("error", "storage_failure", run_id=run_id, error=str(exc))
        sys.stderr.write(f"error: storage failure: {exc}\n")
        return 5, str(exc), len(envelopes), 0, 0

    try:
        db_manager.initialize()
        completed_at_utc = _resolve_clock(clock)
        if update_watermark and candles:
            max_candle_ts = max(c.candle_start_utc for c in candles)
            current_wm = db_manager.get_watermark()
            if current_wm is None or max_candle_ts > current_wm:
                db_manager.set_watermark(max_candle_ts, run_id=run_id, now_utc=completed_at_utc)
    except Exception as exc:
        _log_event("error", "database_failure", run_id=run_id, error=str(exc))
        sys.stderr.write(f"error: database failure: {exc}\n")
        return 5, str(exc), len(envelopes), len(candles), len(partitions)

    return 0, None, len(envelopes), len(candles), len(partitions)


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

    base_db_parser = argparse.ArgumentParser(add_help=False)
    base_db_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to DuckDB database file (default: ./data/state/platform.duckdb).",
    )

    base_storage_parser = argparse.ArgumentParser(add_help=False)
    base_storage_parser.add_argument(
        "--raw-dir",
        default="./data/raw",
        help="Directory containing raw gzip JSON envelopes (default: ./data/raw).",
    )
    base_storage_parser.add_argument(
        "--curated-dir",
        default="./data/curated",
        help="Directory for curated Parquet partitions (default: ./data/curated).",
    )
    base_storage_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to DuckDB database file (default: ./data/state/platform.duckdb).",
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
    subparsers.add_parser(
        "promote",
        parents=[base_storage_parser],
        help="Promote raw envelopes to curated Parquet partitions and DuckDB analytical views.",
        description=(
            "Promote raw envelopes to curated Parquet partitions and DuckDB analytical views."
        ),
    )

    # 4. query command
    query_parser = subparsers.add_parser(
        "query",
        parents=[base_db_parser],
        help="Execute SQL query against DuckDB database and export results.",
        description=(
            "Execute SQL query against DuckDB database and export results in multiple formats."
        ),
    )
    query_source_group = query_parser.add_mutually_exclusive_group(required=True)
    query_source_group.add_argument(
        "--sql",
        help="SQL query string to execute.",
    )
    query_source_group.add_argument(
        "--file",
        type=Path,
        help="Path to SQL query file to execute.",
    )
    query_parser.add_argument(
        "--param",
        action="append",
        default=[],
        help="Query parameter in KEY=VALUE format (can be specified multiple times).",
    )
    query_parser.add_argument(
        "--format",
        choices=["json", "csv", "parquet", "arrow"],
        default="json",
        help="Export format (choices: json, csv, parquet, arrow; default: json).",
    )
    query_parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path for exported data.",
    )

    # 5. incremental command
    incremental_parser = subparsers.add_parser(
        "incremental",
        parents=[base_storage_parser],
        help="Execute watermark-based incremental ingestion, promotion, and analytical update.",
        description=(
            "Execute watermark-based incremental ingestion, promotion, and analytical update."
        ),
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
        parents=[base_db_parser],
        help="Inspect platform state, watermark freshness, run history, and data gaps.",
        description="Inspect platform state, watermark freshness, run history, and data gaps.",
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
        parents=[base_storage_parser],
        help="Rebuild curated Parquet layer from scratch without altering watermark.",
        description=(
            "Rebuild curated Parquet layer from scratch from raw envelopes without "
            "altering watermark."
        ),
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

    # 9. fetch-network command
    fetch_net_parser = subparsers.add_parser(
        "fetch-network",
        help="Fetch daily on-chain network metrics from Coin Metrics Community API v4.",
        description=(
            "Fetch daily on-chain network metrics from Coin Metrics Community API v4 "
            "and persist raw checksummed gzip JSON envelopes."
        ),
    )
    fetch_net_parser.add_argument(
        "--start",
        required=True,
        help="Start date in YYYY-MM-DD or ISO-8601 UTC (e.g. 2026-01-01).",
    )
    fetch_net_parser.add_argument(
        "--end",
        required=True,
        help="End date in YYYY-MM-DD or ISO-8601 UTC (e.g. 2026-01-07).",
    )
    fetch_net_parser.add_argument(
        "--output-dir",
        default="./data/raw/coin_metrics",
        help=(
            "Directory where raw gzip JSON envelopes will be saved "
            "(default: ./data/raw/coin_metrics)."
        ),
    )
    fetch_net_parser.add_argument(
        "--db-path",
        default=None,
        help=(
            "Path to DuckDB database file for run locking "
            "(default: <output-dir>/../../state/platform.duckdb)."
        ),
    )

    # 10. promote-network command
    promote_net_parser = subparsers.add_parser(
        "promote-network",
        parents=[base_db_parser],
        help="Promote raw on-chain network envelopes to curated Parquet and DuckDB views.",
        description=(
            "Promote raw on-chain network envelopes to curated Parquet partitions, "
            "refresh fact_network_metrics_daily & mart_btc_market_and_network_daily views, "
            "and record coin_metrics_daily watermark."
        ),
    )
    promote_net_parser.add_argument(
        "--raw-dir",
        default="./data/raw/coin_metrics",
        help=(
            "Directory containing raw gzip JSON network envelopes "
            "(default: ./data/raw/coin_metrics)."
        ),
    )
    promote_net_parser.add_argument(
        "--curated-dir",
        default="./data/curated",
        help="Directory for curated Parquet partitions (default: ./data/curated).",
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
    client: CoinbaseClient | None = None,
    network_client: CoinMetricsClient | None = None,
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

        _log_event(
            "info",
            "backfill_started",
            run_id=run_id,
            windows_planned=len(plan.windows),
            output_dir=str(output_dir),
        )

        with _managed_run_lock(
            db_manager,
            run_id,
            "backfill",
            now_utc,
            clock=clock,
            requested_start_utc=start_utc,
            requested_end_utc=end_utc,
        ) as lock_ctx:
            if lock_ctx is None:
                return 6

            cb_client = client if client is not None else CoinbaseClient()
            owns_client = client is None
            try:
                exit_code, w_succ, w_fail, c_ing, files_w, err_msg = _ingest_windows(
                    cb_client, plan.windows, run_id, output_dir
                )
            finally:
                if owns_client:
                    cb_client.close()

            if exit_code != 0:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = err_msg

            summary = {
                "run_id": run_id,
                "status": "success" if exit_code == 0 else "failure",
                "requested_start_utc": format_canonical_utc(start_utc),
                "requested_end_utc": format_canonical_utc(end_utc),
                "windows_planned": len(plan.windows),
                "windows_succeeded": w_succ,
                "windows_failed": w_fail,
                "candles_ingested": c_ing,
                "output_dir": str(output_dir),
                "files_written": files_w,
            }
            sys.stdout.write(json.dumps(summary, indent=2) + "\n")
            if err_msg is not None:
                sys.stderr.write(f"error: {err_msg}\n")
            return exit_code

    if args.command == "promote":
        now_utc = _resolve_clock(clock)
        run_id = str(uuid.uuid4())
        raw_dir = Path(args.raw_dir)
        curated_dir = Path(args.curated_dir)
        db_path = Path(args.db_path)
        db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
        db_manager.initialize()

        with _managed_run_lock(db_manager, run_id, "promote", now_utc, clock=clock) as lock_ctx:
            if lock_ctx is None:
                return 6

            _log_event(
                "info",
                "promote_started",
                run_id=run_id,
                raw_dir=str(raw_dir),
                curated_dir=str(curated_dir),
                db_path=str(db_path),
            )

            code, err, n_env, n_rows, n_parts = _promote_market_data(
                raw_dir,
                curated_dir,
                db_manager,
                run_id,
                now_utc,
                clock=clock,
                update_watermark=True,
            )
            if code != 0:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = err
                return code

            if n_env == 0:
                sys.stderr.write(f"No raw envelopes found in {raw_dir}\n")

            new_wm = db_manager.get_watermark()
            lock_ctx.rows_promoted = n_rows
            lock_ctx.partitions_written = n_parts
            lock_ctx.raw_envelopes_read = n_env
            lock_ctx.new_watermark_utc = new_wm

            _log_event(
                "info",
                "promote_completed",
                run_id=run_id,
                rows_promoted=n_rows,
                partitions_written=n_parts,
            )

            summary = {
                "run_id": run_id,
                "status": "success",
                "raw_envelopes_read": n_env,
                "rows_promoted": n_rows,
                "partitions_written": n_parts,
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

        with _managed_run_lock(db_manager, run_id, "incremental", now_utc, clock=clock) as lock_ctx:
            if lock_ctx is None:
                return 6

            watermark = db_manager.get_watermark()
            if watermark is None:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = "No watermark found. Run a backfill first."
                sys.stderr.write("error: No watermark found. Run a backfill first.\n")
                return 2

            lock_ctx.old_watermark_utc = watermark
            start_utc = watermark - timedelta(hours=overlap_hours)
            end_utc = now_utc.replace(minute=0, second=0, microsecond=0)

            if start_utc >= end_utc:
                lock_ctx.new_watermark_utc = watermark
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

            try:
                plan = plan_backfill(start_utc, end_utc, now_utc=now_utc)
            except Exception as exc:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = str(exc)
                sys.stderr.write(f"error: window planning failed: {exc}\n")
                return 2

            cb_client = client if client is not None else CoinbaseClient()
            owns_client = client is None
            try:
                exit_code, w_succ, w_fail, c_ing, files_w, err_msg = _ingest_windows(
                    cb_client, plan.windows, run_id, raw_dir
                )
            finally:
                if owns_client:
                    cb_client.close()

            if exit_code != 0:
                lock_ctx.status = "FAILED"
                lock_ctx.new_watermark_utc = watermark
                lock_ctx.error_message = err_msg
                summary = {
                    "run_id": run_id,
                    "status": "failure",
                    "mode": "incremental",
                    "old_watermark_utc": format_canonical_utc(watermark),
                    "new_watermark_utc": format_canonical_utc(watermark),
                    "windows_planned": len(plan.windows),
                    "windows_succeeded": w_succ,
                    "windows_failed": w_fail,
                    "candles_ingested": c_ing,
                    "output_dir": str(raw_dir),
                    "files_written": files_w,
                }
                sys.stdout.write(json.dumps(summary, indent=2) + "\n")
                if err_msg:
                    sys.stderr.write(f"error: {err_msg}\n")
                return exit_code

            code, err, n_env, n_rows, n_parts = _promote_market_data(
                raw_dir,
                curated_dir,
                db_manager,
                run_id,
                now_utc,
                clock=clock,
                update_watermark=True,
            )
            if code != 0:
                lock_ctx.status = "FAILED"
                lock_ctx.new_watermark_utc = watermark
                lock_ctx.error_message = err
                return code

            new_watermark = db_manager.get_watermark() or watermark
            lock_ctx.rows_promoted = n_rows
            lock_ctx.partitions_written = n_parts
            lock_ctx.raw_envelopes_read = n_env
            lock_ctx.new_watermark_utc = new_watermark

            summary = {
                "run_id": run_id,
                "status": "success",
                "mode": "incremental",
                "old_watermark_utc": format_canonical_utc(watermark),
                "new_watermark_utc": format_canonical_utc(new_watermark),
                "windows_planned": len(plan.windows),
                "windows_succeeded": w_succ,
                "windows_failed": 0,
                "candles_ingested": c_ing,
                "raw_envelopes_read": n_env,
                "rows_promoted": n_rows,
                "partitions_written": n_parts,
                "curated_dir": str(curated_dir),
                "db_path": str(db_path),
            }
            sys.stdout.write(json.dumps(summary, indent=2) + "\n")
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

        with _managed_run_lock(db_manager, run_id, "repair", now_utc, clock=clock) as lock_ctx:
            if lock_ctx is None:
                return 6

            code, err, n_env, n_rows, n_parts = _promote_market_data(
                raw_dir,
                curated_dir,
                db_manager,
                run_id,
                now_utc,
                clock=clock,
                update_watermark=False,
                clear_existing=True,
            )
            if code != 0:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = err
                return code

            if n_env == 0:
                sys.stderr.write(f"No raw envelopes found in {raw_dir}\n")

            lock_ctx.rows_promoted = n_rows
            lock_ctx.partitions_written = n_parts
            lock_ctx.raw_envelopes_read = n_env

            summary = {
                "run_id": run_id,
                "status": "success",
                "mode": "repair",
                "raw_envelopes_read": n_env,
                "rows_promoted": n_rows,
                "partitions_written": n_parts,
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

    if args.command == "fetch-network":
        now_utc = _resolve_clock(clock)
        try:
            start_dt = _parse_cli_date(args.start, "--start")
            end_dt = _parse_cli_date(args.end, "--end")
            if start_dt > end_dt:
                sys.stderr.write(
                    f"error: --start ({args.start}) must not be after --end ({args.end})\n"
                )
                return 2
        except (TimeRangeError, ValueError) as exc:
            sys.stderr.write(f"error: {exc}\n")
            return 2

        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        run_id = str(uuid.uuid4())
        output_dir = Path(args.output_dir)
        db_path = (
            Path(args.db_path) if args.db_path is not None else Path("./data/state/platform.duckdb")
        )
        db_manager = DuckDBManager(db_path=db_path, curated_dir=db_path.parent)
        db_manager.initialize()

        _log_event(
            "info",
            "fetch_network_started",
            run_id=run_id,
            start=start_str,
            end=end_str,
            output_dir=str(output_dir),
        )

        with _managed_run_lock(
            db_manager,
            run_id,
            "fetch_network",
            now_utc,
            clock=clock,
            requested_start_utc=start_dt,
            requested_end_utc=end_dt,
        ) as lock_ctx:
            if lock_ctx is None:
                return 6

            cm_client = network_client if network_client is not None else CoinMetricsClient()
            owns_client = network_client is None
            exit_code = 0
            net_error_msg: str | None = None
            records_ingested = 0
            net_files_written: list[str] = []
            cm_response = None

            try:
                try:
                    cm_response = cm_client.fetch_asset_metrics(
                        start_time=start_str,
                        end_time=end_str,
                    )
                except (
                    CoinMetricsSourceUnavailableError,
                    CoinMetricsHTTPError,
                    CoinMetricsClientError,
                ) as exc:
                    if isinstance(exc, CoinMetricsSourceUnavailableError):
                        net_error_msg = f"Coin Metrics source unavailable: {exc}"
                        event = "source_unavailable"
                    elif isinstance(exc, CoinMetricsHTTPError):
                        net_error_msg = f"Coin Metrics HTTP error: {exc}"
                        event = "coin_metrics_http_error"
                    else:
                        net_error_msg = f"Coin Metrics client error: {exc}"
                        event = "coin_metrics_error"
                    _log_event("error", event, run_id=run_id, error=str(exc))
                    exit_code = 3
                except CoinMetricsContractViolationError as exc:
                    net_error_msg = f"Contract violation: {exc}"
                    _log_event("error", "contract_violation", run_id=run_id, error=str(exc))
                    exit_code = 4

                if exit_code == 0 and cm_response is not None:
                    cm_validation = validate_coin_metrics_payload(cm_response.raw_payload)
                    if not cm_validation.is_valid:
                        net_error_msg = f"Contract violation: {'; '.join(cm_validation.violations)}"
                        _log_event(
                            "error",
                            "contract_violation",
                            run_id=run_id,
                            violations=cm_validation.violations,
                        )
                        exit_code = 4
                    else:
                        try:
                            envelope = create_network_raw_envelope(
                                run_id=run_id,
                                asset="btc",
                                metrics="TxCnt,AdrActCnt",
                                frequency="1d",
                                start_time=start_str,
                                end_time=end_str,
                                retrieved_at_utc=cm_response.retrieved_at_utc,
                                http_status=cm_response.http_status,
                                payload=cm_response.raw_payload,
                                provider_request_id=cm_response.provider_request_id,
                            )
                            file_path = write_network_raw_envelope(output_dir, envelope)
                            net_files_written.append(str(file_path))
                            records_ingested = len(cm_validation.valid_records)
                            _log_event(
                                "info",
                                "network_persisted",
                                run_id=run_id,
                                path=str(file_path),
                                records=records_ingested,
                            )
                        except Exception as exc:
                            net_error_msg = f"Storage failure: {exc}"
                            _log_event("error", "storage_failure", run_id=run_id, error=str(exc))
                            exit_code = 5
            finally:
                if owns_client:
                    cm_client.close()

            if exit_code != 0:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = net_error_msg

            fetch_summary: dict[str, Any] = {
                "run_id": run_id,
                "status": "success" if exit_code == 0 else "failure",
                "requested_start": start_str,
                "requested_end": end_str,
                "records_ingested": records_ingested,
                "output_dir": str(output_dir),
                "files_written": net_files_written,
            }
            sys.stdout.write(json.dumps(fetch_summary, indent=2) + "\n")
            if net_error_msg is not None:
                sys.stderr.write(f"error: {net_error_msg}\n")
            return exit_code

    if args.command == "promote-network":
        now_utc = _resolve_clock(clock)
        run_id = str(uuid.uuid4())
        raw_dir = Path(args.raw_dir)
        curated_dir = Path(args.curated_dir)
        db_path = Path(args.db_path)

        db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
        db_manager.initialize()

        with _managed_run_lock(
            db_manager, run_id, "promote_network", now_utc, clock=clock
        ) as lock_ctx:
            if lock_ctx is None:
                return 6

            _log_event(
                "info",
                "promote_network_started",
                run_id=run_id,
                raw_dir=str(raw_dir),
                curated_dir=str(curated_dir),
                db_path=str(db_path),
            )

            try:
                net_envelopes = read_network_raw_envelopes(raw_dir)
            except Exception as exc:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = str(exc)
                _log_event("error", "raw_read_failed", run_id=run_id, error=str(exc))
                sys.stderr.write(f"error: failed to read raw network envelopes: {exc}\n")
                return 2

            if not net_envelopes:
                sys.stderr.write(f"No raw network envelopes found in {raw_dir}\n")
                empty_summary: dict[str, Any] = {
                    "run_id": run_id,
                    "status": "success",
                    "raw_envelopes_read": 0,
                    "rows_promoted": 0,
                    "partitions_written": 0,
                    "watermark_utc": None,
                    "curated_dir": str(curated_dir),
                    "db_path": str(db_path),
                }
                sys.stdout.write(json.dumps(empty_summary, indent=2) + "\n")
                return 0

            try:
                net_metrics = normalize_network_envelopes(net_envelopes, now_utc=now_utc)
            except CoinMetricsContractViolationError as exc:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = str(exc)
                _log_event("error", "quality_failure", run_id=run_id, error=str(exc))
                sys.stderr.write(f"error: network metric contract violation: {exc}\n")
                return 4
            except Exception as exc:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = str(exc)
                _log_event("error", "normalization_failure", run_id=run_id, error=str(exc))
                sys.stderr.write(f"error: network normalization failure: {exc}\n")
                return 4

            try:
                net_partitions = write_network_parquet_partitions(
                    net_metrics, curated_dir=curated_dir
                )
            except NetworkParquetStorageError as exc:
                lock_ctx.status = "FAILED"
                lock_ctx.error_message = str(exc)
                _log_event("error", "parquet_write_failed", run_id=run_id, error=str(exc))
                sys.stderr.write(f"error: failed writing network Parquet: {exc}\n")
                return 5

            db_manager.create_network_fact_view()
            db_manager.create_cross_domain_mart_view()

            max_date = max((m.metric_date_utc for m in net_metrics), default=None)
            old_wm = db_manager.get_watermark(pipeline_id="coin_metrics_daily")
            new_wm = old_wm
            if max_date is not None:
                db_manager.set_watermark(
                    watermark_utc=max_date,
                    run_id=run_id,
                    pipeline_id="coin_metrics_daily",
                    now_utc=now_utc,
                )
                new_wm = max_date

            lock_ctx.rows_promoted = len(net_metrics)
            lock_ctx.partitions_written = len(net_partitions)
            lock_ctx.raw_envelopes_read = len(net_envelopes)
            lock_ctx.old_watermark_utc = old_wm
            lock_ctx.new_watermark_utc = new_wm

            promote_summary: dict[str, Any] = {
                "run_id": run_id,
                "status": "success",
                "raw_envelopes_read": len(net_envelopes),
                "rows_promoted": len(net_metrics),
                "partitions_written": len(net_partitions),
                "watermark_utc": format_canonical_utc(new_wm) if new_wm else None,
                "curated_dir": str(curated_dir),
                "db_path": str(db_path),
            }
            sys.stdout.write(json.dumps(promote_summary, indent=2) + "\n")
            return 0

    if args.command == "query":
        db_path = Path(args.db_path)
        if not db_path.exists():
            sys.stderr.write(f"error: database file not found at {db_path}\n")
            return 2

        if args.file:
            query_file = Path(args.file)
            if not query_file.exists():
                sys.stderr.write(f"error: query file not found at {query_file}\n")
                return 2
            try:
                sql = load_query_file(query_file)
            except Exception as exc:
                sys.stderr.write(f"error: failed to read query file: {exc}\n")
                return 2
        else:
            sql = args.sql

        params: dict[str, Any] = {}
        for p in args.param or []:
            if "=" not in p:
                sys.stderr.write(f"error: invalid param format '{p}', expected KEY=VALUE\n")
                return 2
            k, v = p.split("=", 1)
            params[k.strip()] = v.strip()

        db_manager = DuckDBManager(db_path=db_path, curated_dir=db_path.parent)
        try:
            with db_manager:
                table = execute_parameterized_query(db_manager, sql, params if params else None)
                if args.output:
                    export_arrow_table(table, fmt=args.format, output_path=args.output)
                    return 0

                data = export_arrow_table(table, fmt=args.format)
                if isinstance(data, Path):
                    return 0
                if args.format in ("json", "csv"):
                    text = data.decode("utf-8")
                    if not text.endswith("\n"):
                        text += "\n"
                    sys.stdout.write(text)
                else:
                    sys.stdout.buffer.write(data)
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
