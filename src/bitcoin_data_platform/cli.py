"""Console entry point and CLI commands for Bitcoin Data Engineering Platform."""

import argparse
import json
import os
import sys
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.sources.coinbase_client import (
    CoinbaseClient,
    CoinbaseClientError,
    SourceUnavailableError,
)
from bitcoin_data_platform.sources.coinbase_contract import validate_candle_payload
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

    sys.stderr.write(f"error: unrecognized command: {args.command}\n")
    return 2


if __name__ == "__main__":
    sys.exit(main())
