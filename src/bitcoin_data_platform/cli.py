"""Console entry point and CLI commands for Bitcoin Data Engineering Platform."""

import argparse
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from bitcoin_data_platform.time_range import (
    TimeRangeError,
    parse_iso_utc,
    validate_hourly_boundary,
)
from bitcoin_data_platform.window_planner import plan_backfill


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

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
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
        except TimeRangeError as exc:
            sys.stderr.write(f"error: {exc}\n")
            return 2
        except Exception as exc:
            sys.stderr.write(f"error: unexpected failure: {exc}\n")
            return 2

    sys.stderr.write(f"error: unrecognized command: {args.command}\n")
    return 2


if __name__ == "__main__":
    sys.exit(main())
