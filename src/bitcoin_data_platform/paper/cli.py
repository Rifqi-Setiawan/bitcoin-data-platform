"""CLI subcommands and handlers for Phase 15 Forward Paper Trading Engine."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from bitcoin_data_platform.paper.engine import PaperEngineError, PaperTradingEngine


def register_paper_cli(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the 'paper' top-level subcommand and nested operations."""
    paper_parser = subparsers.add_parser(
        "paper",
        help="Forward paper trading simulation engine ($1,000 virtual capital).",
        description=(
            "Execute and monitor forward paper trading simulations using systematic "
            "DynamicReserveDCAStrategy rules against live market data."
        ),
    )
    paper_subparsers = paper_parser.add_subparsers(
        dest="paper_command",
        title="paper trading subcommands",
        metavar="<subcommand>",
    )

    # 1. init
    init_parser = paper_subparsers.add_parser(
        "init",
        help="Initialize a new virtual portfolio with cash capital.",
        description="Initialize DuckDB balance, snapshot, and ledger tables with virtual capital.",
    )
    init_parser.add_argument(
        "--initial-cash",
        type=float,
        default=1000.0,
        help="Initial USD virtual capital (default: 1000.0).",
    )
    init_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    init_parser.add_argument(
        "--portfolio-id",
        default="default",
        help="Unique identifier for the paper portfolio (default: default).",
    )

    # 2. step
    step_parser = paper_subparsers.add_parser(
        "step",
        help="Advance the portfolio by one day executing DCA strategy rules.",
        description=(
            "Query today's analytical signal, execute DynamicReserveDCA order, "
            "deduct 10 bps simulated fee, update balances, and record snapshot."
        ),
    )
    step_parser.add_argument(
        "--date",
        dest="trade_date",
        type=str,
        default=None,
        help="Trade date YYYY-MM-DD (default: current UTC date).",
    )
    step_parser.add_argument(
        "--daily-budget",
        type=float,
        default=10.0,
        help="Daily budget slice for DCA allocation (default: 10.0).",
    )
    step_parser.add_argument(
        "--force-spot-price",
        "--force-price",
        dest="force_spot_price",
        type=float,
        default=None,
        help="Force explicit spot price for simulation execution.",
    )
    step_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    step_parser.add_argument(
        "--portfolio-id",
        default="default",
        help="Unique identifier for the paper portfolio (default: default).",
    )

    # 3. status
    status_parser = paper_subparsers.add_parser(
        "status",
        help="Display current portfolio balance, equity, and performance metrics.",
        description="Show consolidated performance metrics, ROI, and BTC accumulation stats.",
    )
    status_parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output presentation format (default: table).",
    )
    status_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    status_parser.add_argument(
        "--portfolio-id",
        default="default",
        help="Unique identifier for the paper portfolio (default: default).",
    )

    # 4. reset
    reset_parser = paper_subparsers.add_parser(
        "reset",
        help="Reset paper portfolio and wipe ledger history.",
        description="Purge paper trading tables and re-initialize with pristine capital.",
    )
    reset_parser.add_argument(
        "--initial-cash",
        type=float,
        default=1000.0,
        help="Initial USD virtual capital (default: 1000.0).",
    )
    reset_parser.add_argument(
        "--force",
        action="store_true",
        help="Confirm reset of portfolio and ledger history.",
    )
    reset_parser.add_argument(
        "--db-path",
        default="./data/state/platform.duckdb",
        help="Path to platform DuckDB database file (default: ./data/state/platform.duckdb).",
    )
    reset_parser.add_argument(
        "--portfolio-id",
        default="default",
        help="Unique identifier for the paper portfolio (default: default).",
    )


def handle_paper_cli(args: argparse.Namespace) -> int:
    """Execute paper trading commands dispatched from the root CLI parser."""
    if not hasattr(args, "paper_command") or not args.paper_command:
        sys.stderr.write("error: paper subcommand required (init, step, status, reset)\n")
        return 2

    command = args.paper_command
    db_path = Path(args.db_path)
    portfolio_id = getattr(args, "portfolio_id", "default")

    try:
        engine = PaperTradingEngine(db_path=db_path, portfolio_id=portfolio_id)

        if command == "init":
            initial_cash = float(args.initial_cash)
            if initial_cash <= 0.0:
                sys.stderr.write(f"error: --initial-cash must be > 0, got {initial_cash}\n")
                return 2

            balance = engine.init_portfolio(initial_cash=initial_cash)
            sys.stdout.write(
                f"Initialized forward paper portfolio '{balance.portfolio_id}' with "
                f"${balance.initial_cash:,.2f} USD virtual capital "
                f"(${balance.base_cash:,.2f} Base Cash, "
                f"${balance.reserve_cash:,.2f} Tactical Reserve).\n"
            )
            return 0

        elif command == "step":
            if args.trade_date:
                try:
                    t_date = date.fromisoformat(args.trade_date)
                except ValueError:
                    sys.stderr.write(
                        f"error: invalid --date '{args.trade_date}', expected YYYY-MM-DD\n"
                    )
                    return 2
            else:
                t_date = datetime.now(UTC).date()

            daily_budget = float(args.daily_budget)
            if daily_budget <= 0.0:
                sys.stderr.write(f"error: --daily-budget must be > 0, got {daily_budget}\n")
                return 2

            force_price = float(args.force_spot_price) if args.force_spot_price else None

            record = engine.step(
                trade_date=t_date,
                daily_budget=daily_budget,
                force_spot_price=force_price,
            )

            summary = engine.get_portfolio_summary()
            sys.stdout.write(
                f"[{record.trade_date.isoformat()}] Side: {record.side} | "
                f"Signal: {record.signal_regime} | "
                f"Spot: ${record.spot_price:,.2f} | "
                f"Gross: ${record.gross_amount_usd:,.2f} | "
                f"Fee: ${record.fee_usd:,.4f} | "
                f"BTC: +{record.btc_amount:.8f} BTC\n"
                f"Narrative: {record.narrative}\n"
                f"Portfolio Equity: ${summary.total_equity:,.2f} "
                f"(Cash: ${summary.total_cash:,.2f}, BTC: {summary.btc_balance:.8f})\n"
            )
            return 0

        elif command == "status":
            fmt = getattr(args, "format", "table")
            summary = engine.get_portfolio_summary()

            if fmt == "json":
                sys.stdout.write(json.dumps(summary.to_dict(), indent=2) + "\n")
                return 0

            # Fincept Terminal-style table formatting
            pnl_sign = "+" if summary.unrealized_pnl_usd >= 0 else ""
            alpha_sign = "+" if summary.outperformance_usd >= 0 else ""
            table_output = (
                "=" * 80 + "\n"
                "FINCEPT FORWARD PAPER TRADING PORTFOLIO SUMMARY ($1,000 VIRTUAL CAPITAL)\n"
                + "="
                * 80
                + "\n"
                f"Portfolio ID          : {summary.portfolio_id}\n"
                f"Total Equity (USD)    : ${summary.total_equity:,.2f}\n"
                f"Initial Capital (USD) : ${summary.initial_cash:,.2f}\n"
                f"Unrealized PnL (USD)  : {pnl_sign}${summary.unrealized_pnl_usd:,.2f} "
                f"({pnl_sign}{summary.unrealized_pnl_pct:.2f}%)\n"
                f"Benchmark B&H (USD)   : ${summary.benchmark_equity:,.2f} "
                f"(Alpha: {alpha_sign}${summary.outperformance_usd:,.2f} USD)\n\n"
                f"Cash Holdings         : ${summary.total_cash:,.2f}\n"
                f"  - Base Cash Pool    : ${summary.base_cash:,.2f} (70% pool)\n"
                f"  - Tactical Reserve  : ${summary.reserve_cash:,.2f} (30% pool)\n\n"
                f"Bitcoin Holdings      : {summary.btc_balance:.8f} BTC "
                f"(≈ ${summary.btc_value_usd:,.2f} USD)\n"
                f"Avg Purchase Price    : ${summary.avg_buy_price:,.2f} USD\n"
                f"Current Spot Price    : ${summary.current_spot_price:,.2f} USD\n"
                f"Acquisition Discount  : {summary.acquisition_discount_pct:.2f}%\n"
                f"Total Executed Trades : {summary.total_trades}\n" + "=" * 80 + "\n"
            )
            sys.stdout.write(table_output)
            return 0

        elif command == "reset":
            if not getattr(args, "force", False):
                sys.stderr.write(
                    "error: resetting paper portfolio requires explicit --force flag\n"
                )
                return 2

            initial_cash = float(args.initial_cash)
            if initial_cash <= 0.0:
                sys.stderr.write(f"error: --initial-cash must be > 0, got {initial_cash}\n")
                return 2

            balance = engine.reset_portfolio(initial_cash=initial_cash)
            sys.stdout.write(
                f"Reset forward paper portfolio '{balance.portfolio_id}' to initial state "
                f"(${balance.initial_cash:,.2f} USD virtual capital).\n"
            )
            return 0

        else:
            sys.stderr.write(f"error: unknown paper command '{command}'\n")
            return 2

    except PaperEngineError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except Exception as exc:
        sys.stderr.write(f"error: unexpected failure in paper command: {exc}\n")
        return 2
