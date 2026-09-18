"""Formatting and reporting facilities for backtesting benchmark results."""

from __future__ import annotations

import json
from typing import Any

from bitcoin_data_platform.backtest.models import (
    BenchmarkSummary,
    StrategyType,
)

STRATEGY_DISPLAY_NAMES: dict[StrategyType, str] = {
    StrategyType.LUMP_SUM: "Lump Sum Buy & Hold",
    StrategyType.BLIND_DCA: "Blind DCA",
    StrategyType.DYNAMIC_RESERVE: "Dynamic Reserve DCA",
}


def _get_display_name(st: StrategyType) -> str:
    return STRATEGY_DISPLAY_NAMES.get(st, st.value)


def format_table(summary: BenchmarkSummary) -> str:
    """Format benchmark comparison summary as an ASCII terminal table.

    Args:
        summary: BenchmarkSummary instance.

    Returns:
        Formatted ASCII table string.
    """
    if not summary.results:
        return "No backtest results to display."

    strategies = list(summary.results.keys())
    col_headers = [_get_display_name(st) for st in strategies]

    col_width = max(24, max(len(h) for h in col_headers) + 2)
    metric_width = 30

    header_line = "=" * (metric_width + col_width * len(strategies))
    divider_line = "-" * (metric_width + col_width * len(strategies))

    lines: list[str] = [
        header_line,
        " " * ((len(header_line) - 34) // 2) + "BITCOIN STRATEGY BENCHMARK REPORT",
        (
            f"Period: {summary.start_date.isoformat()} to "
            f"{summary.end_date.isoformat()} ({summary.duration_days} days)"
        ),
        header_line,
        f"{'Metric':<{metric_width}}" + "".join(f"{h:>{col_width}}" for h in col_headers),
        divider_line,
    ]

    def _row(label: str, getter: Any) -> str:
        row_str = f"{label:<{metric_width}}"
        for st in strategies:
            res = summary.results[st]
            val = getter(res)
            row_str += f"{val:>{col_width}}"
        return row_str

    lines.append(_row("Total Contributed ($)", lambda r: f"${r.total_contributed:,.2f}"))
    lines.append(_row("Final Equity ($)", lambda r: f"${r.final_equity:,.2f}"))
    lines.append(
        _row(
            "Net Profit ($)",
            lambda r: f"{'+' if r.net_profit >= 0 else ''}${r.net_profit:,.2f}",
        )
    )
    lines.append(
        _row(
            "Total Return (%)",
            lambda r: f"{'+' if r.total_return_pct >= 0 else ''}{r.total_return_pct:,.2f}%",
        )
    )
    lines.append(
        _row(
            "Annualized Return / CAGR (%)",
            lambda r: f"{'+' if r.cagr_pct >= 0 else ''}{r.cagr_pct:,.2f}%",
        )
    )
    lines.append(_row("Max Drawdown (%)", lambda r: f"{r.max_drawdown_pct:,.2f}%"))
    lines.append(_row("Sharpe Ratio (365d)", lambda r: f"{r.sharpe_ratio:.2f}"))
    lines.append(_row("Sortino Ratio (365d)", lambda r: f"{r.sortino_ratio:.2f}"))
    lines.append(_row("Calmar Ratio", lambda r: f"{r.calmar_ratio:.2f}"))
    lines.append(divider_line)
    lines.append(_row("Total BTC Accumulated", lambda r: f"{r.total_btc_accumulated:.8f}"))
    lines.append(_row("Average Buy Price ($)", lambda r: f"${r.average_buy_price:,.2f}"))
    lines.append(_row("Average Market Price ($)", lambda r: f"${r.average_market_price:,.2f}"))
    lines.append(
        _row(
            "Acquisition Discount (%)",
            lambda r: (
                f"{'+' if r.acquisition_discount_pct >= 0 else ''}"
                f"{r.acquisition_discount_pct:,.2f}%"
            ),
        )
    )
    lines.append(_row("Tactical Reserve Final ($)", lambda r: f"${r.reserve_pool_final:,.2f}"))
    lines.append(_row("Tactical Reserve Peak ($)", lambda r: f"${r.reserve_pool_peak:,.2f}"))
    lines.append(header_line)

    return "\n".join(lines)


def format_markdown(summary: BenchmarkSummary) -> str:
    """Format benchmark comparison summary as GitHub-flavored Markdown.

    Args:
        summary: BenchmarkSummary instance.

    Returns:
        Formatted Markdown table string.
    """
    if not summary.results:
        return "_No backtest results to display._"

    strategies = list(summary.results.keys())
    col_headers = [_get_display_name(st) for st in strategies]

    lines: list[str] = [
        "# Bitcoin Strategy Benchmark Report",
        "",
        f"- **Start Date:** `{summary.start_date.isoformat()}`",
        f"- **End Date:** `{summary.end_date.isoformat()}`",
        f"- **Duration:** `{summary.duration_days} days`",
        "",
        "| Metric | " + " | ".join(col_headers) + " |",
        "| :--- | " + " | ".join(["---:"] * len(strategies)) + " |",
    ]

    def _md_row(label: str, getter: Any) -> str:
        vals = [getter(summary.results[st]) for st in strategies]
        return f"| {label} | " + " | ".join(vals) + " |"

    lines.append(_md_row("Total Contributed", lambda r: f"${r.total_contributed:,.2f}"))
    lines.append(_md_row("Final Equity", lambda r: f"${r.final_equity:,.2f}"))
    lines.append(
        _md_row(
            "Net Profit",
            lambda r: f"{'+' if r.net_profit >= 0 else ''}${r.net_profit:,.2f}",
        )
    )
    lines.append(
        _md_row(
            "Total Return",
            lambda r: f"{'+' if r.total_return_pct >= 0 else ''}{r.total_return_pct:,.2f}%",
        )
    )
    lines.append(
        _md_row(
            "CAGR (Annualized)",
            lambda r: f"{'+' if r.cagr_pct >= 0 else ''}{r.cagr_pct:,.2f}%",
        )
    )
    lines.append(_md_row("Max Drawdown", lambda r: f"{r.max_drawdown_pct:,.2f}%"))
    lines.append(_md_row("Sharpe Ratio (365d)", lambda r: f"{r.sharpe_ratio:.2f}"))
    lines.append(_md_row("Sortino Ratio (365d)", lambda r: f"{r.sortino_ratio:.2f}"))
    lines.append(_md_row("Calmar Ratio", lambda r: f"{r.calmar_ratio:.2f}"))
    lines.append(_md_row("Total BTC Accumulated", lambda r: f"{r.total_btc_accumulated:.8f}"))
    lines.append(_md_row("Average Buy Price", lambda r: f"${r.average_buy_price:,.2f}"))
    lines.append(_md_row("Average Market Price", lambda r: f"${r.average_market_price:,.2f}"))
    lines.append(
        _md_row(
            "Acquisition Discount",
            lambda r: (
                f"{'+' if r.acquisition_discount_pct >= 0 else ''}"
                f"{r.acquisition_discount_pct:,.2f}%"
            ),
        )
    )
    lines.append(_md_row("Tactical Reserve Final", lambda r: f"${r.reserve_pool_final:,.2f}"))
    lines.append(_md_row("Tactical Reserve Peak", lambda r: f"${r.reserve_pool_peak:,.2f}"))

    return "\n".join(lines)


def format_json(summary: BenchmarkSummary, include_daily_states: bool = False) -> str:
    """Format benchmark summary as JSON string.

    Args:
        summary: BenchmarkSummary instance.
        include_daily_states: True to include daily portfolio simulation history.

    Returns:
        Indented JSON string.
    """
    return json.dumps(summary.to_dict(include_daily_states=include_daily_states), indent=2)
