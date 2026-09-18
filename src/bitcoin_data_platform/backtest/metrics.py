"""Quantitative metrics and statistical formulations for backtest evaluation."""

from __future__ import annotations

import math
import statistics

from bitcoin_data_platform.backtest.models import (
    BacktestConfig,
    DailyPortfolioState,
    StrategyResult,
    StrategyType,
)


def compute_total_return(final_equity: float, total_contributed: float) -> float:
    """Compute total percentage return over total contributed capital.

    Args:
        final_equity: Portfolio liquidation value at end of backtest.
        total_contributed: Total fiat capital injected.

    Returns:
        Total return percentage (e.g. 50.0 for +50.0%).
    """
    if total_contributed <= 0.0:
        return 0.0
    return ((final_equity - total_contributed) / total_contributed) * 100.0


def compute_cagr(final_equity: float, total_contributed: float, days: int) -> float:
    """Compute Compound Annual Growth Rate (CAGR) on a 365-day continuous basis.

    Formula:
        CAGR = (V_final / Total_Invested) ^ (365 / days) - 1

    Args:
        final_equity: Final portfolio valuation.
        total_contributed: Total capital injected.
        days: Duration in calendar days.

    Returns:
        Annualized percentage return (e.g. 25.5 for +25.5%).
    """
    if days <= 0 or total_contributed <= 0.0:
        return 0.0
    if final_equity <= 0.0:
        return -100.0

    ratio = final_equity / total_contributed
    exponent = 365.0 / float(days)
    try:
        cagr = (math.pow(ratio, exponent) - 1.0) * 100.0
        return cagr
    except (OverflowError, ValueError):
        return 0.0


def compute_drawdowns(equity_series: list[float]) -> tuple[list[float], float]:
    """Compute daily percentage drawdown series and maximum drawdown magnitude.

    Formulas:
        Peak_t = max(0..t, Equity_s)
        Drawdown_t = (Equity_t - Peak_t) / Peak_t (<= 0.0)
        Max_DD = abs(min(Drawdown_t)) (>= 0.0)

    Args:
        equity_series: Chronological portfolio equity values.

    Returns:
        Tuple of (drawdowns_series, max_drawdown_magnitude).
    """
    if not equity_series:
        return [], 0.0

    drawdowns: list[float] = []
    peak = 0.0

    for equity in equity_series:
        if equity > peak:
            peak = equity
        dd = (equity - peak) / peak if peak > 0.0 else 0.0
        drawdowns.append(dd)

    max_dd = abs(min(drawdowns)) if drawdowns else 0.0
    return drawdowns, max_dd


def compute_sharpe_ratio(daily_returns: list[float], risk_free_rate: float = 0.03) -> float:
    """Compute annualized Sharpe Ratio using 365 crypto calendar days.

    Formula:
        Sharpe = sqrt(365) * (mean(R) - R_f / 365) / stdev(R)

    Args:
        daily_returns: Daily returns series.
        risk_free_rate: Annualized risk-free benchmark rate (default 0.03 = 3.0%).

    Returns:
        Annualized Sharpe ratio.
    """
    if len(daily_returns) < 2:
        return 0.0

    daily_rf = risk_free_rate / 365.0
    mean_ret = statistics.mean(daily_returns)
    std_ret = statistics.stdev(daily_returns)

    if std_ret <= 0.0 or math.isnan(std_ret):
        return 0.0

    return math.sqrt(365.0) * (mean_ret - daily_rf) / std_ret


def compute_sortino_ratio(daily_returns: list[float], risk_free_rate: float = 0.03) -> float:
    """Compute annualized Sortino Ratio penalizing only downside volatility.

    Formula:
        sigma_down = sqrt( (1 / N) * sum( min(0, R_t - R_f / 365)^2 ) )
        Sortino = sqrt(365) * (mean(R) - R_f / 365) / sigma_down

    Args:
        daily_returns: Daily returns series.
        risk_free_rate: Annualized risk-free benchmark rate.

    Returns:
        Annualized Sortino ratio.
    """
    if not daily_returns:
        return 0.0

    daily_rf = risk_free_rate / 365.0
    mean_ret = statistics.mean(daily_returns)

    downside_sq = [min(0.0, r - daily_rf) ** 2 for r in daily_returns]
    downside_var = sum(downside_sq) / len(daily_returns)
    downside_dev = math.sqrt(downside_var)

    if downside_dev <= 0.0 or math.isnan(downside_dev):
        return 0.0

    return math.sqrt(365.0) * (mean_ret - daily_rf) / downside_dev


def compute_calmar_ratio(cagr_pct: float, max_drawdown_pct: float) -> float:
    """Compute Calmar Ratio measuring annual return relative to maximum drawdown.

    Formula:
        Calmar = CAGR / |MDD|

    Args:
        cagr_pct: Annualized percentage return.
        max_drawdown_pct: Maximum percentage drawdown magnitude.

    Returns:
        Calmar ratio.
    """
    abs_mdd = abs(max_drawdown_pct)
    if abs_mdd < 1e-9:
        return 0.0
    return cagr_pct / abs_mdd


def compute_strategy_result(
    strategy_type: StrategyType,
    config: BacktestConfig,
    daily_states: list[DailyPortfolioState],
    buys: list[tuple[float, float]],
    market_prices: list[float],
) -> StrategyResult:
    """Calculate and populate all metrics into a comprehensive StrategyResult.

    Args:
        strategy_type: Enum of the strategy.
        config: Configuration parameters used.
        daily_states: Recorded daily portfolio states.
        buys: List of (usd_spent, btc_received) for all executed purchases.
        market_prices: Historical close prices over the backtest duration.

    Returns:
        Populated StrategyResult instance.
    """
    if not daily_states:
        return StrategyResult(
            strategy_type=strategy_type,
            config=config,
        )

    final_state = daily_states[-1]
    final_equity = final_state.portfolio_equity
    total_contributed = final_state.total_contributed
    net_profit = final_equity - total_contributed

    total_return_pct = compute_total_return(final_equity, total_contributed)

    duration_days = len(daily_states)
    cagr_pct = compute_cagr(final_equity, total_contributed, duration_days)

    equity_series = [s.portfolio_equity for s in daily_states]
    _, max_dd = compute_drawdowns(equity_series)
    max_drawdown_pct = max_dd * 100.0

    # Daily returns (omit day 0 inception)
    daily_returns = [s.daily_return for s in daily_states[1:]]
    sharpe = compute_sharpe_ratio(daily_returns, config.risk_free_rate)
    sortino = compute_sortino_ratio(daily_returns, config.risk_free_rate)
    calmar = compute_calmar_ratio(cagr_pct, max_drawdown_pct)

    total_btc = sum(btc for _, btc in buys)
    total_usd_spent = sum(usd for usd, _ in buys)
    avg_buy_price = (total_usd_spent / total_btc) if total_btc > 0.0 else 0.0

    avg_market_price = statistics.mean(market_prices) if market_prices else 0.0

    if avg_market_price > 0.0:
        acquisition_discount_pct = ((avg_market_price - avg_buy_price) / avg_market_price) * 100.0
    else:
        acquisition_discount_pct = 0.0

    reserve_final = final_state.reserve_cash_balance
    reserve_peak = max((s.reserve_cash_balance for s in daily_states), default=0.0)

    return StrategyResult(
        strategy_type=strategy_type,
        config=config,
        daily_states=daily_states,
        total_contributed=total_contributed,
        final_equity=final_equity,
        net_profit=net_profit,
        total_return_pct=total_return_pct,
        cagr_pct=cagr_pct,
        max_drawdown_pct=max_drawdown_pct,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        calmar_ratio=calmar,
        total_btc_accumulated=total_btc,
        average_buy_price=avg_buy_price,
        average_market_price=avg_market_price,
        acquisition_discount_pct=acquisition_discount_pct,
        reserve_pool_final=reserve_final,
        reserve_pool_peak=reserve_peak,
    )
