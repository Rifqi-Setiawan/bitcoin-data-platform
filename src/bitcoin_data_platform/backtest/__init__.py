"""Backtesting and quantitative validation package for systematic Bitcoin strategies."""

from bitcoin_data_platform.backtest.engine import BacktestEngine
from bitcoin_data_platform.backtest.metrics import (
    compute_cagr,
    compute_calmar_ratio,
    compute_drawdowns,
    compute_sharpe_ratio,
    compute_sortino_ratio,
    compute_strategy_result,
    compute_total_return,
)
from bitcoin_data_platform.backtest.models import (
    BacktestConfig,
    BacktestDayRecord,
    BenchmarkSummary,
    DailyPortfolioState,
    FrequencyType,
    StrategyResult,
    StrategyType,
)
from bitcoin_data_platform.backtest.reporter import (
    format_json,
    format_markdown,
    format_table,
)
from bitcoin_data_platform.backtest.strategies import (
    BaseStrategy,
    BlindDCAStrategy,
    DynamicReserveDCAStrategy,
    EventDrivenRegimeStrategy,
    LumpSumStrategy,
    WeeklyBatchDCAStrategy,
)

__all__ = [
    "BacktestConfig",
    "BacktestDayRecord",
    "BacktestEngine",
    "BaseStrategy",
    "BenchmarkSummary",
    "BlindDCAStrategy",
    "DailyPortfolioState",
    "DynamicReserveDCAStrategy",
    "EventDrivenRegimeStrategy",
    "FrequencyType",
    "LumpSumStrategy",
    "StrategyResult",
    "StrategyType",
    "WeeklyBatchDCAStrategy",
    "compute_cagr",
    "compute_calmar_ratio",
    "compute_drawdowns",
    "compute_sharpe_ratio",
    "compute_sortino_ratio",
    "compute_strategy_result",
    "compute_total_return",
    "format_json",
    "format_markdown",
    "format_table",
]
