"""Data models and contracts for the Backtest & Validation Engine."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from enum import Enum
from typing import Any


class StrategyType(str, Enum):
    """Supported backtesting investment strategies."""

    LUMP_SUM = "lump-sum"
    BLIND_DCA = "blind-dca"
    DYNAMIC_RESERVE = "dynamic-reserve"

    @classmethod
    def from_str(cls, val: str) -> StrategyType:
        """Parse strategy type from string flexibly."""
        norm = val.lower().replace("_", "-").strip()
        for member in cls:
            if member.value == norm or member.name.lower() == norm:
                return member
        raise ValueError(
            f"Unknown strategy type '{val}'. Supported types: {[m.value for m in cls]}"
        )


class FrequencyType(str, Enum):
    """Contribution injection frequency."""

    DAILY = "daily"
    WEEKLY = "weekly"

    @classmethod
    def from_str(cls, val: str) -> FrequencyType:
        """Parse frequency type from string flexibly."""
        norm = val.lower().strip()
        for member in cls:
            if member.value == norm or member.name.lower() == norm:
                return member
        raise ValueError(
            f"Unknown frequency '{val}'. Supported frequencies: {[m.value for m in cls]}"
        )


@dataclass(frozen=True)
class BacktestConfig:
    """Configuration parameters for a backtest simulation run."""

    start_date: date | None = None
    end_date: date | None = None
    initial_cash: float = 10000.0
    periodic_amount: float = 100.0
    frequency: FrequencyType = FrequencyType.DAILY
    fee_bps: float = 10.0  # 10.0 bps = 0.10% (Coinbase spot fee tier)
    risk_free_rate: float = 0.03  # 3.0% annualized risk-free rate

    def __post_init__(self) -> None:
        """Validate configuration constraints."""
        if self.initial_cash < 0.0:
            raise ValueError(f"initial_cash must be >= 0.0, got {self.initial_cash}")
        if self.periodic_amount < 0.0:
            raise ValueError(f"periodic_amount must be >= 0.0, got {self.periodic_amount}")
        if self.fee_bps < 0.0:
            raise ValueError(f"fee_bps must be >= 0.0, got {self.fee_bps}")
        if self.risk_free_rate < 0.0:
            raise ValueError(f"risk_free_rate must be >= 0.0, got {self.risk_free_rate}")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) cannot be after end_date ({self.end_date})"
            )
        if isinstance(self.frequency, str):
            object.__setattr__(self, "frequency", FrequencyType.from_str(self.frequency))


@dataclass(frozen=True)
class BacktestDayRecord:
    """Historical single-day market and signal record for simulation input."""

    trade_date: date
    market_close_usd: float
    sma_200: float | None = None
    mayer_multiple: float | None = None
    mvrv_ratio: float | None = None
    fng_value: int | None = None
    has_high_impact_macro_event: bool = False
    investment_signal: str | None = None

    def __post_init__(self) -> None:
        """Validate day record data constraints."""
        if self.market_close_usd <= 0.0:
            raise ValueError(
                f"market_close_usd must be > 0.0, got {self.market_close_usd} on {self.trade_date}"
            )


@dataclass
class DailyPortfolioState:
    """Daily portfolio snapshot reflecting mark-to-market balances and cash flows."""

    trade_date: date
    cash_balance: float
    reserve_cash_balance: float
    btc_balance: float
    btc_price: float
    portfolio_equity: float
    total_contributed: float
    daily_cash_flow: float
    daily_return: float
    drawdown: float
    action_taken: str

    def to_dict(self) -> dict[str, Any]:
        """Convert daily state to JSON-serializable dictionary."""
        data = asdict(self)
        data["trade_date"] = self.trade_date.isoformat()
        return data


@dataclass
class StrategyResult:
    """Comprehensive performance results for a single executed strategy."""

    strategy_type: StrategyType
    config: BacktestConfig
    daily_states: list[DailyPortfolioState] = field(default_factory=list)
    total_contributed: float = 0.0
    final_equity: float = 0.0
    net_profit: float = 0.0
    total_return_pct: float = 0.0
    cagr_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    total_btc_accumulated: float = 0.0
    average_buy_price: float = 0.0
    average_market_price: float = 0.0
    acquisition_discount_pct: float = 0.0
    reserve_pool_final: float = 0.0
    reserve_pool_peak: float = 0.0

    def to_dict(self, include_daily_states: bool = False) -> dict[str, Any]:
        """Convert strategy result to summary dictionary."""
        data: dict[str, Any] = {
            "strategy_type": self.strategy_type.value,
            "total_contributed": round(self.total_contributed, 2),
            "final_equity": round(self.final_equity, 2),
            "net_profit": round(self.net_profit, 2),
            "total_return_pct": round(self.total_return_pct, 2),
            "cagr_pct": round(self.cagr_pct, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "sortino_ratio": round(self.sortino_ratio, 2),
            "calmar_ratio": round(self.calmar_ratio, 2),
            "total_btc_accumulated": round(self.total_btc_accumulated, 8),
            "average_buy_price": round(self.average_buy_price, 2),
            "average_market_price": round(self.average_market_price, 2),
            "acquisition_discount_pct": round(self.acquisition_discount_pct, 2),
            "reserve_pool_final": round(self.reserve_pool_final, 2),
            "reserve_pool_peak": round(self.reserve_pool_peak, 2),
        }
        if include_daily_states:
            data["daily_states"] = [s.to_dict() for s in self.daily_states]
        return data


@dataclass
class BenchmarkSummary:
    """Benchmark results comparing multiple backtested strategies."""

    start_date: date
    end_date: date
    duration_days: int
    results: dict[StrategyType, StrategyResult] = field(default_factory=dict)

    def to_dict(self, include_daily_states: bool = False) -> dict[str, Any]:
        """Convert benchmark summary to serializable dictionary."""
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "duration_days": self.duration_days,
            "results": {
                st.value: res.to_dict(include_daily_states=include_daily_states)
                for st, res in self.results.items()
            },
        }
