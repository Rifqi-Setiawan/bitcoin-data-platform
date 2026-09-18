"""Data models and contracts for Phase 15 Forward Paper Trading Engine."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass
class PaperPortfolioBalance:
    """Current portfolio account balances across cash pools and asset holdings."""

    portfolio_id: str
    initial_cash: float
    base_cash: float
    reserve_cash: float
    btc_balance: float
    total_contributed: float
    last_updated_utc: datetime
    total_trades: int = 0

    @property
    def total_cash(self) -> float:
        """Sum of base and tactical reserve cash balances."""
        return self.base_cash + self.reserve_cash

    def to_dict(self) -> dict[str, Any]:
        """Convert portfolio balance to JSON-serializable dictionary."""
        return {
            "portfolio_id": self.portfolio_id,
            "initial_cash": round(self.initial_cash, 2),
            "base_cash": round(self.base_cash, 2),
            "reserve_cash": round(self.reserve_cash, 2),
            "total_cash": round(self.total_cash, 2),
            "btc_balance": round(self.btc_balance, 8),
            "total_contributed": round(self.total_contributed, 2),
            "last_updated_utc": self.last_updated_utc.isoformat(),
            "total_trades": self.total_trades,
        }


@dataclass(frozen=True)
class PaperTradeRecord:
    """Immutable ledger record of a paper trade execution or daily hold event."""

    trade_id: str
    portfolio_id: str
    executed_at_utc: datetime
    trade_date: date
    side: str  # 'BUY', 'HOLD'
    signal_regime: str  # 'AGGRESSIVE_ACCUMULATE', etc.
    spot_price: float
    gross_amount_usd: float
    fee_usd: float
    net_amount_usd: float
    btc_amount: float
    narrative: str

    def to_dict(self) -> dict[str, Any]:
        """Convert trade record to JSON-serializable dictionary."""
        return {
            "trade_id": self.trade_id,
            "portfolio_id": self.portfolio_id,
            "executed_at_utc": self.executed_at_utc.isoformat(),
            "trade_date": self.trade_date.isoformat(),
            "side": self.side,
            "signal_regime": self.signal_regime,
            "spot_price": round(self.spot_price, 2),
            "gross_amount_usd": round(self.gross_amount_usd, 2),
            "fee_usd": round(self.fee_usd, 4),
            "net_amount_usd": round(self.net_amount_usd, 2),
            "btc_amount": round(self.btc_amount, 8),
            "narrative": self.narrative,
        }


@dataclass(frozen=True)
class PaperSnapshotRecord:
    """End-of-day mark-to-market snapshot of the paper portfolio vs benchmark."""

    snapshot_date: date
    portfolio_id: str
    base_cash: float
    reserve_cash: float
    total_cash: float
    btc_balance: float
    btc_price: float
    portfolio_equity: float
    unrealized_pnl_usd: float
    unrealized_pnl_pct: float
    benchmark_equity: float

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot record to JSON-serializable dictionary."""
        return {
            "snapshot_date": self.snapshot_date.isoformat(),
            "portfolio_id": self.portfolio_id,
            "base_cash": round(self.base_cash, 2),
            "reserve_cash": round(self.reserve_cash, 2),
            "total_cash": round(self.total_cash, 2),
            "btc_balance": round(self.btc_balance, 8),
            "btc_price": round(self.btc_price, 2),
            "portfolio_equity": round(self.portfolio_equity, 2),
            "unrealized_pnl_usd": round(self.unrealized_pnl_usd, 2),
            "unrealized_pnl_pct": round(self.unrealized_pnl_pct, 2),
            "benchmark_equity": round(self.benchmark_equity, 2),
        }


@dataclass
class PaperSummary:
    """Consolidated KPI metrics for the forward paper trading portfolio."""

    portfolio_id: str
    initial_cash: float
    total_equity: float
    unrealized_pnl_usd: float
    unrealized_pnl_pct: float
    base_cash: float
    reserve_cash: float
    total_cash: float
    btc_balance: float
    btc_value_usd: float
    avg_buy_price: float
    current_spot_price: float
    acquisition_discount_pct: float
    total_trades: int
    benchmark_equity: float
    outperformance_usd: float

    def to_dict(self) -> dict[str, Any]:
        """Convert summary to JSON-serializable dictionary matching /api/portfolio."""
        return {
            "portfolio_id": self.portfolio_id,
            "initial_cash": round(self.initial_cash, 2),
            "total_equity": round(self.total_equity, 2),
            "unrealized_pnl_usd": round(self.unrealized_pnl_usd, 2),
            "unrealized_pnl_pct": round(self.unrealized_pnl_pct, 2),
            "base_cash": round(self.base_cash, 2),
            "reserve_cash": round(self.reserve_cash, 2),
            "total_cash": round(self.total_cash, 2),
            "btc_balance": round(self.btc_balance, 8),
            "btc_value_usd": round(self.btc_value_usd, 2),
            "avg_buy_price": round(self.avg_buy_price, 2),
            "current_spot_price": round(self.current_spot_price, 2),
            "acquisition_discount_pct": round(self.acquisition_discount_pct, 2),
            "total_trades": self.total_trades,
            "benchmark_equity": round(self.benchmark_equity, 2),
            "outperformance_usd": round(self.outperformance_usd, 2),
        }


@dataclass(frozen=True)
class RiskCheckResult:
    """Pre-trade risk verification verdict from RiskGuard."""

    allowed: bool
    reason: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert risk check result to dictionary."""
        return asdict(self)
