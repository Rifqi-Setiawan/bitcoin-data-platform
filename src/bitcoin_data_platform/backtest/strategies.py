"""Investment strategies for backtesting and validation."""

from __future__ import annotations

from abc import ABC, abstractmethod

from bitcoin_data_platform.backtest.models import (
    BacktestConfig,
    BacktestDayRecord,
    DailyPortfolioState,
)


class BaseStrategy(ABC):
    """Abstract base class for all systematic backtest strategies."""

    def __init__(self, config: BacktestConfig) -> None:
        """Initialize strategy with backtest configuration."""
        self.config = config

    @abstractmethod
    def step(
        self,
        day: BacktestDayRecord,
        state: DailyPortfolioState,
        is_injection_day: bool,
    ) -> tuple[float, float, str]:
        """Advance strategy by one simulation step (day).

        Args:
            day: Market indicators and signals for the current day.
            state: Current portfolio state prior to today's order execution.
            is_injection_day: True if today is a scheduled periodic contribution date.

        Returns:
            Tuple of (buy_amount_usd, reserve_add_usd, action_description).
            - buy_amount_usd: Total USD capital allocated to purchase BTC today.
            - reserve_add_usd: Net USD transfer from base cash into tactical reserve.
            - action_description: Human-readable narrative of the action taken.
        """
        ...


class LumpSumStrategy(BaseStrategy):
    """Lump Sum Buy & Hold Strategy.

    Deploys 100% of initial cash on Day 0, with zero subsequent capital contributions.
    """

    def __init__(self, config: BacktestConfig) -> None:
        super().__init__(config)
        self._deployed: bool = False

    def reset(self) -> None:
        self._deployed = False

    def step(
        self,
        day: BacktestDayRecord,
        state: DailyPortfolioState,
        is_injection_day: bool,
    ) -> tuple[float, float, str]:
        if not self._deployed and state.cash_balance > 0.0:
            self._deployed = True
            buy_usd = state.cash_balance
            return buy_usd, 0.0, f"LUMP_SUM: Deployed initial cash (${buy_usd:,.2f})"

        return 0.0, 0.0, "HOLD: Lump sum capital fully deployed"


class BlindDCAStrategy(BaseStrategy):
    """Blind Dollar-Cost Averaging Strategy.

    Invests 100% of periodic contribution amount unconditionally on every injection day,
    regardless of valuation indicators, sentiment, or macro events.
    """

    def step(
        self,
        day: BacktestDayRecord,
        state: DailyPortfolioState,
        is_injection_day: bool,
    ) -> tuple[float, float, str]:
        if not is_injection_day:
            return 0.0, 0.0, "HOLD (Non-injection day)"

        buy_usd = min(self.config.periodic_amount, state.cash_balance)
        return buy_usd, 0.0, f"BLIND_DCA: Buy ${buy_usd:,.2f}"


class DynamicReserveDCAStrategy(BaseStrategy):
    """Dynamic Reserve DCA + Macro Regime Overlay Strategy.

    Allocates periodic budget into Base Pool (70%) and Tactical Reserve Pool (30%).
    Modulates purchase sizes according to analytical investment signals:
      - AGGRESSIVE_ACCUMULATE: 2.0x Base + 25% draw from existing tactical reserve
      - OPPORTUNISTIC_ACCUMULATE: 1.3x Base
      - STANDARD_DCA: 1.0x Base
      - DEFENSIVE_RESERVE: 0.5x Base (remaining 50% base diverted into reserve)
      - HARD_FREEZE: 0x Base (100% base diverted into reserve)
    Macro Circuit Breaker: Halts purchasing when high-impact USD macro events occur,
    diverting 100% of the periodic contribution into tactical reserve.
    """

    def step(
        self,
        day: BacktestDayRecord,
        state: DailyPortfolioState,
        is_injection_day: bool,
    ) -> tuple[float, float, str]:
        if not is_injection_day:
            return 0.0, 0.0, "HOLD (Non-injection day)"

        budget = self.config.periodic_amount

        # 1. Macro Circuit Breaker
        if day.has_high_impact_macro_event:
            return (
                0.0,
                budget,
                f"MACRO_CIRCUIT_BREAKER: Event pause, diverted ${budget:,.2f} to reserve",
            )

        base_allocation = 0.70 * budget
        reserve_allocation = 0.30 * budget

        signal = (day.investment_signal or "STANDARD_DCA").upper().strip()

        # 2. Hard Freeze: Overheated market, zero purchase, 100% budget to tactical reserve
        if signal == "HARD_FREEZE":
            total_reserve_transfer = reserve_allocation + base_allocation
            return (
                0.0,
                total_reserve_transfer,
                f"HARD_FREEZE: Overheated, ${total_reserve_transfer:,.2f} to reserve",
            )

        # 3. Defensive Reserve: Elevated risk, 50% base buy, 50% base diverted to reserve
        elif signal == "DEFENSIVE_RESERVE":
            base_buy_target = 0.50 * base_allocation
            diverted_to_reserve = 0.50 * base_allocation
            total_reserve_transfer = reserve_allocation + diverted_to_reserve
            available_base = max(0.0, state.cash_balance - total_reserve_transfer)
            actual_buy = min(base_buy_target, available_base)
            return (
                actual_buy,
                total_reserve_transfer,
                f"DEFENSIVE_RESERVE: Buy ${actual_buy:,.2f} (0.5x base), remainder to reserve",
            )

        # 4. Aggressive Accumulate: Deep undervaluation, 2.0x base buy + 25% reserve draw
        elif signal == "AGGRESSIVE_ACCUMULATE":
            base_buy_target = 2.0 * base_allocation
            reserve_draw = 0.25 * max(0.0, state.reserve_cash_balance)
            net_reserve_transfer = reserve_allocation - reserve_draw
            available_base = max(0.0, state.cash_balance - reserve_allocation)
            actual_base_buy = min(base_buy_target, available_base)
            total_buy = actual_base_buy + reserve_draw
            return (
                total_buy,
                net_reserve_transfer,
                f"AGGRESSIVE_ACCUMULATE: Buy ${total_buy:,.2f} (2.0x base + 25% reserve draw)",
            )

        # 5. Opportunistic Accumulate: Undervalued market, 1.3x base buy
        elif signal == "OPPORTUNISTIC_ACCUMULATE":
            base_buy_target = 1.30 * base_allocation
            available_base = max(0.0, state.cash_balance - reserve_allocation)
            actual_buy = min(base_buy_target, available_base)
            return (
                actual_buy,
                reserve_allocation,
                f"OPPORTUNISTIC_ACCUMULATE: Buy ${actual_buy:,.2f} (1.3x base)",
            )

        # 6. Standard DCA: Fair value accumulation, 1.0x base buy
        else:
            base_buy_target = 1.0 * base_allocation
            available_base = max(0.0, state.cash_balance - reserve_allocation)
            actual_buy = min(base_buy_target, available_base)
            return (
                actual_buy,
                reserve_allocation,
                f"STANDARD_DCA: Buy ${actual_buy:,.2f} (1.0x base)",
            )
