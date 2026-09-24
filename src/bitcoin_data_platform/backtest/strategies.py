"""Investment strategies for backtesting and validation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

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


class WeeklyBatchDCAStrategy(BaseStrategy):
    """Weekly Batch Dollar-Cost Averaging Strategy.

    Batches routine capital contributions into a single weekly purchase order on the
    configured weekly cadence day (default Sunday UTC), holding without market execution
    on non-cadence days.
    """

    def __init__(
        self,
        config: BacktestConfig,
        cadence_weekday: int = 6,  # 6 = Sunday (in Python datetime.date.weekday)
    ) -> None:
        """Initialize Weekly Batch DCA strategy."""
        super().__init__(config)
        self.cadence_weekday = cadence_weekday

    def reset(self) -> None:
        """Reset internal strategy state."""
        pass

    def step(
        self,
        day: BacktestDayRecord,
        state: DailyPortfolioState,
        is_injection_day: bool,
    ) -> tuple[float, float, str]:
        is_cadence = (
            day.is_weekly_cadence_day
            if day.is_weekly_cadence_day is not None
            else (day.trade_date.weekday() == self.cadence_weekday)
        )

        if not is_cadence:
            return 0.0, 0.0, "HOLD: Accumulating cash for weekly batch"

        if state.cash_balance <= 0.0:
            return 0.0, 0.0, "HOLD: Weekly cadence due but zero cash balance"

        buy_usd = state.cash_balance
        return buy_usd, 0.0, f"WEEKLY_BATCH_DCA: Buy ${buy_usd:,.2f}"


class EventDrivenRegimeStrategy(BaseStrategy):
    """Event-Driven Regime Pacing and Asymmetric Drawdown Sniper Strategy (Phase 18).

    Operates a deterministic finite-state machine with state precedence:
      FROTH_FREEZE > SNIPER_DEPLOYMENT > WEEKLY_CORE > IDLE_CHOP

    Dual-Pool Capital Isolation:
      - 00 Base Pool: 40% of capital contributions (funds routine weekly core buys).
      - 00 Tactical Reserve Pool: 60% of capital contributions (funds tactical sniper tranches).

    Protective Constraints:
      - Base Runway Guard: Halves weekly buy when Base cash runway < 8 weeks.
      - Daily Tactical Cap: Max 15% of available Tactical Reserve per UTC day.
      - 48-Hour Velocity Bound: Aggregate Tactical debits <= 40% of window opening balance.
      - Episode Latch: Drawdown sniper fires once on entry and latches until recovery hysteresis.
      - Macro / Hard Safety Halt: Freezes all buying during macro events or black swans.
    """

    def __init__(
        self,
        config: BacktestConfig,
        weekly_base_usd: float = 20.0,
        initial_base_pct: float = 0.40,
        initial_tactical_pct: float = 0.60,
        sniper_request_pct: float = 0.25,
        daily_tactical_cap_pct: float = 0.15,
        tactical_48h_cap_pct: float = 0.40,
        runway_min_weeks: int = 8,
        low_runway_factor: float = 0.50,
        cadence_weekday: int = 6,  # Sunday UTC (0=Monday, 6=Sunday)
        sniper_return_24h_lte: float = -0.05,
        sniper_drawdown_7d_lte: float = -0.12,
        capitulation_mvrv_lt: float = 1.0,
        capitulation_mayer_lt: float = 0.8,
        froth_fng_gte: int = 80,
        froth_mayer_gte: float = 2.0,
        preservation_mvrv_gt: float = 2.5,
        preservation_mayer_gt: float = 2.2,
        drawdown_rearm_7d: float = -0.08,
    ) -> None:
        super().__init__(config)
        self.weekly_base_usd = weekly_base_usd
        self.initial_base_pct = initial_base_pct
        self.initial_tactical_pct = initial_tactical_pct
        self.sniper_request_pct = sniper_request_pct
        self.daily_tactical_cap_pct = daily_tactical_cap_pct
        self.tactical_48h_cap_pct = tactical_48h_cap_pct
        self.runway_min_weeks = runway_min_weeks
        self.low_runway_factor = low_runway_factor
        self.cadence_weekday = cadence_weekday
        self.sniper_return_24h_lte = sniper_return_24h_lte
        self.sniper_drawdown_7d_lte = sniper_drawdown_7d_lte
        self.capitulation_mvrv_lt = capitulation_mvrv_lt
        self.capitulation_mayer_lt = capitulation_mayer_lt
        self.froth_fng_gte = froth_fng_gte
        self.froth_mayer_gte = froth_mayer_gte
        self.preservation_mvrv_gt = preservation_mvrv_gt
        self.preservation_mayer_gt = preservation_mayer_gt
        self.drawdown_rearm_7d = drawdown_rearm_7d

        # Internal state tracking (zero lookahead bias)
        self.sniper_armed: bool = True
        self._initialized: bool = False
        self._prev_close: float | None = None
        self._prev_mvrv: float | None = None
        self._prev_mayer: float | None = None
        self._rolling_highs_7d: list[tuple[date, float]] = []
        self._tactical_debits: list[tuple[date, float]] = []

    def reset(self) -> None:
        """Reset internal simulation tracking state."""
        self.sniper_armed = True
        self._initialized = False
        self._prev_close = None
        self._prev_mvrv = None
        self._prev_mayer = None
        self._rolling_highs_7d.clear()
        self._tactical_debits.clear()

    def step(
        self,
        day: BacktestDayRecord,
        state: DailyPortfolioState,
        is_injection_day: bool,
    ) -> tuple[float, float, str]:
        # 1. Capital injection and dual-pool partitioning
        reserve_inflow = 0.0

        if not self._initialized:
            self._initialized = True
            # Day 0: If starting with lump sum / unpartitioned cash in state.cash_balance
            if state.reserve_cash_balance == 0.0 and state.cash_balance > 0.0:
                initial_reserve = round(state.cash_balance * self.initial_tactical_pct, 2)
                reserve_inflow += initial_reserve
            elif is_injection_day:
                periodic_inflow = self.config.periodic_amount
                reserve_inflow += round(periodic_inflow * self.initial_tactical_pct, 2)
        elif is_injection_day:
            periodic_inflow = self.config.periodic_amount
            reserve_inflow += round(periodic_inflow * self.initial_tactical_pct, 2)

        # 2. Chronological observation derived indicators (zero lookahead)
        close_p = day.market_close_usd

        # 24h Return
        if day.return_24h is not None:
            r24: float | None = day.return_24h
        elif self._prev_close is not None and self._prev_close > 0.0:
            r24 = (close_p / self._prev_close) - 1.0
        else:
            r24 = None

        # 7-day rolling peak and drawdown
        self._rolling_highs_7d.append((day.trade_date, close_p))
        self._rolling_highs_7d = [
            (d, h) for (d, h) in self._rolling_highs_7d if (day.trade_date - d).days < 7
        ]
        peak_7d = max(h for _, h in self._rolling_highs_7d)

        if day.drawdown_7d is not None:
            dd7: float = day.drawdown_7d
        else:
            dd7 = (close_p / peak_7d - 1.0) if peak_7d > 0.0 else 0.0

        # Weekly cadence day
        if day.is_weekly_cadence_day is not None:
            is_weekly = bool(day.is_weekly_cadence_day)
        else:
            is_weekly = day.trade_date.weekday() == self.cadence_weekday

        # Capitulation crossings
        mvrv_cross = (
            day.mvrv_ratio is not None
            and self._prev_mvrv is not None
            and day.mvrv_ratio < self.capitulation_mvrv_lt
            and self._prev_mvrv >= self.capitulation_mvrv_lt
        )
        mayer_cross = (
            day.mayer_multiple is not None
            and self._prev_mayer is not None
            and day.mayer_multiple < self.capitulation_mayer_lt
            and self._prev_mayer >= self.capitulation_mayer_lt
        )
        if day.is_regime_capitulation is not None:
            is_cap = bool(day.is_regime_capitulation)
        else:
            is_cap = bool(mvrv_cross or mayer_cross)

        # Froth predicates
        fng_mayer_froth = (
            day.fng_value is not None
            and day.mayer_multiple is not None
            and day.fng_value >= self.froth_fng_gte
            and day.mayer_multiple >= self.froth_mayer_gte
        )
        mvrv_preservation = (
            day.mvrv_ratio is not None
            and self._prev_mvrv is not None
            and day.mvrv_ratio > self.preservation_mvrv_gt
            and self._prev_mvrv <= self.preservation_mvrv_gt
        )
        mayer_preservation = (
            day.mayer_multiple is not None
            and self._prev_mayer is not None
            and day.mayer_multiple > self.preservation_mayer_gt
            and self._prev_mayer <= self.preservation_mayer_gt
        )
        if day.is_regime_froth is not None:
            is_froth = bool(day.is_regime_froth)
        else:
            is_froth = bool(fng_mayer_froth or mvrv_preservation or mayer_preservation)

        # Drawdown event
        if day.is_drawdown_event is not None:
            is_dd = bool(day.is_drawdown_event)
        else:
            is_dd = bool(
                (r24 is not None and r24 <= self.sniper_return_24h_lte)
                or dd7 <= self.sniper_drawdown_7d_lte
            )

        # Hard freeze / macro halt
        is_hard_safety = day.has_high_impact_macro_event or (
            (day.investment_signal or "").upper() == "HARD_FREEZE"
        )

        # 3. State Evaluation & Precedence
        # State 1: FROTH_FREEZE (Highest Precedence)
        if is_hard_safety or is_froth:
            reason_code = "HARD_RISK_FREEZE" if is_hard_safety else "FROTH_FNG_MAYER"
            if is_froth and not is_hard_safety and (mvrv_preservation or mayer_preservation):
                reason_code = "CAPITAL_PRESERVATION_VALUATION"

            self._update_history(close_p, day.mvrv_ratio, day.mayer_multiple)
            return (
                0.0,
                reserve_inflow,
                f"FROTH_FREEZE: Suspended accumulation ({reason_code})",
            )

        # State 2: SNIPER_DEPLOYMENT
        if (is_dd or is_cap) and self.sniper_armed:
            self.sniper_armed = False  # Latch episode

            if is_cap:
                reason_code = "CAPITULATION_EDGE_MVRV" if mvrv_cross else "CAPITULATION_EDGE_MAYER"
            elif r24 is not None and r24 <= self.sniper_return_24h_lte:
                reason_code = "DRAWDOWN_EDGE_24H"
            else:
                reason_code = "DRAWDOWN_EDGE_7D"

            available_tactical = max(0.0, state.reserve_cash_balance + reserve_inflow)
            requested_tactical = available_tactical * self.sniper_request_pct
            daily_cap = available_tactical * self.daily_tactical_cap_pct

            # 48-Hour Velocity Bound
            tactical_debits_48h = sum(
                amt for d, amt in self._tactical_debits if (day.trade_date - d).days <= 2
            )
            opening_tactical = available_tactical + tactical_debits_48h
            remaining_48h_cap = max(
                0.0, (self.tactical_48h_cap_pct * opening_tactical) - tactical_debits_48h
            )

            authorized_tactical = min(
                requested_tactical,
                available_tactical,
                daily_cap,
                remaining_48h_cap,
            )

            self._update_history(close_p, day.mvrv_ratio, day.mayer_multiple)

            if authorized_tactical > 0.0:
                self._tactical_debits.append((day.trade_date, authorized_tactical))
                net_reserve_transfer = reserve_inflow - authorized_tactical
                desc = (
                    f"SNIPER_DEPLOYMENT: Buy ${authorized_tactical:,.2f} from Tactical Reserve "
                    f"({reason_code})"
                )
                return (
                    authorized_tactical,
                    net_reserve_transfer,
                    desc,
                )
            else:
                return (
                    0.0,
                    reserve_inflow,
                    f"SNIPER_DEPLOYMENT: Clamped to $0.00 ({reason_code})",
                )

        # Re-arm check: if not in drawdown, check recovery hysteresis
        if (
            not is_dd
            and not is_cap
            and dd7 > self.drawdown_rearm_7d
            and (r24 is None or r24 > -0.02)
        ):
            self.sniper_armed = True

        # State 3: WEEKLY_CORE
        if is_weekly:
            available_base = max(0.0, state.cash_balance - reserve_inflow)
            runway_weeks = (
                available_base / self.weekly_base_usd if self.weekly_base_usd > 0.0 else 0.0
            )

            requested_base = self.weekly_base_usd
            is_low_runway = runway_weeks < self.runway_min_weeks
            if is_low_runway:
                requested_base *= self.low_runway_factor
                reason_code = "BASE_RUNWAY_REDUCTION"
            else:
                reason_code = "WEEKLY_CADENCE_DUE"

            authorized_base = min(requested_base, available_base)

            self._update_history(close_p, day.mvrv_ratio, day.mayer_multiple)

            if authorized_base > 0.0:
                desc = (
                    f"WEEKLY_CORE: Buy ${authorized_base:,.2f} from Base Cash "
                    f"({reason_code}, runway={runway_weeks:.1f}w)"
                )
                return authorized_base, reserve_inflow, desc
            else:
                return 0.0, reserve_inflow, "WEEKLY_CORE: Zero Base cash available"

        # State 4: IDLE_CHOP (Lowest Precedence)
        self._update_history(close_p, day.mvrv_ratio, day.mayer_multiple)
        return 0.0, reserve_inflow, "IDLE_CHOP: Preserving capital, zero execution (NO_EVENT_CHOP)"

    def _update_history(
        self,
        close: float,
        mvrv: float | None,
        mayer: float | None,
    ) -> None:
        """Update chronological history with today's values for next day's calculations."""
        self._prev_close = close
        if mvrv is not None:
            self._prev_mvrv = mvrv
        if mayer is not None:
            self._prev_mayer = mayer
