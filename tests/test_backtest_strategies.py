"""Tests for backtest strategy decision logic and allocation algorithms."""

from datetime import date

from bitcoin_data_platform.backtest.models import (
    BacktestConfig,
    BacktestDayRecord,
    DailyPortfolioState,
)
from bitcoin_data_platform.backtest.strategies import (
    BlindDCAStrategy,
    DynamicReserveDCAStrategy,
    EventDrivenRegimeStrategy,
    LumpSumStrategy,
    WeeklyBatchDCAStrategy,
)


def _make_day(
    signal: str | None = "STANDARD_DCA",
    macro: bool = False,
    price: float = 50000.0,
    d: date | None = None,
    drawdown_7d: float | None = None,
    drawdown_30d: float | None = None,
    return_24h: float | None = None,
    is_weekly_cadence_day: bool | None = None,
    is_drawdown_event: bool | None = None,
    is_regime_capitulation: bool | None = None,
    is_regime_froth: bool | None = None,
    mvrv_ratio: float | None = 1.5,
    mayer_multiple: float | None = None,
    fng_value: int | None = 50,
) -> BacktestDayRecord:
    mm = mayer_multiple if mayer_multiple is not None else (price / 48000.0)
    return BacktestDayRecord(
        trade_date=d or date(2025, 1, 1),
        market_close_usd=price,
        sma_200=48000.0,
        mayer_multiple=mm,
        mvrv_ratio=mvrv_ratio,
        fng_value=fng_value,
        has_high_impact_macro_event=macro,
        investment_signal=signal,
        drawdown_7d=drawdown_7d,
        drawdown_30d=drawdown_30d,
        return_24h=return_24h,
        is_weekly_cadence_day=is_weekly_cadence_day,
        is_drawdown_event=is_drawdown_event,
        is_regime_capitulation=is_regime_capitulation,
        is_regime_froth=is_regime_froth,
    )


def _make_state(
    cash: float = 100.0,
    reserve: float = 0.0,
    btc: float = 0.0,
    price: float = 50000.0,
) -> DailyPortfolioState:
    return DailyPortfolioState(
        trade_date=date(2025, 1, 1),
        cash_balance=cash,
        reserve_cash_balance=reserve,
        btc_balance=btc,
        btc_price=price,
        portfolio_equity=cash + reserve + btc * price,
        total_contributed=cash + reserve,
        daily_cash_flow=cash,
        daily_return=0.0,
        drawdown=0.0,
        action_taken="",
    )


class TestLumpSumStrategy:
    def test_day_zero_deployment(self) -> None:
        cfg = BacktestConfig(initial_cash=10000.0)
        strat = LumpSumStrategy(cfg)
        day = _make_day()
        state = _make_state(cash=10000.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 10000.0
        assert reserve_add == 0.0
        assert "Deployed initial cash" in desc

    def test_subsequent_days_hold(self) -> None:
        cfg = BacktestConfig(initial_cash=10000.0)
        strat = LumpSumStrategy(cfg)
        day = _make_day()
        state = _make_state(cash=10000.0)

        # Day 0
        strat.step(day, state, is_injection_day=True)

        # Day 1
        state_day1 = _make_state(cash=0.0, btc=0.2)
        buy, reserve_add, desc = strat.step(day, state_day1, is_injection_day=False)
        assert buy == 0.0
        assert reserve_add == 0.0
        assert "HOLD" in desc

    def test_reset_allows_redeployment(self) -> None:
        cfg = BacktestConfig(initial_cash=5000.0)
        strat = LumpSumStrategy(cfg)
        day = _make_day()
        state = _make_state(cash=5000.0)

        strat.step(day, state, is_injection_day=True)
        strat.reset()

        buy, _, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 5000.0
        assert "Deployed initial cash" in desc


class TestBlindDCAStrategy:
    def test_non_injection_day_holds(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = BlindDCAStrategy(cfg)
        day = _make_day()
        state = _make_state(cash=100.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=False)
        assert buy == 0.0
        assert reserve_add == 0.0
        assert "HOLD" in desc

    def test_injection_day_buys_full_periodic_amount(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = BlindDCAStrategy(cfg)
        day = _make_day()
        state = _make_state(cash=100.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 100.0
        assert reserve_add == 0.0
        assert "BLIND_DCA: Buy $100.00" in desc

    def test_injection_day_caps_at_available_cash(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = BlindDCAStrategy(cfg)
        day = _make_day()
        state = _make_state(cash=45.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 45.0
        assert reserve_add == 0.0


class TestDynamicReserveDCAStrategy:
    def test_non_injection_day_holds(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal="AGGRESSIVE_ACCUMULATE")
        state = _make_state(cash=100.0, reserve=500.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=False)
        assert buy == 0.0
        assert reserve_add == 0.0
        assert "HOLD" in desc

    def test_macro_circuit_breaker(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal="AGGRESSIVE_ACCUMULATE", macro=True)
        state = _make_state(cash=100.0, reserve=200.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 0.0
        assert reserve_add == 100.0
        assert "MACRO_CIRCUIT_BREAKER" in desc

    def test_hard_freeze_signal(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal="HARD_FREEZE")
        state = _make_state(cash=100.0, reserve=100.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 0.0
        assert reserve_add == 100.0
        assert "HARD_FREEZE" in desc

    def test_defensive_reserve_signal(self) -> None:
        # budget 100: base=70, res=30.
        # buy = 0.5 * 70 = 35. diverted = 35. total reserve add = 30 + 35 = 65.
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal="DEFENSIVE_RESERVE")
        state = _make_state(cash=100.0, reserve=100.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 35.0
        assert reserve_add == 65.0
        assert "DEFENSIVE_RESERVE" in desc

    def test_standard_dca_signal(self) -> None:
        # budget 100: base=70, res=30.
        # buy = 1.0 * 70 = 70. reserve add = 30.
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal="STANDARD_DCA")
        state = _make_state(cash=100.0, reserve=50.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 70.0
        assert reserve_add == 30.0
        assert "STANDARD_DCA" in desc

    def test_opportunistic_accumulate_signal(self) -> None:
        # budget 100: base=70, res=30. target buy = 1.3 * 70 = 91.
        # available cash = cash_balance - 30 = 70.
        # since available base is 70, capped at 70 if cash_balance is exactly 100.
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal="OPPORTUNISTIC_ACCUMULATE")

        # Case A: exactly 100 injected today, no prior base cash -> capped at 70
        state1 = _make_state(cash=100.0, reserve=50.0)
        buy1, reserve_add1, _ = strat.step(day, state1, is_injection_day=True)
        assert buy1 == 70.0
        assert reserve_add1 == 30.0

        # Case B: leftover base cash from previous defensive periods -> can buy full 91
        state2 = _make_state(cash=150.0, reserve=50.0)
        buy2, reserve_add2, _ = strat.step(day, state2, is_injection_day=True)
        assert buy2 == 91.0
        assert reserve_add2 == 30.0

    def test_aggressive_accumulate_with_reserve_draw(self) -> None:
        # budget 100: base=70, res=30.
        # target base buy = 2.0 * 70 = 140.
        # reserve = 400. reserve draw = 0.25 * 400 = 100.
        # net reserve add = 30 - 100 = -70.
        # available base = cash (100) - 30 = 70 -> base buy = min(140, 70) = 70.
        # total buy = base buy (70) + reserve draw (100) = 170.
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal="AGGRESSIVE_ACCUMULATE")
        state = _make_state(cash=100.0, reserve=400.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 170.0
        assert reserve_add == -70.0
        assert "AGGRESSIVE_ACCUMULATE" in desc

    def test_aggressive_accumulate_zero_reserve(self) -> None:
        # reserve = 0 -> reserve draw = 0.
        # net reserve add = 30 - 0 = 30.
        # base buy = 70. total buy = 70.
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal="AGGRESSIVE_ACCUMULATE")
        state = _make_state(cash=100.0, reserve=0.0)

        buy, reserve_add, _ = strat.step(day, state, is_injection_day=True)
        assert buy == 70.0
        assert reserve_add == 30.0

    def test_fallback_signal_defaults_to_standard_dca(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = DynamicReserveDCAStrategy(cfg)
        day = _make_day(signal=None)
        state = _make_state(cash=100.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 70.0
        assert reserve_add == 30.0
        assert "STANDARD_DCA" in desc


class TestWeeklyBatchDCAStrategy:
    """Unit tests for WeeklyBatchDCAStrategy."""

    def test_non_cadence_day_holds_and_accumulates(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = WeeklyBatchDCAStrategy(cfg)
        # 2025-01-01 is Wednesday (weekday 2 != 6)
        day = _make_day(d=date(2025, 1, 1))
        state = _make_state(cash=300.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=False)
        assert buy == 0.0
        assert reserve_add == 0.0
        assert "HOLD: Accumulating" in desc

    def test_cadence_day_buys_full_accumulated_cash(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = WeeklyBatchDCAStrategy(cfg)
        # 2025-01-05 is Sunday (weekday 6)
        day = _make_day(d=date(2025, 1, 5))
        state = _make_state(cash=700.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 700.0
        assert reserve_add == 0.0
        assert "WEEKLY_BATCH_DCA: Buy $700.00" in desc

    def test_cadence_day_zero_cash_holds(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = WeeklyBatchDCAStrategy(cfg)
        day = _make_day(d=date(2025, 1, 5))
        state = _make_state(cash=0.0)

        buy, reserve_add, desc = strat.step(day, state, is_injection_day=False)
        assert buy == 0.0
        assert reserve_add == 0.0
        assert "HOLD: Weekly cadence due but zero cash balance" in desc

    def test_explicit_is_weekly_cadence_day_flag_override(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = WeeklyBatchDCAStrategy(cfg)

        # Wednesday, but flag is explicitly True -> should buy
        day_wed_true = _make_day(d=date(2025, 1, 1), is_weekly_cadence_day=True)
        state1 = _make_state(cash=500.0)
        buy1, _, desc1 = strat.step(day_wed_true, state1, is_injection_day=True)
        assert buy1 == 500.0
        assert "WEEKLY_BATCH_DCA" in desc1

        # Sunday, but flag is explicitly False -> should hold
        day_sun_false = _make_day(d=date(2025, 1, 5), is_weekly_cadence_day=False)
        state2 = _make_state(cash=500.0)
        buy2, _, desc2 = strat.step(day_sun_false, state2, is_injection_day=True)
        assert buy2 == 0.0
        assert "HOLD: Accumulating" in desc2

    def test_custom_cadence_weekday(self) -> None:
        cfg = BacktestConfig(periodic_amount=100.0)
        # Custom weekday: Monday (0)
        strat = WeeklyBatchDCAStrategy(cfg, cadence_weekday=0)

        # Monday 2025-01-06 -> buys
        day_mon = _make_day(d=date(2025, 1, 6))
        state = _make_state(cash=250.0)
        buy, _, desc = strat.step(day_mon, state, is_injection_day=True)
        assert buy == 250.0
        assert "WEEKLY_BATCH_DCA" in desc

        # Sunday 2025-01-05 -> holds
        day_sun = _make_day(d=date(2025, 1, 5))
        buy_sun, _, desc_sun = strat.step(day_sun, state, is_injection_day=True)
        assert buy_sun == 0.0
        assert "HOLD: Accumulating" in desc_sun

    def test_reset_method(self) -> None:
        cfg = BacktestConfig()
        strat = WeeklyBatchDCAStrategy(cfg)
        strat.reset()  # should run cleanly


class TestEventDrivenRegimeStrategy:
    """Comprehensive unit tests for EventDrivenRegimeStrategy (Phase 18)."""

    def test_state_precedence_froth_over_all(self) -> None:
        """AC-1, AC-4: FROTH_FREEZE dominates drawdown and weekly cadence triggers."""
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Sunday with drawdown crash (-15%) BUT FNG=85 & Mayer=2.1 (froth overheat)
        day = _make_day(
            d=date(2025, 1, 5),  # Sunday
            price=90000.0,
            mayer_multiple=2.1,
            fng_value=85,
            drawdown_7d=-0.15,
            return_24h=-0.08,
            is_weekly_cadence_day=True,
            is_drawdown_event=True,
            is_regime_froth=True,
        )
        state = _make_state(cash=400.0, reserve=600.0)

        buy, res_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 0.0
        # 60% of periodic 100 goes to tactical reserve safely
        assert res_add == 60.0
        assert "FROTH_FREEZE" in desc
        assert "FROTH_FNG_MAYER" in desc

    def test_state_precedence_hard_safety_macro_over_all(self) -> None:
        """AC-4, AC-10: Macro event and HARD_FREEZE signal trigger FROTH_FREEZE."""
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Drawdown trigger present on Sunday, but high-impact USD macro event active
        day = _make_day(
            d=date(2025, 1, 5),
            macro=True,
            drawdown_7d=-0.14,
            is_drawdown_event=True,
            is_weekly_cadence_day=True,
        )
        state = _make_state(cash=400.0, reserve=600.0)

        buy, res_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 0.0
        assert res_add == 60.0
        assert "FROTH_FREEZE" in desc
        assert "HARD_RISK_FREEZE" in desc

    def test_state_precedence_sniper_over_weekly_on_collision(self) -> None:
        """AC-1, Section 6.1: On collision (drawdown edge + Sunday), SNIPER_DEPLOYMENT wins."""
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Sunday 2025-01-05 with return_24h = -0.06 (<= -0.05 drawdown edge)
        day = _make_day(
            d=date(2025, 1, 5),
            price=47000.0,
            return_24h=-0.06,
            is_weekly_cadence_day=True,
            is_drawdown_event=True,
        )
        state = _make_state(cash=400.0, reserve=600.0)

        buy, res_add, desc = strat.step(day, state, is_injection_day=True)
        # Tactical reserve: 600 + 60 inflow = 660. 15% daily cap = 99.0
        assert buy == 99.0
        # net reserve transfer = 60 inflow - 99 buy = -39.0
        assert res_add == -39.0
        assert "SNIPER_DEPLOYMENT" in desc
        assert "DRAWDOWN_EDGE_24H" in desc

    def test_idle_chop_neutral_market(self) -> None:
        """AC-2: Neutral/chop day without cadence or drawdown produces zero market orders."""
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Wednesday, price flat, normal valuation
        day = _make_day(d=date(2025, 1, 1), price=50000.0)
        state = _make_state(cash=400.0, reserve=600.0)

        buy, res_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 0.0
        assert res_add == 60.0
        assert "IDLE_CHOP" in desc
        assert "NO_EVENT_CHOP" in desc

    def test_weekly_core_base_runway_above_8_weeks(self) -> None:
        """AC-3, AC-7: Sunday UTC with runway >= 8 weeks buys full weekly base ($20.00)."""
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = EventDrivenRegimeStrategy(cfg, weekly_base_usd=20.0)

        # Sunday, Base cash = 400.0. Inflow = 100 (40 base / 60 res).
        # Base available = 400. Runway = 400 / 20 = 20 weeks >= 8.
        day = _make_day(d=date(2025, 1, 5), price=50000.0, is_weekly_cadence_day=True)
        state = _make_state(cash=500.0, reserve=600.0)  # 500 has 100 incoming

        buy, res_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 20.0
        assert res_add == 60.0  # 60 to reserve, base unaffected
        assert "WEEKLY_CORE" in desc
        assert "WEEKLY_CADENCE_DUE" in desc

    def test_weekly_core_base_runway_below_8_weeks_halved(self) -> None:
        """AC-7: Sunday UTC with runway < 8 weeks halves weekly base buy to $10.00."""
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = EventDrivenRegimeStrategy(cfg, weekly_base_usd=20.0)

        # Sunday, state cash = 120.0 (has 60 incoming reserve inflow).
        # Available base = 120 - 60 = 60.0.
        # Runway = 60 / 20 = 3.0 weeks < 8 weeks -> buy halved to 10.0.
        day = _make_day(d=date(2025, 1, 5), price=50000.0, is_weekly_cadence_day=True)
        state = _make_state(cash=120.0, reserve=600.0)

        buy, res_add, desc = strat.step(day, state, is_injection_day=True)
        assert buy == 10.0
        assert res_add == 60.0
        assert "WEEKLY_CORE" in desc
        assert "BASE_RUNWAY_REDUCTION" in desc

    def test_weekly_core_zero_base_cash(self) -> None:
        """Sunday UTC with 0 base cash available executes zero order."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(cfg, weekly_base_usd=20.0)

        day = _make_day(d=date(2025, 1, 5), is_weekly_cadence_day=True)
        state = _make_state(cash=0.0, reserve=600.0)

        buy, res_add, desc = strat.step(day, state, is_injection_day=False)
        assert buy == 0.0
        assert res_add == 0.0
        assert "Zero Base cash available" in desc

    def test_sniper_deployment_return_24h_trigger(self) -> None:
        """AC-3, AC-7: Return 24h <= -5% triggers tactical sniper debit from Tactical Reserve."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(cfg)

        day = _make_day(d=date(2025, 1, 2), return_24h=-0.07, is_drawdown_event=True)
        state = _make_state(cash=400.0, reserve=600.0)

        # 15% daily cap of 600 = 90.0
        buy, res_add, desc = strat.step(day, state, is_injection_day=False)
        assert buy == 90.0
        assert res_add == -90.0  # net reserve transfer is negative (draw from reserve)
        assert "SNIPER_DEPLOYMENT" in desc
        assert "DRAWDOWN_EDGE_24H" in desc

    def test_sniper_deployment_drawdown_7d_trigger(self) -> None:
        """AC-3, AC-7: Drawdown 7d <= -12% triggers tactical sniper debit."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(cfg)

        day = _make_day(d=date(2025, 1, 2), drawdown_7d=-0.14, is_drawdown_event=True)
        state = _make_state(cash=400.0, reserve=600.0)

        buy, res_add, desc = strat.step(day, state, is_injection_day=False)
        assert buy == 90.0
        assert res_add == -90.0
        assert "SNIPER_DEPLOYMENT" in desc
        assert "DRAWDOWN_EDGE_7D" in desc

    def test_sniper_deployment_capitulation_mvrv_crossing(self) -> None:
        """Section 5: Downward crossing of MVRV 1.0 triggers capitulation sniper."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Day 0: MVRV = 1.05
        day0 = _make_day(d=date(2025, 1, 1), mvrv_ratio=1.05)
        state0 = _make_state(cash=400.0, reserve=600.0)
        strat.step(day0, state0, is_injection_day=False)

        # Day 1: MVRV drops to 0.95 (< 1.0 crossing)
        day1 = _make_day(d=date(2025, 1, 2), mvrv_ratio=0.95)
        state1 = _make_state(cash=400.0, reserve=600.0)
        buy, res_add, desc = strat.step(day1, state1, is_injection_day=False)
        assert buy == 90.0
        assert res_add == -90.0
        assert "CAPITULATION_EDGE_MVRV" in desc

    def test_sniper_deployment_capitulation_mayer_crossing(self) -> None:
        """Section 5: Downward crossing of Mayer 0.8 triggers capitulation sniper."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Day 0: Mayer = 0.85
        day0 = _make_day(d=date(2025, 1, 1), mayer_multiple=0.85)
        state0 = _make_state(cash=400.0, reserve=600.0)
        strat.step(day0, state0, is_injection_day=False)

        # Day 1: Mayer drops to 0.75 (< 0.8 crossing)
        day1 = _make_day(d=date(2025, 1, 2), mayer_multiple=0.75)
        state1 = _make_state(cash=400.0, reserve=600.0)
        buy, res_add, desc = strat.step(day1, state1, is_injection_day=False)
        assert buy == 90.0
        assert res_add == -90.0
        assert "CAPITULATION_EDGE_MAYER" in desc

    def test_episode_latching_prevents_repeated_sniper(self) -> None:
        """AC-6, F-03: Persistent drawdown level fires sniper once, then latches."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Day 1: Enters drawdown (-14%) -> fires sniper
        day1 = _make_day(d=date(2025, 1, 2), drawdown_7d=-0.14, is_drawdown_event=True)
        state1 = _make_state(cash=400.0, reserve=600.0)
        buy1, _, desc1 = strat.step(day1, state1, is_injection_day=False)
        assert buy1 == 90.0
        assert "SNIPER_DEPLOYMENT" in desc1

        # Day 2: Still in deep drawdown (-15%) -> latched episode suppresses sniper!
        day2 = _make_day(d=date(2025, 1, 3), drawdown_7d=-0.15, is_drawdown_event=True)
        state2 = _make_state(cash=400.0, reserve=510.0)
        buy2, _, desc2 = strat.step(day2, state2, is_injection_day=False)
        assert buy2 == 0.0
        assert "IDLE_CHOP" in desc2

        # Day 3: Still in drawdown (-14%) -> still latched!
        day3 = _make_day(d=date(2025, 1, 4), drawdown_7d=-0.14, is_drawdown_event=True)
        state3 = _make_state(cash=400.0, reserve=510.0)
        buy3, _, desc3 = strat.step(day3, state3, is_injection_day=False)
        assert buy3 == 0.0
        assert "IDLE_CHOP" in desc3

    def test_episode_rearm_after_recovery_hysteresis(self) -> None:
        """AC-6, F-12: Sniper re-arms only after recovery past hysteresis threshold (-8%)."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Day 1: Crash -14% -> fires sniper
        day1 = _make_day(d=date(2025, 1, 2), drawdown_7d=-0.14, is_drawdown_event=True)
        state1 = _make_state(cash=400.0, reserve=600.0)
        strat.step(day1, state1, is_injection_day=False)

        # Day 2: Market recovers! Drawdown improves to -4% (> -8% re-arm threshold)
        day2 = _make_day(d=date(2025, 1, 3), drawdown_7d=-0.04, return_24h=0.05)
        state2 = _make_state(cash=400.0, reserve=510.0)
        strat.step(day2, state2, is_injection_day=False)
        assert strat.sniper_armed is True

        # Day 3: Another drawdown event (-13%) -> sniper fires again!
        day3 = _make_day(d=date(2025, 1, 4), drawdown_7d=-0.13, is_drawdown_event=True)
        state3 = _make_state(cash=400.0, reserve=510.0)
        buy3, _, desc3 = strat.step(day3, state3, is_injection_day=False)
        assert buy3 > 0.0
        assert "SNIPER_DEPLOYMENT" in desc3

    def test_tactical_48h_velocity_bound(self) -> None:
        """AC-7: Rolling 48h tactical debits are capped at 40% of window opening reserve."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(
            cfg,
            daily_tactical_cap_pct=0.30,  # loosen daily cap to test 48h velocity cap
            sniper_request_pct=0.35,
            tactical_48h_cap_pct=0.40,
        )

        # Day 1: Debit from 1000 reserve. 30% daily cap = 300.
        day1 = _make_day(d=date(2025, 1, 1), return_24h=-0.06, is_drawdown_event=True)
        state1 = _make_state(cash=400.0, reserve=1000.0)
        buy1, _, _ = strat.step(day1, state1, is_injection_day=False)
        assert buy1 == 300.0

        # Day 2: Another crash (re-arm artificially for velocity test)
        strat.sniper_armed = True
        # Reserve now has 700. Window opening balance = 700 + 300 = 1000.
        # Max 48h allowed = 40% * 1000 = 400. Already spent = 300.
        # Remaining 48h cap = 100.
        day2 = _make_day(d=date(2025, 1, 2), return_24h=-0.06, is_drawdown_event=True)
        state2 = _make_state(cash=400.0, reserve=700.0)
        buy2, _, _ = strat.step(day2, state2, is_injection_day=False)
        assert buy2 == 100.0  # Capped strictly at 100.0 by 48h limit!

    def test_froth_freeze_valuation_preservation(self) -> None:
        """Section 5: Froth freeze triggers on MVRV > 2.5 or Mayer > 2.2 upward crossing."""
        cfg = BacktestConfig(periodic_amount=0.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Day 0: MVRV = 2.4
        day0 = _make_day(d=date(2025, 1, 1), mvrv_ratio=2.4)
        strat.step(day0, _make_state(), is_injection_day=False)

        # Day 1: MVRV crosses above 2.5 (2.6)
        day1 = _make_day(d=date(2025, 1, 2), mvrv_ratio=2.6)
        buy, _, desc = strat.step(day1, _make_state(), is_injection_day=False)
        assert buy == 0.0
        assert "FROTH_FREEZE" in desc
        assert "CAPITAL_PRESERVATION_VALUATION" in desc

    def test_dual_pool_initial_split_and_periodic_inflows(self) -> None:
        """AC-3, Section 7.1: Strict dual-pool 40/60 allocation without capital bleed."""
        cfg = BacktestConfig(periodic_amount=100.0)
        strat = EventDrivenRegimeStrategy(cfg)

        # Day 0: Initial cash 1,000.0 in state.cash_balance, reserve 0.0
        day0 = _make_day(d=date(2025, 1, 1))  # Wednesday
        state0 = _make_state(cash=1000.0, reserve=0.0)
        buy0, res_add0, _ = strat.step(day0, state0, is_injection_day=False)
        assert buy0 == 0.0
        # 60% of 1000 transferred to reserve = 600.0
        assert res_add0 == 600.0

        # Day 1: Periodic injection of 100.0 arrives
        day1 = _make_day(d=date(2025, 1, 2))
        state1 = _make_state(cash=500.0, reserve=600.0)  # 500 has 100 inflow
        buy1, res_add1, _ = strat.step(day1, state1, is_injection_day=True)
        assert buy1 == 0.0
        # 60% of 100 periodic amount = 60.0 transferred to reserve
        assert res_add1 == 60.0

    def test_reset_method_clears_state(self) -> None:
        """Strategy reset clears internal indicators and rearms sniper."""
        cfg = BacktestConfig()
        strat = EventDrivenRegimeStrategy(cfg)
        strat.sniper_armed = False
        strat._initialized = True
        strat._prev_close = 50000.0

        strat.reset()
        assert strat.sniper_armed is True
        assert strat._initialized is False
        assert strat._prev_close is None
