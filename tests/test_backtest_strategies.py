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
    LumpSumStrategy,
)


def _make_day(
    signal: str | None = "STANDARD_DCA",
    macro: bool = False,
    price: float = 50000.0,
    d: date | None = None,
) -> BacktestDayRecord:
    return BacktestDayRecord(
        trade_date=d or date(2025, 1, 1),
        market_close_usd=price,
        sma_200=48000.0,
        mayer_multiple=price / 48000.0,
        mvrv_ratio=1.5,
        fng_value=50,
        has_high_impact_macro_event=macro,
        investment_signal=signal,
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
