"""Tests for quantitative and statistical performance metrics."""

import math
from datetime import date

import pytest

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
    DailyPortfolioState,
    StrategyType,
)


class TestTotalReturn:
    def test_positive_return(self) -> None:
        assert compute_total_return(15000.0, 10000.0) == pytest.approx(50.0)

    def test_negative_return(self) -> None:
        assert compute_total_return(6000.0, 10000.0) == pytest.approx(-40.0)

    def test_break_even(self) -> None:
        assert compute_total_return(10000.0, 10000.0) == pytest.approx(0.0)

    def test_zero_contributed(self) -> None:
        assert compute_total_return(5000.0, 0.0) == 0.0


class TestCAGR:
    def test_one_year_doubling(self) -> None:
        # 10k -> 20k over 365 days is 100% CAGR
        assert compute_cagr(20000.0, 10000.0, 365) == pytest.approx(100.0)

    def test_two_year_quadrupling(self) -> None:
        # 10k -> 40k over 730 days is 100% CAGR (sqrt(4) - 1 = 1)
        assert compute_cagr(40000.0, 10000.0, 730) == pytest.approx(100.0)

    def test_break_even(self) -> None:
        assert compute_cagr(10000.0, 10000.0, 365) == pytest.approx(0.0)

    def test_zero_days(self) -> None:
        assert compute_cagr(20000.0, 10000.0, 0) == 0.0

    def test_zero_capital(self) -> None:
        assert compute_cagr(20000.0, 0.0, 365) == 0.0

    def test_total_wipeout(self) -> None:
        assert compute_cagr(0.0, 10000.0, 365) == -100.0


class TestDrawdowns:
    def test_drawdown_series_and_max(self) -> None:
        equities = [100.0, 120.0, 90.0, 110.0, 80.0, 130.0]
        dds, max_dd = compute_drawdowns(equities)

        assert len(dds) == 6
        assert dds[0] == pytest.approx(0.0)
        assert dds[1] == pytest.approx(0.0)  # new peak 120
        assert dds[2] == pytest.approx((90.0 - 120.0) / 120.0)  # -0.25
        assert dds[3] == pytest.approx((110.0 - 120.0) / 120.0)  # -0.0833
        assert dds[4] == pytest.approx((80.0 - 120.0) / 120.0)  # -0.3333
        assert dds[5] == pytest.approx(0.0)  # new peak 130

        assert max_dd == pytest.approx(1.0 / 3.0)  # 33.33%

    def test_empty_series(self) -> None:
        dds, max_dd = compute_drawdowns([])
        assert dds == []
        assert max_dd == 0.0

    def test_monotonic_increase(self) -> None:
        equities = [100.0, 110.0, 120.0, 130.0]
        dds, max_dd = compute_drawdowns(equities)
        assert all(dd == 0.0 for dd in dds)
        assert max_dd == 0.0


class TestSharpeRatio:
    def test_zero_stdev_returns_zero(self) -> None:
        returns = [0.01, 0.01, 0.01, 0.01]
        assert compute_sharpe_ratio(returns) == 0.0

    def test_insufficient_data(self) -> None:
        assert compute_sharpe_ratio([]) == 0.0
        assert compute_sharpe_ratio([0.02]) == 0.0

    def test_positive_sharpe(self) -> None:
        returns = [0.02, -0.01, 0.03, -0.005, 0.025, 0.015, -0.01]
        sharpe = compute_sharpe_ratio(returns, risk_free_rate=0.03)
        assert sharpe > 0.0
        assert not math.isnan(sharpe)


class TestSortinoRatio:
    def test_empty_returns(self) -> None:
        assert compute_sortino_ratio([]) == 0.0

    def test_downside_volatility(self) -> None:
        returns = [0.03, 0.02, -0.02, 0.04, -0.01, 0.02]
        sortino = compute_sortino_ratio(returns, risk_free_rate=0.03)
        assert sortino > 0.0
        assert not math.isnan(sortino)

    def test_no_downside_returns_zero_denominator(self) -> None:
        # All returns strictly above daily rf
        daily_rf = 0.03 / 365.0
        returns = [daily_rf + 0.01, daily_rf + 0.02, daily_rf + 0.015]
        assert compute_sortino_ratio(returns, risk_free_rate=0.03) == 0.0


class TestCalmarRatio:
    def test_valid_calmar(self) -> None:
        assert compute_calmar_ratio(50.0, 25.0) == pytest.approx(2.0)
        assert compute_calmar_ratio(50.0, -25.0) == pytest.approx(2.0)

    def test_zero_drawdown(self) -> None:
        assert compute_calmar_ratio(50.0, 0.0) == 0.0


class TestComputeStrategyResult:
    def test_empty_states(self) -> None:
        cfg = BacktestConfig()
        res = compute_strategy_result(
            strategy_type=StrategyType.LUMP_SUM,
            config=cfg,
            daily_states=[],
            buys=[],
            market_prices=[],
        )
        assert res.strategy_type == StrategyType.LUMP_SUM
        assert res.final_equity == 0.0

    def test_populated_states(self) -> None:
        cfg = BacktestConfig(initial_cash=10000.0, risk_free_rate=0.03)
        states = [
            DailyPortfolioState(
                trade_date=date(2025, 1, 1),
                cash_balance=0.0,
                reserve_cash_balance=500.0,
                btc_balance=0.2,
                btc_price=50000.0,
                portfolio_equity=10500.0,
                total_contributed=10000.0,
                daily_cash_flow=10000.0,
                daily_return=0.0,
                drawdown=0.0,
                action_taken="Action 1",
            ),
            DailyPortfolioState(
                trade_date=date(2025, 1, 2),
                cash_balance=0.0,
                reserve_cash_balance=1000.0,
                btc_balance=0.2,
                btc_price=60000.0,
                portfolio_equity=13000.0,
                total_contributed=10000.0,
                daily_cash_flow=0.0,
                daily_return=0.238,
                drawdown=0.0,
                action_taken="Action 2",
            ),
        ]
        buys = [(10000.0, 0.2)]
        prices = [50000.0, 60000.0]

        res = compute_strategy_result(
            strategy_type=StrategyType.DYNAMIC_RESERVE,
            config=cfg,
            daily_states=states,
            buys=buys,
            market_prices=prices,
        )

        assert res.final_equity == 13000.0
        assert res.total_contributed == 10000.0
        assert res.net_profit == 3000.0
        assert res.total_return_pct == pytest.approx(30.0)
        assert res.total_btc_accumulated == 0.2
        assert res.average_buy_price == 50000.0
        assert res.average_market_price == 55000.0
        assert res.acquisition_discount_pct == pytest.approx((55000.0 - 50000.0) / 55000.0 * 100.0)
        assert res.reserve_pool_final == 1000.0
        assert res.reserve_pool_peak == 1000.0
