"""Tests for backtesting data models and configuration contracts."""

from datetime import date

import pytest

from bitcoin_data_platform.backtest.models import (
    BacktestConfig,
    BacktestDayRecord,
    BenchmarkSummary,
    DailyPortfolioState,
    FrequencyType,
    StrategyResult,
    StrategyType,
)


class TestStrategyType:
    def test_enum_values(self) -> None:
        assert StrategyType.LUMP_SUM.value == "lump-sum"
        assert StrategyType.BLIND_DCA.value == "blind-dca"
        assert StrategyType.DYNAMIC_RESERVE.value == "dynamic-reserve"
        assert StrategyType.WEEKLY_BATCH_DCA.value == "weekly-batch-dca"
        assert StrategyType.EVENT_DRIVEN_REGIME.value == "event-driven-regime"

    def test_from_str_valid(self) -> None:
        assert StrategyType.from_str("lump-sum") == StrategyType.LUMP_SUM
        assert StrategyType.from_str("LUMP_SUM") == StrategyType.LUMP_SUM
        assert StrategyType.from_str("blind-dca") == StrategyType.BLIND_DCA
        assert StrategyType.from_str("BLIND_DCA") == StrategyType.BLIND_DCA
        assert StrategyType.from_str("dynamic-reserve") == StrategyType.DYNAMIC_RESERVE
        assert StrategyType.from_str("DYNAMIC_RESERVE") == StrategyType.DYNAMIC_RESERVE
        assert StrategyType.from_str("weekly-batch-dca") == StrategyType.WEEKLY_BATCH_DCA
        assert StrategyType.from_str("WEEKLY_BATCH_DCA") == StrategyType.WEEKLY_BATCH_DCA
        assert StrategyType.from_str("event-driven-regime") == StrategyType.EVENT_DRIVEN_REGIME
        assert StrategyType.from_str("EVENT_DRIVEN_REGIME") == StrategyType.EVENT_DRIVEN_REGIME

    def test_from_str_invalid(self) -> None:
        with pytest.raises(ValueError, match="Unknown strategy type"):
            StrategyType.from_str("unknown-strategy")


class TestFrequencyType:
    def test_enum_values(self) -> None:
        assert FrequencyType.DAILY.value == "daily"
        assert FrequencyType.WEEKLY.value == "weekly"

    def test_from_str_valid(self) -> None:
        assert FrequencyType.from_str("daily") == FrequencyType.DAILY
        assert FrequencyType.from_str("DAILY") == FrequencyType.DAILY
        assert FrequencyType.from_str("weekly") == FrequencyType.WEEKLY
        assert FrequencyType.from_str("WEEKLY") == FrequencyType.WEEKLY

    def test_from_str_invalid(self) -> None:
        with pytest.raises(ValueError, match="Unknown frequency"):
            FrequencyType.from_str("monthly")


class TestBacktestConfig:
    def test_default_config(self) -> None:
        cfg = BacktestConfig()
        assert cfg.start_date is None
        assert cfg.end_date is None
        assert cfg.initial_cash == 10000.0
        assert cfg.periodic_amount == 100.0
        assert cfg.frequency == FrequencyType.DAILY
        assert cfg.fee_bps == 10.0
        assert cfg.risk_free_rate == 0.03

    def test_custom_valid_config(self) -> None:
        cfg = BacktestConfig(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
            initial_cash=50000.0,
            periodic_amount=250.0,
            frequency=FrequencyType.WEEKLY,
            fee_bps=15.0,
            risk_free_rate=0.04,
        )
        assert cfg.initial_cash == 50000.0
        assert cfg.frequency == FrequencyType.WEEKLY

    def test_string_frequency_coercion(self) -> None:
        cfg = BacktestConfig(frequency="weekly")  # type: ignore[arg-type]
        assert cfg.frequency == FrequencyType.WEEKLY

    def test_invalid_negative_initial_cash(self) -> None:
        with pytest.raises(ValueError, match="initial_cash must be >= 0.0"):
            BacktestConfig(initial_cash=-100.0)

    def test_invalid_negative_periodic_amount(self) -> None:
        with pytest.raises(ValueError, match="periodic_amount must be >= 0.0"):
            BacktestConfig(periodic_amount=-50.0)

    def test_invalid_negative_fee(self) -> None:
        with pytest.raises(ValueError, match="fee_bps must be >= 0.0"):
            BacktestConfig(fee_bps=-5.0)

    def test_invalid_negative_rf(self) -> None:
        with pytest.raises(ValueError, match="risk_free_rate must be >= 0.0"):
            BacktestConfig(risk_free_rate=-0.01)

    def test_invalid_date_range(self) -> None:
        with pytest.raises(ValueError, match="cannot be after end_date"):
            BacktestConfig(start_date=date(2025, 6, 1), end_date=date(2025, 1, 1))


class TestBacktestDayRecord:
    def test_valid_record(self) -> None:
        rec = BacktestDayRecord(
            trade_date=date(2025, 3, 1),
            market_close_usd=85000.0,
            sma_200=80000.0,
            mayer_multiple=1.0625,
            mvrv_ratio=1.95,
            fng_value=65,
            has_high_impact_macro_event=False,
            investment_signal="STANDARD_DCA",
        )
        assert rec.trade_date == date(2025, 3, 1)
        assert rec.market_close_usd == 85000.0
        assert rec.has_high_impact_macro_event is False
        assert rec.investment_signal == "STANDARD_DCA"

    def test_invalid_zero_close_price(self) -> None:
        with pytest.raises(ValueError, match="market_close_usd must be > 0.0"):
            BacktestDayRecord(trade_date=date(2025, 3, 1), market_close_usd=0.0)

    def test_invalid_negative_close_price(self) -> None:
        with pytest.raises(ValueError, match="market_close_usd must be > 0.0"):
            BacktestDayRecord(trade_date=date(2025, 3, 1), market_close_usd=-500.0)

    def test_drawdown_bounds_validation(self) -> None:
        # Valid drawdown in [-1.0, 0.0]
        rec = BacktestDayRecord(
            trade_date=date(2025, 3, 1),
            market_close_usd=50000.0,
            drawdown_7d=-0.15,
            drawdown_30d=-0.25,
            return_24h=-0.05,
            is_weekly_cadence_day=True,
            is_drawdown_event=True,
            is_regime_capitulation=False,
            is_regime_froth=False,
        )
        assert rec.drawdown_7d == -0.15
        assert rec.is_drawdown_event is True

        # Invalid positive drawdown
        with pytest.raises(ValueError, match="drawdown_7d must be in"):
            BacktestDayRecord(
                trade_date=date(2025, 3, 1),
                market_close_usd=50000.0,
                drawdown_7d=0.05,
            )

        with pytest.raises(ValueError, match="drawdown_30d must be in"):
            BacktestDayRecord(
                trade_date=date(2025, 3, 1),
                market_close_usd=50000.0,
                drawdown_30d=-1.5,
            )


class TestDailyPortfolioState:
    def test_to_dict_serialization(self) -> None:
        state = DailyPortfolioState(
            trade_date=date(2025, 3, 15),
            cash_balance=150.0,
            reserve_cash_balance=450.0,
            btc_balance=0.15,
            btc_price=80000.0,
            portfolio_equity=12600.0,
            total_contributed=10000.0,
            daily_cash_flow=100.0,
            daily_return=0.015,
            drawdown=-0.05,
            action_taken="STANDARD_DCA: Buy $70.00",
        )
        d = state.to_dict()
        assert d["trade_date"] == "2025-03-15"
        assert d["portfolio_equity"] == 12600.0
        assert d["btc_balance"] == 0.15
        assert d["action_taken"] == "STANDARD_DCA: Buy $70.00"


class TestStrategyResult:
    def test_to_dict_without_daily_states(self) -> None:
        res = StrategyResult(
            strategy_type=StrategyType.DYNAMIC_RESERVE,
            config=BacktestConfig(),
            total_contributed=10000.0,
            final_equity=15000.0,
            net_profit=5000.0,
            total_return_pct=50.0,
            cagr_pct=25.0,
            max_drawdown_pct=15.0,
            sharpe_ratio=1.45,
            sortino_ratio=2.10,
            calmar_ratio=1.67,
            total_btc_accumulated=0.25,
            average_buy_price=40000.0,
            average_market_price=50000.0,
            acquisition_discount_pct=20.0,
            reserve_pool_final=1500.0,
            reserve_pool_peak=2500.0,
        )
        d = res.to_dict(include_daily_states=False)
        assert d["strategy_type"] == "dynamic-reserve"
        assert d["final_equity"] == 15000.0
        assert d["net_profit"] == 5000.0
        assert "daily_states" not in d

    def test_to_dict_with_daily_states(self) -> None:
        state = DailyPortfolioState(
            trade_date=date(2025, 1, 1),
            cash_balance=0.0,
            reserve_cash_balance=0.0,
            btc_balance=0.1,
            btc_price=50000.0,
            portfolio_equity=5000.0,
            total_contributed=5000.0,
            daily_cash_flow=5000.0,
            daily_return=0.0,
            drawdown=0.0,
            action_taken="LUMP_SUM: Deployed initial cash",
        )
        res = StrategyResult(
            strategy_type=StrategyType.LUMP_SUM,
            config=BacktestConfig(),
            daily_states=[state],
        )
        d = res.to_dict(include_daily_states=True)
        assert len(d["daily_states"]) == 1
        assert d["daily_states"][0]["trade_date"] == "2025-01-01"


class TestBenchmarkSummary:
    def test_summary_to_dict(self) -> None:
        res = StrategyResult(
            strategy_type=StrategyType.BLIND_DCA,
            config=BacktestConfig(),
            final_equity=12000.0,
        )
        summary = BenchmarkSummary(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
            duration_days=365,
            results={StrategyType.BLIND_DCA: res},
        )
        d = summary.to_dict()
        assert d["start_date"] == "2025-01-01"
        assert d["end_date"] == "2025-12-31"
        assert d["duration_days"] == 365
        assert "blind-dca" in d["results"]
        assert d["results"]["blind-dca"]["final_equity"] == 12000.0
