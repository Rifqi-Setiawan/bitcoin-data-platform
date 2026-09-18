"""Tests for the event-driven backtesting simulation engine."""

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from bitcoin_data_platform.backtest.engine import BacktestEngine
from bitcoin_data_platform.backtest.models import (
    BacktestConfig,
    BacktestDayRecord,
    FrequencyType,
    StrategyType,
)


def _create_test_records(count: int = 14) -> list[BacktestDayRecord]:
    records: list[BacktestDayRecord] = []
    base_date = date(2025, 1, 1)
    for i in range(count):
        d = base_date + timedelta(days=i)
        price = 50000.0 + i * 500.0  # gentle bull run
        signal = "STANDARD_DCA"
        macro = False
        if i == 3:
            macro = True
        elif i == 5:
            signal = "DEFENSIVE_RESERVE"
        elif i == 8:
            signal = "AGGRESSIVE_ACCUMULATE"
        elif i == 10:
            signal = "HARD_FREEZE"

        records.append(
            BacktestDayRecord(
                trade_date=d,
                market_close_usd=price,
                sma_200=48000.0,
                mayer_multiple=price / 48000.0,
                mvrv_ratio=1.5,
                fng_value=50,
                has_high_impact_macro_event=macro,
                investment_signal=signal,
            )
        )
    return records


class TestBacktestEngineLoadDuckDB:
    def test_missing_database_file(self, tmp_path: Path) -> None:
        engine = BacktestEngine()
        missing_db = tmp_path / "non_existent.duckdb"
        with pytest.raises(FileNotFoundError, match="Database file does not exist"):
            engine.load_data_from_duckdb(missing_db)

    def test_missing_view_raises_error(self, tmp_path: Path) -> None:
        engine = BacktestEngine()
        db_file = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_file))
        con.execute("CREATE TABLE dummy (id INT)")
        con.close()

        with pytest.raises(ValueError, match="mart_btc_investment_signals_daily not found"):
            engine.load_data_from_duckdb(db_file)

    def test_load_records_success_and_filtering(self, tmp_path: Path) -> None:
        engine = BacktestEngine()
        db_file = tmp_path / "test_signals.duckdb"
        con = duckdb.connect(str(db_file))
        con.execute(
            """
            CREATE VIEW mart_btc_investment_signals_daily AS
            SELECT * FROM (VALUES
                ('2025-01-01', 50000.0, 48000.0, 1.04, 1.5, 50, FALSE, 'STANDARD_DCA'),
                ('2025-01-02', 51000.0, 48100.0, 1.06, 1.55, 55, TRUE, 'DEFENSIVE_RESERVE'),
                ('2025-01-03', 52000.0, 48200.0, 1.08, 1.6, 60, FALSE, 'OPPORTUNISTIC_ACCUMULATE'),
                ('2025-01-04', 0.0, 48300.0, 0.0, 1.6, 60, FALSE, 'STANDARD_DCA'),
                ('2025-01-05', 53000.0, NULL, NULL, NULL, NULL, FALSE, NULL)
            ) t(
                trade_date_utc, market_close_usd, sma_200, mayer_multiple,
                mvrv_ratio, fng_value, has_high_impact_macro_event, investment_signal
            )
            """
        )
        con.close()

        # Unfiltered
        records = engine.load_data_from_duckdb(db_file)
        assert len(records) == 4  # 0.0 close price skipped
        assert records[0].trade_date == date(2025, 1, 1)
        assert records[0].market_close_usd == 50000.0
        assert records[1].has_high_impact_macro_event is True
        assert records[3].investment_signal == "STANDARD_DCA"  # default fallback

        # Filtered by start and end date
        filtered = engine.load_data_from_duckdb(
            db_file, start_date=date(2025, 1, 2), end_date=date(2025, 1, 3)
        )
        assert len(filtered) == 2
        assert filtered[0].trade_date == date(2025, 1, 2)
        assert filtered[1].trade_date == date(2025, 1, 3)


class TestBacktestEngineExecution:
    def test_lump_sum_simulation(self) -> None:
        engine = BacktestEngine()
        records = _create_test_records(count=5)
        config = BacktestConfig(
            initial_cash=10000.0,
            fee_bps=10.0,  # 0.10%
        )

        res = engine.run_strategy(records, StrategyType.LUMP_SUM, config)
        assert res.strategy_type == StrategyType.LUMP_SUM
        assert res.total_contributed == 10000.0
        assert len(res.daily_states) == 5

        # Day 0: 10,000 invested. Fee = 10.0. Net buy = 9990.0. BTC = 9990 / 50000 = 0.1998
        day0 = res.daily_states[0]
        assert day0.cash_balance == pytest.approx(0.0)
        assert day0.btc_balance == pytest.approx(9990.0 / 50000.0)
        assert day0.portfolio_equity == pytest.approx(9990.0)

        # Day 4: price is 52000.0. Equity = 0.1998 * 52000 = 10389.6
        day4 = res.daily_states[4]
        assert day4.cash_balance == pytest.approx(0.0)
        assert day4.btc_balance == pytest.approx(9990.0 / 50000.0)
        assert res.final_equity == pytest.approx(day4.portfolio_equity)
        assert res.net_profit > 0.0

    def test_blind_dca_daily_simulation(self) -> None:
        engine = BacktestEngine()
        records = _create_test_records(count=7)
        config = BacktestConfig(
            periodic_amount=100.0,
            frequency=FrequencyType.DAILY,
            fee_bps=10.0,
        )

        res = engine.run_strategy(records, StrategyType.BLIND_DCA, config)
        assert res.total_contributed == 700.0
        assert len(res.daily_states) == 7
        assert res.total_btc_accumulated > 0.0
        assert all(s.cash_balance == pytest.approx(0.0) for s in res.daily_states)

    def test_blind_dca_weekly_simulation(self) -> None:
        engine = BacktestEngine()
        records = _create_test_records(count=14)
        config = BacktestConfig(
            periodic_amount=100.0,
            frequency=FrequencyType.WEEKLY,
        )

        res = engine.run_strategy(records, StrategyType.BLIND_DCA, config)
        # Day 0 and Day 7 get injected -> 2 injections = 200.0 total
        assert res.total_contributed == 200.0
        # Non-injection days should have action_taken "HOLD"
        assert "HOLD" in res.daily_states[1].action_taken
        assert "BLIND_DCA" in res.daily_states[0].action_taken
        assert "BLIND_DCA" in res.daily_states[7].action_taken

    def test_dynamic_reserve_simulation_accounting_integrity(self) -> None:
        engine = BacktestEngine()
        records = _create_test_records(count=12)
        config = BacktestConfig(
            periodic_amount=100.0,
            frequency=FrequencyType.DAILY,
            fee_bps=10.0,
        )

        res = engine.run_strategy(records, StrategyType.DYNAMIC_RESERVE, config)
        assert res.total_contributed == 1200.0
        assert len(res.daily_states) == 12

        # Check cash balance and reserve cash balance are always non-negative
        for state in res.daily_states:
            assert state.cash_balance >= -1e-6
            assert state.reserve_cash_balance >= -1e-6
            assert state.portfolio_equity >= 0.0

        # Tactical reserve should have accumulated capital
        assert res.reserve_pool_peak > 0.0
        assert res.total_btc_accumulated > 0.0

    def test_run_benchmark_all_strategies(self) -> None:
        engine = BacktestEngine()
        records = _create_test_records(count=10)
        config = BacktestConfig(
            initial_cash=10000.0,
            periodic_amount=100.0,
            frequency=FrequencyType.DAILY,
        )

        summary = engine.run_benchmark(records, config)
        assert summary.duration_days == 10
        assert summary.start_date == records[0].trade_date
        assert summary.end_date == records[-1].trade_date
        assert StrategyType.LUMP_SUM in summary.results
        assert StrategyType.BLIND_DCA in summary.results
        assert StrategyType.DYNAMIC_RESERVE in summary.results

    def test_run_strategy_empty_records(self) -> None:
        engine = BacktestEngine()
        config = BacktestConfig()
        res = engine.run_strategy([], StrategyType.LUMP_SUM, config)
        assert res.final_equity == 0.0
        assert res.total_contributed == 0.0

    def test_run_benchmark_empty_records(self) -> None:
        engine = BacktestEngine()
        config = BacktestConfig()
        summary = engine.run_benchmark([], config)
        assert summary.duration_days == 0
        assert len(summary.results) == 0
