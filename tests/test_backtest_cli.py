"""Tests for the backtest CLI subcommand and reporting interfaces."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from bitcoin_data_platform.backtest.models import (
    BacktestConfig,
    BacktestDayRecord,
    BenchmarkSummary,
    DailyPortfolioState,
    StrategyResult,
    StrategyType,
)
from bitcoin_data_platform.cli import main


def _dummy_result(
    strat_type: StrategyType = StrategyType.DYNAMIC_RESERVE,
) -> StrategyResult:
    cfg = BacktestConfig()
    day0 = DailyPortfolioState(
        trade_date=date(2025, 1, 1),
        cash_balance=0.0,
        reserve_cash_balance=300.0,
        btc_balance=0.1,
        btc_price=50000.0,
        portfolio_equity=5300.0,
        total_contributed=5000.0,
        daily_cash_flow=5000.0,
        daily_return=0.0,
        drawdown=0.0,
        action_taken="Action",
    )
    return StrategyResult(
        strategy_type=strat_type,
        config=cfg,
        daily_states=[day0],
        total_contributed=5000.0,
        final_equity=5300.0,
        net_profit=300.0,
        total_return_pct=6.0,
        cagr_pct=6.0,
        max_drawdown_pct=0.0,
        sharpe_ratio=1.5,
        sortino_ratio=2.0,
        calmar_ratio=3.0,
        total_btc_accumulated=0.1,
        average_buy_price=50000.0,
        average_market_price=50000.0,
        acquisition_discount_pct=0.0,
        reserve_pool_final=300.0,
        reserve_pool_peak=300.0,
    )


def _dummy_summary() -> BenchmarkSummary:
    return BenchmarkSummary(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 10),
        duration_days=10,
        results={
            StrategyType.LUMP_SUM: _dummy_result(StrategyType.LUMP_SUM),
            StrategyType.BLIND_DCA: _dummy_result(StrategyType.BLIND_DCA),
            StrategyType.DYNAMIC_RESERVE: _dummy_result(StrategyType.DYNAMIC_RESERVE),
        },
    )


class TestBacktestCLIValidation:
    def test_missing_command_help(self, capsys: object) -> None:
        exit_code = main(["backtest", "--help"])
        assert exit_code == 0

    def test_invalid_start_date_format(self, tmp_path: Path, capsys: object) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()
        exit_code = main(["backtest", "--db-path", str(db_file), "--start", "2025-99-99"])
        assert exit_code == 2

    def test_invalid_end_date_format(self, tmp_path: Path, capsys: object) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()
        exit_code = main(["backtest", "--db-path", str(db_file), "--end", "invalid-date"])
        assert exit_code == 2

    def test_start_after_end_date(self, tmp_path: Path, capsys: object) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()
        exit_code = main(
            [
                "backtest",
                "--db-path",
                str(db_file),
                "--start",
                "2025-06-01",
                "--end",
                "2025-01-01",
            ]
        )
        assert exit_code == 2

    def test_negative_numeric_arguments(self, tmp_path: Path) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()

        assert main(["backtest", "--db-path", str(db_file), "--initial-cash", "-100"]) == 2
        assert main(["backtest", "--db-path", str(db_file), "--periodic-amount", "-50"]) == 2
        assert main(["backtest", "--db-path", str(db_file), "--fee-bps", "-1"]) == 2

    def test_missing_database_file(self, tmp_path: Path) -> None:
        missing_db = tmp_path / "non_existent.duckdb"
        exit_code = main(["backtest", "--db-path", str(missing_db)])
        assert exit_code == 2


class TestBacktestCLIExecution:
    def test_backtest_all_strategies_table_format(self, tmp_path: Path, capsys: object) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()

        mock_engine = MagicMock()
        mock_records = [
            BacktestDayRecord(
                trade_date=date(2025, 1, 1),
                market_close_usd=50000.0,
            )
        ]
        mock_engine.load_data_from_duckdb.return_value = mock_records
        mock_engine.run_benchmark.return_value = _dummy_summary()

        exit_code = main(
            ["backtest", "--db-path", str(db_file), "--format", "table"],
            backtest_engine=mock_engine,
        )
        assert exit_code == 0
        mock_engine.run_benchmark.assert_called_once()

    def test_backtest_single_strategy_json_format(self, tmp_path: Path, capsys: object) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()

        mock_engine = MagicMock()
        mock_records = [
            BacktestDayRecord(
                trade_date=date(2025, 1, 1),
                market_close_usd=50000.0,
            )
        ]
        mock_engine.load_data_from_duckdb.return_value = mock_records
        mock_engine.run_strategy.return_value = _dummy_result(StrategyType.DYNAMIC_RESERVE)

        exit_code = main(
            [
                "backtest",
                "--db-path",
                str(db_file),
                "--strategy",
                "dynamic-reserve",
                "--format",
                "json",
            ],
            backtest_engine=mock_engine,
        )
        assert exit_code == 0
        mock_engine.run_strategy.assert_called_once()

    def test_backtest_markdown_file_output(self, tmp_path: Path) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()
        out_file = tmp_path / "report.md"

        mock_engine = MagicMock()
        mock_records = [
            BacktestDayRecord(
                trade_date=date(2025, 1, 1),
                market_close_usd=50000.0,
            )
        ]
        mock_engine.load_data_from_duckdb.return_value = mock_records
        mock_engine.run_benchmark.return_value = _dummy_summary()

        exit_code = main(
            [
                "backtest",
                "--db-path",
                str(db_file),
                "--format",
                "markdown",
                "--output",
                str(out_file),
            ],
            backtest_engine=mock_engine,
        )
        assert exit_code == 0
        assert out_file.exists()
        content = out_file.read_text()
        assert "# Bitcoin Strategy Benchmark Report" in content

    def test_no_records_returns_exit_code_2(self, tmp_path: Path) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()

        mock_engine = MagicMock()
        mock_engine.load_data_from_duckdb.return_value = []

        exit_code = main(
            ["backtest", "--db-path", str(db_file)],
            backtest_engine=mock_engine,
        )
        assert exit_code == 2

    def test_engine_load_exception_returns_exit_code_2(self, tmp_path: Path) -> None:
        db_file = tmp_path / "test.duckdb"
        db_file.touch()

        mock_engine = MagicMock()
        mock_engine.load_data_from_duckdb.side_effect = RuntimeError("DuckDB read error")

        exit_code = main(
            ["backtest", "--db-path", str(db_file)],
            backtest_engine=mock_engine,
        )
        assert exit_code == 2

    def test_backtest_cli_unmocked_duckdb_end_to_end(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """End-to-end unmocked CLI execution against real DuckDB database."""
        db_file = tmp_path / "test_e2e.duckdb"
        con = duckdb.connect(str(db_file))
        con.execute(
            """
            CREATE TABLE mart_btc_investment_signals_daily (
                trade_date_utc DATE PRIMARY KEY,
                market_close_usd DOUBLE,
                sma_200 DOUBLE,
                mayer_multiple DOUBLE,
                mvrv_ratio DOUBLE,
                fng_value INTEGER,
                fng_classification VARCHAR,
                has_high_impact_macro_event BOOLEAN,
                investment_signal VARCHAR
            );
            INSERT INTO mart_btc_investment_signals_daily VALUES
            ('2025-01-01', 50000.0, 48000.0, 1.04, 1.5, 50, 'Neutral', FALSE, 'STANDARD_DCA'),
            (
                '2025-01-02',
                51000.0,
                48100.0,
                1.06,
                1.55,
                55,
                'Neutral',
                TRUE,
                'DEFENSIVE_RESERVE',
            ),
            (
                '2025-01-03',
                52000.0,
                48200.0,
                1.08,
                1.6,
                60,
                'Greed',
                FALSE,
                'OPPORTUNISTIC_ACCUMULATE',
            );
            """
        )
        con.close()

        exit_code = main(["backtest", "--db-path", str(db_file)])
        assert exit_code == 0

        captured = capsys.readouterr()
        assert "BITCOIN STRATEGY BENCHMARK REPORT" in captured.out
        assert "Lump Sum Buy & Hold" in captured.out
        assert "Blind DCA" in captured.out
        assert "Dynamic Reserve DCA" in captured.out
