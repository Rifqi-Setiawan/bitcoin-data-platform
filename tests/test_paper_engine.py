"""Unit and integration tests for PaperTradingEngine."""

from datetime import date
from pathlib import Path

import duckdb
import pytest

from bitcoin_data_platform.paper.engine import PaperEngineError, PaperTradingEngine
from bitcoin_data_platform.paper.risk_guard import RiskGuard


@pytest.fixture
def populated_signals_db(tmp_path: Path) -> Path:
    """Create a temporary DuckDB database with mart_btc_investment_signals_daily populated."""
    db_file = tmp_path / "test_platform.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("SET TimeZone='UTC';")

    con.execute(
        """
        CREATE TABLE mart_btc_investment_signals_daily (
            trade_date_utc TIMESTAMPTZ,
            market_close_usd DOUBLE,
            sma_200 DOUBLE,
            mayer_multiple DOUBLE,
            mvrv_ratio DOUBLE,
            fng_value INTEGER,
            fng_classification VARCHAR,
            has_high_impact_macro_event BOOLEAN,
            investment_signal VARCHAR
        );
        """
    )

    con.execute(
        """
        INSERT INTO mart_btc_investment_signals_daily VALUES
        ('2026-09-01 00:00:00+00', 80000.0, 70000.0, 1.14, 1.8, 55, 'Neutral',
         false, 'STANDARD_DCA'),
        ('2026-09-02 00:00:00+00', 76000.0, 70200.0, 1.08, 1.7, 45, 'Neutral',
         false, 'OPPORTUNISTIC_ACCUMULATE'),
        ('2026-09-03 00:00:00+00', 65000.0, 70100.0, 0.75, 1.1, 20, 'Extreme Fear',
         false, 'AGGRESSIVE_ACCUMULATE'),
        ('2026-09-04 00:00:00+00', 88000.0, 70500.0, 1.95, 2.8, 88, 'Extreme Greed',
         false, 'DEFENSIVE_RESERVE'),
        ('2026-09-05 00:00:00+00', 98000.0, 71000.0, 2.55, 3.6, 92, 'Extreme Greed',
         false, 'HARD_FREEZE'),
        ('2026-09-06 00:00:00+00', 78000.0, 71200.0, 1.10, 1.6, 50, 'Neutral',
         true, 'STANDARD_DCA');
        """
    )

    con.close()
    return db_file


def test_init_portfolio_creates_tables_and_partitions_capital(tmp_path: Path) -> None:
    db_file = tmp_path / "test.duckdb"
    engine = PaperTradingEngine(db_path=db_file)

    balance = engine.init_portfolio(initial_cash=1000.0)

    assert balance.portfolio_id == "default"
    assert balance.initial_cash == 1000.0
    assert balance.base_cash == 700.0
    assert balance.reserve_cash == 300.0
    assert balance.btc_balance == 0.0
    assert balance.total_cash == 1000.0
    assert balance.total_trades == 0

    # Test idempotency: calling init again without overwrite returns existing
    balance2 = engine.init_portfolio(initial_cash=2000.0, overwrite=False)
    assert balance2.initial_cash == 1000.0


def test_init_portfolio_rejects_non_positive_capital(tmp_path: Path) -> None:
    engine = PaperTradingEngine(db_path=tmp_path / "test.duckdb")
    with pytest.raises(ValueError, match="initial_cash must be > 0"):
        engine.init_portfolio(initial_cash=0.0)


def test_step_standard_dca(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-01: STANDARD_DCA @ 80,000 spot price
    # daily_budget = 10.0 -> gross = 10.0
    # fee = 10.0 * 0.0010 = 0.0100 -> net = 9.99
    # btc = 9.99 / 80000 = 0.000124875
    rec = engine.step(trade_date=date(2026, 9, 1), daily_budget=10.0)

    assert rec.side == "BUY"
    assert rec.signal_regime == "STANDARD_DCA"
    assert rec.spot_price == 80000.0
    assert rec.gross_amount_usd == 10.0
    assert rec.fee_usd == 0.0100
    assert rec.net_amount_usd == 9.99
    assert rec.btc_amount == pytest.approx(0.000124875, abs=1e-8)

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 690.0
    assert bal.reserve_cash == 300.0
    assert bal.total_trades == 1
    assert bal.btc_balance == pytest.approx(0.000124875, abs=1e-8)


def test_step_opportunistic_accumulate(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-02: OPPORTUNISTIC_ACCUMULATE @ 76,000 spot
    # budget = 10.0 -> 1.3x base = 13.0
    rec = engine.step(trade_date=date(2026, 9, 2), daily_budget=10.0)

    assert rec.side == "BUY"
    assert rec.signal_regime == "OPPORTUNISTIC_ACCUMULATE"
    assert rec.gross_amount_usd == 13.0
    assert rec.fee_usd == 0.0130
    assert rec.net_amount_usd == 12.987

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 687.0
    assert bal.reserve_cash == 300.0


def test_step_aggressive_accumulate_draws_from_reserve(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)  # base: 700, reserve: 300

    # 2026-09-03: AGGRESSIVE_ACCUMULATE @ 65,000 spot
    # budget = 10.0:
    # base_buy = 2.0 * 10 = 20.0
    # reserve_draw = 25% of 300.0 = 75.0
    # gross = 20.0 + 75.0 = 95.0
    rec = engine.step(trade_date=date(2026, 9, 3), daily_budget=10.0)

    assert rec.side == "BUY"
    assert rec.signal_regime == "AGGRESSIVE_ACCUMULATE"
    assert rec.gross_amount_usd == 95.0
    assert rec.fee_usd == round(95.0 * 0.0010, 4)

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 680.0  # 700 - 20
    assert bal.reserve_cash == 225.0  # 300 - 75


def test_step_defensive_reserve_diverts_portion_to_reserve(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)  # base: 700, reserve: 300

    # 2026-09-04: DEFENSIVE_RESERVE @ 88,000 spot
    # budget = 10.0:
    # buy = 0.5 * 10 = 5.0
    # reserve transfer = 0.5 * 10 = 5.0
    rec = engine.step(trade_date=date(2026, 9, 4), daily_budget=10.0)

    assert rec.side == "BUY"
    assert rec.signal_regime == "DEFENSIVE_RESERVE"
    assert rec.gross_amount_usd == 5.0
    assert rec.fee_usd == 0.0050

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 690.0  # 700 - 10
    assert bal.reserve_cash == 305.0  # 300 + 5


def test_step_hard_freeze_diverts_100_percent_to_reserve(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)  # base: 700, reserve: 300

    # 2026-09-05: HARD_FREEZE @ 98,000 spot
    # buy = 0.0, 100% of 10.0 transferred to reserve
    rec = engine.step(trade_date=date(2026, 9, 5), daily_budget=10.0)

    assert rec.side == "HOLD"
    assert rec.signal_regime == "HARD_FREEZE"
    assert rec.gross_amount_usd == 0.0
    assert rec.fee_usd == 0.0
    assert rec.btc_amount == 0.0

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 690.0  # 700 - 10
    assert bal.reserve_cash == 310.0  # 300 + 10
    assert bal.total_trades == 0


def test_step_macro_circuit_breaker(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-06: has_high_impact_macro_event = True
    rec = engine.step(trade_date=date(2026, 9, 6), daily_budget=10.0)

    assert rec.side == "HOLD"
    assert "JEDA MAKRO" in rec.narrative
    assert rec.gross_amount_usd == 0.0

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 690.0
    assert bal.reserve_cash == 310.0


def test_step_kill_switch_active_halts_execution(
    populated_signals_db: Path,
    tmp_path: Path,
) -> None:
    kill_switch = tmp_path / "PAPER_KILL_SWITCH"
    kill_switch.touch()

    guard = RiskGuard(kill_switch_path=kill_switch)
    engine = PaperTradingEngine(db_path=populated_signals_db, risk_guard=guard)
    engine.init_portfolio()

    with pytest.raises(PaperEngineError, match="Kill switch active"):
        engine.step(trade_date=date(2026, 9, 1))


def test_step_force_spot_price(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # Force spot price of 75,000 on custom date
    rec = engine.step(
        trade_date=date(2026, 9, 10),
        daily_budget=10.0,
        force_spot_price=75000.0,
    )
    assert rec.spot_price == 75000.0
    assert rec.gross_amount_usd == 10.0


def test_reset_portfolio(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    engine.step(trade_date=date(2026, 9, 1), daily_budget=10.0)
    bal_before = engine.get_portfolio_balance()
    assert bal_before.total_trades == 1

    engine.reset_portfolio(initial_cash=1000.0)
    bal_after = engine.get_portfolio_balance()
    assert bal_after.total_trades == 0
    assert bal_after.base_cash == 700.0
    assert bal_after.reserve_cash == 300.0
    assert bal_after.btc_balance == 0.0

    blotter = engine.get_trade_blotter()
    assert len(blotter) == 0


def test_portfolio_summary_and_equity_series(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    engine.step(trade_date=date(2026, 9, 1), daily_budget=10.0)
    engine.step(trade_date=date(2026, 9, 2), daily_budget=10.0)

    summary = engine.get_portfolio_summary()
    assert summary.initial_cash == 1000.0
    assert summary.total_trades == 2
    assert summary.btc_balance > 0.0
    assert summary.total_equity > 0.0
    assert summary.current_spot_price > 0.0

    series = engine.get_equity_series(limit=30)
    assert len(series) == 2
    assert series[0]["date"] == "2026-09-01"
    assert series[1]["date"] == "2026-09-02"
    assert "equity" in series[0]
    assert "benchmark" in series[0]

    blotter = engine.get_trade_blotter(limit=10)
    assert len(blotter) == 2
    assert blotter[0]["date"] == "2026-09-02"  # Latest first
