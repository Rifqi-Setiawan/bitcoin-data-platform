"""Unit and integration tests for PaperTradingEngine with Pacing Guard & Sniper Mode."""

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
         true, 'STANDARD_DCA'),
        ('2026-09-13 00:00:00+00', 79000.0, 71500.0, 1.10, 1.6, 50, 'Neutral',
         false, 'STANDARD_DCA');
        """
    )

    con.close()
    return db_file


def test_init_portfolio_creates_tables_and_partitions_capital(tmp_path: Path) -> None:
    """Verify capital is restructured into 40% Base ($400) and 60% Tactical Reserve ($600)."""
    db_file = tmp_path / "test.duckdb"
    engine = PaperTradingEngine(db_path=db_file)

    balance = engine.init_portfolio(initial_cash=1000.0)

    assert balance.portfolio_id == "default"
    assert balance.initial_cash == 1000.0
    assert balance.base_cash == 400.0
    assert balance.reserve_cash == 600.0
    assert balance.btc_balance == 0.0
    assert balance.total_cash == 1000.0
    assert balance.total_trades == 0

    # Test idempotency: calling init again without overwrite returns existing
    balance2 = engine.init_portfolio(initial_cash=2000.0, overwrite=False)
    assert balance2.initial_cash == 1000.0
    assert balance2.base_cash == 400.0
    assert balance2.reserve_cash == 600.0


def test_init_portfolio_rejects_non_positive_capital(tmp_path: Path) -> None:
    engine = PaperTradingEngine(db_path=tmp_path / "test.duckdb")
    with pytest.raises(ValueError, match="initial_cash must be > 0"):
        engine.init_portfolio(initial_cash=0.0)


def test_step_pacing_guard_sideways_chop_records_no_action(populated_signals_db: Path) -> None:
    """Verify Pacing Guard records NO_ACTION without fee/cash burn during sideways chop."""
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-01: Tuesday, standard neutral chop, no drawdown trigger
    rec = engine.step(trade_date=date(2026, 9, 1))

    assert rec.side == "NO_ACTION"
    assert rec.signal_regime == "NO_ACTION"
    assert rec.spot_price == 80000.0
    assert rec.gross_amount_usd == 0.0
    assert rec.fee_usd == 0.0
    assert rec.net_amount_usd == 0.0
    assert rec.btc_amount == 0.0
    assert "Sideways chop" in rec.narrative

    # Balances must be 100% preserved (zero burn)
    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 400.0
    assert bal.reserve_cash == 600.0
    assert bal.btc_balance == 0.0
    assert bal.total_trades == 0


def test_step_manual_force_bypasses_chop(populated_signals_db: Path) -> None:
    """Verify MANUAL_FORCE execution mode allows operator to force order on chop day."""
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-01: Manual force execution
    rec = engine.step(
        trade_date=date(2026, 9, 1),
        daily_budget=10.0,
        execution_mode="MANUAL_FORCE",
    )

    assert rec.side == "BUY"
    assert rec.spot_price == 80000.0
    assert rec.gross_amount_usd == 10.0
    assert rec.fee_usd == 0.0100
    assert rec.net_amount_usd == 9.99
    assert rec.btc_amount == pytest.approx(0.000124875, abs=1e-8)

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 390.0
    assert bal.reserve_cash == 600.0
    assert bal.total_trades == 1


def test_step_dynamic_sniper_drawdown_trigger(populated_signals_db: Path) -> None:
    """Verify dynamic sniper order deploys 20%-35% from tactical reserve on triggers."""
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)  # base: 400, reserve: 600

    # 2026-09-03: Spot 65,000, Mayer 0.75 < 0.8 (capitulation/drawdown trigger)
    # Sniper should deploy 35% (capitulation conviction) from $600 Tactical Reserve = $210.00
    rec = engine.step(trade_date=date(2026, 9, 3))

    assert rec.side == "BUY"
    assert rec.signal_regime == "SNIPER_DEPLOYMENT"
    assert rec.spot_price == 65000.0
    assert rec.gross_amount_usd == 210.0
    assert rec.fee_usd == round(210.0 * 0.0010, 4)
    assert rec.net_amount_usd == round(210.0 - (210.0 * 0.0010), 4)
    assert "SNIPER_DEPLOYMENT" in rec.narrative
    assert "Tactical Reserve" in rec.narrative

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 400.0  # Base cash completely untouched!
    assert bal.reserve_cash == 390.0  # 600 - 210
    assert bal.total_trades == 1
    assert bal.btc_balance > 0.0


def test_step_dynamic_sniper_episode_latching(populated_signals_db: Path) -> None:
    """Verify sniper episode latches and does not fire repeatedly in persistent drawdown."""
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # First sniper deployment fires
    rec1 = engine.step(trade_date=date(2026, 9, 3))
    assert rec1.side == "BUY"
    assert rec1.signal_regime == "SNIPER_DEPLOYMENT"

    # Second call on same or persistent drawdown condition without recovery
    # Should latch and NOT fire another sniper order (latched)
    rec2 = engine.step(trade_date=date(2026, 9, 3))
    assert rec2.side == "NO_ACTION"
    assert rec2.gross_amount_usd == 0.0


def test_step_weekly_core_execution_on_sunday(populated_signals_db: Path) -> None:
    """Verify Sunday UTC executes WEEKLY_CORE routine DCA order from Base Cash only."""
    engine = PaperTradingEngine(db_path=populated_signals_db, weekly_base_usd=20.0)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-13 is a Sunday UTC without macro event and without froth
    rec = engine.step(trade_date=date(2026, 9, 13))

    assert rec.side == "BUY"
    assert rec.signal_regime == "WEEKLY_CORE"
    assert rec.gross_amount_usd == 20.0
    assert rec.fee_usd == 0.0200
    assert "WEEKLY_CORE" in rec.narrative

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 380.0  # 400 - 20
    assert bal.reserve_cash == 600.0  # Tactical reserve completely untouched!
    assert bal.total_trades == 1


def test_step_froth_freeze_halts_buying(populated_signals_db: Path) -> None:
    """Verify Froth Freeze suspends buying without cash burn when market overheats."""
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-05: Spot 98,000, Mayer 2.55, FNG 92 (HARD_FREEZE / Froth)
    rec = engine.step(trade_date=date(2026, 9, 5))

    assert rec.side == "HOLD"
    assert rec.signal_regime == "FROTH_FREEZE"
    assert rec.gross_amount_usd == 0.0
    assert rec.fee_usd == 0.0
    assert rec.btc_amount == 0.0
    assert "FROTH_FREEZE" in rec.narrative

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 400.0
    assert bal.reserve_cash == 600.0
    assert bal.total_trades == 0


def test_step_macro_circuit_breaker(populated_signals_db: Path) -> None:
    """Verify high-impact macro event trips circuit breaker and halts execution."""
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-06: has_high_impact_macro_event = True
    rec = engine.step(trade_date=date(2026, 9, 6))

    assert rec.side == "HOLD"
    assert rec.gross_amount_usd == 0.0
    assert "FROTH_FREEZE" in rec.narrative or "HARD_RISK_FREEZE" in rec.narrative

    bal = engine.get_portfolio_balance()
    assert bal.base_cash == 400.0
    assert bal.reserve_cash == 600.0


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


def test_reset_portfolio(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # Force a buy to record a trade
    engine.step(trade_date=date(2026, 9, 1), daily_budget=10.0, execution_mode="MANUAL_FORCE")
    bal_before = engine.get_portfolio_balance()
    assert bal_before.total_trades == 1

    engine.reset_portfolio(initial_cash=1000.0)
    bal_after = engine.get_portfolio_balance()
    assert bal_after.total_trades == 0
    assert bal_after.base_cash == 400.0
    assert bal_after.reserve_cash == 600.0
    assert bal_after.btc_balance == 0.0

    blotter = engine.get_trade_blotter()
    assert len(blotter) == 0


def test_portfolio_summary_and_equity_series(populated_signals_db: Path) -> None:
    engine = PaperTradingEngine(db_path=populated_signals_db)
    engine.init_portfolio(initial_cash=1000.0)

    # 2026-09-01: Chop -> NO_ACTION
    engine.step(trade_date=date(2026, 9, 1))
    # 2026-09-03: Drawdown -> Sniper Buy
    engine.step(trade_date=date(2026, 9, 3))

    summary = engine.get_portfolio_summary()
    assert summary.initial_cash == 1000.0
    assert summary.total_trades == 1  # 1 sniper buy executed, 1 chop skipped
    assert summary.btc_balance > 0.0
    assert summary.total_equity > 0.0

    series = engine.get_equity_series(limit=30)
    assert len(series) == 2
    assert series[0]["date"] == "2026-09-01"
    assert series[1]["date"] == "2026-09-03"

    blotter = engine.get_trade_blotter(limit=10)
    assert len(blotter) == 2
    assert blotter[0]["date"] == "2026-09-03"  # Latest first
