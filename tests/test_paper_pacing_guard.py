"""Unit and property tests for PacingGuard and updated RiskGuard invariants (Phase 18)."""

from datetime import UTC, date, datetime
from pathlib import Path

from bitcoin_data_platform.pacing.models import CapitalPool, PacingState
from bitcoin_data_platform.paper.models import PaperPortfolioBalance
from bitcoin_data_platform.paper.pacing_guard import PacingGuard
from bitcoin_data_platform.paper.risk_guard import RiskGuard


def _portfolio(base: float = 400.0, reserve: float = 600.0) -> PaperPortfolioBalance:
    return PaperPortfolioBalance(
        portfolio_id="test",
        initial_cash=base + reserve,
        base_cash=base,
        reserve_cash=reserve,
        btc_balance=0.0,
        total_contributed=base + reserve,
        last_updated_utc=datetime.now(UTC),
        total_trades=0,
    )


# ---------------------------------------------------------------------------
# PacingGuard State Transition & Precedence Tests
# ---------------------------------------------------------------------------


def test_pacing_guard_precedence_froth_over_drawdown() -> None:
    """AC-4: Froth/hard safety precedence freezes buying even when drawdown trigger coincides."""
    guard = PacingGuard()
    # Coincident event: Drawdown (-15% 7d) AND Overheat (FNG 85, Mayer 2.2)
    decision, armed = guard.evaluate(
        trade_date=date(2026, 9, 20),  # Sunday
        base_cash=400.0,
        reserve_cash=600.0,
        sniper_armed=True,
        drawdown_7d=-0.15,
        fng_value=85,
        mayer_multiple=2.2,
    )
    assert decision.state == PacingState.FROTH_FREEZE
    assert decision.action == "HOLD"
    assert decision.authorized_amount_usd == 0.0
    assert decision.reason_code == "FROTH_FNG_MAYER"
    # Sniper should NOT have been spent/latched on freeze day
    assert armed is True


def test_pacing_guard_precedence_hard_safety_over_all() -> None:
    """AC-4: Black swan or macro release window dominates all other states."""
    guard = PacingGuard()
    decision, armed = guard.evaluate(
        trade_date=date(2026, 9, 20),
        base_cash=400.0,
        reserve_cash=600.0,
        sniper_armed=True,
        drawdown_7d=-0.15,
        has_high_impact_macro_event=True,
    )
    assert decision.state == PacingState.FROTH_FREEZE
    assert decision.action == "HOLD"
    assert decision.authorized_amount_usd == 0.0
    assert decision.reason_code == "HARD_RISK_FREEZE"


def test_pacing_guard_dynamic_sniper_mild_drawdown() -> None:
    """AC-3 / Task: Dynamic sniper orders deploy 20%-35% from reserve during triggers."""
    guard = PacingGuard()
    # Mild trigger: 24h return -5.0%
    decision, armed = guard.evaluate(
        trade_date=date(2026, 9, 15),  # Tuesday
        base_cash=400.0,
        reserve_cash=600.0,
        sniper_armed=True,
        return_24h=-0.05,
    )
    assert decision.state == PacingState.SNIPER_DEPLOYMENT
    assert decision.action == "BUY"
    assert decision.pool == CapitalPool.TACTICAL_RESERVE
    assert decision.is_sniper is True
    # At boundary -5%, dynamic deployment should be 20%
    assert decision.sniper_pct == 0.20
    assert decision.authorized_amount_usd == 120.0  # 20% of 600.0
    assert armed is False  # Latched


def test_pacing_guard_dynamic_sniper_deep_drawdown() -> None:
    """Verify deep drawdown scales sniper allocation up to 35%."""
    guard = PacingGuard()
    # Deep trigger: 24h return -10.0%
    decision, armed = guard.evaluate(
        trade_date=date(2026, 9, 15),
        base_cash=400.0,
        reserve_cash=600.0,
        sniper_armed=True,
        return_24h=-0.10,
    )
    assert decision.state == PacingState.SNIPER_DEPLOYMENT
    assert decision.action == "BUY"
    assert decision.pool == CapitalPool.TACTICAL_RESERVE
    assert decision.is_sniper is True
    # At -10% or deeper, dynamic deployment reaches 35%
    assert decision.sniper_pct == 0.35
    assert decision.authorized_amount_usd == 210.0  # 35% of 600.0
    assert armed is False


def test_pacing_guard_dynamic_sniper_capitulation() -> None:
    """Verify capitulation trigger (MVRV < 1.0 or Mayer < 0.8) deploys 35% maximum conviction."""
    guard = PacingGuard()
    decision, armed = guard.evaluate(
        trade_date=date(2026, 9, 15),
        base_cash=400.0,
        reserve_cash=600.0,
        sniper_armed=True,
        mvrv_ratio=0.85,  # Capitulation (< 1.0)
    )
    assert decision.state == PacingState.SNIPER_DEPLOYMENT
    assert decision.sniper_pct == 0.35
    assert decision.authorized_amount_usd == 210.0
    assert "CAPITULATION" in decision.reason_code


def test_pacing_guard_episode_latching_and_hysteresis_rearm() -> None:
    """AC-6: One drawdown episode cannot repeatedly consume reserve without re-arm."""
    guard = PacingGuard(drawdown_rearm_7d=-0.08)

    # 1. Day 1: Trigger fires, latches episode
    dec1, armed1 = guard.evaluate(
        trade_date=date(2026, 9, 15),
        base_cash=400.0,
        reserve_cash=600.0,
        sniper_armed=True,
        return_24h=-0.06,
    )
    assert dec1.state == PacingState.SNIPER_DEPLOYMENT
    assert armed1 is False

    # 2. Day 2: Still in persistent drawdown (-10% 7d), armed is False -> suppressed (IDLE_CHOP)
    dec2, armed2 = guard.evaluate(
        trade_date=date(2026, 9, 16),
        base_cash=400.0,
        reserve_cash=480.0,
        sniper_armed=armed1,
        drawdown_7d=-0.10,
    )
    assert dec2.state == PacingState.IDLE_CHOP
    assert dec2.action == "NO_ACTION"
    assert armed2 is False

    # 3. Day 3: Price recovers, drawdown rises to -0.05 (> -0.08 re-arm threshold) -> re-arms!
    dec3, armed3 = guard.evaluate(
        trade_date=date(2026, 9, 17),
        base_cash=400.0,
        reserve_cash=480.0,
        sniper_armed=armed2,
        drawdown_7d=-0.05,
        return_24h=0.01,
    )
    assert dec3.state == PacingState.IDLE_CHOP
    assert armed3 is True  # Re-armed!


def test_pacing_guard_weekly_core_and_runway_reduction() -> None:
    """AC-7: Base runway guard (< 8 weeks) halves requested weekly base buy."""
    guard = PacingGuard(weekly_base_usd=20.0, runway_min_weeks=8)

    # Sunday with ample runway ($400 Base / $20 = 20 weeks >= 8 weeks)
    dec_ample, _ = guard.evaluate(
        trade_date=date(2026, 9, 20),  # Sunday
        base_cash=400.0,
        reserve_cash=600.0,
        is_weekly_cadence_day=True,
    )
    assert dec_ample.state == PacingState.WEEKLY_CORE
    assert dec_ample.pool == CapitalPool.BASE
    assert dec_ample.authorized_amount_usd == 20.0
    assert dec_ample.reason_code == "WEEKLY_CADENCE_DUE"

    # Sunday with low runway ($100 Base / $20 = 5 weeks < 8 weeks) -> halved to $10.00
    dec_low, _ = guard.evaluate(
        trade_date=date(2026, 9, 20),
        base_cash=100.0,
        reserve_cash=600.0,
        is_weekly_cadence_day=True,
    )
    assert dec_low.state == PacingState.WEEKLY_CORE
    assert dec_low.authorized_amount_usd == 10.0  # 50% of $20
    assert dec_low.reason_code == "BASE_RUNWAY_REDUCTION"


def test_pacing_guard_sideways_chop_zero_execution() -> None:
    """AC-2: Neutral/chop AUTO evaluations settle no trade."""
    guard = PacingGuard()
    # Wednesday, no triggers, normal conditions
    dec, _ = guard.evaluate(
        trade_date=date(2026, 9, 16),  # Wednesday
        base_cash=400.0,
        reserve_cash=600.0,
        drawdown_7d=-0.02,
        return_24h=0.005,
        fng_value=50,
        mayer_multiple=1.1,
    )
    assert dec.state == PacingState.IDLE_CHOP
    assert dec.action == "NO_ACTION"
    assert dec.authorized_amount_usd == 0.0
    assert dec.reason_code == "NO_EVENT_CHOP"


# ---------------------------------------------------------------------------
# RiskGuard Updated Invariant Tests
# ---------------------------------------------------------------------------


def test_risk_guard_pool_isolation_rejects_cross_pool_overdraft(tmp_path: Path) -> None:
    """AC-3: Weekly events debit only Base; sniper events debit only Tactical Reserve."""
    guard = RiskGuard(kill_switch_path=tmp_path / "INACTIVE")
    p = _portfolio(base=50.0, reserve=500.0)

    # Base order requesting $60 from Base pool (which only has $50)
    res_base = guard.validate_trade(
        portfolio=p,
        side="BUY",
        gross_amount_usd=60.0,
        spot_price=80000.0,
        pool="00_BASE",
    )
    assert res_base.allowed is False
    assert "Insufficient funds in Base Cash" in res_base.reason

    # Tactical order requesting $600 from Tactical pool (which only has $500)
    res_reserve = guard.validate_trade(
        portfolio=p,
        side="BUY",
        gross_amount_usd=600.0,
        spot_price=80000.0,
        pool="00_TACTICAL_RESERVE",
        is_sniper=True,
    )
    assert res_reserve.allowed is False
    assert "Insufficient funds in Tactical Reserve" in res_reserve.reason


def test_risk_guard_sniper_cap_invariant(tmp_path: Path) -> None:
    """Verify RiskGuard clamps / rejects sniper orders exceeding the 35% tactical reserve cap."""
    guard = RiskGuard(kill_switch_path=tmp_path / "INACTIVE")
    p = _portfolio(base=400.0, reserve=600.0)  # 35% cap is $210.00

    # Proposing $250.00 sniper order (> $210.00)
    res = guard.validate_trade(
        portfolio=p,
        side="BUY",
        gross_amount_usd=250.0,
        spot_price=80000.0,
        is_sniper=True,
        pool="00_TACTICAL_RESERVE",
    )
    assert res.allowed is False
    assert "SNIPER_CAP_EXCEEDED" in res.reason


def test_risk_guard_tactical_48h_velocity_bound(tmp_path: Path) -> None:
    """AC-7: 40%-per-48h tactical velocity bound."""
    guard = RiskGuard(kill_switch_path=tmp_path / "INACTIVE")
    # Opening tactical reserve = 600. Prior 48h debits = 200. Current reserve = 400.
    # Max 48h allowed = 40% of (400 + 200) = 240. Remaining = 240 - 200 = 40.
    p = _portfolio(base=400.0, reserve=400.0)

    # Proposing $50 (> remaining $40)
    res = guard.validate_trade(
        portfolio=p,
        side="BUY",
        gross_amount_usd=50.0,
        spot_price=80000.0,
        is_sniper=True,
        pool="00_TACTICAL_RESERVE",
        tactical_debits_48h=200.0,
    )
    assert res.allowed is False
    assert "TACTICAL_48H_CAP_EXCEEDED" in res.reason


def test_risk_guard_overheat_lockout_invariant(tmp_path: Path) -> None:
    """Verify RiskGuard deterministically halts buying when FNG >= 80 and Mayer >= 2.0."""
    guard = RiskGuard(kill_switch_path=tmp_path / "INACTIVE")
    p = _portfolio(base=400.0, reserve=600.0)

    res = guard.validate_trade(
        portfolio=p,
        side="BUY",
        gross_amount_usd=20.0,
        spot_price=100000.0,
        fng_value=85,
        mayer_multiple=2.1,
    )
    assert res.allowed is False
    assert "Overheat Lockout" in res.reason
