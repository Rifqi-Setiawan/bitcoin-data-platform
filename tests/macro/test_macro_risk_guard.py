"""Unit tests for AutoHedge-inspired RiskGuard macro circuit breakers (Group E)."""

from pathlib import Path

from bitcoin_data_platform.paper.risk_guard import RiskGuard


def test_risk_guard_passes_in_normal_regime(tmp_path: Path) -> None:
    """35. Regular trading passes pre-trade check under normal conditions."""
    guard = RiskGuard(kill_switch_path=tmp_path / "NON_EXISTENT_SWITCH")
    res = guard.validate_pre_trade(
        spot_price=85000.0,
        last_known_price=84000.0,
        available_cash=1000.0,
        required_cash=100.0,
        composite_mni=0.25,
        black_swan_flag=False,
        macro_event_proximity_minutes=180,
    )
    assert res.allowed is True
    assert res.passed is True
    assert "passed" in res.reason.lower()


def test_risk_guard_blocks_on_black_swan_flag(tmp_path: Path) -> None:
    """36. Trips circuit breaker on black swan alert."""
    guard = RiskGuard(kill_switch_path=tmp_path / "NON_EXISTENT_SWITCH")
    res = guard.validate_pre_trade(
        spot_price=85000.0,
        last_known_price=84000.0,
        available_cash=1000.0,
        required_cash=100.0,
        composite_mni=0.25,
        black_swan_flag=True,
    )
    assert res.allowed is False
    assert res.passed is False
    assert "CIRCUIT_BREAKER_TRIPPED" in res.reason
    assert "Black Swan" in res.reason


def test_risk_guard_blocks_near_macro_announcement(tmp_path: Path) -> None:
    """37. Trips breaker within +/- 120-minute high-impact macro window."""
    guard = RiskGuard(kill_switch_path=tmp_path / "NON_EXISTENT_SWITCH")
    # 45 minutes before release
    res_before = guard.validate_pre_trade(
        spot_price=85000.0,
        last_known_price=85000.0,
        available_cash=1000.0,
        required_cash=50.0,
        macro_event_proximity_minutes=-45,
    )
    assert res_before.allowed is False
    assert "High-impact macro release window" in res_before.reason

    # 90 minutes after release
    res_after = guard.validate_pre_trade(
        spot_price=85000.0,
        last_known_price=85000.0,
        available_cash=1000.0,
        required_cash=50.0,
        macro_event_proximity_minutes=90,
    )
    assert res_after.allowed is False
    assert "High-impact macro release window" in res_after.reason


def test_risk_guard_allows_post_announcement(tmp_path: Path) -> None:
    """38. Passes once macro window expires (> 120 minutes)."""
    guard = RiskGuard(kill_switch_path=tmp_path / "NON_EXISTENT_SWITCH")
    res = guard.validate_pre_trade(
        spot_price=85000.0,
        last_known_price=85000.0,
        available_cash=1000.0,
        required_cash=50.0,
        macro_event_proximity_minutes=150,  # 2.5 hours post announcement
    )
    assert res.allowed is True
    assert res.passed is True


def test_risk_guard_blocks_severe_negative_mni(tmp_path: Path) -> None:
    """39. Trips breaker when MNI < -0.65 (severe contraction)."""
    guard = RiskGuard(kill_switch_path=tmp_path / "NON_EXISTENT_SWITCH")
    res = guard.validate_pre_trade(
        spot_price=85000.0,
        last_known_price=85000.0,
        available_cash=1000.0,
        required_cash=50.0,
        composite_mni=-0.72,
    )
    assert res.allowed is False
    assert "CIRCUIT_BREAKER_TRIPPED" in res.reason
    assert "Severe Macro Liquidity Contraction" in res.reason


def test_risk_guard_preserves_solvency_and_kill_switch(tmp_path: Path) -> None:
    """40. Existing Phase 15 checks remain active (kill switch, insolvency, price spike)."""
    # 1. Kill Switch
    ks = tmp_path / "KILL_SWITCH_ACTIVE"
    ks.touch()
    guard_ks = RiskGuard(kill_switch_path=ks)
    res_ks = guard_ks.validate_pre_trade(
        spot_price=85000.0,
        last_known_price=85000.0,
        available_cash=1000.0,
        required_cash=50.0,
    )
    assert res_ks.allowed is False
    assert "Kill switch active" in res_ks.reason

    # 2. Insufficient funds
    guard_ok = RiskGuard(kill_switch_path=tmp_path / "INACTIVE")
    res_solvency = guard_ok.validate_pre_trade(
        spot_price=85000.0,
        last_known_price=85000.0,
        available_cash=50.0,
        required_cash=100.0,
    )
    assert res_solvency.allowed is False
    assert "Insufficient funds" in res_solvency.reason

    # 3. Price deviation > 20%
    res_dev = guard_ok.validate_pre_trade(
        spot_price=120000.0,
        last_known_price=80000.0,
        available_cash=1000.0,
        required_cash=50.0,
    )
    assert res_dev.allowed is False
    assert "Price deviation exceeded" in res_dev.reason
