"""Automated tests for Neuro-Symbolic Invariant Solver and Pre-Trade RiskGuard (Group C)."""

from __future__ import annotations

from unittest.mock import MagicMock

from bitcoin_data_platform.committee.invariant_solver import InvariantSolver
from bitcoin_data_platform.paper.risk_guard import RiskGuard


# 17. test_invariant_solvency_clamping
def test_invariant_solvency_clamping() -> None:
    solver = InvariantSolver()
    # Total available cash = 50 (base 50, reserve 0). Proposing $100.
    receipt = solver.solve_and_clamp(
        proposed_usd=100.0,
        available_base_cash=50.0,
        available_reserve_cash=0.0,
    )
    assert receipt.is_clamped is True
    assert receipt.clamped_allocation_usd == 50.0
    assert any("SOLVENCY" in r for r in receipt.triggered_rules)


# 18. test_invariant_daily_15pct_reserve_cap
def test_invariant_daily_15pct_reserve_cap() -> None:
    solver = InvariantSolver()
    # Base 0, Reserve 1000. 15% cap = $150.00. Proposing $250.
    receipt = solver.solve_and_clamp(
        proposed_usd=250.0,
        available_base_cash=0.0,
        available_reserve_cash=1000.0,
        proposed_tactical_usd=250.0,
    )
    assert receipt.is_clamped is True
    assert receipt.clamped_allocation_usd == 150.0
    assert any("DAILY_RESERVE_CAP" in r for r in receipt.triggered_rules)


# 19. test_invariant_macro_proximity_breaker_trips
def test_invariant_macro_proximity_breaker_trips() -> None:
    solver = InvariantSolver()
    # Proximity is 45 minutes (<= 120 minutes)
    receipt = solver.solve_and_clamp(
        proposed_usd=30.0,
        available_base_cash=100.0,
        available_reserve_cash=200.0,
        macro_proximity_minutes=45,
    )
    assert receipt.is_clamped is True
    assert receipt.clamped_allocation_usd == 0.0
    assert any("CIRCUIT_BREAKER" in r for r in receipt.triggered_rules)


# 20. test_invariant_macro_proximity_allows_outside_window
def test_invariant_macro_proximity_allows_outside_window() -> None:
    solver = InvariantSolver()
    # Proximity is 180 minutes (> 120 minutes)
    receipt = solver.solve_and_clamp(
        proposed_usd=25.0,
        available_base_cash=100.0,
        available_reserve_cash=500.0,
        macro_proximity_minutes=180,
    )
    assert receipt.is_clamped is False
    assert receipt.clamped_allocation_usd == 25.0


# 21. test_invariant_black_swan_emergency_halt
def test_invariant_black_swan_emergency_halt() -> None:
    solver = InvariantSolver()
    receipt = solver.solve_and_clamp(
        proposed_usd=50.0,
        available_base_cash=500.0,
        available_reserve_cash=500.0,
        has_black_swan=True,
    )
    assert receipt.is_clamped is True
    assert receipt.clamped_allocation_usd == 0.0
    assert any("Black Swan" in r for r in receipt.triggered_rules)


# 22. test_invariant_max_drawdown_freezes_tactical
def test_invariant_max_drawdown_freezes_tactical() -> None:
    solver = InvariantSolver()
    # Drawdown is 30% (> 25%). Base proposal 10, tactical proposal 40.
    receipt = solver.solve_and_clamp(
        proposed_usd=50.0,
        available_base_cash=50.0,
        available_reserve_cash=500.0,
        portfolio_drawdown=0.30,
        proposed_base_usd=10.0,
        proposed_tactical_usd=40.0,
    )
    assert receipt.is_clamped is True
    assert receipt.clamped_allocation_usd == 10.0
    assert any("DRAWDOWN_LIMIT" in r for r in receipt.triggered_rules)


# 23. test_invariant_price_deviation_brake
def test_invariant_price_deviation_brake() -> None:
    solver = InvariantSolver()
    # Spot 80,000 vs Last Close 60,000 -> 33.3% deviation (> 20%)
    receipt = solver.solve_and_clamp(
        proposed_usd=20.0,
        available_base_cash=100.0,
        available_reserve_cash=100.0,
        spot_price=80000.0,
        last_validated_price=60000.0,
    )
    assert receipt.is_clamped is True
    assert receipt.clamped_allocation_usd == 0.0
    assert any("SAFETY_BRAKE" in r for r in receipt.triggered_rules)


# 24. test_solver_normal_conditions_zero_clamping
def test_solver_normal_conditions_zero_clamping() -> None:
    solver = InvariantSolver()
    receipt = solver.solve_and_clamp(
        proposed_usd=25.0,
        available_base_cash=100.0,
        available_reserve_cash=500.0,
        has_black_swan=False,
        macro_proximity_minutes=None,
        portfolio_drawdown=0.05,
        spot_price=65000.0,
        last_validated_price=64000.0,
    )
    assert receipt.is_clamped is False
    assert receipt.clamped_allocation_usd == 25.0
    assert len(receipt.triggered_rules) == 0
    assert "terpenuhi" in receipt.explanation


# 25. test_clamping_receipt_audit_trail
def test_clamping_receipt_audit_trail() -> None:
    solver = InvariantSolver()
    receipt = solver.solve_and_clamp(
        proposed_usd=100.0,
        available_base_cash=20.0,
        available_reserve_cash=0.0,
    )
    receipt_dict = receipt.to_dict()
    assert "proposed_allocation_usd" in receipt_dict
    assert "clamped_allocation_usd" in receipt_dict
    assert "is_clamped" in receipt_dict
    assert "triggered_rules" in receipt_dict
    assert "explanation" in receipt_dict
    assert receipt_dict["is_clamped"] is True
    assert receipt_dict["clamped_allocation_usd"] == 20.0


# 26. test_risk_guard_integration_hook
def test_risk_guard_integration_hook() -> None:
    mock_solver = MagicMock(spec=InvariantSolver)
    from bitcoin_data_platform.committee.models import ClampingReceipt

    mock_solver.solve_and_clamp.return_value = ClampingReceipt(
        proposed_allocation_usd=20.0,
        clamped_allocation_usd=20.0,
        is_clamped=False,
        triggered_rules=[],
        explanation="Hook verified",
    )

    guard = RiskGuard(invariant_solver=mock_solver)
    res = guard.validate_pre_trade(
        spot_price=65000.0,
        available_cash=100.0,
        required_cash=20.0,
    )
    assert res.allowed is True
    mock_solver.solve_and_clamp.assert_called_once()
    assert "clamping_receipt" in res.details
