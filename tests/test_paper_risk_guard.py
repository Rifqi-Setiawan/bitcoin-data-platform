"""Unit tests for RiskGuard pre-trade validation gatekeeper."""

from datetime import UTC, datetime
from pathlib import Path

from bitcoin_data_platform.paper.models import PaperPortfolioBalance
from bitcoin_data_platform.paper.risk_guard import RiskGuard


def _create_portfolio(base: float = 700.0, reserve: float = 300.0) -> PaperPortfolioBalance:
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


def test_risk_guard_passes_valid_buy(tmp_path: Path) -> None:
    guard = RiskGuard(kill_switch_path=tmp_path / "PAPER_KILL_SWITCH")
    portfolio = _create_portfolio(700.0, 300.0)

    result = guard.validate_trade(
        portfolio=portfolio,
        side="BUY",
        gross_amount_usd=20.0,
        spot_price=80000.0,
    )
    assert result.allowed is True
    assert "All risk checks passed" in result.reason


def test_risk_guard_kill_switch_active_fails_closed(tmp_path: Path) -> None:
    kill_switch = tmp_path / "PAPER_KILL_SWITCH"
    kill_switch.touch()

    guard = RiskGuard(kill_switch_path=kill_switch)
    portfolio = _create_portfolio(700.0, 300.0)

    result = guard.validate_trade(
        portfolio=portfolio,
        side="BUY",
        gross_amount_usd=10.0,
        spot_price=80000.0,
    )
    assert result.allowed is False
    assert "Kill switch active" in result.reason
    assert result.details["kill_switch_path"] == str(kill_switch)


def test_risk_guard_rejects_non_positive_spot_price(tmp_path: Path) -> None:
    guard = RiskGuard(kill_switch_path=tmp_path / "PAPER_KILL_SWITCH")
    portfolio = _create_portfolio()

    res_zero = guard.validate_trade(portfolio, side="BUY", gross_amount_usd=10.0, spot_price=0.0)
    assert res_zero.allowed is False
    assert "Invalid spot price" in res_zero.reason

    res_neg = guard.validate_trade(portfolio, side="BUY", gross_amount_usd=10.0, spot_price=-100.0)
    assert res_neg.allowed is False


def test_risk_guard_rejects_excessive_price_deviation(tmp_path: Path) -> None:
    guard = RiskGuard(
        kill_switch_path=tmp_path / "PAPER_KILL_SWITCH",
        max_price_deviation_pct=20.0,
    )
    portfolio = _create_portfolio()

    # 100,000 vs 80,000 is 25% deviation -> exceeds 20%
    result = guard.validate_trade(
        portfolio=portfolio,
        side="BUY",
        gross_amount_usd=10.0,
        spot_price=100000.0,
        reference_price=80000.0,
    )
    assert result.allowed is False
    assert "Price deviation exceeded" in result.reason
    assert result.details["deviation_pct"] == 25.0


def test_risk_guard_price_within_deviation_passes(tmp_path: Path) -> None:
    guard = RiskGuard(
        kill_switch_path=tmp_path / "PAPER_KILL_SWITCH",
        max_price_deviation_pct=20.0,
    )
    portfolio = _create_portfolio()

    # 85,000 vs 80,000 is 6.25% deviation -> allowed
    result = guard.validate_trade(
        portfolio=portfolio,
        side="BUY",
        gross_amount_usd=10.0,
        spot_price=85000.0,
        reference_price=80000.0,
    )
    assert result.allowed is True


def test_risk_guard_rejects_insolvent_orders(tmp_path: Path) -> None:
    guard = RiskGuard(kill_switch_path=tmp_path / "PAPER_KILL_SWITCH")
    portfolio = _create_portfolio(base=50.0, reserve=20.0)  # total cash = 70.0

    result = guard.validate_trade(
        portfolio=portfolio,
        side="BUY",
        gross_amount_usd=75.0,  # exceeds 70.0
        spot_price=80000.0,
    )
    assert result.allowed is False
    assert "Insufficient funds" in result.reason
    assert result.details["gross_amount_usd"] == 75.0


def test_risk_guard_rejects_negative_gross_amount(tmp_path: Path) -> None:
    guard = RiskGuard(kill_switch_path=tmp_path / "PAPER_KILL_SWITCH")
    portfolio = _create_portfolio()

    result = guard.validate_trade(
        portfolio=portfolio,
        side="BUY",
        gross_amount_usd=-5.0,
        spot_price=80000.0,
    )
    assert result.allowed is False
    assert "Negative trade gross amount" in result.reason


def test_risk_guard_macro_event_policy_rejection(tmp_path: Path) -> None:
    # Policy with reject_on_macro = True
    guard_reject = RiskGuard(
        kill_switch_path=tmp_path / "PAPER_KILL_SWITCH",
        reject_on_macro=True,
    )
    portfolio = _create_portfolio()

    res_reject = guard_reject.validate_trade(
        portfolio=portfolio,
        side="BUY",
        gross_amount_usd=10.0,
        spot_price=80000.0,
        macro_event=True,
    )
    assert res_reject.allowed is False
    assert "High-impact macro event active" in res_reject.reason

    # Default policy with reject_on_macro = False (defers to engine reserve diversion)
    guard_default = RiskGuard(
        kill_switch_path=tmp_path / "PAPER_KILL_SWITCH",
        reject_on_macro=False,
    )
    res_allowed = guard_default.validate_trade(
        portfolio=portfolio,
        side="BUY",
        gross_amount_usd=10.0,
        spot_price=80000.0,
        macro_event=True,
    )
    assert res_allowed.allowed is True
