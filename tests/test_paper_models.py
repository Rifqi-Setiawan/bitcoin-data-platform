"""Unit tests for Paper Trading models and data contracts."""

from datetime import UTC, date, datetime

from bitcoin_data_platform.paper.models import (
    PaperPortfolioBalance,
    PaperSnapshotRecord,
    PaperSummary,
    PaperTradeRecord,
    RiskCheckResult,
)


def test_paper_portfolio_balance_properties_and_serialization() -> None:
    now = datetime(2026, 9, 18, 12, 0, 0, tzinfo=UTC)
    balance = PaperPortfolioBalance(
        portfolio_id="default",
        initial_cash=1000.0,
        base_cash=700.0,
        reserve_cash=300.0,
        btc_balance=0.005,
        total_contributed=1000.0,
        last_updated_utc=now,
        total_trades=5,
    )

    assert balance.total_cash == 1000.0
    data = balance.to_dict()

    assert data["portfolio_id"] == "default"
    assert data["initial_cash"] == 1000.0
    assert data["base_cash"] == 700.0
    assert data["reserve_cash"] == 300.0
    assert data["total_cash"] == 1000.0
    assert data["btc_balance"] == 0.005
    assert data["total_trades"] == 5
    assert data["last_updated_utc"] == "2026-09-18T12:00:00+00:00"


def test_paper_trade_record_to_dict() -> None:
    exec_time = datetime(2026, 9, 18, 14, 30, 0, tzinfo=UTC)
    trade_d = date(2026, 9, 18)
    record = PaperTradeRecord(
        trade_id="tr_123456",
        portfolio_id="default",
        executed_at_utc=exec_time,
        trade_date=trade_d,
        side="BUY",
        signal_regime="AGGRESSIVE_ACCUMULATE",
        spot_price=78080.0,
        gross_amount_usd=17.50,
        fee_usd=0.0175,
        net_amount_usd=17.4825,
        btc_amount=0.0002239,
        narrative="🟢 AKUMULASI AGRESIF",
    )

    d = record.to_dict()
    assert d["trade_id"] == "tr_123456"
    assert d["trade_date"] == "2026-09-18"
    assert d["executed_at_utc"] == "2026-09-18T14:30:00+00:00"
    assert d["side"] == "BUY"
    assert d["signal_regime"] == "AGGRESSIVE_ACCUMULATE"
    assert d["spot_price"] == 78080.0
    assert d["gross_amount_usd"] == 17.50
    assert d["fee_usd"] == 0.0175
    assert d["net_amount_usd"] == 17.48
    assert d["btc_amount"] == 0.0002239
    assert d["narrative"] == "🟢 AKUMULASI AGRESIF"


def test_paper_snapshot_record_to_dict() -> None:
    snap_date = date(2026, 9, 18)
    snap = PaperSnapshotRecord(
        snapshot_date=snap_date,
        portfolio_id="default",
        base_cash=412.50,
        reserve_cash=280.00,
        total_cash=692.50,
        btc_balance=0.00448123,
        btc_price=78182.0,
        portfolio_equity=1042.85,
        unrealized_pnl_usd=42.85,
        unrealized_pnl_pct=4.29,
        benchmark_equity=1018.40,
    )

    d = snap.to_dict()
    assert d["snapshot_date"] == "2026-09-18"
    assert d["total_cash"] == 692.50
    assert d["portfolio_equity"] == 1042.85
    assert d["unrealized_pnl_usd"] == 42.85
    assert d["unrealized_pnl_pct"] == 4.29
    assert d["benchmark_equity"] == 1018.40


def test_paper_summary_matches_api_contract() -> None:
    summary = PaperSummary(
        portfolio_id="default",
        initial_cash=1000.00,
        total_equity=1042.85,
        unrealized_pnl_usd=42.85,
        unrealized_pnl_pct=4.29,
        base_cash=412.50,
        reserve_cash=280.00,
        total_cash=692.50,
        btc_balance=0.00448123,
        btc_value_usd=350.35,
        avg_buy_price=75850.20,
        current_spot_price=78182.00,
        acquisition_discount_pct=2.98,
        total_trades=18,
        benchmark_equity=1018.40,
        outperformance_usd=24.45,
    )

    data = summary.to_dict()
    assert data["portfolio_id"] == "default"
    assert data["initial_cash"] == 1000.00
    assert data["total_equity"] == 1042.85
    assert data["unrealized_pnl_usd"] == 42.85
    assert data["unrealized_pnl_pct"] == 4.29
    assert data["base_cash"] == 412.50
    assert data["reserve_cash"] == 280.00
    assert data["total_cash"] == 692.50
    assert data["btc_balance"] == 0.00448123
    assert data["btc_value_usd"] == 350.35
    assert data["avg_buy_price"] == 75850.20
    assert data["current_spot_price"] == 78182.00
    assert data["acquisition_discount_pct"] == 2.98
    assert data["total_trades"] == 18
    assert data["benchmark_equity"] == 1018.40
    assert data["outperformance_usd"] == 24.45


def test_risk_check_result_serialization() -> None:
    res_pass = RiskCheckResult(allowed=True, reason="All risk checks passed", details={"test": 1})
    assert res_pass.allowed is True
    assert res_pass.to_dict()["allowed"] is True

    res_fail = RiskCheckResult(allowed=False, reason="Kill switch active")
    assert res_fail.allowed is False
    assert res_fail.to_dict()["reason"] == "Kill switch active"
