"""Integration tests for Dashboard Server Paper Trading Portfolio API endpoints."""

import json
import threading
import urllib.request
from collections.abc import Generator
from datetime import date
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from bitcoin_data_platform.dashboard.server import (
    DashboardServer,
    create_dashboard_server,
)
from bitcoin_data_platform.paper.engine import PaperTradingEngine


@pytest.fixture
def test_dashboard_server(
    tmp_path: Path,
) -> Generator[tuple[DashboardServer, str, Path], None, None]:
    """Start an ephemeral DashboardServer with temporary DuckDB database."""
    db_file = tmp_path / "test_api_platform.duckdb"
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
        ('2026-09-18 00:00:00+00', 82000.0, 71000.0, 1.15, 1.8, 55, 'Neutral',
         false, 'STANDARD_DCA');
        """
    )
    con.close()

    server = create_dashboard_server(
        host="127.0.0.1",
        port=0,
        db_path=db_file,
    )
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield server, base_url, db_file

    server.shutdown()
    server.server_close()
    thread.join(timeout=2.0)


def test_api_portfolio_fallback_when_uninitialized(
    test_dashboard_server: tuple[DashboardServer, str, Path],
) -> None:
    _, base_url, _ = test_dashboard_server

    url = f"{base_url}/api/portfolio"
    with urllib.request.urlopen(url) as response:
        assert response.status == 200
        assert "application/json" in response.headers.get("Content-Type", "")
        data = json.loads(response.read().decode("utf-8"))

    assert data["portfolio_id"] == "default"
    assert data["initial_cash"] == 1000.0
    assert data["total_equity"] == 1000.0
    assert data["base_cash"] == 700.0
    assert data["reserve_cash"] == 300.0
    assert data["btc_balance"] == 0.0


def test_api_portfolio_populated(
    test_dashboard_server: tuple[DashboardServer, str, Path],
) -> None:
    _, base_url, db_file = test_dashboard_server

    engine = PaperTradingEngine(db_path=db_file)
    engine.init_portfolio(initial_cash=1000.0)
    engine.step(trade_date=date(2026, 9, 18), daily_budget=10.0)

    with patch(
        "bitcoin_data_platform.dashboard.server._fetch_live_spot_price",
        return_value=84000.0,
    ):
        url = f"{base_url}/api/portfolio"
        with urllib.request.urlopen(url) as response:
            assert response.status == 200
            data = json.loads(response.read().decode("utf-8"))

    assert data["portfolio_id"] == "default"
    assert data["total_trades"] == 1
    assert data["current_spot_price"] == 84000.0
    assert data["btc_balance"] > 0.0
    assert data["total_equity"] > 0.0


def test_api_portfolio_equity(
    test_dashboard_server: tuple[DashboardServer, str, Path],
) -> None:
    _, base_url, db_file = test_dashboard_server

    engine = PaperTradingEngine(db_path=db_file)
    engine.init_portfolio(initial_cash=1000.0)
    engine.step(trade_date=date(2026, 9, 18), daily_budget=10.0)

    url = f"{base_url}/api/portfolio/equity?limit=30"
    with urllib.request.urlopen(url) as response:
        assert response.status == 200
        series = json.loads(response.read().decode("utf-8"))

    assert isinstance(series, list)
    assert len(series) == 1
    point = series[0]
    assert point["date"] == "2026-09-18"
    assert "equity" in point
    assert "cash" in point
    assert "reserve" in point
    assert "benchmark" in point


def test_api_portfolio_trades(
    test_dashboard_server: tuple[DashboardServer, str, Path],
) -> None:
    _, base_url, db_file = test_dashboard_server

    engine = PaperTradingEngine(db_path=db_file)
    engine.init_portfolio(initial_cash=1000.0)
    engine.step(trade_date=date(2026, 9, 18), daily_budget=10.0)

    url = f"{base_url}/api/portfolio/trades?limit=10"
    with urllib.request.urlopen(url) as response:
        assert response.status == 200
        trades = json.loads(response.read().decode("utf-8"))

    assert isinstance(trades, list)
    assert len(trades) == 1
    trade = trades[0]
    assert trade["date"] == "2026-09-18"
    assert trade["side"] == "BUY"
    assert trade["signal"] == "STANDARD_DCA"
    assert trade["spot_price"] == 82000.0
    assert trade["gross_usd"] == 10.0
    assert trade["fee_usd"] == 0.0100
    assert trade["btc_amount"] > 0.0
    assert "narrative" in trade
