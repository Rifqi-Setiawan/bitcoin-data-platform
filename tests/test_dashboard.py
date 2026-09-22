"""Automated unit tests for Bitcoin Market Hub web dashboard and API server."""

import csv
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.dashboard.server import (
    DashboardServer,
    _clear_price_cache,
    _fetch_live_spot_price,
    _price_cache,
    _price_cache_lock,
    create_dashboard_server,
)


@pytest.fixture
def test_server(tmp_path: Path) -> Generator[tuple[DashboardServer, str], None, None]:
    """Start an ephemeral DashboardServer in background thread."""
    server = create_dashboard_server(
        host="127.0.0.1",
        port=0,
        db_path=tmp_path / "non_existent.duckdb",
    )
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield server, base_url

    server.shutdown()
    server.server_close()
    thread.join(timeout=2.0)


@pytest.fixture
def populated_duckdb(tmp_path: Path) -> Path:
    """Create a temporary DuckDB database populated with test views."""
    db_file = tmp_path / "test_platform.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("SET TimeZone='UTC';")

    con.execute("""
        CREATE TABLE mart_btc_market_and_network_daily (
            trade_date_utc TIMESTAMPTZ,
            asset VARCHAR,
            market_open_usd DECIMAL(38,18),
            market_high_usd DECIMAL(38,18),
            market_low_usd DECIMAL(38,18),
            market_close_usd DECIMAL(38,18),
            market_volume_btc DECIMAL(38,18),
            market_observed_hour_count INTEGER,
            is_market_day_complete BOOLEAN,
            transaction_count BIGINT,
            active_addresses_count BIGINT,
            tx_per_active_address DOUBLE
        );
    """)

    con.execute("""
        INSERT INTO mart_btc_market_and_network_daily VALUES
        ('2026-09-18 00:00:00+00', 'BTC', 86000.0, 89000.0, 85000.0, 88500.0,
         950.0, 24, true, 360000, 910000, 0.3956),
        ('2026-09-17 00:00:00+00', 'BTC', 84000.0, 86500.0, 83500.0, 86000.0,
         910.0, 24, true, 340000, 890000, 0.3820);
    """)

    con.execute("""
        CREATE TABLE mart_btc_usd_daily (
            source VARCHAR,
            product_id VARCHAR,
            trade_date_utc TIMESTAMPTZ,
            open DECIMAL(38,18),
            high DECIMAL(38,18),
            low DECIMAL(38,18),
            close DECIMAL(38,18),
            volume_base DECIMAL(38,18),
            observed_hour_count INTEGER,
            is_complete BOOLEAN
        );
    """)

    con.execute("""
        INSERT INTO mart_btc_usd_daily VALUES
        ('coinbase', 'BTC-USD', '2026-09-17 00:00:00+00', 84000.0, 86500.0, 83500.0,
         86000.0, 910.0, 24, true),
        ('coinbase', 'BTC-USD', '2026-09-18 00:00:00+00', 86000.0, 89000.0, 85000.0,
         88500.0, 950.0, 24, true);
    """)

    con.execute("""
        CREATE TABLE fact_market_candle_hourly (
            source VARCHAR,
            product_id VARCHAR,
            granularity_seconds INTEGER,
            candle_start_utc TIMESTAMPTZ,
            open DECIMAL(38,18),
            high DECIMAL(38,18),
            low DECIMAL(38,18),
            close DECIMAL(38,18),
            volume_base DECIMAL(38,18),
            ingested_at_utc TIMESTAMPTZ,
            source_run_id VARCHAR,
            year INTEGER
        );
    """)

    con.execute("""
        INSERT INTO fact_market_candle_hourly VALUES
        ('coinbase', 'BTC-USD', 3600, '2026-09-18 10:00:00+00', 88000.0, 88300.0,
         87900.0, 88200.0, 42.5, now(), 'run1', 2026),
        ('coinbase', 'BTC-USD', 3600, '2026-09-18 11:00:00+00', 88200.0, 88600.0,
         88100.0, 88500.0, 55.0, now(), 'run1', 2026);
    """)

    con.close()
    return db_file


def _http_get(url: str) -> tuple[int, dict[str, str], bytes]:
    """Helper to perform HTTP GET using stdlib urllib."""
    req = urllib.request.Request(url, headers={"User-Agent": "TestClient/1.0"})
    try:
        with urllib.request.urlopen(req) as resp:
            status = resp.status
            headers = {k.lower(): v for k, v in resp.getheaders()}
            content = resp.read()
            return status, headers, content
    except urllib.error.HTTPError as exc:
        headers = {k.lower(): v for k, v in exc.headers.items()}
        return exc.code, headers, exc.read()


def test_create_dashboard_server_initialization(tmp_path: Path) -> None:
    """Test server initialization and configuration."""
    db_path = tmp_path / "custom.duckdb"
    server = create_dashboard_server("127.0.0.1", 0, db_path=db_path)
    try:
        assert server.server_address[0] == "127.0.0.1"
        assert server.server_port > 0
        assert server.db_path == db_path
        assert server.assets_dir.is_dir()
        assert (server.assets_dir / "index.html").is_file()
    finally:
        server.server_close()


def test_get_root_dashboard_html(test_server: tuple[DashboardServer, str]) -> None:
    """Test GET / and /index.html return 200 with approved dashboard HTML."""
    _, base_url = test_server

    for path in ("/", "/index.html"):
        status, headers, content = _http_get(f"{base_url}{path}")
        assert status == 200
        assert "text/html" in headers.get("content-type", "")
        html = content.decode("utf-8")
        assert "Bitcoin Market Hub" in html
        assert "Pasar Buka (24/7)" in html
        assert "Harga Saat Ini (Spot)" in html
        assert "Aktivitas Jaringan Blockchain" in html

    # Verify modular static assets are accessible with correct MIME types
    status_css, headers_css, content_css = _http_get(f"{base_url}/assets/css/styles.css")
    assert status_css == 200
    assert "text/css" in headers_css.get("content-type", "")
    assert ".live-dot" in content_css.decode("utf-8")

    status_js, headers_js, content_js = _http_get(f"{base_url}/assets/js/app.js")
    assert status_js == 200
    assert "application/javascript" in headers_js.get("content-type", "")
    assert "switchMainTab" in content_js.decode("utf-8")


def test_get_kpi_metrics_schema_and_defaults(test_server: tuple[DashboardServer, str]) -> None:
    """Test GET /api/kpi for BTC and ETH metrics and fallbacks."""
    _, base_url = test_server

    with patch(
        "bitcoin_data_platform.dashboard.server._fetch_live_spot_price",
        side_effect=lambda asset: 78191.0 if asset == "BTC" else 2514.0,
    ):
        # 1. BTC with live price
        status, headers, content = _http_get(f"{base_url}/api/kpi?asset=BTC")
        assert status == 200
        assert "application/json" in headers.get("content-type", "")
        data = json.loads(content.decode("utf-8"))
        assert data["asset"] == "BTC"
        assert data["name"] == "Bitcoin"
        assert data["spot_price"] == 78191.0
        assert isinstance(data["high_24h"], int | float)
        assert isinstance(data["low_24h"], int | float)
        assert isinstance(data["volume_usd"], int | float)
        assert isinstance(data["volume_asset"], int | float)
        assert isinstance(data["tx_count"], int)
        assert isinstance(data["active_addrs"], int)

        # 2. ETH with live price
        status, _, content = _http_get(f"{base_url}/api/kpi?asset=ETH")
        assert status == 200
        eth_data = json.loads(content.decode("utf-8"))
        assert eth_data["asset"] == "ETH"
        assert eth_data["name"] == "Ethereum"
        assert eth_data["spot_price"] == 2514.0


def test_get_chart_series(test_server: tuple[DashboardServer, str]) -> None:
    """Test GET /api/chart returns valid schema and series array."""
    _, base_url = test_server

    # 30D timeframe
    status, headers, content = _http_get(f"{base_url}/api/chart?asset=BTC&range=30D")
    assert status == 200
    assert "application/json" in headers.get("content-type", "")
    chart_data = json.loads(content.decode("utf-8"))
    assert chart_data["asset"] == "BTC"
    assert chart_data["range"] == "30D"
    assert isinstance(chart_data["series"], list)
    assert len(chart_data["series"]) > 0

    first_item = chart_data["series"][0]
    assert "date" in first_item
    assert "price" in first_item
    assert "volume" in first_item

    # 24H timeframe
    status, _, content = _http_get(f"{base_url}/api/chart?asset=BTC&range=24H")
    assert status == 200
    c24 = json.loads(content.decode("utf-8"))
    assert c24["range"] == "24H"
    assert len(c24["series"]) > 0


def test_get_trades(test_server: tuple[DashboardServer, str]) -> None:
    """Test GET /api/trades returns recent trades."""
    _, base_url = test_server

    status, headers, content = _http_get(f"{base_url}/api/trades?asset=BTC")
    assert status == 200
    assert "application/json" in headers.get("content-type", "")
    data = json.loads(content.decode("utf-8"))
    assert data["asset"] == "BTC"
    assert isinstance(data["trades"], list)
    assert len(data["trades"]) >= 1

    trade = data["trades"][0]
    assert "time" in trade
    assert trade["side"] in ("BUY", "SELL")
    assert isinstance(trade["price"], int | float)
    assert isinstance(trade["size"], int | float)


def test_get_ledger_rows(test_server: tuple[DashboardServer, str]) -> None:
    """Test GET /api/ledger returns daily ledger rows."""
    _, base_url = test_server

    status, headers, content = _http_get(f"{base_url}/api/ledger?asset=BTC&limit=15")
    assert status == 200
    assert "application/json" in headers.get("content-type", "")
    data = json.loads(content.decode("utf-8"))
    assert data["asset"] == "BTC"
    assert data["total_rows"] == 15
    assert len(data["rows"]) == 15

    row = data["rows"][0]
    for key in (
        "date",
        "trade_date_utc",
        "open",
        "high",
        "low",
        "close",
        "change",
        "volume",
        "tx",
        "active_addresses",
    ):
        assert key in row


def test_get_export_csv(test_server: tuple[DashboardServer, str]) -> None:
    """Test GET /api/export returns valid CSV with Content-Disposition."""
    _, base_url = test_server

    status, headers, content = _http_get(f"{base_url}/api/export?format=csv&asset=BTC")
    assert status == 200
    assert "text/csv" in headers.get("content-type", "")
    disp = headers.get("content-disposition", "")
    assert "attachment" in disp
    assert 'filename="btc_market_history_' in disp
    assert disp.endswith('.csv"')

    text = content.decode("utf-8")
    reader = list(csv.reader(text.strip().split("\n")))
    assert len(reader) > 5

    header = reader[0]
    assert header == [
        "Tanggal",
        "Aset",
        "Harga_Buka_USD",
        "Tertinggi_USD",
        "Terendah_USD",
        "Harga_Tutup_USD",
        "Perubahan_Persen",
        "Volume_Koin",
        "Total_Transaksi",
    ]

    first_data_row = reader[1]
    assert first_data_row[1] == "BTC"
    assert float(first_data_row[2]) > 0.0


def test_duckdb_integration_querying(populated_duckdb: Path) -> None:
    """Test server dynamically loads live metrics from DuckDB marts."""
    server = create_dashboard_server(
        host="127.0.0.1",
        port=0,
        db_path=populated_duckdb,
    )
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        # Test KPI fallback to DuckDB when live API is unavailable
        with patch(
            "bitcoin_data_platform.dashboard.server._fetch_live_spot_price", return_value=None
        ):
            status, _, content = _http_get(f"{base_url}/api/kpi?asset=BTC")
            assert status == 200
            kpi = json.loads(content.decode("utf-8"))
            assert kpi["spot_price"] == 88500.0
            assert kpi["high_24h"] == 89000.0
            assert kpi["low_24h"] == 85000.0
            assert kpi["volume_asset"] == 950.0
            assert kpi["tx_count"] == 360000
            assert kpi["active_addrs"] == 910000

        # Test KPI with live price active: spot_price is live, historical metrics come from DuckDB
        with patch(
            "bitcoin_data_platform.dashboard.server._fetch_live_spot_price",
            return_value=95000.50,
        ):
            status, _, content = _http_get(f"{base_url}/api/kpi?asset=BTC")
            assert status == 200
            kpi = json.loads(content.decode("utf-8"))
            assert kpi["spot_price"] == 95000.50
            assert kpi["high_24h"] == 89000.0
            assert kpi["low_24h"] == 85000.0
            assert kpi["volume_asset"] == 950.0
            assert kpi["tx_count"] == 360000
            assert kpi["active_addrs"] == 910000

        # Test Ledger from DuckDB
        status, _, content = _http_get(f"{base_url}/api/ledger?asset=BTC&limit=10")
        assert status == 200
        ledger = json.loads(content.decode("utf-8"))
        assert ledger["total_rows"] == 2
        assert ledger["rows"][0]["close"] == 88500.0
        assert ledger["rows"][1]["close"] == 86000.0

        # Test 24H chart from fact_market_candle_hourly
        status, _, content = _http_get(f"{base_url}/api/chart?asset=BTC&range=24H")
        assert status == 200
        chart_24h = json.loads(content.decode("utf-8"))
        assert len(chart_24h["series"]) == 2
        assert chart_24h["series"][0]["close"] == 88200.0
        assert chart_24h["series"][1]["close"] == 88500.0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def test_favicon_and_404_routing(test_server: tuple[DashboardServer, str]) -> None:
    """Test favicon returns 204 and missing paths return 404."""
    _, base_url = test_server

    status, _, _ = _http_get(f"{base_url}/favicon.ico")
    assert status == 204

    status, _, content = _http_get(f"{base_url}/nonexistent/endpoint")
    assert status == 404
    err_json = json.loads(content.decode("utf-8"))
    assert err_json["error"] == "not_found"


def test_cli_dashboard_subcommand_help(capsys: pytest.CaptureFixture[str]) -> None:
    """Test CLI subcommand 'bitcoin-data dashboard --help'."""
    exit_code = main(["dashboard", "--help"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "--host" in captured.out
    assert "--port" in captured.out
    assert "--db-path" in captured.out


def test_cli_dashboard_dispatch(tmp_path: Path) -> None:
    """Test CLI dispatch to run_dashboard."""
    db_file = tmp_path / "dummy.duckdb"
    with patch("bitcoin_data_platform.dashboard.server.run_dashboard") as mock_run:
        cmd = ["dashboard", "--host", "0.0.0.0", "--port", "9090", "--db-path", str(db_file)]
        exit_code = main(cmd)
        assert exit_code == 0
        mock_run.assert_called_once_with(
            host="0.0.0.0",
            port=9090,
            db_path=str(db_file),
        )


def test_cli_dashboard_keyboard_interrupt(capsys: pytest.CaptureFixture[str]) -> None:
    """Test CLI handling of KeyboardInterrupt when user cancels."""
    with patch(
        "bitcoin_data_platform.dashboard.server.run_dashboard", side_effect=KeyboardInterrupt
    ):
        exit_code = main(["dashboard"])
        assert exit_code == 0
    err = capsys.readouterr().err
    assert "Dashboard server stopped by user" in err


def test_cli_dashboard_exception_handling(capsys: pytest.CaptureFixture[str]) -> None:
    """Test CLI handling of server unexpected runtime errors."""
    with patch(
        "bitcoin_data_platform.dashboard.server.run_dashboard",
        side_effect=RuntimeError("Bind port failed"),
    ):
        exit_code = main(["dashboard"])
        assert exit_code == 1
    err = capsys.readouterr().err
    assert "Bind port failed" in err


def test_dashboard_invalid_params_fallback(test_server: tuple[DashboardServer, str]) -> None:
    """Test graceful handling of unknown assets and invalid ranges."""
    _, base_url = test_server

    # Unknown asset defaults to BTC
    with patch(
        "bitcoin_data_platform.dashboard.server._fetch_live_spot_price",
        return_value=78191.0,
    ):
        status, _, content = _http_get(f"{base_url}/api/kpi?asset=XYZ")
        assert status == 200
        data = json.loads(content.decode("utf-8"))
        assert data["asset"] == "BTC"

    # Invalid range defaults to 30D
    status, _, content = _http_get(f"{base_url}/api/chart?asset=BTC&range=999D")
    assert status == 200
    chart_data = json.loads(content.decode("utf-8"))
    assert chart_data["range"] == "30D"

    # Invalid limit defaults to 30
    status, _, content = _http_get(f"{base_url}/api/ledger?asset=BTC&limit=-5")
    assert status == 200
    ledger_data = json.loads(content.decode("utf-8"))
    assert ledger_data["total_rows"] == 30


def test_dashboard_missing_template_asset(tmp_path: Path) -> None:
    """Test 404 response when index.html template is absent."""
    empty_assets = tmp_path / "empty_assets"
    empty_assets.mkdir()
    server = create_dashboard_server("127.0.0.1", 0, assets_dir=empty_assets)
    port = server.server_port
    base_url = f"http://127.0.0.1:{port}"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        status, _, content = _http_get(f"{base_url}/")
        assert status == 404
        data = json.loads(content.decode("utf-8"))
        assert data["error"] == "not_found"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def _mock_coinbase_response(amount: str, base: str = "BTC") -> MagicMock:
    """Helper to mock a successful Coinbase spot price HTTP response."""
    payload = json.dumps(
        {
            "data": {
                "amount": amount,
                "base": base,
                "currency": "USD",
            }
        }
    ).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.read.return_value = payload
    mock_resp.__enter__.return_value = mock_resp
    return mock_resp


def test_fetch_live_spot_price_btc_success() -> None:
    """Test successful live spot price fetch for BTC from Coinbase API."""
    _clear_price_cache()
    mock_resp = _mock_coinbase_response("78191.095", base="BTC")
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        return_value=mock_resp,
    ) as mock_open:
        price = _fetch_live_spot_price("BTC")
        assert price == 78191.10
        assert mock_open.call_count == 1

        call_args, call_kwargs = mock_open.call_args
        req = call_args[0]
        assert isinstance(req, urllib.request.Request)
        assert req.full_url == "https://api.coinbase.com/v2/prices/BTC-USD/spot"
        assert req.headers.get("User-agent") == "BitcoinDataPlatformDashboard/1.0"
        assert call_kwargs.get("timeout") == 3.0


def test_fetch_live_spot_price_eth_success() -> None:
    """Test successful live spot price fetch for ETH from Coinbase API."""
    _clear_price_cache()
    mock_resp = _mock_coinbase_response("2514.675", base="ETH")
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        return_value=mock_resp,
    ) as mock_open:
        price = _fetch_live_spot_price("ETH")
        assert price == 2514.68
        assert mock_open.call_count == 1

        req = mock_open.call_args[0][0]
        assert req.full_url == "https://api.coinbase.com/v2/prices/ETH-USD/spot"


def test_fetch_live_spot_price_caching() -> None:
    """Test price caching behavior: subsequent calls within 30s reuse cached price."""
    _clear_price_cache()
    mock_resp = _mock_coinbase_response("78191.00", base="BTC")
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        return_value=mock_resp,
    ) as mock_open:
        p1 = _fetch_live_spot_price("BTC")
        assert p1 == 78191.00
        assert mock_open.call_count == 1

        p2 = _fetch_live_spot_price("BTC")
        assert p2 == 78191.00
        assert mock_open.call_count == 1


def test_fetch_live_spot_price_cache_expiry() -> None:
    """Test cache expires after 30 seconds and triggers a new API request."""
    _clear_price_cache()
    mock_resp1 = _mock_coinbase_response("78191.00", base="BTC")
    mock_resp2 = _mock_coinbase_response("78250.50", base="BTC")
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        side_effect=[mock_resp1, mock_resp2],
    ) as mock_open:
        p1 = _fetch_live_spot_price("BTC")
        assert p1 == 78191.00
        assert mock_open.call_count == 1

        with _price_cache_lock:
            _price_cache["BTC"] = (78191.00, time.monotonic() - 31.0)

        p2 = _fetch_live_spot_price("BTC")
        assert p2 == 78250.50
        assert mock_open.call_count == 2


def test_fetch_live_spot_price_network_error_fallback() -> None:
    """Test graceful fallback returning None when URLError occurs."""
    _clear_price_cache()
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        side_effect=urllib.error.URLError("Network unreachable"),
    ):
        price = _fetch_live_spot_price("BTC")
        assert price is None


def test_fetch_live_spot_price_http_error_fallback() -> None:
    """Test graceful fallback returning None when HTTPError occurs."""
    _clear_price_cache()
    err = urllib.error.HTTPError(
        url="https://api.coinbase.com/v2/prices/BTC-USD/spot",
        code=503,
        msg="Service Unavailable",
        hdrs=None,  # type: ignore[arg-type]
        fp=None,
    )
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        side_effect=err,
    ):
        price = _fetch_live_spot_price("BTC")
        assert price is None


def test_fetch_live_spot_price_timeout_fallback() -> None:
    """Test graceful fallback returning None when connection times out."""
    _clear_price_cache()
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        side_effect=TimeoutError("Request timed out"),
    ):
        price = _fetch_live_spot_price("BTC")
        assert price is None


def test_fetch_live_spot_price_malformed_json_fallback() -> None:
    """Test graceful fallback returning None on invalid JSON response."""
    _clear_price_cache()
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b"<!DOCTYPE html><html>Service Unavailable</html>"
    mock_resp.__enter__.return_value = mock_resp
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        return_value=mock_resp,
    ):
        price = _fetch_live_spot_price("BTC")
        assert price is None


def test_fetch_live_spot_price_missing_amount_fallback() -> None:
    """Test graceful fallback returning None when amount key is absent."""
    _clear_price_cache()
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b'{"data": {"base": "BTC", "currency": "USD"}}'
    mock_resp.__enter__.return_value = mock_resp
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        return_value=mock_resp,
    ):
        price = _fetch_live_spot_price("BTC")
        assert price is None


def test_fetch_live_spot_price_non_200_status() -> None:
    """Test response with non-200 status code returns None."""
    _clear_price_cache()
    mock_resp = MagicMock()
    mock_resp.status = 502
    mock_resp.read.return_value = b'{"error": "bad gateway"}'
    mock_resp.__enter__.return_value = mock_resp
    with patch(
        "bitcoin_data_platform.dashboard.server.urllib.request.urlopen",
        return_value=mock_resp,
    ):
        price = _fetch_live_spot_price("BTC")
        assert price is None
