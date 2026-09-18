"""Tests for Phase 13 CLI subcommands: generate-signal, news-sentinel, send-alert."""

import json
from pathlib import Path

import httpx
import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.signals.news_sentinel import NewsSentinel
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def _setup_test_db(db_path: Path) -> DuckDBManager:
    db_mgr = DuckDBManager(db_path=db_path)
    con = db_mgr.get_connection()
    con.execute(
        """
        CREATE OR REPLACE TABLE mart_btc_investment_signals_daily (
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
        """
    )
    con.execute(
        """
        INSERT INTO mart_btc_investment_signals_daily VALUES
        ('2026-09-18', 65000.0, 55000.0, 1.18, 1.45, 48, 'Neutral', FALSE, 'STANDARD_DCA');
        """
    )
    db_mgr.create_signal_history_table()
    db_mgr.create_news_sentinel_alerts_table()
    return db_mgr


def test_cli_generate_signal_human(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """AC-1: generate-signal produces readable output with correct signal classification."""
    db_path = tmp_path / "test.duckdb"
    _setup_test_db(db_path)

    exit_code = main(["generate-signal", "--db-path", str(db_path)])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "Bitcoin Investment Signal: 2026-09-18" in captured.out
    assert "Close: $65,000.00" in captured.out
    assert "Signal: STANDARD_DCA" in captured.out
    assert "DCA STANDAR" in captured.out


def test_cli_generate_signal_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """generate-signal --json outputs valid JSON structure."""
    db_path = tmp_path / "test.duckdb"
    _setup_test_db(db_path)

    exit_code = main(["generate-signal", "--db-path", str(db_path), "--json"])
    assert exit_code == 0

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["signal_date_utc"] == "2026-09-18"
    assert data["investment_signal"] == "STANDARD_DCA"
    assert data["market_close_usd"] == 65000.0
    assert data["signal_strength"] in ("STRONG", "MODERATE", "WEAK")


def test_cli_generate_signal_save(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """AC-5: generate-signal --save persists signal to signal_history table."""
    db_path = tmp_path / "test.duckdb"
    db_mgr = _setup_test_db(db_path)

    exit_code = main(["generate-signal", "--db-path", str(db_path), "--save"])
    assert exit_code == 0

    con = db_mgr.get_connection()
    row = con.execute(
        "SELECT signal_date_utc, market_close_usd, investment_signal "
        "FROM signal_history WHERE signal_date_utc = '2026-09-18'"
    ).fetchone()
    assert row is not None
    assert row[2] == "STANDARD_DCA"


def test_cli_generate_signal_specific_date(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """generate-signal --date retrieves specific date."""
    db_path = tmp_path / "test.duckdb"
    _setup_test_db(db_path)

    exit_code = main(["generate-signal", "--db-path", str(db_path), "--date", "2026-09-18"])
    assert exit_code == 0

    # Date not in DB returns 2
    exit_code_missing = main(["generate-signal", "--db-path", str(db_path), "--date", "2026-01-01"])
    assert exit_code_missing == 2


def test_cli_news_sentinel(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """AC-2 & AC-6: news-sentinel scans RSS, detects matches, and persists alerts."""
    db_path = tmp_path / "test.duckdb"
    db_mgr = _setup_test_db(db_path)

    rss_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0"><channel><title>CoinDesk</title>\n'
        "<item>\n"
        "  <title>Security Alert: DeFi Protocol Hacked for $80M</title>\n"
        "  <link>https://www.coindesk.com/defi-hack</link>\n"
        "  <pubDate>Fri, 18 Sep 2026 12:00:00 GMT</pubDate>\n"
        "  <description>Major exploit detected in lending contract.</description>\n"
        "</item>\n"
        "</channel></rss>"
    )

    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=rss_xml))
    sentinel = NewsSentinel(transport=transport, db_manager=db_mgr)

    exit_code = main(
        ["news-sentinel", "--db-path", str(db_path)],
        news_sentinel=sentinel,
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "CRITICAL" in captured.out
    assert "DeFi Protocol Hacked" in captured.out

    # Verify AC-6: alert stored in news_sentinel_alerts with telegram_sent=False
    con = db_mgr.get_connection()
    row = con.execute("SELECT severity, telegram_sent FROM news_sentinel_alerts").fetchone()
    assert row is not None
    assert row[0] == "CRITICAL"
    assert row[1] is False


def test_cli_send_alert_signal_dry_run(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """AC-3: send-alert --type signal --dry-run prints formatted Telegram message."""
    db_path = tmp_path / "test.duckdb"
    _setup_test_db(db_path)

    exit_code = main(["send-alert", "--db-path", str(db_path), "--type", "signal", "--dry-run"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "SINYAL INVESTASI BITCOIN" in captured.out
    assert "DCA STANDAR" in captured.out
    assert "Harga: $65,000.00" in captured.out


def test_cli_send_alert_news_dry_run(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """AC-4: send-alert --type news --dry-run prints formatted alert message."""
    db_path = tmp_path / "test.duckdb"
    db_mgr = _setup_test_db(db_path)

    # Insert an alert into news_sentinel_alerts
    con = db_mgr.get_connection()
    con.execute(
        """
        INSERT INTO news_sentinel_alerts VALUES (
            'alert-1', 'Breaking: SEC Sue Leading Crypto Exchange',
            'https://www.coindesk.com/sec-sue', '2026-09-18 10:00:00+00',
            'SEC sue/charge', 'CRITICAL', '2026-09-18 10:05:00+00', FALSE
        );
        """
    )

    exit_code = main(["send-alert", "--db-path", str(db_path), "--type", "news", "--dry-run"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "ALERT CRITICAL: Breaking: SEC Sue Leading Crypto Exchange" in captured.out
    assert "Keywords: SEC sue/charge" in captured.out
    assert "Review sebelum mengambil keputusan investasi." in captured.out
