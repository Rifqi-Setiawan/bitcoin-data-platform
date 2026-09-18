"""Integration tests for paper trading CLI commands."""

import json
from pathlib import Path

import duckdb
import pytest

from bitcoin_data_platform.cli import main


@pytest.fixture
def cli_db(tmp_path: Path) -> Path:
    """Create a temporary DuckDB database with mock signals."""
    db_file = tmp_path / "cli_platform.duckdb"
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
        ('2026-09-18 00:00:00+00', 85000.0, 72000.0, 1.18, 1.8, 55, 'Neutral',
         false, 'STANDARD_DCA');
        """
    )
    con.close()
    return db_file


def test_cli_paper_init_success(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["paper", "init", "--initial-cash", "1000.0", "--db-path", str(cli_db)])
    assert code == 0
    out = capsys.readouterr().out
    assert "Initialized forward paper portfolio 'default' with $1,000.00 USD" in out
    assert "$700.00 Base Cash" in out
    assert "$300.00 Tactical Reserve" in out


def test_cli_paper_init_invalid_capital(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["paper", "init", "--initial-cash", "-100.0", "--db-path", str(cli_db)])
    assert code == 2
    err = capsys.readouterr().err
    assert "--initial-cash must be > 0" in err


def test_cli_paper_step_success(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["paper", "init", "--db-path", str(cli_db)])
    capsys.readouterr()  # Clear stdout

    code = main(
        [
            "paper",
            "step",
            "--date",
            "2026-09-18",
            "--daily-budget",
            "15.0",
            "--db-path",
            str(cli_db),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "[2026-09-18]" in out
    assert "Side: BUY" in out
    assert "Gross: $15.00" in out
    assert "Portfolio Equity:" in out


def test_cli_paper_step_force_spot_price(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["paper", "init", "--db-path", str(cli_db)])
    capsys.readouterr()

    code = main(
        [
            "paper",
            "step",
            "--date",
            "2026-09-20",
            "--daily-budget",
            "10.0",
            "--force-price",
            "90000.0",
            "--db-path",
            str(cli_db),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Spot: $90,000.00" in out


def test_cli_paper_step_invalid_date(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        [
            "paper",
            "step",
            "--date",
            "not-a-date",
            "--db-path",
            str(cli_db),
        ]
    )
    assert code == 2
    err = capsys.readouterr().err
    assert "invalid --date 'not-a-date'" in err


def test_cli_paper_status_table(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["paper", "init", "--db-path", str(cli_db)])
    capsys.readouterr()

    code = main(["paper", "status", "--format", "table", "--db-path", str(cli_db)])
    assert code == 0
    out = capsys.readouterr().out
    assert "FINCEPT FORWARD PAPER TRADING PORTFOLIO SUMMARY" in out
    assert "Total Equity (USD)" in out
    assert "Cash Holdings" in out


def test_cli_paper_status_json(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["paper", "init", "--db-path", str(cli_db)])
    capsys.readouterr()

    code = main(["paper", "status", "--format", "json", "--db-path", str(cli_db)])
    assert code == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["portfolio_id"] == "default"
    assert data["initial_cash"] == 1000.0
    assert "total_equity" in data


def test_cli_paper_reset_requires_force(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["paper", "init", "--db-path", str(cli_db)])
    capsys.readouterr()

    code = main(["paper", "reset", "--db-path", str(cli_db)])
    assert code == 2
    err = capsys.readouterr().err
    assert "resetting paper portfolio requires explicit --force flag" in err


def test_cli_paper_reset_with_force_succeeds(
    cli_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    main(["paper", "init", "--db-path", str(cli_db)])
    capsys.readouterr()

    code = main(["paper", "reset", "--force", "--initial-cash", "1000.0", "--db-path", str(cli_db)])
    assert code == 0
    out = capsys.readouterr().out
    assert "Reset forward paper portfolio 'default' to initial state ($1,000.00 USD" in out


def test_cli_paper_missing_subcommand(cli_db: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["paper"])
    assert code == 2
    err = capsys.readouterr().err
    assert "paper subcommand required" in err
