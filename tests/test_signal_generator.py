"""Tests for SignalGenerator and investment signal engine."""

from datetime import UTC, date, datetime
from pathlib import Path

from bitcoin_data_platform.signals.generator import (
    SignalGenerator,
    compute_signal_strength,
    generate_narrative,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def _setup_test_db(db_path: Path, rows: list[tuple]) -> DuckDBManager:
    """Create a test DuckDB with mart_btc_investment_signals_daily populated."""
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
    con.executemany(
        """
        INSERT INTO mart_btc_investment_signals_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )
    db_mgr.create_signal_history_table()
    return db_mgr


def test_generate_signal_aggressive_accumulate(tmp_path: Path) -> None:
    """1. test_generate_signal_aggressive_accumulate: low Mayer + low MVRV + extreme fear."""
    db_path = tmp_path / "test.duckdb"
    # Mayer 0.70 (<0.80), MVRV 0.85 (<1.00), FNG 15 (<=25) -> AGGRESSIVE_ACCUMULATE, STRONG
    rows = [
        (
            date(2026, 9, 18),
            50000.0,
            71428.5,
            0.70,
            0.85,
            15,
            "Extreme Fear",
            False,
            "AGGRESSIVE_ACCUMULATE",
        )
    ]
    db_mgr = _setup_test_db(db_path, rows)
    generator = SignalGenerator(db_path=db_path, db_manager=db_mgr)

    signal = generator.generate_latest()
    assert signal.signal_date_utc == date(2026, 9, 18)
    assert signal.investment_signal == "AGGRESSIVE_ACCUMULATE"
    assert signal.signal_strength == "STRONG"
    assert "AKUMULASI AGRESIF" in signal.narrative
    assert signal.market_close_usd == 50000.0
    assert signal.fng_value == 15


def test_generate_signal_opportunistic_accumulate(tmp_path: Path) -> None:
    """2. test_generate_signal_opportunistic_accumulate: below-average valuation."""
    db_path = tmp_path / "test.duckdb"
    # Mayer 0.90 (<1.00), MVRV 1.10 (<1.20), FNG 35 (<=45) -> OPPORTUNISTIC_ACCUMULATE, STRONG
    rows = [
        (
            date(2026, 9, 18),
            58000.0,
            64444.4,
            0.90,
            1.10,
            35,
            "Fear",
            False,
            "OPPORTUNISTIC_ACCUMULATE",
        )
    ]
    db_mgr = _setup_test_db(db_path, rows)
    generator = SignalGenerator(db_path=db_path, db_manager=db_mgr)

    signal = generator.generate_latest()
    assert signal.investment_signal == "OPPORTUNISTIC_ACCUMULATE"
    assert signal.signal_strength == "STRONG"
    assert "AKUMULASI OPORTUNISTIK" in signal.narrative


def test_generate_signal_standard_dca(tmp_path: Path) -> None:
    """3. test_generate_signal_standard_dca: normal market conditions."""
    db_path = tmp_path / "test.duckdb"
    # Mayer 1.20 (1.00..1.80), MVRV 1.60, FNG 50 -> STANDARD_DCA, STRONG
    rows = [
        (
            date(2026, 9, 18),
            65000.0,
            54166.6,
            1.20,
            1.60,
            50,
            "Neutral",
            False,
            "STANDARD_DCA",
        )
    ]
    db_mgr = _setup_test_db(db_path, rows)
    generator = SignalGenerator(db_path=db_path, db_manager=db_mgr)

    signal = generator.generate_latest()
    assert signal.investment_signal == "STANDARD_DCA"
    assert signal.signal_strength == "STRONG"
    assert "DCA STANDAR" in signal.narrative


def test_generate_signal_defensive_reserve(tmp_path: Path) -> None:
    """4. test_generate_signal_defensive_reserve: overheated market."""
    db_path = tmp_path / "test.duckdb"
    # Mayer 1.95 (>1.80), MVRV 2.60 (>2.50), FNG 80 (>=75) -> DEFENSIVE_RESERVE, STRONG
    rows = [
        (
            date(2026, 9, 18),
            95000.0,
            48717.9,
            1.95,
            2.60,
            80,
            "Extreme Greed",
            True,
            "DEFENSIVE_RESERVE",
        )
    ]
    db_mgr = _setup_test_db(db_path, rows)
    generator = SignalGenerator(db_path=db_path, db_manager=db_mgr)

    signal = generator.generate_latest()
    assert signal.investment_signal == "DEFENSIVE_RESERVE"
    assert signal.signal_strength == "STRONG"
    assert "CADANGAN DEFENSIF" in signal.narrative
    assert signal.has_high_impact_macro_event is True


def test_generate_signal_hard_freeze(tmp_path: Path) -> None:
    """5. test_generate_signal_hard_freeze: bubble territory."""
    db_path = tmp_path / "test.duckdb"
    # Mayer 2.60 (>2.40), MVRV 3.80 (>3.50), FNG 92 (>=80) -> HARD_FREEZE, STRONG
    rows = [
        (
            date(2026, 9, 18),
            140000.0,
            53846.1,
            2.60,
            3.80,
            92,
            "Extreme Greed",
            False,
            "HARD_FREEZE",
        )
    ]
    db_mgr = _setup_test_db(db_path, rows)
    generator = SignalGenerator(db_path=db_path, db_manager=db_mgr)

    signal = generator.generate_latest()
    assert signal.investment_signal == "HARD_FREEZE"
    assert signal.signal_strength == "STRONG"
    assert "STOP TOTAL" in signal.narrative


def test_signal_strength_strong() -> None:
    """6. test_signal_strength_strong: 3+ indicators align."""
    # AGGRESSIVE_ACCUMULATE: mayer < 0.8, mvrv < 1.0, fng <= 25 -> 3 indicators -> STRONG
    strength = compute_signal_strength("AGGRESSIVE_ACCUMULATE", 0.75, 0.90, 20)
    assert strength == "STRONG"

    # OPPORTUNISTIC_ACCUMULATE: mayer < 1.0, mvrv < 1.2, fng <= 45 -> 3 indicators -> STRONG
    strength_opp = compute_signal_strength("OPPORTUNISTIC_ACCUMULATE", 0.95, 1.15, 30)
    assert strength_opp == "STRONG"

    # HARD_FREEZE: mayer > 2.4, mvrv > 3.5, fng >= 80 -> 3 indicators -> STRONG
    strength_hf = compute_signal_strength("HARD_FREEZE", 2.50, 3.60, 85)
    assert strength_hf == "STRONG"


def test_signal_strength_weak() -> None:
    """7. test_signal_strength_weak: single indicator trigger."""
    # AGGRESSIVE_ACCUMULATE: only mayer < 0.8 (0.75), but mvrv=1.3 and fng=50 -> 1 indicator -> WEAK
    strength = compute_signal_strength("AGGRESSIVE_ACCUMULATE", 0.75, 1.30, 50)
    assert strength == "WEAK"

    # DEFENSIVE_RESERVE: only mayer > 1.8 (1.90), but mvrv=1.8 and fng=60 -> 1 indicator -> WEAK
    strength_dr = compute_signal_strength("DEFENSIVE_RESERVE", 1.90, 1.80, 60)
    assert strength_dr == "WEAK"


def test_signal_narrative_contains_metrics() -> None:
    """8. test_signal_narrative_contains_metrics: narrative includes actual metric values."""
    narrative_agg = generate_narrative("AGGRESSIVE_ACCUMULATE", 0.72, 0.95, 18)
    assert "0.72" in narrative_agg
    assert "0.95" in narrative_agg
    assert "18" in narrative_agg

    narrative_opp = generate_narrative("OPPORTUNISTIC_ACCUMULATE", 0.88, 1.10, 32)
    assert "0.88" in narrative_opp

    narrative_def = generate_narrative("DEFENSIVE_RESERVE", 1.92, 2.70, 88)
    assert "1.92" in narrative_def
    assert "88" in narrative_def

    narrative_hf = generate_narrative("HARD_FREEZE", 2.55, 3.90, 95)
    assert "2.55" in narrative_hf
    assert "3.90" in narrative_hf


def test_signal_persists_to_history(tmp_path: Path) -> None:
    """9. test_signal_persists_to_history: signal saved to signal_history table."""
    db_path = tmp_path / "test.duckdb"
    rows = [
        (
            date(2026, 9, 18),
            60000.0,
            50000.0,
            1.20,
            1.50,
            55,
            "Greed",
            False,
            "STANDARD_DCA",
        )
    ]
    db_mgr = _setup_test_db(db_path, rows)
    generator = SignalGenerator(db_path=db_path, db_manager=db_mgr)

    signal = generator.generate_latest()
    generator.save_signal(signal)

    # Verify query on signal_history
    con = db_mgr.get_connection()
    history_row = con.execute(
        "SELECT signal_date_utc, market_close_usd, investment_signal, signal_strength, narrative "
        "FROM signal_history WHERE signal_date_utc = ?;",
        [date(2026, 9, 18)],
    ).fetchone()

    assert history_row is not None
    assert history_row[0] == date(2026, 9, 18)
    assert history_row[1] == 60000.0
    assert history_row[2] == "STANDARD_DCA"
    assert history_row[3] == "STRONG"
    assert "DCA STANDAR" in history_row[4]


def test_signal_idempotent(tmp_path: Path) -> None:
    """10. test_signal_idempotent: same date always produces same signal."""
    db_path = tmp_path / "test.duckdb"
    target_date = date(2026, 9, 18)
    fixed_time = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)
    rows = [
        (
            target_date,
            55000.0,
            60000.0,
            0.9167,
            1.05,
            30,
            "Fear",
            False,
            "OPPORTUNISTIC_ACCUMULATE",
        )
    ]
    db_mgr = _setup_test_db(db_path, rows)
    generator = SignalGenerator(db_path=db_path, clock=lambda: fixed_time, db_manager=db_mgr)

    sig1 = generator.generate_for_date(target_date)
    sig2 = generator.generate_for_date(target_date)

    assert sig1 is not None and sig2 is not None
    assert sig1.signal_date_utc == sig2.signal_date_utc
    assert sig1.generated_at_utc == sig2.generated_at_utc
    assert sig1.market_close_usd == sig2.market_close_usd
    assert sig1.sma_200 == sig2.sma_200
    assert sig1.mayer_multiple == sig2.mayer_multiple
    assert sig1.mvrv_ratio == sig2.mvrv_ratio
    assert sig1.fng_value == sig2.fng_value
    assert sig1.fng_classification == sig2.fng_classification
    assert sig1.has_high_impact_macro_event == sig2.has_high_impact_macro_event
    assert sig1.investment_signal == sig2.investment_signal
    assert sig1.signal_strength == sig2.signal_strength
    assert sig1.narrative == sig2.narrative
    assert sig1 == sig2
