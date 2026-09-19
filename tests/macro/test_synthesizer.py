"""Unit tests for 3-tier composite Macro-Narrative synthesizer and MNI regimes (Group D)."""

from datetime import UTC, date, datetime
from typing import Any

from bitcoin_data_platform.macro.models import (
    DailyNarrativeReport,
    MacroPillar,
    MacroRegime,
)
from bitcoin_data_platform.macro.synthesizer import (
    MacroNarrativeSynthesizer,
    classify_macro_regime,
    compute_composite_mni,
    generate_bahasa_indonesia_narrative,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def test_composite_mni_weighting_formula() -> None:
    """27. Verifies 0.40 / 0.35 / 0.25 weighting calculation."""
    # S_macro = 1.0, S_sentiment = 0.5, S_narrative = -0.2
    # Expected MNI = 0.40*1.0 + 0.35*0.5 + 0.25*(-0.2) = 0.40 + 0.175 - 0.05 = 0.525
    mni = compute_composite_mni(hard_macro_score=1.0, sentiment_score=0.5, narrative_score=-0.2)
    assert round(mni, 3) == 0.525


def test_regime_classification_risk_on() -> None:
    """28. High positive MNI (>= 0.50) yields RISK_ON_EXPANSION."""
    regime = classify_macro_regime(composite_mni=0.55, black_swan_flag=False)
    assert regime == MacroRegime.RISK_ON_EXPANSION


def test_regime_classification_crisis_flag() -> None:
    """29. black_swan_flag = True forces BLACK_SWAN_CRISIS even with positive MNI."""
    regime = classify_macro_regime(composite_mni=0.70, black_swan_flag=True)
    assert regime == MacroRegime.BLACK_SWAN_CRISIS


def test_regime_classification_neutral() -> None:
    """30. Near-zero values resolve to NEUTRAL_CHOP."""
    regime_zero = classify_macro_regime(composite_mni=0.0, black_swan_flag=False)
    assert regime_zero == MacroRegime.NEUTRAL_CHOP

    regime_mild = classify_macro_regime(composite_mni=0.10, black_swan_flag=False)
    assert regime_mild == MacroRegime.NEUTRAL_CHOP


def test_bahasa_indonesia_narrative_generation() -> None:
    """31. Validates Indonesian commentary output and appropriate emoji markers."""
    narrative = generate_bahasa_indonesia_narrative(
        regime=MacroRegime.CAUTIOUS_BULL,
        composite_mni=0.35,
        hard_macro_score=0.20,
        sentiment_score=0.40,
        narrative_score=0.30,
        dominant_pillar=MacroPillar.INSTITUTIONAL,
    )
    assert "🔵" in narrative
    assert "AKUMULASI OPORTUNISTIK" in narrative
    assert "Arus Masuk Institusional" in narrative or "INSTITUTIONAL" in narrative


def test_synthesizer_missing_macro_fallback() -> None:
    """32. Gracefully falls back when no macro events exist (S_macro = 0.0)."""
    synth = MacroNarrativeSynthesizer()
    report = synth.synthesize(
        hard_macro_score=0.0,
        sentiment_score=0.0,
        narrative_score=0.0,
        black_swan_flag=False,
    )
    assert report.composite_mni == 0.0
    assert report.regime == MacroRegime.NEUTRAL_CHOP
    assert report.hard_macro_score == 0.0


def test_synthesizer_duckdb_persistence(tmp_path: Any) -> None:
    """33. Confirms insert into daily_narrative_intelligence and retrieval."""
    db_file = tmp_path / "test_synth.duckdb"
    db_mgr = DuckDBManager(db_path=db_file)

    report = DailyNarrativeReport(
        intelligence_date=date(2026, 9, 19),
        synthesized_at_utc=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
        hard_macro_score=0.45,
        sentiment_score=0.60,
        narrative_score=0.30,
        composite_mni=0.465,
        regime=MacroRegime.CAUTIOUS_BULL,
        black_swan_flag=False,
        active_critical_alerts=0,
        dominant_pillar=MacroPillar.INSTITUTIONAL,
        narrative_summary_id="🔵 Akumulasi Oportunistik",
    )

    with db_mgr:
        db_mgr.create_macro_tables()
        db_mgr.insert_daily_narrative_intelligence(report)
        retrieved = db_mgr.get_latest_narrative_intelligence()

    assert retrieved is not None
    assert retrieved.intelligence_date == date(2026, 9, 19)
    assert retrieved.regime == MacroRegime.CAUTIOUS_BULL
    assert retrieved.composite_mni == 0.465
    assert retrieved.black_swan_flag is False


def test_mart_macro_narrative_daily_view(tmp_path: Any) -> None:
    """34. Queries conformed analytical view mart_macro_narrative_daily successfully."""
    db_file = tmp_path / "test_mart_view.duckdb"
    db_mgr = DuckDBManager(db_path=db_file)

    with db_mgr:
        con = db_mgr.get_connection()
        # Create base investment signals mart table with test record
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_btc_investment_signals_daily (
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
            INSERT INTO mart_btc_investment_signals_daily VALUES (
                DATE '2026-09-19', 88000.0, 75000.0, 1.17, 1.95, 65,
                'Greed', FALSE, 'OPPORTUNISTIC_ACCUMULATE'
            );
            """
        )

        db_mgr.create_macro_tables()
        report = DailyNarrativeReport(
            intelligence_date=date(2026, 9, 19),
            synthesized_at_utc=datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC),
            hard_macro_score=0.25,
            sentiment_score=0.50,
            narrative_score=0.40,
            composite_mni=0.375,
            regime=MacroRegime.CAUTIOUS_BULL,
            black_swan_flag=False,
            active_critical_alerts=0,
            dominant_pillar=MacroPillar.INSTITUTIONAL,
            narrative_summary_id="🔵 Akumulasi Oportunistik",
        )
        db_mgr.insert_daily_narrative_intelligence(report)
        db_mgr.create_macro_mart_view()

        rows = con.execute("SELECT * FROM mart_macro_narrative_daily;").fetchall()
        cols = [d[0] for d in con.description]
        assert len(rows) == 1
        record = dict(zip(cols, rows[0], strict=True))
        assert record["composite_mni"] == 0.375
        assert record["macro_regime"] == "CAUTIOUS_BULL"
        assert record["black_swan_flag"] is False
        assert record["hard_macro_score"] == 0.25
