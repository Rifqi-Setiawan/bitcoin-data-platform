"""Composite Macro-Narrative Index (MNI) calculation and narrative generation."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from bitcoin_data_platform.macro.models import (
    DailyNarrativeReport,
    MacroPillar,
    MacroRegime,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def compute_sentiment_subscore(
    fng_value: float | None = None,
    mvrv_ratio: float | None = None,
    mayer_multiple: float | None = None,
) -> float:
    """Compute Tier 2 Sentiment & Valuation score S_sentiment in [-1.0, +1.0]."""
    # 1. Fear & Greed Component (0 to 100 -> -1.0 to +1.0)
    s_fng = (fng_value - 50.0) / 50.0 if fng_value is not None else 0.0
    s_fng = max(-1.0, min(1.0, s_fng))

    # 2. MVRV Ratio Valuation Component
    if mvrv_ratio is not None:
        if mvrv_ratio < 1.0:
            s_mvrv = 1.0
        elif mvrv_ratio < 1.8:
            s_mvrv = 0.5
        elif mvrv_ratio < 2.8:
            s_mvrv = -0.2
        else:
            s_mvrv = -1.0
    else:
        s_mvrv = 0.0

    # 3. Mayer Multiple Technical Momentum Component
    if mayer_multiple is not None:
        if mayer_multiple < 0.8:
            s_mm = 1.0
        elif mayer_multiple < 1.4:
            s_mm = 0.3
        elif mayer_multiple < 2.4:
            s_mm = -0.4
        else:
            s_mm = -1.0
    else:
        s_mm = 0.0

    # 40% FNG + 30% MVRV + 30% Mayer Multiple
    s_sentiment = 0.40 * s_fng + 0.30 * s_mvrv + 0.30 * s_mm
    return max(-1.0, min(1.0, round(s_sentiment, 4)))


def compute_composite_mni(
    hard_macro_score: float,
    sentiment_score: float,
    narrative_score: float,
) -> float:
    """Compute 3-Tier Composite Macro-Narrative Index (MNI).

    Formula: MNI = 0.40 * S_macro + 0.35 * S_sentiment + 0.25 * S_narrative
    """
    mni = 0.40 * hard_macro_score + 0.35 * sentiment_score + 0.25 * narrative_score
    return max(-1.0, min(1.0, round(mni, 4)))


def classify_macro_regime(
    composite_mni: float,
    black_swan_flag: bool = False,
) -> MacroRegime:
    """Determine market regime from Composite MNI and black swan sentinel flag."""
    if black_swan_flag or composite_mni < -0.60:
        return MacroRegime.BLACK_SWAN_CRISIS
    if composite_mni >= 0.50:
        return MacroRegime.RISK_ON_EXPANSION
    if composite_mni >= 0.15:
        return MacroRegime.CAUTIOUS_BULL
    if composite_mni >= -0.20:
        return MacroRegime.NEUTRAL_CHOP
    return MacroRegime.RISK_OFF_DEFENSE


def generate_bahasa_indonesia_narrative(
    regime: MacroRegime,
    composite_mni: float,
    hard_macro_score: float,
    sentiment_score: float,
    narrative_score: float,
    dominant_pillar: MacroPillar,
    black_swan_flag: bool = False,
    active_critical_alerts: int = 0,
) -> str:
    """Generate professional, localized narrative commentary in Bahasa Indonesia."""
    pillar_labels: dict[MacroPillar, str] = {
        MacroPillar.REGULATORY: "Regulasi & Kebijakan Hukum",
        MacroPillar.SECURITY_EXPLOIT: "Keamanan Sistem & Eksploitasi Protokol",
        MacroPillar.INSTITUTIONAL: "Arus Masuk Institusional",
        MacroPillar.MACRO_LIQUIDITY: "Likuiditas Makroekonomi & Suku Bunga",
        MacroPillar.GENERAL: "Sentimen Pasar Umum",
    }
    pillar_name = pillar_labels.get(dominant_pillar, "Umum")

    if regime == MacroRegime.BLACK_SWAN_CRISIS:
        if black_swan_flag:
            return (
                "🔴 PERINGATAN DARURAT (BLACK SWAN): Terdeteksi ancaman sistemik "
                f"kritis pada sektor {pillar_name} ({active_critical_alerts} peringatan aktif). "
                f"Composite MNI anjlok ke {composite_mni:.2f}. "
                "Sistem mengaktifkan Circuit Breaker: pembekuan total eksekusi pembelian."
            )
        return (
            "🔴 KONTRAKSI MAKRO EKSTRIM: Kondisi likuiditas global dan sentimen pasar "
            f"mengalami penurunan drastis (MNI {composite_mni:.2f}). "
            "Circuit breaker diaktifkan secara otomatis."
        )

    if regime == MacroRegime.RISK_ON_EXPANSION:
        return (
            "🟢 EKSPANSI LIKUIDITAS MAKRO (Risk-On): Indeks komposit makro-naratif mencapai "
            f"{composite_mni:.2f}. Likuiditas akomodatif (+{hard_macro_score:.2f}) "
            f"dan sentimen on-chain kuat (+{sentiment_score:.2f}). "
            f"Didukung katalis pilar {pillar_name}. Postur: Akumulasi agresif optimal."
        )

    if regime == MacroRegime.CAUTIOUS_BULL:
        return (
            "🔵 AKUMULASI OPORTUNISTIK (Cautious Bull): Indeks MNI berada pada "
            f"teritori konstruktif (+{composite_mni:.2f}). Kondisi makro stabil tanpa tekanan "
            f"regulasi signifikan. Katalis pilar {pillar_name} mendukung momentum. "
            "Postur: Akumulasi terukur dan disiplin."
        )

    if regime == MacroRegime.NEUTRAL_CHOP:
        return (
            "⚪ PASAR KONSOLIDASI (Neutral Chop): Indeks MNI seimbang pada level "
            f"{composite_mni:.2f}. Tekanan inflasi dan suku bunga termitigasi, volatilitas "
            f"berita tenang pada pilar {pillar_name}. Postur: Pembelian rutin DCA standar."
        )

    # RISK_OFF_DEFENSE
    return (
        "🟡 DEFENSIVE RESERVE (Risk-Off): Terdeteksi pengetatan likuiditas makro atau tekanan "
        f"naratif (MNI {composite_mni:.2f}, naratif {narrative_score:.2f}). "
        f"Fokus risiko pada {pillar_name}. Postur: Alokasi kas dialihkan ke Tactical Reserve."
    )


class MacroNarrativeSynthesizer:
    """Coordinates 3-tier synthesis, DuckDB persistence, and daily report generation."""

    def __init__(self, db_manager: DuckDBManager | None = None) -> None:
        self.db_manager = db_manager

    def synthesize(
        self,
        *,
        intelligence_date: date | None = None,
        hard_macro_score: float = 0.0,
        sentiment_score: float | None = None,
        narrative_score: float = 0.0,
        fng_value: float | None = None,
        mvrv_ratio: float | None = None,
        mayer_multiple: float | None = None,
        dominant_pillar: MacroPillar = MacroPillar.GENERAL,
        black_swan_flag: bool = False,
        active_critical_alerts: int = 0,
        now_utc: datetime | None = None,
    ) -> DailyNarrativeReport:
        """Execute 3-tier synthesis and return DailyNarrativeReport."""
        current_time = now_utc or datetime.now(UTC)
        target_date = intelligence_date or current_time.date()

        # Compute sentiment score if not provided directly
        if sentiment_score is None:
            sentiment_score = compute_sentiment_subscore(
                fng_value=fng_value,
                mvrv_ratio=mvrv_ratio,
                mayer_multiple=mayer_multiple,
            )

        # Calculate composite MNI
        composite_mni = compute_composite_mni(
            hard_macro_score=hard_macro_score,
            sentiment_score=sentiment_score,
            narrative_score=narrative_score,
        )

        # Determine regime
        regime = classify_macro_regime(
            composite_mni=composite_mni,
            black_swan_flag=black_swan_flag,
        )

        # Generate localized narrative
        narrative_id = generate_bahasa_indonesia_narrative(
            regime=regime,
            composite_mni=composite_mni,
            hard_macro_score=hard_macro_score,
            sentiment_score=sentiment_score,
            narrative_score=narrative_score,
            dominant_pillar=dominant_pillar,
            black_swan_flag=black_swan_flag,
            active_critical_alerts=active_critical_alerts,
        )

        report = DailyNarrativeReport(
            intelligence_date=target_date,
            synthesized_at_utc=current_time,
            hard_macro_score=hard_macro_score,
            sentiment_score=sentiment_score,
            narrative_score=narrative_score,
            composite_mni=composite_mni,
            regime=regime,
            black_swan_flag=black_swan_flag,
            active_critical_alerts=active_critical_alerts,
            dominant_pillar=dominant_pillar,
            narrative_summary_id=narrative_id,
        )

        if self.db_manager is not None:
            self.db_manager.insert_daily_narrative_intelligence(report)

        return report

    def synthesize_from_db(
        self,
        target_date: date | None = None,
        now_utc: datetime | None = None,
    ) -> DailyNarrativeReport:
        """Query DuckDB analytical views for today's inputs and generate report."""
        if self.db_manager is None:
            raise ValueError("DuckDBManager required for database-backed synthesis.")

        current_time = now_utc or datetime.now(UTC)
        query_date = target_date or current_time.date()

        con = self.db_manager.get_connection()
        self.db_manager.initialize()

        # 1. Query latest sentiment & indicators from mart_btc_investment_signals_daily
        fng_val = 50.0
        mvrv = None
        mm = None

        try:
            row = con.execute(
                """
                SELECT fng_value, mvrv_ratio, mayer_multiple
                FROM mart_btc_investment_signals_daily
                ORDER BY trade_date_utc DESC
                LIMIT 1;
                """
            ).fetchone()
            if row:
                fng_val = float(row[0]) if row[0] is not None else 50.0
                mvrv = float(row[1]) if row[1] is not None else None
                mm = float(row[2]) if row[2] is not None else None
        except Exception:
            pass

        # 2. Query Hard Macro Releases from last 72 hours
        releases_data: list[dict[str, Any]] = self.db_manager.get_macro_economic_releases()
        from bitcoin_data_platform.macro.macro_analyzer import MacroAnalyzer

        analyzer = MacroAnalyzer()
        parsed_releases = [analyzer.analyze_event(r) for r in releases_data]
        hard_macro_score = analyzer.compute_hard_macro_score(parsed_releases, now_utc=current_time)

        # 3. Query News Articles from last 24 hours
        articles_data = self.db_manager.get_macro_articles(limit=100)
        from bitcoin_data_platform.macro.sentiment_analyzer import SentimentAnalyzer

        sent_analyzer = SentimentAnalyzer()
        parsed_articles = []
        for a in articles_data:
            pub_dt = a.get("published_utc")
            if isinstance(pub_dt, str):
                pub_dt = datetime.fromisoformat(pub_dt)
            elif not isinstance(pub_dt, datetime):
                pub_dt = current_time

            ing_dt = a.get("ingested_at_utc")
            if isinstance(ing_dt, str):
                ing_dt = datetime.fromisoformat(ing_dt)
            elif not isinstance(ing_dt, datetime):
                ing_dt = current_time

            from bitcoin_data_platform.macro.models import AlertSeverity, MacroArticle

            art = MacroArticle(
                article_id=str(a.get("article_id", "")),
                source=str(a.get("source", "General")),
                title=str(a.get("title", "")),
                url=str(a.get("url", "")),
                published_utc=pub_dt,
                summary=str(a.get("summary", "")),
                pillar=MacroPillar(a.get("pillar", MacroPillar.GENERAL)),
                severity=AlertSeverity(a.get("severity", AlertSeverity.LOW)),
                polarity=float(a.get("polarity", 0.0)),
                matched_keywords=(
                    a.get("matched_keywords", "").split(", ")
                    if isinstance(a.get("matched_keywords"), str)
                    else []
                ),
                ingested_at_utc=ing_dt,
            )
            parsed_articles.append(art)

        (
            narrative_score,
            black_swan,
            critical_alerts,
            dominant_pillar,
        ) = sent_analyzer.compute_narrative_score(parsed_articles, now_utc=current_time)

        sentiment_score = compute_sentiment_subscore(
            fng_value=fng_val,
            mvrv_ratio=mvrv,
            mayer_multiple=mm,
        )

        return self.synthesize(
            intelligence_date=query_date,
            hard_macro_score=hard_macro_score,
            sentiment_score=sentiment_score,
            narrative_score=narrative_score,
            dominant_pillar=dominant_pillar,
            black_swan_flag=black_swan,
            active_critical_alerts=critical_alerts,
            now_utc=current_time,
        )
