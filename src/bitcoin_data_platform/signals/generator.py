"""Investment signal generator querying mart_btc_investment_signals_daily."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@dataclass(frozen=True)
class InvestmentSignal:
    """Represents a computed investment signal for Bitcoin."""

    signal_date_utc: date
    generated_at_utc: datetime
    market_close_usd: float
    sma_200: float | None
    mayer_multiple: float | None
    mvrv_ratio: float | None
    fng_value: int
    fng_classification: str
    has_high_impact_macro_event: bool
    investment_signal: str
    signal_strength: str  # STRONG | MODERATE | WEAK
    narrative: str  # Human-readable explanation in Bahasa Indonesia

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "signal_date_utc": self.signal_date_utc.isoformat(),
            "generated_at_utc": self.generated_at_utc.isoformat(),
            "market_close_usd": self.market_close_usd,
            "sma_200": self.sma_200,
            "mayer_multiple": self.mayer_multiple,
            "mvrv_ratio": self.mvrv_ratio,
            "fng_value": self.fng_value,
            "fng_classification": self.fng_classification,
            "has_high_impact_macro_event": self.has_high_impact_macro_event,
            "investment_signal": self.investment_signal,
            "signal_strength": self.signal_strength,
            "narrative": self.narrative,
        }


def generate_narrative(
    signal_name: str,
    mayer_multiple: float | None,
    mvrv_ratio: float | None,
    fng_value: int,
) -> str:
    """Generate human-readable narrative in Bahasa Indonesia."""
    mm_str = f"{mayer_multiple:.2f}" if mayer_multiple is not None else "N/A"
    mvrv_str = f"{mvrv_ratio:.2f}" if mvrv_ratio is not None else "N/A"

    narratives = {
        "AGGRESSIVE_ACCUMULATE": (
            f"🟢 AKUMULASI AGRESIF: Bitcoin di bawah nilai wajar (Mayer {mm_str}, MVRV {mvrv_str}) "
            f"+ pasar panik (FNG {fng_value}). Peluang beli terbaik."
        ),
        "OPPORTUNISTIC_ACCUMULATE": (
            f"🔵 AKUMULASI OPORTUNISTIK: Bitcoin di zona diskon (Mayer {mm_str}). "
            "Beli lebih dari normal."
        ),
        "STANDARD_DCA": (
            f"⚪ DCA STANDAR: Pasar di zona normal (Mayer {mm_str}). Lanjutkan investasi reguler."
        ),
        "DEFENSIVE_RESERVE": (
            f"🟡 CADANGAN DEFENSIF: Pasar mulai panas (Mayer {mm_str}, FNG {fng_value}). "
            "Kurangi beli, sisihkan ke kas."
        ),
        "HARD_FREEZE": (
            f"🔴 STOP TOTAL: Pasar bubble (Mayer {mm_str}, MVRV {mvrv_str}). "
            "Jangan beli, tunggu koreksi."
        ),
    }
    return narratives.get(
        signal_name,
        f"⚪ {signal_name}: Kondisi pasar (Mayer {mm_str}, MVRV {mvrv_str}, FNG {fng_value}).",
    )


def compute_signal_strength(
    signal_name: str,
    mayer_multiple: float | None,
    mvrv_ratio: float | None,
    fng_value: int,
) -> str:
    """Compute strength by counting agreeing indicators (STRONG: 3+, MODERATE: 2, WEAK: 1)."""
    agreeing_count = 0
    if signal_name == "AGGRESSIVE_ACCUMULATE":
        if mayer_multiple is not None and mayer_multiple < 0.80:
            agreeing_count += 1
        if mvrv_ratio is not None and mvrv_ratio < 1.00:
            agreeing_count += 1
        if fng_value <= 25:
            agreeing_count += 1
    elif signal_name == "OPPORTUNISTIC_ACCUMULATE":
        if mayer_multiple is not None and mayer_multiple < 1.00:
            agreeing_count += 1
        if mvrv_ratio is not None and mvrv_ratio < 1.20:
            agreeing_count += 1
        if fng_value <= 45:
            agreeing_count += 1
    elif signal_name == "DEFENSIVE_RESERVE":
        if mayer_multiple is not None and mayer_multiple > 1.80:
            agreeing_count += 1
        if mvrv_ratio is not None and mvrv_ratio > 2.50:
            agreeing_count += 1
        if fng_value >= 75:
            agreeing_count += 1
    elif signal_name == "HARD_FREEZE":
        if mayer_multiple is not None and mayer_multiple > 2.40:
            agreeing_count += 1
        if mvrv_ratio is not None and mvrv_ratio > 3.50:
            agreeing_count += 1
        if fng_value >= 80:
            agreeing_count += 1
    else:  # STANDARD_DCA or others
        if mayer_multiple is not None and 1.00 <= mayer_multiple <= 1.80:
            agreeing_count += 1
        if mvrv_ratio is not None and 1.00 <= mvrv_ratio <= 2.50:
            agreeing_count += 1
        if 25 < fng_value < 75:
            agreeing_count += 1

    if agreeing_count >= 3:
        return "STRONG"
    elif agreeing_count == 2:
        return "MODERATE"
    else:
        return "WEAK"


class SignalGenerator:
    """Generates deterministic investment signals from DuckDB investment signals mart view."""

    def __init__(
        self,
        db_path: Path | str,
        clock: Callable[[], datetime] | None = None,
        db_manager: DuckDBManager | None = None,
    ) -> None:
        self.db_path = Path(db_path) if str(db_path) != ":memory:" else Path(":memory:")
        self._clock = clock or (lambda: datetime.now(UTC))
        self.db_manager = db_manager or DuckDBManager(db_path=self.db_path)

    def _row_to_signal(self, row: tuple[Any, ...]) -> InvestmentSignal:
        raw_date = row[0]
        if isinstance(raw_date, datetime):
            signal_date = raw_date.date()
        elif isinstance(raw_date, date):
            signal_date = raw_date
        elif isinstance(raw_date, str):
            signal_date = date.fromisoformat(raw_date)
        else:
            signal_date = date.fromisoformat(str(raw_date))

        market_close_usd = float(row[1]) if row[1] is not None else 0.0
        sma_200 = float(row[2]) if row[2] is not None else None
        mayer_multiple = float(row[3]) if row[3] is not None else None
        mvrv_ratio = float(row[4]) if row[4] is not None else None
        fng_value = int(row[5]) if row[5] is not None else 50
        fng_classification = str(row[6]) if row[6] is not None else "Neutral"
        has_macro = bool(row[7]) if row[7] is not None else False
        signal_type = str(row[8]) if row[8] is not None else "STANDARD_DCA"

        strength = compute_signal_strength(signal_type, mayer_multiple, mvrv_ratio, fng_value)
        narrative = generate_narrative(signal_type, mayer_multiple, mvrv_ratio, fng_value)

        return InvestmentSignal(
            signal_date_utc=signal_date,
            generated_at_utc=self._clock(),
            market_close_usd=market_close_usd,
            sma_200=sma_200,
            mayer_multiple=mayer_multiple,
            mvrv_ratio=mvrv_ratio,
            fng_value=fng_value,
            fng_classification=fng_classification,
            has_high_impact_macro_event=has_macro,
            investment_signal=signal_type,
            signal_strength=strength,
            narrative=narrative,
        )

    def generate_latest(self) -> InvestmentSignal:
        """Query mart_btc_investment_signals_daily and generate the latest signal."""
        con = self.db_manager.get_connection()
        row = con.execute(
            """
            SELECT trade_date_utc, market_close_usd, sma_200, mayer_multiple,
                   mvrv_ratio, fng_value, fng_classification,
                   has_high_impact_macro_event, investment_signal
            FROM mart_btc_investment_signals_daily
            ORDER BY trade_date_utc DESC
            LIMIT 1;
            """
        ).fetchone()
        if row is None:
            raise ValueError("No signal data found in mart_btc_investment_signals_daily")
        return self._row_to_signal(row)

    def generate_for_date(self, target_date: date) -> InvestmentSignal | None:
        """Query mart_btc_investment_signals_daily and generate signal for specific date."""
        con = self.db_manager.get_connection()
        row = con.execute(
            """
            SELECT trade_date_utc, market_close_usd, sma_200, mayer_multiple,
                   mvrv_ratio, fng_value, fng_classification,
                   has_high_impact_macro_event, investment_signal
            FROM mart_btc_investment_signals_daily
            WHERE CAST(trade_date_utc AS DATE) = ?;
            """,
            [target_date],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_signal(row)

    def save_signal(self, signal: InvestmentSignal) -> None:
        """Persist investment signal to signal_history table."""
        self.db_manager.insert_signal_history(
            signal_date_utc=signal.signal_date_utc,
            generated_at_utc=signal.generated_at_utc,
            market_close_usd=signal.market_close_usd,
            sma_200=signal.sma_200,
            mayer_multiple=signal.mayer_multiple,
            mvrv_ratio=signal.mvrv_ratio,
            fng_value=signal.fng_value,
            fng_classification=signal.fng_classification,
            has_high_impact_macro_event=signal.has_high_impact_macro_event,
            investment_signal=signal.investment_signal,
            signal_strength=signal.signal_strength,
            narrative=signal.narrative,
        )
