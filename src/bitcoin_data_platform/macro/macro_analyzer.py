"""Macroeconomic calendar analysis, surprise deltas, and directional liquidity scoring."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

from bitcoin_data_platform.macro.models import MacroEconomicRelease
from bitcoin_data_platform.sources.macro_calendar_client import MacroCalendarClient
from bitcoin_data_platform.sources.macro_calendar_contract import MacroEvent

# Exponential decay constant: 24h half-life -> lambda = ln(2) / 24
LAMBDA_24H = math.log(2) / 24.0

IMPACT_WEIGHTS: dict[str, float] = {
    "HIGH": 1.0,
    "MEDIUM": 0.5,
    "LOW": 0.2,
    "HOLIDAY": 0.0,
}


def parse_numeric_economic_value(val: Any) -> float | None:
    """Parse economic indicator string like '0.3%', '250K', '1.4M', '5.25%' into float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if math.isnan(val):
            return None
        return float(val)

    s = str(val).strip()
    if not s or s == "-" or s.lower() == "none" or s.lower() == "nan":
        return None

    # Strip currency and commas
    s = s.replace("$", "").replace(",", "")

    # Check for percentage
    is_pct = "%" in s
    s = s.replace("%", "").strip()

    multiplier = 1.0
    if s.endswith("K") or s.endswith("k"):
        multiplier = 1.0  # Keep in thousands for consistency with K forecasts
        s = s[:-1].strip()
    elif s.endswith("M") or s.endswith("m"):
        multiplier = 1000.0  # Millions to thousands
        s = s[:-1].strip()
    elif s.endswith("B") or s.endswith("b"):
        multiplier = 1000000.0
        s = s[:-1].strip()

    try:
        num = float(s) * multiplier
        return num if not is_pct else num
    except ValueError:
        # Match leading numeric part
        match = re.match(r"^[-+]?[0-9]*\.?[0-9]+", s)
        if match:
            try:
                return float(match.group(0)) * multiplier
            except ValueError:
                return None
        return None


def compute_release_id(event_name: str, release_date: date, country: str) -> str:
    """Compute deterministic SHA-256 release ID."""
    raw = f"{event_name.strip().lower()}:{release_date.isoformat()}:{country.strip().upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class MacroAnalyzer:
    """Analyzes macroeconomic releases, surprises, and Hard Macro Scores."""

    def __init__(
        self,
        *,
        scale_cpi: float = 0.4,
        scale_nfp: float = 100.0,
        rolling_window_hours: float = 72.0,
    ) -> None:
        self.scale_cpi = scale_cpi
        self.scale_nfp = scale_nfp
        self.rolling_window_hours = rolling_window_hours

    def evaluate_directional_score(
        self,
        event_name: str,
        surprise_delta: float | None,
        actual: float | None,
        forecast: float | None,
    ) -> float:
        """Evaluate directional liquidity score z_i in [-1.0, +1.0].

        Hawkish (negative liquidity shock for BTC) -> z_i < 0
        Dovish (positive liquidity shock for BTC) -> z_i > 0
        """
        name_lower = event_name.lower()

        # 1. FOMC / Interest Rate Decision
        if (
            "fed funds" in name_lower
            or "interest rate" in name_lower
            or "fomc rate" in name_lower
            or "rate decision" in name_lower
        ):
            if surprise_delta is not None:
                if surprise_delta > 0.0:
                    return -1.0  # Hawkish hike surprise
                if surprise_delta < 0.0:
                    return 1.0  # Dovish cut surprise
            if actual is not None and forecast is not None:
                if actual < forecast:
                    return 1.0  # Cut
                if actual > forecast:
                    return -1.0  # Hike
            return 0.0

        # If upcoming or missing actual/surprise
        if surprise_delta is None:
            return 0.0

        # 2. CPI / PPI / Inflation
        if "cpi" in name_lower or "ppi" in name_lower or "inflation" in name_lower:
            # Higher inflation than expected -> Hawkish -> Negative
            score = -(surprise_delta / self.scale_cpi)
            return max(-1.0, min(1.0, score))

        # 3. Non-Farm Payrolls / Employment
        if (
            "non-farm" in name_lower
            or "nfp" in name_lower
            or "employment change" in name_lower
            or "payrolls" in name_lower
        ):
            # Stronger jobs than expected -> delays rate cuts -> Hawkish -> Negative
            score = -(surprise_delta / self.scale_nfp)
            return max(-1.0, min(1.0, score))

        # 4. Unemployment Rate
        if "unemployment rate" in name_lower:
            # Higher unemployment -> slower economy -> Dovish cuts -> Positive
            scale_unemp = 0.3
            score = surprise_delta / scale_unemp
            return max(-1.0, min(1.0, score))

        # 5. GDP Growth
        if "gdp" in name_lower:
            # Growth surprise with stable inflation -> positive moderate expansion
            if surprise_delta > 0.0:
                return 0.5
            if surprise_delta < 0.0:
                return -0.5
            return 0.0

        # Default: General economic indicator
        scale_gen = 1.0
        return max(-1.0, min(1.0, surprise_delta / scale_gen))

    def analyze_event(
        self,
        event: MacroEvent | dict[str, Any],
        ingested_at: datetime | None = None,
    ) -> MacroEconomicRelease:
        """Convert MacroEvent or raw dict into parsed MacroEconomicRelease."""
        now_utc = ingested_at or datetime.now(UTC)

        if isinstance(event, MacroEvent):
            event_name = event.title
            country = event.country
            impact = event.impact
            dt = event.scheduled_utc
            forecast_raw = event.forecast
            previous_raw = event.previous
            actual_raw = getattr(event, "actual", None)
            raw_payload = json.dumps(
                {
                    "title": event.title,
                    "country": event.country,
                    "impact": event.impact,
                    "scheduled_utc": event.scheduled_utc.isoformat(),
                    "forecast": event.forecast,
                    "previous": event.previous,
                }
            )
        else:
            event_name = str(event.get("title") or event.get("event_name", "Macro Event"))
            country = str(event.get("country", "USD"))
            impact = str(event.get("impact", "Low"))
            raw_date = event.get("date") or event.get("scheduled_utc")
            if isinstance(raw_date, datetime):
                dt = raw_date if raw_date.tzinfo else raw_date.replace(tzinfo=UTC)
            elif isinstance(raw_date, str):
                try:
                    dt = datetime.fromisoformat(raw_date)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                except Exception:
                    dt = now_utc
            else:
                dt = now_utc

            forecast_raw = event.get("forecast")
            previous_raw = event.get("previous")
            actual_raw = event.get("actual")
            raw_payload = json.dumps(event)

        release_date = dt.date()
        release_time = dt.strftime("%H:%M")

        actual_val = parse_numeric_economic_value(actual_raw)
        forecast_val = parse_numeric_economic_value(forecast_raw)
        previous_val = parse_numeric_economic_value(previous_raw)

        surprise_delta: float | None = None
        if actual_val is not None and forecast_val is not None:
            surprise_delta = round(actual_val - forecast_val, 4)

        directional_score = self.evaluate_directional_score(
            event_name=event_name,
            surprise_delta=surprise_delta,
            actual=actual_val,
            forecast=forecast_val,
        )

        release_id = compute_release_id(event_name, release_date, country)

        return MacroEconomicRelease(
            release_id=release_id,
            event_name=event_name,
            country=country,
            release_date=release_date,
            release_time_utc=release_time,
            impact=impact,
            actual_value=actual_val,
            forecast_value=forecast_val,
            previous_value=previous_val,
            surprise_delta=surprise_delta,
            directional_score=directional_score,
            raw_payload_json=raw_payload,
            ingested_at_utc=now_utc,
        )

    def compute_hard_macro_score(
        self,
        releases: Sequence[MacroEconomicRelease],
        now_utc: datetime | None = None,
    ) -> float:
        """Compute Tier 1 Hard Macro Score S_macro in [-1.0, +1.0] with 72h time decay."""
        if not releases:
            return 0.0

        current_time = now_utc or datetime.now(UTC)
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=UTC)

        weighted_score_sum = 0.0
        weight_sum = 0.0

        for r in releases:
            # Reconstruct UTC release datetime
            try:
                hour, minute = map(int, r.release_time_utc.split(":"))
            except Exception:
                hour, minute = 0, 0
            release_dt = datetime(
                r.release_date.year,
                r.release_date.month,
                r.release_date.day,
                hour,
                minute,
                tzinfo=UTC,
            )

            # Age in hours
            age_hours = (current_time - release_dt).total_seconds() / 3600.0

            # Only consider events within rolling window (e.g. 72 hours) and not future beyond 2h
            if age_hours < -2.0 or age_hours > self.rolling_window_hours:
                continue

            impact_norm = r.impact.strip().upper()
            impact_weight = IMPACT_WEIGHTS.get(impact_norm, 0.2)

            # Exponential decay: w_i = ImpactWeight * exp(-lambda * delta_t)
            time_decay = math.exp(-LAMBDA_24H * max(0.0, age_hours))
            w_i = impact_weight * time_decay

            weighted_score_sum += w_i * r.directional_score
            weight_sum += w_i

        if weight_sum <= 1e-6:
            return 0.0

        score = weighted_score_sum / weight_sum
        return max(-1.0, min(1.0, score))

    def fetch_and_analyze_calendar(
        self,
        client: MacroCalendarClient | None = None,
        country_filter: str | None = "USD",
        impact_filter: str | None = None,
    ) -> list[MacroEconomicRelease]:
        """Fetch live calendar events and analyze economic surprises."""
        c = client or MacroCalendarClient()
        try:
            raw_events = c.fetch_week_events(
                country_filter=country_filter,
                impact_filter=impact_filter,
            )
            return [self.analyze_event(e) for e in raw_events]
        finally:
            if client is None:
                c.close()
