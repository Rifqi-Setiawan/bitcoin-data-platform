"""Canonical domain models and data contracts for Phase 18 event-driven regime pacing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class PacingState(str, Enum):
    """Pacing execution states for the deterministic finite-state machine."""

    IDLE_CHOP = "IDLE_CHOP"
    WEEKLY_CORE = "WEEKLY_CORE"
    SNIPER_DEPLOYMENT = "SNIPER_DEPLOYMENT"
    FROTH_FREEZE = "FROTH_FREEZE"


class DecisionStatus(str, Enum):
    """Lifecycle status for pacing execution decisions."""

    PROPOSED = "PROPOSED"
    AUTHORIZED = "AUTHORIZED"
    REJECTED = "REJECTED"
    SETTLED = "SETTLED"
    FAILED = "FAILED"


class CapitalPool(str, Enum):
    """Isolated capital allocation pools."""

    BASE = "00_BASE"
    TACTICAL_RESERVE = "00_TACTICAL_RESERVE"


class ExecutionMode(str, Enum):
    """Execution invocation mode."""

    AUTO = "AUTO"
    MANUAL_FORCE = "MANUAL_FORCE"


@dataclass(frozen=True)
class TriggerObservation:
    """Canonical daily trigger observation consumed from mart_btc_event_triggers."""

    product_id: str
    trade_date_utc: date
    observed_at_utc: datetime
    close: Decimal
    return_24h: Decimal | None
    rolling_peak_7d: Decimal
    rolling_peak_30d: Decimal
    drawdown_7d: float
    drawdown_30d: float
    mvrv_ratio: float | None
    mayer_multiple: float | None
    fng_value: int | None
    is_weekly_cadence_day: bool
    is_drawdown_event: bool
    is_regime_capitulation: bool
    is_regime_froth: bool
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    volume_base: Decimal | None = None
    sma_200: float | None = None
    sample_count_7d: int | None = None
    sample_count_30d: int | None = None
    observed_hour_count: int | None = None
    is_complete: bool | None = None
    source: str | None = None

    def __post_init__(self) -> None:
        """Validate input domain invariants and boundaries."""
        if not self.product_id or not self.product_id.strip():
            raise ValueError("product_id must be a non-empty string")
        if self.close <= Decimal(0):
            raise ValueError(f"close must be strictly positive, got {self.close}")
        if self.rolling_peak_7d <= Decimal(0):
            raise ValueError(
                f"rolling_peak_7d must be strictly positive, got {self.rolling_peak_7d}"
            )
        if self.rolling_peak_30d <= Decimal(0):
            raise ValueError(
                f"rolling_peak_30d must be strictly positive, got {self.rolling_peak_30d}"
            )
        if self.drawdown_7d < -1.0 or self.drawdown_7d > 0.0:
            raise ValueError(f"drawdown_7d must be in [-1.0, 0.0], got {self.drawdown_7d}")
        if self.drawdown_30d < -1.0 or self.drawdown_30d > 0.0:
            raise ValueError(f"drawdown_30d must be in [-1.0, 0.0], got {self.drawdown_30d}")
        if self.fng_value is not None and not (0 <= self.fng_value <= 100):
            raise ValueError(f"fng_value must be in [0, 100], got {self.fng_value}")
        if self.mvrv_ratio is not None and self.mvrv_ratio < 0.0:
            raise ValueError(f"mvrv_ratio cannot be negative, got {self.mvrv_ratio}")
        if self.mayer_multiple is not None and self.mayer_multiple < 0.0:
            raise ValueError(f"mayer_multiple cannot be negative, got {self.mayer_multiple}")
        if self.observed_at_utc.tzinfo is None:
            raise ValueError("observed_at_utc must be a timezone-aware UTC datetime")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TriggerObservation:
        """Construct a validated TriggerObservation from a dictionary representation."""
        raw_trade_date = data["trade_date_utc"]
        if isinstance(raw_trade_date, datetime):
            trade_date = raw_trade_date.date()
        elif isinstance(raw_trade_date, date):
            trade_date = raw_trade_date
        else:
            trade_date = date.fromisoformat(str(raw_trade_date)[:10])

        raw_observed_at = data["observed_at_utc"]
        if isinstance(raw_observed_at, datetime):
            if raw_observed_at.tzinfo is None:
                observed_at = raw_observed_at.replace(tzinfo=UTC)
            else:
                observed_at = raw_observed_at.astimezone(UTC)
        else:
            observed_at = datetime.fromisoformat(str(raw_observed_at))
            if observed_at.tzinfo is None:
                observed_at = observed_at.replace(tzinfo=UTC)
            else:
                observed_at = observed_at.astimezone(UTC)

        raw_return = data.get("return_24h")
        return_24h = Decimal(str(raw_return)) if raw_return is not None else None

        raw_open = data.get("open")
        open_val = Decimal(str(raw_open)) if raw_open is not None else None

        raw_high = data.get("high")
        high_val = Decimal(str(raw_high)) if raw_high is not None else None

        raw_low = data.get("low")
        low_val = Decimal(str(raw_low)) if raw_low is not None else None

        raw_vol = data.get("volume_base")
        volume_val = Decimal(str(raw_vol)) if raw_vol is not None else None

        return cls(
            product_id=str(data["product_id"]),
            trade_date_utc=trade_date,
            observed_at_utc=observed_at,
            close=Decimal(str(data["close"])),
            return_24h=return_24h,
            rolling_peak_7d=Decimal(str(data["rolling_peak_7d"])),
            rolling_peak_30d=Decimal(str(data["rolling_peak_30d"])),
            drawdown_7d=float(data["drawdown_7d"]),
            drawdown_30d=float(data["drawdown_30d"]),
            mvrv_ratio=float(data["mvrv_ratio"]) if data.get("mvrv_ratio") is not None else None,
            mayer_multiple=float(data["mayer_multiple"])
            if data.get("mayer_multiple") is not None
            else None,
            fng_value=int(data["fng_value"]) if data.get("fng_value") is not None else None,
            is_weekly_cadence_day=bool(data["is_weekly_cadence_day"]),
            is_drawdown_event=bool(data["is_drawdown_event"]),
            is_regime_capitulation=bool(data["is_regime_capitulation"]),
            is_regime_froth=bool(data["is_regime_froth"]),
            open=open_val,
            high=high_val,
            low=low_val,
            volume_base=volume_val,
            sma_200=float(data["sma_200"]) if data.get("sma_200") is not None else None,
            sample_count_7d=int(data["sample_count_7d"])
            if data.get("sample_count_7d") is not None
            else None,
            sample_count_30d=int(data["sample_count_30d"])
            if data.get("sample_count_30d") is not None
            else None,
            observed_hour_count=int(data["observed_hour_count"])
            if data.get("observed_hour_count") is not None
            else None,
            is_complete=bool(data["is_complete"]) if data.get("is_complete") is not None else None,
            source=str(data["source"]) if data.get("source") is not None else None,
        )
