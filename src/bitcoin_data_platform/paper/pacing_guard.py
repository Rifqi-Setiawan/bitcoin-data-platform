"""Pacing Guard and deterministic finite-state machine for Phase 18 event-driven execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from bitcoin_data_platform.pacing.models import (
    CapitalPool,
    PacingState,
)


@dataclass(frozen=True)
class PacingDecision:
    """Immutable execution decision candidate emitted by PacingGuard."""

    state: PacingState
    action: str  # 'NO_ACTION', 'BUY', 'HOLD'
    pool: CapitalPool  # CapitalPool.BASE, CapitalPool.TACTICAL_RESERVE
    requested_amount_usd: float
    authorized_amount_usd: float
    reason_code: str
    narrative: str
    is_sniper: bool = False
    sniper_pct: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert decision to JSON-serializable dictionary."""
        return {
            "state": self.state.value,
            "action": self.action,
            "pool": self.pool.value,
            "requested_amount_usd": round(self.requested_amount_usd, 2),
            "authorized_amount_usd": round(self.authorized_amount_usd, 2),
            "reason_code": self.reason_code,
            "narrative": self.narrative,
            "is_sniper": self.is_sniper,
            "sniper_pct": round(self.sniper_pct, 4),
        }


class PacingGuard:
    """Deterministic event-driven regime pacing guard and state transition engine.

    Enforces state precedence:
      FROTH_FREEZE > SNIPER_DEPLOYMENT > WEEKLY_CORE > IDLE_CHOP

    Dual-Pool Capital Isolation:
      - 00_BASE: Funds weekly core routine DCA orders.
      - 00_TACTICAL_RESERVE: Funds asymmetric drawdown sniper tranches.

    Key Invariants:
      - Sideways chop: Records NO_ACTION with $0.00 execution and zero fee/cash burn.
      - Dynamic sniper: Deploys 20% to 35% from Tactical Reserve during drawdown triggers.
      - Base runway guard: Halves weekly buy when Base cash runway < 8 weeks.
      - Episode latch: Drawdown sniper fires once on trigger entry and latches until recovery.
    """

    def __init__(
        self,
        weekly_base_usd: float = 20.0,
        runway_min_weeks: int = 8,
        low_runway_factor: float = 0.50,
        cadence_weekday: int = 6,  # Sunday UTC (0=Monday, 6=Sunday)
        sniper_return_24h_lte: float = -0.05,
        sniper_drawdown_7d_lte: float = -0.12,
        sniper_pct_min: float = 0.20,
        sniper_pct_max: float = 0.35,
        capitulation_mvrv_lt: float = 1.0,
        capitulation_mayer_lt: float = 0.8,
        froth_fng_gte: int = 80,
        froth_mayer_gte: float = 2.0,
        preservation_mvrv_gt: float = 2.5,
        preservation_mayer_gt: float = 2.2,
        drawdown_rearm_7d: float = -0.08,
        daily_tactical_cap_pct: float = 0.35,  # Max 35% daily sniper cap
        tactical_48h_cap_pct: float = 0.40,  # Max 40% velocity bound in 48 hours
    ) -> None:
        self.weekly_base_usd = weekly_base_usd
        self.runway_min_weeks = runway_min_weeks
        self.low_runway_factor = low_runway_factor
        self.cadence_weekday = cadence_weekday
        self.sniper_return_24h_lte = sniper_return_24h_lte
        self.sniper_drawdown_7d_lte = sniper_drawdown_7d_lte
        self.sniper_pct_min = sniper_pct_min
        self.sniper_pct_max = sniper_pct_max
        self.capitulation_mvrv_lt = capitulation_mvrv_lt
        self.capitulation_mayer_lt = capitulation_mayer_lt
        self.froth_fng_gte = froth_fng_gte
        self.froth_mayer_gte = froth_mayer_gte
        self.preservation_mvrv_gt = preservation_mvrv_gt
        self.preservation_mayer_gt = preservation_mayer_gt
        self.drawdown_rearm_7d = drawdown_rearm_7d
        self.daily_tactical_cap_pct = daily_tactical_cap_pct
        self.tactical_48h_cap_pct = tactical_48h_cap_pct

    def calculate_dynamic_sniper_pct(
        self,
        return_24h: float | None = None,
        drawdown_7d: float | None = None,
        is_capitulation: bool = False,
    ) -> float:
        """Calculate dynamic deployment fraction (20% - 35%) from Tactical Reserve.

        Mild trigger (-5% 24h / -12% 7d): 20%
        Deep drawdown (-10% 24h / -25% 7d) or capitulation: 35%
        Scales continuously between 20% and 35% based on drawdown severity.
        """
        if is_capitulation:
            return self.sniper_pct_max

        pct_r24 = self.sniper_pct_min
        if return_24h is not None and return_24h <= self.sniper_return_24h_lte:
            # Interpolate between -0.05 (20%) and -0.10 (35%)
            excess_24h = abs(return_24h) - abs(self.sniper_return_24h_lte)
            ratio_24h = min(1.0, max(0.0, excess_24h / 0.05))
            pct_r24 = self.sniper_pct_min + (self.sniper_pct_max - self.sniper_pct_min) * ratio_24h

        pct_dd7 = self.sniper_pct_min
        if drawdown_7d is not None and drawdown_7d <= self.sniper_drawdown_7d_lte:
            # Interpolate between -0.12 (20%) and -0.25 (35%)
            excess_dd7 = abs(drawdown_7d) - abs(self.sniper_drawdown_7d_lte)
            ratio_dd7 = min(1.0, max(0.0, excess_dd7 / 0.13))
            pct_dd7 = self.sniper_pct_min + (self.sniper_pct_max - self.sniper_pct_min) * ratio_dd7

        selected_pct = max(pct_r24, pct_dd7)
        return round(min(self.sniper_pct_max, max(self.sniper_pct_min, selected_pct)), 4)

    def evaluate(
        self,
        trade_date: date,
        base_cash: float,
        reserve_cash: float,
        sniper_armed: bool = True,
        *,
        return_24h: float | None = None,
        drawdown_7d: float | None = None,
        drawdown_30d: float | None = None,
        mvrv_ratio: float | None = None,
        prev_mvrv_ratio: float | None = None,
        mayer_multiple: float | None = None,
        prev_mayer_multiple: float | None = None,
        fng_value: int | None = None,
        is_weekly_cadence_day: bool | None = None,
        has_high_impact_macro_event: bool = False,
        black_swan_flag: bool = False,
        composite_mni: float = 0.0,
        macro_event_proximity_minutes: int | None = None,
        investment_signal: str | None = None,
        is_drawdown_event: bool | None = None,
        is_regime_capitulation: bool | None = None,
        is_regime_froth: bool | None = None,
        tactical_debits_48h: float = 0.0,
        execution_mode: str = "AUTO",
        manual_budget: float | None = None,
    ) -> tuple[PacingDecision, bool]:
        """Evaluate market state and emit deterministic pacing decision.

        Returns:
            tuple[PacingDecision, bool]: Emitted decision and updated sniper_armed state.
        """
        # 1. Weekly cadence day predicate
        if is_weekly_cadence_day is not None:
            is_weekly = bool(is_weekly_cadence_day)
        else:
            is_weekly = trade_date.weekday() == self.cadence_weekday

        # 2. Capitulation predicates
        mvrv_cross = (
            mvrv_ratio is not None
            and prev_mvrv_ratio is not None
            and mvrv_ratio < self.capitulation_mvrv_lt
            and prev_mvrv_ratio >= self.capitulation_mvrv_lt
        )
        mayer_cross = (
            mayer_multiple is not None
            and prev_mayer_multiple is not None
            and mayer_multiple < self.capitulation_mayer_lt
            and prev_mayer_multiple >= self.capitulation_mayer_lt
        )
        if is_regime_capitulation is not None:
            is_cap = bool(is_regime_capitulation)
        else:
            is_cap = bool(
                mvrv_cross
                or mayer_cross
                or (mvrv_ratio is not None and mvrv_ratio < self.capitulation_mvrv_lt)
                or (mayer_multiple is not None and mayer_multiple < self.capitulation_mayer_lt)
            )

        # 3. Froth / Overheat predicates
        fng_mayer_froth = (
            fng_value is not None
            and mayer_multiple is not None
            and fng_value >= self.froth_fng_gte
            and mayer_multiple >= self.froth_mayer_gte
        )
        mvrv_preservation = (
            mvrv_ratio is not None
            and prev_mvrv_ratio is not None
            and mvrv_ratio > self.preservation_mvrv_gt
            and prev_mvrv_ratio <= self.preservation_mvrv_gt
        )
        mayer_preservation = (
            mayer_multiple is not None
            and prev_mayer_multiple is not None
            and mayer_multiple > self.preservation_mayer_gt
            and prev_mayer_multiple <= self.preservation_mayer_gt
        )
        if is_regime_froth is not None:
            is_froth = bool(is_regime_froth)
        else:
            is_froth = bool(
                fng_mayer_froth
                or mvrv_preservation
                or mayer_preservation
                or (mvrv_ratio is not None and mvrv_ratio > self.preservation_mvrv_gt)
                or (mayer_multiple is not None and mayer_multiple > self.preservation_mayer_gt)
            )

        # 4. Drawdown trigger predicates
        norm_signal = (investment_signal or "").upper().strip()
        signal_aggressive = norm_signal in ("AGGRESSIVE_ACCUMULATE", "SNIPER_ACCUMULATE")
        if is_drawdown_event is not None:
            is_dd = bool(is_drawdown_event) or signal_aggressive
        else:
            is_dd = bool(
                (return_24h is not None and return_24h <= self.sniper_return_24h_lte)
                or (drawdown_7d is not None and drawdown_7d <= self.sniper_drawdown_7d_lte)
                or signal_aggressive
            )

        # 5. Hard safety / Circuit breaker predicates
        macro_proximity_trip = (
            macro_event_proximity_minutes is not None and abs(macro_event_proximity_minutes) <= 120
        )
        is_hard_safety = (
            has_high_impact_macro_event
            or black_swan_flag
            or (composite_mni < -0.65)
            or macro_proximity_trip
            or (norm_signal == "HARD_FREEZE")
        )

        # Manual Force handling
        if execution_mode == "MANUAL_FORCE":
            if is_hard_safety or is_froth:
                reason = "HARD_RISK_FREEZE" if is_hard_safety else "FROTH_FNG_MAYER"
                decision = PacingDecision(
                    state=PacingState.FROTH_FREEZE,
                    action="HOLD",
                    pool=CapitalPool.BASE,
                    requested_amount_usd=0.0,
                    authorized_amount_usd=0.0,
                    reason_code=reason,
                    narrative=f"🔴 FROTH_FREEZE: Manual force halted by safety layer ({reason}).",
                )
                return decision, sniper_armed

            target = manual_budget if manual_budget is not None else self.weekly_base_usd
            alloc = min(target, base_cash)
            decision = PacingDecision(
                state=PacingState.WEEKLY_CORE,
                action="BUY" if alloc > 0 else "HOLD",
                pool=CapitalPool.BASE,
                requested_amount_usd=target,
                authorized_amount_usd=round(alloc, 2),
                reason_code="MANUAL_FORCE_EXECUTION",
                narrative=f"⚡ MANUAL_FORCE: Eksekusi paksa manual ${alloc:,.2f} dari Base Cash.",
            )
            return decision, sniper_armed

        # -------------------------------------------------------------
        # STATE EVALUATION & PRECEDENCE (FROTH > SNIPER > WEEKLY > CHOP)
        # -------------------------------------------------------------

        # State 1: FROTH_FREEZE (Highest Precedence)
        if is_hard_safety or is_froth:
            if is_hard_safety:
                reason_code = "HARD_RISK_FREEZE"
            elif mvrv_preservation or (
                mvrv_ratio is not None and mvrv_ratio > self.preservation_mvrv_gt
            ):
                reason_code = "CAPITAL_PRESERVATION_VALUATION"
            else:
                reason_code = "FROTH_FNG_MAYER"

            decision = PacingDecision(
                state=PacingState.FROTH_FREEZE,
                action="HOLD",
                pool=CapitalPool.BASE,
                requested_amount_usd=0.0,
                authorized_amount_usd=0.0,
                reason_code=reason_code,
                narrative=(
                    f"🔴 FROTH_FREEZE: Suspended accumulation ({reason_code}). Preserving capital."
                ),
            )
            return decision, sniper_armed

        # State 2: SNIPER_DEPLOYMENT
        if (is_dd or is_cap) and sniper_armed:
            new_sniper_armed = False  # Latch episode immediately

            if is_cap:
                reason_code = (
                    "CAPITULATION_EDGE_MVRV"
                    if (mvrv_ratio is not None and mvrv_ratio < self.capitulation_mvrv_lt)
                    else "CAPITULATION_EDGE_MAYER"
                )
            elif return_24h is not None and return_24h <= self.sniper_return_24h_lte:
                reason_code = "DRAWDOWN_EDGE_24H"
            elif drawdown_7d is not None and drawdown_7d <= self.sniper_drawdown_7d_lte:
                reason_code = "DRAWDOWN_EDGE_7D"
            else:
                reason_code = "DRAWDOWN_EDGE_7D"

            # Dynamic sniper order percentage: 20% to 35% from tactical reserve
            sniper_pct = self.calculate_dynamic_sniper_pct(
                return_24h=return_24h,
                drawdown_7d=drawdown_7d,
                is_capitulation=is_cap,
            )

            available_tactical = max(0.0, reserve_cash)
            requested_tactical = available_tactical * sniper_pct
            daily_cap = available_tactical * self.daily_tactical_cap_pct

            # 48-Hour Velocity Bound on Tactical Reserve (max 40% in 48 hours)
            opening_tactical = available_tactical + tactical_debits_48h
            remaining_48h_cap = max(
                0.0, (self.tactical_48h_cap_pct * opening_tactical) - tactical_debits_48h
            )

            authorized_tactical = min(
                requested_tactical,
                available_tactical,
                daily_cap,
                remaining_48h_cap,
            )
            authorized_tactical = round(authorized_tactical, 2)

            if authorized_tactical > 0.0:
                decision = PacingDecision(
                    state=PacingState.SNIPER_DEPLOYMENT,
                    action="BUY",
                    pool=CapitalPool.TACTICAL_RESERVE,
                    requested_amount_usd=round(requested_tactical, 2),
                    authorized_amount_usd=authorized_tactical,
                    reason_code=reason_code,
                    narrative=(
                        f"🎯 SNIPER_DEPLOYMENT: Dynamic sniper order {sniper_pct:.1%} "
                        f"(${authorized_tactical:,.2f}) deployed from "
                        f"Tactical Reserve ({reason_code})."
                    ),
                    is_sniper=True,
                    sniper_pct=sniper_pct,
                )
            else:
                decision = PacingDecision(
                    state=PacingState.SNIPER_DEPLOYMENT,
                    action="HOLD",
                    pool=CapitalPool.TACTICAL_RESERVE,
                    requested_amount_usd=round(requested_tactical, 2),
                    authorized_amount_usd=0.0,
                    reason_code=reason_code,
                    narrative=f"🎯 SNIPER_DEPLOYMENT: Clamped to $0.00 ({reason_code}).",
                    is_sniper=True,
                    sniper_pct=sniper_pct,
                )
            return decision, new_sniper_armed

        # Re-arm check: if not in drawdown, verify recovery hysteresis
        new_sniper_armed = sniper_armed
        if (
            not is_dd
            and not is_cap
            and (drawdown_7d is None or drawdown_7d > self.drawdown_rearm_7d)
            and (return_24h is None or return_24h > -0.02)
        ):
            new_sniper_armed = True

        # State 3: WEEKLY_CORE
        if is_weekly:
            available_base = max(0.0, base_cash)
            runway_weeks = (
                available_base / self.weekly_base_usd if self.weekly_base_usd > 0.0 else 0.0
            )

            requested_base = self.weekly_base_usd
            if runway_weeks < self.runway_min_weeks:
                requested_base *= self.low_runway_factor
                reason_code = "BASE_RUNWAY_REDUCTION"
            else:
                reason_code = "WEEKLY_CADENCE_DUE"

            authorized_base = min(requested_base, available_base)
            authorized_base = round(authorized_base, 2)

            if authorized_base > 0.0:
                decision = PacingDecision(
                    state=PacingState.WEEKLY_CORE,
                    action="BUY",
                    pool=CapitalPool.BASE,
                    requested_amount_usd=round(requested_base, 2),
                    authorized_amount_usd=authorized_base,
                    reason_code=reason_code,
                    narrative=(
                        f"📅 WEEKLY_CORE: Buy ${authorized_base:,.2f} from Base Cash "
                        f"({reason_code}, runway={runway_weeks:.1f}w)."
                    ),
                )
            else:
                decision = PacingDecision(
                    state=PacingState.WEEKLY_CORE,
                    action="HOLD",
                    pool=CapitalPool.BASE,
                    requested_amount_usd=round(requested_base, 2),
                    authorized_amount_usd=0.0,
                    reason_code=reason_code,
                    narrative="📅 WEEKLY_CORE: Zero Base cash available.",
                )
            return decision, new_sniper_armed

        # State 4: IDLE_CHOP (Lowest Precedence - Sideways Chop)
        decision = PacingDecision(
            state=PacingState.IDLE_CHOP,
            action="NO_ACTION",
            pool=CapitalPool.BASE,
            requested_amount_usd=0.0,
            authorized_amount_usd=0.0,
            reason_code="NO_EVENT_CHOP",
            narrative=(
                "⏸️ PACING GUARD: Sideways chop (NO_ACTION). "
                "Preserving capital without fee/cash burn."
            ),
        )
        return decision, new_sniper_armed
