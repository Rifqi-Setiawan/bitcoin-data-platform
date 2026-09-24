"""Pure Python deterministic symbolic invariant solver and allocation clamp engine."""

from __future__ import annotations

from bitcoin_data_platform.committee.models import ClampingReceipt


class InvariantSolver:
    """Symbolic solver validating hard mathematical safety invariants before trade execution."""

    MAX_DAILY_RESERVE_PCT: float = 0.15  # Max 15% tactical reserve deployed per day (standard DCA)
    MAX_SNIPER_RESERVE_PCT: float = 0.35  # Max 35% tactical reserve for dynamic sniper mode
    MAX_48H_TACTICAL_PCT: float = 0.40  # Max 40% tactical reserve deployed in a 48h rolling window
    MIN_BASE_RUNWAY_WEEKS: int = 8  # Minimum 8 weeks base cash runway
    LOW_RUNWAY_FACTOR: float = 0.50  # 50% cut if runway < 8 weeks
    FROTH_FNG_THRESHOLD: int = 80
    FROTH_MAYER_THRESHOLD: float = 2.0
    MAX_DRAWDOWN_LIMIT: float = 0.25  # Freeze tactical pool if drawdown > 25%
    MACRO_BUFFER_MINUTES: int = 120  # +/- 2 hours around high-impact macro events
    MAX_PRICE_DEVIATION: float = 0.20  # 20% price reasonableness check

    def solve_and_clamp(
        self,
        *,
        proposed_usd: float,
        available_base_cash: float,
        available_reserve_cash: float,
        has_black_swan: bool = False,
        macro_proximity_minutes: int | None = None,
        portfolio_drawdown: float = 0.0,
        spot_price: float = 0.0,
        last_validated_price: float = 0.0,
        proposed_base_usd: float | None = None,
        proposed_tactical_usd: float | None = None,
        is_sniper: bool = False,
        tactical_debits_48h: float = 0.0,
        weekly_base_usd: float | None = None,
        fng_value: int | None = None,
        mayer_multiple: float | None = None,
    ) -> ClampingReceipt:
        """Validate 6 mathematical invariants and clamp proposed capital allocation.

        Invariants:
        1. Solvency: D <= C_base + C_reserve
        2. Daily Tactical Reserve Cap: D_tactical <= 0.15 * C_reserve
        3. Macro Proximity Circuit Breaker: |t_now - t_macro| <= 120m => Halt
        4. Black Swan / Critical Sentinel Halt: black_swan_flag == True => Halt
        5. Severe Drawdown Brake: Drawdown > 25% => D_tactical = 0
        6. Spot Price Freshness & Deviation Bounds: |Spot - LastClose| / LastClose <= 0.20
        """
        if proposed_usd <= 0.0:
            return ClampingReceipt(
                proposed_allocation_usd=0.0,
                clamped_allocation_usd=0.0,
                is_clamped=False,
                triggered_rules=[],
                explanation="Tidak ada alokasi modal yang diusulkan.",
            )

        triggered: list[str] = []

        # Invariant 4: Black Swan Sentinel Halt
        if has_black_swan:
            return ClampingReceipt(
                proposed_allocation_usd=proposed_usd,
                clamped_allocation_usd=0.0,
                is_clamped=True,
                triggered_rules=["CIRCUIT_BREAKER: Active Black Swan / Critical Sentinel"],
                explanation=(
                    "Sistem membekukan alokasi modal karena terdeteksi anomali kritis / black swan."
                ),
            )

        # Invariant 7: Overheat Lockout (Froth Freeze: FNG >= 80 and Mayer Multiple >= 2.0)
        if (
            fng_value is not None
            and fng_value >= self.FROTH_FNG_THRESHOLD
            and mayer_multiple is not None
            and mayer_multiple >= self.FROTH_MAYER_THRESHOLD
        ):
            return ClampingReceipt(
                proposed_allocation_usd=proposed_usd,
                clamped_allocation_usd=0.0,
                is_clamped=True,
                triggered_rules=["CIRCUIT_BREAKER: Overheat Lockout (FNG >= 80 & Mayer >= 2.0)"],
                explanation="Sistem membekukan alokasi modal karena kondisi pasar overheat/froth.",
            )

        # Invariant 3: Macro Proximity Buffer (+/- 120 minutes)
        if (
            macro_proximity_minutes is not None
            and abs(macro_proximity_minutes) <= self.MACRO_BUFFER_MINUTES
        ):
            return ClampingReceipt(
                proposed_allocation_usd=proposed_usd,
                clamped_allocation_usd=0.0,
                is_clamped=True,
                triggered_rules=["CIRCUIT_BREAKER: Proksimitas Rilis Makro Tinggi (+/- 2 jam)"],
                explanation=(
                    f"Jendela rilis makro berdampak tinggi aktif "
                    f"({macro_proximity_minutes} menit). Alokasi ditahan."
                ),
            )

        # Invariant 6: Price Freshness & Deviation Check
        if last_validated_price > 0.0 and spot_price > 0.0:
            dev = abs(spot_price - last_validated_price) / last_validated_price
            if dev > self.MAX_PRICE_DEVIATION:
                return ClampingReceipt(
                    proposed_allocation_usd=proposed_usd,
                    clamped_allocation_usd=0.0,
                    is_clamped=True,
                    triggered_rules=["SAFETY_BRAKE: Deviasi Harga Spot Melebihi 20%"],
                    explanation=f"Deviasi harga spot ({dev:.1%}) melampaui batas toleransi 20%.",
                )

        # Partition proposed allocation into Base DCA and Tactical Reserve components
        if proposed_base_usd is not None and proposed_tactical_usd is not None:
            base_req = proposed_base_usd
            tactical_req = proposed_tactical_usd
        elif available_reserve_cash > 0.0 and available_base_cash > 0.0:
            base_req = proposed_usd * 0.40
            tactical_req = max(0.0, proposed_usd - base_req)
        elif available_reserve_cash > 0.0 and available_base_cash <= 0.0:
            base_req = 0.0
            tactical_req = proposed_usd
        else:
            base_req = proposed_usd
            tactical_req = 0.0

        # Base runway guard (< 8 weeks runway halves weekly base buy)
        if weekly_base_usd is not None and weekly_base_usd > 0.0 and available_base_cash > 0.0:
            runway_weeks = available_base_cash / weekly_base_usd
            if runway_weeks < self.MIN_BASE_RUNWAY_WEEKS:
                reduced_target = weekly_base_usd * self.LOW_RUNWAY_FACTOR
                if base_req > reduced_target:
                    triggered.append(
                        f"BASE_RUNWAY_REDUCTION: Halved weekly buy to ${reduced_target:.2f} "
                        f"(runway {runway_weeks:.1f}w < {self.MIN_BASE_RUNWAY_WEEKS}w)"
                    )
                    base_req = reduced_target

        # Base allocation clamped by available base cash
        base_alloc = min(base_req, max(0.0, available_base_cash))

        # Invariant 5: Drawdown Limit on Tactical Reserve
        if portfolio_drawdown > self.MAX_DRAWDOWN_LIMIT:
            triggered.append(
                f"DRAWDOWN_LIMIT: Tactical reserve buying frozen due to "
                f">{self.MAX_DRAWDOWN_LIMIT:.0%} drawdown"
            )
            tactical_allowed = 0.0
        else:
            # Invariant 2: Tactical Reserve Cap (15% standard DCA, up to 35% for dynamic sniper)
            cap_pct = self.MAX_SNIPER_RESERVE_PCT if is_sniper else self.MAX_DAILY_RESERVE_PCT
            reserve_cap = max(0.0, available_reserve_cash * cap_pct)
            tactical_allowed = min(tactical_req, reserve_cap, max(0.0, available_reserve_cash))
            if tactical_allowed < tactical_req:
                rule_name = "SNIPER_RESERVE_CAP" if is_sniper else "DAILY_RESERVE_CAP"
                cap_str = f"{cap_pct:.0%}"
                triggered.append(f"{rule_name}: Clamped to {cap_str} limit (${reserve_cap:.2f})")

            # 48-Hour Velocity Bound on Tactical Reserve (max 40% in 48h)
            if (is_sniper or proposed_tactical_usd is not None) and available_reserve_cash > 0.0:
                opening_tactical = available_reserve_cash + tactical_debits_48h
                remaining_48h_cap = max(
                    0.0, (self.MAX_48H_TACTICAL_PCT * opening_tactical) - tactical_debits_48h
                )
                if tactical_allowed > remaining_48h_cap:
                    triggered.append(
                        f"TACTICAL_48H_VELOCITY_CAP: Clamped to limit (${remaining_48h_cap:.2f})"
                    )
                    tactical_allowed = remaining_48h_cap

        total_clamped = base_alloc + tactical_allowed

        # Invariant 1: Total Solvency Check
        total_cash = max(0.0, available_base_cash + available_reserve_cash)
        if total_clamped > total_cash:
            triggered.append("SOLVENCY: Clamped to available total cash")
            total_clamped = total_cash
        elif proposed_usd > total_cash:
            triggered.append(
                f"SOLVENCY: Proposed ${proposed_usd:.2f} exceeds total cash ${total_cash:.2f}"
            )

        is_clamped = total_clamped < (proposed_usd - 1e-6) or len(triggered) > 0
        explanation = "; ".join(triggered) if is_clamped else "Semua invariant simbolik terpenuhi."

        return ClampingReceipt(
            proposed_allocation_usd=proposed_usd,
            clamped_allocation_usd=round(total_clamped, 2),
            is_clamped=is_clamped,
            triggered_rules=triggered,
            explanation=explanation,
        )
