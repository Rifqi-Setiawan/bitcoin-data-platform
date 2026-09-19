"""Pure Python deterministic symbolic invariant solver and allocation clamp engine."""

from __future__ import annotations

from bitcoin_data_platform.committee.models import ClampingReceipt


class InvariantSolver:
    """Symbolic solver validating hard mathematical safety invariants before trade execution."""

    MAX_DAILY_RESERVE_PCT: float = 0.15  # Max 15% of tactical reserve pool deployed per day
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
            # Invariant 2: Daily 15% Tactical Reserve Cap
            reserve_cap = max(0.0, available_reserve_cash * self.MAX_DAILY_RESERVE_PCT)
            tactical_allowed = min(tactical_req, reserve_cap, max(0.0, available_reserve_cash))
            if tactical_allowed < tactical_req:
                triggered.append(f"DAILY_RESERVE_CAP: Clamped to 15% limit (${reserve_cap:.2f})")

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
