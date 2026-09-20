"""Pre-trade risk gatekeeper inspired by institutional AutoHedge controls."""

from __future__ import annotations

from pathlib import Path

from bitcoin_data_platform.committee.invariant_solver import InvariantSolver
from bitcoin_data_platform.paper.models import PaperPortfolioBalance, RiskCheckResult


class RiskGuard:
    """Deterministic pre-trade risk validator enforcing capital safety invariants."""

    def __init__(
        self,
        kill_switch_path: Path | str = "data/state/PAPER_KILL_SWITCH",
        max_price_deviation_pct: float = 20.0,
        reject_on_macro: bool = False,
        invariant_solver: InvariantSolver | None = None,
    ) -> None:
        self.kill_switch_path = Path(kill_switch_path)
        self.max_price_deviation_pct = max_price_deviation_pct
        self.reject_on_macro = reject_on_macro
        self.invariant_solver = invariant_solver or InvariantSolver()

    def is_kill_switch_active(self) -> bool:
        """Check whether emergency filesystem kill-switch exists."""
        return self.kill_switch_path.exists()

    def activate_kill_switch(self, reason: str = "") -> None:
        """Activate emergency filesystem kill-switch."""
        self.kill_switch_path.parent.mkdir(parents=True, exist_ok=True)
        self.kill_switch_path.write_text(
            f"ACTIVATED: {reason or 'Emergency kill switch triggered'}\n"
        )

    def validate_trade(
        self,
        portfolio: PaperPortfolioBalance,
        side: str,
        gross_amount_usd: float,
        spot_price: float,
        macro_event: bool = False,
        reference_price: float | None = None,
        *,
        black_swan_flag: bool = False,
        composite_mni: float = 0.0,
        macro_event_proximity_minutes: int | None = None,
    ) -> RiskCheckResult:
        """Validate proposed trade against institutional safety bounds."""
        # 1. Emergency Kill-Switch Check
        if self.is_kill_switch_active():
            return RiskCheckResult(
                allowed=False,
                reason="Kill switch active",
                details={"kill_switch_path": str(self.kill_switch_path)},
            )

        # 2. Black Swan Emergency Halt
        if black_swan_flag:
            return RiskCheckResult(
                allowed=False,
                reason=(
                    "CIRCUIT_BREAKER_TRIPPED: Active Black Swan / "
                    "Critical Regulatory Sentinel alert"
                ),
                details={"black_swan_flag": True},
            )

        # 3. Macro Proximity Buffer (+/- 120 minutes)
        if macro_event_proximity_minutes is not None and abs(macro_event_proximity_minutes) <= 120:
            return RiskCheckResult(
                allowed=False,
                reason=(
                    f"CIRCUIT_BREAKER_TRIPPED: High-impact macro release window "
                    f"(+/- 2h, current: {macro_event_proximity_minutes}m)"
                ),
                details={"macro_event_proximity_minutes": macro_event_proximity_minutes},
            )

        # 4. Severe Macro Contraction Halt
        if composite_mni < -0.65:
            return RiskCheckResult(
                allowed=False,
                reason=(
                    f"CIRCUIT_BREAKER_TRIPPED: Severe Macro Liquidity Contraction "
                    f"(MNI = {composite_mni:.2f})"
                ),
                details={"composite_mni": composite_mni},
            )

        # 5. Spot Price Sanity Checks
        if spot_price <= 0.0:
            return RiskCheckResult(
                allowed=False,
                reason=f"Invalid spot price: {spot_price}",
                details={"spot_price": spot_price},
            )

        if reference_price is not None and reference_price > 0.0:
            deviation_pct = abs(spot_price - reference_price) / reference_price * 100.0
            if deviation_pct > self.max_price_deviation_pct:
                return RiskCheckResult(
                    allowed=False,
                    reason=(
                        f"Price deviation exceeded: {deviation_pct:.2f}% > "
                        f"{self.max_price_deviation_pct:.2f}%"
                    ),
                    details={
                        "spot_price": spot_price,
                        "reference_price": reference_price,
                        "deviation_pct": round(deviation_pct, 2),
                        "max_allowed_pct": self.max_price_deviation_pct,
                    },
                )

        # 6. Macro Event Policy Check
        if macro_event and self.reject_on_macro and side.upper() == "BUY":
            return RiskCheckResult(
                allowed=False,
                reason="High-impact macro event active (policy rejects buy orders)",
                details={"macro_event": True},
            )

        # 7. Solvency Checks
        if side.upper() == "BUY":
            if gross_amount_usd < 0.0:
                return RiskCheckResult(
                    allowed=False,
                    reason=f"Negative trade gross amount: {gross_amount_usd}",
                    details={"gross_amount_usd": gross_amount_usd},
                )

            available_cash = portfolio.total_cash
            # Tolerance of 1e-6 to avoid floating point imprecision issues
            if gross_amount_usd > available_cash + 1e-6:
                return RiskCheckResult(
                    allowed=False,
                    reason=(
                        f"Insufficient funds: gross ${gross_amount_usd:,.2f} "
                        f"exceeds available cash ${available_cash:,.2f}"
                    ),
                    details={
                        "gross_amount_usd": round(gross_amount_usd, 2),
                        "available_cash": round(available_cash, 2),
                    },
                )

        return RiskCheckResult(
            allowed=True,
            reason="All risk checks passed",
            details={
                "side": side,
                "gross_amount_usd": round(gross_amount_usd, 2),
                "macro_event": macro_event,
                "composite_mni": composite_mni,
            },
        )

    def validate_pre_trade(
        self,
        *,
        spot_price: float,
        last_known_price: float = 0.0,
        available_cash: float = 0.0,
        required_cash: float = 0.0,
        has_high_impact_macro_event: bool = False,
        black_swan_flag: bool = False,
        composite_mni: float = 0.0,
        macro_event_proximity_minutes: int | None = None,
        available_base_cash: float | None = None,
        available_reserve_cash: float | None = None,
        portfolio_drawdown: float = 0.0,
    ) -> RiskCheckResult:
        """Validate proposed pre-trade state against institutional bounds (AutoHedge style)."""
        # Invariant solver hook for neuro-symbolic verification
        base_cash = available_cash if available_base_cash is None else available_base_cash
        reserve_cash = 0.0 if available_reserve_cash is None else available_reserve_cash
        receipt = self.invariant_solver.solve_and_clamp(
            proposed_usd=required_cash,
            available_base_cash=base_cash,
            available_reserve_cash=reserve_cash,
            has_black_swan=black_swan_flag,
            macro_proximity_minutes=macro_event_proximity_minutes,
            portfolio_drawdown=portfolio_drawdown,
            spot_price=spot_price,
            last_validated_price=last_known_price,
        )

        # 1. Emergency Kill-Switch Check
        if self.is_kill_switch_active():
            return RiskCheckResult(
                allowed=False,
                reason="Kill switch active",
                details={
                    "kill_switch_path": str(self.kill_switch_path),
                    "clamping_receipt": receipt.to_dict(),
                },
            )

        # 2. Black Swan Emergency Halt
        has_bs = black_swan_flag or (
            receipt.is_clamped and any("Black Swan" in r for r in receipt.triggered_rules)
        )
        if has_bs:
            return RiskCheckResult(
                allowed=False,
                reason=(
                    "CIRCUIT_BREAKER_TRIPPED: Active Black Swan / "
                    "Critical Regulatory Sentinel alert"
                ),
                details={
                    "black_swan_flag": True,
                    "clamping_receipt": receipt.to_dict(),
                },
            )

        # 3. Macro Proximity Buffer (+/- 120 minutes around High Impact announcement)
        macro_in_window = (
            macro_event_proximity_minutes is not None and abs(macro_event_proximity_minutes) <= 120
        )
        macro_clamped = receipt.is_clamped and any(
            "Makro" in r or "Macro" in r for r in receipt.triggered_rules
        )
        if macro_in_window or macro_clamped:
            return RiskCheckResult(
                allowed=False,
                reason=(
                    f"CIRCUIT_BREAKER_TRIPPED: High-impact macro release window "
                    f"(+/- 2h, current: {macro_event_proximity_minutes}m)"
                ),
                details={
                    "macro_event_proximity_minutes": macro_event_proximity_minutes,
                    "clamping_receipt": receipt.to_dict(),
                },
            )

        # 4. Severe Macro Contraction Halt
        if composite_mni < -0.65:
            return RiskCheckResult(
                allowed=False,
                reason=(
                    f"CIRCUIT_BREAKER_TRIPPED: Severe Macro Liquidity Contraction "
                    f"(MNI = {composite_mni:.2f})"
                ),
                details={
                    "composite_mni": composite_mni,
                    "clamping_receipt": receipt.to_dict(),
                },
            )

        # 5. Spot Price Sanity Checks
        if spot_price <= 0.0:
            return RiskCheckResult(
                allowed=False,
                reason=f"Invalid spot price: {spot_price}",
                details={
                    "spot_price": spot_price,
                    "clamping_receipt": receipt.to_dict(),
                },
            )

        if last_known_price > 0.0:
            deviation_pct = abs(spot_price - last_known_price) / last_known_price * 100.0
            if deviation_pct > self.max_price_deviation_pct:
                return RiskCheckResult(
                    allowed=False,
                    reason=(
                        f"Price deviation exceeded: {deviation_pct:.2f}% > "
                        f"{self.max_price_deviation_pct:.2f}%"
                    ),
                    details={
                        "spot_price": spot_price,
                        "reference_price": last_known_price,
                        "deviation_pct": round(deviation_pct, 2),
                        "max_allowed_pct": self.max_price_deviation_pct,
                        "clamping_receipt": receipt.to_dict(),
                    },
                )

        # 6. Macro Event Policy Check
        if has_high_impact_macro_event and self.reject_on_macro and required_cash > 0.0:
            return RiskCheckResult(
                allowed=False,
                reason="High-impact macro event active (policy rejects buy orders)",
                details={
                    "has_high_impact_macro_event": True,
                    "clamping_receipt": receipt.to_dict(),
                },
            )

        # 7. Solvency Checks
        if required_cash < 0.0:
            return RiskCheckResult(
                allowed=False,
                reason=f"Negative trade gross amount: {required_cash}",
                details={
                    "required_cash": required_cash,
                    "clamping_receipt": receipt.to_dict(),
                },
            )

        if required_cash > available_cash + 1e-6:
            return RiskCheckResult(
                allowed=False,
                reason=(
                    f"Insufficient funds: gross ${required_cash:,.2f} "
                    f"exceeds available cash ${available_cash:,.2f}"
                ),
                details={
                    "gross_amount_usd": round(required_cash, 2),
                    "available_cash": round(available_cash, 2),
                    "clamping_receipt": receipt.to_dict(),
                },
            )

        return RiskCheckResult(
            allowed=True,
            reason="All risk checks passed",
            details={
                "spot_price": spot_price,
                "composite_mni": composite_mni,
                "black_swan_flag": black_swan_flag,
                "required_cash": round(required_cash, 2),
                "available_cash": round(available_cash, 2),
                "clamping_receipt": receipt.to_dict(),
            },
        )
