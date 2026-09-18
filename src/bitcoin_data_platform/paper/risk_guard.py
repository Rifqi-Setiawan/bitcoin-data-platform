"""Pre-trade risk gatekeeper inspired by institutional AutoHedge controls."""

from __future__ import annotations

from pathlib import Path

from bitcoin_data_platform.paper.models import PaperPortfolioBalance, RiskCheckResult


class RiskGuard:
    """Deterministic pre-trade risk validator enforcing capital safety invariants."""

    def __init__(
        self,
        kill_switch_path: Path | str = "data/state/PAPER_KILL_SWITCH",
        max_price_deviation_pct: float = 20.0,
        reject_on_macro: bool = False,
    ) -> None:
        self.kill_switch_path = Path(kill_switch_path)
        self.max_price_deviation_pct = max_price_deviation_pct
        self.reject_on_macro = reject_on_macro

    def is_kill_switch_active(self) -> bool:
        """Check whether emergency filesystem kill-switch exists."""
        return self.kill_switch_path.exists()

    def validate_trade(
        self,
        portfolio: PaperPortfolioBalance,
        side: str,
        gross_amount_usd: float,
        spot_price: float,
        macro_event: bool = False,
        reference_price: float | None = None,
    ) -> RiskCheckResult:
        """Validate proposed trade against institutional safety bounds.

        Args:
            portfolio: Current balance state of the portfolio.
            side: 'BUY' or 'HOLD'.
            gross_amount_usd: Total USD capital proposed for execution.
            spot_price: Execution price for BTC.
            macro_event: High-impact macro event flag for the trade date.
            reference_price: Optional baseline price (e.g. previous close) for deviation checks.

        Returns:
            RiskCheckResult indicating allowed flag, reason, and context details.
        """
        # 1. Emergency Kill-Switch Check
        if self.is_kill_switch_active():
            return RiskCheckResult(
                allowed=False,
                reason="Kill switch active",
                details={"kill_switch_path": str(self.kill_switch_path)},
            )

        # 2. Spot Price Sanity Checks
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

        # 3. Macro Event Policy Check
        if macro_event and self.reject_on_macro and side.upper() == "BUY":
            return RiskCheckResult(
                allowed=False,
                reason="High-impact macro event active (policy rejects buy orders)",
                details={"macro_event": True},
            )

        # 4. Solvency Checks
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
            },
        )
