"""Committee persona implementations: Macro Strategist, Valuation Analyst, and Risk Officer."""

from __future__ import annotations

import math
import uuid
from datetime import UTC, datetime
from typing import Any

from bitcoin_data_platform.committee.models import (
    CommitteePersona,
    MemberStance,
    PersonaVote,
)
from bitcoin_data_platform.intelligence.ingester import IntelligenceIngester


class MacroStrategistPersona:
    """Macro & Geopolitical Strategist evaluating global liquidity, rates, and user intelligence."""

    @staticmethod
    def evaluate(
        snapshot: dict[str, Any],
        user_alpha: list[dict[str, Any]] | None = None,
        base_budget: float = 10.0,
        memo_id: str = "",
    ) -> PersonaVote:
        mni = float(snapshot.get("composite_mni", 0.0))
        user_score = (
            IntelligenceIngester.compute_decayed_user_score(user_alpha) if user_alpha else 0.0
        )

        # Macro score formula: s_macro = 0.60 * MNI + 0.40 * UserAlphaPolarity
        s_macro = 0.60 * mni + 0.40 * math.tanh(user_score)
        s_macro = max(-1.0, min(1.0, s_macro))

        if s_macro >= 0.35:
            stance = MemberStance.BULLISH
            alloc = base_budget * 1.5
            conf = 0.85
            rationale = (
                f"Kondisi likuiditas makro akomodatif (MNI: {mni:+.2f}). "
                f"Alpha riset pengguna mengindikasikan katalis positif ({user_score:+.2f}). "
                "Disarankan alokasi akumulasi di atas baseline."
            )
        elif s_macro >= 0.10:
            stance = MemberStance.MODERATELY_BULLISH
            alloc = base_budget * 1.2
            conf = 0.80
            rationale = (
                f"Likuiditas makro stabil moderat (MNI: {mni:+.2f}). "
                "Sentimen pasar mendukung kelanjutan akumulasi terencana."
            )
        elif s_macro >= -0.20:
            stance = MemberStance.NEUTRAL
            alloc = base_budget * 1.0
            conf = 0.75
            rationale = (
                f"Regime makro berada pada zona netral/konsolidasi (MNI: {mni:+.2f}). "
                "Disarankan melanjutkan DCA standar secara disiplin."
            )
        elif s_macro >= -0.40:
            stance = MemberStance.DEFENSIVE
            alloc = base_budget * 0.5
            conf = 0.80
            rationale = (
                f"Tekanan likuiditas makro terdeteksi (MNI: {mni:+.2f}). "
                "Kurangi deploy taktis dan jaga likuiditas cadangan tunai."
            )
        else:
            stance = MemberStance.CRISIS
            alloc = 0.0
            conf = 0.90
            rationale = (
                f"Kontraksi likuiditas makro parah / shock ekonomi (MNI: {mni:+.2f}). "
                "Bekukan alokasi modal baru hingga kondisi makro stabil."
            )

        return PersonaVote(
            vote_id=uuid.uuid4().hex[:16],
            memo_id=memo_id,
            persona=CommitteePersona.MACRO_STRATEGIST,
            stance=stance,
            target_allocation_usd=round(alloc, 2),
            confidence=conf,
            rationale=rationale,
            voted_at_utc=datetime.now(UTC),
        )


class ValuationAnalystPersona:
    """On-chain & Fundamental Valuation Analyst evaluating MVRV and Mayer Multiple."""

    @staticmethod
    def evaluate(
        snapshot: dict[str, Any],
        base_budget: float = 10.0,
        memo_id: str = "",
    ) -> PersonaVote:
        mvrv = snapshot.get("mvrv_ratio")
        mm = snapshot.get("mayer_multiple")

        mvrv_val = float(mvrv) if mvrv is not None else 1.5
        mm_val = float(mm) if mm is not None else 1.0

        # MVRV scoring
        if mvrv_val < 1.0:
            s_mvrv = 1.0
        elif mvrv_val < 1.4:
            s_mvrv = 0.6
        elif mvrv_val < 2.0:
            s_mvrv = 0.1
        elif mvrv_val < 2.8:
            s_mvrv = -0.5
        else:
            s_mvrv = -1.0

        # Mayer Multiple scoring
        if mm_val < 0.8:
            s_mm = 1.0
        elif mm_val < 1.1:
            s_mm = 0.4
        elif mm_val < 1.5:
            s_mm = -0.1
        elif mm_val < 2.4:
            s_mm = -0.6
        else:
            s_mm = -1.0

        s_val = 0.50 * s_mvrv + 0.50 * s_mm

        if s_val >= 0.45:
            stance = MemberStance.BULLISH
            alloc = base_budget * 1.5
            conf = 0.90
            rationale = (
                f"Valuasi on-chain sangat murah (MVRV: {mvrv_val:.2f}, MM: {mm_val:.2f}). "
                "Harga berada jauh di bawah basis biaya historis; zona akumulasi prima."
            )
        elif s_val >= 0.15:
            stance = MemberStance.MODERATELY_BULLISH
            alloc = base_budget * 1.2
            conf = 0.85
            rationale = (
                f"Valuasi on-chain sehat (MVRV: {mvrv_val:.2f}, MM: {mm_val:.2f}). "
                "Akumulasi investor jangka panjang stabil."
            )
        elif s_val >= -0.20:
            stance = MemberStance.NEUTRAL
            alloc = base_budget * 1.0
            conf = 0.80
            rationale = (
                f"Valuasi berada di sekitar nilai wajar (MVRV: {mvrv_val:.2f}, MM: {mm_val:.2f}). "
                "Pertahankan laju alokasi normal."
            )
        elif s_val >= -0.55:
            stance = MemberStance.DEFENSIVE
            alloc = base_budget * 0.5
            conf = 0.85
            rationale = (
                f"Valuasi on-chain mulai panas (MVRV: {mvrv_val:.2f}, MM: {mm_val:.2f}). "
                "Risiko koreksi jangka menengah meningkat; batasi deployment agresif."
            )
        else:
            stance = MemberStance.CRISIS
            alloc = 0.0
            conf = 0.95
            rationale = (
                f"Valuasi on-chain di puncak euforia (MVRV: {mvrv_val:.2f}, MM: {mm_val:.2f}). "
                "Risiko penurunan tajam sangat tinggi; tahan pembelian."
            )

        return PersonaVote(
            vote_id=uuid.uuid4().hex[:16],
            memo_id=memo_id,
            persona=CommitteePersona.VALUATION_ANALYST,
            stance=stance,
            target_allocation_usd=round(alloc, 2),
            confidence=conf,
            rationale=rationale,
            voted_at_utc=datetime.now(UTC),
        )


class RiskOfficerPersona:
    """Technical Momentum & Risk Officer evaluating drawdowns, volatility, and black swans."""

    @staticmethod
    def evaluate(
        snapshot: dict[str, Any],
        portfolio_state: dict[str, Any] | None = None,
        base_budget: float = 10.0,
        memo_id: str = "",
    ) -> PersonaVote:
        port = portfolio_state or {}
        has_black_swan = bool(snapshot.get("black_swan_flag", False))
        drawdown_pct = float(port.get("drawdown_pct", 0.0))
        spot = float(snapshot.get("market_close_usd", 0.0))
        sma200 = float(snapshot.get("sma_200", 0.0))

        if has_black_swan:
            return PersonaVote(
                vote_id=uuid.uuid4().hex[:16],
                memo_id=memo_id,
                persona=CommitteePersona.RISK_OFFICER,
                stance=MemberStance.CRISIS,
                target_allocation_usd=0.0,
                confidence=0.99,
                rationale=(
                    "ANOMALI KRITIS: Black swan atau sentinel regulasi aktif. "
                    "Alokasi modal wajib dibekukan total."
                ),
                voted_at_utc=datetime.now(UTC),
            )

        price_vs_sma = (spot - sma200) / sma200 if (sma200 > 0.0 and spot > 0.0) else 0.0

        # Risk score calculation
        s_risk = 0.50 * max(-1.0, min(1.0, price_vs_sma)) - 0.50 * min(1.0, drawdown_pct)

        if drawdown_pct > 0.25:
            stance = MemberStance.DEFENSIVE
            alloc = base_budget * 0.4
            conf = 0.90
            rationale = (
                f"Drawdown portofolio menyentuh {drawdown_pct:.1%} (>25% batas toleransi). "
                "Pagu cadangan taktis dibekukan untuk mengamankan solvabilitas kas."
            )
        elif s_risk >= 0.20:
            stance = MemberStance.BULLISH
            alloc = base_budget * 1.3
            conf = 0.80
            rationale = (
                f"Momentum harga solid di atas SMA-200 ({price_vs_sma:+.1%}), "
                f"drawdown terkendali ({drawdown_pct:.1%}). Risiko eksekusi rendah."
            )
        elif s_risk >= -0.15:
            stance = MemberStance.NEUTRAL
            alloc = base_budget * 1.0
            conf = 0.85
            rationale = (
                f"Volatilitas normal dan drawdown berada pada ambang wajar ({drawdown_pct:.1%}). "
                "Parameter risiko membolehkan alokasi standar."
            )
        elif s_risk >= -0.45:
            stance = MemberStance.DEFENSIVE
            alloc = base_budget * 0.5
            conf = 0.85
            rationale = (
                f"Tekanan harga di bawah tren atau drawdown meningkat ({drawdown_pct:.1%}). "
                "Pertahankan sikap defensif dan batasi risiko drawdown lanjutan."
            )
        else:
            stance = MemberStance.CRISIS
            alloc = 0.0
            conf = 0.90
            rationale = (
                f"Risiko memburuk (DD: {drawdown_pct:.1%}, Deviasi: {price_vs_sma:+.1%}). "
                "Stop deployment sementara untuk proteksi modal."
            )

        return PersonaVote(
            vote_id=uuid.uuid4().hex[:16],
            memo_id=memo_id,
            persona=CommitteePersona.RISK_OFFICER,
            stance=stance,
            target_allocation_usd=round(alloc, 2),
            confidence=conf,
            rationale=rationale,
            voted_at_utc=datetime.now(UTC),
        )
