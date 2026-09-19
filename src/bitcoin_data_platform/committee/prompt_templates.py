"""Prompt templates, Bahasa Indonesia executive summary generators, and Markdown renderers."""

from __future__ import annotations

from datetime import date
from typing import Any

from bitcoin_data_platform.committee.models import (
    AllocationAction,
    ClampingReceipt,
    PersonaVote,
)


def extract_dissenting_opinions(votes: list[PersonaVote]) -> str:
    """Extract notable disagreements among committee personas."""
    if len(votes) < 2:
        return "Tidak ada pandangan berbeda yang dicatat."

    stances = {v.persona.value: v.stance.value for v in votes}
    unique_stances = set(stances.values())

    if len(unique_stances) <= 1:
        return "Seluruh anggota komite sepakat dalam pandangan pasar yang sejalan."

    dissent_lines: list[str] = []
    for v in votes:
        if v.stance.value in ("DEFENSIVE", "CRISIS") and any(
            x in ("BULLISH", "MODERATELY_BULLISH") for x in unique_stances
        ):
            dissent_lines.append(
                f"{v.persona.replace('_', ' ').title()} bersikap {v.stance.value} "
                f'dan mengimbau kehati-hatian: "{v.rationale}"'
            )
        elif v.stance.value in ("BULLISH", "MODERATELY_BULLISH") and any(
            x in ("DEFENSIVE", "CRISIS") for x in unique_stances
        ):
            dissent_lines.append(
                f"{v.persona.replace('_', ' ').title()} melihat peluang akumulasi "
                f'({v.stance.value}): "{v.rationale}"'
            )

    if not dissent_lines:
        items_str = ", ".join(f"{k}: {v}" for k, v in stances.items())
        return f"Terdapat variasi stance antar anggota: {items_str}."

    return " | ".join(dissent_lines)


def generate_executive_summary_id(
    target_date: date,
    market_regime: str,
    consensus_score: float,
    proposed_action: AllocationAction,
    receipt: ClampingReceipt,
    votes: list[PersonaVote],
    user_alpha_count: int = 0,
) -> str:
    """Generate concise institutional executive summary in Bahasa Indonesia."""
    action_map = {
        AllocationAction.AGGRESSIVE_ACCUMULATE: "Akumulasi Agresif",
        AllocationAction.OPPORTUNISTIC_BUY: "Beli Oportunistik",
        AllocationAction.STANDARD_DCA: "DCA Standar",
        AllocationAction.DEFENSIVE_HOLD: "Tahan Defensif",
        AllocationAction.EMERGENCY_HALT: "Pembekuan Darurat (Circuit Breaker)",
    }
    action_str = action_map.get(proposed_action, proposed_action.value)
    t_str = target_date.isoformat()

    summary_parts: list[str] = [
        f"Komite Investasi merekomendasikan stance '{action_str}' untuk siklus perdagangan {t_str} "
        f"dengan skor konsensus {consensus_score:+.2f} pada regime {market_regime}."
    ]

    if user_alpha_count > 0:
        summary_parts.append(
            f"Pertimbangan mencakup {user_alpha_count} catatan inteligensi riset pengguna aktif."
        )

    if receipt.is_clamped:
        summary_parts.append(
            f"RiskGuard membatasi modal dari ${receipt.proposed_allocation_usd:,.2f} "
            f"menjadi ${receipt.clamped_allocation_usd:,.2f} ({receipt.explanation})."
        )
    else:
        summary_parts.append(
            f"Seluruh invariant RiskGuard terpenuhi; modal ${receipt.clamped_allocation_usd:,.2f} "
            "disetujui tanpa pembatasan."
        )

    return " ".join(summary_parts)


def render_memorandum_markdown(
    target_date: date,
    market_regime: str,
    composite_mni: float,
    consensus_score: float,
    proposed_action: AllocationAction,
    receipt: ClampingReceipt,
    votes: list[PersonaVote],
    executive_summary: str,
    dissent: str,
    snapshot: dict[str, Any],
) -> str:
    """Render complete institutional Investment Memorandum in Markdown format."""
    spot = snapshot.get("market_close_usd", 0.0)
    mvrv = snapshot.get("mvrv_ratio", "N/A")
    fng = snapshot.get("fng_value", "N/A")

    lines = [
        f"# Institutional Investment Committee Memorandum — {target_date.isoformat()}",
        "",
        "## Ringkasan Eksekutif",
        f"> {executive_summary}",
        "",
        "## Parameter Pasar & Snapshot Kuantitatif",
        f"- **Tanggal Perdagangan (UTC):** `{target_date.isoformat()}`",
        f"- **Harga Spot Terakhir:** `${float(spot):,.2f}`" if spot else "- **Harga Spot:** `N/A`",
        f"- **Regime Makro (MNI):** `{market_regime}` (`{composite_mni:+.2f}`)",
        f"- **Rasio On-Chain MVRV:** `{mvrv}` | **Fear & Greed Index:** `{fng}`",
        f"- **Skor Konsensus Komite:** `{consensus_score:+.2f}`",
        f"- **Rekomendasi Aksi:** `{proposed_action.value}`",
        "",
        "## Suara & Tesis Persona Komite",
    ]

    for v in votes:
        lines.extend(
            [
                f"### {v.persona.value.replace('_', ' ').title()}",
                f"- **Sikap (Stance):** `{v.stance.value}`",
                f"- **Keyakinan (Confidence):** `{v.confidence:.0%}`",
                f"- **Rekomendasi Alokasi Ideal:** `${v.target_allocation_usd:,.2f}`",
                f"- **Tesis:** {v.rationale}",
                "",
            ]
        )

    clamp_label = "DIBATASI (CLAMPED)" if receipt.is_clamped else "LULUS PENUH (UNCLAMPED)"
    rules_str = ", ".join(receipt.triggered_rules) if receipt.triggered_rules else "Tidak ada"

    lines.extend(
        [
            "## Pandangan Berbeda (Dissenting Views)",
            f"{dissent}",
            "",
            "## Verifikasi Neuro-Simbolik (Pre-Trade RiskGuard)",
            f"- **Alokasi Diusulkan Neural:** `${receipt.proposed_allocation_usd:,.2f}`",
            f"- **Alokasi Disetujui Simbolik:** `${receipt.clamped_allocation_usd:,.2f}`",
            f"- **Status Clamping:** `{clamp_label}`",
            f"- **Aturan Terpicu:** {rules_str}",
            f"- **Justifikasi Invariant:** {receipt.explanation}",
            "",
            "---",
            "*Dokumen dihasilkan otomatis oleh Dual-System Neuro-Symbolic Engine.*",
        ]
    )

    return "\n".join(lines)
