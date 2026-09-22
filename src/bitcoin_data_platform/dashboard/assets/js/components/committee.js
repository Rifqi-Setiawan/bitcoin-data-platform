/**
 * Committee View Controller (Komite Investasi)
 * Manages AI deliberation memo, RiskGuard invariants, persona voting cards,
 * user intelligence injection, and pipeline scheduling status.
 */

import { api } from '../api.js';
import { formatCurrency, formatTimestamp } from '../formatters.js';

export function initCommittee() {
  loadCommitteeData();
}

export async function loadCommitteeData() {
  await Promise.all([
    fetchCommitteeLatest(),
    fetchUserIntelligenceList(),
    fetchPipelineSchedule(),
  ]);
}

export async function fetchCommitteeLatest() {
  const memo = await api.getCommitteeLatest();
  if (!memo) return;

  const dateEl = document.getElementById('commMemoDate');
  if (dateEl) dateEl.innerText = memo.memo_date || 'N/A';

  const regimeEl = document.getElementById('commRegimeBadge');
  if (regimeEl) regimeEl.innerText = memo.market_regime || 'NEUTRAL_CHOP';

  const scoreEl = document.getElementById('commConsensusScore');
  if (scoreEl) {
    const score = Number(memo.consensus_score ?? 0.0);
    scoreEl.innerText = (score >= 0 ? '+' : '') + score.toFixed(2);
  }

  const actionEl = document.getElementById('commActionBadge');
  if (actionEl) actionEl.innerText = memo.proposed_action || 'STANDARD_DCA';

  const sumEl = document.getElementById('commExecutiveSummary');
  if (sumEl) sumEl.innerText = memo.executive_summary_id || 'Belum ada memorandum tersimpan.';

  const neuralEl = document.getElementById('commNeuralAlloc');
  if (neuralEl) neuralEl.innerText = formatCurrency(memo.proposed_allocation_usd ?? 0.0);

  const execEl = document.getElementById('commExecutedAlloc');
  if (execEl) execEl.innerText = formatCurrency(memo.clamped_allocation_usd ?? 0.0);

  const clampedBadge = document.getElementById('commClampedBadge');
  if (clampedBadge) {
    clampedBadge.innerText = memo.allocation_clamped ? 'CLAMPED' : 'PASSED';
    clampedBadge.className = memo.allocation_clamped
      ? 'px-2 py-0.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-400 border border-amber-500/30'
      : 'px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
  }

  const reasonEl = document.getElementById('commClampingReason');
  if (reasonEl) reasonEl.innerText = memo.clamping_reason || 'Semua invariant simbolik terpenuhi.';

  const personaGrid = document.getElementById('personaCardsGrid');
  if (personaGrid && Array.isArray(memo.votes)) {
    personaGrid.innerHTML = memo.votes.map(v => {
      const pTitle = (v.persona || '').replace('_', ' ');
      const stanceClass = (v.stance === 'BULLISH' || v.stance === 'MODERATELY_BULLISH')
        ? 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20'
        : ((v.stance === 'DEFENSIVE' || v.stance === 'CRISIS')
          ? 'text-rose-400 bg-rose-500/10 border-rose-500/20'
          : 'text-zinc-300 bg-zinc-800/40 border-white/[0.06]');
      const confPct = Math.round((v.confidence || 0) * 100);

      return `
        <div class="p-4 rounded-xl bg-[#0F1218] border border-white/[0.07] space-y-3">
          <div class="flex items-center justify-between pb-2 border-b border-white/[0.05]">
            <div class="font-bold text-white text-xs uppercase tracking-wide">${pTitle}</div>
            <span class="px-2 py-0.5 rounded text-[10px] font-semibold border ${stanceClass}">${v.stance}</span>
          </div>
          <div class="flex items-baseline justify-between tabular">
            <span class="text-[11px] text-zinc-400">Target Alokasi:</span>
            <span class="font-mono font-bold text-white text-sm">${formatCurrency(v.target_allocation_usd ?? 0)}</span>
          </div>
          <div class="space-y-1">
            <div class="flex justify-between text-[10px] text-zinc-400">
              <span>Keyakinan (Confidence)</span>
              <span class="font-mono font-bold text-purple-400">${confPct}%</span>
            </div>
            <div class="w-full bg-zinc-800 rounded-full h-1.5 overflow-hidden">
              <div class="bg-purple-500 h-1.5 rounded-full" style="width: ${confPct}%"></div>
            </div>
          </div>
          <p class="text-[11px] text-zinc-300 leading-relaxed pt-1 border-t border-white/[0.04]">${v.rationale || ''}</p>
        </div>
      `;
    }).join('');
  }

  const invBody = document.getElementById('invariantsTableBody');
  if (invBody && Array.isArray(memo.invariants_checked)) {
    invBody.innerHTML = memo.invariants_checked.map(inv => {
      const isOk = inv.status === 'PASSED';
      const badgeClass = isOk
        ? 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20'
        : (inv.status === 'CLAMPED'
          ? 'text-amber-400 bg-amber-500/10 border-amber-500/20'
          : 'text-rose-400 bg-rose-500/10 border-rose-500/20');

      let limitDesc = 'Mathematically Checked';
      if (inv.name === 'Daily 15% Reserve Cap') limitDesc = 'Max 15% Pool / 24h';
      else if (inv.name === 'Macro 2h Proximity Buffer') limitDesc = '±120 min rilis High-Impact';
      else if (inv.name === 'Max Drawdown Limit (<25%)') limitDesc = 'MDD <= 25%';

      return `
        <tr class="hover:bg-white/[0.02]">
          <td class="py-2.5 px-3 font-semibold text-white">${inv.name}</td>
          <td class="py-2.5 px-3 text-zinc-400 font-mono text-[11px]">${limitDesc}</td>
          <td class="py-2.5 px-3"><span class="px-2 py-0.5 rounded text-[10px] font-bold border ${badgeClass}">${inv.status}</span></td>
          <td class="py-2.5 px-3 text-zinc-400 text-[11px]">${inv.detail || (isOk ? 'Memenuhi invariant batas risiko' : 'Batas terpicu')}</td>
        </tr>
      `;
    }).join('');
  }
}

export async function fetchUserIntelligenceList() {
  const items = await api.getIntelligenceList(20);
  const tbody = document.getElementById('intelligenceBlotterBody');
  if (!tbody) return;

  if (!Array.isArray(items) || items.length === 0) {
    tbody.innerHTML = `<tr><td colspan="5" class="py-4 text-center text-zinc-500">Belum ada user intelligence tersimpan. Klik "Ingest Intelligence" untuk menambahkan.</td></tr>`;
    return;
  }

  tbody.innerHTML = items.map(it => {
    const sent = Number(it.sentiment_bias ?? 0.0);
    const sentClass = sent > 0 ? 'text-emerald-400' : (sent < 0 ? 'text-rose-400' : 'text-zinc-400');
    const sentStr = (sent >= 0 ? '+' : '') + sent.toFixed(2);
    const confStr = Math.round(Number(it.confidence_score ?? 0.8) * 100) + '%';
    const link = it.source_url
      ? `<a href="${it.source_url}" target="_blank" rel="noopener noreferrer" class="text-sky-400 hover:underline inline-flex items-center gap-1 font-mono text-[11px]">Buka Tautan ↗</a>`
      : `<span class="text-zinc-500 font-mono text-[11px]">-</span>`;

    return `
      <tr class="hover:bg-white/[0.02]">
        <td class="py-2.5 px-3 font-mono text-[11px] text-zinc-400">
          <div class="text-white font-semibold">${it.intelligence_id || ''}</div>
          <div class="text-[10px]">${(it.created_at_utc || '').substring(0, 16).replace('T', ' ')}</div>
        </td>
        <td class="py-2.5 px-3">
          <span class="px-2 py-0.5 rounded text-[10px] font-semibold bg-purple-500/10 text-purple-400 border border-purple-500/20">${it.pillar || 'USER_THESIS'}</span>
        </td>
        <td class="py-2.5 px-3 space-y-0.5">
          <div class="font-semibold text-white text-xs">${it.title || 'Note'}</div>
          <div class="text-zinc-400 text-[11px] line-clamp-2">${it.user_thesis || ''}</div>
        </td>
        <td class="py-2.5 px-3 font-mono text-[11px]">
          <div class="${sentClass} font-bold">${sentStr}</div>
          <div class="text-[10px] text-zinc-400">Conf: ${confStr}</div>
        </td>
        <td class="py-2.5 px-3 text-right">${link}</td>
      </tr>
    `;
  }).join('');
}

export async function fetchPipelineSchedule() {
  const data = await api.getPipelineSchedule();
  const container = document.getElementById('pipelineCadenceCards');
  if (!container || !data || !data.cadences) return;

  const cadences = [
    { key: 'hourly', title: 'Hourly Pipeline (*:05 UTC)', icon: '⏱️', desc: data.cadences.hourly?.description || '' },
    { key: 'daily', title: 'Daily Pipeline (00:05 UTC)', icon: '📅', desc: data.cadences.daily?.description || '' },
    { key: 'weekly', title: 'Weekly Pipeline (Mon 01:00 UTC)', icon: '📊', desc: data.cadences.weekly?.description || '' },
  ];

  container.innerHTML = cadences.map(c => {
    const lockInfo = data.locks?.[c.key] || {};
    const isLocked = Boolean(lockInfo.locked);
    const statusText = isLocked ? 'AKTIF BERJALAN' : 'STANDBY IDLE';
    const badgeClass = isLocked ? 'bg-amber-500/20 text-amber-400 border-amber-500/30' : 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
    return `
      <div class="p-4 rounded-xl bg-[#12151E] border border-white/[0.06] space-y-2">
        <div class="flex items-center justify-between">
          <span class="text-sm font-bold text-white flex items-center gap-1.5"><span>${c.icon}</span> ${c.title}</span>
          <span class="px-2 py-0.5 rounded text-[10px] font-bold border ${badgeClass}">${statusText}</span>
        </div>
        <p class="text-[11px] text-zinc-400 leading-relaxed">${c.desc}</p>
        <div class="pt-2 border-t border-white/[0.05] text-[10px] font-mono text-zinc-500">
          Lock: ${isLocked ? 'PID ' + lockInfo.active_pid : 'Idle'}
        </div>
      </div>
    `;
  }).join('');
}

export function toggleIntelModal(show) {
  const m = document.getElementById('intelModal');
  if (m) {
    if (show) m.classList.remove('hidden');
    else m.classList.add('hidden');
  }
}

export async function submitUserIntelligence() {
  const titleEl = document.getElementById('intelTitle');
  const thesisEl = document.getElementById('intelThesis');
  const urlEl = document.getElementById('intelUrl');
  const pillarEl = document.getElementById('intelPillar');
  const confEl = document.getElementById('intelConfidence');
  const sentEl = document.getElementById('intelSentiment');
  const tagsEl = document.getElementById('intelTags');
  const statusEl = document.getElementById('intelFormStatus');

  const title = titleEl ? titleEl.value.trim() : '';
  const thesis = thesisEl ? thesisEl.value.trim() : '';
  const url = urlEl ? urlEl.value.trim() : '';
  const pillar = pillarEl ? pillarEl.value : 'USER_THESIS';
  const confidence = confEl ? (parseFloat(confEl.value) || 0.8) : 0.8;
  const sentiment = sentEl ? (parseFloat(sentEl.value) || 0.0) : 0.0;
  const tags = tagsEl ? tagsEl.value.trim() : '';

  if (!thesis) {
    if (statusEl) {
      statusEl.innerText = 'Tesis analitis wajib diisi.';
      statusEl.classList.remove('hidden');
    }
    return;
  }

  try {
    const resp = await api.postIntelligence({
      title: title || 'User Alpha Note',
      user_thesis: thesis,
      source_url: url || null,
      pillar: pillar,
      confidence_score: confidence,
      sentiment_bias: sentiment,
      tags: tags,
    });

    if (!resp.ok) {
      const errData = await resp.json().catch(() => ({}));
      if (statusEl) {
        statusEl.innerText = 'Gagal: ' + (errData.message || 'Error server');
        statusEl.classList.remove('hidden');
      }
      return;
    }

    toggleIntelModal(false);
    if (titleEl) titleEl.value = '';
    if (thesisEl) thesisEl.value = '';
    if (urlEl) urlEl.value = '';
    if (tagsEl) tagsEl.value = '';
    if (statusEl) statusEl.classList.add('hidden');
    await fetchUserIntelligenceList();
  } catch (err) {
    if (statusEl) {
      statusEl.innerText = 'Koneksi gagal: ' + err;
      statusEl.classList.remove('hidden');
    }
  }
}
