/**
 * Macro Radar View Controller
 * Manages Composite Macro-Narrative Index (MNI) gauge, economic calendar, and curated news feed.
 */

import { api } from '../api.js';
import { formatTimestamp, formatUtcTime } from '../formatters.js';

export function initMacro() {
  loadMacroData();
}

export async function loadMacroData() {
  await Promise.all([
    fetchMacroRadar(),
    fetchMacroNews(),
    fetchMacroCalendar(),
  ]);
}

export async function fetchMacroRadar() {
  const data = await api.getMacroRadar();
  if (!data) return;

  const mni = Number(data.composite_mni ?? 0.0);
  const mniEl = document.getElementById('macroMniScore');
  if (mniEl) mniEl.textContent = `${mni >= 0 ? '+' : ''}${mni.toFixed(2)}`;

  const regimeEl = document.getElementById('macroRegimeBadge');
  if (regimeEl) {
    regimeEl.textContent = data.regime_label || data.regime || 'NEUTRAL_CHOP';
    let badgeClass = 'bg-slate-500/20 text-slate-300 border-slate-500/30';
    if (data.regime === 'RISK_ON_EXPANSION') badgeClass = 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
    else if (data.regime === 'CAUTIOUS_BULL') badgeClass = 'bg-sky-500/20 text-sky-400 border-sky-500/30';
    else if (data.regime === 'RISK_OFF_DEFENSE') badgeClass = 'bg-amber-500/20 text-amber-400 border-amber-500/30';
    else if (data.regime === 'BLACK_SWAN_CRISIS') badgeClass = 'bg-rose-500/25 text-rose-400 border-rose-500/40 animate-pulse';
    regimeEl.className = `px-2.5 py-1 text-xs font-bold rounded-lg border tabular ${badgeClass}`;
  }

  const sentinelEl = document.getElementById('macroSentinelBadge');
  if (sentinelEl) {
    if (data.black_swan_flag) {
      sentinelEl.textContent = '🚨 BLACK SWAN AKTIF';
      sentinelEl.className = 'px-2 py-0.5 text-[11px] font-bold rounded bg-rose-500/20 text-rose-400 border border-rose-500/40 animate-pulse';
    } else {
      sentinelEl.textContent = '✅ AMAN (PASS)';
      sentinelEl.className = 'px-2 py-0.5 text-[11px] font-bold rounded bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
    }
  }

  const scores = data.scores || {};
  const hm = Number(scores.hard_macro ?? 0.0);
  const st = Number(scores.sentiment ?? 0.0);
  const nr = Number(scores.narrative ?? 0.0);

  const hmEl = document.getElementById('macroHardMacroScore');
  if (hmEl) hmEl.textContent = `${hm >= 0 ? '+' : ''}${hm.toFixed(2)}`;
  const stEl = document.getElementById('macroSentimentScore');
  if (stEl) stEl.textContent = `${st >= 0 ? '+' : ''}${st.toFixed(2)}`;
  const nrEl = document.getElementById('macroNarrativeScore');
  if (nrEl) nrEl.textContent = `${nr >= 0 ? '+' : ''}${nr.toFixed(2)}`;

  const pillarEl = document.getElementById('macroDominantPillar');
  if (pillarEl) pillarEl.textContent = data.dominant_pillar || 'GENERAL';

  const alertsEl = document.getElementById('macroCriticalAlerts');
  if (alertsEl) alertsEl.textContent = `${data.critical_alerts_count || 0}`;

  const summaryEl = document.getElementById('macroNarrativeSummary');
  if (summaryEl) summaryEl.textContent = data.narrative_summary || 'Tidak ada ringkasan naratif.';

  const updatedEl = document.getElementById('macroLastUpdated');
  if (updatedEl) updatedEl.textContent = formatUtcTime(data.last_updated_utc);
}

export async function fetchMacroNews() {
  const items = await api.getMacroNews(20);
  const tbody = document.getElementById('macroNewsBody');
  if (!tbody) return;

  if (!items || items.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="p-4 text-center text-zinc-500">Belum ada berita terkurasi yang tercatat.</td></tr>';
    return;
  }

  tbody.innerHTML = items.map(item => {
    const pubDate = formatTimestamp(item.published_utc);
    const pol = Number(item.polarity ?? 0.0);
    let polClass = 'text-zinc-400';
    if (pol > 0.2) polClass = 'text-emerald-400';
    else if (pol < -0.2) polClass = 'text-rose-400';

    const sev = item.severity || 'LOW';
    let sevBadge = 'bg-zinc-800 text-zinc-400 border-zinc-700';
    if (sev === 'CRITICAL') sevBadge = 'bg-rose-500/20 text-rose-400 border-rose-500/30 font-bold';
    else if (sev === 'HIGH') sevBadge = 'bg-amber-500/20 text-amber-400 border-amber-500/30';

    return `
      <tr class="hover:bg-white/[0.02] transition-colors border-b border-white/[0.04]">
        <td class="px-3 py-2.5 font-mono text-zinc-400 text-[11px] whitespace-nowrap">${pubDate}</td>
        <td class="px-3 py-2.5 font-semibold text-zinc-300 text-xs">${item.source}</td>
        <td class="px-3 py-2.5">
          <span class="px-1.5 py-0.5 text-[10px] font-mono rounded bg-white/[0.04] text-zinc-300 border border-white/[0.08]">${item.pillar}</span>
        </td>
        <td class="px-3 py-2.5 whitespace-nowrap">
          <span class="px-1.5 py-0.5 text-[10px] rounded border ${sevBadge}">${sev}</span>
          <span class="ml-1 text-[11px] font-mono ${polClass}">${pol >= 0 ? '+' : ''}${pol.toFixed(2)}</span>
        </td>
        <td class="px-3 py-2.5 text-zinc-200 text-xs font-medium max-w-md truncate" title="${item.title}">${item.title}</td>
        <td class="px-3 py-2.5 whitespace-nowrap text-right">
          <a href="${item.url}" target="_blank" rel="noopener noreferrer" class="text-sky-400 hover:text-sky-300 hover:underline font-mono text-[11px] inline-flex items-center gap-1">
            Buka Berita ↗
          </a>
        </td>
      </tr>
    `;
  }).join('');
}

export async function fetchMacroCalendar() {
  const items = await api.getMacroCalendar(7);
  const tbody = document.getElementById('macroCalendarBody');
  if (!tbody) return;

  if (!items || items.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" class="p-4 text-center text-zinc-500">Belum ada agenda makroekonomi yang tercatat.</td></tr>';
    return;
  }

  tbody.innerHTML = items.map(item => {
    let biasClass = 'bg-zinc-800 text-zinc-400 border-zinc-700';
    if (item.directional_bias === 'DOVISH') biasClass = 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30';
    else if (item.directional_bias === 'HAWKISH') biasClass = 'bg-rose-500/20 text-rose-400 border-rose-500/30';

    const surprise = item.surprise !== null && item.surprise !== undefined ? `${item.surprise >= 0 ? '+' : ''}${item.surprise}` : '-';

    return `
      <tr class="hover:bg-white/[0.02] transition-colors border-b border-white/[0.04]">
        <td class="px-3 py-2.5 font-mono text-zinc-400 text-[11px] whitespace-nowrap">${item.release_date} ${item.release_time_utc}</td>
        <td class="px-3 py-2.5 font-mono font-semibold text-zinc-300 text-xs">${item.country}</td>
        <td class="px-3 py-2.5 text-zinc-200 text-xs font-medium">${item.event_name}</td>
        <td class="px-3 py-2.5">
          <span class="px-1.5 py-0.5 text-[10px] font-bold rounded ${item.impact === 'High' ? 'bg-rose-500/20 text-rose-400 border border-rose-500/30' : 'bg-amber-500/20 text-amber-400 border border-amber-500/30'}">${item.impact}</span>
        </td>
        <td class="px-3 py-2.5 font-mono text-zinc-400 text-xs">${item.forecast !== null && item.forecast !== undefined ? item.forecast : '-'}</td>
        <td class="px-3 py-2.5 font-mono text-zinc-200 text-xs font-semibold">${item.actual !== null && item.actual !== undefined ? item.actual : '-'}</td>
        <td class="px-3 py-2.5 font-mono text-xs ${item.surprise && item.surprise > 0 ? 'text-amber-400' : 'text-zinc-300'}">${surprise}</td>
        <td class="px-3 py-2.5 whitespace-nowrap text-right">
          <span class="px-2 py-0.5 text-[10px] font-bold rounded border ${biasClass}">${item.directional_bias}</span>
        </td>
      </tr>
    `;
  }).join('');
}
