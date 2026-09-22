/**
 * Portfolio View Controller (Portofolio Simulasi $1,000)
 * Manages Fincept-style portfolio KPIs, allocation donut chart, dual-curve equity chart, and order blotter.
 */

import { api } from '../api.js';
import { formatCurrency, formatBtc, formatPercent, formatNumber } from '../formatters.js';
import { state } from '../state.js';

export function initPortfolio() {
  loadPortfolioData();
}

export async function loadPortfolioData() {
  await Promise.all([
    fetchPortfolioSummary(),
    fetchPortfolioEquity(),
    fetchPortfolioTrades(),
  ]);
}

export async function fetchPortfolioSummary() {
  const data = await api.getPortfolio();
  if (!data) return;

  const totalEquity = Number(data.total_equity ?? 1000.0);
  const pnlUsd = Number(data.unrealized_pnl_usd ?? 0.0);
  const pnlPct = Number(data.unrealized_pnl_pct ?? 0.0);
  const outperformance = Number(data.outperformance_usd ?? 0.0);

  const equityEl = document.getElementById('pTotalEquity');
  if (equityEl) equityEl.textContent = formatCurrency(totalEquity);

  const pnlSign = pnlPct >= 0 ? '+' : '';
  const pnlColorClass = pnlPct >= 0 ? 'bg-emerald-500/15 text-emerald-400 border-emerald-500/25' : 'bg-rose-500/15 text-rose-400 border-rose-500/25';
  const roiBadge = document.getElementById('pRoiBadge');
  if (roiBadge) {
    roiBadge.textContent = `${pnlSign}${pnlPct.toFixed(2)}% ROI`;
    roiBadge.className = `text-[11px] px-2 py-0.5 rounded font-semibold border tabular ${pnlColorClass}`;
  }

  const pnlEl = document.getElementById('pUnrealizedPnl');
  if (pnlEl) pnlEl.textContent = `${pnlSign}${formatCurrency(pnlUsd)}`;

  const alphaSign = outperformance >= 0 ? '+' : '';
  const alphaEl = document.getElementById('pOutperformance');
  if (alphaEl) alphaEl.textContent = `${alphaSign}${formatCurrency(outperformance)} USD`;

  const btcBal = Number(data.btc_balance ?? 0.0);
  const btcVal = Number(data.btc_value_usd ?? 0.0);
  const tradesCount = Number(data.total_trades ?? 0);

  const balEl = document.getElementById('pBtcBalance');
  if (balEl) balEl.textContent = formatBtc(btcBal, 8);

  const valEl = document.getElementById('pBtcValueUsd');
  if (valEl) valEl.textContent = formatCurrency(btcVal);

  const tradesEl = document.getElementById('pTotalTrades');
  if (tradesEl) tradesEl.textContent = `${tradesCount} Transaksi`;

  const baseCash = Number(data.base_cash ?? 700.0);
  const reserveCash = Number(data.reserve_cash ?? 300.0);
  const totalCash = Number(data.total_cash ?? (baseCash + reserveCash));

  const cashEl = document.getElementById('pTotalCash');
  if (cashEl) cashEl.textContent = formatCurrency(totalCash);

  const baseEl = document.getElementById('pBaseCash');
  if (baseEl) baseEl.textContent = formatCurrency(baseCash);

  const resEl = document.getElementById('pReserveCash');
  if (resEl) resEl.textContent = formatCurrency(reserveCash);

  const avgBuyPrice = Number(data.avg_buy_price ?? 0.0);
  const spotPrice = Number(data.current_spot_price ?? 85000.0);
  const discountPct = Number(data.acquisition_discount_pct ?? 0.0);

  const avgBuyEl = document.getElementById('pAvgBuyPrice');
  if (avgBuyEl) avgBuyEl.textContent = avgBuyPrice > 0 ? formatCurrency(avgBuyPrice) : '$0.00';

  const spotEl = document.getElementById('pCurrentSpotPrice');
  if (spotEl) spotEl.textContent = formatCurrency(spotPrice);

  const discBadge = document.getElementById('pDiscountBadge');
  if (discBadge) {
    if (avgBuyPrice > 0) {
      if (discountPct >= 0) {
        discBadge.textContent = `${discountPct.toFixed(2)}% Diskon`;
        discBadge.className = 'text-[11px] px-2 py-0.5 rounded font-semibold bg-emerald-500/15 text-emerald-400 border border-emerald-500/25 tabular';
      } else {
        discBadge.textContent = `${Math.abs(discountPct).toFixed(2)}% Premi`;
        discBadge.className = 'text-[11px] px-2 py-0.5 rounded font-semibold bg-amber-500/15 text-amber-400 border border-amber-500/25 tabular';
      }
    } else {
      discBadge.textContent = 'Belum Ada Posisi';
      discBadge.className = 'text-[11px] px-2 py-0.5 rounded font-semibold bg-zinc-700/40 text-zinc-300 border border-zinc-600/30 tabular';
    }
  }

  const discEl = document.getElementById('pAcquisitionDiscount');
  if (discEl) discEl.textContent = `${discountPct.toFixed(2)}%`;

  updatePortfolioDonut(baseCash, reserveCash, btcVal);
}

export function updatePortfolioDonut(base, reserve, btc) {
  const total = base + reserve + btc;
  const basePct = total > 0 ? ((base / total) * 100).toFixed(1) : '70.0';
  const resPct = total > 0 ? ((reserve / total) * 100).toFixed(1) : '30.0';
  const btcPct = total > 0 ? ((btc / total) * 100).toFixed(1) : '0.0';

  const baseLabel = document.getElementById('donutBaseVal');
  if (baseLabel) baseLabel.textContent = `${formatCurrency(base)} (${basePct}%)`;

  const resLabel = document.getElementById('donutReserveVal');
  if (resLabel) resLabel.textContent = `${formatCurrency(reserve)} (${resPct}%)`;

  const btcLabel = document.getElementById('donutBtcVal');
  if (btcLabel) btcLabel.textContent = `${formatCurrency(btc)} (${btcPct}%)`;

  const canvas = document.getElementById('portfolioDonutChart');
  if (!canvas || typeof Chart === 'undefined') return;

  if (!state.charts.portfolioDonut) {
    state.charts.portfolioDonut = new Chart(canvas.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: ['Kas Utama (70%)', 'Cadangan Taktis (30%)', 'Bitcoin (BTC)'],
        datasets: [{
          data: [base, reserve, btc],
          backgroundColor: ['#3b82f6', '#10b981', '#f59e0b'],
          borderColor: '#090a0d',
          borderWidth: 2,
          hoverOffset: 4,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '72%',
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#181b24',
            titleColor: '#e4e4e7',
            bodyColor: '#a1a1aa',
            borderColor: 'rgba(255, 255, 255, 0.15)',
            borderWidth: 1,
            callbacks: {
              label: function(context) {
                const val = Number(context.raw);
                const pct = total > 0 ? ((val / total) * 100).toFixed(1) : 0;
                return ` ${formatCurrency(val)} (${pct}%)`;
              },
            },
          },
        },
      },
    });
  } else {
    state.charts.portfolioDonut.data.datasets[0].data = [base, reserve, btc];
    state.charts.portfolioDonut.update();
  }
}

export async function fetchPortfolioEquity() {
  const series = await api.getPortfolioEquity(90);
  if (!Array.isArray(series) || series.length === 0) return;
  renderPortfolioEquityChart(series);
}

export function renderPortfolioEquityChart(series) {
  const canvas = document.getElementById('portfolioEquityChart');
  if (!canvas || typeof Chart === 'undefined') return;
  const ctx = canvas.getContext('2d');

  const labels = series.map(d => d.date);
  const equities = series.map(d => d.equity);
  const benchmarks = series.map(d => d.benchmark);
  const reserves = series.map(d => d.reserve);

  if (!state.charts.portfolioEquity) {
    state.charts.portfolioEquity = new Chart(ctx, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [
          {
            label: 'Ekuitas Strategi ($)',
            data: equities,
            borderColor: '#10b981',
            backgroundColor: 'rgba(16, 185, 129, 0.08)',
            borderWidth: 2.5,
            fill: true,
            tension: 0.25,
            pointRadius: series.length > 30 ? 0 : 3,
            pointHoverRadius: 6,
            yAxisID: 'y',
            order: 1,
          },
          {
            label: 'Benchmark B&H $1,000 ($)',
            data: benchmarks,
            borderColor: '#c084fc',
            borderWidth: 2,
            borderDash: [5, 5],
            fill: false,
            tension: 0.25,
            pointRadius: 0,
            pointHoverRadius: 5,
            yAxisID: 'y',
            order: 2,
          },
          {
            label: 'Cadangan Kas ($)',
            data: reserves,
            borderColor: '#3b82f6',
            backgroundColor: 'rgba(59, 130, 246, 0.05)',
            borderWidth: 1.5,
            borderDash: [2, 2],
            fill: true,
            tension: 0.2,
            pointRadius: 0,
            yAxisID: 'y',
            order: 3,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: {
          mode: 'index',
          intersect: false,
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#181b24',
            titleColor: '#e4e4e7',
            bodyColor: '#a1a1aa',
            borderColor: 'rgba(255, 255, 255, 0.15)',
            borderWidth: 1,
            padding: 10,
            callbacks: {
              label: function(context) {
                return ` ${context.dataset.label}: ${formatCurrency(context.raw)}`;
              },
            },
          },
        },
        scales: {
          x: {
            grid: { color: 'rgba(255, 255, 255, 0.04)' },
            ticks: { color: '#71717a', font: { family: "'JetBrains Mono', monospace", size: 10 } },
          },
          y: {
            grid: { color: 'rgba(255, 255, 255, 0.06)' },
            ticks: {
              color: '#71717a',
              font: { family: "'JetBrains Mono', monospace", size: 10 },
              callback: function(v) { return `$${Number(v).toLocaleString('en-US')}`; },
            },
          },
        },
      },
    });
  } else {
    state.charts.portfolioEquity.data.labels = labels;
    state.charts.portfolioEquity.data.datasets[0].data = equities;
    state.charts.portfolioEquity.data.datasets[1].data = benchmarks;
    state.charts.portfolioEquity.data.datasets[2].data = reserves;
    state.charts.portfolioEquity.update();
  }
}

export async function fetchPortfolioTrades() {
  const trades = await api.getPortfolioTrades(50);
  if (!Array.isArray(trades)) return;
  renderPortfolioBlotter(trades);
}

export function renderPortfolioBlotter(trades) {
  const tbody = document.getElementById('portfolioBlotterBody');
  const countEl = document.getElementById('blotterCount');
  if (!tbody) return;

  if (countEl) countEl.textContent = trades.length;

  if (trades.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" class="py-8 text-center text-zinc-400">
          Belum ada transaksi forward paper trading yang tereksekusi. Jalankan <code class="text-amber-400 font-mono">bitcoin-data paper step</code> untuk mengeksekusi order harian pertama.
        </td>
      </tr>
    `;
    return;
  }

  const signalBadgeMap = {
    'AGGRESSIVE_ACCUMULATE': 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
    'OPPORTUNISTIC_ACCUMULATE': 'bg-blue-500/15 text-blue-400 border-blue-500/30',
    'STANDARD_DCA': 'bg-zinc-500/15 text-zinc-300 border-zinc-500/30',
    'DEFENSIVE_RESERVE': 'bg-amber-500/15 text-amber-400 border-amber-500/30',
    'HARD_FREEZE': 'bg-rose-500/15 text-rose-400 border-rose-500/30',
  };

  tbody.innerHTML = trades.map(t => {
    const sideClass = t.side === 'BUY' ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30' : 'bg-zinc-700/30 text-zinc-400 border-zinc-600/30';
    const signalClass = signalBadgeMap[t.signal] || 'bg-zinc-500/15 text-zinc-300 border-zinc-500/30';

    return `
      <tr class="hover:bg-white/[0.02] transition-colors">
        <td class="py-3 px-3 tabular text-zinc-300 font-mono text-[11px]">${t.date}</td>
        <td class="py-3 px-3">
          <span class="px-2 py-0.5 rounded text-[10px] font-semibold border ${signalClass}">
            ${t.signal}
          </span>
        </td>
        <td class="py-3 px-3">
          <span class="px-2 py-0.5 rounded text-[10px] font-bold border ${sideClass}">
            ${t.side}
          </span>
        </td>
        <td class="py-3 px-3 text-right tabular font-mono font-medium text-white">${formatCurrency(t.spot_price)}</td>
        <td class="py-3 px-3 text-right tabular font-mono text-zinc-200">${formatCurrency(t.gross_usd)}</td>
        <td class="py-3 px-3 text-right tabular font-mono text-zinc-400">$${Number(t.fee_usd).toFixed(4)}</td>
        <td class="py-3 px-3 text-right tabular font-mono text-amber-400 font-medium">${formatBtc(t.btc_amount, 8)} BTC</td>
        <td class="py-3 px-3 text-zinc-300 text-[11px] max-w-xs truncate" title="${t.narrative || ''}">${t.narrative || ''}</td>
      </tr>
    `;
  }).join('');
}
