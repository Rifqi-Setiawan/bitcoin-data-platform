/**
 * Market View Controller (Ringkasan Pasar)
 * Manages KPI cards, Chart.js line/bar toggle, live trade blotter, and 30-day history table.
 */

import { api } from '../api.js';
import { formatCurrency, formatPercent, formatNumber } from '../formatters.js';
import { state, initialBtcTrades, initialEthTrades } from '../state.js';

export function initMarket() {
  initMarketChart();
  renderLiveTrades();
  renderHistoryTable();

  fetchKpiData(state.currentAsset);
  fetchChartData(state.currentAsset, state.currentTimeframe);
  fetchTradesData(state.currentAsset);
  fetchLedgerData(state.currentAsset);
}

export function refreshMarket() {
  fetchKpiData(state.currentAsset);
  if (!state.streamPaused) {
    fetchTradesData(state.currentAsset);
  }
}

export function initMarketChart() {
  if (typeof Chart === 'undefined') {
    setTimeout(initMarketChart, 200);
    return;
  }
  const canvas = document.getElementById('marketChart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dataSet = state.assetData[state.currentAsset];
  const series = state.currentTimeframe === '24H' ? dataSet.chartData24H : dataSet.chartData30D;

  const labels = series.map(d => d.date);
  const prices = series.map(d => d.price);
  const volumes = series.map(d => d.volume);

  const gradient = ctx.createLinearGradient(0, 0, 0, 360);
  gradient.addColorStop(0, 'rgba(16, 185, 129, 0.25)');
  gradient.addColorStop(1, 'rgba(16, 185, 129, 0.00)');

  state.charts.marketChart = new Chart(ctx, {
    type: state.chartViewMode === 'line' ? 'line' : 'bar',
    data: {
      labels: labels,
      datasets: [
        {
          type: state.chartViewMode === 'line' ? 'line' : 'bar',
          label: `Harga ${state.currentAsset} (USD)`,
          data: prices,
          borderColor: '#10b981',
          backgroundColor: state.chartViewMode === 'line' ? gradient : 'rgba(16, 185, 129, 0.70)',
          borderWidth: 3,
          fill: state.chartViewMode === 'line',
          tension: 0.30,
          pointRadius: state.chartViewMode === 'line' ? 4 : 0,
          pointHoverRadius: 7,
          pointBackgroundColor: '#34d399',
          pointBorderColor: '#090a0d',
          pointBorderWidth: 2,
          yAxisID: 'y',
          order: 1,
        },
        {
          type: 'bar',
          label: `Volume (${state.currentAsset})`,
          data: state.showVolume ? volumes : [],
          backgroundColor: 'rgba(255, 255, 255, 0.08)',
          hoverBackgroundColor: 'rgba(52, 211, 153, 0.30)',
          borderRadius: 3,
          yAxisID: 'yVolume',
          order: 2,
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
          boxPadding: 4,
          usePointStyle: true,
          callbacks: {
            label: function(context) {
              if (context.dataset.yAxisID === 'y') {
                return ` Harga: $${context.raw.toLocaleString('en-US', { minimumFractionDigits: 2 })}`;
              } else {
                return ` Volume: ${context.raw.toLocaleString('en-US')} ${state.currentAsset}`;
              }
            },
          },
        },
      },
      scales: {
        x: {
          grid: { color: 'rgba(255, 255, 255, 0.04)' },
          ticks: { color: '#71717a', font: { size: 11, family: 'Inter' } },
        },
        y: {
          position: 'left',
          grace: '8%',
          grid: { color: 'rgba(255, 255, 255, 0.05)' },
          ticks: {
            color: '#34d399',
            font: { size: 11, family: 'JetBrains Mono' },
            callback: value => '$' + Number(value).toLocaleString(),
          },
        },
        yVolume: {
          position: 'right',
          beginAtZero: true,
          grid: { display: false },
          ticks: {
            color: '#71717a',
            font: { size: 10, family: 'JetBrains Mono' },
            callback: value => (value >= 1000 ? (value / 1000).toFixed(1) + 'k' : value),
          },
          max: Math.max(...volumes) * 3,
        },
      },
    },
  });
}

export function updateMarketChart() {
  const chartInstance = state.charts.marketChart;
  if (!chartInstance) return;
  const dataSet = state.assetData[state.currentAsset];
  const series = state.currentTimeframe === '24H' ? dataSet.chartData24H : dataSet.chartData30D;

  chartInstance.data.labels = series.map(d => d.date);
  chartInstance.data.datasets[0].data = series.map(d => d.price);
  chartInstance.data.datasets[0].label = `Harga ${state.currentAsset} (USD)`;
  chartInstance.data.datasets[0].type = state.chartViewMode === 'line' ? 'line' : 'bar';
  chartInstance.data.datasets[0].fill = state.chartViewMode === 'line';

  const volumes = series.map(d => d.volume);
  chartInstance.data.datasets[1].data = state.showVolume ? volumes : [];
  chartInstance.data.datasets[1].label = `Volume (${state.currentAsset})`;
  chartInstance.options.scales.yVolume.max = Math.max(...volumes) * 3;

  chartInstance.update();

  const prices = series.map(d => d.price);
  const high = Math.max(...prices);
  const low = Math.min(...prices);
  const firstPrice = prices[0];
  const lastPrice = prices[prices.length - 1];
  const diffPrice = lastPrice - firstPrice;
  const diffPct = firstPrice !== 0 ? (diffPrice / firstPrice) * 100 : 0;

  const highEl = document.getElementById('summaryHigh');
  if (highEl) highEl.textContent = formatCurrency(high);
  const lowEl = document.getElementById('summaryLow');
  if (lowEl) lowEl.textContent = formatCurrency(low);

  const sign = diffPrice >= 0 ? '+' : '';
  const trendEl = document.getElementById('summaryTrend');
  if (trendEl) {
    trendEl.textContent = `${sign}${diffPct.toFixed(2)}% (${sign}${formatCurrency(Math.abs(diffPrice))})`;
    trendEl.className = diffPrice >= 0 ? 'font-bold text-emerald-400 tabular text-sm' : 'font-bold text-rose-400 tabular text-sm';
  }
}

export function setChartType(type) {
  state.chartViewMode = type;
  const btnLine = document.getElementById('btnChartLine');
  const btnCandle = document.getElementById('btnChartCandle');
  if (btnLine) {
    btnLine.className = type === 'line'
      ? 'px-2.5 py-1 rounded font-medium bg-white/[0.1] text-white transition-all'
      : 'px-2.5 py-1 rounded font-medium text-zinc-400 hover:text-white transition-all';
  }
  if (btnCandle) {
    btnCandle.className = type === 'bar'
      ? 'px-2.5 py-1 rounded font-medium bg-white/[0.1] text-white transition-all'
      : 'px-2.5 py-1 rounded font-medium text-zinc-400 hover:text-white transition-all';
  }
  updateMarketChart();
}

export function toggleVolumeSeries() {
  state.showVolume = !state.showVolume;
  const dot = document.getElementById('volumeCheckIcon');
  if (dot) {
    dot.className = state.showVolume ? 'w-2 h-2 rounded-full bg-emerald-400' : 'w-2 h-2 rounded-full bg-zinc-600';
  }
  updateMarketChart();
}

export function switchTimeframe(tf) {
  state.currentTimeframe = tf;
  const buttons = ['24H', '7D', '30D', '1Y', 'ALL'];
  buttons.forEach(b => {
    const el = document.getElementById('tf' + b);
    if (el) {
      el.className = b === tf
        ? 'tf-btn px-2.5 py-1 rounded text-xs font-semibold bg-white/[0.1] text-white shadow-sm transition-colors'
        : 'tf-btn px-2.5 py-1 rounded text-xs font-medium text-zinc-400 hover:text-white transition-colors';
    }
  });

  const labelMap = {
    '24H': '24 Jam Terakhir',
    '7D': '7 Hari Terakhir',
    '30D': '30 Hari Terakhir',
    '1Y': '1 Tahun Terakhir',
    'ALL': 'Seluruh Riwayat',
  };
  const periodLabel = document.getElementById('chartPeriodLabel');
  if (periodLabel) periodLabel.textContent = labelMap[tf] || tf;

  updateMarketChart();
  fetchChartData(state.currentAsset, tf);
}

export function switchAsset(asset) {
  state.currentAsset = asset;
  const data = state.assetData[asset];

  const btnBTC = document.getElementById('btnAssetBTC');
  const btnETH = document.getElementById('btnAssetETH');

  if (asset === 'BTC') {
    if (btnBTC) btnBTC.className = 'flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-semibold bg-amber-500 text-zinc-950 shadow-sm transition-all';
    if (btnETH) btnETH.className = 'flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-medium text-zinc-400 hover:text-white transition-all';
    state.liveTradesList = [...initialBtcTrades];
  } else {
    if (btnETH) btnETH.className = 'flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-semibold bg-blue-500 text-white shadow-sm transition-all';
    if (btnBTC) btnBTC.className = 'flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-medium text-zinc-400 hover:text-white transition-all';
    state.liveTradesList = [...initialEthTrades];
  }

  updateKpiView();

  const chartTitle = document.getElementById('chartTitle');
  if (chartTitle) chartTitle.textContent = `Grafik Pergerakan Harga ${data.name}`;

  state.tableCurrentPage = 1;
  renderLiveTrades();
  renderHistoryTable();
  updateMarketChart();

  fetchKpiData(asset);
  fetchChartData(asset, state.currentTimeframe);
  fetchTradesData(asset);
  fetchLedgerData(asset);
}

export function updateKpiView() {
  const data = state.assetData[state.currentAsset];
  const hdrPrice = document.getElementById('headerPrice');
  if (hdrPrice) hdrPrice.textContent = formatCurrency(data.spotPrice);

  const hdrBadge = document.getElementById('headerChangeBadge');
  if (hdrBadge) {
    const isUp = data.change24h >= 0;
    hdrBadge.innerHTML = (isUp ? '▲ +' : '▼ ') + Math.abs(data.change24h).toFixed(2) + '%';
    hdrBadge.className = `flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-semibold ${isUp ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/25' : 'bg-rose-500/15 text-rose-400 border border-rose-500/25'}`;
  }

  const spotPriceEl = document.getElementById('cardSpotPrice');
  if (spotPriceEl) spotPriceEl.textContent = formatCurrency(data.spotPrice);

  const spotBadgeEl = document.getElementById('spotBadge');
  if (spotBadgeEl) {
    const isUp = data.change24h >= 0;
    spotBadgeEl.innerHTML = (isUp ? '▲ +' : '▼ ') + Math.abs(data.change24h).toFixed(2) + '%';
    spotBadgeEl.className = `text-[11px] px-2 py-0.5 rounded font-semibold border tabular ${isUp ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' : 'bg-rose-500/10 text-rose-400 border-rose-500/20'}`;
  }

  const rangeLowEl = document.getElementById('rangeLow');
  if (rangeLowEl) rangeLowEl.textContent = formatCurrency(data.low24h);

  const rangeHighEl = document.getElementById('rangeHigh');
  if (rangeHighEl) rangeHighEl.textContent = formatCurrency(data.high24h);

  const volUsdEl = document.getElementById('cardVolumeUsd');
  if (volUsdEl) volUsdEl.textContent = '$' + (typeof data.volumeUsd === 'number' ? data.volumeUsd.toFixed(2) : data.volumeUsd) + ' Juta';

  const volAssetEl = document.getElementById('cardVolumeAsset');
  if (volAssetEl) volAssetEl.textContent = formatNumber(data.volumeAsset, 2) + ' ' + state.currentAsset;

  const txCountEl = document.getElementById('cardTxCount');
  if (txCountEl) txCountEl.textContent = formatNumber(data.txCount);

  const activeAddrsEl = document.getElementById('cardActiveAddrs');
  if (activeAddrsEl) activeAddrsEl.textContent = formatNumber(data.activeAddrs) + ' Alamat';

  const rangeSpan = data.high24h - data.low24h;
  if (rangeSpan > 0) {
    const rangePct = Math.min(100, Math.max(0, ((data.spotPrice - data.low24h) / rangeSpan) * 100));
    const rBar = document.getElementById('rangeBar');
    if (rBar) rBar.style.width = rangePct.toFixed(0) + '%';
  }

  if (data.ath) {
    const athEl = document.getElementById('statAth');
    if (athEl) athEl.textContent = formatCurrency(data.ath);
  }
  if (data.athDiff !== undefined) {
    const athDiffEl = document.getElementById('statAthDiff');
    if (athDiffEl) athDiffEl.textContent = `(${data.athDiff}%)`;
  }
  if (data.marketCap) {
    const mcEl = document.getElementById('statMarketCap');
    if (mcEl) mcEl.textContent = '$' + data.marketCap + ' ' + (data.marketCapUnit || 'Triliun');
  }
  if (data.dominance !== undefined) {
    const domEl = document.getElementById('statDominance');
    if (domEl) domEl.textContent = data.dominance + '%';
  }
  if (data.supply) {
    const supEl = document.getElementById('statSupply');
    if (supEl) supEl.textContent = data.supply;
  }
  if (data.change7d !== undefined) {
    const c7El = document.getElementById('stat7dChange');
    if (c7El) c7El.textContent = (data.change7d >= 0 ? '+' : '') + data.change7d + '%';
  }
  if (data.avgFee) {
    const feeEl = document.getElementById('statAvgFee');
    if (feeEl) feeEl.textContent = data.avgFee;
  }
}

export function renderLiveTrades() {
  const tbody = document.getElementById('liveTradesBody');
  if (!tbody) return;
  tbody.innerHTML = '';

  state.liveTradesList.slice(0, 6).forEach((trade, idx) => {
    const tr = document.createElement('tr');
    const isBuy = trade.side === 'BUY';
    tr.className = `py-1.5 transition-colors ${idx === 0 ? (isBuy ? 'flash-buy' : 'flash-sell') : ''}`;
    tr.innerHTML = `
      <td class="py-2 text-zinc-400 font-mono text-[11px]">${trade.time}</td>
      <td class="py-2">
        <span class="px-1.5 py-0.5 rounded text-[10px] font-bold ${isBuy ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/20' : 'bg-rose-500/15 text-rose-400 border border-rose-500/20'}">
          ${isBuy ? 'BELI' : 'JUAL'}
        </span>
      </td>
      <td class="py-2 text-right font-medium text-zinc-200 font-mono text-[11px]">${formatCurrency(trade.price)}</td>
      <td class="py-2 text-right font-mono text-zinc-300 text-[11px]">${Number(trade.size).toFixed(4)} ${state.currentAsset}</td>
    `;
    tbody.appendChild(tr);
  });
}

export function toggleStream() {
  state.streamPaused = !state.streamPaused;
  const btn = document.getElementById('btnPauseStream');
  if (!btn) return;
  if (state.streamPaused) {
    btn.textContent = 'Lanjutkan';
    btn.className = 'text-[11px] px-2 py-0.5 rounded bg-emerald-600 hover:bg-emerald-500 text-zinc-950 font-bold transition-colors';
  } else {
    btn.textContent = 'Jeda';
    btn.className = 'text-[11px] px-2 py-0.5 rounded bg-zinc-800 hover:bg-zinc-700 text-zinc-300 transition-colors';
    fetchTradesData(state.currentAsset);
  }
}

export function renderHistoryTable() {
  const tbody = document.getElementById('historyTableBody');
  if (!tbody) return;
  const allRows = state.assetData[state.currentAsset].historyRows;

  let filtered = allRows;
  if (state.tableSearchQuery.trim()) {
    const q = state.tableSearchQuery.toLowerCase();
    filtered = allRows.filter(r =>
      r.date.toLowerCase().includes(q) ||
      r.close.toString().includes(q) ||
      r.open.toString().includes(q)
    );
  }

  const totalPages = Math.max(1, Math.ceil(filtered.length / state.tablePageSize));
  if (state.tableCurrentPage > totalPages) state.tableCurrentPage = totalPages;

  const startIndex = (state.tableCurrentPage - 1) * state.tablePageSize;
  const pageRows = filtered.slice(startIndex, startIndex + state.tablePageSize);

  tbody.innerHTML = '';

  if (pageRows.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" class="text-center py-6 text-zinc-500">
          Tidak ditemukan data yang sesuai dengan pencarian "${state.tableSearchQuery}"
        </td>
      </tr>
    `;
  } else {
    pageRows.forEach(row => {
      const isUp = row.change >= 0;
      const tr = document.createElement('tr');
      tr.className = 'hover:bg-white/[0.02] transition-colors';
      tr.innerHTML = `
        <td class="py-2.5 px-3 font-medium text-zinc-300">${row.date}</td>
        <td class="py-2.5 px-3 text-right font-mono text-zinc-400">${formatCurrency(row.open)}</td>
        <td class="py-2.5 px-3 text-right font-mono text-emerald-400/90">${formatCurrency(row.high)}</td>
        <td class="py-2.5 px-3 text-right font-mono text-rose-400/90">${formatCurrency(row.low)}</td>
        <td class="py-2.5 px-3 text-right font-mono font-semibold text-white">${formatCurrency(row.close)}</td>
        <td class="py-2.5 px-3 text-right font-mono">
          <span class="inline-flex items-center px-1.5 py-0.5 rounded text-[11px] font-semibold ${isUp ? 'text-emerald-400 bg-emerald-500/10' : 'text-rose-400 bg-rose-500/10'}">
            ${isUp ? '+' : ''}${row.change.toFixed(2)}%
          </span>
        </td>
        <td class="py-2.5 px-3 text-right font-mono text-zinc-300">${formatNumber(row.volume, 2)} ${state.currentAsset}</td>
        <td class="py-2.5 px-3 text-right font-mono text-zinc-400">${formatNumber(row.tx)}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  const recordInfo = document.getElementById('tableRecordInfo');
  if (recordInfo) {
    recordInfo.textContent = `Menampilkan baris ${filtered.length > 0 ? startIndex + 1 : 0} - ${Math.min(startIndex + state.tablePageSize, filtered.length)} dari ${filtered.length} hari riwayat pasar`;
  }
  const indicator = document.getElementById('pageIndicator');
  if (indicator) indicator.textContent = `${state.tableCurrentPage} / ${totalPages}`;

  const prevBtn = document.getElementById('btnPrevPage');
  if (prevBtn) prevBtn.disabled = (state.tableCurrentPage <= 1);
  const nextBtn = document.getElementById('btnNextPage');
  if (nextBtn) nextBtn.disabled = (state.tableCurrentPage >= totalPages);
}

export function changeTablePage(delta) {
  state.tableCurrentPage += delta;
  renderHistoryTable();
}

export function handleTableSearch(val) {
  state.tableSearchQuery = val;
  state.tableCurrentPage = 1;
  renderHistoryTable();
}

export function downloadDataCSV() {
  try {
    const link = document.createElement('a');
    link.href = api.getExportCsvUrl(state.currentAsset);
    link.download = `${state.currentAsset.toLowerCase()}_market_history_${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  } catch (err) {
    const rows = state.assetData[state.currentAsset].historyRows;
    let csvContent = 'data:text/csv;charset=utf-8,';
    csvContent += 'Tanggal,Aset,Harga_Buka_USD,Tertinggi_USD,Terendah_USD,Harga_Tutup_USD,Perubahan_Persen,Volume_Koin,Total_Transaksi\n';

    rows.forEach(r => {
      csvContent += `${r.date},${state.currentAsset},${r.open},${r.high},${r.low},${r.close},${r.change},${r.volume},${r.tx}\n`;
    });

    const encodedUri = encodeURI(csvContent);
    const fallbackLink = document.createElement('a');
    fallbackLink.setAttribute('href', encodedUri);
    fallbackLink.setAttribute('download', `${state.currentAsset.toLowerCase()}_market_history_${new Date().toISOString().slice(0, 10)}.csv`);
    document.body.appendChild(fallbackLink);
    fallbackLink.click();
    document.body.removeChild(fallbackLink);
  }
}

export async function fetchKpiData(asset) {
  const data = await api.getKpi(asset);
  if (!data || !data.spot_price) return;

  state.assetData[asset].spotPrice = data.spot_price;
  state.assetData[asset].change24h = data.change_24h;
  state.assetData[asset].change24hUsd = data.change_24h_usd;
  state.assetData[asset].high24h = data.high_24h;
  state.assetData[asset].low24h = data.low_24h;
  state.assetData[asset].volumeUsd = data.volume_usd;
  state.assetData[asset].volumeAsset = data.volume_asset;
  if (data.tx_count) state.assetData[asset].txCount = data.tx_count;
  if (data.active_addrs) state.assetData[asset].activeAddrs = data.active_addrs;
  if (data.market_cap) state.assetData[asset].marketCap = data.market_cap;
  if (data.dominance) state.assetData[asset].dominance = data.dominance;
  if (data.ath) state.assetData[asset].ath = data.ath;
  if (data.ath_diff !== undefined) state.assetData[asset].athDiff = data.ath_diff;
  if (data.supply) state.assetData[asset].supply = data.supply;
  if (data.change_7d !== undefined) state.assetData[asset].change7d = data.change_7d;
  if (data.avg_fee) state.assetData[asset].avgFee = data.avg_fee;

  if (asset === state.currentAsset) {
    updateKpiView();
  }
}

export async function fetchChartData(asset, tf) {
  const data = await api.getChart(asset, tf);
  if (!data || !Array.isArray(data.series) || data.series.length === 0) return;

  if (tf === '24H') {
    state.assetData[asset].chartData24H = data.series;
  } else {
    state.assetData[asset].chartData30D = data.series;
  }

  if (asset === state.currentAsset && tf === state.currentTimeframe) {
    updateMarketChart();
  }
}

export async function fetchTradesData(asset) {
  const data = await api.getTrades(asset);
  if (!data || !Array.isArray(data.trades) || data.trades.length === 0) return;

  if (asset === state.currentAsset) {
    state.liveTradesList = [...data.trades];
    renderLiveTrades();
  }
}

export async function fetchLedgerData(asset) {
  const data = await api.getLedger(asset, 30);
  if (!data || !Array.isArray(data.rows) || data.rows.length === 0) return;

  state.assetData[asset].historyRows = data.rows;
  if (asset === state.currentAsset) {
    renderHistoryTable();
  }
}
