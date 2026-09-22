/**
 * Robust REST API service layer for Bitcoin Market Hub dashboard.
 */

/**
 * Generic GET helper with error handling.
 * @param {string} url
 * @returns {Promise<any|null>}
 */
async function fetchJson(url) {
  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      console.warn(`HTTP ${resp.status} for ${url}`);
      return null;
    }
    return await resp.json();
  } catch (err) {
    console.warn(`Network error fetching ${url}:`, err);
    return null;
  }
}

export const api = {
  /**
   * Fetch 3 KPI card metrics for the specified asset.
   * @param {string} asset
   * @returns {Promise<any|null>}
   */
  async getKpi(asset) {
    return fetchJson(`/api/kpi?asset=${encodeURIComponent(asset)}`);
  },

  /**
   * Fetch historical and mark-to-market chart series.
   * @param {string} asset
   * @param {string} range
   * @returns {Promise<any|null>}
   */
  async getChart(asset, range) {
    return fetchJson(`/api/chart?asset=${encodeURIComponent(asset)}&range=${encodeURIComponent(range)}&live=true`);
  },

  /**
   * Fetch recent trade tape fills.
   * @param {string} asset
   * @returns {Promise<any|null>}
   */
  async getTrades(asset) {
    return fetchJson(`/api/trades?asset=${encodeURIComponent(asset)}`);
  },

  /**
   * Fetch conformed daily market and network ledger rows.
   * @param {string} asset
   * @param {number} [limit=30]
   * @returns {Promise<any|null>}
   */
  async getLedger(asset, limit = 30) {
    return fetchJson(`/api/ledger?asset=${encodeURIComponent(asset)}&limit=${limit}`);
  },

  /**
   * Fetch portfolio summary and balance telemetry.
   * @returns {Promise<any|null>}
   */
  async getPortfolio() {
    return fetchJson('/api/portfolio');
  },

  /**
   * Fetch forward paper trading equity timeseries curves.
   * @param {number} [limit=90]
   * @returns {Promise<any|null>}
   */
  async getPortfolioEquity(limit = 90) {
    return fetchJson(`/api/portfolio/equity?limit=${limit}&live=true`);
  },

  /**
   * Fetch forward paper trading order ledger entries.
   * @param {number} [limit=50]
   * @returns {Promise<any|null>}
   */
  async getPortfolioTrades(limit = 50) {
    return fetchJson(`/api/portfolio/trades?limit=${limit}`);
  },

  /**
   * Fetch Composite Macro-Narrative Index (MNI) radar state.
   * @returns {Promise<any|null>}
   */
  async getMacroRadar() {
    return fetchJson('/api/macro/radar');
  },

  /**
   * Fetch curated multi-source news articles.
   * @param {number} [limit=20]
   * @returns {Promise<any|null>}
   */
  async getMacroNews(limit = 20) {
    return fetchJson(`/api/macro/news?limit=${limit}`);
  },

  /**
   * Fetch macroeconomic calendar releases and surprises.
   * @param {number} [days=7]
   * @returns {Promise<any|null>}
   */
  async getMacroCalendar(days = 7) {
    return fetchJson(`/api/macro/calendar?days=${days}`);
  },

  /**
   * Fetch latest AI Investment Committee memorandum.
   * @returns {Promise<any|null>}
   */
  async getCommitteeLatest() {
    return fetchJson('/api/committee/latest');
  },

  /**
   * Fetch operator market intelligence notes.
   * @param {number} [limit=20]
   * @returns {Promise<any|null>}
   */
  async getIntelligenceList(limit = 20) {
    return fetchJson(`/api/intelligence/list?limit=${limit}`);
  },

  /**
   * Submit operator intelligence analysis note.
   * @param {object} payload
   * @returns {Promise<Response>}
   */
  async postIntelligence(payload) {
    return fetch('/api/intelligence/ingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  },

  /**
   * Fetch automated scheduling pipeline execution status and locks.
   * @returns {Promise<any|null>}
   */
  async getPipelineSchedule() {
    return fetchJson('/api/pipeline/schedule');
  },

  /**
   * Build CSV export URL.
   * @param {string} asset
   * @returns {string}
   */
  getExportCsvUrl(asset) {
    return `/api/export?format=csv&asset=${encodeURIComponent(asset)}`;
  },
};
