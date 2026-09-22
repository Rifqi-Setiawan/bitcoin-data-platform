/**
 * Main Application Bootstrap & Coordinator for Bitcoin Market Hub.
 * ES Module orchestrating state, tab switching, and periodic polling.
 */

import { state } from './state.js';
import * as market from './components/market.js';
import * as portfolio from './components/portfolio.js';
import * as macro from './components/macro.js';
import * as committee from './components/committee.js';

export function switchMainTab(tab) {
  state.currentMainTab = tab;
  const marketView = document.getElementById('marketView');
  const portfolioView = document.getElementById('portfolioView');
  const macroView = document.getElementById('macroView');
  const committeeView = document.getElementById('committeeView');

  const tabMarketBtn = document.getElementById('tabMarketBtn');
  const tabPortfolioBtn = document.getElementById('tabPortfolioBtn');
  const tabMacroBtn = document.getElementById('tabMacroBtn');
  const tabCommitteeBtn = document.getElementById('tabCommitteeBtn');

  const activeClass = 'whitespace-nowrap shrink-0 flex items-center gap-2 px-4 py-2 rounded-lg text-xs sm:text-sm font-semibold bg-amber-500 text-zinc-950 shadow-sm transition-all';
  const inactiveClass = 'whitespace-nowrap shrink-0 flex items-center gap-2 px-4 py-2 rounded-lg text-xs sm:text-sm font-medium text-zinc-400 hover:text-white transition-all';

  if (marketView) marketView.classList.add('hidden');
  if (portfolioView) portfolioView.classList.add('hidden');
  if (macroView) macroView.classList.add('hidden');
  if (committeeView) committeeView.classList.add('hidden');

  if (tabMarketBtn) tabMarketBtn.className = inactiveClass;
  if (tabPortfolioBtn) tabPortfolioBtn.className = inactiveClass;
  if (tabMacroBtn) tabMacroBtn.className = inactiveClass;
  if (tabCommitteeBtn) tabCommitteeBtn.className = inactiveClass;

  if (tab === 'market') {
    if (marketView) marketView.classList.remove('hidden');
    if (tabMarketBtn) tabMarketBtn.className = activeClass;
    market.refreshMarket();
  } else if (tab === 'portfolio') {
    if (portfolioView) portfolioView.classList.remove('hidden');
    if (tabPortfolioBtn) tabPortfolioBtn.className = activeClass;
    portfolio.loadPortfolioData();
  } else if (tab === 'macro') {
    if (macroView) macroView.classList.remove('hidden');
    if (tabMacroBtn) tabMacroBtn.className = activeClass;
    macro.loadMacroData();
  } else if (tab === 'committee') {
    if (committeeView) committeeView.classList.remove('hidden');
    if (tabCommitteeBtn) tabCommitteeBtn.className = activeClass;
    committee.loadCommitteeData();
  }
}

// Expose handlers globally for backward compatibility with inline HTML attributes
window.switchMainTab = switchMainTab;
window.switchAsset = market.switchAsset;
window.switchTimeframe = market.switchTimeframe;
window.setChartType = market.setChartType;
window.toggleVolumeSeries = market.toggleVolumeSeries;
window.toggleStream = market.toggleStream;
window.changeTablePage = market.changeTablePage;
window.handleTableSearch = market.handleTableSearch;
window.downloadDataCSV = market.downloadDataCSV;
window.toggleIntelModal = committee.toggleIntelModal;
window.submitUserIntelligence = committee.submitUserIntelligence;

function bootstrap() {
  market.initMarket();
  portfolio.initPortfolio();
  macro.initMacro();

  // Coordinated periodic background polling (15s for Market Ticker/Trades, 30s for Portfolio/Committee)
  setInterval(() => {
    if (state.currentMainTab === 'market') {
      market.refreshMarket();
    }
  }, 15000);

  setInterval(() => {
    if (state.currentMainTab === 'portfolio') {
      portfolio.loadPortfolioData();
    } else if (state.currentMainTab === 'committee') {
      committee.loadCommitteeData();
    } else if (state.currentMainTab === 'macro') {
      macro.loadMacroData();
    }
  }, 30000);
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', bootstrap);
} else {
  bootstrap();
}
