# Phase 14: Backtest & Validation Engine — Event-Driven Historical Simulation & Quantitative Benchmarking

**Owner:** Engineering Team  
**Verification:** Automated Test Suite, Quantitative Calibration & Peer Review  
**Branch:** `feature/P14-backtest-validation`  
**Status:** In Progress  

---

## 1. Objective

Build an institutional-grade, event-driven backtesting and quantitative validation engine for the Bitcoin Data Platform. The engine simulates and benchmarks multi-year systematic investment strategies—specifically contrasting the **Dynamic Reserve DCA + Macro Regime Overlay** against industry benchmarks (**Blind DCA** and **Lump Sum Buy & Hold**)—across complete market cycles (bull, bear, accumulation, and blow-off tops).

This delivers the empirical foundation required before autonomous capital deployment (Phases 15 & 16), proving out-of-sample risk-adjusted outperformance (Sharpe, Sortino, Max Drawdown).

---

## 2. User Story

As an autonomous crypto investor and quantitative engineer, I need a deterministic backtesting engine that consumes curated daily analytical marts (`mart_btc_investment_signals_daily`) to simulate portfolio growth, track cash reserve utilization, and report annualized risk-adjusted metrics, so that I can validate that Dynamic Reserve DCA strictly outperforms naive DCA and Buy & Hold in drawdown protection and acquisition efficiency without lookahead bias.

---

## 3. Scope

### In Scope
1. **Event-Driven Simulation Engine (`src/bitcoin_data_platform/backtest/engine.py`)**:
   - Day-by-day chronological iteration preventing lookahead bias.
   - Dual-wallet accounting: Fiat Reserve Cash ($USD) and Asset Holding ($BTC).
   - Cash flow tracking: periodic contributions, cash reserves, deployment, fees/slippage modeling (configurable bps).
2. **Strategy Taxonomy (`src/bitcoin_data_platform/backtest/strategies.py`)**:
   - `LumpSumStrategy`: 100% initial capital deployed at $T_0$, zero subsequent contributions.
   - `BlindDCAStrategy`: Constant periodic USD contribution executed unconditionally on schedule.
   - `DynamicReserveDCAStrategy`:
     - Base DCA Pool (70%) + Tactical Reserve Pool (30%).
     - Multiplier modulation:
       - `AGGRESSIVE_ACCUMULATE`: $2.0 \times \text{base} + 25\%$ draw from tactical reserve pool.
       - `OPPORTUNISTIC_ACCUMULATE`: $1.3 \times \text{base}$.
       - `STANDARD_DCA`: $1.0 \times \text{base}$ (reserve untouched).
       - `DEFENSIVE_RESERVE`: $0.5 \times \text{base}$ (remaining $50\%$ diverted into reserve).
       - `HARD_FREEZE`: $0.0 \times \text{base}$ ($100\%$ diverted into reserve).
     - Macro Circuit Breaker: When `has_high_impact_macro_event == True`, halts execution on event day to bypass liquidity shocks.
3. **Quantitative Metrics Suite (`src/bitcoin_data_platform/backtest/metrics.py`)**:
   - Total Fiat Contributed, Final Equity ($V_{portfolio}$), Net PnL ($), Total Return (%).
   - Annualized Return / CAGR (Compound Annual Growth Rate, 365-day basis).
   - Maximum Drawdown (MDD %) and Peak-to-Trough duration.
   - Annualized Sharpe Ratio (continuous 365-day crypto basis, default $R_f = 3.0\%$).
   - Annualized Sortino Ratio (downside semivariance).
   - Calmar Ratio ($\text{CAGR} / |\text{MDD}|$).
   - Average BTC Acquisition Price vs Average Market Price (Acquisition Discount %).
   - Reserve Pool High-Water Mark & Peak Cash Utilization.
4. **Multi-Format Reporting & CLI (`src/bitcoin_data_platform/backtest/reporter.py`, `cli.py`)**:
   - CLI command: `bitcoin-data backtest`
     - Options: `--strategy` (`all` | `dynamic-reserve` | `blind-dca` | `lump-sum`), `--start`, `--end`, `--initial-cash`, `--periodic-amount`, `--frequency` (`daily` | `weekly`), `--fee-bps`, `--db-path`, `--format` (`table` | `json` | `markdown`), `--output`.
   - Side-by-side benchmarking table comparing all 3 strategies.
5. **Research Reproducibility Notebook (`notebooks/investment_strategy_backtest.ipynb`)**:
   - Visualizing equity curves, drawdown series, reserve pool dynamics, and signal scatter plots.
6. **Comprehensive Test Suite**:
   - Unit tests for engine accounting, strategy execution rules, metric calculations, and CLI integration.

### Out of Scope
- Intraday order book / tick-level simulation (daily close granularity is canonical for long-horizon investment).
- Derivative hedging (futures / options).
- Real order submission via Coinbase API (Phase 16).

---

## 4. Architecture & Data Flow

```
┌────────────────────────────────────────────────────────────────────────┐
│ DuckDB / Parquet Serving Layer                                         │
│ mart_btc_investment_signals_daily                                      │
│ (trade_date_utc, market_close_usd, sma_200, mayer_multiple,           │
│  mvrv_ratio, fng_value, has_high_impact_macro_event, investment_signal)│
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BacktestEngine (src/bitcoin_data_platform/backtest/engine.py)          │
│                                                                        │
│ For each Date T (chronological, no lookahead):                         │
│  1. Inject periodic cash contribution into simulation wallet.          │
│  2. Strategy receives Snapshot(T-1 context, signal(T), price(T)).      │
│  3. Strategy returns OrderAction(buy_amount_usd, reserve_allocation).   │
│  4. Engine deducts fee (bps), purchases BTC: ΔBTC = (USD - fee)/Price. │
│  5. Portfolio valuation update: Equity(T) = Cash(T) + BTC(T)*Price(T). │
│  6. Daily step recorded in SimulationHistory.                          │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Metrics Calculator (src/bitcoin_data_platform/backtest/metrics.py)     │
│ Computes: CAGR, MDD, Sharpe, Sortino, Calmar, Avg Cost, Win Rates      │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Multi-Strategy Benchmark Reporter                                      │
│ Outputs: Tabular CLI comparison, Markdown report, JSON export          │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Mathematical Formulations

### 5.1 Portfolio Valuation
At date $t$:
$$V_t = \text{Cash}_t + \text{BTC}_t \times P_t$$
where $P_t$ is `market_close_usd` on date $t$.

### 5.2 Capital Flows & Simple Return
Let $C_0$ be initial cash and $c_t$ be periodic capital injected at step $t$:
$$\text{Total Invested}_T = C_0 + \sum_{t=1}^T c_t$$
$$\text{Total Return} = \frac{V_T - \text{Total Invested}_T}{\text{Total Invested}_T} \times 100\%$$

### 5.3 Daily Time-Weighted Performance Return
To account for external cash infusions without distorting trading skill:
$$R_t = \frac{V_t - c_t - V_{t-1}}{V_{t-1}}$$

### 5.4 Annualized Return (CAGR)
For duration of $N$ calendar days:
$$\text{CAGR} = \left( \frac{V_T}{\text{Total Invested}_T} \right)^{\frac{365}{N}} - 1$$

### 5.5 Maximum Drawdown (MDD)
$$\text{Peak}_t = \max_{0 \le s \le t} V_s$$
$$\text{Drawdown}_t = \frac{V_t - \text{Peak}_t}{\text{Peak}_t}$$
$$\text{MDD} = \min_{0 \le t \le T} \text{Drawdown}_t$$

### 5.6 Annualized Sharpe Ratio (365 Days)
$$\text{Sharpe} = \sqrt{365} \times \frac{\bar{R} - R_f / 365}{\sigma_R}$$

### 5.7 Annualized Sortino Ratio
$$\sigma_{\text{downside}} = \sqrt{\frac{1}{N} \sum_{t=1}^N \min\left(0, R_t - \frac{R_f}{365}\right)^2}$$
$$\text{Sortino} = \sqrt{365} \times \frac{\bar{R} - R_f / 365}{\sigma_{\text{downside}}}$$

---

## 6. Module & File Layout

### NEW Files
| Path | Purpose |
|---|---|
| `src/bitcoin_data_platform/backtest/__init__.py` | Package exports |
| `src/bitcoin_data_platform/backtest/models.py` | Data contracts: `BacktestConfig`, `BacktestDayRecord`, `DailyPortfolioState`, `StrategyResult`, `BenchmarkSummary` |
| `src/bitcoin_data_platform/backtest/strategies.py` | Strategy classes: `BaseStrategy`, `LumpSumStrategy`, `BlindDCAStrategy`, `DynamicReserveDCAStrategy` |
| `src/bitcoin_data_platform/backtest/engine.py` | Event-driven simulation runner with deterministic cash/BTC accounting |
| `src/bitcoin_data_platform/backtest/metrics.py` | Statistical & quantitative metrics calculator |
| `src/bitcoin_data_platform/backtest/reporter.py` | Terminal table, Markdown, and JSON benchmarking formats |
| `tests/test_backtest_models.py` | Contract validations and configuration bounds tests |
| `tests/test_backtest_strategies.py` | Unit tests for strategy decision logic & multipliers |
| `tests/test_backtest_metrics.py` | Mathematical verification of CAGR, MDD, Sharpe, Sortino |
| `tests/test_backtest_engine.py` | Full simulation cycle, fee deductions, cash accounting |
| `tests/test_backtest_cli.py` | CLI argument parsing, execution, and output formats |
| `notebooks/investment_strategy_backtest.ipynb` | Reproducible visual research notebook |

### MODIFIED Files
| Path | Modifications |
|---|---|
| `src/bitcoin_data_platform/cli.py` | Register `backtest` subcommand and handler |
| `src/bitcoin_data_platform/__init__.py` | Export backtest modules |
| `docs/data_dictionary/DATA_DICTIONARY.md` | Document backtest reporting schemas and metrics |
| `docs/roadmap/ROADMAP.md` | Update Phase 14 status |
| `README.md` | Document backtest CLI usage |

---

## 7. Acceptance Criteria

- **AC-1:** Deterministic, zero-lookahead backtest engine executes on `mart_btc_investment_signals_daily` records.
- **AC-2:** Simulates all 3 strategies: `lump-sum`, `blind-dca`, and `dynamic-reserve` with identical cash injection schedules.
- **AC-3:** Correctly handles `DynamicReserveDCAStrategy` multipliers (2.0x, 1.3x, 1.0x, 0.5x, 0.0x) and draws from tactical cash reserve.
- **AC-4:** Macro circuit breaker pauses buying when `has_high_impact_macro_event == True`.
- **AC-5:** Accurately computes Total Return, CAGR, MDD, Sharpe Ratio, Sortino Ratio, Calmar Ratio, and Avg Cost.
- **AC-6:** CLI `bitcoin-data backtest` supports `--strategy`, `--start`, `--end`, `--initial-cash`, `--periodic-amount`, `--frequency`, `--format`, and `--output`.
- **AC-7:** 100% test pass rate with zero regressions across prior phases (545+ existing tests passing).
- **AC-8:** Clean quality gates: `ruff check`, `ruff format --check`, `mypy src`.
- **AC-9:** Zero AI or sub-agent traces anywhere in code or documentation.
