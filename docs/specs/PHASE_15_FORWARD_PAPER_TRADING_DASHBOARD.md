# Phase 15: Forward Paper Trading Engine ($1,000 Virtual Capital) & Web Dashboard Integration

**Owner:** Engineering Team  
**Verification:** Automated Test Suite & Independent Peer Review  
**Branch:** `feature/P15-paper-trading-dashboard`  
**Status:** Complete  

---

## 1. Objective

Implement a forward-testing **Paper Trading Simulation Engine** initialized with **$1,000.00 USD virtual capital** and integrate a **Fincept Terminal-style Portfolio Tracker** into the existing Bitcoin Market Hub Web UI dashboard.

This allows continuous forward validation of the `DynamicReserveDCAStrategy` against live market data without risking real capital, providing full transparency through visual equity curves, cash reserve utilization charts, and an institutional-grade trade blotter.

---

## 2. Institutional Reference Architecture Synthesis

Drawing from the evaluation of **AutoHedge**, **Vibe-Trading**, and **Fincept Terminal**:

1. **Pre-Trade Risk Gatekeeper (`RiskGuard`)** (AutoHedge-inspired):
   - Deterministic verification before order execution.
   - Enforces cash solvency, price freshness (< 20% deviation check), macro volatility circuit breaker, and emergency filesystem kill-switch (`data/state/PAPER_KILL_SWITCH`).
2. **Shadow Account Ledger** (Vibe-Trading-inspired):
   - Double-entry accounting in DuckDB tables separating Base Cash, Tactical Reserve, and BTC holdings.
   - Standard 10 bps (0.10%) simulated Coinbase Spot commission deduction.
3. **High-Density Terminal UI & Order Blotter** (Fincept Terminal-inspired):
   - Multi-curve equity comparison: Dynamic Reserve Strategy vs $1,000 Buy & Hold Benchmark.
   - Asset allocation breakdown: Base Cash vs Tactical Reserve vs BTC Stack.
   - Interactive order blotter table with signal tags, transaction hashes, and Indonesian strategy narratives.

---

## 3. Database Schema (`data/state/platform.duckdb`)

### 3.1 `paper_portfolio_balance`
```sql
CREATE TABLE IF NOT EXISTS paper_portfolio_balance (
    portfolio_id VARCHAR PRIMARY KEY,
    initial_cash DOUBLE NOT NULL,
    base_cash DOUBLE NOT NULL,
    reserve_cash DOUBLE NOT NULL,
    btc_balance DOUBLE NOT NULL,
    total_contributed DOUBLE NOT NULL,
    last_updated_utc TIMESTAMPTZ NOT NULL,
    total_trades INTEGER NOT NULL DEFAULT 0
);
```

### 3.2 `paper_portfolio_snapshots_daily`
```sql
CREATE TABLE IF NOT EXISTS paper_portfolio_snapshots_daily (
    snapshot_date DATE NOT NULL,
    portfolio_id VARCHAR NOT NULL,
    base_cash DOUBLE NOT NULL,
    reserve_cash DOUBLE NOT NULL,
    total_cash DOUBLE NOT NULL,
    btc_balance DOUBLE NOT NULL,
    btc_price DOUBLE NOT NULL,
    portfolio_equity DOUBLE NOT NULL,
    unrealized_pnl_usd DOUBLE NOT NULL,
    unrealized_pnl_pct DOUBLE NOT NULL,
    benchmark_equity DOUBLE NOT NULL,
    PRIMARY KEY (snapshot_date, portfolio_id)
);
```

### 3.3 `paper_trade_ledger`
```sql
CREATE TABLE IF NOT EXISTS paper_trade_ledger (
    trade_id VARCHAR PRIMARY KEY,
    portfolio_id VARCHAR NOT NULL,
    executed_at_utc TIMESTAMPTZ NOT NULL,
    trade_date DATE NOT NULL,
    side VARCHAR NOT NULL,               -- 'BUY', 'HOLD'
    signal_regime VARCHAR NOT NULL,      -- 'AGGRESSIVE_ACCUMULATE', etc.
    spot_price DOUBLE NOT NULL,
    gross_amount_usd DOUBLE NOT NULL,
    fee_usd DOUBLE NOT NULL,
    net_amount_usd DOUBLE NOT NULL,
    btc_amount DOUBLE NOT NULL,
    narrative VARCHAR NOT NULL
);
```

---

## 4. Module & Component Layout

### NEW Package: `src/bitcoin_data_platform/paper/`
| File | Description |
|---|---|
| `__init__.py` | Exports `PaperTradingEngine`, `RiskGuard`, `PaperPortfolioState` |
| `models.py` | Data contracts: `PaperPortfolioBalance`, `PaperTradeRecord`, `PaperSnapshotRecord`, `PaperSummary` |
| `risk_guard.py` | Pre-trade risk gatekeeper with kill-switch, solvency, and deviation validation |
| `engine.py` | Chronological paper trading execution engine, DuckDB persistence, and equity calculations |

### MODIFIED Modules
| File | Modifications |
|---|---|
| `src/bitcoin_data_platform/storage/duckdb_manager.py` | Create tables `paper_portfolio_balance`, `paper_portfolio_snapshots_daily`, `paper_trade_ledger` in `initialize()` |
| `src/bitcoin_data_platform/dashboard/server.py` | Add endpoints `/api/portfolio`, `/api/portfolio/equity`, `/api/portfolio/trades` with fallback mocks if DB not populated |
| `src/bitcoin_data_platform/dashboard/assets/index.html` | Add "Portfolio Simulasi ($1,000)" navigation, 4 KPI cards, dual-curve chart, asset donut, and trade blotter table |
| `src/bitcoin_data_platform/cli.py` | Register `paper` subcommand (`init`, `step`, `status`, `reset`) |
| `docs/data_dictionary/DATA_DICTIONARY.md` | Document paper trading tables and schemas |
| `docs/roadmap/ROADMAP.md` | Mark Phase 15 in-progress / complete |
| `README.md` | Add Paper Trading documentation and CLI instructions |

---

## 5. Dashboard API Contracts

### 5.1 `GET /api/portfolio`
```json
{
  "portfolio_id": "default",
  "initial_cash": 1000.00,
  "total_equity": 1042.85,
  "unrealized_pnl_usd": 42.85,
  "unrealized_pnl_pct": 4.29,
  "base_cash": 412.50,
  "reserve_cash": 280.00,
  "total_cash": 692.50,
  "btc_balance": 0.00448123,
  "btc_value_usd": 350.35,
  "avg_buy_price": 75850.20,
  "current_spot_price": 78182.00,
  "acquisition_discount_pct": 2.98,
  "total_trades": 18,
  "benchmark_equity": 1018.40,
  "outperformance_usd": 24.45
}
```

### 5.2 `GET /api/portfolio/equity`
Array of `{ "date": "2026-09-01", "equity": 1000.0, "cash": 700.0, "reserve": 300.0, "btc_value": 0.0, "benchmark": 1000.0 }`.

### 5.3 `GET /api/portfolio/trades`
Array of `{ "trade_id": "...", "date": "2026-09-18", "side": "BUY", "signal": "AGGRESSIVE_ACCUMULATE", "spot_price": 78080.0, "gross_usd": 17.50, "fee_usd": 0.0175, "btc_amount": 0.0002239, "narrative": "..." }`.

---

## 6. Acceptance Criteria

- **AC-1:** DuckDB tables `paper_portfolio_balance`, `paper_portfolio_snapshots_daily`, and `paper_trade_ledger` initialized cleanly.
- **AC-2:** `bitcoin-data paper init --initial-cash 1000.0` initializes portfolio state with $1,000 cash.
- **AC-3:** `RiskGuard` halts execution if `data/state/PAPER_KILL_SWITCH` exists, or if cash is insufficient.
- **AC-4:** `bitcoin-data paper step` executes daily trade according to `DynamicReserveDCAStrategy` signal, deducts 10 bps fee, updates balances, and logs trade.
- **AC-5:** Dashboard REST endpoints `/api/portfolio`, `/api/portfolio/equity`, `/api/portfolio/trades` return valid JSON.
- **AC-6:** Web UI displays "Portfolio Simulasi" tab with Fincept-inspired KPI cards, equity curve vs benchmark chart, and trade blotter.
- **AC-7:** 100% test pass rate with zero regressions across prior phases (>626 existing tests).
- **AC-8:** Clean quality gates: `ruff check`, `ruff format --check`, `mypy src`.
- **AC-9:** Zero AI or sub-agent traces anywhere in codebase or documentation.
