# Phase 17: Agentic Autonomous Investment Committee, User Intelligence Ingestion & Automated Scheduling Pipeline

**Owner:** Engineering Team  
**Verification:** Automated Test Suite & Independent Peer Review  
**Branch:** `feat/phase-17-agentic-investment-committee`  
**Status:** Approved Architectural Specification  

---

## 1. Executive Summary & Strategic Objective

Phase 17 elevates the Bitcoin Data Platform from a passive quantitative analytical repository and rule-based paper execution sandbox into a fully autonomous, institutional-grade **Agentic Investment Decision System**. 

While Phase 15 proved paper trading execution under synthetic invariants and Phase 16 established multi-source macro/narrative awareness (3-Tier Composite MNI), capital allocation still lacked qualitative contextual reasoning, user-driven tactical insight ingestion, and 24/7 continuous autonomous orchestration.

Phase 17 resolves these final architectural hurdles by implementing three tightly integrated institutional pillars:

1. **Agentic Autonomous Investment Committee (Dual-System Neuro-Symbolic Architecture):**
   - Solves the critical **AutoHedge failure mode** ("Risk-as-a-Prompt"), where unconstrained LLM risk managers hallucinate arithmetic or rationalize disastrous trades.
   - Employs a **Dual-System Neuro-Symbolic design**: A qualitative multi-persona AI Investment Committee (Macro Strategist, Valuation Analyst, Risk Officer) debates market conditions and proposes tactical allocations; a deterministic, sub-millisecond Python symbolic solver (`Pre-Trade RiskGuard`) rigorously enforces mathematical invariants (solvency, daily reserve deployment caps, macro event buffers, black swan halts, and strict allocation clamping).
   - Generates and persists daily institutional **Investment Memorandums** with localized summaries in Bahasa Indonesia.

2. **User Intelligence Ingestion Engine:**
   - Establishes a first-class ingestion pipeline for user-provided market intelligence, macro hypotheses, research URLs, and institutional notes.
   - Ingests content via CLI, REST API (`POST /api/intelligence/ingest`), and Web Dashboard modal.
   - Parses, cleans, extracts metadata, dedupes via SHA-256, and stores entries in DuckDB (`user_market_intelligence`), injecting user alpha directly into the daily Investment Committee prompt context.

3. **Automated Scheduling Pipeline (Layered 24/7 VPS Orchestration):**
   - Implements a resilient, tiered automation pipeline engineered specifically for single-host VPS constraints (preventing CPU/RAM spikes and DuckDB write lock collisions).
   - **Hourly Pipeline (`*:05 UTC`):** Multi-source RSS news ingestion, lexical black swan scanning, and immediate Telegram alert dispatch for critical market emergencies.
   - **Daily Pipeline (`00:05 UTC`):** Market data increment, Fear & Greed fetch, ForexFactory calendar update, 3-tier MNI synthesis, Investment Committee deliberation, RiskGuard invariant clamping, paper trading execution step, equity mark-to-market, and daily executive digest dispatch.
   - **Weekly Pipeline (Monday `01:00 UTC`):** Portfolio drawdown audit, acquisition cost discount review, tactical reserve health analysis, and retrospective memo generation.
   - Hardened systemd oneshot services and monotonic timers (`bitcoin-data-hourly`, `bitcoin-data-daily`, `bitcoin-data-weekly`) with POSIX lockfile supervision.

---

## 2. Institutional Reference Architecture & Core Principles

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             EXTERNAL SOURCES                                                │
│  ┌──────────────────────┐  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌───────────────────┐  │
│  │ Curated Crypto RSS   │  │ ForexFactory Calendar   │  │ Alternative.me FNG &    │  │ User Intelligence │  │
│  │ (CoinDesk, Decrypt,  │  │ (CPI, NFP, FOMC, GDP)   │  │ Coin Metrics On-Chain   │  │ (URLs, Notes,     │  │
│  │  Cointelegraph, Mag) │  │ Actual vs Forecast JSON │  │ (MVRV, TxCount, Price)  │  │  Theses via UI/API)│  │
│  └──────────┬───────────┘  └────────────┬────────────┘  └────────────┬────────────┘  └─────────┬─────────┘  │
└─────────────┼───────────────────────────┼────────────────────────────┼─────────────────────────┼────────────┘
              │                           │                            │                         │
              ▼                           ▼                            ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   LAYERED AUTOMATED SCHEDULING PIPELINE                                     │
│  ┌───────────────────────────────┐  ┌─────────────────────────────────┐  ┌───────────────────────────────┐  │
│  │ Hourly Pipeline (*:05 UTC)    │  │ Daily Pipeline (00:05 UTC)      │  │ Weekly Pipeline (Mon 01:00)   │  │
│  │ - Multi-source news ingest    │  │ - Incremental candle sync       │  │ - Portfolio drawdown audit    │  │
│  │ - Lexical black swan scan     │  │ - Sentiment & macro fetch       │  │ - Cost discount review        │  │
│  │ - Emergency Telegram alert    │  │ - 3-Tier MNI synthesis          │  │ - Tactical reserve health     │  │
│  │ - <15s runtime, <150MB RAM    │  │ - Investment committee session  │  │ - Weekly retrospective memo   │  │
│  │ - Lock: pipeline_hourly.lock  │  │ - Lock: pipeline_daily.lock     │  │ - Lock: pipeline_weekly.lock  │  │
│  └───────────────────────────────┘  └────────────────┬────────────────┘  └───────────────────────────────┘  │
└──────────────────────────────────────────────────────┼──────────────────────────────────────────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                       DUAL-SYSTEM NEURO-SYMBOLIC INVESTMENT COMMITTEE ARCHITECTURE                          │
│                                                                                                             │
│   SYSTEM 1: QUALITATIVE NEURAL REASONER (AI Investment Committee)                                           │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Snapshot Ingestion: mart_macro_narrative_daily + user_market_intelligence + paper_portfolio_balance  │   │
│   │                                                                                                     │   │
│   │  ┌───────────────────────┐   ┌───────────────────────────┐   ┌───────────────────────────────────┐  │   │
│   │  │ Macro & Geopolitical  │   │ On-Chain & Fundamental    │   │ Technical Momentum &              │  │   │
│   │  │ Strategist Persona    │   │ Valuation Persona         │   │ Risk Officer Persona              │  │   │
│   │  │ - Liquidity, Rates,   │   │ - MVRV, Mayer Multiple,   │   │ - Volatility, Drawdown,           │  │   │
│   │  │   Inflation, Regimes  │   │   Realized Price, Age     │   │   Order-Book Liquidity            │  │   │
│   │  └───────────┬───────────┘   └─────────────┬─────────────┘   └─────────────────┬─────────────────┘  │   │
│   │              │                             │                                   │                    │   │
│   │              └──────────────────────┬──────┴───────────────────────────────────┘                    │   │
│   │                                     ▼                                                               │   │
│   │                 Consensus Deliberation & Institutional Memorandum                                    │   │
│   │                 Proposes: Target Stance & Unclamped Allocation ($D_{\text{proposed}}$)               │   │
│   └─────────────────────────────────────┬───────────────────────────────────────────────────────────────┘   │
│                                         │                                                                   │
│                                         ▼ [Proposed Allocation JSON: Amount, Stance, Rationale]             │
│                                                                                                             │
│   SYSTEM 2: DETERMINISTIC SYMBOLIC SOLVER (Pre-Trade RiskGuard Engine)                                      │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Pure Python Invariant Verifier (Sub-millisecond, Zero Hallucination, Strict Mathematical Bounds)    │   │
│   │                                                                                                     │   │
│   │  [Invariant 1] Solvency: $D \le \text{Available Cash}$ (Base + Reserve)                             │   │
│   │  [Invariant 2] Daily Tactical Reserve Cap: $D_{\text{tactical}} \le 0.15 \times \text{ReserveCash}$  │   │
│   │  [Invariant 3] Macro Proximity Buffer: $\pm 2\text{ hours around High-Impact Releases} \implies 0$ │   │
│   │  [Invariant 4] Black Swan Circuit Breaker: $\text{black\_swan\_flag} == \text{True} \implies \text{Halt}$ │
│   │  [Invariant 5] Max Drawdown Threshold: $\text{Drawdown} > 25\% \implies \text{Require Human Approval}$ │
│   │  [Invariant 6] Spot Price Freshness: $|\text{Spot} - \text{SMA}_{24}| / \text{SMA}_{24} \le 0.20$   │   │
│   │                                                                                                     │   │
│   │  DETERMINISTIC CLAMPING FUNCTION:                                                                   │   │
│   │  $D_{\text{executed}} = \min(D_{\text{proposed}}, D_{\text{max\_allowed}})$                           │   │
│   └─────────────────────────────────────┬───────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────┼───────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼ [Verified Execution Order]
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 EXECUTION, PERSISTENCE & SERVING LAYER                                      │
│  ┌───────────────────────────────┐  ┌─────────────────────────────────┐  ┌───────────────────────────────┐  │
│  │ Paper Trading Engine          │  │ DuckDB Analytical Storage       │  │ Web UI & REST API Serving     │  │
│  │ - Executes $D_{\text{executed}}$│  │ - investment_committee_memos    │  │ - /api/committee/*            │  │
│  │ - Deducts 10 bps fee          │  │ - investment_committee_votes    │  │ - /api/intelligence/*         │  │
│  │ - Updates shadow balances     │  │ - user_market_intelligence      │  │ - Investment Committee Tab    │  │
│  │ - Logs to paper_trade_ledger  │  │ - mart_committee_deliberation   │  │ - Anti-slop Fincept UI        │  │
│  └───────────────────────────────┘  └─────────────────────────────────┘  └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Core Architectural Principles

1. **The AutoHedge Lesson: Never Rely on "Risk-as-a-Prompt":**
   - In naive LLM trading systems, the risk manager is simply a system prompt instructed to "be conservative." Under market stress or compelling narrative generation, language models rationalize rule exceptions, misunderstand order of operations, and hallucinate arithmetic balances.
   - **Resolution:** System 1 (Neural Reasoner) has **zero authority to execute**. It only produces a *recommendation*. System 2 (Symbolic Solver) is an immutable Python state machine that checks invariants mathematically and clamps execution values before any trade reaches the ledger.

2. **Single-Host VPS Symbiosis (Ponytail Principle):**
   - High-concurrency architectures with separate microservice daemons cause memory exhaustion and DuckDB locking conflicts (`duckdb.IOException: Could not set lock on file`).
   - The scheduling architecture uses sequential systemd oneshot services with strict execution timeouts, explicit POSIX run locks (`/srv/data/bitcoin-data-platform/state/locks/*.lock`), and non-overlapping schedules.
   - Memory footprint is strictly capped via `MemoryMax=1.5G` in systemd unit definitions.

3. **Verifiable Information Provenance & Anti-Slop:**
   - User-injected intelligence must retain original URLs, timestamps, author notes, and calculated SHA-256 hashes.
   - Dashboard UI strictly follows anti-slop rules: dark palette (`#0B0E14`), hairline borders (`rgba(255,255,255,0.07)`), `font-variant-numeric: tabular-nums` on all numbers, zero gradient fog, and verifiable hyperlinks (`target="_blank"`).

4. **Dual-Language Operational Standard:**
   - Quantitative outputs, structured JSON schemas, API contracts, and log events are strictly in English.
   - Executive summaries, Investment Memorandums, and Telegram alert broadcasts are generated in **Bahasa Indonesia** to serve primary user operations.

---

## 3. Scope of Work

### In Scope

1. **Agentic Investment Committee (`src/bitcoin_data_platform/committee/`):**
   - Multi-persona qualitative reasoning engine (`MacroStrategist`, `ValuationAnalyst`, `RiskOfficer`).
   - Institutional Investment Memorandum generator with consensus voting and Bahasa Indonesia executive summary.
   - LLM provider abstraction (pluggable: `HermesClient`, `OpenAICompatibleClient`, and offline deterministic `MockCommitteeClient` for reproducible unit testing).

2. **Neuro-Symbolic Pre-Trade Gatekeeper (`src/bitcoin_data_platform/committee/invariant_solver.py` & `paper/risk_guard.py`):**
   - Pure Python mathematical solver validating 6 hard invariants.
   - Allocation clamping logic enforcing daily tactical reserve ceiling ($\le 15\%$ daily reserve deployment).
   - Execution audit receipt logging (proposed vs. clamped delta and rule justifications).

3. **User Intelligence Ingestion Engine (`src/bitcoin_data_platform/intelligence/`):**
   - CLI command `bitcoin-data intelligence ingest` and `list`.
   - REST API endpoints `POST /api/intelligence/ingest` and `GET /api/intelligence/list`.
   - Lightweight URL content extractor (using `httpx` + stdlib `html.parser` without headless browser overhead).
   - SHA-256 deduplication and DuckDB persistence in `user_market_intelligence`.

4. **Layered Automated Scheduling Pipeline (`src/bitcoin_data_platform/pipeline/` & `infra/systemd/`):**
   - Orchestrator module coordinating hourly, daily, and weekly job DAGs.
   - Systemd units and timers:
     - `bitcoin-data-hourly.service` / `.timer` (`*:05 UTC`)
     - `bitcoin-data-daily.service` / `.timer` (`00:05 UTC`)
     - `bitcoin-data-weekly.service` / `.timer` (`Mon 01:00 UTC`)
   - File-based run lock manager with PID liveness verification to prevent overlapping runs.
   - Telegram alerting integration on job failure (`OnFailure=bitcoin-data-failure@%n.service`).

5. **DuckDB Database Layer (`src/bitcoin_data_platform/storage/duckdb_manager.py`):**
   - Schema creation for `user_market_intelligence`, `investment_committee_memos`, `investment_committee_votes`.
   - Analytical view `mart_committee_deliberation_daily` joining market price, MVRV, FNG, MNI, committee decisions, and active user alpha.

6. **Web Dashboard Integration (`src/bitcoin_data_platform/dashboard/`):**
   - Dedicated "Investment Committee" tab (`assets/index.html`).
   - Memorandum viewer with persona vote cards, consensus meter, and execution receipts.
   - Neuro-Symbolic Invariant Verification panel comparing Neural Proposal vs. Clamped Allocation.
   - User Intelligence submission modal and historical alpha blotter with clickable source links.
   - REST API routes: `/api/committee/latest`, `/api/committee/history`, `/api/intelligence/ingest`, `/api/intelligence/list`, `/api/pipeline/schedule`.

7. **Comprehensive Test Suite & Verification:**
   - $\ge 56$ new unit, integration, and contract tests across 7 test groups.
   - 100% test pass rate with zero regression across all 712 existing tests.
   - Quality gates: `ruff check`, `ruff format --check`, `mypy src`.

### Out of Scope

- Live real-money exchange API key integration (Phase 17 operates on the proven Phase 15 paper sandbox).
- Heavy headless browser automation (`playwright`, `selenium`, `browser-use`).
- Multi-host distributed orchestration clusters (Kubernetes, Airflow, Celery).
- Real-time tick-by-tick high-frequency order book trading.

---

## 4. Mathematical & Algorithmic Formulation

### 4.1 Neural Committee Qualitative Reasoner & Consensus Synthesis

The Investment Committee evaluates conformed daily market features:
$$\mathbf{X}_t = \{ \text{Price}_t, \text{SMA200}_t, \text{MM}_t, \text{MVRV}_t, \text{FNG}_t, \text{MNI}_t, \Delta_{\text{macro}}, \mathbf{News}_t, \mathbf{UserAlpha}_t \}$$

#### Multi-Persona Scoring Engine
Each persona $p \in \{ \text{Strategist}, \text{Valuation}, \text{RiskOfficer} \}$ outputs a normalized directional score $s_p \in [-1.0, +1.0]$, confidence $c_p \in [0.0, 1.0]$, and proposed tactical allocation multiplier $m_p \in [0.0, 2.0]$:

1. **Macro & Geopolitical Strategist ($s_{\text{macro}}$):**
   Evaluates global liquidity conditions, Fed interest rate expectations, inflation surprises, and geopolitical risks.
   $$s_{\text{macro}} = 0.60 \cdot MNI_t + 0.40 \cdot \tanh\left(\sum_{k} \text{PillarWeight}_k \cdot \text{UserAlphaPolarity}_k\right)$$

2. **On-Chain & Fundamental Valuation Analyst ($s_{\text{val}}$):**
   Evaluates long-term structural value against historical Bitcoin cost-basis distribution.
   $$s_{\text{val}} = 0.50 \cdot S_{\text{mvrv}}(MVRV_t) + 0.50 \cdot S_{\text{mm}}(MM_t)$$
   Where $S_{\text{mvrv}}$ maps $MVRV < 1.0 \implies +1.0$, $MVRV > 2.8 \implies -1.0$.

3. **Technical Momentum & Risk Officer ($s_{\text{risk}}$):**
   Evaluates drawdown trajectory, price volatility, and short-term capital exposure risk.
   $$s_{\text{risk}} = -1.0 \cdot \mathbf{1}_{\{\text{black\_swan\_flag}\}} + 0.50 \cdot \left(\frac{\text{Price}_t - \text{SMA200}_t}{\text{SMA200}_t}\right) - 0.50 \cdot \text{DrawdownPct}_t$$

#### Committee Consensus Synthesis
The Committee Chair computes the weighted consensus score $S_{\text{consensus}}$ and the unconstrained Neural Allocation Proposal $D_{\text{proposed}}$:

$$S_{\text{consensus}} = \frac{\sum_{p} w_p c_p s_p}{\sum_{p} w_p c_p} \in [-1.0, +1.0]$$
Where baseline weights are $w_{\text{macro}} = 0.35$, $w_{\text{val}} = 0.35$, $w_{\text{risk}} = 0.30$.

The Neural Reasoner proposes daily tactical capital deployment in USD:
$$D_{\text{proposed}} = \text{BaseDCA} \times \phi(S_{\text{consensus}}) + \text{TacticalPool} \times \psi(S_{\text{consensus}})$$
Where:
- $\phi(S)$ is the baseline DCA multiplier ($\phi(S) \in [0.5, 1.5]$ for $S \ge -0.20$, else $0.0$).
- $\psi(S)$ is the neural tactical deployment appetite ($\psi(S) \in [0.0, 0.30]$ when $S > 0.40$).

---

### 4.2 Symbolic Pre-Trade RiskGuard Solver: Invariant Verification & Allocation Clamping

System 2 receives $D_{\text{proposed}}$ and the proposed stance from System 1. The symbolic solver evaluates six mathematical invariants in sub-millisecond Python runtime.

#### Invariant Definitions

1. **Invariant 1: Absolute Solvency:**
   The total proposed order cost cannot exceed current liquid fiat cash balances:
   $$D_{\text{proposed}} \le C_{\text{base}} + C_{\text{reserve}}$$
   If violated, $D$ is clamped to total available cash: $D = \max(0.0, C_{\text{base}} + C_{\text{reserve}})$.

2. **Invariant 2: Daily Tactical Reserve Deployment Ceiling:**
   To prevent thesis-driven exhaustion of reserves during extended bear market bottoms, maximum tactical reserve deployment in any 24-hour cycle is strictly capped at 15% of the existing reserve balance:
   $$D_{\text{tactical\_max}} = 0.15 \times C_{\text{reserve}}$$
   Any proposed tactical deployment exceeding this threshold is clamped:
   $$D_{\text{tactical\_clamped}} = \min(D_{\text{proposed\_tactical}}, D_{\text{tactical\_max}})$$

3. **Invariant 3: Macro Announcement Proximity Circuit Breaker:**
   High-impact macro events create extreme bid-ask spread widening and liquidity gaps.
   Let $t_{\text{now}}$ be execution time and $t_{\text{macro}}$ be the timestamp of any scheduled High-Impact event (FOMC, CPI, NFP):
   $$\text{If } |t_{\text{now}} - t_{\text{macro}}| \le 120 \text{ minutes} \implies D_{\text{executed}} = 0.0 \text{ (Order Blocked)}$$

4. **Invariant 4: Black Swan & Critical Regulatory Sentinel:**
   If lexical sentinel or user intelligence tags an active critical emergency (exchange insolvency, major stablecoin depeg, nationwide crypto ban):
   $$\text{If } \text{black\_swan\_flag} == \text{True} \implies D_{\text{executed}} = 0.0 \text{ (Circuit Breaker Tripped)}$$

5. **Invariant 5: Severe Portfolio Drawdown Brake:**
   Let $\text{MDD}_t = \frac{\text{Equity}_t - \max_{\tau \le t} \text{Equity}_\tau}{\max_{\tau \le t} \text{Equity}_\tau}$.
   $$\text{If } \text{MDD}_t < -0.25 \implies \text{Tactical Deployment Frozen } (D_{\text{tactical}} = 0.0)$$

6. **Invariant 6: Spot Price Freshness & Deviation Bounds:**
   $$\left|\frac{\text{SpotPrice} - \text{LastClose}}{\text{LastClose}}\right| \le 0.20$$
   Prevents executing orders against stale or corrupted API price feeds.

#### Deterministic Clamping Equation
The final capital deployment executed by the paper trading engine is given by:

$$D_{\text{executed}} = \begin{cases} 
0.0 & \text{if Invariant 3, 4, or 6 fails} \\
\min\left(D_{\text{base\_dca}}, C_{\text{base}}\right) + \min\left(D_{\text{proposed\_tactical}}, 0.15 \times C_{\text{reserve}}, C_{\text{reserve}}\right) & \text{if Invariant 1, 2, 5 pass}
\end{cases}$$

If $D_{\text{executed}} < D_{\text{proposed}}$, the solver flags `allocation_clamped = True` and records the exact mathematical difference:
$$\Delta_{\text{clamped}} = D_{\text{proposed}} - D_{\text{executed}}$$
along with the triggered invariant rule in the execution memorandum.

---

### 4.3 User Intelligence Impact Scoring & Weighting Formula

When a user submits intelligence item $u \in \mathbf{U}_t$, it is normalized into a directional alpha vector:
$$\mathbf{z}_u = \{ \text{Pillar}_u, \text{Sentiment}_u \in [-1.0, +1.0], \text{Confidence}_u \in [0.0, 1.0], \text{AgeHours}_u \}$$

The time-decayed aggregate User Intelligence Score $S_{\text{user}}$ over an active 72-hour window is:

$$S_{\text{user}} = \frac{\sum_{u \in \mathbf{U}_t} \text{Confidence}_u \cdot \text{Sentiment}_u \cdot e^{-\lambda \cdot \text{AgeHours}_u}}{\sum_{u \in \mathbf{U}_t} \text{Confidence}_u \cdot e^{-\lambda \cdot \text{AgeHours}_u} + \epsilon}$$
Where $\lambda = \frac{\ln(2)}{24}$ (24-hour half-life).

If $|S_{\text{user}}| > 0.60$ and $\sum \text{Confidence}_u \ge 1.5$, the User Intelligence directly shifts the Macro Strategist's score by up to $\pm 0.20$.

---

### 4.4 Tiered Scheduling Execution Windows & Concurrency Locks

To eliminate process collisions on a single-host VPS:
- Every pipeline job acquires an exclusive flock on `/srv/data/bitcoin-data-platform/state/locks/<tier>.lock`.
- If an existing lockfile is active, the job checks whether the PID in the lockfile is actively running via `os.kill(pid, 0)`.
- If the PID is dead, the stale lock is safely cleared with a structured log warning.
- If the PID is active, the job exits cleanly with **Exit Code 6** (`CONCURRENT_RUN_LOCK`).

```
00:00 UTC ── Candle Close
00:05 UTC ── Daily Pipeline: Incremental Sync -> FNG/Macro -> MNI -> Committee Deliberate -> Clamped Step -> Memo
*:05 UTC  ── Hourly Pipeline: RSS Feeds -> Sentinel Black Swan Scan -> Emergency Telegram Alert
Mon 01:00 ── Weekly Pipeline: Drawdown Audit -> Cost Discount Review -> Reserve Health -> Weekly Retrospective Memo
```

---

## 5. Database Architecture (`data/state/platform.duckdb`)

### 5.1 Table: `user_market_intelligence`
Stores all verified user-submitted research, market intelligence, theses, and extracted article metadata.

```sql
CREATE TABLE IF NOT EXISTS user_market_intelligence (
    intelligence_id VARCHAR PRIMARY KEY,        -- SHA-256(source_url:user_thesis:created_at)
    source_url VARCHAR,                         -- Optional verified original URL
    title VARCHAR NOT NULL,                     -- Title or headline summary
    user_thesis VARCHAR NOT NULL,               -- User analytical thesis / rationale
    raw_content VARCHAR NOT NULL DEFAULT '',    -- Extracted article text or markdown
    pillar VARCHAR NOT NULL,                    -- 'MACRO_LIQUIDITY', 'REGULATORY', 'SECURITY_EXPLOIT', 'INSTITUTIONAL', 'GEOPOLITICAL', 'USER_THESIS'
    sentiment_bias DOUBLE NOT NULL,             -- -1.0 (bearish) to +1.0 (bullish)
    confidence_score DOUBLE NOT NULL,           -- 0.0 to 1.0
    tags VARCHAR NOT NULL DEFAULT '',           -- Comma-separated tags
    created_at_utc TIMESTAMPTZ NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);
```

### 5.2 Table: `investment_committee_memos`
Stores daily institutional Investment Memorandums generated by the committee, including proposed allocations, symbolic clamping results, and Indonesian summaries.

```sql
CREATE TABLE IF NOT EXISTS investment_committee_memos (
    memo_id VARCHAR PRIMARY KEY,                -- UUID v4
    memo_date DATE NOT NULL,                    -- Target trade date (YYYY-MM-DD)
    created_at_utc TIMESTAMPTZ NOT NULL,
    market_regime VARCHAR NOT NULL,             -- 'RISK_ON_EXPANSION', 'CAUTIOUS_BULL', 'NEUTRAL_CHOP', 'RISK_OFF_DEFENSE', 'BLACK_SWAN_CRISIS'
    composite_mni DOUBLE NOT NULL,              -- Latest MNI score
    consensus_score DOUBLE NOT NULL,            -- Weighted committee consensus (-1.0 to +1.0)
    executive_summary_id VARCHAR NOT NULL,      -- Executive summary in Bahasa Indonesia
    macro_thesis VARCHAR NOT NULL,              -- Macroeconomic & liquidity rationale
    valuation_thesis VARCHAR NOT NULL,          -- On-chain valuation & multiple rationale
    technical_thesis VARCHAR NOT NULL,          -- Technical momentum & volatility rationale
    dissenting_opinions VARCHAR NOT NULL,       -- Notable disagreements among personas
    proposed_action VARCHAR NOT NULL,           -- 'AGGRESSIVE_ACCUMULATE', 'OPPORTUNISTIC_BUY', 'STANDARD_DCA', 'DEFENSIVE_HOLD', 'EMERGENCY_HALT'
    proposed_allocation_usd DOUBLE NOT NULL,    -- Allocation proposed by neural reasoner
    clamped_allocation_usd DOUBLE NOT NULL,     -- Allocation validated/clamped by symbolic solver
    allocation_clamped BOOLEAN NOT NULL,        -- True if symbolic solver clamped the proposal
    clamping_reason VARCHAR,                    -- Explanation if clamped (e.g. 'Daily 15% reserve cap exceeded')
    risk_guard_passed BOOLEAN NOT NULL,         -- True if all pre-trade safety invariants passed
    memo_markdown VARCHAR NOT NULL              -- Complete rendered Markdown memorandum
);
```

### 5.3 Table: `investment_committee_votes`
Stores individual persona ballots, target allocations, confidence levels, and specific arguments for each deliberation session.

```sql
CREATE TABLE IF NOT EXISTS investment_committee_votes (
    vote_id VARCHAR PRIMARY KEY,                -- UUID v4
    memo_id VARCHAR NOT NULL,                   -- Foreign key referencing investment_committee_memos(memo_id)
    persona VARCHAR NOT NULL,                   -- 'MACRO_STRATEGIST', 'VALUATION_ANALYST', 'RISK_OFFICER'
    stance VARCHAR NOT NULL,                    -- 'BULLISH', 'MODERATELY_BULLISH', 'NEUTRAL', 'DEFENSIVE', 'CRISIS'
    target_allocation_usd DOUBLE NOT NULL,      -- Persona's ideal allocation recommendation
    confidence DOUBLE NOT NULL,                 -- Persona confidence score (0.0 to 1.0)
    rationale VARCHAR NOT NULL,                 -- Persona's core argument
    voted_at_utc TIMESTAMPTZ NOT NULL
);
```

### 5.4 Analytical View: `mart_committee_deliberation_daily`
Conformed analytical view joining market prices, valuation ratios, sentiment indices, macro narrative regimes, latest committee decisions, and active user intelligence count.

```sql
CREATE OR REPLACE VIEW mart_committee_deliberation_daily AS
SELECT
    m.trade_date_utc,
    m.market_close_usd,
    m.sma_200,
    m.mayer_multiple,
    m.mvrv_ratio,
    m.fng_value,
    m.composite_mni,
    m.macro_regime,
    m.black_swan_flag,
    c.memo_id,
    c.consensus_score,
    c.proposed_action,
    c.proposed_allocation_usd,
    c.clamped_allocation_usd,
    c.allocation_clamped,
    c.clamping_reason,
    c.risk_guard_passed,
    c.executive_summary_id,
    COALESCE(u.active_user_alpha_count, 0) AS active_user_alpha_count
FROM mart_macro_narrative_daily m
LEFT JOIN investment_committee_memos c
    ON m.trade_date_utc = c.memo_date
LEFT JOIN (
    SELECT 
        CAST(created_at_utc AS DATE) AS alpha_date, 
        COUNT(*) AS active_user_alpha_count
    FROM user_market_intelligence
    WHERE is_active = TRUE
    GROUP BY CAST(created_at_utc AS DATE)
) u ON m.trade_date_utc = u.alpha_date;
```

---

## 6. Python Component & Module Layout

```
src/bitcoin_data_platform/
├── committee/                                # [NEW PACKAGE] Phase 17 Investment Committee
│   ├── __init__.py                           # Package exports
│   ├── models.py                             # Committee contracts, Ballot, Memo, Stance enums
│   ├── personas.py                           # Persona definitions (Strategist, Valuation, Risk)
│   ├── engine.py                             # Deliberation coordinator & memorandum builder
│   ├── invariant_solver.py                   # Pure Python symbolic invariant solver & clamp engine
│   ├── prompt_templates.py                   # Structured prompts & Bahasa Indonesia summary templates
│   └── cli.py                                # CLI subcommands for 'bitcoin-data committee'
├── intelligence/                             # [NEW PACKAGE] Phase 17 User Intelligence Ingestion
│   ├── __init__.py                           # Package exports
│   ├── models.py                             # UserIntelligenceRecord contracts & dataclasses
│   ├── ingester.py                           # URL fetcher, text extractor, deduplicator, storer
│   └── cli.py                                # CLI subcommands for 'bitcoin-data intelligence'
├── pipeline/                                 # [NEW PACKAGE] Phase 17 Automated Scheduling Pipeline
│   ├── __init__.py                           # Package exports
│   ├── models.py                             # PipelineJob, RunReport, ScheduleTier enums
│   ├── lock_manager.py                       # Safe POSIX file-locking & PID liveness supervisor
│   ├── orchestrator.py                       # Hourly, Daily, and Weekly pipeline DAG runners
│   └── cli.py                                # CLI subcommands for 'bitcoin-data pipeline'
├── paper/
│   ├── risk_guard.py                         # [MODIFIED] Wired with invariant_solver clamp hook
│   └── engine.py                             # [MODIFIED] Integrates committee memo tracking
├── storage/
│   └── duckdb_manager.py                     # [MODIFIED] Added Phase 17 tables, views, and helpers
├── dashboard/
│   ├── server.py                             # [MODIFIED] Added /api/committee/*, /api/intelligence/*, /api/pipeline/*
│   └── assets/
│       └── index.html                        # [MODIFIED] Added "Investment Committee" tab & Intelligence modal
├── infra/
│   └── systemd/
│       ├── bitcoin-data-hourly.service       # [NEW] Hourly news & sentinel service
│       ├── bitcoin-data-hourly.timer         # [NEW] Hourly timer (*:05 UTC)
│       ├── bitcoin-data-daily.service        # [NEW] Daily committee & execution service
│       ├── bitcoin-data-daily.timer          # [NEW] Daily timer (00:05 UTC)
│       ├── bitcoin-data-weekly.service       # [NEW] Weekly portfolio audit service
│       └── bitcoin-data-weekly.timer         # [NEW] Weekly timer (Mon 01:00 UTC)
└── cli.py                                    # [MODIFIED] Registered committee, intelligence, pipeline CLI
```

### 6.1 Data Contracts & Models

#### Committee Contracts (`src/bitcoin_data_platform/committee/models.py`)

```python
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any

class CommitteePersona(StrEnum):
    MACRO_STRATEGIST = "MACRO_STRATEGIST"
    VALUATION_ANALYST = "VALUATION_ANALYST"
    RISK_OFFICER = "RISK_OFFICER"

class MemberStance(StrEnum):
    BULLISH = "BULLISH"
    MODERATELY_BULLISH = "MODERATELY_BULLISH"
    NEUTRAL = "NEUTRAL"
    DEFENSIVE = "DEFENSIVE"
    CRISIS = "CRISIS"

class AllocationAction(StrEnum):
    AGGRESSIVE_ACCUMULATE = "AGGRESSIVE_ACCUMULATE"
    OPPORTUNISTIC_BUY = "OPPORTUNISTIC_BUY"
    STANDARD_DCA = "STANDARD_DCA"
    DEFENSIVE_HOLD = "DEFENSIVE_HOLD"
    EMERGENCY_HALT = "EMERGENCY_HALT"

@dataclass(frozen=True)
class PersonaVote:
    vote_id: str
    memo_id: str
    persona: CommitteePersona
    stance: MemberStance
    target_allocation_usd: float
    confidence: float  # 0.0 to 1.0
    rationale: str
    voted_at_utc: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "vote_id": self.vote_id,
            "memo_id": self.memo_id,
            "persona": self.persona.value,
            "stance": self.stance.value,
            "target_allocation_usd": self.target_allocation_usd,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "voted_at_utc": self.voted_at_utc.isoformat(),
        }

@dataclass(frozen=True)
class ClampingReceipt:
    proposed_allocation_usd: float
    clamped_allocation_usd: float
    is_clamped: bool
    triggered_rules: list[str] = field(default_factory=list)
    explanation: str = ""

@dataclass(frozen=True)
class InvestmentMemorandum:
    memo_id: str
    memo_date: date
    created_at_utc: datetime
    market_regime: str
    composite_mni: float
    consensus_score: float
    executive_summary_id: str
    macro_thesis: str
    valuation_thesis: str
    technical_thesis: str
    dissenting_opinions: str
    proposed_action: AllocationAction
    proposed_allocation_usd: float
    clamped_allocation_usd: float
    allocation_clamped: bool
    clamping_reason: str | None
    risk_guard_passed: bool
    memo_markdown: str
    votes: list[PersonaVote] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "memo_id": self.memo_id,
            "memo_date": self.memo_date.isoformat(),
            "created_at_utc": self.created_at_utc.isoformat(),
            "market_regime": self.market_regime,
            "composite_mni": self.composite_mni,
            "consensus_score": self.consensus_score,
            "executive_summary_id": self.executive_summary_id,
            "macro_thesis": self.macro_thesis,
            "valuation_thesis": self.valuation_thesis,
            "technical_thesis": self.technical_thesis,
            "dissenting_opinions": self.dissenting_opinions,
            "proposed_action": self.proposed_action.value,
            "proposed_allocation_usd": self.proposed_allocation_usd,
            "clamped_allocation_usd": self.clamped_allocation_usd,
            "allocation_clamped": self.allocation_clamped,
            "clamping_reason": self.clamping_reason,
            "risk_guard_passed": self.risk_guard_passed,
            "memo_markdown": self.memo_markdown,
            "votes": [v.to_dict() for v in self.votes],
        }
```

#### User Intelligence Contracts (`src/bitcoin_data_platform/intelligence/models.py`)

```python
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

class IntelligencePillar(StrEnum):
    MACRO_LIQUIDITY = "MACRO_LIQUIDITY"
    REGULATORY = "REGULATORY"
    SECURITY_EXPLOIT = "SECURITY_EXPLOIT"
    INSTITUTIONAL = "INSTITUTIONAL"
    GEOPOLITICAL = "GEOPOLITICAL"
    USER_THESIS = "USER_THESIS"

@dataclass(frozen=True)
class UserIntelligenceRecord:
    intelligence_id: str
    source_url: str | None
    title: str
    user_thesis: str
    raw_content: str
    pillar: IntelligencePillar
    sentiment_bias: float  # -1.0 to +1.0
    confidence_score: float  # 0.0 to 1.0
    tags: list[str]
    created_at_utc: datetime
    is_active: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "intelligence_id": self.intelligence_id,
            "source_url": self.source_url,
            "title": self.title,
            "user_thesis": self.user_thesis,
            "raw_content": self.raw_content,
            "pillar": self.pillar.value,
            "sentiment_bias": self.sentiment_bias,
            "confidence_score": self.confidence_score,
            "tags": self.tags,
            "created_at_utc": self.created_at_utc.isoformat(),
            "is_active": self.is_active,
        }
```

#### Pipeline Contracts (`src/bitcoin_data_platform/pipeline/models.py`)

```python
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

class PipelineCadence(StrEnum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"

class JobStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    LOCKED = "LOCKED"

@dataclass(frozen=True)
class JobStepResult:
    step_name: str
    status: JobStatus
    duration_seconds: float
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class PipelineRunReport:
    run_id: str
    cadence: PipelineCadence
    started_at_utc: datetime
    completed_at_utc: datetime
    overall_status: JobStatus
    steps: list[JobStepResult]
    total_duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "cadence": self.cadence.value,
            "started_at_utc": self.started_at_utc.isoformat(),
            "completed_at_utc": self.completed_at_utc.isoformat(),
            "overall_status": self.overall_status.value,
            "total_duration_seconds": self.total_duration_seconds,
            "steps": [
                {
                    "step_name": s.step_name,
                    "status": s.status.value,
                    "duration_seconds": s.duration_seconds,
                    "error_message": s.error_message,
                    "metadata": s.metadata,
                }
                for s in self.steps
            ],
        }
```

---

### 6.2 User Intelligence Ingestion Engine (`src/bitcoin_data_platform/intelligence/ingester.py`)

The ingester validates incoming user intelligence, optionally fetches and cleans article markdown/text without browser overhead, computes deterministic SHA-256 deduplication IDs, and persists the record into DuckDB:

```python
import hashlib
from datetime import UTC, datetime
from typing import Any
import httpx

from bitcoin_data_platform.intelligence.models import IntelligencePillar, UserIntelligenceRecord
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager

class IntelligenceIngester:
    def __init__(self, db_manager: DuckDBManager, http_client: httpx.Client | None = None) -> None:
        self.db = db_manager
        self.http_client = http_client or httpx.Client(timeout=10.0, follow_redirects=True)

    def extract_article_content(self, url: str) -> tuple[str, str]:
        """Fetch article HTML and extract clean text using stdlib html.parser (zero heavy dependencies)."""
        try:
            resp = self.http_client.get(url, headers={"User-Agent": "BitcoinDataPlatform/1.0"})
            resp.raise_for_status()
            # Lightweight extraction of <title> and paragraph text
            ...
            return title, clean_text[:4000]
        except Exception as exc:
            return "", ""

    def ingest(
        self,
        *,
        title: str,
        user_thesis: str,
        source_url: str | None = None,
        pillar: IntelligencePillar = IntelligencePillar.USER_THESIS,
        sentiment_bias: float = 0.0,
        confidence_score: float = 0.8,
        tags: list[str] | None = None,
        created_at_utc: datetime | None = None,
    ) -> UserIntelligenceRecord:
        now = created_at_utc or datetime.now(UTC)
        clean_tags = tags or []
        
        extracted_title = ""
        extracted_content = ""
        if source_url:
            extracted_title, extracted_content = self.extract_article_content(source_url)
        
        final_title = title or extracted_title or "User Market Intelligence Note"
        
        # Deterministic SHA-256 ID
        raw_key = f"{source_url or ''}:{user_thesis}:{now.date().isoformat()}"
        intel_id = hashlib.sha256(raw_key.encode()).hexdigest()[:16]
        
        record = UserIntelligenceRecord(
            intelligence_id=intel_id,
            source_url=source_url,
            title=final_title,
            user_thesis=user_thesis,
            raw_content=extracted_content,
            pillar=pillar,
            sentiment_bias=max(-1.0, min(1.0, sentiment_bias)),
            confidence_score=max(0.0, min(1.0, confidence_score)),
            tags=clean_tags,
            created_at_utc=now,
            is_active=True,
        )
        
        self._persist_record(record)
        return record

    def _persist_record(self, record: UserIntelligenceRecord) -> None:
        sql = """
        INSERT INTO user_market_intelligence (
            intelligence_id, source_url, title, user_thesis, raw_content,
            pillar, sentiment_bias, confidence_score, tags, created_at_utc, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (intelligence_id) DO UPDATE SET
            user_thesis = EXCLUDED.user_thesis,
            sentiment_bias = EXCLUDED.sentiment_bias,
            confidence_score = EXCLUDED.confidence_score,
            tags = EXCLUDED.tags;
        """
        # Execute parameterized SQL
        ...
```

---

### 6.3 Multi-Persona Committee Deliberation Engine (`src/bitcoin_data_platform/committee/engine.py`)

Coordinates qualitative evaluations from three distinct expert personas, synthesizes consensus, invokes the symbolic clamp solver, formats the Indonesian executive memorandum, and stores results:

```python
class InvestmentCommitteeEngine:
    def __init__(
        self,
        db_manager: DuckDBManager,
        invariant_solver: InvariantSolver | None = None,
        llm_client: Any | None = None,
    ) -> None:
        self.db = db_manager
        self.solver = invariant_solver or InvariantSolver()
        self.llm = llm_client

    def deliberate(
        self,
        target_date: date,
        portfolio_state: dict[str, float],
        dry_run: bool = False,
    ) -> InvestmentMemorandum:
        # 1. Fetch conformed daily snapshot from mart_macro_narrative_daily
        snapshot = self._load_daily_snapshot(target_date)
        
        # 2. Fetch active user intelligence for the window
        user_alpha = self._load_user_intelligence(target_date)
        
        # 3. Deliberate across personas
        votes: list[PersonaVote] = [
            MacroStrategistPersona.evaluate(snapshot, user_alpha),
            ValuationAnalystPersona.evaluate(snapshot),
            RiskOfficerPersona.evaluate(snapshot, portfolio_state),
        ]
        
        # 4. Synthesize consensus & unconstrained neural proposal
        consensus_score = self._calculate_consensus(votes)
        proposed_action, proposed_usd = self._derive_proposal(consensus_score, portfolio_state)
        
        # 5. Deterministic Symbolic Verification & Clamping
        receipt = self.solver.solve_and_clamp(
            proposed_usd=proposed_usd,
            available_base_cash=portfolio_state["base_cash"],
            available_reserve_cash=portfolio_state["reserve_cash"],
            has_black_swan=snapshot.get("black_swan_flag", False),
            macro_proximity_minutes=snapshot.get("macro_proximity_minutes"),
            portfolio_drawdown=portfolio_state.get("drawdown_pct", 0.0),
            spot_price=snapshot.get("market_close_usd", 0.0),
            last_validated_price=snapshot.get("last_close_usd", 0.0),
        )
        
        # 6. Generate Markdown & Bahasa Indonesia Executive Summary
        memo_md, summary_id = self._render_memorandum(
            target_date=target_date,
            snapshot=snapshot,
            votes=votes,
            consensus_score=consensus_score,
            proposed_action=proposed_action,
            receipt=receipt,
        )
        
        memo = InvestmentMemorandum(
            memo_id=str(uuid.uuid4()),
            memo_date=target_date,
            created_at_utc=datetime.now(UTC),
            market_regime=snapshot.get("macro_regime", "NEUTRAL_CHOP"),
            composite_mni=snapshot.get("composite_mni", 0.0),
            consensus_score=consensus_score,
            executive_summary_id=summary_id,
            macro_thesis=votes[0].rationale,
            valuation_thesis=votes[1].rationale,
            technical_thesis=votes[2].rationale,
            dissenting_opinions=self._extract_dissent(votes),
            proposed_action=proposed_action,
            proposed_allocation_usd=receipt.proposed_allocation_usd,
            clamped_allocation_usd=receipt.clamped_allocation_usd,
            allocation_clamped=receipt.is_clamped,
            clamping_reason=receipt.explanation if receipt.is_clamped else None,
            risk_guard_passed=(receipt.clamped_allocation_usd > 0 or not snapshot.get("black_swan_flag")),
            memo_markdown=memo_md,
            votes=votes,
        )
        
        if not dry_run:
            self._persist_memorandum(memo)
            
        return memo
```

---

### 6.4 Neuro-Symbolic Invariant Solver (`src/bitcoin_data_platform/committee/invariant_solver.py`)

Pure Python deterministic state machine implementing all mathematical invariants without external LLM dependencies:

```python
class InvariantSolver:
    MAX_DAILY_RESERVE_PCT = 0.15  # Max 15% of tactical reserve pool deployed per day
    MAX_DRAWDOWN_LIMIT = 0.25     # Freeze tactical pool if drawdown > 25%
    MACRO_BUFFER_MINUTES = 120    # +/- 2 hours around high-impact macro events
    MAX_PRICE_DEVIATION = 0.20    # 20% price reasonableness check

    def solve_and_clamp(
        self,
        *,
        proposed_usd: float,
        available_base_cash: float,
        available_reserve_cash: float,
        has_black_swan: bool,
        macro_proximity_minutes: int | None,
        portfolio_drawdown: float,
        spot_price: float,
        last_validated_price: float,
    ) -> ClampingReceipt:
        triggered: list[str] = []
        
        # Invariant 4: Black Swan Sentinel Halt
        if has_black_swan:
            return ClampingReceipt(
                proposed_allocation_usd=proposed_usd,
                clamped_allocation_usd=0.0,
                is_clamped=True,
                triggered_rules=["CIRCUIT_BREAKER: Active Black Swan / Critical Sentinel"],
                explanation="Sistem membekukan alokasi modal karena terdeteksi anomali kritis / black swan.",
            )

        # Invariant 3: Macro Proximity Buffer (+/- 2h)
        if macro_proximity_minutes is not None and abs(macro_proximity_minutes) <= self.MACRO_BUFFER_MINUTES:
            return ClampingReceipt(
                proposed_allocation_usd=proposed_usd,
                clamped_allocation_usd=0.0,
                is_clamped=True,
                triggered_rules=["CIRCUIT_BREAKER: Proksimitas Rilis Makro Tinggi (+/- 2 jam)"],
                explanation=f"Jendela rilis makro berdampak tinggi aktif ({macro_proximity_minutes} menit). Alokasi ditahan.",
            )

        # Invariant 6: Price Freshness & Deviation Check
        if last_validated_price > 0:
            dev = abs(spot_price - last_validated_price) / last_validated_price
            if dev > self.MAX_PRICE_DEVIATION:
                return ClampingReceipt(
                    proposed_allocation_usd=proposed_usd,
                    clamped_allocation_usd=0.0,
                    is_clamped=True,
                    triggered_rules=["SAFETY_BRAKE: Deviasi Harga Spot Melebihi 20%"],
                    explanation=f"Deviasi harga spot ({dev:.1%}) melampaui batas toleransi.",
                )

        # Base vs Tactical Partitioning
        # Standard daily base allocation is capped by base cash
        base_alloc = min(proposed_usd * 0.40, available_base_cash)
        tactical_proposed = max(0.0, proposed_usd - base_alloc)
        
        # Invariant 5: Drawdown Limit on Tactical Reserve
        if portfolio_drawdown > self.MAX_DRAWDOWN_LIMIT:
            triggered.append("DRAWDOWN_LIMIT: Tactical reserve buying frozen due to >25% drawdown")
            tactical_allowed = 0.0
        else:
            # Invariant 2: Daily 15% Tactical Reserve Cap
            reserve_cap = available_reserve_cash * self.MAX_DAILY_RESERVE_PCT
            tactical_allowed = min(tactical_proposed, reserve_cap, available_reserve_cash)
            if tactical_allowed < tactical_proposed:
                triggered.append(f"DAILY_RESERVE_CAP: Clamped tactical deployment to 15% limit (${reserve_cap:.2f})")

        total_clamped = base_alloc + tactical_allowed
        
        # Invariant 1: Total Solvency Check
        total_cash = available_base_cash + available_reserve_cash
        if total_clamped > total_cash:
            triggered.append("SOLVENCY: Clamped to available total cash")
            total_clamped = total_cash

        is_clamped = total_clamped < proposed_usd or len(triggered) > 0
        explanation = "; ".join(triggered) if is_clamped else "Semua invariant simbolik terpenuhi."

        return ClampingReceipt(
            proposed_allocation_usd=proposed_usd,
            clamped_allocation_usd=round(total_clamped, 2),
            is_clamped=is_clamped,
            triggered_rules=triggered,
            explanation=explanation,
        )
```

---

### 6.5 Automated Pipeline Orchestrator (`src/bitcoin_data_platform/pipeline/orchestrator.py`)

Coordinates layered execution cadences with sequential step guarantees, POSIX file-locking, and failure alerting:

```python
class PipelineOrchestrator:
    def __init__(self, repo_root: Path, db_manager: DuckDBManager) -> None:
        self.repo_root = repo_root
        self.db = db_manager
        self.lock_mgr = LockManager(repo_root / "data" / "state" / "locks")

    def run_hourly(self) -> PipelineRunReport:
        """Hourly pipeline (*:05 UTC): RSS fetch, lexical sentiment, and black swan sentinel."""
        with self.lock_mgr.acquire(PipelineCadence.HOURLY):
            steps = [
                self._step_fetch_news(),
                self._step_run_sentinel_and_alert(),
            ]
            return self._build_report(PipelineCadence.HOURLY, steps)

    def run_daily(self) -> PipelineRunReport:
        """Daily pipeline (00:05 UTC): Market data, FNG, Calendar, MNI, Committee, Clamped Step, Telegram."""
        with self.lock_mgr.acquire(PipelineCadence.DAILY):
            steps = [
                self._step_incremental_sync(),
                self._step_fetch_sentiment(),
                self._step_fetch_calendar(),
                self._step_synthesize_mni(),
                self._step_committee_deliberation(),
                self._step_paper_trading_step(),
                self._step_dispatch_daily_digest(),
            ]
            return self._build_report(PipelineCadence.DAILY, steps)

    def run_weekly(self) -> PipelineRunReport:
        """Weekly pipeline (Mon 01:00 UTC): Drawdown review, cost discount, reserve health, retrospective."""
        with self.lock_mgr.acquire(PipelineCadence.WEEKLY):
            steps = [
                self._step_portfolio_risk_audit(),
                self._step_tactical_reserve_audit(),
                self._step_generate_weekly_memo(),
                self._step_dispatch_weekly_digest(),
            ]
            return self._build_report(PipelineCadence.WEEKLY, steps)
```

---

## 7. Systemd Service & Timer Infrastructure (`infra/systemd/`)

### 7.1 Security & Sandboxing Policies
Every Phase 17 systemd unit adheres to strict Linux privilege-separation and resource containment standards:
- `ProtectSystem=strict`, `ProtectHome=true`, `NoNewPrivileges=true`, `PrivateTmp=true`.
- Explicit write paths: `ReadWritePaths=/srv/data/bitcoin-data-platform /tmp`.
- Read-only execution paths: `ReadOnlyPaths=/srv/apps/services/bitcoin-data-platform`.
- Resource constraints: `MemoryMax=1.5G`, `CPUQuota=100%`.
- Low-noise failure notification: `OnFailure=bitcoin-data-failure@%n.service`.

### 7.2 Unit Definitions

#### 1. Hourly Service & Timer (`bitcoin-data-hourly.service` / `.timer`)
Executes 5 minutes past each hour (`*:05 UTC`).

```ini
# infra/systemd/bitcoin-data-hourly.service
[Unit]
Description=Bitcoin Data Platform - Hourly News Ingestion & Sentinel Alerting
Documentation=https://github.com/Rifqi-Setiawan/bitcoin-data-platform
After=network-online.target
Wants=network-online.target
OnFailure=bitcoin-data-failure@%n.service

[Service]
Type=oneshot
User=bitcoin-data
Group=bitcoin-data
EnvironmentFile=-/etc/bitcoin-data/bitcoin-data.env
ExecStart=/srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data pipeline run-hourly \
    --db-path /srv/data/bitcoin-data-platform/state/platform.duckdb

# Sandboxing
ProtectSystem=strict
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true
ReadWritePaths=/srv/data/bitcoin-data-platform /tmp
ReadOnlyPaths=/srv/apps/services/bitcoin-data-platform
MemoryMax=500M
CPUQuota=50%
StandardOutput=journal
StandardError=journal
```

```ini
# infra/systemd/bitcoin-data-hourly.timer
[Unit]
Description=Run Bitcoin Data Platform Hourly Pipeline at 5 minutes past each hour
Documentation=https://github.com/Rifqi-Setiawan/bitcoin-data-platform

[Timer]
OnCalendar=*-*-* *:05:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
```

#### 2. Daily Service & Timer (`bitcoin-data-daily.service` / `.timer`)
Executes daily at 00:05 UTC (5 minutes after UTC candle closure).

```ini
# infra/systemd/bitcoin-data-daily.service
[Unit]
Description=Bitcoin Data Platform - Daily Investment Committee & Execution Pipeline
Documentation=https://github.com/Rifqi-Setiawan/bitcoin-data-platform
After=network-online.target
Wants=network-online.target
OnFailure=bitcoin-data-failure@%n.service

[Service]
Type=oneshot
User=bitcoin-data
Group=bitcoin-data
EnvironmentFile=-/etc/bitcoin-data/bitcoin-data.env
ExecStart=/srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data pipeline run-daily \
    --db-path /srv/data/bitcoin-data-platform/state/platform.duckdb

# Sandboxing
ProtectSystem=strict
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true
ReadWritePaths=/srv/data/bitcoin-data-platform /tmp
ReadOnlyPaths=/srv/apps/services/bitcoin-data-platform
MemoryMax=1.5G
CPUQuota=100%
StandardOutput=journal
StandardError=journal
```

```ini
# infra/systemd/bitcoin-data-daily.timer
[Unit]
Description=Run Bitcoin Data Platform Daily Pipeline at 00:05 UTC
Documentation=https://github.com/Rifqi-Setiawan/bitcoin-data-platform

[Timer]
OnCalendar=*-*-* 00:05:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
```

#### 3. Weekly Service & Timer (`bitcoin-data-weekly.service` / `.timer`)
Executes every Monday at 01:00 UTC.

```ini
# infra/systemd/bitcoin-data-weekly.service
[Unit]
Description=Bitcoin Data Platform - Weekly Portfolio Risk & Retrospective Audit
Documentation=https://github.com/Rifqi-Setiawan/bitcoin-data-platform
After=network-online.target
Wants=network-online.target
OnFailure=bitcoin-data-failure@%n.service

[Service]
Type=oneshot
User=bitcoin-data
Group=bitcoin-data
EnvironmentFile=-/etc/bitcoin-data/bitcoin-data.env
ExecStart=/srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data pipeline run-weekly \
    --db-path /srv/data/bitcoin-data-platform/state/platform.duckdb

# Sandboxing
ProtectSystem=strict
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true
ReadWritePaths=/srv/data/bitcoin-data-platform /tmp
ReadOnlyPaths=/srv/apps/services/bitcoin-data-platform
MemoryMax=1G
CPUQuota=75%
StandardOutput=journal
StandardError=journal
```

```ini
# infra/systemd/bitcoin-data-weekly.timer
[Unit]
Description=Run Bitcoin Data Platform Weekly Pipeline on Mondays at 01:00 UTC
Documentation=https://github.com/Rifqi-Setiawan/bitcoin-data-platform

[Timer]
OnCalendar=Mon *-*-* 01:00:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
```

---

## 8. Web Dashboard Integration: "Investment Committee" Tab & Anti-Slop UI Specification

### 8.1 REST API Contracts

#### `GET /api/committee/latest`
Returns the most recent deliberation session, complete institutional memorandum, individual persona ballots, and symbolic clamping receipts.

```json
{
  "memo_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "memo_date": "2026-09-19",
  "created_at_utc": "2026-09-19T00:05:12Z",
  "market_regime": "CAUTIOUS_BULL",
  "composite_mni": 0.42,
  "consensus_score": 0.38,
  "proposed_action": "OPPORTUNISTIC_BUY",
  "proposed_allocation_usd": 35.00,
  "clamped_allocation_usd": 22.50,
  "allocation_clamped": true,
  "clamping_reason": "DAILY_RESERVE_CAP: Clamped tactical deployment to 15% limit ($22.50)",
  "risk_guard_passed": true,
  "executive_summary_id": "Komite merekomendasikan akumulasi terukur pasca rilis CPI yang melandai. Deployment taktis dibatasi oleh RiskGuard pada pagu harian 15%.",
  "macro_thesis": "Likuiditas global stabil dengan probabilitas penurunan suku bunga The Fed tetap kuat.",
  "valuation_thesis": "MVRV pada level 1.45 berada di zona akumulasi awal siklus; Mayer Multiple 1.12 netral.",
  "technical_thesis": "Volatilitas 24 jam rendah, tidak ada anomali likuiditas buku order.",
  "dissenting_opinions": "Risk Officer mengingatkan adanya rilis Core PPI dalam 48 jam ke depan.",
  "votes": [
    {
      "persona": "MACRO_STRATEGIST",
      "stance": "BULLISH",
      "target_allocation_usd": 40.00,
      "confidence": 0.85,
      "rationale": "Inflasi AS melandai, sentimen institusional mencatat arus masuk ETF positif."
    },
    {
      "persona": "VALUATION_ANALYST",
      "stance": "MODERATELY_BULLISH",
      "target_allocation_usd": 35.00,
      "confidence": 0.90,
      "rationale": "Valuasi on-chain menunjukkan rasio MVRV sehat, akumulasi hodler jangka panjang berlanjut."
    },
    {
      "persona": "RISK_OFFICER",
      "stance": "NEUTRAL",
      "target_allocation_usd": 30.00,
      "confidence": 0.80,
      "rationale": "Likuiditas stabil namun drawdown 30 hari mendekati 8%, anjurkan pembatasan alokasi cadangan."
    }
  ],
  "invariants_checked": [
    {"name": "Solvency", "status": "PASSED"},
    {"name": "Daily 15% Reserve Cap", "status": "CLAMPED", "detail": "Clamped from $35.00 to $22.50"},
    {"name": "Macro 2h Proximity Buffer", "status": "PASSED"},
    {"name": "Black Swan Sentinel", "status": "PASSED"},
    {"name": "Max Drawdown Limit (<25%)", "status": "PASSED"}
  ]
}
```

#### `POST /api/intelligence/ingest`
Allows users to inject external intelligence, theses, and research links directly into the system.

**Request Payload:**
```json
{
  "source_url": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260918a.htm",
  "title": "Fed Announces Balance Sheet Runoff Adjustment",
  "user_thesis": "Federal Reserve memperlambat laju quantitative tightening (QT), berpotensi melonggarkan likuiditas dolar dalam 30 hari ke depan.",
  "pillar": "MACRO_LIQUIDITY",
  "sentiment_bias": 0.65,
  "confidence_score": 0.85,
  "tags": ["fed", "liquidity", "qt", "rates"]
}
```

**Response Payload (`201 Created`):**
```json
{
  "status": "SUCCESS",
  "intelligence_id": "a4f91b028c41d9e2",
  "created_at_utc": "2026-09-19T08:15:00Z",
  "message": "User intelligence recorded and indexed for next committee deliberation."
}
```

#### `GET /api/intelligence/list?limit=20`
Returns ingested user intelligence records with verifiable source links and sentiment annotations.

---

### 8.2 Frontend UI Component (`dashboard/assets/index.html`)

A dedicated **"Investment Committee"** navigation tab (`🏛️ Komite Investasi`) is added alongside "Ringkasan Pasar", "Portofolio Simulasi", and "Macro Radar".

Conforming to anti-slop guidelines:
1. **Hairline Geometry & Achromatic Theme:**
   - Card backgrounds set to `#0F1218`, container background `#0B0E14`, borders hairline `rgba(255, 255, 255, 0.07)`.
   - Zero gradient fog, zero marketing glow, zero generic 3-card grids.
   - Strictly enforces `font-variant-numeric: tabular-nums` across all dollar amounts, scores, and timestamps.

2. **Daily Memorandum Card & Indonesian Executive Summary:**
   - Prominently displays `executive_summary_id` in Bahasa Indonesia with consensus stance badge (e.g. `🟢 AKUMULASI OPORTUNISTIK`).
   - Markdown viewer rendering the formal investment memo sections: Macro Thesis, On-Chain Valuation, Technical Momentum, Dissenting Views.

3. **Multi-Persona Vote Blotter:**
   - Three structured sub-cards (Macro Strategist, Valuation Analyst, Risk Officer) showing stance badge, confidence progress bar, target USD recommendation, and rationale.

4. **Neuro-Symbolic Invariant Verification Gatekeeper Receipt:**
   - Side-by-side terminal comparison table:
     - Left column: **Neural Proposal (AI Committee)**: e.g. `$35.00`
     - Right column: **Symbolic Execution (RiskGuard Invariant Engine)**: e.g. `$22.50` (Badge: `CLAMPED - 15% RESERVE CAP`)
   - Invariant checklist table showing green checks for passed rules and amber/red notices for clamped or tripped rules.

5. **User Intelligence Submission Drawer & Alpha Blotter:**
   - Modal/drawer allowing instant submission of URL, title, thesis, pillar, and sentiment slider.
   - Alpha blotter table listing user submissions with clickable external links (`target="_blank" rel="noopener noreferrer"`).

---

## 9. CLI Command Specification

### 9.1 `bitcoin-data committee`
Manages AI Investment Committee sessions, historical memorandums, and status audits.

```bash
# Deliberate daily investment session and generate memorandum
bitcoin-data committee deliberate [--date YYYY-MM-DD] [--provider mock|hermes|openai] [--model MODEL] [--dry-run] [--db-path PATH]

# View rendered memorandum for a given date
bitcoin-data committee memo [--date YYYY-MM-DD] [--format text|json|markdown] [--db-path PATH]

# Audit committee operational status and latest consensus
bitcoin-data committee status [--db-path PATH]
```

### 9.2 `bitcoin-data intelligence`
Manages ingestion, extraction, and listing of user-provided research intelligence.

```bash
# Ingest external market intelligence or research note
bitcoin-data intelligence ingest \
    --title "Fed Slows Quantitative Tightening" \
    --thesis "Fed memperlambat laju QT, positif untuk likuiditas BTC" \
    --url "https://example.com/fed-qt" \
    --pillar MACRO_LIQUIDITY \
    --sentiment 0.65 \
    --confidence 0.85 \
    --tags fed,liquidity \
    [--db-path PATH]

# List active user intelligence entries
bitcoin-data intelligence list [--limit 20] [--format table|json] [--db-path PATH]
```

### 9.3 `bitcoin-data pipeline`
Operates and monitors the 24/7 automated scheduling pipeline cadences.

```bash
# Execute hourly pipeline job (news, sentinel, alerts)
bitcoin-data pipeline run-hourly [--db-path PATH]

# Execute daily pipeline job (incremental, mni, committee, step, digest)
bitcoin-data pipeline run-daily [--db-path PATH]

# Execute weekly pipeline job (drawdown audit, reserve health, retrospective)
bitcoin-data pipeline run-weekly [--db-path PATH]

# Inspect pipeline lock status, timer health, and execution history
bitcoin-data pipeline status [--db-path PATH]
```

### Exit Codes
- `0`: Success
- `2`: Invalid arguments or configuration error
- `3`: External upstream API / network failure after retries
- `4`: Data contract or payload validation violation
- `5`: Database write failure
- `6`: Concurrent run lock detected (another instance running)

---

## 10. Automated Test Suite Specification

A minimum of **56 new automated tests** must be implemented across `tests/committee/`, `tests/intelligence/`, `tests/pipeline/`, and `tests/dashboard/`:

### Group A: User Intelligence Ingestion (`test_intelligence.py` - 8 tests)
1. `test_user_intelligence_creation`: Ingests structured record with thesis, pillar, and sentiment.
2. `test_intelligence_sha256_deduplication`: Confirms identical source and thesis produces identical ID.
3. `test_article_text_extraction_mocked`: Tests HTTP article fetching and clean paragraph parsing.
4. `test_article_fetch_failure_graceful_fallback`: Ingests user thesis cleanly even when source URL returns 404/500.
5. `test_user_intelligence_sentiment_bounds`: Enforces sentiment clamping strictly within $[-1.0, +1.0]$.
6. `test_user_intelligence_duckdb_persistence`: Verifies insertion and query from `user_market_intelligence`.
7. `test_intelligence_time_decay_weighting`: Verifies 24-hour exponential decay formula on older items.
8. `test_intelligence_listing_filtering`: Verifies active/inactive item filtering and pagination.

### Group B: Committee Personas & Consensus (`test_committee_engine.py` - 8 tests)
9. `test_macro_strategist_persona_evaluation`: Verifies strategist scoring across dovish vs. hawkish regimes.
10. `test_valuation_analyst_persona_mvrv`: Verifies valuation scoring during undervaluation vs. cycle peak.
11. `test_risk_officer_persona_drawdown_response`: Verifies risk officer stance during high drawdown periods.
12. `test_committee_consensus_synthesis_math`: Tests weighted voting formula across persona ballots.
13. `test_bahasa_indonesia_memo_generation`: Confirms executive summary and thesis rendered in Bahasa Indonesia.
14. `test_dissenting_opinions_extraction`: Confirms persona disagreements are surfaced in the memorandum.
15. `test_mock_llm_provider_offline_reproducibility`: Ensures deterministic evaluation without network.
16. `test_committee_memo_duckdb_persistence`: Validates storage in `investment_committee_memos` and `votes`.

### Group C: Neuro-Symbolic Invariant Solver (`test_invariant_solver.py` - 10 tests)
17. `test_invariant_solvency_clamping`: Proposing $100 with only $50 available cash clamps to $50.
18. `test_invariant_daily_15pct_reserve_cap`: Tactical reserve request exceeding 15% is clamped to exactly 15%.
19. `test_invariant_macro_proximity_breaker_trips`: Order completely halted within $\pm 2$ hours of macro event.
20. `test_invariant_macro_proximity_allows_outside_window`: Execution proceeds once outside 2-hour window.
21. `test_invariant_black_swan_emergency_halt`: `black_swan_flag = True` completely freezes allocation to $0.00.
22. `test_invariant_max_drawdown_freezes_tactical`: Portfolio drawdown $>25\%$ freezes tactical reserve pool.
23. `test_invariant_price_deviation_brake`: Spot price deviating $>20\%$ from last close halts execution.
24. `test_solver_normal_conditions_zero_clamping`: Clean market conditions execute neural proposal unclamped.
25. `test_clamping_receipt_audit_trail`: Confirms receipt includes triggered rules and human-readable explanation.
26. `test_risk_guard_integration_hook`: Verifies `RiskGuard.validate_pre_trade()` invokes symbolic solver.

### Group D: DuckDB Storage & Analytical Views (`test_committee_storage.py` - 8 tests)
27. `test_duckdb_schema_initialization`: Verifies all Phase 17 tables and indexes initialize cleanly.
28. `test_mart_committee_deliberation_daily_view`: Verifies analytical view joins market data and memos.
29. `test_memo_and_votes_foreign_key_integrity`: Ensures votes reference valid memo IDs.
30. `test_concurrent_read_write_duckdb_safety`: Validates proper connection cleanup avoiding lock contention.
31. `test_idempotent_table_creation`: Repeated `initialize()` calls do not corrupt data or raise errors.
32. `test_user_intelligence_active_count_join`: Confirms conformed view counts user alpha accurately.
33. `test_memorandum_retrieval_by_date`: Retrieves historical memorandum by exact date.
34. `test_memo_query_fallback_on_unpopulated_db`: Handles empty state gracefully with default structure.

### Group E: Automated Scheduling Pipeline & Systemd (`test_pipeline.py` - 8 tests)
35. `test_pipeline_lock_acquisition_and_release`: Acquires exclusive lock and releases on completion.
36. `test_pipeline_concurrent_run_lock_exit_6`: Detects active lock and raises exit code 6.
37. `test_stale_lock_cleanup_on_dead_pid`: Detects deceased PID in lockfile and safely cleans up lock.
38. `test_pipeline_run_hourly_dag`: Executes hourly steps (news, sentinel, alerts) sequentially.
39. `test_pipeline_run_daily_dag`: Executes daily steps (sync, MNI, committee, step, digest) sequentially.
40. `test_pipeline_run_weekly_dag`: Executes weekly steps (drawdown audit, reserve health, retrospective).
41. `test_systemd_hourly_service_timer_config`: Validates INI syntax, timer schedule, and sandboxing.
42. `test_systemd_daily_weekly_service_timer_config`: Validates daily/weekly systemd service configurations.

### Group F: Dashboard API & User Intelligence REST (`test_committee_dashboard_api.py` - 8 tests)
43. `test_api_committee_latest_endpoint`: Returns latest memorandum matching schema.
44. `test_api_committee_history_endpoint`: Returns historical list of memorandums with pagination.
45. `test_api_intelligence_ingest_endpoint`: Accepts valid JSON and returns 201 Created with ID.
46. `test_api_intelligence_ingest_validation`: Rejects invalid payload (missing thesis/pillar) with 400.
47. `test_api_intelligence_list_endpoint`: Returns list of ingested user alpha with clickable URLs.
48. `test_api_pipeline_schedule_endpoint`: Returns timer schedules, next run timestamps, and lock status.
49. `test_api_resilient_fallback_empty_db`: Endpoints return safe default JSON if database is unpopulated.
50. `test_dashboard_index_html_contains_committee_tab`: Confirms HTML contains tab markup and anti-slop styling.

### Group G: CLI Command Integration (`test_committee_cli.py` - 6 tests)
51. `test_cli_committee_deliberate_dry_run`: Executes deliberation command with `--dry-run`.
52. `test_cli_committee_memo_render`: Renders formatted Markdown memorandum to stdout.
53. `test_cli_committee_status`: Outputs status table of committee consensus and latest regime.
54. `test_cli_intelligence_ingest`: Executes user intelligence ingestion from command line.
55. `test_cli_intelligence_list`: Lists active intelligence items in tabular terminal format.
56. `test_cli_pipeline_run_daily`: Executes complete daily pipeline DAG from CLI interface.

---

## 11. Acceptance Criteria (AC)

- **AC-1:** User Intelligence Ingestion engine parses, validates, and stores research notes, macro hypotheses, and verified URLs into `user_market_intelligence` with deterministic SHA-256 deduplication.
- **AC-2:** Multi-persona Investment Committee (`MacroStrategist`, `ValuationAnalyst`, `RiskOfficer`) evaluates market state, computes weighted consensus, and generates structured Investment Memorandums with executive summaries in Bahasa Indonesia.
- **AC-3:** Pre-Trade Symbolic Solver (`InvariantSolver`) evaluates 6 mathematical invariants and strictly clamps proposed allocations exceeding solvency or the 15% daily tactical reserve cap.
- **AC-4:** Macro Proximity Circuit Breaker halts all order execution within a $\pm 2$-hour window of scheduled High-Impact economic calendar events (FOMC, CPI, NFP).
- **AC-5:** Black Swan Sentinel Halt immediately freezes capital allocation to $0.00 when `black_swan_flag == True`.
- **AC-6:** Execution Clamping Receipts are recorded in DuckDB `investment_committee_memos`, capturing proposed vs. clamped dollar amounts and rule explanations.
- **AC-7:** Automated Scheduling Pipeline orchestrator executes hourly (`*:05 UTC`), daily (`00:05 UTC`), and weekly (`Mon 01:00 UTC`) job DAGs with sequential step guarantees and step-level run reports.
- **AC-8:** POSIX file locking (`LockManager`) prevents overlapping pipeline executions, cleanly exiting with Exit Code 6 when a concurrent run is detected, and automatically self-healing stale locks from dead PIDs.
- **AC-9:** Hardened systemd service and timer units (`bitcoin-data-hourly`, `bitcoin-data-daily`, `bitcoin-data-weekly`) are authored in `infra/systemd/` with strict sandboxing (`ProtectSystem=strict`, `MemoryMax=1.5G`, `OnFailure` alerting).
- **AC-10:** DuckDB tables `user_market_intelligence`, `investment_committee_memos`, `investment_committee_votes`, and conformed view `mart_committee_deliberation_daily` are initialized cleanly.
- **AC-11:** Dashboard REST endpoints (`/api/committee/*`, `/api/intelligence/*`, `/api/pipeline/schedule`) return valid JSON with 100% resilient fallback if DuckDB is unpopulated.
- **AC-12:** Web Dashboard incorporates the "Investment Committee" tab conforming to anti-slop design: dark palette (`#0B0E14`), hairline borders, `font-variant-numeric: tabular-nums`, clickable original URLs, and interactive Invariant Gatekeeper Receipts.
- **AC-13:** CLI subcommands `bitcoin-data committee`, `bitcoin-data intelligence`, and `bitcoin-data pipeline` are fully functional with standardized exit codes.
- **AC-14:** 100% test pass rate across $\ge 56$ new automated tests and zero regression on prior phases ($\ge 712$ existing tests passing), passing all quality gates (`ruff check`, `ruff format --check`, `mypy src`).

---

## 12. Implementation Sequence & Boundaries

### Ordered Implementation Steps

1. **Contracts & Data Models:**
   - Implement `src/bitcoin_data_platform/committee/models.py`.
   - Implement `src/bitcoin_data_platform/intelligence/models.py`.
   - Implement `src/bitcoin_data_platform/pipeline/models.py`.

2. **DuckDB Database Schemas & Migrations:**
   - Update `src/bitcoin_data_platform/storage/duckdb_manager.py` to create tables `user_market_intelligence`, `investment_committee_memos`, `investment_committee_votes`.
   - Create analytical conformed view `mart_committee_deliberation_daily`.

3. **Neuro-Symbolic Invariant Solver:**
   - Implement `src/bitcoin_data_platform/committee/invariant_solver.py`.
   - Connect invariant clamping hooks into `src/bitcoin_data_platform/paper/risk_guard.py`.

4. **User Intelligence Ingestion Engine:**
   - Implement `src/bitcoin_data_platform/intelligence/ingester.py` with URL extraction and SHA-256 deduplication.
   - Implement CLI handler in `src/bitcoin_data_platform/intelligence/cli.py`.

5. **Multi-Persona Committee Deliberation Engine:**
   - Implement persona evaluation logic in `src/bitcoin_data_platform/committee/personas.py`.
   - Implement prompt templates and Bahasa Indonesia memorandum synthesis in `src/bitcoin_data_platform/committee/prompt_templates.py`.
   - Implement deliberation engine coordinator in `src/bitcoin_data_platform/committee/engine.py`.
   - Implement CLI handler in `src/bitcoin_data_platform/committee/cli.py`.

6. **Pipeline Orchestrator & Concurrency Lock Manager:**
   - Implement `src/bitcoin_data_platform/pipeline/lock_manager.py`.
   - Implement `src/bitcoin_data_platform/pipeline/orchestrator.py` (hourly, daily, weekly DAGs).
   - Implement CLI handler in `src/bitcoin_data_platform/pipeline/cli.py`.

7. **Systemd Infrastructure Units:**
   - Author `infra/systemd/bitcoin-data-hourly.{service,timer}`.
   - Author `infra/systemd/bitcoin-data-daily.{service,timer}`.
   - Author `infra/systemd/bitcoin-data-weekly.{service,timer}`.
   - Author comprehensive systemd configuration verification tests in `tests/test_systemd_config.py`.

8. **CLI Central Registration:**
   - Register `committee`, `intelligence`, and `pipeline` subcommands in `src/bitcoin_data_platform/cli.py`.

9. **Dashboard REST Endpoints & Web UI:**
   - Add routes `/api/committee/*`, `/api/intelligence/*`, `/api/pipeline/schedule` to `src/bitcoin_data_platform/dashboard/server.py`.
   - Implement "Investment Committee" tab, Invariant Gatekeeper panel, and User Intelligence modal in `src/bitcoin_data_platform/dashboard/assets/index.html`.

10. **Automated Testing & Quality Gates:**
    - Implement all 56+ tests in `tests/committee/`, `tests/intelligence/`, `tests/pipeline/`, `tests/dashboard/`.
    - Run full repository test suite (`pytest -v`), format checks (`ruff format --check .`), linting (`ruff check .`), and type checks (`mypy src`).

11. **Documentation & Roadmap Sync:**
    - Update `docs/data_dictionary/DATA_DICTIONARY.md` with Phase 17 schemas.
    - Update `docs/roadmap/ROADMAP.md` marking Phase 17 approved.
    - Update `README.md` with new CLI commands and architecture diagrams.

### Preservation Constraints (Do Not Break)

- **Preserve Lakehouse & Batch Immutability:** Historical raw parquet partitions, batch envelopes, and lakehouse metadata catalogs must remain untouched.
- **Preserve Paper Trading Dual-Wallet Math:** Phase 15 dual-pool cash accounting ($700 base / $300 tactical) and 10 bps fee deductions must not be altered; Phase 17 only supplies the clamped allocation amount.
- **Zero Heavy Browser Dependencies:** Ingestion must strictly use `httpx` and stdlib `html.parser`; do not install `playwright`, `puppeteer`, or `selenium`.
- **Zero Secret Exposure:** Telegram bot tokens, Webhook URLs, and API keys must never be committed, logged, or hardcoded.
- **Strict Anti-Slop & Tabular Numbers:** All dashboard additions must maintain hairline borders, zero gradient fog, tabular numbers, and original source clickable hyperlinks (`target="_blank"`).
