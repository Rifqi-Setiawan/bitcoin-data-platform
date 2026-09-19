# Phase 16: Macro & Narrative Intelligence Engine & Web Dashboard Macro Radar Integration

**Owner:** Engineering Team  
**Verification:** Automated Test Suite & Independent Peer Review  
**Branch:** `feat/phase-16-macro-narrative-intelligence`  
**Status:** Approved Architectural Specification  

---

## 1. Executive Summary & Objective

Phase 16 transitions the Bitcoin Data Platform from retrospective market-metric collection into an active **Macro & Narrative Intelligence Engine**. The engine continuously ingests, scores, and synthesizes unstructured market news, institutional regulatory alerts, and high-impact macroeconomic releases, integrating them directly into:

1. **Web Dashboard Macro Radar**: An interactive terminal-grade widget displaying real-time market sentiment, macroeconomic event surprises, and curated news with verifiable original hyperlinks (`target="_blank"`).
2. **Pre-Trade Gatekeeper (`RiskGuard`)**: An AutoHedge-inspired circuit breaker that halts forward execution during severe black swan events, regulatory crises, or extreme macroeconomic liquidity shocks.

### Core Problems Solved
- **Single-Source Fragility:** Phase 13 relied exclusively on CoinDesk RSS. Phase 16 implements a robust, multi-source curated RSS and news aggregator (CoinDesk, Cointelegraph, Decrypt, Bitcoin Magazine) with SHA-256 content deduplication.
- **Unstructured Macro Data:** Phase 12 collected static ForexFactory calendar schedules. Phase 16 parses and evaluates economic surprise deltas ($\Delta_{\text{surprise}} = \text{Actual} - \text{Forecast}$) for FOMC, CPI, NFP, and GDP, converting them into directional liquidity impact scores.
- **Narrative Blind Spots:** Pure quantitative on-chain models (MVRV, Mayer Multiple) fail during exogenous black swans (e.g., exchange insolvencies, sudden regulatory actions). Phase 16's 3-Tier Macro-Narrative Index (MNI) provides early warning signals and automated circuit breakers.

---

## 2. Institutional Reference Architecture & Core Principles

Drawing from state-of-the-art financial engineering patterns (AutoHedge, Fincept Terminal, and quantitative event-driven macro modeling):

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   EXTERNAL DATA SOURCES                                         │
│  ┌───────────────────────┐  ┌─────────────────────────┐  ┌───────────────────────────────────┐  │
│  │ Curated Crypto RSS    │  │ ForexFactory Calendar   │  │ SEC / Regulatory & Black Swan     │  │
│  │ (CoinDesk, Cointelegraph,│ │ (CPI, NFP, FOMC, GDP)   │  │ (Enforcement, Hacks, Insolvency)  │  │
│  │  Decrypt, BTC Mag)    │  │ Actual vs Forecast JSON │  │ Keyword Scanners & Sentinels      │  │
│  └───────────┬───────────┘  └────────────┬────────────┘  └─────────────────┬─────────────────┘  │
└──────────────┼───────────────────────────┼─────────────────────────────────┼────────────────────┘
               │                           │                                 │
               ▼                           ▼                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                    PHASE 16: MACRO & NARRATIVE INGESTION ENGINE                                 │
│  ┌───────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ Lightweight Ingestion Client (httpx + stdlib xml.etree, TLS fingerprinting, retry backoff) │  │
│  │ Deterministic Content Hashing (SHA-256) & In-Memory / DuckDB Deduplication Layer          │  │
│  └─────────────────────────────────────────────┬─────────────────────────────────────────────┘  │
└────────────────────────────────────────────────┼────────────────────────────────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                    3-TIER MACRO-NARRATIVE SYNTHESIS & SCORING LAYER                             │
│  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────────────────────┐  │
│  │ Tier 1: Hard Macro      │  │ Tier 2: Market Sentiment│  │ Tier 3: Narrative & Black Swan  │  │
│  │ - ForexFactory Surprise │  │ - Normalized FNG (0-100)│  │ - Multi-Pillar Keyword Scoring  │  │
│  │ - Hawkish/Dovish Metric │  │ - MVRV Valuation Regime │  │ - Regulatory & Exploit Polarity │  │
│  │ - Exponential Decay     │  │ - Mayer Multiple Trend  │  │ - Binary Black Swan Gate Flag   │  │
│  └───────────┬─────────────┘  └────────────┬────────────┘  └─────────────────┬───────────────┘  │
│              │                             │                                 │                  │
│              └──────────────────────┬──────┴─────────────────────────────────┘                  │
│                                     ▼                                                           │
│           Composite Macro-Narrative Index (MNI) Calculation & 5-Regime Classifier               │
│           (RISK_ON_EXPANSION | CAUTIOUS_BULL | NEUTRAL_CHOP | RISK_OFF_DEFENSE | CRISIS)        │
└─────────────────────────────────────┬───────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                       DUCKDB ANALYTICAL SERVING & MART STORAGE                                  │
│  - macro_news_articles            - macro_economic_releases                                     │
│  - daily_narrative_intelligence   - mart_macro_narrative_daily (Analytical View)                │
└──────────────────────┬───────────────────────────────────────────────────┬──────────────────────┘
                       │                                                   │
                       ▼                                                   ▼
┌──────────────────────────────────────────────┐   ┌──────────────────────────────────────────────┐
│        EXECUTION LAYER (RiskGuard)           │   │      WEB DASHBOARD (Macro Radar UI)          │
│  - Black Swan Pre-Trade Kill-Switch          │   │  - REST Endpoints: /api/macro/radar, news    │
│  - High-Impact Macro Announcement Buffer     │   │  - Fincept-Style Anti-Slop Dark Mode UI      │
│  - Solvency & Allocation Override            │   │  - Original Clickable Hyperlinks             │
└──────────────────────────────────────────────┘   └──────────────────────────────────────────────┘
```

### Core Design Principles

1. **VPS Resource Efficiency (Ponytail Principle):**
   - Strictly avoid heavyweight browser engines (`browser-use`, `playwright`, `puppeteer`, `selenium`) or heavy microservice stacks that exceed 2 GB RAM.
   - Employ lightweight, connection-pooled HTTP clients (`httpx`) with standard user-agent rotation and exponential backoff.
   - Deterministic XML parsing via standard library `xml.etree.ElementTree` without external parser dependencies (`feedparser`, `lxml`).
2. **Clickable Attribution & Zero Synthesized Slop:**
   - Every headline, sentiment badge, and alert rendered on the Web Dashboard must include a direct, clickable hyperlink (`target="_blank" rel="noopener noreferrer"`) pointing to the upstream source URL.
   - AI/heuristic narratives must never be presented as detached facts without audit trails to original source articles.
3. **Achromatic Anti-Slop Visual Hierarchy:**
   - Consistent with existing dashboard styling: deep charcoal/slate dark theme (`bg-slate-900`, `border-white/10`), hairline borders, zero gradient fog, zero generic 3-card marketing grids.
   - Enforce `font-variant-numeric: tabular-nums` across all numeric metrics, surprise deltas, and indices.
   - Anchor all bar chart axes at zero.
4. **Deterministic Pre-Trade Circuit Breakers:**
   - The paper trading engine must query the latest macro intelligence state before executing trades.
   - A tripped circuit breaker pauses execution and logs structured rationale in the trade blotter instead of failing silently.

---

## 3. Scope of Work

### In Scope
1. **Multi-Source News Ingestion Engine (`src/bitcoin_data_platform/macro/feed_ingester.py`)**:
   - Curated RSS feeds: CoinDesk, Cointelegraph, Decrypt, Bitcoin Magazine.
   - Feed configuration, retry backoff with jitter, error isolation (one broken feed does not halt others).
   - SHA-256 deduplication key: `hashlib.sha256(f"{source}:{url}:{title}".encode()).hexdigest()`.
2. **ForexFactory Calendar & Economic Surprise Engine (`src/bitcoin_data_platform/macro/macro_analyzer.py`)**:
   - Parsing `actual`, `forecast`, `previous` economic indicators (CPI, NFP, FOMC, GDP).
   - Normalizing surprise delta: $\Delta = \text{Actual} - \text{Forecast}$.
   - Directional liquidity classification: Hawkish (liquidity contractionary) vs. Dovish (liquidity expansionary).
3. **Lexical & Sentiment Polarity Engine (`src/bitcoin_data_platform/macro/sentiment_analyzer.py`)**:
   - Categorized regex/keyword rules across 4 pillars: `REGULATORY`, `SECURITY_EXPLOIT`, `INSTITUTIONAL_ADOPTION`, `MACRO_LIQUIDITY`.
   - Severity rating: `CRITICAL` (score weight 3.0), `HIGH` (2.0), `MEDIUM` (1.0), `LOW` (0.5).
   - Rule-based polarity scoring mapped to $[-1.0, +1.0]$.
4. **Composite Macro-Narrative Synthesis Engine (`src/bitcoin_data_platform/macro/synthesizer.py`)**:
   - 3-Tier mathematical weighting calculating Composite MNI $\in [-1.0, +1.0]$.
   - 5-Regime determination: `RISK_ON_EXPANSION`, `CAUTIOUS_BULL`, `NEUTRAL_CHOP`, `RISK_OFF_DEFENSE`, `BLACK_SWAN_CRISIS`.
   - Localized narrative generator in Bahasa Indonesia.
5. **DuckDB Persistence & Analytical View (`src/bitcoin_data_platform/storage/duckdb_manager.py`)**:
   - Tables: `macro_news_articles`, `macro_economic_releases`, `daily_narrative_intelligence`.
   - View: `mart_macro_narrative_daily` joining macro intelligence with market and on-chain metrics.
6. **Pre-Trade Gatekeeper Extension (`src/bitcoin_data_platform/paper/risk_guard.py`)**:
   - Macro circuit breaker: Blocks order execution if `black_swan_flag == True` or if within $\pm 2$ hours of a High-Impact macro release.
7. **Dashboard REST Endpoints & Web UI Widget (`src/bitcoin_data_platform/dashboard/`)**:
   - Endpoints: `GET /api/macro/radar`, `GET /api/macro/news`, `GET /api/macro/calendar`.
   - New "Macro Radar" interactive tab in `assets/index.html`.
8. **CLI Interface (`src/bitcoin_data_platform/cli.py`)**:
   - Subcommand `bitcoin-data macro` with actions: `fetch-news`, `fetch-calendar`, `synthesize`, `radar`, `status`.
9. **Comprehensive Test Suite & Quality Gates**:
   - Minimum 45 new unit, integration, and contract tests. Zero regression across existing 665 tests.

### Out of Scope
- Heavy headless browser scraping (`playwright`, `selenium`).
- High-frequency tick-by-tick order-flow news trading.
- Paid/authenticated API feeds (Bloomberg Terminal, Refinitiv, Reuters Eikon).
- LLM API dependencies that require external API keys or induce variable monthly token costs.

---

## 4. Mathematical & Analytical Formulation

### 4.1 Tier 1: Hard Macro Economic Score ($S_{\text{macro}} \in [-1.0, +1.0]$)

Macro releases impact Bitcoin primarily through US dollar liquidity expectations and real interest rates:
- **Hawkish Outcome** (Higher inflation, tight labor market, rate hike): Bearish for liquidity $\implies S_{\text{macro}} < 0$.
- **Dovish Outcome** (Lower inflation, slowing jobs, rate cut, stimulus): Bullish for liquidity $\implies S_{\text{macro}} > 0$.

#### Indicator Surprise Normalization
For each economic release $i$ occurring within the active window (last 72 hours):
$$\Delta_i = \text{Actual}_i - \text{Forecast}_i$$

The normalized indicator shock $z_i$ is mapped based on the indicator category:
- **CPI / PPI (Inflation):** If $\text{Actual} > \text{Forecast}$, liquidity tightens $\implies z_i = -\min\left(\frac{\Delta_i}{\text{Scale}_{\text{CPI}}}, 1.0\right)$.
- **Non-Farm Payrolls (NFP) / Unemployment:** Strong jobs delay rate cuts $\implies z_i = -\min\left(\frac{\Delta_i}{\text{Scale}_{\text{NFP}}}, 1.0\right)$.
- **FOMC Rate Decision:** Rate cut $\implies z_i = +1.0$; Rate hike $\implies z_i = -1.0$; Pause/Hold $\implies z_i = 0.0$.
- **GDP (Economic Growth):** Balanced expansion with disinflation $\implies z_i = +0.5 \cdot \text{sign}(\Delta_i)$.

#### Time Decay Weighting
Recent events exert stronger pricing pressure than older events within the 72-hour rolling window:
$$w_i = \text{ImpactWeight}_i \times e^{-\lambda (t - t_i)}$$
Where:
- $\text{ImpactWeight} = 1.0$ for `High`, $0.5$ for `Medium`, $0.2$ for `Low`.
- $\lambda = \frac{\ln(2)}{24 \text{ hours}}$ (24-hour half-life).

The aggregated Hard Macro Score:
$$S_{\text{macro}} = \frac{\sum_i w_i z_i}{\sum_i w_i + \epsilon} \in [-1.0, +1.0]$$
*(If no macro events occurred within 72 hours, $S_{\text{macro}} = 0.0$.)*

---

### 4.2 Tier 2: Aggregate Market Sentiment & Valuation ($S_{\text{sentiment}} \in [-1.0, +1.0]$)

Aggregates behavioral fear/greed, on-chain fundamental valuation, and technical moving average momentum:

1. **Fear & Greed Component ($S_{\text{fng}}$):**
   $$S_{\text{fng}} = \frac{\text{FNG\_Value} - 50}{50} \in [-1.0, +1.0]$$
2. **On-Chain MVRV Valuation Component ($S_{\text{mvrv}}$):**
   - $MVRV < 1.0$ (Deep Undervaluation / Cycle Accumulation) $\implies +1.0$
   - $1.0 \le MVRV < 1.8$ (Fair Value / Early Bull) $\implies +0.5$
   - $1.8 \le MVRV < 2.8$ (Expansion / Moderate Risk) $\implies -0.2$
   - $MVRV \ge 2.8$ (Overheated / Cycle Distribution Bubble) $\implies -1.0$
3. **Mayer Multiple Technical Momentum ($S_{\text{mm}}$):**
   - $MM < 0.8$ (Oversold / Strong Value) $\implies +1.0$
   - $0.8 \le MM < 1.4$ (Neutral Trend) $\implies +0.3$
   - $1.4 \le MM < 2.4$ (Overbought) $\implies -0.4$
   - $MM \ge 2.4$ (Extreme Speculative Bubble) $\implies -1.0$

The composite Sentiment Score is:
$$S_{\text{sentiment}} = 0.40 \cdot S_{\text{fng}} + 0.30 \cdot S_{\text{mvrv}} + 0.30 \cdot S_{\text{mm}} \in [-1.0, +1.0]$$

---

### 4.3 Tier 3: Narrative & Regulatory Polarity ($S_{\text{narrative}} \in [-1.0, +1.0]$)

Scans ingested news headlines and summaries against curated regex dictionaries across 4 thematic pillars:

| Pillar | Bullish Keywords (+1) | Bearish Keywords (-1) | Critical Black Swan (-3) |
|---|---|---|---|
| **Regulatory** | ETF Approval, Legal Clarification, Pro-Crypto Bill, License Granted | SEC Lawsuit, Subpoena, Regulatory Clampdown, Ban Proposal | DOJ Indictment, Nationwide Outright Ban, Asset Freezing Order |
| **Security & Exploit** | Recovery of Funds, Whitehat Return, Audit Passed | Bridge Vulnerability, Phishing Surge, Protocol Bug | Exchange Insolvency, Major CEX Hack ($100M+), Bank Run, Stablecoin Depeg |
| **Institutional** | Treasury Allocation, Sovereign Adoption, ETF Inflow Record | Fund Liquidation, Miner Capitulation, Outflow Streak | Major Custodian Bankruptcy, Mega-Whale Forced Liquidation |
| **Macro Liquidity** | Fed Rate Cut, Quantitative Easing, Stimulus, Debt Relief | Fed Rate Hike, Balance Sheet Runoff, Banking Panic | Global Liquidity Freeze, Systemic Bank Failure |

For article $j$ published in the last 24 hours:
$$\text{Score}_j = \sum \text{Weights}_{\text{bullish}} - \sum \text{Weights}_{\text{bearish}}$$
$$S_{\text{narrative}} = \tanh\left(\frac{\sum_j \text{Score}_j}{10}\right) \in [-1.0, +1.0]$$

#### Black Swan Sentinel Rule
If any ingested news item matches a `CRITICAL` severity negative rule within the last 24 hours:
$$\text{black\_swan\_flag} = \text{True}$$

---

### 4.4 Composite Macro-Narrative Index (MNI)

$$MNI = 0.40 \cdot S_{\text{macro}} + 0.35 \cdot S_{\text{sentiment}} + 0.25 \cdot S_{\text{narrative}}$$

#### 5-Tier Macro-Narrative Regimes

| Regime | MNI Threshold | Risk Posture | Strategy Allocation Modifier |
|---|---|---|---|
| `RISK_ON_EXPANSION` | $MNI \ge +0.50$ | Maximum Expansion | Aggressive Accumulation / Boost |
| `CAUTIOUS_BULL` | $+0.15 \le MNI < +0.50$ | Constructive | Opportunistic Accumulation |
| `NEUTRAL_CHOP` | $-0.20 \le MNI < +0.15$ | Neutral | Standard Baseline DCA |
| `RISK_OFF_DEFENSE` | $-0.60 \le MNI < -0.20$ | Defensive | Divert to Tactical Cash Reserve |
| `BLACK_SWAN_CRISIS` | $MNI < -0.60$ OR $\text{flag} = \text{True}$ | Emergency Halt | **Complete Trading Freeze (Circuit Breaker Tripped)** |

---

## 5. Database Architecture (`data/state/platform.duckdb`)

### 5.1 Table: `macro_news_articles`
Stores all ingested and deduplicated news articles.

```sql
CREATE TABLE IF NOT EXISTS macro_news_articles (
    article_id VARCHAR PRIMARY KEY,         -- SHA-256(source:url:title)
    source VARCHAR NOT NULL,                -- 'CoinDesk', 'Cointelegraph', 'Decrypt', 'BitcoinMagazine'
    title VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    published_utc TIMESTAMPTZ NOT NULL,
    summary VARCHAR NOT NULL DEFAULT '',
    pillar VARCHAR NOT NULL,                -- 'REGULATORY', 'SECURITY_EXPLOIT', 'INSTITUTIONAL', 'MACRO_LIQUIDITY', 'GENERAL'
    severity VARCHAR NOT NULL,               -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    polarity DOUBLE NOT NULL,               -- Score between -1.0 and +1.0
    matched_keywords VARCHAR NOT NULL,      -- Comma-separated matched tokens
    ingested_at_utc TIMESTAMPTZ NOT NULL
);
```

### 5.2 Table: `macro_economic_releases`
Stores parsed economic calendar releases and calculated surprise deltas.

```sql
CREATE TABLE IF NOT EXISTS macro_economic_releases (
    release_id VARCHAR PRIMARY KEY,         -- SHA-256(event_name:date_utc:country)
    event_name VARCHAR NOT NULL,            -- 'CPI m/m', 'Non-Farm Employment Change', 'Fed Funds Rate'
    country VARCHAR NOT NULL,               -- 'USD', 'EUR', etc.
    release_date DATE NOT NULL,
    release_time_utc VARCHAR NOT NULL,
    impact VARCHAR NOT NULL,                -- 'High', 'Medium', 'Low'
    actual_value DOUBLE,
    forecast_value DOUBLE,
    previous_value DOUBLE,
    surprise_delta DOUBLE,                  -- actual - forecast
    directional_score DOUBLE NOT NULL,      -- -1.0 (hawkish) to +1.0 (dovish)
    raw_payload_json VARCHAR,
    ingested_at_utc TIMESTAMPTZ NOT NULL
);
```

### 5.3 Table: `daily_narrative_intelligence`
Stores daily synthesized scores, composite MNI, and Bahasa Indonesia commentary.

```sql
CREATE TABLE IF NOT EXISTS daily_narrative_intelligence (
    intelligence_date DATE PRIMARY KEY,
    synthesized_at_utc TIMESTAMPTZ NOT NULL,
    hard_macro_score DOUBLE NOT NULL,
    sentiment_score DOUBLE NOT NULL,
    narrative_score DOUBLE NOT NULL,
    composite_mni DOUBLE NOT NULL,
    regime VARCHAR NOT NULL,                -- 'RISK_ON_EXPANSION', 'CAUTIOUS_BULL', 'NEUTRAL_CHOP', 'RISK_OFF_DEFENSE', 'BLACK_SWAN_CRISIS'
    black_swan_flag BOOLEAN NOT NULL DEFAULT FALSE,
    active_critical_alerts INTEGER NOT NULL DEFAULT 0,
    dominant_pillar VARCHAR NOT NULL,
    narrative_summary_id VARCHAR NOT NULL   -- Localized market narrative in Bahasa Indonesia
);
```

### 5.4 View: `mart_macro_narrative_daily`
Analytical conformed view joining price, on-chain valuation, and macro intelligence.

```sql
CREATE OR REPLACE VIEW mart_macro_narrative_daily AS
SELECT
    m.trade_date_utc,
    m.market_close_usd,
    m.sma_200,
    m.mayer_multiple,
    m.mvrv_ratio,
    m.fng_value,
    m.investment_signal,
    COALESCE(n.composite_mni, 0.0) AS composite_mni,
    COALESCE(n.regime, 'NEUTRAL_CHOP') AS macro_regime,
    COALESCE(n.black_swan_flag, FALSE) AS black_swan_flag,
    COALESCE(n.hard_macro_score, 0.0) AS hard_macro_score,
    COALESCE(n.sentiment_score, 0.0) AS sentiment_score,
    COALESCE(n.narrative_score, 0.0) AS narrative_score,
    n.narrative_summary_id,
    COALESCE(n.active_critical_alerts, 0) AS active_critical_alerts
FROM mart_btc_investment_signals_daily m
LEFT JOIN daily_narrative_intelligence n
    ON m.trade_date_utc = n.intelligence_date;
```

---

## 6. Python Component & Module Layout

```
src/bitcoin_data_platform/
├── macro/                                    # [NEW PACKAGE] Phase 16 Macro & Narrative Engine
│   ├── __init__.py                           # Package exports
│   ├── models.py                             # Dataclasses & Type contracts
│   ├── feed_ingester.py                      # Multi-source RSS fetcher with retry & backoff
│   ├── macro_analyzer.py                     # Economic surprise & hawkish/dovish evaluation
│   ├── sentiment_analyzer.py                 # Lexical scoring, pillar classification & polarity
│   ├── synthesizer.py                        # 3-Tier MNI calculator & Bahasa Indonesia narrative
│   └── cli.py                                # Subcommand implementation for 'bitcoin-data macro'
├── paper/
│   └── risk_guard.py                         # [MODIFIED] Added macro circuit breaker verification
├── storage/
│   └── duckdb_manager.py                     # [MODIFIED] Added Phase 16 tables, views, and helpers
├── dashboard/
│   ├── server.py                             # [MODIFIED] Added /api/macro/* REST endpoints
│   └── assets/
│       └── index.html                        # [MODIFIED] Added "Macro Radar" view with clickable news links
└── cli.py                                    # [MODIFIED] Registered 'macro' subcommand
```

### 6.1 Data Contracts (`src/bitcoin_data_platform/macro/models.py`)

```python
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Any

class MacroPillar(StrEnum):
    REGULATORY = "REGULATORY"
    SECURITY_EXPLOIT = "SECURITY_EXPLOIT"
    INSTITUTIONAL = "INSTITUTIONAL"
    MACRO_LIQUIDITY = "MACRO_LIQUIDITY"
    GENERAL = "GENERAL"

class AlertSeverity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class MacroRegime(StrEnum):
    RISK_ON_EXPANSION = "RISK_ON_EXPANSION"
    CAUTIOUS_BULL = "CAUTIOUS_BULL"
    NEUTRAL_CHOP = "NEUTRAL_CHOP"
    RISK_OFF_DEFENSE = "RISK_OFF_DEFENSE"
    BLACK_SWAN_CRISIS = "BLACK_SWAN_CRISIS"

@dataclass(frozen=True)
class MacroArticle:
    article_id: str
    source: str
    title: str
    url: str
    published_utc: datetime
    summary: str
    pillar: MacroPillar
    severity: AlertSeverity
    polarity: float
    matched_keywords: list[str]
    ingested_at_utc: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "article_id": self.article_id,
            "source": self.source,
            "title": self.title,
            "url": self.url,
            "published_utc": self.published_utc.isoformat(),
            "summary": self.summary,
            "pillar": self.pillar.value,
            "severity": self.severity.value,
            "polarity": self.polarity,
            "matched_keywords": self.matched_keywords,
            "ingested_at_utc": self.ingested_at_utc.isoformat(),
        }

@dataclass(frozen=True)
class MacroEconomicRelease:
    release_id: str
    event_name: str
    country: str
    release_date: date
    release_time_utc: str
    impact: str
    actual_value: float | None
    forecast_value: float | None
    previous_value: float | None
    surprise_delta: float | None
    directional_score: float
    raw_payload_json: str
    ingested_at_utc: datetime

@dataclass(frozen=True)
class DailyNarrativeReport:
    intelligence_date: date
    synthesized_at_utc: datetime
    hard_macro_score: float
    sentiment_score: float
    narrative_score: float
    composite_mni: float
    regime: MacroRegime
    black_swan_flag: bool
    active_critical_alerts: int
    dominant_pillar: MacroPillar
    narrative_summary_id: str
```

---

## 7. AutoHedge-Inspired RiskGuard Integration

Phase 15 established the `RiskGuard` pre-trade gatekeeper. Phase 16 extends `RiskGuard.validate_pre_trade()` to evaluate live macro intelligence:

```python
class RiskGuard:
    def validate_pre_trade(
        self,
        *,
        spot_price: float,
        last_known_price: float,
        available_cash: float,
        required_cash: float,
        has_high_impact_macro_event: bool,
        # Phase 16 Additions:
        black_swan_flag: bool = False,
        composite_mni: float = 0.0,
        macro_event_proximity_minutes: int | None = None,
    ) -> RiskCheckResult:
        # 1. Standard Phase 15 checks (Kill Switch, Solvency, 20% Price Deviation)
        ...
        # 2. Black Swan Emergency Halt
        if black_swan_flag:
            return RiskCheckResult(
                passed=False,
                reason="CIRCUIT_BREAKER_TRIPPED: Active Black Swan / Critical Regulatory Sentinel alert",
            )
        
        # 3. Macro Proximity Buffer (+/- 120 minutes around High Impact announcement)
        if macro_event_proximity_minutes is not None and abs(macro_event_proximity_minutes) <= 120:
            return RiskCheckResult(
                passed=False,
                reason=f"CIRCUIT_BREAKER_TRIPPED: High-impact macro release window (+/- 2h, current: {macro_event_proximity_minutes}m)",
            )

        # 4. Severe Macro Contraction Halt
        if composite_mni < -0.65:
            return RiskCheckResult(
                passed=False,
                reason=f"CIRCUIT_BREAKER_TRIPPED: Severe Macro Liquidity Contraction (MNI = {composite_mni:.2f})",
            )

        return RiskCheckResult(passed=True)
```

---

## 8. Web Dashboard Macro Radar & Anti-Slop UI Specification

### 8.1 REST API Contracts

#### `GET /api/macro/radar`
Returns the latest synthesized Macro Radar metrics, MNI gauge score, 3-tier subscores, regime, and active warnings.

```json
{
  "date": "2026-09-19",
  "composite_mni": 0.42,
  "regime": "CAUTIOUS_BULL",
  "regime_label": "Cautious Bull (Akumulasi Oportunistik)",
  "black_swan_flag": false,
  "scores": {
    "hard_macro": 0.25,
    "sentiment": 0.58,
    "narrative": 0.40
  },
  "narrative_summary": "Pasar kondusif pasca rilis CPI moderat. Likuiditas stabil tanpa ancaman regulasi kritis.",
  "dominant_pillar": "INSTITUTIONAL",
  "critical_alerts_count": 0,
  "last_updated_utc": "2026-09-19T08:30:00Z"
}
```

#### `GET /api/macro/news?limit=20`
Returns verified news items with **mandatory original source hyperlinks**.

```json
[
  {
    "article_id": "8f3b20c...",
    "source": "CoinDesk",
    "title": "SEC Approves In-Kind Creation for Spot Bitcoin ETFs",
    "url": "https://www.coindesk.com/policy/2026/09/19/sec-etf-in-kind-ruling",
    "published_utc": "2026-09-19T07:15:00Z",
    "pillar": "REGULATORY",
    "severity": "HIGH",
    "polarity": 0.85,
    "summary": "The SEC has finalized rules allowing authorized participants to use physical Bitcoin..."
  }
]
```

#### `GET /api/macro/calendar?days=7`
Returns scheduled high and medium impact economic events with surprise evaluations.

```json
[
  {
    "release_id": "cpi_2026_09",
    "event_name": "Core CPI m/m",
    "country": "USD",
    "release_date": "2026-09-18",
    "release_time_utc": "12:30",
    "impact": "High",
    "actual": 0.2,
    "forecast": 0.3,
    "previous": 0.2,
    "surprise": -0.1,
    "directional_bias": "DOVISH"
  }
]
```

### 8.2 Frontend UI Component (`dashboard/assets/index.html`)

The dashboard incorporates a dedicated **"Macro Radar"** navigation view conforming to anti-slop guidelines:

1. **MNI Meter & 3-Domain Breakdown Cards**:
   - Clean hairline border (`rgba(255, 255, 255, 0.07)`), dark background (`#0B0E14`), zero gradient fog.
   - Tabular numbers (`tabular-nums`) displaying exact MNI values and subscores.
   - Status indicators: Emerald for `RISK_ON`, Sky for `CAUTIOUS_BULL`, Slate for `NEUTRAL`, Amber for `RISK_OFF`, Rose/Red for `CRISIS`.
2. **Interactive News Blotter with Clickable Links**:
   - Table columns: `Waktu (UTC)`, `Sumber`, `Pillar`, `Sentimen`, `Headline Berita`, `Link`.
   - The `Link` column renders an external SVG icon with an anchor tag:
     `<a href="${item.url}" target="_blank" rel="noopener noreferrer" class="text-sky-400 hover:underline">Buka Berita ↗</a>`.
   - Never render synthetic AI sentiment without the upstream verifiable link.
3. **Macro Economic Calendar Feed**:
   - Table displaying upcoming events, countdown timer, `Forecast`, `Actual`, and calculated `Surprise Delta`.

---

## 9. CLI Command Specification

```bash
# Ingest latest news from all configured RSS feeds
bitcoin-data macro fetch-news [--sources coindesk,cointelegraph,decrypt] [--db-path PATH]

# Fetch economic calendar releases & calculate surprise deltas
bitcoin-data macro fetch-calendar [--impact High,Medium] [--db-path PATH]

# Execute daily 3-tier synthesis and persist MNI report
bitcoin-data macro synthesize [--date YYYY-MM-DD] [--db-path PATH]

# Render terminal Macro Radar summary report
bitcoin-data macro radar [--format text|json] [--db-path PATH]

# Check health and pipeline status of macro intelligence
bitcoin-data macro status [--db-path PATH]
```

### Exit Codes
- `0`: Success
- `2`: Invalid arguments or configuration
- `3`: External feed / source network failure after retries
- `4`: Data contract or parsing corruption
- `5`: Database write failure
- `6`: Concurrent run lock detected

---

## 10. Automated Test Suite Specification

A minimum of **45 new automated tests** must be implemented in `tests/macro/` and `tests/dashboard/`:

### Group A: Multi-Source RSS Ingestion (`test_feed_ingester.py` - 10 tests)
1. `test_rss_feed_parsing_coindesk`: Validates XML parsing of CoinDesk feed.
2. `test_rss_feed_parsing_cointelegraph`: Validates Cointelegraph feed format.
3. `test_rss_feed_parsing_decrypt`: Validates Decrypt feed format.
4. `test_rss_deduplication_hash`: Confirms identical title + URL yields identical `article_id`.
5. `test_rss_retry_backoff_transient_error`: Verifies HTTP 503 retry backoff.
6. `test_rss_graceful_degradation_malformed_xml`: Handles invalid XML without crash.
7. `test_rss_partial_feed_failure`: Ensure failure in 1 source does not block other 3 sources.
8. `test_rss_timeout_handling`: Validates connect/read timeout isolation.
9. `test_rss_empty_feed`: Returns empty list without exceptions.
10. `test_rss_pubdate_parsing`: Tests RFC 822 and ISO-8601 parsing into UTC.

### Group B: Macro Calendar & Surprise Analysis (`test_macro_analyzer.py` - 8 tests)
11. `test_cpi_surprise_calculation`: $\text{Actual} > \text{Forecast} \implies$ Hawkish negative score.
12. `test_cpi_dovish_surprise`: $\text{Actual} < \text{Forecast} \implies$ Dovish positive score.
13. `test_nfp_surprise_mapping`: Tests job addition surprises to liquidity impact.
14. `test_fomc_rate_hike_cut`: Tests direct directional score for rate decisions.
15. `test_macro_time_decay_72h`: Verifies older events decay exponentially.
16. `test_macro_missing_actual_value`: Scheduled upcoming events produce zero surprise.
17. `test_macro_calendar_empty_graceful`: Handles empty calendar gracefully.
18. `test_macro_release_deduplication`: Confirms idempotent database insertion.

### Group C: Lexical Polarity & Pillar Classification (`test_sentiment_analyzer.py` - 8 tests)
19. `test_regulatory_pillar_classification`: Correctly categorizes SEC lawsuits.
20. `test_security_exploit_classification`: Detects exchange hacks and bridge bugs.
21. `test_critical_severity_black_swan`: Matches insolvency/bankrupt terms as `CRITICAL`.
22. `test_institutional_adoption_polarity`: Scores ETF inflows as positive.
23. `test_general_neutral_news`: Scores generic commentary within neutral boundaries.
24. `test_regex_boundary_matching`: Prevents partial word false positives (e.g. "asset" vs "sec").
25. `test_polarity_saturation_clamp`: Ensures polarity strictly bounded within $[-1.0, +1.0]$.
26. `test_mixed_sentiment_resolution`: Balances both positive and negative keyword matches.

### Group D: 3-Tier Synthesizer & MNI Index (`test_synthesizer.py` - 8 tests)
27. `test_composite_mni_weighting_formula`: Verifies $0.40/0.35/0.25$ weighting calculation.
28. `test_regime_classification_risk_on`: High positive MNI yields `RISK_ON_EXPANSION`.
29. `test_regime_classification_crisis_flag`: `black_swan_flag = True` forces `BLACK_SWAN_CRISIS`.
30. `test_regime_classification_neutral`: Zero values resolve to `NEUTRAL_CHOP`.
31. `test_bahasa_indonesia_narrative_generation`: Validates Indonesian commentary output.
32. `test_synthesizer_missing_macro_fallback`: Gracefully falls back when no macro events exist.
33. `test_synthesizer_duckdb_persistence`: Confirms insert into `daily_narrative_intelligence`.
34. `test_mart_macro_narrative_daily_view`: Queries conformed analytical view successfully.

### Group E: RiskGuard Circuit Breaker (`test_macro_risk_guard.py` - 6 tests)
35. `test_risk_guard_passes_in_normal_regime`: Regular trading passes pre-trade check.
36. `test_risk_guard_blocks_on_black_swan_flag`: Trips circuit breaker on black swan alert.
37. `test_risk_guard_blocks_near_macro_announcement`: Trips breaker within 2-hour window.
38. `test_risk_guard_allows_post_announcement`: Passes once macro window expires.
39. `test_risk_guard_blocks_severe_negative_mni`: Trips breaker when MNI $< -0.65$.
40. `test_risk_guard_preserves_solvency_and_kill_switch`: Existing Phase 15 checks remain active.

### Group F: Dashboard API & CLI Integration (`test_macro_dashboard_cli.py` - 7 tests)
41. `test_api_macro_radar_endpoint`: Returns valid JSON matching schema.
42. `test_api_macro_news_endpoint_with_urls`: Verifies all returned news items have valid URLs.
43. `test_api_macro_calendar_endpoint`: Returns list of economic events.
44. `test_api_fallback_when_db_empty`: Endpoints return resilient fallbacks if DB unpopulated.
45. `test_cli_macro_fetch_news`: Executes fetch-news command successfully.
46. `test_cli_macro_synthesize`: Executes daily synthesis from CLI.
47. `test_cli_macro_radar_output`: Renders tabular terminal radar output.

---

## 11. Acceptance Criteria (AC)

- **AC-1:** Multi-source RSS fetcher successfully pulls and parses feeds from CoinDesk, Cointelegraph, Decrypt, and Bitcoin Magazine with deterministic SHA-256 deduplication.
- **AC-2:** ForexFactory economic releases are parsed into structured surprise deltas ($\text{Actual} - \text{Forecast}$) with directional liquidity scores.
- **AC-3:** Lexical analyzer categorizes news into 4 pillars and tags black swan / emergency events with `CRITICAL` severity.
- **AC-4:** Synthesizer calculates Composite MNI using the $0.40 \cdot S_{\text{macro}} + 0.35 \cdot S_{\text{sentiment}} + 0.25 \cdot S_{\text{narrative}}$ formula and classifies market into 5 regimes.
- **AC-5:** DuckDB tables `macro_news_articles`, `macro_economic_releases`, `daily_narrative_intelligence`, and analytical view `mart_macro_narrative_daily` are initialized and queryable.
- **AC-6:** `RiskGuard.validate_pre_trade()` halts forward paper trade execution when `black_swan_flag == True` or within a $\pm 2$-hour high-impact macro window.
- **AC-7:** REST endpoints `/api/macro/radar`, `/api/macro/news`, and `/api/macro/calendar` return valid JSON with 100% resilient fallback if DuckDB is unpopulated.
- **AC-8:** Web UI renders the "Macro Radar" dashboard tab with anti-slop styling, tabular numbers, and verifiable original clickable hyperlinks (`target="_blank"`).
- **AC-9:** CLI subcommand `bitcoin-data macro` operates all lifecycle actions (`fetch-news`, `fetch-calendar`, `synthesize`, `radar`, `status`).
- **AC-10:** 100% test pass rate on all new Phase 16 tests ($\ge 45$) and zero regressions on prior phases ($>665$ existing tests passing).
- **AC-11:** Quality gates pass cleanly: `ruff check`, `ruff format --check`, `mypy src`.
- **AC-12:** Zero AI agent names, internal profile metadata, or prompt identifiers in code or commit messages.

---

## 12. Implementation Sequence & Boundaries

### Ordered Implementation Steps
1. **Contracts & Models**: Create `src/bitcoin_data_platform/macro/models.py`.
2. **DuckDB Schemas & Migrations**: Update `storage/duckdb_manager.py` with tables and analytical view.
3. **Feed Ingestion & Deduplication**: Implement `macro/feed_ingester.py`.
4. **Macro Economic Analyzer**: Implement `macro/macro_analyzer.py`.
5. **Sentiment & Polarity Engine**: Implement `macro/sentiment_analyzer.py`.
6. **Composite Synthesizer**: Implement `macro/synthesizer.py`.
7. **RiskGuard Extension**: Update `paper/risk_guard.py` with macro circuit breakers.
8. **Dashboard REST Endpoints**: Add `/api/macro/*` in `dashboard/server.py`.
9. **Web UI Macro Radar View**: Update `dashboard/assets/index.html`.
10. **CLI Integration**: Implement `macro/cli.py` and register in `src/bitcoin_data_platform/cli.py`.
11. **Documentation Updates**: Update `docs/data_dictionary/DATA_DICTIONARY.md`, `docs/roadmap/ROADMAP.md`, and `README.md`.
12. **Automated Testing & Verification**: Run test suite, quality gates, and verify zero regression.

### Preservation Constraints (Do Not Break)
- Do NOT modify historical raw ingestion envelope logic or batch parquet writers.
- Do NOT alter watermark semantics in `pipeline_watermark`.
- Do NOT modify Phase 14 backtest math or fee calculation logic.
- Do NOT introduce external heavy browser dependencies (`playwright`, `selenium`, `browser-use`).
