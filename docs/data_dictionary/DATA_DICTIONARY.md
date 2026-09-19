# Bitcoin Data Platform — Official Data Dictionary

Status: Production Reference  
Version: 1.0.0 (Phase 8 Research Serving Layer)  
Verification: Automated Test Suite & Data Modeling Quality Gates  

---

## Overview

This Data Dictionary defines the official logical schemas, storage layers, natural keys, granularities, data types, nullability constraints, and business definitions for all curated fact views and analytical data marts within the Bitcoin Data Platform.

The platform employs a two-tier storage and modeling architecture:
1. **Curated Parquet Layer**: Immutable, columnar Parquet files stored on the local filesystem with Hive-style partitioning (`year=YYYY`), providing schema enforcement and fast vectorized scanning.
2. **DuckDB Analytical Mart Layer**: Zero-copy in-process SQL views exposing normalized facts and aggregated analytical marts without redundant data duplication.

---

## Table of Contents

1. [Entity Relationship & Grain Summary](#1-entity-relationship--grain-summary)
2. [fact_market_candle_hourly](#2-fact_market_candle_hourly)
3. [mart_btc_usd_daily](#3-mart_btc_usd_daily)
4. [fact_network_metrics_daily](#4-fact_network_metrics_daily)
5. [mart_btc_market_and_network_daily](#5-mart_btc_market_and_network_daily)
6. [raw_crypto_sentiment_daily](#6-raw_crypto_sentiment_daily)
7. [raw_macro_economic_events](#7-raw_macro_economic_events)
8. [mart_btc_investment_signals_daily](#8-mart_btc_investment_signals_daily)
9. [signal_history](#9-signal_history)
10. [news_sentinel_alerts](#10-news_sentinel_alerts)
11. [Backtest & Validation Contracts](#11-backtest--validation-contracts)
12. [Forward Paper Trading Tables](#12-forward-paper-trading-tables)
13. [Type System & Serialization Conventions](#13-type-system--serialization-conventions)

---

## 1. Entity Relationship & Grain Summary

| View / Table Name | Layer | Grain | Primary / Natural Key | Storage Engine |
| :--- | :--- | :--- | :--- | :--- |
| `fact_market_candle_hourly` | Curated Fact | 1 hour per source + product | `(source, product_id, candle_start_utc)` | Hive Parquet (`curated/market/candles_hourly/`) |
| `mart_btc_usd_daily` | Analytical Mart | 1 UTC day per source + product | `(source, product_id, trade_date_utc)` | DuckDB SQL View |
| `fact_network_metrics_daily` | Curated Fact | 1 UTC day per source + asset | `(source, asset, metric_date_utc)` | Hive Parquet (`curated/onchain/network_metrics_daily/`) |
| `mart_btc_market_and_network_daily` | Conformed Mart | 1 UTC day per asset | `(asset, trade_date_utc)` | DuckDB SQL View (FULL OUTER JOIN) |
| `raw_crypto_sentiment_daily` | Ingestion Table | 1 UTC day | `sentiment_date_utc` | DuckDB Table |
| `raw_macro_economic_events` | Ingestion Table | 1 scheduled event | `event_id` | DuckDB Table |
| `mart_btc_investment_signals_daily` | Analytical Mart | 1 UTC day | `trade_date_utc` | DuckDB SQL View |
| `signal_history` | Audit & Serving | 1 UTC day | `signal_date_utc` | DuckDB Table |
| `news_sentinel_alerts` | Ingestion & Alerting | 1 alert event | `alert_id` | DuckDB Table |
| `paper_portfolio_balance` | Paper Trading State | 1 portfolio | `portfolio_id` | DuckDB Table |
| `paper_portfolio_snapshots_daily` | Paper Trading Snapshot | 1 UTC day per portfolio | `(snapshot_date, portfolio_id)` | DuckDB Table |
| `paper_trade_ledger` | Paper Trading Ledger | 1 trade order | `trade_id` | DuckDB Table |

---

## 2. fact_market_candle_hourly

### Description
Contains normalized, validated 1-hour OHLCV market trading candles for Bitcoin against USD. Each row represents a non-overlapping, half-open interval `[candle_start_utc, candle_start_utc + 1 hour)`.

- **Layer**: Curated Fact Layer (Market Domain)
- **Physical Location**: `curated/market/candles_hourly/source=coinbase_exchange/year=YYYY/*.parquet`
- **Upstream Source**: Coinbase Exchange REST API (`/products/BTC-USD/candles`)
- **Natural Key**: `(source, product_id, candle_start_utc)`

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `source` | `VARCHAR` | No | Originating exchange identifier (`coinbase_exchange`). Partition key. |
| `product_id` | `VARCHAR` | No | Market trading pair instrument symbol (`BTC-USD`). |
| `granularity_seconds` | `INTEGER` | No | Duration of candle bucket in seconds (`3600` for hourly). |
| `candle_start_utc` | `TIMESTAMPTZ` | No | Start of the hourly interval in UTC (`YYYY-MM-DD HH:00:00+00`). |
| `open` | `DECIMAL(38,18)` | No | Execution price of the first trade within the hourly window (USD). |
| `high` | `DECIMAL(38,18)` | No | Highest execution price during the hourly window (USD). |
| `low` | `DECIMAL(38,18)` | No | Lowest execution price during the hourly window (USD). |
| `close` | `DECIMAL(38,18)` | No | Execution price of the final trade within the hourly window (USD). |
| `volume_base` | `DECIMAL(38,18)` | No | Total quantity of base asset (BTC) traded across the hourly interval. |
| `ingested_at_utc` | `TIMESTAMPTZ` | No | Timestamp when the raw envelope was ingested into the platform. |
| `source_run_id` | `VARCHAR` | No | Unique UUID of the batch pipeline run responsible for ingestion. |
| `year` | `INTEGER` | No | UTC calendar year extracted from `candle_start_utc`. Partition key. |

---

## 3. mart_btc_usd_daily

### Description
Aggregated analytical mart summarizing 24-hour trading activity for BTC-USD across UTC calendar days. Derived dynamically from `fact_market_candle_hourly`.

- **Layer**: Analytical Mart Layer (Market Domain)
- **Implementation**: DuckDB SQL View
- **Natural Key**: `(source, product_id, trade_date_utc)`
- **Grain**: 1 row per UTC calendar day

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `source` | `VARCHAR` | No | Data provider identifier (`coinbase_exchange`). |
| `product_id` | `VARCHAR` | No | Trading instrument (`BTC-USD`). |
| `trade_date_utc` | `TIMESTAMPTZ` | No | Midnight boundary of the UTC calendar day (`DATE_TRUNC('day', candle_start_utc)`). |
| `open` | `DECIMAL(38,18)` | No | Opening price of the day (`FIRST(open ORDER BY candle_start_utc)`). |
| `high` | `DECIMAL(38,18)` | No | Daily intraday high (`MAX(high)`). |
| `low` | `DECIMAL(38,18)` | No | Daily intraday low (`MIN(low)`). |
| `close` | `DECIMAL(38,18)` | No | Closing price of the day (`LAST(close ORDER BY candle_start_utc)`). |
| `volume_base` | `DECIMAL(38,18)` | No | Total daily traded volume in Bitcoin (`SUM(volume_base)`). |
| `observed_hour_count` | `BIGINT` | No | Number of distinct hourly candles present in the calendar day (`COUNT(*)`). |
| `is_complete` | `BOOLEAN` | No | `TRUE` if `observed_hour_count = 24`, indicating full daily coverage; otherwise `FALSE`. |

---

## 4. fact_network_metrics_daily

### Description
Normalized daily on-chain network activity observations for Bitcoin, tracking transaction throughput and active address participation.

- **Layer**: Curated Fact Layer (On-Chain Domain)
- **Physical Location**: `curated/onchain/network_metrics_daily/source=coin_metrics/year=YYYY/*.parquet`
- **Upstream Source**: Coin Metrics Community API v4 (`/timeseries/asset-metrics`)
- **Natural Key**: `(source, asset, metric_date_utc)`
- **Grain**: 1 row per UTC calendar day

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `source` | `VARCHAR` | No | Upstream data provider identifier (`coin_metrics`). Partition key. |
| `asset` | `VARCHAR` | No | Crypto asset ticker (`BTC`). |
| `metric_date_utc` | `TIMESTAMPTZ` | No | Midnight UTC timestamp of the observation day. |
| `transaction_count` | `BIGINT` | No | Number of confirmed on-chain transactions on the Bitcoin ledger (`TxCnt`). Must be >= 0. |
| `active_addresses_count` | `BIGINT` | No | Count of unique active on-chain addresses participating as senders or receivers (`AdrActCnt`). Must be >= 0. |
| `mvrv_ratio` | `DOUBLE` | Yes | Daily Market Value to Realized Value ratio (`CapMVRVCur`) from Coin Metrics. Null for historical records prior to MVRV tracking. |
| `ingested_at_utc` | `TIMESTAMPTZ` | No | UTC timestamp when the metric payload was retrieved from provider. |
| `source_run_id` | `VARCHAR` | No | Unique UUID of the fetch-network pipeline run. |
| `year` | `INTEGER` | No | Partition year extracted from `metric_date_utc`. Partition key. |

---

## 5. mart_btc_market_and_network_daily

### Description
Conformed cross-domain analytical mart bridging off-chain trading dynamics and on-chain network fundamentals. Joined on UTC date (`trade_date_utc = metric_date_utc`) using a `FULL OUTER JOIN` to accommodate asynchronous data arrivals without data loss.

- **Layer**: Conformed Analytical Mart Layer (Cross-Domain Serving)
- **Implementation**: DuckDB SQL View
- **Natural Key**: `(asset, trade_date_utc)`
- **Grain**: 1 row per UTC calendar day

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `trade_date_utc` | `TIMESTAMPTZ` | No | Conformed UTC calendar date (`COALESCE(m.trade_date_utc, n.metric_date_utc)`). |
| `asset` | `VARCHAR` | No | Asset symbol (`BTC`). |
| `market_open_usd` | `DECIMAL(38,18)` | Yes | Day's opening price in USD. Null if market data missing. |
| `market_high_usd` | `DECIMAL(38,18)` | Yes | Day's intraday high in USD. Null if market data missing. |
| `market_low_usd` | `DECIMAL(38,18)` | Yes | Day's intraday low in USD. Null if market data missing. |
| `market_close_usd` | `DECIMAL(38,18)` | Yes | Day's closing price in USD. Null if market data missing. |
| `market_volume_btc` | `DECIMAL(38,18)` | Yes | Day's total traded base volume in BTC. Null if market data missing. |
| `market_observed_hour_count` | `BIGINT` | Yes | Count of recorded market hours for the day. Null if market data missing. |
| `is_market_day_complete` | `BOOLEAN` | Yes | `TRUE` if all 24 hours of market trading are present; otherwise `FALSE` or `NULL`. |
| `transaction_count` | `BIGINT` | Yes | Daily on-chain transaction count. Null if on-chain data missing. |
| `active_addresses_count` | `BIGINT` | Yes | Daily active address count. Null if on-chain data missing. |
| `tx_per_active_address` | `DOUBLE` | Yes | Network velocity ratio: `ROUND(transaction_count / active_addresses_count, 4)`. Null if active addresses <= 0 or missing. |

---

## 6. raw_crypto_sentiment_daily

### Description
Daily Crypto Fear & Greed Index ingested from Alternative.me, measuring market sentiment from extreme fear to extreme greed.

- **Layer**: Raw Ingestion Layer (Sentiment Domain)
- **Implementation**: DuckDB Table
- **Primary Key**: `sentiment_date_utc`
- **Grain**: 1 row per UTC calendar day

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `sentiment_date_utc` | `DATE` | No | Calendar date of sentiment reading in UTC. Primary Key. |
| `fng_value` | `INTEGER` | No | Sentiment index score from 0 (Extreme Fear) to 100 (Extreme Greed). |
| `fng_classification` | `VARCHAR` | No | Sentiment category: `Extreme Fear`, `Fear`, `Neutral`, `Greed`, `Extreme Greed`. |
| `ingested_at_utc` | `TIMESTAMPTZ` | No | UTC timestamp when the sentiment record was ingested. |

---

## 7. raw_macro_economic_events

### Description
Scheduled macroeconomic calendar events ingested from ForexFactory, focusing on high-impact US economic indicators (FOMC, CPI, Non-Farm Payrolls).

- **Layer**: Raw Ingestion Layer (Macroeconomic Domain)
- **Implementation**: DuckDB Table
- **Primary Key**: `event_id`
- **Grain**: 1 row per scheduled event

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `event_id` | `VARCHAR` | No | Deterministic SHA-256 hash of `(title, scheduled_utc)`. Primary Key. |
| `country` | `VARCHAR` | No | Currency/country code (`USD`). |
| `title` | `VARCHAR` | No | Event release title (e.g. `FOMC Statement`, `CPI m/m`). |
| `impact` | `VARCHAR` | No | Forecasted market impact level (`High`, `Medium`, `Low`, `Holiday`). |
| `scheduled_utc` | `TIMESTAMPTZ` | No | Scheduled event release timestamp in UTC. |
| `forecast` | `VARCHAR` | Yes | Market consensus forecast value. Null if unforecasted. |
| `previous` | `VARCHAR` | Yes | Prior period reported metric value. Null if absent. |
| `ingested_at_utc` | `TIMESTAMPTZ` | No | UTC timestamp when the event was ingested. |

---

## 8. mart_btc_investment_signals_daily

### Description
Multi-domain conformed analytical mart joining daily market prices, 200-day rolling moving averages, Mayer Multiple, on-chain MVRV ratio, market sentiment, and macroeconomic event awareness into deterministic investment allocation signals.

- **Layer**: Conformed Analytical Mart Layer (Investment Strategy Serving)
- **Implementation**: DuckDB SQL View
- **Natural Key**: `trade_date_utc`
- **Grain**: 1 row per UTC calendar day

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `trade_date_utc` | `TIMESTAMPTZ` | No | Midnight boundary of observation day in UTC. |
| `market_close_usd` | `DECIMAL(38,18)` | Yes | Closing trade execution price in USD for the day. |
| `sma_200` | `DOUBLE` | Yes | 200-day simple moving average of daily close prices (`AVG(close) OVER (...)`). |
| `mayer_multiple` | `DOUBLE` | Yes | Mayer Multiple: `market_close_usd / sma_200`. Ratio of current price to 200-day SMA. |
| `mvrv_ratio` | `DOUBLE` | Yes | Market Value to Realized Value ratio from on-chain metrics. Null if unobserved. |
| `fng_value` | `INTEGER` | No | Fear & Greed Index score (0–100). Defaults to 50 if sentiment unobserved. |
| `fng_classification` | `VARCHAR` | No | Fear & Greed label. Defaults to `Neutral` if sentiment unobserved. |
| `has_high_impact_macro_event` | `BOOLEAN` | No | `TRUE` if at least one High-impact USD macro event was scheduled on this date; else `FALSE`. |
| `investment_signal` | `VARCHAR` | No | Rule-based tactical asset allocation signal: `AGGRESSIVE_ACCUMULATE`, `OPPORTUNISTIC_ACCUMULATE`, `STANDARD_DCA`, `DEFENSIVE_RESERVE`, or `HARD_FREEZE`. |

---

## 9. signal_history

### Description
Audit trail and historical store of generated daily investment signals, indicator snapshots, signal strength assessments, and human-readable narratives in Bahasa Indonesia.

- **Layer**: Audit & Serving Layer
- **Implementation**: DuckDB Table
- **Natural Key**: `signal_date_utc`
- **Grain**: 1 row per UTC calendar day

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `signal_date_utc` | `DATE` | No | Target calendar date for the investment signal in UTC. Primary key. |
| `generated_at_utc` | `TIMESTAMPTZ` | No | Timestamp when the signal was computed and recorded. |
| `market_close_usd` | `DOUBLE` | Yes | Market close price in USD evaluated at generation. |
| `sma_200` | `DOUBLE` | Yes | 200-day simple moving average value evaluated at generation. |
| `mayer_multiple` | `DOUBLE` | Yes | Mayer Multiple evaluated at generation. |
| `mvrv_ratio` | `DOUBLE` | Yes | On-chain MVRV ratio evaluated at generation. |
| `fng_value` | `INTEGER` | No | Fear & Greed index score (0–100) at generation. |
| `fng_classification` | `VARCHAR` | No | Fear & Greed classification label at generation. |
| `has_high_impact_macro_event` | `BOOLEAN` | No | Flag indicating whether a high-impact macroeconomic event coincided with the date. |
| `investment_signal` | `VARCHAR` | No | Computed regime signal (`AGGRESSIVE_ACCUMULATE`, `OPPORTUNISTIC_ACCUMULATE`, `STANDARD_DCA`, `DEFENSIVE_RESERVE`, `HARD_FREEZE`). |
| `signal_strength` | `VARCHAR` | No | Multi-indicator agreement strength (`STRONG`, `MODERATE`, `WEAK`). |
| `narrative` | `VARCHAR` | Yes | Contextual explanation and rationale for the signal in Bahasa Indonesia. |

---

## 10. news_sentinel_alerts

### Description
Deduplicated log of market-moving news events detected by the CoinDesk RSS scanner, tracking keywords, severity, and Telegram dispatch status.

- **Layer**: Ingestion & Alerting Layer
- **Implementation**: DuckDB Table
- **Natural Key**: `alert_id`
- **Grain**: 1 row per distinct news event

### Schema

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `alert_id` | `VARCHAR` | No | SHA-256 hash of headline title + published timestamp. Primary key. |
| `title` | `VARCHAR` | No | Headline title of the news article. |
| `link` | `VARCHAR` | Yes | Web URL link to the original article. |
| `published_utc` | `TIMESTAMPTZ` | Yes | Publication timestamp of the article converted to UTC. |
| `matched_keywords` | `VARCHAR` | Yes | Comma-separated list of trigger keywords matched in title/description. |
| `severity` | `VARCHAR` | No | Alert severity level (`CRITICAL` or `WARNING`). |
| `ingested_at_utc` | `TIMESTAMPTZ` | No | UTC timestamp when the sentinel scanned and recorded the alert. |
| `telegram_sent` | `BOOLEAN` | No | Dispatch status flag indicating whether the alert was sent to Telegram. |

---

## 11. Backtest Models & Performance Metrics

### Description
Data contracts and quantitative metrics generated by the event-driven Backtest & Validation Engine (`src/bitcoin_data_platform/backtest/`).

### BacktestDayRecord Schema
Represents input simulation records loaded chronologically from `mart_btc_investment_signals_daily`.

| Field Name | Data Type | Description |
| :--- | :--- | :--- |
| `trade_date` | `DATE` | Simulation calendar trade date. |
| `market_close_usd` | `FLOAT` | Bitcoin spot close price in USD evaluated at end of day. |
| `sma_200` | `FLOAT` | 200-day simple moving average price. |
| `mayer_multiple` | `FLOAT` | Ratio of close price to SMA-200. |
| `mvrv_ratio` | `FLOAT` | On-chain Market-Value-to-Realized-Value ratio. |
| `fng_value` | `INTEGER` | Alternative.me Fear & Greed sentiment index (0–100). |
| `has_high_impact_macro_event` | `BOOLEAN` | High-impact USD macroeconomic calendar event flag. |
| `investment_signal` | `VARCHAR` | 5-tier classification signal (`AGGRESSIVE_ACCUMULATE`, `OPPORTUNISTIC_ACCUMULATE`, `STANDARD_DCA`, `DEFENSIVE_RESERVE`, `HARD_FREEZE`). |

### DailyPortfolioState Schema
Daily mark-to-market portfolio snapshot recorded throughout simulation.

| Field Name | Data Type | Description |
| :--- | :--- | :--- |
| `trade_date` | `DATE` | Current simulation date. |
| `cash_balance` | `FLOAT` | Active fiat cash in base pool ($USD). |
| `reserve_cash_balance` | `FLOAT` | Dry-powder tactical reserve cash balance ($USD). |
| `btc_balance` | `FLOAT` | Accumulated Bitcoin asset balance. |
| `btc_price` | `FLOAT` | BTC/USD valuation mark price on date. |
| `portfolio_equity` | `FLOAT` | Mark-to-market liquidation equity (`cash + reserve + btc * price`). |
| `total_contributed` | `FLOAT` | Cumulative fiat cash injected into simulation ($USD). |
| `daily_cash_flow` | `FLOAT` | External fiat capital injected on current step ($USD). |
| `daily_return` | `FLOAT` | Time-weighted daily portfolio return (`(V_t - c_t - V_{t-1}) / V_{t-1}`). |
| `drawdown` | `FLOAT` | Fractional percentage drop from historical peak equity (`(V_t - peak) / peak`). |
| `action_taken` | `VARCHAR` | Description of trade or cash transfer execution. |

### Quantitative Performance Metrics Summary
Metrics calculated by `compute_strategy_result`:

| Metric Name | Formula / Definition | Description |
| :--- | :--- | :--- |
| `total_return_pct` | `(V_final - Total_Invested) / Total_Invested * 100%` | Simple cumulative return on capital. |
| `cagr_pct` | `(V_final / Total_Invested) ^ (365 / N) - 1` | Annualized Compound Annual Growth Rate on 365-day basis. |
| `max_drawdown_pct` | `abs(min(Drawdown_t)) * 100%` | Maximum percentage peak-to-trough decline. |
| `sharpe_ratio` | `sqrt(365) * (mean(R) - Rf/365) / stdev(R)` | Risk-adjusted return normalized by total volatility (Rf = 3.0%). |
| `sortino_ratio` | `sqrt(365) * (mean(R) - Rf/365) / sigma_down` | Risk-adjusted return penalizing only downside semivariance. |
| `calmar_ratio` | `CAGR / abs(MDD)` | Return efficiency relative to maximum drawdown. |
| `acquisition_discount_pct` | `(Avg_Market_Price - Avg_Buy_Price) / Avg_Market_Price * 100%` | Cost basis efficiency vs average market price. |
| `reserve_pool_peak` | `max(0..T, Reserve_Cash_t)` | Highest tactical reserve cash accumulated during simulation. |

---

## 12. Forward Paper Trading Tables

### 12.1 paper_portfolio_balance

#### Description
Stores current mark-to-market balances across separated Base Cash and Tactical Reserve Cash pools, Bitcoin stack, and transaction counters.

- **Layer**: Paper Trading State Layer
- **Physical Location**: `data/state/platform.duckdb`
- **Primary Key**: `portfolio_id`

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `portfolio_id` | `VARCHAR` | No | Unique identifier for portfolio (`default`). Primary key. |
| `initial_cash` | `DOUBLE` | No | Initial USD virtual capital allocation (e.g. `$1,000.00`). |
| `base_cash` | `DOUBLE` | No | Active liquid cash in base DCA pool (starts at 70% of initial). |
| `reserve_cash` | `DOUBLE` | No | Tactical dry-powder reserve cash pool (starts at 30% of initial). |
| `btc_balance` | `DOUBLE` | No | Total accumulated Bitcoin holdings (8 decimal precision). |
| `total_contributed` | `DOUBLE` | No | Cumulative USD capital allocated to portfolio. |
| `last_updated_utc` | `TIMESTAMPTZ` | No | UTC timestamp of last balance update. |
| `total_trades` | `INTEGER` | No | Total count of executed purchase transactions. |

### 12.2 paper_portfolio_snapshots_daily

#### Description
Maintains daily chronological equity snapshots comparing the Dynamic Reserve DCA portfolio against the $1,000 Lump Sum Buy & Hold benchmark.

- **Layer**: Paper Trading Snapshot Layer
- **Physical Location**: `data/state/platform.duckdb`
- **Primary Key**: `(snapshot_date, portfolio_id)`

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `snapshot_date` | `DATE` | No | Snapshot calendar trade date (UTC). Component of primary key. |
| `portfolio_id` | `VARCHAR` | No | Target portfolio identifier. Component of primary key. |
| `base_cash` | `DOUBLE` | No | End-of-day base cash pool balance (USD). |
| `reserve_cash` | `DOUBLE` | No | End-of-day tactical reserve cash balance (USD). |
| `total_cash` | `DOUBLE` | No | Total cash holdings (`base_cash + reserve_cash`). |
| `btc_balance` | `DOUBLE` | No | Accumulated Bitcoin holding balance. |
| `btc_price` | `DOUBLE` | No | Valuation mark spot price (USD). |
| `portfolio_equity` | `DOUBLE` | No | Total liquidation equity (`total_cash + btc_balance * btc_price`). |
| `unrealized_pnl_usd` | `DOUBLE` | No | Net profit/loss in USD (`portfolio_equity - initial_cash`). |
| `unrealized_pnl_pct` | `DOUBLE` | No | Percentage return on virtual capital. |
| `benchmark_equity` | `DOUBLE` | No | Valuation of $1,000 invested at Day 0 opening price. |

### 12.3 paper_trade_ledger

#### Description
Immutable execution order blotter recording all systematic DCA purchases and hold decisions with simulated 10 bps Coinbase Spot fees.

- **Layer**: Paper Trading Ledger Layer
- **Physical Location**: `data/state/platform.duckdb`
- **Primary Key**: `trade_id`

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `trade_id` | `VARCHAR` | No | Unique trade execution identifier (`tr_...`). Primary key. |
| `portfolio_id` | `VARCHAR` | No | Associated portfolio identifier. |
| `executed_at_utc` | `TIMESTAMPTZ` | No | Execution timestamp in UTC. |
| `trade_date` | `DATE` | No | Effective calendar trade date. |
| `side` | `VARCHAR` | No | Transaction order side (`BUY` or `HOLD`). |
| `signal_regime` | `VARCHAR` | No | Analytical signal regime (`AGGRESSIVE_ACCUMULATE`, etc.). |
| `spot_price` | `DOUBLE` | No | Execution spot price in USD. |
| `gross_amount_usd` | `DOUBLE` | No | Total USD capital deployed for order. |
| `fee_usd` | `DOUBLE` | No | Deducted transaction commission (10 bps = 0.10%). |
| `net_amount_usd` | `DOUBLE` | No | Net USD deployed into Bitcoin (`gross - fee`). |
| `btc_amount` | `DOUBLE` | No | Bitcoin amount received (`net / spot_price`). |
| `narrative` | `VARCHAR` | No | Human-readable strategy rationale in Bahasa Indonesia. |

---

## 13. Phase 16: Macro & Narrative Intelligence Tables & Marts

### 13.1 macro_news_articles

#### Description
Stores curated, deduplicated multi-source crypto news articles with lexical polarity scoring, thematic pillar classification, and black swan severity tagging.

- **Layer**: Macro & Narrative Ingestion Layer
- **Physical Location**: `data/state/platform.duckdb`
- **Primary Key**: `article_id` (SHA-256 hash of `source:url:title`)

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `article_id` | `VARCHAR` | No | Deterministic SHA-256 hash of source, url, and title. Primary key. |
| `source` | `VARCHAR` | No | Publisher feed source (e.g. `CoinDesk`, `Cointelegraph`, `Decrypt`, `BitcoinMagazine`). |
| `title` | `VARCHAR` | No | Human-readable article headline text. |
| `url` | `VARCHAR` | No | Verifiable canonical hyperlink pointing to upstream original publisher. |
| `published_utc` | `TIMESTAMPTZ` | No | RFC 822 / ISO-8601 publication timestamp normalized to UTC. |
| `summary` | `VARCHAR` | No | Cleaned textual abstract or excerpt without HTML tags. |
| `pillar` | `VARCHAR` | No | Thematic pillar (`REGULATORY`, `SECURITY_EXPLOIT`, `INSTITUTIONAL`, `MACRO_LIQUIDITY`, `GENERAL`). |
| `severity` | `VARCHAR` | No | Alert severity level (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`). |
| `polarity` | `DOUBLE` | No | Lexical sentiment polarity score bounded strictly within `[-1.0, +1.0]`. |
| `matched_keywords` | `VARCHAR` | No | Comma-separated matched keyword tokens for full scoring auditability. |
| `ingested_at_utc` | `TIMESTAMPTZ` | No | UTC timestamp when the article was fetched and persisted. |

### 13.2 macro_economic_releases

#### Description
Maintains parsed macroeconomic calendar announcements with actual vs. forecast economic surprise deltas and directional liquidity impact scores.

- **Layer**: Macro Ingestion & Analytics Layer
- **Physical Location**: `data/state/platform.duckdb`
- **Primary Key**: `release_id` (SHA-256 hash of `event_name:date_utc:country`)

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `release_id` | `VARCHAR` | No | Deterministic SHA-256 release identifier. Primary key. |
| `event_name` | `VARCHAR` | No | Indicator title (e.g. `Core CPI m/m`, `Non-Farm Employment Change`, `Fed Funds Rate`). |
| `country` | `VARCHAR` | No | Currency/sovereign denomination code (e.g. `USD`). |
| `release_date` | `DATE` | No | Calendar release date in UTC. |
| `release_time_utc` | `VARCHAR` | No | Release time of day (e.g. `12:30` or `18:00`). |
| `impact` | `VARCHAR` | No | ForexFactory qualitative market impact rating (`High`, `Medium`, `Low`, `Holiday`). |
| `actual_value` | `DOUBLE` | Yes | Published actual numeric figure, or `NULL` if upcoming. |
| `forecast_value` | `DOUBLE` | Yes | Institutional consensus forecasted metric value. |
| `previous_value` | `DOUBLE` | Yes | Prior calendar period metric figure. |
| `surprise_delta` | `DOUBLE` | Yes | Normalized economic surprise delta (`actual - forecast`). |
| `directional_score` | `DOUBLE` | No | Directional liquidity impact score bounded within `[-1.0, +1.0]` (Hawkish < 0, Dovish > 0). |
| `raw_payload_json` | `VARCHAR` | Yes | Original upstream JSON payload string for audit compliance. |
| `ingested_at_utc` | `TIMESTAMPTZ` | No | UTC timestamp of ingestion. |

### 13.3 daily_narrative_intelligence

#### Description
Stores daily consolidated reports combining 3-tier synthesis (Hard Macro, Market Sentiment, Narrative Polarity), Composite MNI, 5-regime classification, and Indonesian market commentary.

- **Layer**: Macro Narrative Mart Layer
- **Physical Location**: `data/state/platform.duckdb`
- **Primary Key**: `intelligence_date`

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `intelligence_date` | `DATE` | No | Calendar evaluation date (UTC). Primary key. |
| `synthesized_at_utc` | `TIMESTAMPTZ` | No | Exact UTC timestamp of report calculation and persistence. |
| `hard_macro_score` | `DOUBLE` | No | Tier 1 exponential time-decayed economic score in `[-1.0, +1.0]`. |
| `sentiment_score` | `DOUBLE` | No | Tier 2 composite sentiment score (40% FNG + 30% MVRV + 30% Mayer Multiple) in `[-1.0, +1.0]`. |
| `narrative_score` | `DOUBLE` | No | Tier 3 hyperbolic tangent saturated news polarity score in `[-1.0, +1.0]`. |
| `composite_mni` | `DOUBLE` | No | Composite Macro-Narrative Index (`0.40 * S_macro + 0.35 * S_sentiment + 0.25 * S_narrative`). |
| `regime` | `VARCHAR` | No | 5-regime classification (`RISK_ON_EXPANSION`, `CAUTIOUS_BULL`, `NEUTRAL_CHOP`, `RISK_OFF_DEFENSE`, `BLACK_SWAN_CRISIS`). |
| `black_swan_flag` | `BOOLEAN` | No | Emergency binary indicator tripped by critical severity alerts or extreme contraction. |
| `active_critical_alerts`| `INTEGER` | No | Count of active critical black swan alerts within trailing 24 hours. |
| `dominant_pillar` | `VARCHAR` | No | Thematic news pillar exerting highest influence on current market regime. |
| `narrative_summary_id` | `VARCHAR` | No | Localized Bahasa Indonesia intelligence narrative commentary with emoji posture indicators. |

### 13.4 mart_macro_narrative_daily (Analytical View)

#### Description
Unified conformed analytical serving view joining daily market prices, moving averages, valuation multiples, and 3-tier macro narrative intelligence.

- **Layer**: Curated Analytical Mart View
- **Physical Location**: `data/state/platform.duckdb`
- **Base Tables/Views**: `mart_btc_investment_signals_daily`, `daily_narrative_intelligence`

| Column Name | Data Type | Nullable | Description & Business Rules |
| :--- | :--- | :--- | :--- |
| `trade_date_utc` | `DATE` | No | Calendar trade date in UTC. |
| `market_close_usd` | `DOUBLE` | Yes | Daily closing spot price in USD. |
| `sma_200` | `DOUBLE` | Yes | 200-day rolling simple moving average close. |
| `mayer_multiple` | `DOUBLE` | Yes | Mayer Multiple valuation metric (`close / sma_200`). |
| `mvrv_ratio` | `DOUBLE` | Yes | On-chain Market Value to Realized Value ratio. |
| `fng_value` | `INTEGER` | Yes | Daily Crypto Fear & Greed Index score (0 to 100). |
| `investment_signal` | `VARCHAR` | Yes | 5-tier quantitative DCA signal (`AGGRESSIVE_ACCUMULATE`, etc.). |
| `composite_mni` | `DOUBLE` | No | Composite Macro-Narrative Index in `[-1.0, +1.0]`. Defaults to `0.0`. |
| `macro_regime` | `VARCHAR` | No | Macro-Narrative regime classification. Defaults to `'NEUTRAL_CHOP'`. |
| `black_swan_flag` | `BOOLEAN` | No | Emergency halt sentinel flag. Defaults to `FALSE`. |
| `hard_macro_score` | `DOUBLE` | No | Tier 1 Hard Macro subscore in `[-1.0, +1.0]`. |
| `sentiment_score` | `DOUBLE` | No | Tier 2 Sentiment subscore in `[-1.0, +1.0]`. |
| `narrative_score` | `DOUBLE` | No | Tier 3 Narrative subscore in `[-1.0, +1.0]`. |
| `narrative_summary_id` | `VARCHAR` | Yes | Localized narrative commentary in Bahasa Indonesia. |
| `active_critical_alerts`| `INTEGER` | No | Count of active critical black swan alerts. |

---

## 14. Type System & Serialization Conventions

- **Monetary & Volume Precision**: All monetary prices and asset volumes are maintained as fixed-point `DECIMAL(38,18)` in Parquet and DuckDB to eliminate binary floating-point roundoff errors.
- **Timezone Invariant**: All timestamps are strictly UTC with explicit timezone offset (`TIMESTAMPTZ` / `pyarrow.timestamp("us", tz="UTC")`). Naive datetimes are forbidden.
- **Research Serving Formats**:
  - `parquet`: Vectorized columnar format with Snappy compression; preserves high-precision Decimals and 64-bit integers.
  - `arrow`: PyArrow IPC format for zero-copy memory mapping and IPC transport.
  - `csv`: RFC 4180 standard comma-separated representation with ISO-8601 timestamps.
  - `json`: Standard UTF-8 JSON array of objects with string-serialized timestamps and decimals for precision preservation.
