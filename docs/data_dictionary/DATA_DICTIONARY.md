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
11. [Type System & Serialization Conventions](#11-type-system--serialization-conventions)

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

## 11. Type System & Serialization Conventions

- **Monetary & Volume Precision**: All monetary prices and asset volumes are maintained as fixed-point `DECIMAL(38,18)` in Parquet and DuckDB to eliminate binary floating-point roundoff errors.
- **Timezone Invariant**: All timestamps are strictly UTC with explicit timezone offset (`TIMESTAMPTZ` / `pyarrow.timestamp("us", tz="UTC")`). Naive datetimes are forbidden.
- **Research Serving Formats**:
  - `parquet`: Vectorized columnar format with Snappy compression; preserves high-precision Decimals and 64-bit integers.
  - `arrow`: PyArrow IPC format for zero-copy memory mapping and IPC transport.
  - `csv`: RFC 4180 standard comma-separated representation with ISO-8601 timestamps.
  - `json`: Standard UTF-8 JSON array of objects with string-serialized timestamps and decimals for precision preservation.
