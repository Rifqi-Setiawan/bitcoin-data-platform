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
6. [Type System & Serialization Conventions](#6-type-system--serialization-conventions)

---

## 1. Entity Relationship & Grain Summary

| View / Table Name | Layer | Grain | Primary / Natural Key | Storage Engine |
| :--- | :--- | :--- | :--- | :--- |
| `fact_market_candle_hourly` | Curated Fact | 1 hour per source + product | `(source, product_id, candle_start_utc)` | Hive Parquet (`curated/market/candles_hourly/`) |
| `mart_btc_usd_daily` | Analytical Mart | 1 UTC day per source + product | `(source, product_id, trade_date_utc)` | DuckDB SQL View |
| `fact_network_metrics_daily` | Curated Fact | 1 UTC day per source + asset | `(source, asset, metric_date_utc)` | Hive Parquet (`curated/onchain/network_metrics_daily/`) |
| `mart_btc_market_and_network_daily` | Conformed Mart | 1 UTC day per asset | `(asset, trade_date_utc)` | DuckDB SQL View (FULL OUTER JOIN) |

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

## 6. Type System & Serialization Conventions

- **Monetary & Volume Precision**: All monetary prices and asset volumes are maintained as fixed-point `DECIMAL(38,18)` in Parquet and DuckDB to eliminate binary floating-point roundoff errors.
- **Timezone Invariant**: All timestamps are strictly UTC with explicit timezone offset (`TIMESTAMPTZ` / `pyarrow.timestamp("us", tz="UTC")`). Naive datetimes are forbidden.
- **Research Serving Formats**:
  - `parquet`: Vectorized columnar format with Snappy compression; preserves high-precision Decimals and 64-bit integers.
  - `arrow`: PyArrow IPC format for zero-copy memory mapping and IPC transport.
  - `csv`: RFC 4180 standard comma-separated representation with ISO-8601 timestamps.
  - `json`: Standard UTF-8 JSON array of objects with string-serialized timestamps and decimals for precision preservation.
