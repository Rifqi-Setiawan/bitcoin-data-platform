# Phase 7 Implementation Specification — Second Data Domain: Coin Metrics On-Chain Network Data & Conformed Modeling

Status: approved for implementation
Parent phase: Phase 7 — Second data domain and conformed modeling
Task ID: `P7-second-domain-onchain-modeling`
Recommended branch: `feature/P7-second-domain-onchain-modeling`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Integrate a second, heterogeneous data domain into the Bitcoin Data Platform: on-chain network metrics
from the public Coin Metrics Community API v4 (`TxCnt` - Transaction Count, and `AdrActCnt` - Active Address Count).
Establish conformed dimension modeling in DuckDB to reconcile grain mismatch (hourly market trading vs
daily network activity) via unified analytical mart `mart_btc_market_and_network_daily`, while preserving
zero source coupling, raw isolation, and independent pipeline watermarks.

## 2. User Stories

### On-Chain Network Data Ingestion
As a data engineer, I can execute:
```bash
bitcoin-data fetch-network \
  --start 2026-01-01 \
  --end 2026-01-07 \
  --output-dir ./data/raw/coin_metrics
```
to fetch daily on-chain network metrics for Bitcoin from the Coin Metrics Community REST API v4, validate
the source contract, and persist immutable, checksummed raw gzip JSON envelopes.

### On-Chain Curated Promotion & DuckDB View
As a data engineer, I can execute:
```bash
bitcoin-data promote-network \
  --raw-dir ./data/raw/coin_metrics \
  --curated-dir ./data/curated \
  --db-path ./data/state/platform.duckdb
```
to normalize raw network payloads into typed records, deduplicate by natural key `(source, asset, metric_date_utc)`,
write annual Parquet partitions in `curated/onchain/network_metrics_daily/source=coin_metrics/year=YYYY/`,
and register/update the `fact_network_metrics_daily` view and independent watermark `coin_metrics_daily`.

### Cross-Domain Analytical Modeling
As a quantitative researcher, I can query:
```bash
bitcoin-data query --db-path ./data/state/platform.duckdb \
  --sql "SELECT * FROM mart_btc_market_and_network_daily WHERE trade_date_utc >= '2026-01-01' ORDER BY trade_date_utc"
```
and receive unified daily rows aligning Bitcoin market pricing (open, high, low, close, volume, complete day flag)
with network activity (`transaction_count`, `active_addresses_count`) joined on the conformed `trade_date_utc` dimension.

## 3. Scope

### In Scope
1. **Coin Metrics Client & Contract (`sources/coin_metrics_client.py` & `sources/coin_metrics_contract.py`)**:
   - HTTP client targeting `https://community-api.coinmetrics.io/v4/timeseries/asset-metrics`.
   - Asset: `btc`, Metrics: `TxCnt,AdrActCnt`, Frequency: `1d`.
   - Strict contract validation: 200 OK, JSON structure with `data` array containing ISO timestamp and metric strings.
   - Non-negative integer/decimal parsing, rate limiting (10 req / 6s), bounded retries, and injectable transport.
2. **On-Chain Transformation & Parquet Writer (`transforms/network_normalizer.py` & `storage/network_parquet_writer.py`)**:
   - Parse raw envelopes into `NormalizedNetworkMetric` dataclass.
   - Deduplicate on `(source, asset, metric_date_utc)`.
   - Write annual Parquet partitions: `curated/onchain/network_metrics_daily/source=coin_metrics/year=YYYY/data.parquet`.
   - PyArrow schema with typed fields (`int64` / `decimal128`).
3. **Conformed DuckDB Modeling (`storage/duckdb_manager.py`)**:
   - Register view `fact_network_metrics_daily` over on-chain Parquet files.
   - Create conformed cross-domain mart `mart_btc_market_and_network_daily` joining `mart_btc_usd_daily` and `fact_network_metrics_daily` on `trade_date_utc`.
   - Track independent watermark for `pipeline_name = 'coin_metrics_daily'`.
4. **CLI Integration (`cli.py`)**:
   - Add subcommands `fetch-network` and `promote-network`.
5. **Architecture Decision Record (`docs/decisions/D-009_DBT_EVALUATION.md`)**:
   - Comprehensive architectural evaluation of `dbt-core` adoption vs managed native SQL views in DuckDB.
6. **Testing & Quality**:
   - Comprehensive test suite in `tests/test_coin_metrics_client.py`, `tests/test_coin_metrics_contract.py`,
     `tests/test_network_normalizer.py`, and `tests/test_cross_domain_mart.py`.
   - All 255 existing tests pass without regression (target > 285 total tests).

### Out of Scope
- Paid / Enterprise Coin Metrics endpoints (Community API only).
- WebSocket streaming for on-chain transactions (slated for Phase 9).
- Direct Bitcoin full node RPC parsing (Community API is sufficient for V1 portfolio scope).

## 4. Technical Specifications & Data Models

### 4.1 Coin Metrics Community API Contract
Endpoint: `GET https://community-api.coinmetrics.io/v4/timeseries/asset-metrics`
Parameters:
- `assets`: `btc`
- `metrics`: `TxCnt,AdrActCnt`
- `frequency`: `1d`
- `start_time`: ISO-8601 UTC date or timestamp (e.g. `2026-01-01`)
- `end_time`: ISO-8601 UTC date or timestamp (e.g. `2026-01-07`)

Response payload format:
```json
{
  "data": [
    {
      "asset": "btc",
      "time": "2026-01-01T00:00:00.000000000Z",
      "TxCnt": "345678",
      "AdrActCnt": "890123"
    }
  ]
}
```

### 4.2 Normalized Network Model (`NormalizedNetworkMetric`)
```python
@dataclass(frozen=True)
class NormalizedNetworkMetric:
    source: str                 # "coin_metrics"
    asset: str                  # "btc"
    metric_date_utc: datetime   # Aligned to YYYY-MM-DD 00:00:00 UTC
    transaction_count: int      # Parsed from TxCnt (non-negative)
    active_addresses_count: int # Parsed from AdrActCnt (non-negative)
    ingested_at_utc: datetime
    source_run_id: str
```

### 4.3 DuckDB Views

#### View 1: `fact_network_metrics_daily`
```sql
CREATE OR REPLACE VIEW fact_network_metrics_daily AS
SELECT * FROM read_parquet(
    'curated/onchain/network_metrics_daily/source=*/year=*/*.parquet',
    hive_partitioning=true
);
```

#### View 2: `mart_btc_market_and_network_daily`
```sql
CREATE OR REPLACE VIEW mart_btc_market_and_network_daily AS
SELECT
    COALESCE(m.trade_date_utc, n.metric_date_utc) AS trade_date_utc,
    'BTC' AS asset,
    m.open AS market_open_usd,
    m.high AS market_high_usd,
    m.low AS market_low_usd,
    m.close AS market_close_usd,
    m.volume_base AS market_volume_btc,
    m.observed_hour_count AS market_observed_hour_count,
    m.is_complete AS is_market_day_complete,
    n.transaction_count,
    n.active_addresses_count,
    CASE 
        WHEN n.active_addresses_count > 0 
        THEN ROUND(CAST(n.transaction_count AS DOUBLE) / n.active_addresses_count, 4)
        ELSE NULL 
    END AS tx_per_active_address
FROM mart_btc_usd_daily m
FULL OUTER JOIN fact_network_metrics_daily n
    ON m.trade_date_utc = n.metric_date_utc;
```

## 5. Acceptance Criteria

- **AC-1**: `CoinMetricsClient` successfully retrieves and paginates daily Bitcoin network metrics from Community API v4 with rate limiting and retry handling.
- **AC-2**: `CoinMetricsContract` strictly validates presence of required fields, non-negative integer counts, and daily boundary alignment.
- **AC-3**: Raw responses are persisted in gzip JSON envelopes with SHA-256 checksums under `data/raw/coin_metrics/`.
- **AC-4**: Parquet writer creates annual partitions with explicit types under `curated/onchain/network_metrics_daily/source=coin_metrics/year=YYYY/`.
- **AC-5**: View `fact_network_metrics_daily` and cross-domain mart `mart_btc_market_and_network_daily` query cleanly in DuckDB with correct join logic and `is_market_day_complete` preservation.
- **AC-6**: Subcommands `fetch-network` and `promote-network` operate seamlessly via `bitcoin-data` CLI.
- **AC-7**: `docs/decisions/D-009_DBT_EVALUATION.md` provides an objective evaluation of `dbt-core` adoption.
- **AC-8**: All existing 255 tests pass without regression, and new tests pass (target > 285 tests).
- **AC-9**: Strict code quality gates pass cleanly (`ruff check`, `ruff format --check`, `mypy src`).
