# Phase 12: Investment Data Expansion — On-Chain Valuation, Market Sentiment & Macro Calendar

**Owner:** Engineering Team  
**Verification:** Automated Test Suite & Peer Review  
**Branch:** `feature/P12-investment-data-expansion`  
**Status:** Draft  

---

## Objective

Expand the Bitcoin Data Platform's ingestion pipeline with three new lightweight data sources required for autonomous investment signal generation: (1) MVRV ratio from Coin Metrics, (2) Crypto Fear & Greed Index from Alternative.me, and (3) scheduled high-impact US macro economic events from ForexFactory. All sources are 100% free, require zero API keys, and add negligible compute overhead.

## User Story

As an autonomous investment system operator, I need on-chain valuation metrics (MVRV), market sentiment indicators (Fear & Greed), and macro event awareness (FOMC/CPI calendar) so that the platform can generate evidence-based investment allocation signals rather than blind DCA.

---

## Scope

### In Scope
1. Extend Coin Metrics ingestion to include `CapMVRVCur` alongside existing `TxCnt,AdrActCnt`
2. New `sentiment` ingestion module — Fear & Greed Index client, contract validator, DuckDB table
3. New `macro` ingestion module — ForexFactory calendar client, contract validator, DuckDB table
4. CLI subcommands: `fetch-sentiment`, `fetch-macro-calendar`
5. DuckDB analytical view `mart_btc_investment_signals_daily` combining OHLCV + on-chain + MVRV + sentiment + macro events
6. Automated tests for all new components
7. Update Data Dictionary

### Out of Scope
- Investment signal generation logic / backtesting (Phase 13)
- News sentinel RSS alerting (Phase 13)
- Coinbase execution / order placement (Phase 16)
- NLP/ML sentiment analysis

---

## Data Source Contracts

### Source 1: Coin Metrics — CapMVRVCur (Extension of Existing Pipeline)

**Endpoint:** `GET https://community-api.coinmetrics.io/v4/timeseries/asset-metrics`  
**Change:** Update `DEFAULT_METRICS` from `"TxCnt,AdrActCnt"` to `"TxCnt,AdrActCnt,CapMVRVCur"`  

**Response shape** (verified live):
```json
{"asset": "btc", "time": "2026-09-17T00:00:00.000000000Z", "TxCnt": "345612", "AdrActCnt": "890140", "CapMVRVCur": "1.435849433216295172"}
```

**Contract:** `CapMVRVCur` is a string-encoded `Decimal` value, always positive, typically between 0.3 and 7.0.

### Source 2: Alternative.me — Crypto Fear & Greed Index

**Endpoint:** `GET https://api.alternative.me/fng/?limit=1&format=json`  
**Auth:** None (public, no API key)  
**Rate limit:** Generous (no documented limit; use 1 req/min minimum spacing)  

**Response shape** (verified live):
```json
{
  "name": "Fear and Greed Index",
  "data": [{"value": "56", "value_classification": "Greed", "timestamp": "1789689600", "time_until_update": "54785"}],
  "metadata": {"error": null}
}
```

**Contract:**
- `data[0].value`: string-encoded integer 0–100
- `data[0].value_classification`: one of `"Extreme Fear"`, `"Fear"`, `"Neutral"`, `"Greed"`, `"Extreme Greed"`
- `data[0].timestamp`: Unix epoch seconds (string)

### Source 3: ForexFactory — US Macro Economic Calendar

**Endpoint:** `GET https://nfs.faireconomy.media/ff_calendar_thisweek.json`  
**Auth:** None (public, no API key)  
**Rate limit:** Generous; fetch weekly

**Response shape** (verified live):
```json
{"title": "FOMC Statement", "country": "USD", "date": "2026-09-18T14:00:00-04:00", "impact": "High", "forecast": "", "previous": ""}
```

**Contract:**
- Array of event objects
- Filter: `country == "USD"` AND `impact == "High"`
- Relevant events: `FOMC Statement`, `Federal Funds Rate`, `CPI m/m`, `Core CPI m/m`, `Non-Farm Employment Change`

---

## Module Layout

### NEW Files
| File | Purpose |
|------|---------|
| `src/bitcoin_data_platform/sources/sentiment_client.py` | Fear & Greed API client with retry, rate limit, contract validation |
| `src/bitcoin_data_platform/sources/sentiment_contract.py` | Data contract, response model, validator for FNG |
| `src/bitcoin_data_platform/sources/macro_calendar_client.py` | ForexFactory calendar client with retry, filtering |
| `src/bitcoin_data_platform/sources/macro_calendar_contract.py` | Data contract, response model, validator for macro events |
| `tests/test_sentiment_client.py` | Unit tests for sentiment client |
| `tests/test_macro_calendar.py` | Unit tests for macro calendar client |
| `tests/test_investment_signals.py` | Integration tests for combined analytical view |

### MODIFIED Files (Minimal, Surgical Changes)
| File | Change |
|------|--------|
| `src/bitcoin_data_platform/sources/coin_metrics_client.py` | `DEFAULT_METRICS = "TxCnt,AdrActCnt,CapMVRVCur"` |
| `src/bitcoin_data_platform/sources/coin_metrics_contract.py` | Add `mvrv_ratio: Decimal` field to `CoinMetricsRecord`, update validator to parse `CapMVRVCur` |
| `src/bitcoin_data_platform/transforms/network_normalizer.py` | Add `mvrv_ratio: Decimal` to `NormalizedNetworkMetric` |
| `src/bitcoin_data_platform/storage/network_parquet_writer.py` | Add `mvrv_ratio` column to Parquet schema |
| `src/bitcoin_data_platform/storage/duckdb_manager.py` | Add `raw_crypto_sentiment_daily` and `raw_macro_economic_events` tables; add `mart_btc_investment_signals_daily` view |
| `src/bitcoin_data_platform/cli.py` | Add `fetch-sentiment` and `fetch-macro-calendar` subcommands |
| `docs/data_dictionary/DATA_DICTIONARY.md` | Document new tables and view |

### UNCHANGED Files (Preserve — Do NOT Modify)
All Phase 1–11 and Dashboard files not listed above.

---

## Functional Contracts

### 1. SentimentClient

```python
@dataclass(frozen=True)
class SentimentRecord:
    date_utc: datetime  # from Unix timestamp, aligned to date
    value: int          # 0–100
    classification: str # "Extreme Fear" | "Fear" | "Neutral" | "Greed" | "Extreme Greed"
    ingested_at_utc: datetime

class SentimentClient:
    def __init__(self, *, base_url: str = "https://api.alternative.me/fng",
                 max_retries: int = 3, transport=None, sleeper=None, clock=None): ...
    def fetch_current(self) -> SentimentRecord: ...
    def fetch_history(self, limit: int = 30) -> list[SentimentRecord]: ...
```

### 2. MacroCalendarClient

```python
@dataclass(frozen=True)
class MacroEvent:
    event_id: str       # SHA-256(title + date) for idempotent dedup
    country: str        # "USD"
    title: str          # "FOMC Statement"
    impact: str         # "High"
    scheduled_utc: datetime
    forecast: str | None
    previous: str | None
    ingested_at_utc: datetime

class MacroCalendarClient:
    def __init__(self, *, feed_url: str = "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                 max_retries: int = 3, transport=None, sleeper=None, clock=None): ...
    def fetch_week_events(self, *, country_filter: str = "USD",
                          impact_filter: str = "High") -> list[MacroEvent]: ...
```

### 3. DuckDB View: `mart_btc_investment_signals_daily`

```sql
CREATE OR REPLACE VIEW mart_btc_investment_signals_daily AS
SELECT
    m.trade_date_utc,
    m.market_close_usd,
    AVG(m.market_close_usd) OVER (ORDER BY m.trade_date_utc ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma_200,
    m.market_close_usd / NULLIF(AVG(m.market_close_usd) OVER (...), 0) AS mayer_multiple,
    n.mvrv_ratio,
    COALESCE(s.fng_value, 50) AS fng_value,
    COALESCE(s.fng_classification, 'Neutral') AS fng_classification,
    CASE WHEN e.event_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_high_impact_macro_event,
    -- Investment allocation signal
    CASE
        WHEN mayer_multiple < 0.80 AND COALESCE(s.fng_value, 50) <= 25 THEN 'AGGRESSIVE_ACCUMULATE'
        WHEN mayer_multiple < 1.00 OR n.mvrv_ratio < 1.20 THEN 'OPPORTUNISTIC_ACCUMULATE'
        WHEN mayer_multiple BETWEEN 1.00 AND 1.80 THEN 'STANDARD_DCA'
        WHEN mayer_multiple > 1.80 OR COALESCE(s.fng_value, 50) >= 85 THEN 'DEFENSIVE_RESERVE'
        WHEN mayer_multiple > 2.40 OR n.mvrv_ratio > 3.50 THEN 'HARD_FREEZE'
    END AS investment_signal
FROM mart_btc_usd_daily m
LEFT JOIN fact_network_metrics_daily n ON m.trade_date_utc = n.metric_date_utc
LEFT JOIN raw_crypto_sentiment_daily s ON m.trade_date_utc = s.sentiment_date_utc
LEFT JOIN (SELECT DISTINCT CAST(scheduled_utc AS DATE) AS event_date, event_id
           FROM raw_macro_economic_events WHERE impact = 'High' AND country = 'USD') e
    ON m.trade_date_utc = e.event_date;
```

---

## Required Test Cases

### Group A: Sentiment Client (test_sentiment_client.py)
1. `test_fetch_current_success` — parse valid FNG response
2. `test_fetch_current_retries_on_transient_error` — retry on 500/503
3. `test_fetch_current_fails_after_max_retries` — raises SourceUnavailableError
4. `test_fetch_history_returns_multiple_records` — limit=7 returns 7 records
5. `test_contract_validation_rejects_invalid_value` — value > 100 or < 0
6. `test_contract_validation_rejects_missing_fields` — raises ContractViolationError
7. `test_rate_limiting_enforced` — minimum spacing between requests

### Group B: Macro Calendar Client (test_macro_calendar.py)
8. `test_fetch_week_events_filters_usd_high_impact` — only USD + High events
9. `test_fetch_week_events_handles_empty_week` — returns empty list
10. `test_dedup_by_event_id` — same event_id not duplicated
11. `test_retries_on_transient_error` — retry on network failures
12. `test_contract_validation_rejects_malformed_event` — missing required fields
13. `test_timezone_parsing_to_utc` — ForexFactory EDT/EST → UTC conversion

### Group C: MVRV Extension (integration with existing tests)
14. `test_coin_metrics_record_includes_mvrv` — CoinMetricsRecord.mvrv_ratio parsed
15. `test_normalized_network_metric_includes_mvrv` — NormalizedNetworkMetric.mvrv_ratio
16. `test_parquet_schema_includes_mvrv_column` — Parquet output has mvrv_ratio

### Group D: Investment Signals View (test_investment_signals.py)
17. `test_mart_view_computes_sma_200` — window function correct
18. `test_mart_view_computes_mayer_multiple` — price / SMA_200
19. `test_mart_view_joins_sentiment` — FNG value present
20. `test_mart_view_joins_macro_events` — has_high_impact_macro_event flag
21. `test_investment_signal_classification` — correct signal per band
22. `test_fallback_when_no_sentiment_data` — defaults to Neutral/50

---

## Acceptance Criteria

- **AC-1:** `CapMVRVCur` successfully fetched, normalized, and written to Parquet alongside existing on-chain metrics
- **AC-2:** `bitcoin-data fetch-sentiment` fetches current FNG index and persists to DuckDB `raw_crypto_sentiment_daily`
- **AC-3:** `bitcoin-data fetch-macro-calendar` fetches this week's USD high-impact events and persists to DuckDB `raw_macro_economic_events`
- **AC-4:** DuckDB view `mart_btc_investment_signals_daily` returns correct Mayer Multiple, MVRV, FNG, macro event flag, and investment signal classification
- **AC-5:** All existing 480 tests pass without regression
- **AC-6:** All quality gates pass: `ruff check`, `ruff format --check`, `mypy src`
- **AC-7:** Data Dictionary updated with new tables and view

---

## Implementation Sequence

1. Extend `coin_metrics_contract.py` → add `mvrv_ratio` to `CoinMetricsRecord`
2. Update `DEFAULT_METRICS` in `coin_metrics_client.py`
3. Extend `NormalizedNetworkMetric` in `network_normalizer.py`
4. Extend Parquet schema in `network_parquet_writer.py`
5. Build `sentiment_contract.py` → `sentiment_client.py`
6. Build `macro_calendar_contract.py` → `macro_calendar_client.py`
7. Extend `duckdb_manager.py` → new tables + `mart_btc_investment_signals_daily` view
8. Add CLI subcommands `fetch-sentiment` and `fetch-macro-calendar`
9. Write all tests
10. Update Data Dictionary
11. Run full test suite + quality gates

---

## Design Boundaries

- **Decimal** for MVRV ratio (never float)
- **Dependency injection** for HTTP transport, clock, sleeper (testability)
- **httpx** for HTTP client (consistent with existing clients)
- **No new dependencies** — httpx already in pyproject.toml
- **Atomic DuckDB writes** via transactions
- **Idempotent ingestion** — sentiment deduped by date, macro events by event_id
- **Zero API keys** — all three sources are fully public
