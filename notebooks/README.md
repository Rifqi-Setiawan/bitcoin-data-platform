# Bitcoin Research Notebooks

This directory contains analytical research notebooks designed for reproducible exploration of Bitcoin market dynamics and on-chain network activity.

## Clean-Room Research Design Principles

1. **Decoupled Architecture**: Research notebooks act exclusively as consumers of the **Research Serving Layer** (`src/bitcoin_data_platform/serving/`) and conformed DuckDB analytical marts (`mart_btc_market_and_network_daily`).
2. **No Embedded Pipeline Logic**: Notebooks contain **zero ETL**, zero HTTP requests, and zero raw data manipulation. All data transformation, quality checks, and watermark advancement occur strictly within the core engineering pipeline.
3. **No Secrets or Credentials**: All analytical data is served from local curated Parquet and DuckDB databases. Notebooks require no API keys, bearer tokens, or external credentials.
4. **Reproducibility**: Notebooks run deterministically in a clean Python 3.12 virtual environment without hidden cell dependencies or stateful side effects.

---

## Environment Setup

### 1. Activate the Platform Virtual Environment

```bash
# From repository root
source .venv/bin/activate
```

### 2. Ensure Analytical Marts Are Populated

Before executing research queries, ensure the DuckDB database has been populated with curated data:

```bash
# Ingest and promote market data
bitcoin-data backfill --start 2026-01-01T00:00:00Z --end 2026-01-31T00:00:00Z --output-dir ./data/raw
bitcoin-data promote --raw-dir ./data/raw --curated-dir ./data/curated --db-path ./data/state/platform.duckdb

# Fetch and promote on-chain network metrics
bitcoin-data fetch-network --start 2026-01-01 --end 2026-01-31 --output-dir ./data/raw/coin_metrics
bitcoin-data promote-network --raw-dir ./data/raw/coin_metrics --curated-dir ./data/curated --db-path ./data/state/platform.duckdb
```

### 3. Launching Jupyter

```bash
jupyter notebook notebooks/bitcoin_research_baseline.ipynb
# or
jupyter lab
```

---

## Notebook Catalog

| Notebook | Description | Data Sources |
| :--- | :--- | :--- |
| `bitcoin_research_baseline.ipynb` | Baseline exploratory analysis examining price-volume behavior, network transaction throughput, active address participation, and empirical correlation between trading volume and network velocity. | `mart_btc_market_and_network_daily` |
