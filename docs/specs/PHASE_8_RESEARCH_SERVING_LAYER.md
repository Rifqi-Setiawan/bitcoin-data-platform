# Phase 8 Implementation Specification — Research Serving Layer (Reproducible Notebooks & Parameterized SQL Queries)

Status: approved for implementation
Parent phase: Phase 8 — Research serving layer
Task ID: `P8-research-serving-layer`
Recommended branch: `feature/P8-research-serving-layer`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Establish an ergonomic, reproducible, and decoupled research serving layer for the Bitcoin Data Platform.
Provide a parameterized query service and multi-format exporter (`parquet`, `arrow`, `csv`, `json`), a curated
catalog of versioned analytical SQL queries in `queries/`, a formal Data Dictionary in `docs/data_dictionary/`,
and an executable reference research notebook in `notebooks/` that analyzes the interplay between Bitcoin market
dynamics and on-chain network activity without embedding ETL or transformation logic inside notebook cells.

## 2. User Stories

### Multi-Format Parameterized Export
As a quantitative researcher, I can execute:
```bash
bitcoin-data query \
  --file queries/cross_domain_market_network.sql \
  --param start_date=2026-01-01 \
  --param end_date=2026-01-31 \
  --format parquet \
  --output ./data/exports/btc_market_network_jan2026.parquet
```
to run a version-controlled SQL model with type-safe parameter substitution and export the results directly
to columnar Parquet, Arrow, CSV, or JSON for downstream modeling.

### Formal Data Dictionary Reference
As a data consumer, I can inspect `docs/data_dictionary/DATA_DICTIONARY.md` to see exact semantic definitions,
underlying grains, primary keys, nullability, unit measurements, and upstream provenance for every fact view
and analytical mart across market and on-chain domains.

### Clean-Room Notebook Reproducibility
As a peer reviewer, I can open `notebooks/bitcoin_research_baseline.ipynb` in a fresh environment, run all cells
sequentially without manual setup or embedded pipeline code, and reproduce the analytical charts and statistical
summary directly querying the DuckDB analytical marts.

## 3. Scope

### In Scope
1. **Serving & Export Engine (`src/bitcoin_data_platform/serving/`)**:
   - `query_service.py`: Parameterized SQL execution supporting parameter substitution (`:param_name`), query file loading, and type-safe query execution on DuckDB.
   - `exporter.py`: Multi-format serialization (`json`, `csv`, `parquet`, `arrow`) with atomic file writes and schema preservation.
2. **Version-Controlled Query Catalog (`queries/`)**:
   - `queries/daily_market_summary.sql`: Daily OHLCV summary, rolling volatility, and daily return.
   - `queries/onchain_network_activity.sql`: Daily transaction counts, active addresses, and network velocity.
   - `queries/cross_domain_market_network.sql`: Conformed cross-domain query joining market and on-chain metrics with completeness filtering (`is_market_day_complete = true`).
3. **Formal Data Dictionary (`docs/data_dictionary/DATA_DICTIONARY.md`)**:
   - Exhaustive documentation of `fact_market_candle_hourly`, `mart_btc_usd_daily`, `fact_network_metrics_daily`, and `mart_btc_market_and_network_daily`.
4. **Reproducible Research Notebook (`notebooks/`)**:
   - `notebooks/README.md`: Instructions for environment setup and execution.
   - `notebooks/bitcoin_research_baseline.ipynb`: Clean Jupyter notebook consuming data via the platform API/DuckDB, presenting market-network correlation and visual plots with zero embedded ETL code.
5. **CLI Query Enhancements (`src/bitcoin_data_platform/cli.py`)**:
   - Support `--file <path>`, `--param KEY=VAL` (multi-value), `--format {json,csv,parquet,arrow}`, and `--output <path>`.
6. **Automated Testing**:
   - `tests/test_serving_exporter.py`: Testing export across all 4 formats, atomic writes, and parameter binding.
   - `tests/test_query_catalog.py`: Verifying syntax and execution of all SQL catalog files.
   - `tests/test_notebook_reproducibility.py`: Validating notebook structure, cell execution cleanliness, and absence of hardcoded secrets or embedded ETL.
   - Zero regressions across existing 292 tests (target > 315 tests).

### Out of Scope
- Public HTTP REST API daemon or web server (preserves zero inbound listener security boundary).
- Complex machine learning forecasting models (scope is analytical research serving and reproducibility).

## 4. Technical Specifications

### 4.1 Parameterized Query Service (`src/bitcoin_data_platform/serving/query_service.py`)
- Safely resolves parameters in SQL (e.g. `:start_date`, `:end_date`, `:asset`).
- Integrates with `DuckDBManager` to execute queries against managed views.
- Validates parameter names and types to prevent injection.

### 4.2 Multi-Format Exporter (`src/bitcoin_data_platform/serving/exporter.py`)
- `export_data(arrow_table: pa.Table, output_format: str, output_path: Path | None = None) -> bytes | Path`
- Formats supported:
  - `parquet`: pyarrow.parquet.write_table with snappy/zstd compression.
  - `arrow`: pyarrow.ipc.RecordBatchFileWriter format.
  - `csv`: pyarrow.csv.write_csv with RFC 4180 compliance.
  - `json`: Standard JSON serialization.
- Atomic file write: `.tmp` file followed by `os.replace`.

### 4.3 CLI Interface Extension
```bash
bitcoin-data query \
  [--sql <SQL_STRING> | --file <SQL_FILE_PATH>] \
  [--param KEY=VALUE ...] \
  [--format {json,csv,parquet,arrow}] \
  [--output <OUTPUT_FILE_PATH>] \
  [--db-path <DB_PATH>]
```

## 5. Acceptance Criteria

- **AC-1**: `bitcoin-data query` supports `--file`, `--param KEY=VAL`, `--format`, and `--output` seamlessly.
- **AC-2**: Data exports to `parquet`, `arrow`, `csv`, and `json` correctly preserve data types and write atomically.
- **AC-3**: All catalog queries in `queries/*.sql` parse and execute cleanly against DuckDB.
- **AC-4**: `docs/data_dictionary/DATA_DICTIONARY.md` formally documents all fact views and analytical marts.
- **AC-5**: `notebooks/bitcoin_research_baseline.ipynb` exists, is valid JSON, contains no embedded ETL/credentials, and runs reproducibly.
- **AC-6**: Automated test suite validates query service, exporter, query files, and notebook structure with > 315 tests passing.
- **AC-7**: Quality gates pass cleanly: `pytest`, `ruff check .`, `ruff format --check .`, `mypy src`.
- **AC-8**: Clean commit with zero AI/Hermes references.
