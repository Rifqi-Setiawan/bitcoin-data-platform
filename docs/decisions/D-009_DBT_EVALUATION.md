# ADR D-009: Evaluation of dbt-core vs Native DuckDB SQL Transformations

- **Status:** Accepted
- **Date:** 2026-09-18
- **Author:** Engineering Team
- **Deciders:** Architecture & Infrastructure Working Group
- **Technical Story:** Phase 7 — Second Domain & Conformed Modeling

---

## 1. Context and Problem Statement

With the implementation of Phase 7, the Bitcoin Data Platform integrates a second heterogeneous data domain: on-chain daily network activity from the Coin Metrics Community API v4 (`TxCnt` and `AdrActCnt`), joining with Coinbase Exchange hourly market trade candles. To reconcile the grain mismatch (hourly market trading vs. daily on-chain activity), the platform introduces conformed dimensional modeling in DuckDB:
- `fact_market_candle_hourly` (base hourly market facts over Parquet)
- `mart_btc_usd_daily` (daily market rollup with completeness indicator)
- `fact_network_metrics_daily` (base daily on-chain network facts over Parquet)
- `mart_btc_market_and_network_daily` (conformed cross-domain analytical mart joined on UTC date)

As designated in the project roadmap (`ROADMAP.md` Phase 7), the addition of a second domain mandates a formal architectural evaluation: **Should the platform introduce `dbt-core` (paired with `dbt-duckdb`) to orchestrate SQL transformations, lineage, and data modeling, or should the platform retain native, managed DuckDB SQL view orchestration embedded within the Python runtime?**

---

## 2. Decision Drivers

1. **Dependency Footprint & Supply Chain Surface:** Minimize extraneous third-party dependencies and build-time compilation overhead on the locked single-host VPS runtime.
2. **Execution Latency & Runtime Performance:** Preserve sub-second execution speeds for hourly incremental batch runs and CLI queries without paying CLI invocation and manifest compilation penalties.
3. **Operational Simplicity:** Avoid dual orchestration paradigms (systemd + Python CLI vs. dbt CLI wrappers); preserve single-binary execution ergonomics.
4. **Data Quality & Contract Verification:** Ensure that data contracts, schema validations, and anomaly checks remain blocking, synchronous gates directly tied to watermark advancement.
5. **DAG Scale & Complexity:** Match the engineering solution to the actual graph complexity of the platform (currently 4 models across 2 domains).
6. **Reproducibility & Zero Host Bloat:** Maintain deterministic package installation and clean container packaging without large toolchain dependencies.

---

## 3. Considered Alternatives

### Alternative 1: Full Adoption of dbt-core + dbt-duckdb
Incorporate `dbt-core` and `dbt-duckdb` into the platform dependencies. Modularize transformation logic into standard dbt models (`models/staging/`, `models/marts/`), configure `dbt_project.yml`, use Jinja templating, and invoke `dbt run` and `dbt test` within the promotion lifecycle.

### Alternative 2: Managed Native DuckDB SQL Views (Selected Approach)
Define canonical transformation and conformed mart SQL directly within `DuckDBManager` as declarative view definitions backed by Hive-partitioned Parquet files. Views are idempotently created or refreshed during `initialize()` and promotion commands, accompanied by typed Python contracts and Phase 5 DuckDB quality check tables.

### Alternative 3: Hybrid SQL Template Preprocessor (Jinja2 + Native DuckDB)
Maintain standalone `.sql` files in the repository rendered via lightweight `jinja2` and executed directly by `DuckDBPyConnection`, without the full `dbt-core` compilation framework.

---

## 4. Evaluation & Comparison Matrix

| Evaluation Dimension | Alternative 1: dbt-core + dbt-duckdb | Alternative 2: Native DuckDB Views (Selected) | Alternative 3: Hybrid Jinja2 + DuckDB |
| :--- | :--- | :--- | :--- |
| **Transitive Dependencies** | **Heavy.** Adds 45+ packages (mashumaro, agate, hologram, networkx, dbt-extractor, etc.), increasing package surface by ~120 MB. | **Zero additional dependencies.** Uses existing `duckdb` and `pyarrow` already present in the virtualenv. | **Minimal.** Adds only `jinja2` and `markupsafe` (~3 MB). |
| **Execution Latency** | **High invocation penalty.** Cold-start manifest compilation and Python packaging parsing adds 1.8–3.5s per run. | **Sub-millisecond (< 5 ms).** Direct embedded C++ DuckDB connection execution; instantaneous view registration. | **Low (< 20 ms).** Fast string templating followed by native DuckDB execution. |
| **Orchestration Ergonomics** | **Fragmented.** Requires wrapping `dbt run` CLI subprocesses within Python CLI, managing dbt exit codes, and coordinating logs. | **Unified.** Native Python methods within `DuckDBManager`; atomic transactions and consistent exception hierarchy. | **Unified.** Managed in Python codebase, but requires filesystem file loading and parsing. |
| **Data Quality Integration** | **Decoupled.** dbt tests run after compilation; integrates awkwardly with custom Phase 5 `quality_check_results` table and watermarks. | **Direct & Synchronous.** Quality rules and assertions execute inside transaction prior to monotonic watermark advancement. | **Custom.** Requires manual implementation of test runner. |
| **DAG & Model Scalability** | **Exceptional for large DAGs (50+ models).** Automated topological sort, documentation generator, and column-level lineage. | **Sufficient for targeted DAGs (< 15 models).** Explicit view dependencies declared in code; zero compilation complexity. | **Moderate.** Dependency resolution must be maintained manually or via custom graph solver. |
| **Local Portability & Testing** | **Requires dbt environment.** Tests require dbt profile setup and isolated dbt targets. | **Effortless.** Tests use in-memory DuckDB (`:memory:`) with identical SQL views; instantaneous fixture execution. | **Effortless.** In-memory testing supported. |

---

## 5. Decision Outcome

**We choose Alternative 2: Managed Native DuckDB SQL Views.**

### Rationale

1. **Disproportionate Complexity of dbt at Current Scale:**
   The platform's conformed dimensional graph consists of four models:
   ```
   [raw Coinbase] ---> Parquet ---> fact_market_candle_hourly ---> mart_btc_usd_daily -------+
                                                                                              |---> mart_btc_market_and_network_daily
   [raw Coin Metrics] -> Parquet -> fact_network_metrics_daily ------------------------------+
   ```
   Introducing `dbt-core` and 45+ transitive dependencies to manage a 4-node acyclic graph introduces severe accidental complexity, increases virtualenv install time by 400%, and inflates container images without delivering tangible modeling advantages.

2. **Single-Host Latency Invariant:**
   The Bitcoin Data Platform executes hourly batch runs via systemd oneshot services where execution latency and resource conservation are paramount. Adding 2 to 3 seconds of dbt manifest parsing and Python packaging inspection to an ingestion and promotion cycle that executes in under 200 ms violates our performance invariants.

3. **Tight Integration with Monotonic Watermarks & Quality Gates:**
   Watermark advancement is strictly conditional: a watermark advances if and only if 100% of raw envelopes are normalized, written to Parquet, and pass dataset-level quality invariants without anomalies. In DuckDBManager, view recreation and watermark updating occur deterministically within the same process boundary.

4. **Preservation of Parquet Storage Decoupling:**
   DuckDB views point directly to immutable, Hive-partitioned Parquet files on disk (`read_parquet('curated/.../*.parquet', hive_partitioning=true)`). The data storage tier remains completely independent of DuckDB; DuckDB acts purely as an embedded query and compute engine. Native views preserve this zero-overhead separation cleanly.

---

## 6. Consequences

### Positive Consequences
- **Zero New Dependencies:** The project's `pyproject.toml` and `requirements.lock` remain lightweight, fast to install, and secure against supply-chain vulnerabilities.
- **Microsecond In-Memory Testing:** Automated unit and integration tests spin up DuckDB in `:memory:` mode, create the entire schema, execute cross-domain joins, and assert results in under 5 milliseconds.
- **Single Operational Entrypoint:** Pipeline engineers and automated systemd units interact solely through the `bitcoin-data` CLI without requiring `dbt` wrapper scripts or environment profiles.
- **Graceful Fallbacks:** Native views implement typed fallback schemas (`WHERE 1=0`) ensuring downstream mart views can be initialized and queried even before data partitions are populated.

### Negative Consequences / Trade-offs
- **No Out-of-the-Box Interactive Lineage UI:** The platform does not generate dbt-style HTML lineage graphs automatically. Lineage must be documented in architecture specifications and ADRs.
- **Manual Dependency Ordering:** When adding new views, engineers must declare dependent base facts prior to aggregate marts in `DuckDBManager.initialize()`. At our current scale (4 views), this order is trivial and verified by automated unit tests.

---

## 7. Re-evaluation Triggers

This decision will be formally revisited when any of the following triggers occur:
1. **Model Graph Growth:** The number of analytical marts and intermediate transformation models exceeds 15 distinct interdependent nodes.
2. **Dedicated Analytics Engineering Team:** Organization expands to include analytics engineers whose primary workflow is centered around standalone SQL modeling and dbt package ecosystems.
3. **Data Warehouse Migration:** The platform migrates from embedded single-host DuckDB to an external enterprise analytical warehouse (e.g., Snowflake, BigQuery, ClickHouse cluster) requiring distributed model compilation.
4. **Semantic Layer Requirements:** Downstream business intelligence consumers mandate integration with MetricFlow, Cube, or other semantic layer platforms that require dbt semantic manifests.
