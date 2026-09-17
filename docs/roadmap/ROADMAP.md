# Progressive Roadmap

Each phase solves the next observed problem. A phase is complete only when its outputs are tested, verified, documented, and committed. Dates are deliberately omitted; readiness, not calendar pressure, controls progression.

## Phase 0 — Architecture and repository foundation

- **Objective:** Establish scope, source, grain, security boundaries, repository design, and decision process.
- **Prerequisites:** Same-day infrastructure evidence and known owner goals.
- **Components:** Master plan, V1 architecture, roadmap, concept map, decision log, proposed repository paths.
- **Concepts:** Architecture boundaries, source-of-truth, non-functional requirements, trade-offs, RPO/RTO thinking.
- **Expected output:** Reviewable documentation with one next implementation slice.
- **Tests/review:** Link checks, Mermaid parse/render review, secret scan, consistency review across docs.
- **Portfolio value:** Shows intentional system design rather than a pile of tools.
- **Definition of done:** Required documents exist; assumptions and audit limitation are explicit; no infrastructure changed.
- **Interview outcome:** Explain why the project starts with batch, why a single-host design is sufficient, and how constraints drive architecture.

## Phase 1 — Repository bootstrap and deterministic raw ingestion

- **Objective:** Fetch explicit historical hourly ranges and preserve source evidence safely.
- **Prerequisites:** Phase 0 review; live VPS paths revalidated before any deployment (development remains local first).
- **Components:** Git repository, `pyproject.toml`, locked dependencies, typed config, structured logging, Coinbase client, window planner, raw envelope writer, source contract, sanitized fixture.
- **Concepts:** API contracts, pagination/windowing, timeouts, retries, rate limits, idempotency vs immutability, UTC boundaries, checksums.
- **Expected output:** `backfill --start ... --end ...` writes gzip raw envelopes and a run summary.
- **Tests:** Window boundaries; 300-candle maximum; 429/5xx/timeout retry; permanent 4xx; checksum; atomic rename; malformed tuple fixture; secret-free logs.
- **Portfolio value:** Demonstrates production-minded API ingestion rather than a one-off script.
- **Definition of done:** A bounded date range succeeds from a clean environment; rerun is safe; failed responses do not masquerade as data; README documents exact commands and failure behavior.
- **Interview outcome:** Explain retryability, exponential backoff, rate limiting, source contracts, half-open intervals, and why raw data is immutable.

## Phase 2 — Curated Parquet and analytical modeling

- **Objective:** Turn raw source responses into reproducible, typed analytical tables.
- **Prerequisites:** Phase 1 raw envelopes and contract tests.
- **Components:** Normalizer, annual Parquet partitions, DuckDB views/control tables, hourly fact, daily mart, SQL quality assertions.
- **Concepts:** Columnar storage, schema, grain, natural keys, deduplication, partitions, OLAP, facts vs dimensions, deterministic transforms.
- **Expected output:** Queryable `fact_market_candle_hourly` and `mart_btc_usd_daily` built entirely from raw.
- **Tests:** Types, keys, nulls, OHLC invariants, volume range, complete/partial day aggregation, deterministic rebuild hash/logical equality, partition replacement failure.
- **Portfolio value:** Shows the raw-to-curated lifecycle and analytical SQL.
- **Definition of done:** Full rebuild and overlapping rebuild converge; DuckDB example queries work; data dictionary and lineage are documented.
- **Interview outcome:** Explain Parquet vs CSV, DuckDB vs PostgreSQL, table grain, partitioning trade-offs, and idempotent transforms.

## Phase 3 — Incremental loads and recovery

- **Objective:** Update only recent completed candles without losing corrections or duplicating rows.
- **Prerequisites:** Stable Phase 2 promotion path.
- **Components:** Watermark table, 48-hour overlap policy, run lock, transactional metadata update, repair/reconcile command, bounded backfill safety controls.
- **Concepts:** Incremental loading, watermark semantics, late-arriving/corrected data, upsert, atomic promotion, failure recovery.
- **Expected output:** `incremental` and `repair` commands with an operational runbook.
- **Tests:** No-watermark behavior; overlapping rerun; concurrent invocation; crash before/after partition swap; monotonic watermark; source revision fixture.
- **Portfolio value:** Demonstrates reliability beyond “script ran once.”
- **Definition of done:** Repeated incremental runs converge; failure never advances the watermark; documented recovery succeeds in a test copy.
- **Interview outcome:** Explain why a watermark is not simply the last request time, how overlaps handle corrections, and where exactly-once claims break down.

## Phase 4 — Single-host orchestration

- **Objective:** Operate the proven incremental pipeline reliably on the VPS.
- **Prerequisites:** Fresh live audit; Phase 3 recovery verified; reviewed deployment plan.
- **Components:** Dedicated Unix account, filesystem ownership, systemd oneshot service/timer, resource limits, environment/config path, deployment and rollback runbook.
- **Concepts:** Orchestration, scheduling, service identity, least privilege, missed schedules, process supervision.
- **Expected output:** Hourly run after candle closure with journald records and no new inbound listener.
- **Tests:** Unit security review, manual start, timer trigger, reboot/missed-run behavior, overlap prevention, failure status, rollback.
- **Portfolio value:** Shows real deployment and operations without fake enterprise complexity.
- **Definition of done:** Three days of successful scheduled runs or equivalent accelerated validation; one injected failure and recovery demonstrated; health check updated.
- **Interview outcome:** Explain cron vs systemd timers vs workflow orchestrators and how service users/permissions reduce blast radius.

## Phase 5 — Data quality and observability hardening

- **Objective:** Detect stale, incomplete, anomalous, or failing data quickly and make diagnosis repeatable.
- **Prerequisites:** Scheduled run history.
- **Components:** Status CLI, run/quality tables, freshness and gap rules, row-count baseline, disk threshold, low-noise `OnFailure` notification, quality runbook.
- **Concepts:** Data observability, freshness, completeness, validity, reconciliation, SLIs/SLOs, alert fatigue.
- **Expected output:** One command answers “is the pipeline healthy and is the data current?”
- **Tests:** Stale data, duplicate, gap, invalid range, disk-warning, notification deduplication, scrubbed errors.
- **Portfolio value:** Demonstrates ownership of data reliability, not just movement.
- **Definition of done:** Synthetic failures create correct states/alerts; healthy runs are quiet; troubleshooting guide resolves each injected incident.
- **Interview outcome:** Distinguish infrastructure monitoring from data observability and explain how to choose blocking vs warning checks.

## Phase 6 — Reproducible delivery and CI/CD

- **Objective:** Make clean-clone development, review, release, and deployment predictable.
- **Prerequisites:** Stable CLI/filesystem contract.
- **Components:** GitHub Actions, format/lint/type/test jobs, dependency/security scan, artifact/version strategy, optional OCI image and Compose/systemd comparison, deployment checklist.
- **Concepts:** CI vs CD, immutable artifacts, dependency locking, environment parity, rollback, supply-chain hygiene.
- **Expected output:** A tagged version can be tested and deployed without editing code on the server.
- **Tests:** Clean clone, offline fixture suite, image/non-image parity if container retained, rollback rehearsal, secret scan.
- **Portfolio value:** Public engineering workflow and meaningful commit/release history.
- **Definition of done:** Protected checks pass; release notes and rollback are documented; container is kept only if it improves reproducibility enough to justify it.
- **Interview outcome:** Explain what belongs in CI, why live API tests are not gatekeepers, and how deployment differs from building an image.

## Phase 7 — Second domain and analytics engineering

- **Objective:** Add one research-relevant domain and expose cross-domain modeling problems.
- **Prerequisites:** Stable first-domain operations and a concrete research question.
- **Recommended choice:** Coin Metrics daily Bitcoin network metrics such as transaction count, active addresses, fees, or hash rate—after confirming current Community coverage/licensing for each metric.
- **Components:** Second source adapter/contract, conformed dates/UTC, source-specific raw layer, cross-domain marts; evaluate dbt-core now.
- **Concepts:** Heterogeneous schemas, conformed dimensions, multiple frequencies, data contracts, lineage, revisions, semantic definitions.
- **Expected output:** A documented query relating market behavior to one defensible network metric without claiming causation.
- **Tests:** Source-specific contract, date/frequency alignment, missing-day policy, cross-source freshness, dbt model/tests if adopted.
- **Portfolio value:** Shows extensible architecture and multi-source analytics.
- **Definition of done:** New domain does not couple source logic to Coinbase; source terms/definitions are documented; independent failures are visible.
- **Interview outcome:** Explain conformed dimensions, source-specific vs canonical schemas, metric semantics, and why correlation is not causation.

## Phase 8 — Research serving layer

- **Objective:** Make trusted models usable without duplicating transformation logic.
- **Prerequisites:** At least two stable analytical models and real research questions.
- **Components:** Reproducible notebooks and/or a small dashboard, parameterized SQL, data dictionary, example questions, cached/exported artifacts if useful.
- **Concepts:** Serving vs storage, semantic consistency, reproducible analysis, consumer contracts, access control.
- **Expected output:** A reviewer can run a documented analysis from a clean environment.
- **Tests:** Notebook execution, query snapshots, accessibility/basic visualization checks, no hidden local state.
- **Portfolio value:** Connects engineering outputs to understandable research value.
- **Definition of done:** Consumers use curated models only; calculations are versioned SQL/code; refresh and provenance are visible.
- **Interview outcome:** Explain how serving requirements influence models and why notebooks should not own core transformations.

## Phase 9 — Streaming experiment, not migration

- **Objective:** Learn event processing against a measured use case while preserving the batch source of truth.
- **Prerequisites:** Stable batch platform and a question requiring sub-hour latency.
- **Components:** Coinbase public WebSocket trade capture, append-only event files, sequence/gap detection, micro-batch aggregation; Kafka/Redpanda only if multiple consumers/replay justify it.
- **Concepts:** Batch vs streaming, event time vs processing time, ordering, delivery semantics, replay, windows, backpressure.
- **Expected output:** Time-boxed benchmark and ADR deciding keep, evolve, or remove.
- **Tests:** Disconnect/reconnect, duplicate event, out-of-order event, gap, replay, batch-vs-stream candle reconciliation.
- **Portfolio value:** Shows evidence-led distributed-system judgment.
- **Definition of done:** Latency, loss, recovery, resource cost, and operational burden are measured; no unsupported “exactly once” claim.
- **Interview outcome:** Explain when Kafka is justified, how streaming failures differ from batch, and why batch reconciliation remains valuable.

## Phase 10 — Lakehouse/distributed scale, conditional

- **Objective:** Address demonstrated single-node or multi-writer limits—not complete a technology checklist.
- **Prerequisites:** Measurements cross documented thresholds and simpler optimizations fail.
- **Components:** Candidate object storage, Iceberg/Delta, catalog, Spark/Trino/ClickHouse as individually justified; compaction and disaster recovery.
- **Concepts:** Table formats, snapshots, schema/partition evolution, distributed compute, small files, catalogs, consistency.
- **Expected output:** Benchmark and migration ADR with operating-cost model and rollback.
- **Tests:** Snapshot/time travel, concurrent write behavior, schema evolution, compaction, restore, cost/performance comparison.
- **Portfolio value:** Demonstrates scaling based on evidence.
- **Definition of done:** New stack beats the simpler design on defined SLO/cost criteria and can be operated/restored.
- **Interview outcome:** Explain data lake vs warehouse vs lakehouse and why a table format is more than Parquet files.

## Phase 11 — Automated operational diagnostics

- **Objective:** Add auditable operational diagnostics and automated triage where it reduces investigation effort without controlling pipelines or silently changing data.
- **Prerequisites:** Trustworthy metadata, run history, data dictionary, read-only query boundary, evaluation cases.
- **Components:** Diagnostic CLI, read-only query limits, execution audit logs, incident report templates, operator approval boundary.
- **Concepts:** Operational observability, telemetry grounding, provenance, least privilege, human-in-the-loop operations.
- **Expected output:** Diagnostic tooling answers platform health questions with SQL, run IDs, data timestamps, and structured anomaly reports.
- **Tests:** Malformed query attempts, forbidden write attempts, expensive query limits, service degradation resilience, incident benchmarks.
- **Portfolio value:** Practical DataOps integration and automated telemetry analysis.
- **Definition of done:** Core pipeline is unaffected by diagnostic service outage; outputs are traceable; service controls require explicit operator authorization; no automated write path exists.
- **Interview outcome:** Explain where automated diagnostics help, where deterministic validation is safer, and how permission boundaries reduce operational risk.

## Stage gates

Do not advance merely because a phase is interesting. At each gate ask:

1. Is the current phase operated and recoverable?
2. What observed problem does the next component solve?
3. Can configuration/code already present solve it more simply?
4. What new failure modes and maintenance work appear?
5. What evidence will prove the new choice is better?
