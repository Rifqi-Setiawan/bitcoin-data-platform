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

## Phase 12 — Investment data expansion: on-chain valuation, market sentiment & macro calendar

- **Objective:** Expand data ingestion with three lightweight, zero-key public sources for autonomous investment signal generation: Coin Metrics MVRV ratio, Alternative.me Crypto Fear & Greed Index, and ForexFactory US high-impact economic calendar events.
- **Prerequisites:** Conformed daily market and on-chain fact views, DuckDB analytical engine.
- **Components:** Coin Metrics MVRV extension (`CapMVRVCur`), Sentiment client and contract validator (`fetch-sentiment`), Macro calendar client and contract validator (`fetch-macro-calendar`), DuckDB tables `raw_crypto_sentiment_daily` and `raw_macro_economic_events`, conformed view `mart_btc_investment_signals_daily`.
- **Concepts:** On-chain valuation multiples (MVRV), sentiment indicators, event-driven macro awareness, multi-domain conformed modeling, tactical asset allocation signals.
- **Expected output:** Clean ingestion subcommands, Parquet Hive partitioning with MVRV, conformed DuckDB mart calculating SMA-200, Mayer Multiple, and deterministic investment signals.
- **Tests:** Contract parsing/validation, transient HTTP retry/backoff, rate limiting, timezone conversions, Parquet schema extension, analytical mart window aggregation and investment signal classification bands.
- **Portfolio value:** Bridges pure data engineering with quantitative investment research and evidence-based signal generation.
- **Definition of done:** All 3 data sources operational; DuckDB analytical view generates correct Mayer Multiple, MVRV, FNG, and signal classifications; zero API keys required; 100% test coverage with zero regression.
- **Interview outcome:** Explain the mechanics of MVRV and Mayer Multiple, how multi-domain marts handle asynchronous cadences, and how deterministic signal classification avoids black-box ML pitfalls.

## Phase 13 — Investment signal engine: daily generation, news sentinel & Telegram alert dispatch

- **Objective:** Operationalize tactical asset allocation with daily investment signal generation from `mart_btc_investment_signals_daily`, automated CoinDesk RSS scanning for critical black swan market events, and real-time Telegram notification delivery.
- **Prerequisites:** Phase 12 investment data expansion and conformed mart `mart_btc_investment_signals_daily`.
- **Components:** `SignalGenerator` (`signals/generator.py`), `NewsSentinel` (`signals/news_sentinel.py`), `TelegramDispatcher` (`alerts/telegram_dispatcher.py`), DuckDB tables `signal_history` and `news_sentinel_alerts`, CLI subcommands `generate-signal`, `news-sentinel`, `send-alert`.
- **Concepts:** Rule-based tactical asset allocation, multi-indicator agreement rating (STRONG, MODERATE, WEAK), localized narratives (Bahasa Indonesia), RSS XML parsing and regex filtering, SHA-256 event deduplication, Telegram Bot API integration with retry backoff and credential redaction.
- **Expected output:** Deterministic daily signal reports (JSON and text), deduplicated news alerts with severity categorization, audit trail storage in DuckDB, and zero-dependency Telegram notification dispatch.
- **Tests:** Signal regime evaluation, multi-indicator strength computation, RSS keyword extraction and deduplication, XML error handling, Telegram markdown formatting, dry-run dispatch, transient error retry, and credential protection.
- **Portfolio value:** Transforms passive data warehousing into an active, automated investment advisory and event-monitoring platform.
- **Definition of done:** All CLI subcommands operational; DuckDB audit tables populated; Telegram notifications delivered in dry-run and live modes; zero API credentials leaked; full test suite and quality gates passing.
- **Interview outcome:** Explain how rule-based signal consensus eliminates hallucination risk, how event-driven news monitoring provides defensive circuit breakers, and how to build secure notification channels with zero external framework dependencies.

## Phase 14 — Backtest & validation engine: historical simulation & quantitative benchmarking

- **Objective:** Build an institutional-grade, event-driven backtesting and quantitative validation engine that simulates systematic Bitcoin investment strategies—benchmarking Dynamic Reserve DCA against Blind DCA and Lump Sum Buy & Hold across complete market cycles without lookahead bias.
- **Prerequisites:** Phase 12 conformed mart `mart_btc_investment_signals_daily` and Phase 13 rule-based investment signals.
- **Components:** `BacktestEngine` (`backtest/engine.py`), strategies `BaseStrategy`, `LumpSumStrategy`, `BlindDCAStrategy`, `DynamicReserveDCAStrategy` (`backtest/strategies.py`), quantitative metrics calculator (`backtest/metrics.py`), formatters `format_table`, `format_markdown`, `format_json` (`backtest/reporter.py`), CLI subcommand `backtest`, research notebook `notebooks/investment_strategy_backtest.ipynb`.
- **Concepts:** Chronological event-driven simulation, dual-wallet accounting (fiat base cash vs tactical reserve cash vs BTC holding), cash flow tracking, fee modeling (bps), 5-tier dynamic accumulation multipliers, macro circuit breaker halts, CAGR (365-day basis), Maximum Drawdown (MDD), continuous annualized Sharpe and Sortino ratios, Calmar ratio, acquisition cost discount vs average market price.
- **Expected output:** Deterministic backtest execution across multi-year histories, multi-strategy side-by-side benchmarking tables (ASCII, Markdown, JSON), reproducible visual research notebook, zero cash leakage across simulations.
- **Tests:** Model contract validation, strategy decision logic & multipliers, mathematical verification of metrics (CAGR, MDD, Sharpe, Sortino, Calmar), full event loop simulation, fee deduction, cash accounting conservation, CLI argument parsing and error handling (80 new tests, 625 total tests, 100% pass).
- **Portfolio value:** Provides empirical, quantitative justification for capital deployment before autonomous execution (Phases 15 & 16), proving out-of-sample risk-adjusted outperformance and drawdown reduction.
- **Definition of done:** All 3 strategies benchmarked; CLI `bitcoin-data backtest` operational; quantitative metrics mathematically verified; Jupyter notebook committed and reproducible; 100% test pass rate with zero regression across all phases.
- **Interview outcome:** Defend why Dynamic Reserve DCA outperforms naive DCA and Buy & Hold in drawdown protection, explain the mathematical difference between Sharpe and Sortino ratios in asymmetric return distributions, and demonstrate how to design an event-driven backtester free from lookahead bias.

## Phase 15 — Forward paper trading engine ($1,000 virtual capital) & Fincept-style web dashboard integration

- **Objective:** Implement a forward-testing Paper Trading Simulation Engine initialized with $1,000.00 USD virtual capital and integrate an institutional Fincept Terminal-style Portfolio Tracker into the existing Bitcoin Market Hub Web UI dashboard.
- **Prerequisites:** Phase 12 conformed mart `mart_btc_investment_signals_daily`, Phase 13 investment signal generator, and Phase 14 `DynamicReserveDCAStrategy` logic.
- **Components:** `PaperTradingEngine` (`paper/engine.py`), `RiskGuard` (`paper/risk_guard.py`), `models.py` (`PaperPortfolioBalance`, `PaperTradeRecord`, `PaperSnapshotRecord`, `PaperSummary`), DuckDB tables `paper_portfolio_balance`, `paper_portfolio_snapshots_daily`, `paper_trade_ledger`, Dashboard server REST endpoints `/api/portfolio`, `/api/portfolio/equity`, `/api/portfolio/trades`, Fincept Terminal UI tab in `dashboard/assets/index.html`, CLI subcommand `paper` (`init`, `step`, `status`, `reset`).
- **Concepts:** Forward paper trading, AutoHedge-inspired pre-trade risk gatekeeper (`RiskGuard`), filesystem emergency kill-switch (`data/state/PAPER_KILL_SWITCH`), solvency and 20% price deviation validation, dual-entry shadow accounting (70% Base Cash / 30% Tactical Reserve Cash), simulated 10 bps Coinbase Spot fee deduction, mark-to-market daily snapshots vs $1,000 Buy & Hold benchmark, institutional order blotter, Fincept Terminal dark UI.
- **Expected output:** Deterministic daily forward DCA order execution, real-time portfolio metrics, Chart.js dual-curve equity tracking vs Buy & Hold benchmark, asset allocation donut chart, interactive order blotter, CLI and REST API access with fallback resilience.
- **Tests:** Model contracts & serialization, pre-trade risk guard rules (kill switch, deviation, solvency, macro event policy), step execution across all 5 regimes, fee deduction, benchmark equity calculation, REST endpoints with fallback and populated states, CLI subcommands (39 new tests, 665 total tests, 100% pass).
- **Portfolio value:** Bridges historical quantitative backtesting and autonomous live execution by providing empirical out-of-sample forward paper trading validation with transparent web observability and capital safety guards.
- **Definition of done:** All CLI `bitcoin-data paper` subcommands operational; DuckDB relational schema initialized; `/api/portfolio*` endpoints live; Web UI features Fincept Terminal tab with dual-curve performance charts and order blotter; full test suite and quality gates passing with zero regressions.
- **Interview outcome:** Explain how forward paper trading prevents model overfitting, how pre-trade risk gatekeepers enforce institutional capital preservation before live execution, and how to design dual-pool cash allocation architectures.

## Phase 16 — Macro & Narrative Intelligence Engine & Web Dashboard Macro Radar

- **Objective:** Build a 3-tier Macro & Narrative Intelligence Engine that ingests multi-source crypto RSS feeds, computes macroeconomic calendar surprise deltas, evaluates 4-pillar narrative polarity, determines 5-regime Composite Macro-Narrative Index (MNI), trips automated black swan circuit breakers, and serves an interactive Macro Radar web dashboard with verifiable clickable links.
- **Prerequisites:** Phase 12 macroeconomic calendar, Phase 13 news sentinel, Phase 15 forward paper trading engine and dashboard.
- **Components:** `FeedIngester` (`macro/feed_ingester.py`), `MacroAnalyzer` (`macro/macro_analyzer.py`), `SentimentAnalyzer` (`macro/sentiment_analyzer.py`), `MacroNarrativeSynthesizer` (`macro/synthesizer.py`), `RiskGuard` pre-trade extension (`paper/risk_guard.py`), DuckDB tables `macro_news_articles`, `macro_economic_releases`, `daily_narrative_intelligence`, view `mart_macro_narrative_daily`, dashboard REST endpoints `/api/macro/radar`, `/api/macro/news`, `/api/macro/calendar`, Web UI Macro Radar tab with anti-slop design, and CLI subcommand `macro` (`fetch-news`, `fetch-calendar`, `synthesize`, `radar`, `status`).
- **Concepts:** Multi-source RSS ingestion with error isolation, SHA-256 content deduplication, macroeconomic indicator surprise normalization ($\Delta = \text{Actual} - \text{Forecast}$), 24h half-life exponential time decay over 72h window, 4-pillar lexical regex classification (`REGULATORY`, `SECURITY_EXPLOIT`, `INSTITUTIONAL`, `MACRO_LIQUIDITY`), hyperbolic tangent saturation clamping, 3-tier composite MNI formula ($0.40 \cdot S_{\text{macro}} + 0.35 \cdot S_{\text{sentiment}} + 0.25 \cdot S_{\text{narrative}}$), 5-regime classifier, localized Bahasa Indonesia narratives, pre-trade AutoHedge-inspired circuit breakers, zero-synthesized-slop clickable attribution.
- **Expected output:** Deterministic news and calendar ingestion, automated daily MNI synthesis, DuckDB analytical mart views, web UI Macro Radar visualization with original clickable hyperlinks, and pre-trade circuit breaker protection against black swans and liquidity shocks.
- **Tests:** Multi-source RSS parsing (CoinDesk, Cointelegraph, Decrypt), backoff and error isolation, economic surprise calculations and time decay, lexical polarity and pillar classification, 3-tier MNI weighting and regimes, RiskGuard macro circuit breakers, REST API endpoints with fallback resilience, CLI subcommands (47 new tests, 712 total tests, 100% pass).
- **Portfolio value:** Transforms retrospective metric reporting into proactive institutional macro and narrative intelligence with automated execution circuit breakers and auditable original-source attribution.
- **Definition of done:** CLI subcommand `bitcoin-data macro` fully operational; DuckDB tables and analytical view queryable; `/api/macro/*` REST endpoints live with resilient fallbacks; Web UI renders interactive Macro Radar with verifiable news links; full test suite passing with zero regressions.
- **Interview outcome:** Explain how multi-tier quantitative synthesis balances macroeconomic liquidity expectations with on-chain fundamentals and unstructured news narratives, how pre-trade circuit breakers prevent capital loss during black swan events, and how to design low-footprint deterministic news ingestion without browser bloat.

## Phase 17 — Agentic autonomous investment committee, user intelligence ingestion & layered automated scheduling

- **Objective:** Establish an institutional-grade, fully autonomous Agentic Investment Decision System solving the AutoHedge failure mode ("Risk-as-a-Prompt") via a Dual-System Neuro-Symbolic architecture, enabling user alpha injection, and automating 24/7 continuous pipeline orchestration across hourly, daily, and weekly cycles on a single-host VPS.
- **Prerequisites:** Phase 15 forward paper trading engine ($1,000 virtual sandbox), Phase 16 3-tier Composite Macro-Narrative Index (MNI), and conformed DuckDB mart views.
- **Components:** Dual-System Investment Committee (`committee/engine.py`, `committee/personas.py`, `committee/reporter.py`), symbolic solver & Pre-Trade RiskGuard invariant verifier (`committee/invariant_solver.py`, `paper/risk_guard.py`), User Intelligence Ingestion Engine (`intelligence/ingester.py`, `intelligence/models.py`), Layered Automated Scheduling Orchestrator (`pipeline/orchestrator.py`, `pipeline/lock_manager.py`), systemd oneshot services & timers (`infra/systemd/bitcoin-data-hourly.*`, `infra/systemd/bitcoin-data-daily.*`, `infra/systemd/bitcoin-data-weekly.*`), DuckDB tables (`user_market_intelligence`, `investment_committee_memos`, `investment_committee_votes`), analytical view `mart_committee_deliberation_daily`, Web Dashboard Investment Committee tab & intelligence modal (`dashboard/assets/index.html`), REST API routes (`/api/committee/*`, `/api/intelligence/*`, `/api/pipeline/schedule`), and CLI subcommands `committee`, `intelligence`, and `pipeline`.
- **Concepts:** Dual-System Neuro-Symbolic architecture (System 1: multi-persona qualitative reasoning vs. System 2: deterministic mathematical invariant verification), AutoHedge failure mode mitigation (unconstrained LLM rationalization vs. sub-millisecond symbolic clamping), 6 pre-trade mathematical invariants (absolute solvency, 15% daily tactical reserve deployment ceiling, macro announcement proximity buffer $\pm 2$ hours, black swan emergency halt, 25% max drawdown gate, spot price deviation), user-injected alpha ingestion with SHA-256 deduplication and lightweight web scraping, tiered scheduling for single-host VPS (hourly news/alerts, daily deliberation/paper step, weekly drawdown audit), POSIX run lockfile supervision, and localized institutional memorandums in Bahasa Indonesia.
- **Expected output:** Fully autonomous daily investment deliberations, institutional investment memorandums with consensus voting and Bahasa Indonesia executive summaries, audited execution receipts (neural proposed vs. symbolic clamped), user research injection through CLI and Web UI, 24/7 automated background orchestration via hardened systemd units, and zero-leakage paper portfolio updating.
- **Tests:** Persona scoring and consensus aggregation, symbolic invariant solver and clamping edge cases, user intelligence scraping and deduplication, pipeline lock acquisition and PID liveness validation, orchestrator execution DAGs (hourly, daily, weekly), dashboard REST endpoints, and end-to-end CLI integration (56 new tests, 768 total tests, 100% pass).
- **Portfolio value:** Demonstrates state-of-the-art neuro-symbolic AI architecture for autonomous financial systems, proving how to safely harness large language models for qualitative deliberation while delegating all risk enforcement to deterministic mathematical code.
- **Definition of done:** All CLI subcommands (`committee`, `intelligence`, `pipeline`) operational; DuckDB tables and view queryable; `/api/committee/*`, `/api/intelligence/*`, and `/api/pipeline/schedule` live; Web Dashboard Investment Committee tab fully interactive; hardened systemd unit definitions committed; full test suite passing with zero regressions (768 passed).
- **Interview outcome:** Defend the Dual-System Neuro-Symbolic paradigm against naive prompt-driven agents (AutoHedge critique), explain why risk enforcement must remain deterministic and sub-millisecond in financial systems, and demonstrate how to orchestrate multi-cadence autonomous data pipelines on single-host infrastructure without distributed lock bloat.

## Stage gates

Do not advance merely because a phase is interesting. At each gate ask:

1. Is the current phase operated and recoverable?
2. What observed problem does the next component solve?
3. Can configuration/code already present solve it more simply?
4. What new failure modes and maintenance work appear?
5. What evidence will prove the new choice is better?
