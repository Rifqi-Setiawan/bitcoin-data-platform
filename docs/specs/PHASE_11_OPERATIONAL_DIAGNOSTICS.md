# Phase 11 Implementation Specification — Automated Operational Diagnostics & Self-Healing

Status: approved for implementation
Parent phase: Phase 11 — Automated operational diagnostics
Task ID: `P11-operational-diagnostics`
Recommended branch: `feature/P11-operational-diagnostics`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Implement an auditable, out-of-band operational diagnostics engine and bounded self-healing framework
for the Bitcoin Data Platform. Provide automated incident classification across 6 standard failure modes
(INC-01 through INC-06), root cause analysis with transparent query auditing, structured incident report
generation, and safe, human-in-the-loop remediation workflows that resolve degraded states without
unconstrained database mutation or pipeline coupling.

## 2. User Stories

### Automated System Diagnosis & Triage
As an on-call data engineer, I can execute:
```bash
bitcoin-data diagnose --format text
```
to perform a comprehensive, read-only health audit across DuckDB run metadata, quality check tables,
Lakehouse catalog snapshots, disk utilization, and system logs, receiving a prioritized list of active
incidents with root causes, telemetry evidence, and runbook references.

### Incident Report Generation
As an engineering lead, I can execute:
```bash
bitcoin-data diagnose --run-id <UUID> --output ./reports/incidents/
```
to generate a formal, machine-readable JSON and human-readable Markdown incident report documenting
the failure timeline, affected data partitions, executed SQL investigation queries, and diagnostic confidence.

### Bounded Self-Healing Simulation & Execution
As an operator, I can execute:
```bash
bitcoin-data diagnose --dry-run
```
to view a deterministic remediation plan, and execute:
```bash
bitcoin-data diagnose --auto-heal
```
to execute bounded remediation actions (stale lock clearance, targeted gap backfilling, atomic Parquet rebuild,
or Lakehouse micro-batch compaction) with pre-condition revalidation and post-action health verification.

## 3. Scope & Architectural Guardrails

1. **Zero Pipeline Coupling**: The diagnostics module is strictly out-of-band. Failure or crash of the diagnostics
   subsystem MUST NOT affect, block, or delay ongoing batch or streaming pipelines.
2. **Read-Only by Default**: Telemetry collection, triage, and root cause analysis operate strictly in read-only mode.
   DuckDB connections and SQLite catalog adapters use read-only flags and cannot create directories or alter schemas.
3. **Bounded Remediation Actions**:
   - `ACTION_CLEAR_STALE_LOCK`: Releases `RUNNING` lock only if target process is deceased and duration > 1 hour.
   - `ACTION_BACKFILL_GAPS`: Ingests only verified missing hourly windows (capped at 24 hours per action).
   - `ACTION_REBUILD_CURATED`: Reconstructs Parquet partitions from immutable raw storage without lowering watermarks.
   - `ACTION_COMPACT_LAKEHOUSE`: Merges fragmented small files without altering logical row counts or values.
4. **Human-in-the-Loop**: Remediations default to dry-run mode. Real execution requires explicit `--auto-heal` flag.
5. **No Destructive Sudo/OS Mutations**: Diagnostics never performs `chmod`, `chown`, `rm -rf`, or arbitrary system commands.

## 4. Technical Specifications

### 4.1 Module Structure
```text
src/bitcoin_data_platform/diagnostics/
├── __init__.py
├── models.py           # IncidentRecord, TelemetryBundle, RemediationPlan, RemediationResult
├── collector.py        # Read-only telemetry collector (DuckDB, Lakehouse, alerts, disk, logs)
├── triage.py           # Rule-based triage engine (INC-01 through INC-06 classification)
├── healer.py           # Bounded remediation runner (safe execution adapters)
├── report.py           # Incident report generator (JSON & Markdown export)
└── cli.py              # CLI subcommand: bitcoin-data diagnose
```

### 4.2 Incident Taxonomy (INC-01 through INC-06)
- **INC-01: Source Upstream Outage**
  - Triggers: Exit Code 3, HTTP 429, HTTP 5xx, or request timeouts.
  - Action: Cooldown enforcement; offers `ACTION_BACKFILL_GAPS` after recovery.
- **INC-02: Data Quality Block Violation**
  - Triggers: Exit Code 4, status `FAILED` with severity `BLOCK` in `quality_check_results`.
  - Action: Isolates bad batch in raw inventory; offers `ACTION_REBUILD_CURATED` if valid raw exists.
- **INC-03: Storage & Promotion Failure**
  - Triggers: Exit Code 5, disk usage > 80%, atomic swap failure.
  - Action: Alerts operator; blocks new ingestion until storage headroom is restored.
- **INC-04: Concurrency Lock Collision / Stale Lock**
  - Triggers: Exit Code 6, or run status `RUNNING` for > 1 hour.
  - Action: Evaluates process liveness; triggers `ACTION_CLEAR_STALE_LOCK` if verified dead.
- **INC-05: Data Freshness Lag / Missing Gaps**
  - Triggers: Watermark age > 2 hours or gap count > 0.
  - Action: Plans bounded `ACTION_BACKFILL_GAPS` for missing hours.
- **INC-06: Lakehouse Small-File Bloat**
  - Triggers: Small files (< 16 MiB) count > 32 in any partition.
  - Action: Offers `ACTION_COMPACT_LAKEHOUSE` targeting 128 MiB optimal size.

## 5. Acceptance Criteria

- **AC-1**: Diagnostics package operates under `src/bitcoin_data_platform/diagnostics/` with zero pipeline coupling.
- **AC-2**: Telemetry collector reads DuckDB and Lakehouse catalog strictly in read-only mode.
- **AC-3**: Triage engine accurately classifies simulated incident scenarios (INC-01 through INC-06).
- **AC-4**: Remediation framework executes bounded healing actions safely with dry-run safety gates.
- **AC-5**: Stale lock clearing requires verified process absence and duration > 1 hour.
- **AC-6**: `ACTION_REBUILD_CURATED` and `ACTION_BACKFILL_GAPS` preserve watermark monotonicity.
- **AC-7**: Report generator exports formal JSON and Markdown incident reports with SQL query audit trails.
- **AC-8**: CLI command `bitcoin-data diagnose` supports `--format`, `--run-id`, `--dry-run`, `--auto-heal`, and `--output`.
- **AC-9**: Full test suite passes (>460 tests), quality gates pass cleanly (`ruff`, `mypy`), and ADR D-012 is recorded.
