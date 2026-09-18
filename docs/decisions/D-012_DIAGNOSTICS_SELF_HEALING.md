# D-012: Auditable Operational Diagnostics and Human-in-the-Loop Self-Healing Framework

## Status
Accepted

## Context
As the Bitcoin Data Platform matured through Phases 1–10 (spanning batch ingestion, conformed on-chain modeling,
systemd orchestration, research serving, WebSocket streaming, and transactional lakehouse tables), operational
incident triage became a critical requirement. When a pipeline fails (e.g. rate limit, disk space pressure,
data contract breach, or server reboot during a run), diagnosing the failure requires cross-referencing
`run_metadata`, `quality_check_results`, `platform_catalog.sqlite`, system logs, and disk metrics.

We need an operational diagnostics framework to answer:
1. How can operators rapidly identify root causes without manual, ad-hoc database queries?
2. How can safe, bounded remediation be executed without introducing autonomous AI black-boxes or risking data corruption?
3. How can incident investigations be audited and tracked transparently over time?

## Decision
We implement an out-of-band, rule-based operational diagnostics engine and bounded self-healing framework in
`src/bitcoin_data_platform/diagnostics/`.

Key architectural decisions:
1. **Zero Pipeline Coupling**: Diagnostics is strictly out-of-band. The primary ingestion, processing, and serving
   pipelines do not import or depend on the diagnostics module.
2. **Deterministic Rule Engine over AI Black-Boxes**: Incident triage uses an explicit 6-part failure taxonomy
   (INC-01 to INC-06) based on deterministic exit codes, telemetry thresholds, and quality check records.
3. **Read-Only Telemetry Collection**: The collector connects to DuckDB and SQLite catalogs in read-only mode
   (`read_only=True` / `mode=ro`) to eliminate the possibility of accidental state mutation during investigation.
4. **Bounded Self-Healing with Human-in-the-Loop**: Self-healing actions are strictly bounded to 4 proven operational
   procedures (`ACTION_CLEAR_STALE_LOCK`, `ACTION_BACKFILL_GAPS`, `ACTION_REBUILD_CURATED`, `ACTION_COMPACT_LAKEHOUSE`).
   Actions default to dry-run simulation and require explicit operator authorization (`--auto-heal`) for execution.
5. **Transparent Query Auditing**: Every diagnostic report includes the exact SQL queries executed during the investigation,
   the affected data partitions, telemetry timestamps, and relevant runbook references.

## Consequences

### Positive
- Reduces Mean Time to Detection (MTTD) and Mean Time to Remediation (MTTR) for data pipeline incidents.
- Safe, auditable failure recovery that preserves watermark monotonicity and immutable raw storage.
- Standardized incident reports exported to structured JSON and human-readable Markdown.
- Zero risk of unintended data mutations during diagnostic triage.

### Negative
- Self-healing is intentionally bounded: novel or unclassified failure modes still require human operator triage.
- Maintenance overhead of maintaining diagnostic rules alongside evolving pipeline schemas.

## Verification & Compliance
All self-healing actions must pass pre-condition revalidation before execution and post-condition health
verification following completion.
