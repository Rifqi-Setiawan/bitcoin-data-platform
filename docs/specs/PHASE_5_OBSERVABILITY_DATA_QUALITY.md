# Phase 5 Implementation Specification — Observability, Data Quality Rules & Failure Alerting

Status: approved for implementation
Parent phase: Phase 5 — Observability, data quality rules, and failure alerting
Task ID: `P5-observability-quality-rules`
Recommended branch: `feature/P5-observability-quality-rules`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Enhance the Bitcoin Data Platform with end-to-end data observability, comprehensive dataset-level
and anomaly quality rules, operational storage monitoring, a human-readable and machine-readable
system health check, and a low-noise failure alerting mechanism. After Phase 5, an operator can
determine system health in a single command, automatically detect data gaps/freshness violations,
and receive deduplicated, scrubbed failure notifications.

## 2. User Stories

### Automated Health Check & Diagnostic
As an on-call data engineer, I can run `bitcoin-data status --format text --check` to immediately
see if the data pipeline is healthy (watermark current < 2 hours, no unresolved gaps, disk usage
under threshold, zero stale locks), returning exit code 0 when healthy and non-zero on degradation.

### Data Quality Auditing
As an analyst, I can inspect the `quality_check_results` table in DuckDB to see historical rule
evaluations for every batch (OHLC invariants, natural key uniqueness, boundary reconciliation,
and price/volume anomaly signals) alongside execution metadata.

### Low-Noise Failure Notification
As an operator, if the scheduled systemd service fails, `systemd` triggers an `OnFailure` alert
handler that deduplicates recurring alerts and scrubs sensitive system details before logging
to journald or notifying operational endpoints.

## 3. Scope

### In Scope
1. **Dataset-Level Quality Rules (`quality/dataset_checks.py`)**:
   - Natural key uniqueness verification across batch and partition.
   - Window boundary reconciliation (requested interval vs received timestamps).
   - Price return anomaly check (warn on hourly return move > 15%).
   - Volume spike anomaly check (warn on volume > 5x historical rolling average).
   - Data quality result records and severity classification (`BLOCK`, `WARN`, `INFO`).
2. **DuckDB Quality Table & Schema Enrichment (`storage/duckdb_manager.py`)**:
   - Table `quality_check_results` (`run_id`, `rule_name`, `severity`, `status`, `metric_value`,
     `threshold_value`, `details`, `evaluated_at_utc`).
   - Schema migration for `run_metadata`: add `code_version`, `source_row_count`, `valid_row_count`,
     `gap_count`, `raw_checksums`, `error_class`.
3. **Enhanced Status Command (`cli.py` & `storage/duckdb_manager.py`)**:
   - Disk space inspection (`shutil.disk_usage`) with warnings at > 70% and critical alert at > 80%.
   - Freshness evaluation: healthy if watermark age <= 2.0 hours.
   - Gap detection integration into overall platform health status.
   - `--format text|json` option with clear terminal summary.
   - `--check` flag: exits 0 if healthy, exit 1 if degraded (freshness lag, gaps, disk full, or active error lock).
4. **Low-Noise Failure Alert Dispatcher (`infra/systemd/`)**:
   - Unit template `infra/systemd/bitcoin-data-failure@.service`.
   - Alert handler script `src/bitcoin_data_platform/infra/alert_handler.py` (or CLI subcommand `bitcoin-data alert`):
     - Scrub sensitive system information.
     - Deduplication state (rate limit consecutive identical failures).
     - Structured JSON alert payload.
5. **Runbook**:
   - `docs/runbooks/DATA_QUALITY_RUNBOOK.md` detailing alert severity, threshold tuning, and incident remediation.
6. **Testing**:
   - Comprehensive test suite in `tests/test_dataset_quality.py`, `tests/test_status_healthcheck.py`, and `tests/test_alert_handler.py`.
   - Zero regressions across existing 206 tests.

### Out of Scope
- External SaaS dashboards (Datadog, Grafana Cloud) — keep single-host architecture lean and local.
- Complex machine learning anomaly models — use robust statistical baselines.

## 4. Technical Specifications

### 4.1 Data Quality Severity & Schema
Rules are classified into:
- **BLOCK**: Halts promotion, aborts watermark advancement (e.g. OHLC violation, non-unique key, invalid types).
- **WARN**: Logged in `quality_check_results` and status warnings, but permits promotion (e.g. price move > 15%, volume spike, missing hourly gaps).
- **INFO**: Informational metric recorded for audit (e.g. candle count, observed span).

DuckDB Table `quality_check_results`:
```sql
CREATE TABLE IF NOT EXISTS quality_check_results (
    check_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    rule_name VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,  -- BLOCK, WARN, INFO
    status VARCHAR NOT NULL,    -- PASSED, FAILED
    metric_value DOUBLE,
    threshold_value DOUBLE,
    details VARCHAR,
    evaluated_at_utc TIMESTAMPTZ NOT NULL
);
```

### 4.2 Status Health Check Metrics
The `status` command evaluates:
- `is_healthy = (watermark_age_hours <= 2.0) and (len(gaps) == 0) and (disk_percent < 80.0) and (not is_locked)`
- Output includes disk stats: `total_gb`, `used_gb`, `free_gb`, `percent_used`.
- CLI syntax:
  ```bash
  bitcoin-data status [--format json|text] [--check]
  ```

### 4.3 Low-Noise Alert Dispatcher
- Systemd `OnFailure=bitcoin-data-failure@%n.service` attached to `bitcoin-data.service`.
- Triggered service executes `bitcoin-data alert --failed-unit %I`.
- Maintains state in `/srv/data/bitcoin-data-platform/state/alert_state.json` to prevent alert storming (suppresses duplicate alerts within a 2-hour window unless status changes).

## 5. Acceptance Criteria

- **AC-1**: Dataset quality checks evaluate natural key uniqueness, boundary coverage, and statistical price/volume anomalies.
- **AC-2**: Table `quality_check_results` records all check outcomes in DuckDB.
- **AC-3**: `bitcoin-data status` provides disk usage, freshness evaluation, gap summary, and health status in both JSON and formatted text.
- **AC-4**: `bitcoin-data status --check` returns exit code 0 on healthy pipeline and exit code 1 on degraded conditions.
- **AC-5**: Systemd `OnFailure` template and alert handler script filter sensitive data and throttle repeated alerts.
- **AC-6**: `docs/runbooks/DATA_QUALITY_RUNBOOK.md` documents quality rules, anomaly triage, and health check diagnosis.
- **AC-7**: All existing 206 tests pass, plus all new Phase 5 tests pass (target > 230 total tests).
- **AC-8**: Quality gates pass cleanly (`ruff check`, `ruff format --check`, `mypy src`).
- **AC-9**: Clean commit with no internal agent references, pushed to GitHub.
