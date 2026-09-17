# Data Quality, Observability & Incident Runbook

**System:** Bitcoin Data Engineering Platform  
**Phase:** Phase 5 — Observability, Data Quality Rules & Failure Alerting  
**Target Product:** `BTC-USD` on `coinbase_exchange`  
**Storage Architecture:** Immutable Raw Envelopes (`.json.gz`) → Curated Parquet → DuckDB Views  

---

## 1. Overview & Observability Framework

Phase 5 equips the Bitcoin Data Platform with continuous dataset quality validation, multi-layer observability, health checking, and low-noise operational alerting.

```text
┌─────────────────────────────────────────────────────────────┐
│                 Batch Pipeline Execution                     │
│               (backfill / incremental / promote)            │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 Data Quality Evaluation                      │
│   - Natural Key Uniqueness (BLOCK)                          │
│   - Boundary Reconciliation (BLOCK)                         │
│   - Hourly Price Return Anomaly >15% (WARN)                 │
│   - Volume Spike Anomaly >5x (WARN)                         │
└──────────────┬───────────────────────────────┬──────────────┘
               │ Passed / WARN                 │ FAILED (BLOCK)
               ▼                               ▼
┌──────────────────────────────┐ ┌────────────────────────────┐
│ Persist to Parquet & DuckDB  │ │ Abort with Exit Code 4      │
│ Record in quality_check_     │ │ Trigger OnFailure Systemd   │
│ results table                │ │ Service & Alert Handler    │
└──────────────────────────────┘ └────────────────────────────┘
```

### 1.1 Single-Command Health Check
Operators and automated watchdogs evaluate platform health via:

```bash
bitcoin-data status --format text --check
```

- **Exit code `0`**: System is healthy (`is_healthy: true`).
- **Exit code `1`**: System is degraded (`is_healthy: false`).

A healthy status requires all four conditions:
1. **Fresh Watermark**: Watermark age $\le 2.0$ hours.
2. **Zero Unresolved Gaps**: No missing hours across curated candles.
3. **Storage Headroom**: Curated disk usage $< 80.0\%$.
4. **No Active Locks**: Pipeline is not locked in a failed/running state.

---

## 2. Data Quality Rules & Severity Framework

Quality rules are categorized into three severity levels:

| Severity | Action on Failure | Pipeline Behavior | Example Rules |
|----------|-------------------|-------------------|---------------|
| `BLOCK`  | Exit code `4` | Halts promotion; watermark does NOT advance | Natural key duplicate, candle timestamp outside window, OHLC violation |
| `WARN`   | Logged to stderr & recorded in DuckDB | Allows promotion; flagged in logs & tables | Price return move > 15%, volume spike > 5x average |
| `INFO`   | Recorded in DuckDB | Normal operation | Batch row counts, observed interval span |

### 2.1 DuckDB Quality Audit Table
All evaluations are logged to the `quality_check_results` table:

```sql
SELECT
    check_id,
    run_id,
    rule_name,
    severity,
    status,
    metric_value,
    threshold_value,
    details,
    evaluated_at_utc
FROM quality_check_results
ORDER BY evaluated_at_utc DESC
LIMIT 20;
```

---

## 3. Incident Triaging & Remediation Playbooks

### 3.1 Incident: Watermark Freshness Lag (> 2 Hours)

#### Symptoms
- `bitcoin-data status --check` returns exit code `1`.
- `watermark_age_hours` exceeds `2.0`.

#### Diagnosis
1. Inspect systemd timer and service status:
   ```bash
   systemctl status bitcoin-data.timer
   systemctl status bitcoin-data.service
   journalctl -u bitcoin-data.service -n 50 --no-pager
   ```
2. Verify network connectivity to Coinbase API:
   ```bash
   curl -I https://api.exchange.coinbase.com/products/BTC-USD/candles
   ```
3. Check for lingering run locks:
   ```bash
   bitcoin-data status --format text
   ```

#### Remediation
- If run lock is stuck from a crashed process older than 1 hour, repair with force flag:
  ```bash
  bitcoin-data repair --force
  ```
- Trigger a manual incremental cycle:
  ```bash
  bitcoin-data incremental --overlap-hours 48
  ```
- Re-check health:
  ```bash
  bitcoin-data status --check
  ```

---

### 3.2 Incident: Data Gaps Detected in Curated Layer

#### Symptoms
- `status` output displays `Gaps Detected: N`.
- `is_healthy` evaluates to `false`.

#### Diagnosis
Identify the exact missing interval:
```bash
bitcoin-data status --format json | jq '.gaps'
```

Alternatively, query DuckDB directly:
```bash
bitcoin-data query --sql "
SELECT
    candle_start_utc,
    LEAD(candle_start_utc) OVER (ORDER BY candle_start_utc) AS next_candle,
    date_diff('hour', candle_start_utc, LEAD(candle_start_utc) OVER (ORDER BY candle_start_utc)) AS diff_hours
FROM fact_market_candle_hourly
QUALIFY diff_hours > 1;
"
```

#### Remediation
Execute a targeted backfill over the missing range:
```bash
bitcoin-data backfill --start "<GAP_START_UTC>" --end "<GAP_END_UTC>"
bitcoin-data promote
bitcoin-data status --check
```

---

### 3.3 Incident: Storage Utilization Warning (> 70%) or Critical (> 80%)

#### Symptoms
- `disk_warning: true` at $> 70\%$ capacity.
- `disk_critical: true` at $> 80\%$ capacity (triggers degraded health status).

#### Diagnosis
Check disk utilization across partitions:
```bash
df -h /srv/data/bitcoin-data-platform
du -sh /srv/data/bitcoin-data-platform/*
```

#### Remediation
1. Clean old temporary files in `/tmp` and `.tmp` artifacts.
2. Archive older compressed raw envelopes (`.json.gz`) to secondary cold storage:
   ```bash
   # Identify raw envelopes older than 90 days
   find /srv/data/bitcoin-data-platform/raw -name "*.json.gz" -mtime +90
   ```
3. Parquet layers are column-compressed and should remain intact; do not delete curated partitions without an archived raw source.

---

### 3.4 Incident: Price Return Anomaly or Volume Spike Alert

#### Symptoms
- Warning log emitted during incremental or promote run:
  `{"event": "quality_check_warning", "details": "Price return anomaly detected..."}`
- Records in `quality_check_results` with `severity='WARN'` and `status='FAILED'`.

#### Diagnosis
Query the anomalous batch details:
```sql
SELECT * FROM quality_check_results
WHERE severity = 'WARN' AND status = 'FAILED'
ORDER BY evaluated_at_utc DESC
LIMIT 5;
```

Inspect the candles in the relevant time window:
```sql
SELECT
    candle_start_utc,
    open,
    high,
    low,
    close,
    volume_base
FROM fact_market_candle_hourly
WHERE candle_start_utc >= '<ANOMALY_TIMESTAMP>' - INTERVAL 3 HOUR
  AND candle_start_utc <= '<ANOMALY_TIMESTAMP>' + INTERVAL 3 HOUR
ORDER BY candle_start_utc;
```

#### Remediation
1. Verify if real-world high volatility occurred (e.g. major macro announcement, flash crash).
2. If real market move: No data action needed; the pipeline successfully recorded the move.
3. If upstream provider corruption:
   - Report issue or wait for upstream exchange restatement.
   - Run `repair` or targeted `backfill` once upstream fixes the data.

---

## 4. Failure Alert Dispatcher & Deduplication

### 4.1 Systemd Failure Handler
`bitcoin-data.service` defines `OnFailure=bitcoin-data-failure@%n.service`. When incremental ingestion fails:
1. Systemd instantiates `bitcoin-data-failure@bitcoin-data.service.service`.
2. The unit runs `bitcoin-data alert --failed-unit %I`.
3. The alert dispatcher scrubs sensitive credentials and inspects `/srv/data/bitcoin-data-platform/state/alert_state.json`.
4. If identical failure occurred within 2 hours, duplicate alerts are suppressed to prevent operational alert fatigue.

### 4.2 Manual Alert Testing
Operators can verify alert dispatching manually:

```bash
bitcoin-data alert --failed-unit bitcoin-data.service --message "Manual diagnostic alert test"
```
