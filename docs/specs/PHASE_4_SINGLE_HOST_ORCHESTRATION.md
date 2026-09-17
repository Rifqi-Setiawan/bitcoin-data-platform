# Phase 4 Implementation Specification — Single-Host Orchestration (systemd Timer & Service)

Status: Approved
Parent phase: Phase 4 — Single-host orchestration and scheduled pipeline
Task ID: `P4-systemd-orchestration`
Recommended branch: `feature/P4-single-host-orchestration`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Operationalize the deterministic incremental loading pipeline (from Phase 3) as a scheduled,
supervised, and sandboxed background service on a single-host Linux VPS using native `systemd`
service and timer units. Ensure least-privilege execution under a dedicated non-login system
user (`bitcoin-data`), strict filesystem sandbox constraints, structured journald logging,
missed-run catchup (`Persistent=true`), and clear operational deployment/rollback runbooks.

## 2. User Stories

### Hourly Scheduled Run
As a data engineer, the platform automatically triggers `bitcoin-data incremental` 10 minutes past
every hour (`*:10:00 UTC`), safely after the previous UTC hourly candle has closed, logging structured
JSON events to `journald` without operator intervention.

### Catch-up on Server Downtime
As an operator, if the VPS is rebooted or temporarily offline during a scheduled window, `systemd`
with `Persistent=true` automatically executes the missed hourly batch upon startup, advancing the
watermark reliably.

### Sandboxed Execution & Least Privilege
As a security-minded engineer, the service runs strictly as `User=bitcoin-data`, possesses no `sudo`
or interactive login shell, has read-only access to application code, and is sandboxed with
`ProtectSystem=strict` with write access restricted strictly to `/srv/data/bitcoin-data-platform`.

## 3. Scope

### In Scope
- Creation of `infra/systemd/bitcoin-data.service` (Type=oneshot, sandbox directives, resource limits).
- Creation of `infra/systemd/bitcoin-data.timer` (hourly at :10, Persistent=true, RandomizedDelaySec=120).
- Configuration template `infra/systemd/bitcoin-data.env.example` for environment variable injection.
- Verification script / CLI helper to validate systemd unit syntax and configuration validity offline.
- Comprehensive deployment, verification, and rollback runbook in `docs/runbooks/DEPLOYMENT_RUNBOOK.md`.
- Automated tests in `tests/test_systemd_config.py` verifying unit file structure, security sandboxing,
  calendar schedule syntax, and environment compatibility.
- Documentation updates in `README.md` and `docs/roadmap/ROADMAP.md`.

### Out of Scope
- Multi-host orchestration or distributed workers (Airflow/Dagster/Kubernetes) — unnecessary complexity for V1.
- Inbound listening ports or web dashboards (security boundary: outbound HTTPS only).
- Advanced alerting integrations (PagerDuty, Slack webhook) — slated for Phase 5 (Observability).

## 4. Technical & Security Contract

### 4.1 Systemd Service Unit (`infra/systemd/bitcoin-data.service`)
```ini
[Unit]
Description=Bitcoin Data Engineering Platform - Incremental Ingestion & Promotion
Documentation=https://github.com/Rifqi-Setiawan/bitcoin-data-platform
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=bitcoin-data
Group=bitcoin-data
EnvironmentFile=-/etc/bitcoin-data/bitcoin-data.env
ExecStart=/srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data incremental \
    --raw-dir /srv/data/bitcoin-data-platform/raw \
    --curated-dir /srv/data/bitcoin-data-platform/curated \
    --db-path /srv/data/bitcoin-data-platform/state/platform.duckdb \
    --overlap-hours 48

# Security & Sandboxing
ProtectSystem=strict
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true
ProtectKernelTunables=true
ProtectControlGroups=true
RestrictSUIDSGID=true
ReadWritePaths=/srv/data/bitcoin-data-platform /tmp
ReadOnlyPaths=/srv/apps/services/bitcoin-data-platform

# Resource Limits (Prevent interference with VPS host services)
MemoryMax=1G
CPUQuota=100%

# Logging
StandardOutput=journal
StandardError=journal
```

### 4.2 Systemd Timer Unit (`infra/systemd/bitcoin-data.timer`)
```ini
[Unit]
Description=Hourly Schedule for Bitcoin Data Platform Incremental Ingestion
Documentation=https://github.com/Rifqi-Setiawan/bitcoin-data-platform

[Timer]
OnCalendar=*-*-* *:10:00
RandomizedDelaySec=120
Persistent=true
Unit=bitcoin-data.service

[Install]
WantedBy=timers.target
```

### 4.3 Directory Layout & Permissions (VPS Production Target)
- Application code (owned by deployer, read-only for service):
  `/srv/apps/services/bitcoin-data-platform/`
- Runtime data & state (owned by `bitcoin-data:bitcoin-data`, mode `0750`):
  `/srv/data/bitcoin-data-platform/`
  ├── `raw/`
  ├── `curated/`
  ├── `state/`
  ├── `quarantine/`
  └── `tmp/`
- Systemd units:
  `/etc/systemd/system/bitcoin-data.service`
  `/etc/systemd/system/bitcoin-data.timer`

## 5. Required Tests & Acceptance Criteria

### Test Cases (`tests/test_systemd_config.py`)
1. Service unit file exists and parses as valid INI structure.
2. Service unit specifies `Type=oneshot`.
3. Service unit configures dedicated `User=bitcoin-data` and `Group=bitcoin-data`.
4. Security hardening flags present: `ProtectSystem=strict`, `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectHome=true`.
5. Write access restricted: `ReadWritePaths` contains `/srv/data/bitcoin-data-platform`.
6. Timer unit file exists and parses as valid INI structure.
7. Timer unit specifies `OnCalendar=*-*-* *:10:00`.
8. Timer unit enables `Persistent=true` for missed-run recovery.
9. Timer unit defines jitter `RandomizedDelaySec` between 30 and 300 seconds.
10. Environment template `bitcoin-data.env.example` exists and documents optional tuning variables.

### Acceptance Criteria
- **AC-1**: Unit files `infra/systemd/bitcoin-data.service` and `infra/systemd/bitcoin-data.timer` conform strictly to security directives.
- **AC-2**: Unit tests in `tests/test_systemd_config.py` pass 100%.
- **AC-3**: Complete, step-by-step `docs/runbooks/DEPLOYMENT_RUNBOOK.md` covering:
  - System user provisioning (`useradd --system --no-create-home --shell /usr/sbin/nologin bitcoin-data`).
  - Filesystem permission boundaries and directory creation.
  - Unit installation, enabling, and manual trigger (`systemctl start bitcoin-data.service`).
  - Journald verification commands (`journalctl -u bitcoin-data.service -n 50`).
  - Failure injection and rollback procedure.
- **AC-4**: Zero regressions across all 185 existing tests from Phase 1A, 1B, 2, and 3.
- **AC-5**: Quality gates pass cleanly: `pytest`, `ruff check`, `ruff format --check`, `mypy src`.
