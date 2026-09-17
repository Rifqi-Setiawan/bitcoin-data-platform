# Deployment & Operations Runbook: Single-Host Orchestration (systemd)

**System:** Bitcoin Data Engineering Platform  
**Phase:** Phase 4 — Single-Host Orchestration  
**Target Platform:** Linux VPS (Ubuntu 22.04/24.04 LTS or Debian 12)  
**Supervisor:** systemd (`service` + `timer`)  
**Service Account:** `bitcoin-data` (unprivileged system user, no login shell)  

---

## 1. Overview & Operational Architecture

Phase 4 operationalizes the deterministic incremental loading pipeline (`bitcoin-data incremental`) as a supervised, sandboxed background service on a single-host Linux VPS.

```text
┌───────────────────────────────────────────────────────────────┐
│ systemd Scheduler (timers.target)                             │
│ Unit: bitcoin-data.timer                                      │
│ Schedule: OnCalendar=*-*-* *:10:00 (RandomizedDelaySec=120)  │
└───────────────────────────────┬───────────────────────────────┘
                                │ Triggers hourly (at :10 UTC)
                                ▼
┌───────────────────────────────────────────────────────────────┐
│ systemd Service (oneshot)                                     │
│ Unit: bitcoin-data.service                                    │
│ User: bitcoin-data (no interactive login, no sudo)           │
│ Sandbox: ProtectSystem=strict, PrivateTmp=true, ReadWritePaths│
└───────────────────────────────┬───────────────────────────────┘
                                │ Executes
                                ▼
┌───────────────────────────────────────────────────────────────┐
│ Application Entrypoint                                        │
│ /srv/apps/services/bitcoin-data-platform/.venv/bin/           │
│   bitcoin-data incremental                                    │
│     --raw-dir /srv/data/bitcoin-data-platform/raw             │
│     --curated-dir /srv/data/bitcoin-data-platform/curated     │
│     --db-path /srv/data/bitcoin-data-platform/state/platform.db│
│     --overlap-hours 48                                        │
└───────────────────────────────┬───────────────────────────────┘
                                │ Logs structured JSON
                                ▼
┌───────────────────────────────────────────────────────────────┐
│ systemd-journald (StandardOutput=journal, StandardError=...)  │
└───────────────────────────────────────────────────────────────┘
```

### Key Operational Characteristics
- **Schedule:** Triggers 10 minutes past every hour (`*:10:00 UTC`), allowing upstream Coinbase candle consolidation after the hourly boundary closes.
- **Jitter:** `RandomizedDelaySec=120` prevents thundering herd against the Coinbase API.
- **Catch-up on Downtime:** `Persistent=true` ensures any missed hourly run due to host reboot or maintenance executes immediately upon system boot.
- **Least Privilege:** Executes as unprivileged user `bitcoin-data` with `ProtectSystem=strict`, `NoNewPrivileges=true`, and write boundaries locked to `/srv/data/bitcoin-data-platform` and `/tmp`.

---

## 2. Prerequisites

1. Linux host with systemd (systemd v245+ recommended).
2. Python 3.12 installed.
3. Virtual environment initialized and dependencies installed at:  
   `/srv/apps/services/bitcoin-data-platform/.venv`
4. Administrative (`sudo`) access on the host for user provisioning and systemd unit installation.

---

## 3. Provisioning Step-by-Step

### Step 1: Provision Dedicated System User & Group

Create a dedicated system account without interactive login capabilities, home directory, or sudo privileges:

```bash
# Create dedicated system group
sudo groupadd --system bitcoin-data

# Create dedicated system user without login shell or home directory
sudo useradd --system \
  --no-create-home \
  --gid bitcoin-data \
  --shell /usr/sbin/nologin \
  --comment "Bitcoin Data Platform Service Account" \
  bitcoin-data

# Verify user creation and shell restriction
id bitcoin-data
# Output: uid=... gid=...(bitcoin-data) groups=...(bitcoin-data)
getent passwd bitcoin-data
# Output: bitcoin-data:x:...:...:Bitcoin Data Platform Service Account:/home/bitcoin-data:/usr/sbin/nologin
```

### Step 2: Provision Filesystem Directories & Permissions

Establish the strict separation of concerns:
- **Application Code (`/srv/apps/services/bitcoin-data-platform`):** Owned by deployment operator; read-only for `bitcoin-data`.
- **Data & State (`/srv/data/bitcoin-data-platform`):** Owned by `bitcoin-data:bitcoin-data`; mode `0750`.

```bash
# Create runtime data directories
sudo mkdir -p /srv/data/bitcoin-data-platform/{raw,curated,state,quarantine,tmp}

# Set ownership to bitcoin-data user and group
sudo chown -R bitcoin-data:bitcoin-data /srv/data/bitcoin-data-platform

# Restrict permissions: owner read/write/execute, group read/execute, others no access
sudo chmod 0750 /srv/data/bitcoin-data-platform
sudo chmod 0750 /srv/data/bitcoin-data-platform/*

# Ensure application code is readable by bitcoin-data
sudo chmod -R o+rX /srv/apps/services/bitcoin-data-platform
```

### Step 3: Configure Optional Environment File

Create the configuration directory and deploy the environment template:

```bash
# Create configuration directory
sudo mkdir -p /etc/bitcoin-data

# Copy environment template
sudo cp infra/systemd/bitcoin-data.env.example /etc/bitcoin-data/bitcoin-data.env

# Secure file permissions (readable by root and bitcoin-data group, not world-readable)
sudo chown root:bitcoin-data /etc/bitcoin-data/bitcoin-data.env
sudo chmod 0640 /etc/bitcoin-data/bitcoin-data.env
```

### Step 4: Install Systemd Units

Deploy unit files to the systemd unit directory:

```bash
# Copy service and timer units
sudo cp infra/systemd/bitcoin-data.service /etc/systemd/system/bitcoin-data.service
sudo cp infra/systemd/bitcoin-data.timer /etc/systemd/system/bitcoin-data.timer

# Set standard unit permissions
sudo chmod 0644 /etc/systemd/system/bitcoin-data.service
sudo chmod 0644 /etc/systemd/system/bitcoin-data.timer

# Reload systemd manager configuration
sudo systemctl daemon-reload
```

---

## 4. Activation & Verification

### Step 5: Enable and Start the Timer

The timer manages scheduled execution of the service. Do **not** enable `bitcoin-data.service` directly on boot (it is a oneshot service triggered by the timer).

```bash
# Enable and start timer immediately
sudo systemctl enable --now bitcoin-data.timer

# Verify timer active state
systemctl status bitcoin-data.timer
```

Expected output:
```text
● bitcoin-data.timer - Hourly Schedule for Bitcoin Data Platform Incremental Ingestion
     Loaded: loaded (/etc/systemd/system/bitcoin-data.timer; enabled; preset: enabled)
     Active: active (waiting) since ...
    Trigger: ... (next run at :10 past the hour)
   Triggers: ● bitcoin-data.service
```

### Step 6: Verify Timer Schedule in Calendar

Confirm that systemd recognizes the next scheduled execution:

```bash
systemctl list-timers --all | grep bitcoin-data
```

Expected output confirms next trigger time with randomized delay window.

### Step 7: Test Manual Service Invocation

Run an initial execution manually to verify end-to-end sandbox, permissions, and CLI execution:

```bash
# Manually trigger one-off service execution
sudo systemctl start bitcoin-data.service

# Check execution exit status
systemctl status bitcoin-data.service
```

Expected output:
```text
○ bitcoin-data.service - Bitcoin Data Engineering Platform - Incremental Ingestion & Promotion
     Loaded: loaded (/etc/systemd/system/bitcoin-data.service; static)
     Active: inactive (dead) since ...
    Process: ... ExecStart=... (code=exited, status=0/SUCCESS)
   Main PID: ... (code=exited, status=0/SUCCESS)
```

### Step 8: Inspect Journald Structured Logs

Check the structured JSON log output generated during execution:

```bash
# View recent logs from the service unit
journalctl -u bitcoin-data.service -n 50 --no-pager

# Stream logs continuously during debugging
journalctl -u bitcoin-data.service -f
```

Example JSON log events emitted to journald:
```json
{"timestamp": "2026-09-17T13:10:05Z", "level": "INFO", "event": "incremental_run_started", "run_id": "...", "overlap_hours": 48}
{"timestamp": "2026-09-17T13:10:08Z", "level": "INFO", "event": "incremental_run_completed", "run_id": "...", "status": "success", "rows_promoted": 48}
```

---

## 5. Security & Sandbox Verification

Verify the systemd security directives offline and online:

```bash
# Verify unit configuration syntax
systemd-analyze verify /etc/systemd/system/bitcoin-data.service /etc/systemd/system/bitcoin-data.timer

# Inspect security exposure score
systemd-analyze security bitcoin-data.service
```

### Enforced Hardening Summary:
| Directive | Value | Purpose |
|---|---|---|
| `User` / `Group` | `bitcoin-data` | Runs without root privileges |
| `ProtectSystem` | `strict` | Entire filesystem hierarchy mounted read-only except `ReadWritePaths` |
| `ProtectHome` | `true` | `/home`, `/root`, `/run/user` inaccessible |
| `NoNewPrivileges` | `true` | Prevents privilege escalation via `setuid`/`setgid` binaries |
| `PrivateTmp` | `true` | Process receives isolated `/tmp` namespace |
| `ProtectKernelTunables` | `true` | `/proc/sys`, `/sys` mounted read-only |
| `ProtectControlGroups` | `true` | Prevents modifying cgroup hierarchies |
| `RestrictSUIDSGID` | `true` | Denies attempts to set SUID/SGID bits |
| `ReadWritePaths` | `/srv/data/bitcoin-data-platform /tmp` | Explicitly whitelisted write destinations |
| `ReadOnlyPaths` | `/srv/apps/services/bitcoin-data-platform` | Code directory guaranteed immutable |
| `MemoryMax` | `1G` | Capped RAM to prevent host starvation |
| `CPUQuota` | `100%` | Capped CPU to 1 core maximum |

---

## 6. Incident Scenarios & Failure Recovery

### Scenario A: Run Lock Conflict (Exit Code 6)

**Symptom:**  
The service fails with `status=6` and journald logs:  
`"event": "run_lock_active", "error": "Another run is currently in progress"`.

**Root Cause:**  
A previous execution crashed abruptly without cleaning up `/srv/data/bitcoin-data-platform/state/.run_lock`, or a long-running manual backfill is still processing.

**Resolution:**
1. Check if another pipeline process is running:
   ```bash
   ps aux | grep bitcoin-data
   ```
2. Check lock status using the CLI:
   ```bash
   sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data status \
     --db-path /srv/data/bitcoin-data-platform/state/platform.duckdb \
     --curated-dir /srv/data/bitcoin-data-platform/curated
   ```
3. If no process is running and the lock is stale (>1 hour old), clear it safely:
   ```bash
   sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data repair \
     --raw-dir /srv/data/bitcoin-data-platform/raw \
     --curated-dir /srv/data/bitcoin-data-platform/curated \
     --db-path /srv/data/bitcoin-data-platform/state/platform.duckdb \
     --force
   ```

---

### Scenario B: Upstream Coinbase Outage (Exit Code 3)

**Symptom:**  
The service exits with `status=3` after retries exhausted:  
`"event": "source_unavailable", "error": "All 3 retry attempts failed"`.

**Impact:**  
The watermark is **not** advanced. Data integrity remains uncompromised.

**Resolution:**
1. Verify outbound connectivity:
   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" https://api.exchange.coinbase.com/products/BTC-USD/candles
   ```
2. Once upstream connectivity recovers, systemd timer will automatically retry on the next hourly schedule, or trigger an immediate run:
   ```bash
   sudo systemctl start bitcoin-data.service
   ```
   The 48-hour rolling overlap will automatically backfill any missed hourly candles.

---

### Scenario C: Filesystem Permission Denial (Exit Code 5)

**Symptom:**  
Service fails with `PermissionError` writing to `/srv/data/bitcoin-data-platform/state` or `raw`.

**Resolution:**
Restore correct ownership and permissions:
```bash
sudo chown -R bitcoin-data:bitcoin-data /srv/data/bitcoin-data-platform
sudo chmod 0750 /srv/data/bitcoin-data-platform
sudo chmod -R u+rwX,g+rX,o-rwx /srv/data/bitcoin-data-platform
sudo systemctl start bitcoin-data.service
```

---

### Scenario D: Host Reboot / Downtime Catch-up

**Behavior:**  
Because `Persistent=true` is set on `bitcoin-data.timer`, systemd records the timestamp of the last successful timer trigger in `/var/lib/systemd/timers/stamp-bitcoin-data.timer`.  
If the VPS was offline during an hourly `:10:00` tick, systemd immediately executes `bitcoin-data.service` as soon as the server boots and reaches `timers.target`.

**Verification:**
After reboot, inspect journald to verify catch-up execution:
```bash
journalctl -u bitcoin-data.service --since "10 minutes ago"
```

---

## 7. Rollback Procedures

If an issue is detected with a newly deployed service unit or application version:

### Fast Pause: Suspend Scheduled Execution
```bash
sudo systemctl stop bitcoin-data.timer
sudo systemctl disable bitcoin-data.timer
```

### Complete Service Rollback
1. Stop any in-progress execution:
   ```bash
   sudo systemctl stop bitcoin-data.service
   ```
2. Revert unit files if modified:
   ```bash
   sudo cp /etc/systemd/system/bitcoin-data.service.bak /etc/systemd/system/bitcoin-data.service
   sudo cp /etc/systemd/system/bitcoin-data.timer.bak /etc/systemd/system/bitcoin-data.timer
   sudo systemctl daemon-reload
   ```
3. If reverting application code, update the symlink or git worktree in `/srv/apps/services/bitcoin-data-platform`.
4. Re-enable timer:
   ```bash
   sudo systemctl enable --now bitcoin-data.timer
   ```
5. Confirm operational state:
   ```bash
   systemctl status bitcoin-data.timer
   ```
