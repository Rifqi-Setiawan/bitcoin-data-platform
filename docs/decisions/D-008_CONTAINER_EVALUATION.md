# ADR D-008: Container Deployment vs Native Systemd Evaluation

- **Status:** Accepted
- **Date:** 2026-09-17
- **Author:** Engineering Team
- **Deciders:** Architecture & Infrastructure Working Group
- **Technical Story:** Phase 6 — Reproducible Delivery, Packaging & CI/CD

---

## 1. Context and Problem Statement

The Bitcoin Data Platform runs as a single-host, hourly batch pipeline executing ingestion, data quality validation, Parquet normalization, and DuckDB analytical modeling. In Phase 4, the platform established a single-host production runtime utilizing native Linux `systemd` timers (`bitcoin-data.timer`) and oneshot services (`bitcoin-data.service`) running under a dedicated unprivileged user (`bitcoin-data`, UID 1001) in a locked virtualenv (`/srv/apps/services/bitcoin-data-platform/.venv`).

In Phase 6, we introduce reproducible delivery artifacts, automated CI/CD workflows, and container packaging (`Dockerfile`). The core architectural question is: **Should the production runtime on the single-host VPS migrate to an OCI container runtime (e.g., Docker / Podman), or should the platform retain native systemd execution while using OCI containers exclusively as reproducible packaging and testing artifacts?**

---

## 2. Decision Drivers

1. **Operational Simplicity & Reliability:** Minimize failure domains, external daemon dependencies, and moving parts on a single-host Linux VPS.
2. **Resource Footprint:** Conserve memory and CPU for analytical workloads (DuckDB and Parquet processing) without paying continuous daemon overhead.
3. **Filesystem & Permission Ergonomics:** Ensure seamless, deterministic POSIX permissions for the persistent data store (`/srv/data/bitcoin-data-platform`) across raw, curated, and DuckDB state tiers.
4. **Security & Sandboxing:** Provide strict isolation, preventing privilege escalation and bounding write access to designated data directories.
5. **Observability & Failure Alerting:** Maintain direct, native integration with `systemd-journald` and low-noise automated alerting (`OnFailure=bitcoin-data-failure@%n.service`).
6. **Reproducibility & Portability:** Guarantee that any engineer or CI runner can verify identical packaging and dependencies in a clean environment.

---

## 3. Considered Alternatives

### Alternative 1: Native Virtualenv + systemd (Status Quo)
Deploy Python wheels or locked virtualenvs directly to `/srv/apps/services/bitcoin-data-platform/.venv`, orchestrated via native `systemd` oneshot services and hourly timers.

### Alternative 2: Container-First Production (Docker / Podman)
Package application into an OCI container image and invoke container runs either through Docker daemon / Podman CLI via cron, systemd, or an external container scheduler.

### Alternative 3: Hybrid Architecture (Selected Approach)
Retain native `systemd` as the production runtime engine on the VPS host. Simultaneously, provide a standardized multi-stage `Dockerfile` and `requirements.lock` as reproducible packaging, testing, and CI verification artifacts.

---

## 4. Evaluation & Comparison Matrix

| Evaluation Dimension | Alternative 1: Native systemd | Alternative 2: OCI Container (Docker) | Alternative 3: Hybrid Architecture (Selected) |
| :--- | :--- | :--- | :--- |
| **Daemon Overhead** | **Zero.** Kernel executes native binary via systemd PID 1; no background daemon required. | **Moderate to High.** Requires active `dockerd`/`containerd` daemons consuming 80–150 MB RAM continuously. | **Zero in production.** No container daemon needed on host; container used in CI and local testing. |
| **Startup Latency** | **Sub-millisecond (< 50 ms).** Python interpreter launches directly. | **200–800 ms overhead.** Container namespace setup, cgroup init, and bridge network checks. | **Sub-millisecond** production execution. |
| **Storage & Permissions** | **Direct POSIX.** Native user/group (`bitcoin-data:bitcoin-data`, UID 1001) maps 1:1 to `/srv/data/bitcoin-data-platform`. | **Complex UID Mapping.** Requires rootless podman user namespaces or volume bind mounts with host UID synchronization. | **Direct POSIX** on production host; container uses UID 1001 internally matching host UID. |
| **Security Sandboxing** | **Kernel-native systemd directives:** `ProtectSystem=strict`, `ProtectHome=true`, `PrivateTmp=true`, `NoNewPrivileges=true`. | **OCI namespaces & seccomp:** Strong process isolation; however root daemon (`dockerd`) presents host attack surface. | **Best of both:** Production enjoys systemd strict sandboxing; CI/staging runs in isolated container. |
| **Telemetry & Alerts** | **Seamless.** Native `systemd-journald` log aggregation and atomic `OnFailure` systemd unit alerting. | **Indirect.** Requires Docker log forwarders, custom healthcheck scripts, or wrapper systemd service units. | **Seamless** production alerting and observability. |
| **Clean-Clone Portability** | **Requires host Python 3.12.** Platform engineer must configure Python virtualenv on dev machine. | **Universal.** Single `docker run` works on any OS (Linux, macOS, Windows) without installing Python. | **Universal portability** via Dockerfile + clean wheel distribution for production host. |

---

## 5. Decision Outcome

**We choose Alternative 3: Hybrid Architecture.**

### Production Decision
1. **Retain Native systemd on VPS:** The production runtime remains host-native `systemd` using locked virtual environments (`requirements.lock`) under `/srv/apps/services/bitcoin-data-platform`.
2. **Why:** Eliminates Docker daemon maintenance, avoids socket exposure risks, prevents container filesystem UID drift, and retains native `systemd-journald` structured logging and `OnFailure` alerting.

### Packaging & Delivery Decision
1. **Maintain Production Dockerfile:** The project provides and maintains a multi-stage `Dockerfile` based on `python:3.12-slim` building from clean distribution wheels (`python -m build --wheel`).
2. **Deterministic Lockfile:** All runtime dependencies are pinned with exact versions in `requirements.lock`.
3. **Role of Container:** The container image serves as:
   - A deterministic supply-chain artifact for clean-room testing and containerized CI runners.
   - An instant onboarding vehicle for external engineers to run `bitcoin-data status` or queries without local Python 3.12 toolchain setup.
   - A readiness bridge should the platform ever migrate to distributed orchestration (e.g., Nomad, ECS, or Kubernetes).

---

## 6. Consequences

### Positive Consequences
- **Zero Idle Overhead:** The VPS host consumes zero memory when the pipeline is idle between hourly runs.
- **Robust POSIX File Ownership:** All files written to `/srv/data/bitcoin-data-platform` carry native `bitcoin-data:bitcoin-data` ownership without UID mapping mismatch or docker volume locking.
- **Defense in Depth:** Production leverages systemd's hardened execution environment (`ProtectSystem=strict`, `ReadOnlyPaths=/srv/apps/services/bitcoin-data-platform`, `ReadWritePaths=/srv/data/bitcoin-data-platform`).
- **Reproducible Verification:** The automated test suite validates the `Dockerfile` and `requirements.lock` in CI, ensuring that deployment artifacts remain 100% reproducible.

### Negative Consequences / Trade-offs
- Deployment to the host requires Python virtualenv creation and wheel installation rather than a single `docker pull` command.
- The team maintains two deployment descriptions: systemd unit definitions (`infra/systemd/`) and container configuration (`Dockerfile`).

---

## 7. Re-evaluation Triggers

This decision should be revisited if any of the following conditions occur:
1. **Multi-Node Scaling:** Pipeline moves from single-host VPS to distributed compute clusters (e.g., Kubernetes, AWS ECS, or Nomad).
2. **Non-Python Binary Dependencies:** Addition of complex non-Python system libraries (e.g., CUDA, proprietary database connectors) that justify container layer caching.
3. **Multi-Tenant Deployment:** Hosting multiple isolated instances of the platform on a shared server where kernel namespace virtualization is strictly mandated.
