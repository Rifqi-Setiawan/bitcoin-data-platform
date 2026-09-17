# Phase 6 Implementation Specification — CI/CD Pipeline, Security Scans & Reproducible Packaging

Status: approved for implementation
Parent phase: Phase 6 — Reproducible delivery and CI/CD
Task ID: `P6-cicd-delivery-packaging`
Recommended branch: `feature/P6-cicd-delivery-packaging`
Owner: Engineering Team
Verification: Automated Test Suite & Peer Review

## 1. Objective

Establish a professional continuous integration, continuous delivery (CI/CD), security vulnerability
scanning, and reproducible packaging foundation for the Bitcoin Data Platform. Ensure any engineer
can perform a clean-clone setup, build verifiable distribution artifacts (`wheel` and `sdist`),
validate dependency supply-chain security, and execute deterministic deployments without interactive
editing on production hosts.

## 2. User Stories

### Automated Continuous Integration
As a core contributor, every commit pushed to `main` and every Pull Request triggers an automated
GitHub Actions matrix workflow verifying formatting (`ruff format`), linting (`ruff check`),
strict static type compliance (`mypy`), and the full offline test suite across Python 3.12.

### Automated Security & Supply-Chain Scanning
As a security-conscious engineer, a dedicated security scanning workflow runs on every push and weekly
schedule to audit dependencies against known CVE databases (`pip-audit`) and verify that no secret,
API key, private token, or internal infrastructure identifier is ever committed to the repository.

### Reproducible Packaging & Clean-Clone Parity
As a deployment engineer, running `make build` produces deterministic, verifiable Python distribution
packages (`.whl` and `.tar.gz`) from locked dependencies, and an OCI `Dockerfile` provides runtime
environment parity evaluation against the native `systemd` host service.

## 3. Scope

### In Scope
1. **GitHub Actions Workflows (`.github/workflows/`)**:
   - `ci.yml`: Automated CI pipeline running lint, format, typecheck, test suite, and package build.
   - `security.yml`: Dependency vulnerability audit (`pip-audit`) and secret scan.
2. **Dependency Locking & Reproducible Packaging**:
   - Pinned production lockfile `requirements.lock` with deterministic dependencies and hashes.
   - Build system validation via `hatchling` producing wheel and sdist artifacts.
   - Enhanced `Makefile` with targets: `build`, `audit`, `lock`, `clean`.
3. **Container Parity Evaluation**:
   - Production-grade multi-stage `Dockerfile` (Python 3.12-slim, non-root user `bitcoin-data`, `/srv/data/bitcoin-data-platform` volume mount).
   - `.dockerignore` file excluding runtime data, virtualenvs, cache, and secrets.
   - Architectural decision record `docs/decisions/D-008_CONTAINER_EVALUATION.md` comparing container vs native systemd on a single-host VPS.
4. **Release & Deployment Runbook**:
   - `docs/runbooks/RELEASE_RUNBOOK.md` covering release tagging (`v0.1.0`), package verification, zero-downtime deployment, and rollback rehearsal.
5. **Automated Testing**:
   - `tests/test_packaging_and_ci.py` validating workflow YAML syntax, action versions, Dockerfile configuration, lockfile consistency, and package metadata.
6. **Documentation**:
   - Update `README.md` with CI badge placeholder, packaging instructions, and release references.

### Out of Scope
- Automatic deployment to external public PyPI index (internal distribution only for V1).
- Multi-architecture Kubernetes clusters or Helm charts (incompatible with single-host design).

## 4. Technical Specifications

### 4.1 GitHub Actions CI Workflow (`.github/workflows/ci.yml`)
```yaml
name: CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    name: Lint, Type Check & Test
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.12"]

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
          cache: "pip"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements-dev.txt
          pip install -e .

      - name: Lint with ruff
        run: python -m ruff check .

      - name: Check code formatting with ruff
        run: python -m ruff format --check .

      - name: Static type checking with mypy
        run: python -m mypy src

      - name: Run offline test suite
        run: python -m pytest tests/ -v -m "not integration"

      - name: Build distribution packages
        run: |
          pip install build
          python -m build
```

### 4.2 Security Workflow (`.github/workflows/security.yml`)
```yaml
name: Security & Vulnerability Scan

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: "0 4 * * 1" # Weekly Monday 04:00 UTC

jobs:
  audit:
    name: Dependency Audit & Secret Scan
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install pip-audit
        run: pip install pip-audit

      - name: Audit dependencies for vulnerabilities
        run: pip-audit
```

### 4.3 Production Dockerfile (`Dockerfile`)
```dockerfile
# Multi-stage reproducible container build
FROM python:3.12-slim AS builder

WORKDIR /build
RUN pip install --no-cache-dir build
COPY . /build/
RUN python -m build --wheel

FROM python:3.12-slim AS runtime

# Create isolated non-login system user matching VPS specification
RUN groupadd -g 1001 bitcoin-data && \
    useradd -u 1001 -g bitcoin-data -s /usr/sbin/nologin -M bitcoin-data

WORKDIR /app
COPY --from=builder /build/dist/*.whl /app/
RUN pip install --no-cache-dir /app/*.whl && rm /app/*.whl

# Prepare persistent data mount point
RUN mkdir -p /srv/data/bitcoin-data-platform && \
    chown -R bitcoin-data:bitcoin-data /srv/data/bitcoin-data-platform

USER bitcoin-data
VOLUME ["/srv/data/bitcoin-data-platform"]
ENTRYPOINT ["bitcoin-data"]
CMD ["status", "--format", "text"]
```

## 5. Acceptance Criteria

- **AC-1**: `.github/workflows/ci.yml` and `security.yml` exist, parse as valid GitHub Actions workflows, and pin stable action versions.
- **AC-2**: Production `requirements.lock` file is present and pins all runtime dependencies with exact versions.
- **AC-3**: `python -m build` successfully produces `.whl` and `.tar.gz` packages containing all source modules, CLI entrypoints, and licenses.
- **AC-4**: `Dockerfile` conforms to multi-stage build best practices, enforces non-root execution (`bitcoin-data`), and respects the `/srv/data/bitcoin-data-platform` volume boundary.
- **AC-5**: `docs/decisions/D-008_CONTAINER_EVALUATION.md` provides an objective operational comparison between container deployment and host systemd.
- **AC-6**: `docs/runbooks/RELEASE_RUNBOOK.md` establishes clear SOP for release tagging, artifact verification, deployment, and rollback.
- **AC-7**: All existing 235 tests pass, and new automated tests in `tests/test_packaging_and_ci.py` pass cleanly (total > 245 tests).
- **AC-8**: Code quality gates pass cleanly (`ruff check`, `ruff format --check`, `mypy src`).
- **AC-9**: Clean commit with zero AI/Hermes references, pushed and merged to `main`.
