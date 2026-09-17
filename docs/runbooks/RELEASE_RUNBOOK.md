# Release & Deployment Runbook

**System:** Bitcoin Data Engineering Platform  
**Target Version:** `v0.1.0`  
**Role / Owner:** Engineering Team  
**Review Cadence:** Every Major / Minor Release  

---

## 1. Overview & Release Philosophy

This runbook defines the Standard Operating Procedure (SOP) for cutting, verifying, deploying, and rolling back releases of the Bitcoin Data Platform.

### Core Principles
1. **Deterministic Artifacts:** Production executes verifiable Python wheels (`.whl`) built from tagged Git commits and pinned dependencies (`requirements.lock`).
2. **Zero Code Edits in Production:** No code is modified or patched live on production hosts. All changes originate in version-controlled branches and pass through CI.
3. **Safe, Zero-Downtime Deployment:** Batch pipeline timers are paused during upgrades to eliminate race conditions between deployment and scheduled execution.
4. **Tested Rollback Path:** Every release maintains a proven rollback mechanism to the previous release artifact.

---

## 2. Pre-Release Checklist & Quality Gates

Execute these steps on a clean clone or designated build workstation prior to release tagging.

### Step 2.1: Verify Clean Working Tree
```bash
git checkout main
git pull origin main
git status
# Must output: "nothing to commit, working tree clean"
```

### Step 2.2: Run Full Local Quality Gates
```bash
source .venv/bin/activate
make check
```
All checks must pass:
- Formatting: `ruff format --check .` (0 errors)
- Linting: `ruff check .` (0 errors)
- Type checking: `mypy src` (0 errors)
- Test suite: `pytest tests/ -v -m "not integration"` (all tests passing, 0 failures)

### Step 2.3: Run Security Dependency Audit
```bash
make audit
```
Verifies that no known CVE vulnerabilities exist in production or development dependencies.

### Step 2.4: Build Distribution Packages
```bash
make clean-dist
make build
```
Confirm that `dist/` contains both artifacts:
- `dist/bitcoin_data_platform-0.1.0-py3-none-any.whl`
- `dist/bitcoin_data_platform-0.1.0.tar.gz`

### Step 2.5: Clean-Room Virtualenv Verification
Simulate an isolated, fresh deployment to verify that package metadata and entrypoints resolve cleanly without local development artifacts:

```bash
# Create and activate an ephemeral virtualenv in /tmp
python3 -m venv /tmp/test-release-venv
source /tmp/test-release-venv/bin/activate

# Install from locked production dependencies and built wheel
pip install --upgrade pip
pip install -r requirements.lock
pip install --no-deps dist/bitcoin_data_platform-0.1.0-py3-none-any.whl

# Verify CLI entrypoint resolves and outputs expected metadata
bitcoin-data --help
python -c "import bitcoin_data_platform; print(bitcoin_data_platform.__name__)"

# Deactivate and remove test virtualenv
deactivate
rm -rf /tmp/test-release-venv
```

---

## 3. Git Release Tagging

Follow Semantic Versioning (`vMAJOR.MINOR.PATCH`).

### Step 3.1: Verify Version in `pyproject.toml`
Ensure `version = "0.1.0"` in `pyproject.toml` matches the intended release tag.

### Step 3.2: Create Signed/Annotated Git Tag
```bash
git tag -a v0.1.0 -m "Release v0.1.0: Reproducible delivery, CI/CD pipelines, and packaging"
```

### Step 3.3: Push Tag to Remote Repository
```bash
git push origin v0.1.0
```

GitHub Actions will automatically trigger CI and security verification against the tagged commit.

---

## 4. Production Deployment Procedure (Host VPS)

The production service executes on the target VPS under systemd management.

- **Application Directory:** `/srv/apps/services/bitcoin-data-platform`
- **Releases Archive:** `/srv/apps/releases`
- **Persistent Data Store:** `/srv/data/bitcoin-data-platform`
- **Service User:** `bitcoin-data:bitcoin-data` (UID 1001)

### Step 4.1: Pre-Deployment Health & Lock Inspection
Connect to the production VPS and verify the platform state:

```bash
sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data status --format text
sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data status --check
```
**Constraint:** If `is_locked: true` or a batch run is currently active, wait until the batch concludes (maximum ~2 minutes) before proceeding.

### Step 4.2: Temporarily Pause the Scheduler Timer
To prevent an hourly execution from firing mid-deployment, stop the systemd timer:

```bash
sudo systemctl stop bitcoin-data.timer
```

### Step 4.3: Stage the Release Artifact
Archive the wheel artifact in the releases directory:

```bash
sudo mkdir -p /srv/apps/releases
sudo cp dist/bitcoin_data_platform-0.1.0-py3-none-any.whl /srv/apps/releases/
sudo chown -R bitcoin-data:bitcoin-data /srv/apps/releases
```

### Step 4.4: Upgrade the Production Virtual Environment
Install the verified wheel artifact into the production environment:

```bash
sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/pip install \
  --no-deps --upgrade /srv/apps/releases/bitcoin_data_platform-0.1.0-py3-none-any.whl
```

### Step 4.5: Post-Deployment Smoke Test
Run operational checks using the production service account:

```bash
# Verify CLI entrypoint and version
sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data --help

# Run platform status check
sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data status --check
```

### Step 4.6: Re-Enable Scheduler Timer
Restart the hourly systemd timer:

```bash
sudo systemctl start bitcoin-data.timer
sudo systemctl status bitcoin-data.timer
sudo systemctl list-timers bitcoin-data.timer
```

Confirm `bitcoin-data.timer` shows the next scheduled run at `*:10:00 UTC`.

---

## 5. Rollback Procedure

If the upgraded release fails smoke tests, crashes during execution, or exhibits data anomalies:

### Step 5.1: Stop Scheduler Timer Immediately
```bash
sudo systemctl stop bitcoin-data.timer
```

### Step 5.2: Identify Previous Good Release Artifact
Locate the previous stable wheel in `/srv/apps/releases/`:
```bash
ls -lt /srv/apps/releases/
```

### Step 5.3: Reinstall Previous Wheel
```bash
PREV_WHEEL="/srv/apps/releases/bitcoin_data_platform-0.0.9-py3-none-any.whl"

sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/pip install \
  --no-deps --force-reinstall "$PREV_WHEEL"
```

### Step 5.4: Clear Stale Locks If Present
If the aborted run left an error lock:
```bash
sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data repair \
  --raw-dir /srv/data/bitcoin-data-platform/raw \
  --curated-dir /srv/data/bitcoin-data-platform/curated \
  --db-path /srv/data/bitcoin-data-platform/state/platform.duckdb \
  --force
```

### Step 5.5: Verify Healthy State and Restart Timer
```bash
sudo -u bitcoin-data /srv/apps/services/bitcoin-data-platform/.venv/bin/bitcoin-data status --check
sudo systemctl start bitcoin-data.timer
```

---

## 6. Container Packaging Verification

To run and verify the container deployment artifact locally:

```bash
# Build container image
docker build -t bitcoin-data-platform:0.1.0 .

# Execute status check in container with mounted volume
docker run --rm \
  -v /srv/data/bitcoin-data-platform:/srv/data/bitcoin-data-platform:ro \
  bitcoin-data-platform:0.1.0 status --format text
```
