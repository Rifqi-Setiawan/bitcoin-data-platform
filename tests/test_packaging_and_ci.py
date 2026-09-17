"""Automated unit tests for Phase 6 CI/CD workflows, packaging, and release delivery."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _read_file_lines(path: Path) -> list[str]:
    assert path.is_file(), f"Expected file does not exist: {path}"
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]


class TestGitHubWorkflows:
    """Tests validating GitHub Actions CI and Security workflows."""

    def test_workflow_files_exist(self) -> None:
        root = _repo_root()
        ci_path = root / ".github" / "workflows" / "ci.yml"
        sec_path = root / ".github" / "workflows" / "security.yml"

        assert ci_path.is_file(), f"CI workflow missing at {ci_path}"
        assert sec_path.is_file(), f"Security workflow missing at {sec_path}"
        assert ci_path.stat().st_size > 0, "CI workflow file is empty"
        assert sec_path.stat().st_size > 0, "Security workflow file is empty"

    def test_ci_workflow_triggers(self) -> None:
        ci_path = _repo_root() / ".github" / "workflows" / "ci.yml"
        content = ci_path.read_text(encoding="utf-8")

        assert "push:" in content, "CI missing push trigger"
        assert "pull_request:" in content, "CI missing pull_request trigger"
        assert "main" in content, "CI must target the main branch"

    def test_ci_workflow_matrix_and_environment(self) -> None:
        ci_path = _repo_root() / ".github" / "workflows" / "ci.yml"
        content = ci_path.read_text(encoding="utf-8")

        assert "runs-on: ubuntu-latest" in content, "CI must run on ubuntu-latest"
        assert '"3.12"' in content or "'3.12'" in content or "3.12" in content, (
            "CI matrix must include Python 3.12"
        )

    def test_ci_workflow_action_versions(self) -> None:
        ci_path = _repo_root() / ".github" / "workflows" / "ci.yml"
        content = ci_path.read_text(encoding="utf-8")

        assert "actions/checkout@v4" in content, "CI should use actions/checkout@v4"
        assert "actions/setup-python@v5" in content, "CI should use actions/setup-python@v5"

    def test_ci_workflow_quality_gate_steps(self) -> None:
        ci_path = _repo_root() / ".github" / "workflows" / "ci.yml"
        content = ci_path.read_text(encoding="utf-8")

        assert "ruff check ." in content, "CI must run ruff check"
        assert "ruff format --check ." in content, "CI must run ruff format check"
        assert "mypy src" in content, "CI must run mypy"
        assert "pytest tests/" in content, "CI must run pytest"
        assert "not integration" in content or '"not integration"' in content, (
            "CI must deselect integration tests"
        )
        assert "python -m build" in content, "CI must verify package build"

    def test_security_workflow_triggers(self) -> None:
        sec_path = _repo_root() / ".github" / "workflows" / "security.yml"
        content = sec_path.read_text(encoding="utf-8")

        assert "push:" in content, "Security workflow missing push trigger"
        assert "pull_request:" in content, "Security workflow missing pull_request trigger"
        assert "schedule:" in content, "Security workflow missing schedule trigger"
        assert "cron:" in content, "Security workflow missing cron schedule"

    def test_security_workflow_audit_steps(self) -> None:
        sec_path = _repo_root() / ".github" / "workflows" / "security.yml"
        content = sec_path.read_text(encoding="utf-8")

        assert "actions/checkout@v4" in content, "Security workflow should use actions/checkout@v4"
        assert "actions/setup-python@v5" in content, (
            "Security workflow should use actions/setup-python@v5"
        )
        assert "pip install pip-audit" in content, "Security workflow must install pip-audit"
        assert "pip-audit" in content, "Security workflow must execute pip-audit"

    def test_workflows_syntax_conformance(self) -> None:
        for fname in ["ci.yml", "security.yml"]:
            path = _repo_root() / ".github" / "workflows" / fname
            lines = path.read_text(encoding="utf-8").splitlines()

            for idx, line in enumerate(lines, 1):
                if line.strip().startswith("#") or not line.strip():
                    continue
                indent = len(line) - len(line.lstrip())
                assert indent % 2 == 0, f"{fname}:{idx} Indentation is not a multiple of 2 spaces"
                assert not line.startswith("\t"), (
                    f"{fname}:{idx} Tab character used instead of spaces"
                )


class TestDockerfilePackaging:
    """Tests validating multi-stage Dockerfile and .dockerignore."""

    def test_dockerfile_exists(self) -> None:
        dockerfile = _repo_root() / "Dockerfile"
        assert dockerfile.is_file(), f"Dockerfile not found at {dockerfile}"
        assert dockerfile.stat().st_size > 0, "Dockerfile is empty"

    def test_dockerfile_multistage_stages(self) -> None:
        dockerfile = _repo_root() / "Dockerfile"
        content = dockerfile.read_text(encoding="utf-8")

        builder_match = re.search(
            r"FROM\s+python:3\.12-slim\s+AS\s+builder", content, re.IGNORECASE
        )
        runtime_match = re.search(
            r"FROM\s+python:3\.12-slim\s+AS\s+runtime", content, re.IGNORECASE
        )

        assert builder_match is not None, "Dockerfile missing python:3.12-slim AS builder stage"
        assert runtime_match is not None, "Dockerfile missing python:3.12-slim AS runtime stage"

    def test_dockerfile_wheel_build_command(self) -> None:
        dockerfile = _repo_root() / "Dockerfile"
        content = dockerfile.read_text(encoding="utf-8")

        assert "python -m build --wheel" in content, "Builder stage must build wheels"

    def test_dockerfile_non_root_system_user(self) -> None:
        dockerfile = _repo_root() / "Dockerfile"
        content = dockerfile.read_text(encoding="utf-8")

        assert "groupadd -g 1001 bitcoin-data" in content, "Group bitcoin-data (GID 1001) required"
        assert "useradd -u 1001" in content, "User bitcoin-data (UID 1001) required"
        assert "bitcoin-data" in content
        assert "/usr/sbin/nologin" in content, "Must specify non-login shell"
        assert "USER bitcoin-data" in content, "Must switch to non-root USER bitcoin-data"

    def test_dockerfile_volume_and_entrypoint(self) -> None:
        dockerfile = _repo_root() / "Dockerfile"
        content = dockerfile.read_text(encoding="utf-8")

        assert 'VOLUME ["/srv/data/bitcoin-data-platform"]' in content or (
            "VOLUME /srv/data/bitcoin-data-platform" in content
        ), "Persistent volume mount point /srv/data/bitcoin-data-platform required"

        assert 'ENTRYPOINT ["bitcoin-data"]' in content, "ENTRYPOINT must be bitcoin-data CLI"
        assert 'CMD ["status", "--format", "text"]' in content, (
            "CMD default must be status --format text"
        )

    def test_dockerignore_entries(self) -> None:
        dockerignore = _repo_root() / ".dockerignore"
        assert dockerignore.is_file(), ".dockerignore missing"
        entries = _read_file_lines(dockerignore)

        required_patterns = [
            ".git",
            ".venv",
            "__pycache__",
            "*.pyc",
            "data/",
            "*.duckdb",
            "dist/",
            "build/",
        ]
        for pattern in required_patterns:
            assert pattern in entries, f"Pattern {pattern} must be excluded in .dockerignore"


class TestRequirementsLock:
    """Tests validating production requirements.lock."""

    def test_lockfile_exists(self) -> None:
        lockfile = _repo_root() / "requirements.lock"
        assert lockfile.is_file(), f"requirements.lock not found at {lockfile}"
        assert lockfile.stat().st_size > 0, "requirements.lock is empty"

    def test_lockfile_all_versions_pinned(self) -> None:
        lockfile = _repo_root() / "requirements.lock"
        lines = _read_file_lines(lockfile)

        for line in lines:
            assert "==" in line, f"Requirement '{line}' must be pinned with exact '=='"
            pkg, ver = line.split("==", 1)
            assert pkg.strip() != "", f"Missing package name in line: {line}"
            assert ver.strip() != "", f"Missing package version in line: {line}"

    def test_direct_runtime_dependencies_pinned(self) -> None:
        lockfile = _repo_root() / "requirements.lock"
        lines = _read_file_lines(lockfile)

        req_map: dict[str, str] = {}
        for line in lines:
            pkg, ver = line.split("==", 1)
            req_map[pkg.strip().lower()] = ver.strip()

        assert "duckdb" in req_map, "duckdb missing from requirements.lock"
        assert "pyarrow" in req_map, "pyarrow missing from requirements.lock"
        assert "httpx" in req_map, "httpx missing from requirements.lock"

        # Check minimum version constraints matching pyproject.toml
        from packaging.version import Version

        assert Version(req_map["duckdb"]) >= Version("1.1.0")
        assert Version(req_map["pyarrow"]) >= Version("18.0.0")
        assert Version(req_map["httpx"]) >= Version("0.28.0")

    def test_transitive_runtime_dependencies_pinned(self) -> None:
        lockfile = _repo_root() / "requirements.lock"
        lines = _read_file_lines(lockfile)
        req_map = {
            line.split("==")[0].strip().lower(): line.split("==")[1].strip() for line in lines
        }

        expected_transitive = ["anyio", "certifi", "h11", "httpcore", "idna", "typing_extensions"]
        for pkg in expected_transitive:
            assert pkg in req_map, f"Transitive dependency '{pkg}' missing from requirements.lock"


class TestPackageMetadataAndMakefile:
    """Tests validating pyproject.toml package metadata and Makefile targets."""

    def test_pyproject_metadata(self) -> None:
        pyproject_path = _repo_root() / "pyproject.toml"
        assert pyproject_path.is_file(), "pyproject.toml missing"

        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)

        assert data["build-system"]["build-backend"] == "hatchling.build"
        assert data["project"]["name"] == "bitcoin-data-platform"
        assert data["project"]["version"] == "0.1.0"
        assert data["project"]["requires-python"] == ">=3.12"
        assert "bitcoin-data" in data["project"]["scripts"]
        assert data["project"]["scripts"]["bitcoin-data"] == "bitcoin_data_platform.cli:main"

    def test_makefile_targets_exist(self) -> None:
        makefile = _repo_root() / "Makefile"
        assert makefile.is_file(), "Makefile missing"
        content = makefile.read_text(encoding="utf-8")

        assert "build:" in content, "Makefile must have 'build' target"
        assert "audit:" in content, "Makefile must have 'audit' target"
        assert "clean-dist:" in content, "Makefile must have 'clean-dist' target"
        assert "python -m build" in content or "$(PYTHON) -m build" in content, (
            "Makefile build target must invoke python -m build"
        )
