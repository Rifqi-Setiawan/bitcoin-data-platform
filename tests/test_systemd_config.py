"""Unit tests for Phase 4 systemd service and timer configuration."""

from __future__ import annotations

import configparser
import shutil
import subprocess
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _parse_ini(path: Path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    with open(path, encoding="utf-8") as f:
        parser.read_file(f)
    return parser


class TestSystemdServiceConfig:
    """Tests for infra/systemd/bitcoin-data.service."""

    def test_service_file_exists(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        assert service_path.is_file(), f"Service file not found at {service_path}"
        assert service_path.stat().st_size > 0, "Service file is empty"

    def test_service_ini_sections(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        assert "Unit" in parser.sections()
        assert "Service" in parser.sections()

    def test_service_unit_metadata(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        unit = parser["Unit"]
        assert "Description" in unit
        assert "Documentation" in unit
        assert unit.get("After") == "network-online.target"
        assert unit.get("Wants") == "network-online.target"

    def test_service_type_oneshot(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        service = parser["Service"]
        assert service.get("Type") == "oneshot"

    def test_service_user_and_group(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        service = parser["Service"]
        assert service.get("User") == "bitcoin-data"
        assert service.get("Group") == "bitcoin-data"

    def test_service_security_hardening(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        service = parser["Service"]

        assert service.get("ProtectSystem") == "strict"
        assert service.get("ProtectHome") == "true"
        assert service.get("NoNewPrivileges") == "true"
        assert service.get("PrivateTmp") == "true"
        assert service.get("ProtectKernelTunables") == "true"
        assert service.get("ProtectControlGroups") == "true"
        assert service.get("RestrictSUIDSGID") == "true"

    def test_service_sandbox_paths(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        service = parser["Service"]

        rw_paths = service.get("ReadWritePaths", "").split()
        assert "/srv/data/bitcoin-data-platform" in rw_paths
        assert "/tmp" in rw_paths

        ro_paths = service.get("ReadOnlyPaths", "").split()
        assert "/srv/apps/services/bitcoin-data-platform" in ro_paths

    def test_service_resource_limits(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        service = parser["Service"]

        assert service.get("MemoryMax") == "1G"
        assert service.get("CPUQuota") == "100%"

    def test_service_logging_directives(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        service = parser["Service"]

        assert service.get("StandardOutput") == "journal"
        assert service.get("StandardError") == "journal"

    def test_service_environment_file(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        service = parser["Service"]

        assert service.get("EnvironmentFile") == "-/etc/bitcoin-data/bitcoin-data.env"

    def test_service_exec_start_command(self) -> None:
        service_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.service"
        parser = _parse_ini(service_path)
        service = parser["Service"]
        exec_start = service.get("ExecStart", "")

        assert "bitcoin-data incremental" in exec_start
        assert "--raw-dir /srv/data/bitcoin-data-platform/raw" in exec_start
        assert "--curated-dir /srv/data/bitcoin-data-platform/curated" in exec_start
        assert "--db-path /srv/data/bitcoin-data-platform/state/platform.duckdb" in exec_start
        assert "--overlap-hours 48" in exec_start


class TestSystemdTimerConfig:
    """Tests for infra/systemd/bitcoin-data.timer."""

    def test_timer_file_exists(self) -> None:
        timer_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.timer"
        assert timer_path.is_file(), f"Timer file not found at {timer_path}"
        assert timer_path.stat().st_size > 0, "Timer file is empty"

    def test_timer_ini_sections(self) -> None:
        timer_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.timer"
        parser = _parse_ini(timer_path)
        assert "Unit" in parser.sections()
        assert "Timer" in parser.sections()
        assert "Install" in parser.sections()

    def test_timer_schedule(self) -> None:
        timer_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.timer"
        parser = _parse_ini(timer_path)
        timer = parser["Timer"]
        assert timer.get("OnCalendar") == "*-*-* *:10:00"

    def test_timer_persistence(self) -> None:
        timer_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.timer"
        parser = _parse_ini(timer_path)
        timer = parser["Timer"]
        assert timer.get("Persistent") == "true"

    def test_timer_jitter_range(self) -> None:
        timer_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.timer"
        parser = _parse_ini(timer_path)
        timer = parser["Timer"]
        jitter_str = timer.get("RandomizedDelaySec", "0")
        jitter = int(jitter_str)
        assert 30 <= jitter <= 300, f"Expected jitter between 30 and 300s, got {jitter}"

    def test_timer_unit_reference(self) -> None:
        timer_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.timer"
        parser = _parse_ini(timer_path)
        timer = parser["Timer"]
        assert timer.get("Unit") == "bitcoin-data.service"

    def test_timer_install_target(self) -> None:
        timer_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.timer"
        parser = _parse_ini(timer_path)
        install = parser["Install"]
        assert install.get("WantedBy") == "timers.target"

    def test_systemd_calendar_validity(self) -> None:
        """If systemd-analyze is installed on the host, verify the calendar expression directly."""
        systemd_analyze = shutil.which("systemd-analyze")
        if not systemd_analyze:
            return

        timer_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.timer"
        parser = _parse_ini(timer_path)
        calendar = parser["Timer"]["OnCalendar"]

        result = subprocess.run(
            [systemd_analyze, "calendar", calendar],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, (
            f"systemd-analyze calendar rejected {calendar}: {result.stderr}"
        )
        assert "Normalized form:" in result.stdout


class TestSystemdEnvironmentConfig:
    """Tests for infra/systemd/bitcoin-data.env.example."""

    def test_env_example_exists(self) -> None:
        env_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.env.example"
        assert env_path.is_file(), f"Env example file not found at {env_path}"
        assert env_path.stat().st_size > 0, "Env example file is empty"

    def test_env_example_content(self) -> None:
        env_path = _repo_root() / "infra" / "systemd" / "bitcoin-data.env.example"
        content = env_path.read_text(encoding="utf-8")

        assert "PYTHONUNBUFFERED" in content
        assert "BITCOIN_DATA_LOG_LEVEL" in content
        assert "BITCOIN_DATA_OVERRIDE_NOW_UTC" in content
