"""POSIX file-locking supervisor with PID liveness verification for pipeline cadences."""

from __future__ import annotations

import contextlib
import json
import logging
import os
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.pipeline.models import PipelineCadence

logger = logging.getLogger(__name__)


class PipelineLockError(Exception):
    """Base exception for pipeline locking failures."""


class ConcurrentRunLockError(PipelineLockError):
    """Raised when an active concurrent run is detected (maps to Exit Code 6)."""

    def __init__(self, cadence: PipelineCadence, pid: int, lock_path: Path) -> None:
        msg = f"CONCURRENT_RUN_LOCK: {cadence.value} running (PID {pid}, lock: {lock_path})"
        super().__init__(msg)
        self.cadence = cadence
        self.pid = pid
        self.lock_path = lock_path


class LockManager:
    """Manages POSIX run locks with PID liveness verification and stale lock self-healing."""

    def __init__(self, lock_dir: Path | str = "./data/state/locks") -> None:
        self.lock_dir = Path(lock_dir)

    def _get_lock_path(self, cadence: PipelineCadence) -> Path:
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        return self.lock_dir / f"pipeline_{cadence.value}.lock"

    def _get_writer_lock_path(self) -> Path:
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        return self.lock_dir / "platform_duckdb_writer.lock"

    @staticmethod
    def _is_pid_alive(pid: int) -> bool:
        """Check if process with given PID is actively running."""
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Process exists but owned by another user
            return True
        except OSError:
            return False

    def _is_path_locked(self, lock_path: Path) -> bool:
        if not lock_path.exists():
            return False

        try:
            content = lock_path.read_text(encoding="utf-8").strip()
            data = json.loads(content)
            pid = int(data.get("pid", -1))
        except Exception:
            # Corrupted lockfile - consider stale and clear
            logger.warning("Removing corrupted lockfile: %s", lock_path)
            with contextlib.suppress(Exception):
                lock_path.unlink()
            return False

        if self._is_pid_alive(pid):
            return True

        # Process is dead -> stale lock
        logger.warning(
            "Stale lock detected at %s (dead PID %d). Auto-healing stale lock.",
            lock_path.name,
            pid,
        )
        with contextlib.suppress(Exception):
            lock_path.unlink()
        return False

    @staticmethod
    def _read_lock_pid(lock_path: Path) -> int:
        try:
            content = lock_path.read_text(encoding="utf-8").strip()
            data = json.loads(content)
            return int(data.get("pid", -1))
        except Exception:
            return -1

    def _write_lock_atomic(self, lock_path: Path, payload: dict[str, Any]) -> None:
        tmp_path = lock_path.with_suffix(f".{os.getpid()}.tmp")
        try:
            tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp_path.replace(lock_path)
        except Exception as exc:
            with contextlib.suppress(Exception):
                if tmp_path.exists():
                    tmp_path.unlink()
            raise PipelineLockError(f"Failed creating lockfile {lock_path}: {exc}") from exc

    def is_locked(self, cadence: PipelineCadence) -> bool:
        """Check whether a cadence lock is active, automatically clearing stale locks."""
        return self._is_path_locked(self._get_lock_path(cadence))

    @contextmanager
    def acquire(self, cadence: PipelineCadence) -> Generator[Path, None, None]:
        """Acquire an exclusive cadence lock and global database writer lock."""
        writer_lock = self._get_writer_lock_path()
        cadence_lock = self._get_lock_path(cadence)

        # 1. Check global writer lock first
        if self._is_path_locked(writer_lock):
            pid = self._read_lock_pid(writer_lock)
            raise ConcurrentRunLockError(cadence=cadence, pid=pid, lock_path=writer_lock)

        # 2. Check cadence lock
        if self.is_locked(cadence):
            pid = self._read_lock_pid(cadence_lock)
            raise ConcurrentRunLockError(cadence=cadence, pid=pid, lock_path=cadence_lock)

        current_pid = os.getpid()
        payload = {
            "cadence": cadence.value,
            "pid": current_pid,
            "acquired_at_utc": datetime.now(UTC).isoformat(),
        }

        # Atomic writes for cadence and global writer locks
        self._write_lock_atomic(cadence_lock, payload)
        self._write_lock_atomic(writer_lock, payload)

        try:
            yield cadence_lock
        finally:
            with contextlib.suppress(Exception):
                if cadence_lock.exists():
                    cadence_lock.unlink()
            with contextlib.suppress(Exception):
                if writer_lock.exists():
                    writer_lock.unlink()

    def get_status(self) -> dict[str, Any]:
        """Compile status of all pipeline tier locks."""
        status: dict[str, Any] = {}
        for cadence in PipelineCadence:
            locked = self.is_locked(cadence)
            path = self._get_lock_path(cadence)
            info: dict[str, Any] = {"locked": locked, "lock_file": str(path)}
            if locked and path.exists():
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    info["active_pid"] = data.get("pid")
                    info["acquired_at_utc"] = data.get("acquired_at_utc")
                except Exception:
                    pass
            status[cadence.value] = info
        return status
