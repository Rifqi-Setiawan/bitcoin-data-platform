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

    def is_locked(self, cadence: PipelineCadence) -> bool:
        """Check whether a cadence lock is active, automatically clearing stale locks."""
        lock_path = self._get_lock_path(cadence)
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
            "Stale lock detected for %s pipeline (dead PID %d). Auto-healing stale lock.",
            cadence.value,
            pid,
        )
        with contextlib.suppress(Exception):
            lock_path.unlink()
        return False

    @contextmanager
    def acquire(self, cadence: PipelineCadence) -> Generator[Path, None, None]:
        """Acquire an exclusive cadence lock, raising ConcurrentRunLockError if busy."""
        lock_path = self._get_lock_path(cadence)

        if self.is_locked(cadence):
            try:
                data = json.loads(lock_path.read_text(encoding="utf-8"))
                pid = int(data.get("pid", -1))
            except Exception:
                pid = -1
            raise ConcurrentRunLockError(cadence=cadence, pid=pid, lock_path=lock_path)

        current_pid = os.getpid()
        payload = {
            "cadence": cadence.value,
            "pid": current_pid,
            "acquired_at_utc": datetime.now(UTC).isoformat(),
        }

        # Atomic write
        tmp_path = lock_path.with_suffix(".tmp")
        try:
            tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            tmp_path.replace(lock_path)
        except Exception as exc:
            with contextlib.suppress(Exception):
                if tmp_path.exists():
                    tmp_path.unlink()
            raise PipelineLockError(f"Failed creating lockfile {lock_path}: {exc}") from exc

        try:
            yield lock_path
        finally:
            with contextlib.suppress(Exception):
                if lock_path.exists():
                    lock_path.unlink()

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
