"""Unit tests for DuckDB run locking and stale lock recovery."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def test_acquire_lock_succeeds_when_no_running(tmp_path: Path) -> None:
    """7. Acquire lock succeeds when no RUNNING runs."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        assert manager.check_lock() is False

        acquired = manager.acquire_lock(
            run_id="run-lock-1",
            mode="incremental",
            started_at_utc=now,
        )
        assert acquired is True
        assert manager.check_lock() is True

        running_info = manager.get_running_lock()
        assert running_info is not None
        assert running_info["run_id"] == "run-lock-1"
        assert running_info["mode"] == "incremental"
        assert running_info["status"] == "RUNNING"


def test_acquire_lock_fails_when_running_exists(tmp_path: Path) -> None:
    """8. Acquire lock fails when RUNNING run exists."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        res1 = manager.acquire_lock(
            run_id="run-first",
            mode="backfill",
            started_at_utc=now,
        )
        assert res1 is True

        # Second acquire must fail
        res2 = manager.acquire_lock(
            run_id="run-second",
            mode="incremental",
            started_at_utc=now,
        )
        assert res2 is False

        # Lock is still held by first run
        running = manager.get_running_lock()
        assert running is not None
        assert running["run_id"] == "run-first"


def test_lock_released_on_successful_completion(tmp_path: Path) -> None:
    """9. Lock is released on successful completion."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    start_t = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    end_t = datetime(2026, 1, 1, 12, 5, tzinfo=UTC)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.acquire_lock(
            run_id="run-success",
            mode="incremental",
            started_at_utc=start_t,
        )
        assert manager.check_lock() is True

        manager.release_lock(
            run_id="run-success",
            status="SUCCEEDED",
            completed_at_utc=end_t,
            rows_promoted=24,
        )
        assert manager.check_lock() is False
        assert manager.get_running_lock() is None

        # Verify a new run can acquire the lock
        acquired_next = manager.acquire_lock(
            run_id="run-subsequent",
            mode="incremental",
            started_at_utc=end_t,
        )
        assert acquired_next is True


def test_lock_released_on_failure(tmp_path: Path) -> None:
    """10. Lock is released on failure."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    start_t = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    end_t = datetime(2026, 1, 1, 12, 1, tzinfo=UTC)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.acquire_lock(
            run_id="run-fail",
            mode="incremental",
            started_at_utc=start_t,
        )
        assert manager.check_lock() is True

        manager.release_lock(
            run_id="run-fail",
            status="FAILED",
            completed_at_utc=end_t,
            error_message="Coinbase HTTP 503 unavailable",
        )
        assert manager.check_lock() is False

        last_fail = manager.get_last_failure()
        assert last_fail is not None
        assert last_fail["run_id"] == "run-fail"


def test_force_clear_stale_lock(tmp_path: Path) -> None:
    """11. Force clear stale lock (> 1 hour old) succeeds while keeping recent locks."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    now = datetime(2026, 1, 1, 15, 0, tzinfo=UTC)
    stale_time = now - timedelta(hours=2)  # 2 hours old
    recent_time = now - timedelta(minutes=10)  # 10 minutes old

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()

        # 1. Insert a stale lock
        manager.acquire_lock(
            run_id="run-stale",
            mode="repair",
            started_at_utc=stale_time,
        )
        assert manager.check_lock() is True

        # Force clear with 1-hour threshold
        cleared = manager.force_clear_lock(stale_threshold_seconds=3600, now_utc=now)
        assert cleared == 1
        assert manager.check_lock() is False

        # 2. Insert a recent lock (< 1 hour old)
        manager.acquire_lock(
            run_id="run-recent",
            mode="repair",
            started_at_utc=recent_time,
        )
        assert manager.check_lock() is True

        # Threshold 3600 seconds should NOT clear recent lock
        cleared_recent = manager.force_clear_lock(stale_threshold_seconds=3600, now_utc=now)
        assert cleared_recent == 0
        assert manager.check_lock() is True

        # Threshold 0 should force clear any lock regardless of age
        cleared_all = manager.force_clear_lock(stale_threshold_seconds=0, now_utc=now)
        assert cleared_all == 1
        assert manager.check_lock() is False
