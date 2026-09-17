"""Unit tests for pipeline watermark persistence and monotonicity."""

from datetime import UTC, datetime
from pathlib import Path

from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def test_initial_state_no_watermark(tmp_path: Path) -> None:
    """1. Initial state: no watermark exists in a fresh database."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        assert manager.get_watermark() is None


def test_set_watermark_initial(tmp_path: Path) -> None:
    """2. Set watermark after first promotion / initial run."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    wm_time = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        res = manager.set_watermark(wm_time, run_id="run-init")
        assert res is True
        assert manager.get_watermark() == wm_time


def test_watermark_advances(tmp_path: Path) -> None:
    """3. Watermark advances when a later timestamp is provided."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    t1 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.set_watermark(t1, run_id="run-1")
        res = manager.set_watermark(t2, run_id="run-2")
        assert res is True
        assert manager.get_watermark() == t2


def test_watermark_does_not_lower_on_backfill_or_repair(tmp_path: Path) -> None:
    """4. Watermark does not lower when an older timestamp is supplied (e.g. backfill or repair)."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    t_high = datetime(2026, 1, 10, 0, 0, tzinfo=UTC)
    t_low = datetime(2026, 1, 5, 0, 0, tzinfo=UTC)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.set_watermark(t_high, run_id="run-high")

        # Attempt to set earlier timestamp
        res = manager.set_watermark(t_low, run_id="run-repair")
        assert res is False
        assert manager.get_watermark() == t_high


def test_watermark_monotonically_increases(tmp_path: Path) -> None:
    """5. Watermark monotonically increases across sequential operations."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    t1 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
    t3 = datetime(2026, 1, 3, 0, 0, tzinfo=UTC)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        assert manager.set_watermark(t2, run_id="run-t2") is True
        assert manager.get_watermark() == t2

        # Equal timestamp: should not advance
        assert manager.set_watermark(t2, run_id="run-t2-dup") is False
        assert manager.get_watermark() == t2

        # Earlier timestamp: should not advance
        assert manager.set_watermark(t1, run_id="run-t1") is False
        assert manager.get_watermark() == t2

        # Later timestamp: should advance
        assert manager.set_watermark(t3, run_id="run-t3") is True
        assert manager.get_watermark() == t3


def test_read_watermark_returns_correct_value(tmp_path: Path) -> None:
    """6. Read watermark returns correct value including timezone awareness."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    expected_ts = datetime(2026, 1, 15, 14, 0, 0, tzinfo=UTC)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        manager.set_watermark(expected_ts, run_id="run-read-check")

    # Re-open manager in a fresh context to verify persistence
    manager2 = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager2:
        val = manager2.get_watermark()
        assert val is not None
        assert val.tzinfo is not None
        assert val == expected_ts
