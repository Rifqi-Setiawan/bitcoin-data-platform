"""Unit tests for bounded self-healing remediation runner and safety gates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from bitcoin_data_platform.diagnostics.healer import RemediationRunner
from bitcoin_data_platform.diagnostics.models import RemediationPlan
from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.models import DEFAULT_TRADES_SCHEMA
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def test_healer_dry_run_simulation(tmp_path: Path) -> None:
    """1. Healer in dry_run=True returns simulated outcomes without mutating state."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()
        mgr.acquire_lock(
            run_id="stale-run-1",
            mode="backfill",
            started_at_utc=datetime(2026, 1, 15, 10, 0, tzinfo=UTC),
        )

    runner = RemediationRunner(
        db_path=db_path,
        curated_dir=curated_dir,
        dry_run=True,
        now_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
    )

    plan = RemediationPlan(
        incident_id="inc-1",
        action_type="ACTION_CLEAR_STALE_LOCK",
        description="Clear stale lock",
        parameters={"run_id": "stale-run-1", "duration_seconds": 7200.0},
    )
    result = runner.execute_plan(plan)

    assert result.success is True
    assert "[DRY-RUN]" in result.message
    assert result.details["dry_run"] is True

    # Lock must still be present because it was a dry-run
    with mgr:
        assert mgr.check_lock() is True


def test_healer_clear_stale_lock_safety_precondition(tmp_path: Path) -> None:
    """2. Refuses to clear lock if lock age is under 1-hour stale threshold."""
    runner = RemediationRunner(
        dry_run=False,
        now_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
    )
    # Lock held for only 15 minutes (900 seconds)
    plan = RemediationPlan(
        incident_id="inc-1",
        action_type="ACTION_CLEAR_STALE_LOCK",
        description="Premature clear lock attempt",
        parameters={"run_id": "run-young", "duration_seconds": 900.0},
    )
    result = runner.execute_plan(plan)

    assert result.success is False
    assert "Refusing to clear lock" in result.message


def test_healer_clear_stale_lock_execution(tmp_path: Path) -> None:
    """3. Safely clears stale lock and verifies postcondition when duration > 1 hour."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    now = datetime(2026, 1, 15, 14, 0, tzinfo=UTC)
    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()
        mgr.acquire_lock(
            run_id="run-stale-target",
            mode="incremental",
            started_at_utc=now - timedelta(hours=2),
        )
        assert mgr.check_lock() is True

    runner = RemediationRunner(
        db_path=db_path,
        curated_dir=curated_dir,
        dry_run=False,
        now_utc=now,
    )

    plan = RemediationPlan(
        incident_id="inc-lock-1",
        action_type="ACTION_CLEAR_STALE_LOCK",
        description="Clear stale run lock",
        parameters={"run_id": "run-stale-target", "duration_seconds": 7200.0},
    )
    result = runner.execute_plan(plan)

    assert result.success is True
    assert "Successfully cleared" in result.message
    assert result.details["cleared_count"] == 1

    # Postcondition: lock is now cleared
    with mgr:
        assert mgr.check_lock() is False


def test_healer_backfill_gaps_bounded_and_locked_safety(tmp_path: Path) -> None:
    """4. Backfill rejects execution when locked and caps windows to 24 hours."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()
        mgr.acquire_lock(
            run_id="active-run",
            mode="incremental",
            started_at_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
        )

    # 1. When locked, backfill must fail precondition
    runner_locked = RemediationRunner(
        db_path=db_path,
        curated_dir=curated_dir,
        dry_run=False,
    )
    plan_gaps = RemediationPlan(
        incident_id="inc-gap-1",
        action_type="ACTION_BACKFILL_GAPS",
        description="Backfill missing gaps",
        parameters={"gaps": [{"start": "2026-01-01T00:00:00Z"}]},
    )
    res_locked = runner_locked.execute_plan(plan_gaps)
    assert res_locked.success is False
    assert "active run lock is held" in res_locked.message

    # 2. Release lock and verify backfill bounded to 24 hours
    with mgr:
        mgr.force_clear_lock(stale_threshold_seconds=0)

    executed_gaps: list[dict[str, Any]] = []

    def mock_backfill(gaps: list[dict[str, Any]]) -> None:
        executed_gaps.extend(gaps)

    runner_free = RemediationRunner(
        db_path=db_path,
        curated_dir=curated_dir,
        dry_run=False,
        backfill_fn=mock_backfill,
    )

    # Supply 30 gaps: runner must cap at 24
    thirty_gaps = [{"hour": i} for i in range(30)]
    plan_large = RemediationPlan(
        incident_id="inc-gap-2",
        action_type="ACTION_BACKFILL_GAPS",
        description="Large gap range",
        parameters={"gaps": thirty_gaps},
    )
    res_success = runner_free.execute_plan(plan_large)

    assert res_success.success is True
    assert res_success.details["bounded_gaps_count"] == 24
    assert len(executed_gaps) == 24


def test_healer_rebuild_curated(tmp_path: Path) -> None:
    """5. Rebuild curated partitions calls hook or refreshes views."""
    rebuild_called = False

    def mock_rebuild() -> None:
        nonlocal rebuild_called
        rebuild_called = True

    runner = RemediationRunner(
        dry_run=False,
        rebuild_fn=mock_rebuild,
    )
    plan = RemediationPlan(
        incident_id="inc-reb",
        action_type="ACTION_REBUILD_CURATED",
        description="Rebuild curated views",
    )
    res = runner.execute_plan(plan)

    assert res.success is True
    assert rebuild_called is True


def test_healer_compact_lakehouse_execution(tmp_path: Path) -> None:
    """6. Lakehouse compaction action merges small files and commits new snapshot."""
    catalog_dir = tmp_path / "lakehouse_catalog"
    table_dir = tmp_path / "lakehouse_data" / "trades"

    catalog = LakehouseCatalog(catalog_dir)
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA, location=table_dir)

    part_dir = table_dir / "product_id=BTC-USD"
    part_dir.mkdir(parents=True, exist_ok=True)

    # Create 3 small parquet-like files
    f1 = part_dir / "f1.parquet"
    f2 = part_dir / "f2.parquet"
    f3 = part_dir / "f3.parquet"

    # Write dummy valid parquet tables with schema
    table_data = {
        "source": ["coinbase", "coinbase"],
        "product_id": ["BTC-USD", "BTC-USD"],
        "trade_id": [1, 2],
        "sequence": [1, 2],
        "price": [Decimal("95000.00"), Decimal("95100.00")],
        "size": [Decimal("0.5"), Decimal("1.2")],
        "side": ["buy", "sell"],
        "time_utc": [
            datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
            datetime(2026, 1, 1, 12, 1, tzinfo=UTC),
        ],
        "ingested_at_utc": [
            datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
            datetime(2026, 1, 1, 12, 1, tzinfo=UTC),
        ],
    }
    arrow_t = DEFAULT_TRADES_SCHEMA
    batch_table = pa.Table.from_pydict(table_data, schema=arrow_t)
    pq.write_table(batch_table, f1)
    pq.write_table(batch_table, f2)
    pq.write_table(batch_table, f3)

    catalog.commit_snapshot(
        table_name="trades",
        manifest_files=[
            str(f1.relative_to(table_dir)),
            str(f2.relative_to(table_dir)),
            str(f3.relative_to(table_dir)),
        ],
        summary={"rows": 6},
    )

    runner = RemediationRunner(
        lakehouse_catalog_dir=catalog_dir,
        dry_run=False,
    )
    plan = RemediationPlan(
        incident_id="inc-comp",
        action_type="ACTION_COMPACT_LAKEHOUSE",
        description="Compact trades small files",
        parameters={"table_name": "trades", "target_size_mb": 128},
    )
    res = runner.execute_plan(plan)

    assert res.success is True
    assert "Compacted table 'trades'" in res.message

    # Verify new snapshot was committed
    current_snap = catalog.get_current_snapshot("trades")
    assert current_snap is not None
    assert current_snap.snapshot_id == 2
    # 3 files should have been merged into 1
    assert len(current_snap.manifest_files) == 1


def test_healer_manual_and_unknown_action() -> None:
    """7. Healer handles MANUAL actions and reports unknown action types."""
    runner = RemediationRunner(dry_run=False)

    manual_plan = RemediationPlan(
        incident_id="inc-m",
        action_type="MANUAL",
        description="Operator volume expansion required",
    )
    m_res = runner.execute_plan(manual_plan)
    assert m_res.success is True
    assert "Manual operator action required" in m_res.message

    unknown_plan = RemediationPlan(
        incident_id="inc-u",
        action_type="ACTION_UNKNOWN_MAGIC",
        description="Mysterious action",
    )
    u_res = runner.execute_plan(unknown_plan)
    assert u_res.success is False
    assert "Unknown remediation action" in u_res.message


def test_healer_empty_gaps_success() -> None:
    """8. Backfill with empty gaps returns success with 0 processed."""
    runner = RemediationRunner(dry_run=False)
    plan = RemediationPlan(
        incident_id="inc-empty",
        action_type="ACTION_BACKFILL_GAPS",
        description="No gaps",
        parameters={"gaps": []},
    )
    res = runner.execute_plan(plan)
    assert res.success is True
    assert res.details["gaps_processed"] == 0


def test_healer_execute_all_mixed_sequence(tmp_path: Path) -> None:
    """9. execute_all runs a sequence of mixed remediation plans."""
    runner = RemediationRunner(
        dry_run=True,
        now_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
    )
    plans = [
        RemediationPlan(
            incident_id="i1",
            action_type="ACTION_CLEAR_STALE_LOCK",
            description="Clear stale lock",
            parameters={"run_id": "r1", "duration_seconds": 4000.0},
        ),
        RemediationPlan(
            incident_id="i2",
            action_type="ACTION_REBUILD_CURATED",
            description="Rebuild curated",
        ),
        RemediationPlan(
            incident_id="i3",
            action_type="MANUAL",
            description="Operator review",
        ),
    ]
    results = runner.execute_all(plans)
    assert len(results) == 3
    assert all(r.success for r in results)
    assert results[0].action_type == "ACTION_CLEAR_STALE_LOCK"
    assert results[1].action_type == "ACTION_REBUILD_CURATED"
    assert results[2].action_type == "MANUAL"
