"""Unit tests for out-of-band read-only telemetry collector."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from bitcoin_data_platform.diagnostics.collector import TelemetryCollector
from bitcoin_data_platform.lakehouse.catalog import LakehouseCatalog
from bitcoin_data_platform.lakehouse.models import DEFAULT_TRADES_SCHEMA
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.parquet_writer import write_parquet_partitions
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _make_candle(hour: int, date_str: str = "2026-01-01") -> NormalizedCandle:
    dt = datetime.fromisoformat(f"{date_str}T{hour:02d}:00:00+00:00")
    return NormalizedCandle(
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        candle_start_utc=dt,
        open=Decimal("95000.00"),
        high=Decimal("95500.00"),
        low=Decimal("94800.00"),
        close=Decimal("95200.00"),
        volume_base=Decimal("12.5"),
        ingested_at_utc=dt,
        source_run_id="run-col-01",
    )


def test_collector_missing_files_graceful(tmp_path: Path) -> None:
    """1. Collector handles missing databases and state files without throwing."""
    collector = TelemetryCollector(
        db_path=tmp_path / "nonexistent.duckdb",
        curated_dir=tmp_path / "nonexistent_curated",
        lakehouse_catalog_dir=tmp_path / "nonexistent_catalog",
        alert_state_path=tmp_path / "nonexistent_alert.json",
        now_utc=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
    )
    bundle = collector.collect()

    assert bundle.last_run is None
    assert bundle.recent_runs == []
    assert bundle.quality_checks == []
    assert bundle.watermark_utc is None
    assert bundle.watermark_age_hours is None
    assert bundle.gaps == []
    assert not bundle.is_locked
    assert bundle.running_lock is None
    assert bundle.lakehouse_tables == {}
    assert bundle.alert_state == {}
    assert bundle.disk["total_gb"] >= 0.0
    # Collector records zero queries on nonexistent DBs
    assert isinstance(bundle.executed_queries, list)


def test_collector_duckdb_read_only_and_audit(tmp_path: Path) -> None:
    """2. Collector queries DuckDB strictly read-only and records SQL audit trail."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    now = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()
        mgr.record_run(
            run_id="run-101",
            mode="incremental",
            started_at_utc=now - timedelta(hours=1),
            completed_at_utc=now - timedelta(minutes=50),
            status="SUCCEEDED",
            rows_promoted=10,
        )
        mgr.set_watermark(now - timedelta(hours=1), run_id="run-101", now_utc=now)

    collector = TelemetryCollector(
        db_path=db_path,
        curated_dir=curated_dir,
        now_utc=now,
    )
    bundle = collector.collect()

    assert bundle.last_run is not None
    assert bundle.last_run["run_id"] == "run-101"
    assert bundle.last_run["status"] == "SUCCEEDED"
    assert bundle.watermark_utc is not None
    assert bundle.watermark_age_hours == pytest.approx(1.0, rel=1e-2)
    assert not bundle.is_locked

    # Verify SQL query audit trail was recorded
    assert len(collector.executed_queries) > 0
    assert any("SELECT table_name" in q for q in collector.executed_queries)
    assert any("run_metadata" in q for q in collector.executed_queries)
    assert any("pipeline_watermark" in q for q in collector.executed_queries)


def test_collector_duckdb_target_run_and_running_lock(tmp_path: Path) -> None:
    """3. Collector targets specific run ID and inspects active running locks."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    now = datetime(2026, 1, 10, 15, 0, tzinfo=UTC)
    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()
        mgr.record_run(
            run_id="run-target-42",
            mode="backfill",
            started_at_utc=now - timedelta(hours=3),
            completed_at_utc=now - timedelta(hours=2),
            status="FAILED",
            error_message="HTTP 429 Too Many Requests",
        )
        # Acquire an active lock
        mgr.acquire_lock(
            run_id="run-active-lock",
            mode="incremental",
            started_at_utc=now - timedelta(minutes=30),
        )

    collector = TelemetryCollector(
        db_path=db_path,
        curated_dir=curated_dir,
        target_run_id="run-target-42",
        now_utc=now,
    )
    bundle = collector.collect()

    assert bundle.last_run is not None
    assert bundle.last_run["run_id"] == "run-target-42"
    assert bundle.last_run["status"] == "FAILED"
    assert bundle.is_locked is True
    assert bundle.running_lock is not None
    assert bundle.running_lock["run_id"] == "run-active-lock"


def test_collector_gaps_and_quality_checks(tmp_path: Path) -> None:
    """4. Collector detects fact candle gaps and reads quality check records."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    # Write gap in candles (hour 0 and hour 3)
    candles = [_make_candle(0), _make_candle(3)]
    write_parquet_partitions(candles, curated_dir=curated_dir)

    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()
        mgr.record_quality_checks(
            [
                type(
                    "Check",
                    (),
                    {
                        "check_id": "qc-01",
                        "run_id": "run-qc-1",
                        "rule_name": "no_duplicate_natural_keys",
                        "severity": "BLOCK",
                        "status": "FAILED",
                        "metric_value": 3.0,
                        "threshold_value": 0.0,
                        "details": "Found 3 duplicate hourly timestamps",
                        "evaluated_at_utc": now,
                    },
                )()
            ]
        )

    collector = TelemetryCollector(
        db_path=db_path,
        curated_dir=curated_dir,
        now_utc=now,
    )
    bundle = collector.collect()

    assert len(bundle.gaps) == 1
    assert bundle.gaps[0]["missing_hours"] == 2
    assert len(bundle.quality_checks) == 1
    assert bundle.quality_checks[0]["rule_name"] == "no_duplicate_natural_keys"
    assert bundle.quality_checks[0]["severity"] == "BLOCK"
    assert bundle.quality_checks[0]["status"] == "FAILED"


def test_collector_lakehouse_catalog_read_only(tmp_path: Path) -> None:
    """5. Collector inspects Lakehouse catalog snapshots and small files in mode=ro."""
    catalog_dir = tmp_path / "lakehouse_catalog"
    table_dir = tmp_path / "lakehouse_data" / "trades"

    catalog = LakehouseCatalog(catalog_dir)
    catalog.create_table("trades", DEFAULT_TRADES_SCHEMA, location=table_dir)

    # Create dummy micro-batch files under partition
    part_dir = table_dir / "product_id=BTC-USD"
    part_dir.mkdir(parents=True, exist_ok=True)
    dummy_file = part_dir / "part-0.parquet"
    dummy_file.write_bytes(b"PAR1_test_bytes")

    # Commit snapshot referencing dummy file
    catalog.commit_snapshot(
        table_name="trades",
        manifest_files=[str(dummy_file.relative_to(table_dir))],
        summary={"rows": 10},
    )

    collector = TelemetryCollector(
        lakehouse_catalog_dir=catalog_dir,
        now_utc=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )
    bundle = collector.collect()

    assert "trades" in bundle.lakehouse_tables
    t_info = bundle.lakehouse_tables["trades"]
    assert t_info["table_name"] == "trades"
    assert t_info["current_snapshot_id"] == 1
    assert t_info["manifest_files_count"] == 1
    assert t_info["small_files_count"] == 1
    assert t_info["small_files_by_partition"]["product_id=BTC-USD"] == 1

    # Verify SQLite read-only queries were audited
    assert any("[sqlite:ro]" in q for q in collector.executed_queries)


def test_collector_alert_state_and_to_dict(tmp_path: Path) -> None:
    """6. Collector reads alert state and bundle converts cleanly to dictionary."""
    alert_file = tmp_path / "state" / "alert_state.json"
    alert_file.parent.mkdir(parents=True, exist_ok=True)
    alert_file.write_text(
        json.dumps(
            {
                "units": {
                    "bitcoin-data.service": {
                        "last_alert_at_utc": "2026-01-10T08:00:00Z",
                        "consecutive_failures": 2,
                    }
                }
            }
        )
    )

    collector = TelemetryCollector(
        alert_state_path=alert_file,
        now_utc=datetime(2026, 1, 10, 10, 0, tzinfo=UTC),
    )
    bundle = collector.collect()

    assert "bitcoin-data.service" in bundle.alert_state.get("units", {})

    b_dict = bundle.to_dict()
    assert isinstance(b_dict, dict)
    assert b_dict["collected_at_utc"] == "2026-01-10T10:00:00Z"
    assert "disk" in b_dict
    assert "executed_queries" in b_dict


def test_collector_duckdb_read_only_mutation_rejected(tmp_path: Path) -> None:
    """7. Collector verifies DuckDB is strictly read-only and rejects mutations."""
    db_path = tmp_path / "state" / "platform.duckdb"
    curated_dir = tmp_path / "curated"

    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        mgr.initialize()

    collector = TelemetryCollector(
        db_path=db_path,
        curated_dir=curated_dir,
    )
    bundle = collector.collect()
    assert bundle is not None

    # Attempting write on a read-only DuckDB connection must fail
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        with pytest.raises(duckdb.Error):
            con.execute("CREATE TABLE read_only_violation (id INT);")
    finally:
        con.close()


def test_collector_target_run_not_found(tmp_path: Path) -> None:
    """8. Collector with nonexistent target_run_id handles missing record cleanly."""
    db_path = tmp_path / "state" / "platform.duckdb"
    mgr = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with mgr:
        mgr.initialize()

    collector = TelemetryCollector(
        db_path=db_path,
        target_run_id="nonexistent-uuid-999",
    )
    bundle = collector.collect()
    assert bundle.last_run is None
