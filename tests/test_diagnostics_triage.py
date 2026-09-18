"""Unit tests for deterministic operational incident triage engine across INC-01..INC-06."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from bitcoin_data_platform.diagnostics.models import TelemetryBundle
from bitcoin_data_platform.diagnostics.triage import TriageEngine


@pytest.fixture
def base_now() -> datetime:
    return datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


def test_triage_healthy_state(base_now: datetime) -> None:
    """1. Triage engine reports zero incidents for healthy telemetry."""
    telemetry = TelemetryBundle(
        last_run={"run_id": "run-ok", "status": "SUCCEEDED", "rows_promoted": 24},
        watermark_utc="2026-01-15T11:00:00Z",
        watermark_age_hours=1.0,
        gaps=[],
        is_locked=False,
        disk={"percent_used": 42.0, "disk_critical": False},
        quality_checks=[{"status": "PASSED", "severity": "BLOCK", "rule_name": "no_dups"}],
        lakehouse_tables={"trades": {"small_files_by_partition": {"root": 5}}},
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 0
    assert len(plans) == 0


def test_triage_inc_01_upstream_outage(base_now: datetime) -> None:
    """2. INC-01: Classifies HTTP 429 / 5xx / timeout as Source Upstream Outage."""
    telemetry = TelemetryBundle(
        last_run={
            "run_id": "run-err-01",
            "status": "FAILED",
            "error_message": "Coinbase HTTP 429 Too Many Requests: Rate limit exceeded",
            "error_class": "CoinbaseHTTPError",
        },
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-01"
    assert incidents[0].severity == "CRITICAL"
    assert "Upstream" in incidents[0].title
    assert "OPERATIONAL_RUNBOOK.md#recovery-from-api-outages" in incidents[0].runbook_ref

    assert len(plans) == 1
    assert plans[0].action_type == "ACTION_BACKFILL_GAPS"
    assert plans[0].incident_id == incidents[0].incident_id


def test_triage_inc_02_data_quality_block(base_now: datetime) -> None:
    """3. INC-02: Classifies FAILED quality check with BLOCK severity."""
    telemetry = TelemetryBundle(
        quality_checks=[
            {
                "check_id": "qc-fail-1",
                "rule_name": "natural_key_uniqueness",
                "severity": "BLOCK",
                "status": "FAILED",
                "details": "Duplicate candles detected for 2026-01-14T00:00:00Z",
            }
        ]
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-02"
    assert incidents[0].severity == "CRITICAL"
    assert "DATA_QUALITY_RUNBOOK.md" in incidents[0].runbook_ref

    assert len(plans) == 1
    assert plans[0].action_type == "ACTION_REBUILD_CURATED"


def test_triage_inc_03_storage_critical(base_now: datetime) -> None:
    """4. INC-03: Classifies disk utilization > 80% as storage failure."""
    telemetry = TelemetryBundle(
        disk={
            "percent_used": 87.5,
            "free_gb": 12.0,
            "total_gb": 100.0,
            "disk_critical": True,
        }
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-03"
    assert incidents[0].severity == "CRITICAL"
    assert "Storage" in incidents[0].title

    assert len(plans) == 1
    assert plans[0].action_type == "MANUAL"


def test_triage_inc_04_stale_lock_critical(base_now: datetime) -> None:
    """5. INC-04: Classifies lock running for > 1 hour as Stale Concurrency Lock (CRITICAL)."""
    telemetry = TelemetryBundle(
        is_locked=True,
        running_lock={
            "run_id": "stale-run-99",
            "started_at_utc": (base_now - timedelta(hours=2, minutes=15)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        },
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-04"
    assert incidents[0].severity == "CRITICAL"
    assert "Stale" in incidents[0].title

    assert len(plans) == 1
    assert plans[0].action_type == "ACTION_CLEAR_STALE_LOCK"
    assert plans[0].parameters["run_id"] == "stale-run-99"


def test_triage_inc_04_active_lock_warning(base_now: datetime) -> None:
    """6. INC-04: Classifies lock running for < 1 hour as Active Lock (WARNING, no auto-clear)."""
    telemetry = TelemetryBundle(
        is_locked=True,
        running_lock={
            "run_id": "active-run-12",
            "started_at_utc": (base_now - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-04"
    assert incidents[0].severity == "WARNING"
    assert "Active" in incidents[0].title
    # Active lock should not have an automated clear plan
    assert len(plans) == 0


def test_triage_inc_05_freshness_lag_and_gaps(base_now: datetime) -> None:
    """7. INC-05: Classifies watermark lag > 2h and missing gaps."""
    telemetry = TelemetryBundle(
        watermark_utc="2026-01-15T08:00:00Z",
        watermark_age_hours=4.0,
        gaps=[
            {
                "start_utc": "2026-01-15T09:00:00Z",
                "end_utc": "2026-01-15T11:00:00Z",
                "missing_hours": 2,
            }
        ],
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-05"
    assert incidents[0].severity == "WARNING"
    assert "Freshness" in incidents[0].title

    assert len(plans) == 1
    assert plans[0].action_type == "ACTION_BACKFILL_GAPS"
    assert len(plans[0].parameters["gaps"]) == 1


def test_triage_inc_05_extreme_lag_critical(base_now: datetime) -> None:
    """8. INC-05: Watermark lag > 24 hours escalates to CRITICAL severity."""
    telemetry = TelemetryBundle(
        watermark_utc="2026-01-13T12:00:00Z",
        watermark_age_hours=48.0,
        gaps=[],
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, _ = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-05"
    assert incidents[0].severity == "CRITICAL"


def test_triage_inc_06_lakehouse_bloat(base_now: datetime) -> None:
    """9. INC-06: Partitions with > 32 small files trigger Lakehouse Small-File Bloat."""
    telemetry = TelemetryBundle(
        lakehouse_tables={
            "trades": {
                "small_files_by_partition": {
                    "product_id=BTC-USD": 38,
                    "product_id=ETH-USD": 10,
                }
            }
        }
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-06"
    assert incidents[0].severity == "WARNING"
    assert "Small-File Bloat" in incidents[0].title

    assert len(plans) == 1
    assert plans[0].action_type == "ACTION_COMPACT_LAKEHOUSE"
    assert plans[0].parameters["table_name"] == "trades"
    assert plans[0].parameters["partition"] == "product_id=BTC-USD"


def test_triage_prioritization_critical_before_warning(base_now: datetime) -> None:
    """10. Triage engine orders incidents and bound plans with CRITICAL before WARNING."""
    telemetry = TelemetryBundle(
        # Warning: Freshness lag (3 hours)
        watermark_utc="2026-01-15T09:00:00Z",
        watermark_age_hours=3.0,
        # Critical: Stale lock (> 1 hour)
        is_locked=True,
        running_lock={
            "run_id": "stale-lock-1",
            "started_at_utc": (base_now - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        # Warning: Lakehouse small files bloat
        lakehouse_tables={"trades": {"small_files_by_partition": {"root": 45}}},
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 3
    # Top incident must be CRITICAL (INC-04 stale lock)
    assert incidents[0].incident_code == "INC-04"
    assert incidents[0].severity == "CRITICAL"
    assert plans[0].action_type == "ACTION_CLEAR_STALE_LOCK"

    # Subsequent incidents are WARNING
    assert incidents[1].severity == "WARNING"
    assert incidents[2].severity == "WARNING"


def test_triage_inc_02_contract_violation_in_error_message(base_now: datetime) -> None:
    """11. INC-02: Contract violation in error_message triggers data quality incident."""
    telemetry = TelemetryBundle(
        last_run={
            "run_id": "run-bad-contract",
            "status": "FAILED",
            "error_message": "CoinbaseContractViolationError: Missing candle timestamp",
        }
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-02"
    assert len(plans) == 1
    assert plans[0].action_type == "ACTION_REBUILD_CURATED"


def test_triage_inc_03_storage_error_in_run(base_now: datetime) -> None:
    """12. INC-03: StorageError / No space left on device triggers storage incident."""
    telemetry = TelemetryBundle(
        last_run={
            "run_id": "run-disk-err",
            "status": "FAILED",
            "error_message": "StorageError: No space left on device while writing parquet",
        }
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-03"
    assert incidents[0].severity == "CRITICAL"


def test_triage_inc_04_lock_collision_in_run(base_now: datetime) -> None:
    """13. INC-04: RunLockError / exit code 6 triggers lock collision incident."""
    telemetry = TelemetryBundle(
        last_run={
            "run_id": "run-collided",
            "status": "FAILED",
            "error_message": "RunLockError: exit code: 6: concurrent run detected",
        }
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-04"
    assert incidents[0].severity == "WARNING"
    assert "Collision" in incidents[0].title


def test_triage_inc_06_multi_partition_selective_bloat(base_now: datetime) -> None:
    """14. INC-06: Only partitions exceeding 32 files trigger bloat incident."""
    telemetry = TelemetryBundle(
        lakehouse_tables={
            "trades": {
                "small_files_by_partition": {
                    "product_id=BTC-USD": 35,  # Exceeds 32
                    "product_id=ETH-USD": 12,  # Normal
                    "product_id=SOL-USD": 4,  # Normal
                }
            }
        }
    )
    engine = TriageEngine(telemetry, now_utc=base_now)
    incidents, plans = engine.triage()

    assert len(incidents) == 1
    assert incidents[0].incident_code == "INC-06"
    assert "BTC-USD" in incidents[0].affected_scope
