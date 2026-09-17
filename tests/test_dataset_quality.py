"""Unit tests for Phase 5 dataset quality checks and anomaly detection."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.quality.dataset_checks import (
    QualityCheckRecord,
    check_boundary_reconciliation,
    check_natural_key_uniqueness,
    check_price_return_anomaly,
    check_volume_spike_anomaly,
    run_dataset_quality_checks,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.raw_writer import create_raw_envelope, write_raw_envelope
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _make_test_candle(
    hour: int,
    open_price: str = "95000.00",
    close_price: str = "95500.00",
    high_price: str = "96000.00",
    low_price: str = "94500.00",
    volume: str = "10.0",
    date_str: str = "2026-01-01",
) -> NormalizedCandle:
    dt = datetime.fromisoformat(f"{date_str}T{hour:02d}:00:00+00:00")
    return NormalizedCandle(
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        candle_start_utc=dt,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume_base=Decimal(volume),
        ingested_at_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        source_run_id="run-qc-test",
    )


def test_natural_key_uniqueness_passes_on_distinct_candles() -> None:
    candles = [_make_test_candle(0), _make_test_candle(1), _make_test_candle(2)]
    record = check_natural_key_uniqueness(candles, run_id="run-unique")
    assert record.rule_name == "natural_key_uniqueness"
    assert record.severity == "BLOCK"
    assert record.status == "PASSED"
    assert record.metric_value == 0.0


def test_natural_key_uniqueness_blocks_on_duplicates() -> None:
    candles = [_make_test_candle(0), _make_test_candle(1), _make_test_candle(0)]
    record = check_natural_key_uniqueness(candles, run_id="run-dupe")
    assert record.rule_name == "natural_key_uniqueness"
    assert record.severity == "BLOCK"
    assert record.status == "FAILED"
    assert record.metric_value == 1.0
    assert "duplicate" in (record.details or "").lower()


def test_boundary_reconciliation_passes_within_window() -> None:
    candles = [_make_test_candle(0), _make_test_candle(1), _make_test_candle(2)]
    start_utc = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end_utc = datetime(2026, 1, 1, 3, 0, tzinfo=UTC)

    record = check_boundary_reconciliation(candles, start_utc, end_utc, run_id="run-bound")
    assert record.rule_name == "boundary_reconciliation"
    assert record.severity == "BLOCK"
    assert record.status == "PASSED"
    assert record.metric_value == 0.0


def test_boundary_reconciliation_blocks_on_outliers() -> None:
    # Hour 5 is outside [00:00, 03:00)
    candles = [_make_test_candle(0), _make_test_candle(1), _make_test_candle(5)]
    start_utc = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end_utc = datetime(2026, 1, 1, 3, 0, tzinfo=UTC)

    record = check_boundary_reconciliation(candles, start_utc, end_utc, run_id="run-bound-fail")
    assert record.rule_name == "boundary_reconciliation"
    assert record.severity == "BLOCK"
    assert record.status == "FAILED"
    assert record.metric_value == 1.0
    assert "outside requested window" in (record.details or "")


def test_price_return_anomaly_detects_intrabar_jump() -> None:
    # Open 95000 -> Close 120000 (> 25% return, threshold 15%)
    candle_spike = _make_test_candle(
        0,
        open_price="95000.00",
        close_price="120000.00",
        high_price="125000.00",
        low_price="94000.00",
    )
    record = check_price_return_anomaly([candle_spike], threshold_pct=0.15)
    assert record.rule_name == "price_return_anomaly"
    assert record.severity == "WARN"
    assert record.status == "FAILED"
    assert record.metric_value is not None and record.metric_value > 0.15


def test_price_return_anomaly_detects_interbar_jump() -> None:
    # Hour 0 close 95000, Hour 1 close 115000 (> 20% move)
    c0 = _make_test_candle(0, close_price="95000.00")
    c1 = _make_test_candle(
        1,
        open_price="95000.00",
        close_price="115000.00",
        high_price="116000.00",
    )

    record = check_price_return_anomaly([c0, c1], threshold_pct=0.15)
    assert record.rule_name == "price_return_anomaly"
    assert record.severity == "WARN"
    assert record.status == "FAILED"


def test_price_return_normal_passes() -> None:
    # 1-2% moves pass
    c0 = _make_test_candle(0, open_price="95000.00", close_price="95500.00")
    c1 = _make_test_candle(1, open_price="95500.00", close_price="96000.00")

    record = check_price_return_anomaly([c0, c1], threshold_pct=0.15)
    assert record.rule_name == "price_return_anomaly"
    assert record.severity == "WARN"
    assert record.status == "PASSED"


def test_volume_spike_anomaly_detects_extreme_volume() -> None:
    # Baseline volume is 10.0; Hour 3 has volume 100.0 (10x average)
    candles = [
        _make_test_candle(0, volume="10.0"),
        _make_test_candle(1, volume="10.0"),
        _make_test_candle(2, volume="10.0"),
        _make_test_candle(3, volume="100.0"),
    ]
    record = check_volume_spike_anomaly(candles, spike_multiplier=5.0)
    assert record.rule_name == "volume_spike_anomaly"
    assert record.severity == "WARN"
    assert record.status == "FAILED"
    assert record.metric_value is not None and record.metric_value > 2.0


def test_volume_spike_normal_volume_passes() -> None:
    candles = [
        _make_test_candle(0, volume="10.0"),
        _make_test_candle(1, volume="12.0"),
        _make_test_candle(2, volume="9.0"),
    ]
    record = check_volume_spike_anomaly(candles, spike_multiplier=5.0)
    assert record.rule_name == "volume_spike_anomaly"
    assert record.severity == "WARN"
    assert record.status == "PASSED"


def test_duckdb_persists_and_queries_quality_check_records(tmp_path: Path) -> None:
    db_path = tmp_path / "test_qc.duckdb"
    manager = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with manager:
        manager.initialize()
        checks = [
            QualityCheckRecord(
                check_id="chk-001",
                run_id="run-100",
                rule_name="natural_key_uniqueness",
                severity="BLOCK",
                status="PASSED",
                metric_value=0.0,
                threshold_value=0.0,
                details="Uniqueness verified",
            ),
            QualityCheckRecord(
                check_id="chk-002",
                run_id="run-100",
                rule_name="volume_spike_anomaly",
                severity="WARN",
                status="FAILED",
                metric_value=6.5,
                threshold_value=5.0,
                details="Volume spike 6.5x",
            ),
        ]
        manager.record_quality_checks(checks)

        recent = manager.get_recent_quality_checks(limit=10)
        assert len(recent) == 2
        rule_names = {r["rule_name"] for r in recent}
        assert "natural_key_uniqueness" in rule_names
        assert "volume_spike_anomaly" in rule_names


def test_run_dataset_quality_checks_evaluates_all_rules() -> None:
    candles = [_make_test_candle(0), _make_test_candle(1)]
    start_utc = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end_utc = datetime(2026, 1, 1, 2, 0, tzinfo=UTC)

    records = run_dataset_quality_checks(
        candles,
        run_id="run-batch",
        requested_start=start_utc,
        requested_end=end_utc,
    )
    assert len(records) == 4
    names = [r.rule_name for r in records]
    assert "natural_key_uniqueness" in names
    assert "boundary_reconciliation" in names
    assert "price_return_anomaly" in names
    assert "volume_spike_anomaly" in names


def test_promote_records_quality_checks_in_database(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    payload = []
    for h in range(5):
        epoch = int(datetime(2026, 1, 1, h, 0, tzinfo=UTC).timestamp())
        payload.append([epoch, 94000, 96000, 95000, 95500, 10])

    env = create_raw_envelope(
        run_id="run-qc-promote",
        product_id="BTC-USD",
        granularity_seconds=3600,
        start_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 5, 0, tzinfo=UTC),
        retrieved_at_utc=datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        http_status=200,
        payload=payload,
    )
    write_raw_envelope(raw_dir, env)

    code = main(
        [
            "promote",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ]
    )
    assert code == 0

    mgr = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with mgr:
        checks = mgr.get_recent_quality_checks(limit=10)
        assert len(checks) >= 3
        rule_names = {c["rule_name"] for c in checks}
        assert "natural_key_uniqueness" in rule_names
