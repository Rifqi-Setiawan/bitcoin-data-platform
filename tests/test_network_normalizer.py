"""Tests for Coin Metrics on-chain network normalizer and deduplicator."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from bitcoin_data_platform.sources.coin_metrics_contract import (
    CoinMetricsContractViolationError,
    CoinMetricsRecord,
)
from bitcoin_data_platform.transforms.network_normalizer import (
    NetworkRawEnvelope,
    create_network_raw_envelope,
    normalize_network_envelopes,
    normalize_network_record,
    read_network_raw_envelopes,
    write_network_raw_envelope,
)


def _make_sample_record(day: int = 1, tx: int = 300000, adr: int = 800000) -> dict[str, Any]:
    return {
        "asset": "btc",
        "time": f"2026-01-0{day}T00:00:00.000000000Z",
        "TxCnt": str(tx),
        "AdrActCnt": str(adr),
    }


def test_normalize_raw_record_success() -> None:
    """Normalize a raw dictionary record into typed NormalizedNetworkMetric."""
    raw = _make_sample_record(1, 350000, 900000)
    ingested = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)

    metric = normalize_network_record(
        raw,
        source="coin_metrics",
        ingested_at_utc=ingested,
        source_run_id="run-1",
    )

    assert metric.source == "coin_metrics"
    assert metric.asset == "btc"
    assert metric.metric_date_utc == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert metric.transaction_count == 350000
    assert metric.active_addresses_count == 900000
    assert metric.ingested_at_utc == ingested
    assert metric.source_run_id == "run-1"


def test_normalize_coin_metrics_record_object() -> None:
    """Normalize a pre-validated CoinMetricsRecord object."""
    rec = CoinMetricsRecord(
        asset="btc",
        time_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        tx_count=400000,
        active_addresses=950000,
    )
    ingested = datetime(2026, 1, 3, 0, 0, tzinfo=UTC)

    metric = normalize_network_record(
        rec,
        source="coin_metrics",
        ingested_at_utc=ingested,
        source_run_id="run-2",
    )

    assert metric.transaction_count == 400000
    assert metric.active_addresses_count == 950000
    assert metric.metric_date_utc == rec.time_utc


def test_normalize_record_violation_raises_error() -> None:
    """Contract violation in raw record raises CoinMetricsContractViolationError."""
    bad_raw = {"asset": "btc", "time": "2026-01-01T00:00:00Z", "TxCnt": "-5"}
    with pytest.raises(CoinMetricsContractViolationError) as exc_info:
        normalize_network_record(
            bad_raw,
            ingested_at_utc=datetime.now(UTC),
            source_run_id="run-bad",
        )
    assert "TxCnt must be non-negative" in str(exc_info.value)


def test_deduplication_latest_run_wins() -> None:
    """When same (source, asset, metric_date_utc) appears in multiple envelopes, latest run wins."""
    t1 = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 1, 2, 1, 0, tzinfo=UTC)

    # First envelope with older data
    env1 = NetworkRawEnvelope(
        schema_version=1,
        run_id="run-001",
        source="coin_metrics",
        endpoint="timeseries_asset_metrics",
        asset="btc",
        metrics="TxCnt,AdrActCnt",
        frequency="1d",
        start_time="2026-01-01",
        end_time="2026-01-02",
        retrieved_at_utc=t1,
        http_status=200,
        payload_sha256="abc",
        record_count=1,
        payload={"data": [_make_sample_record(1, tx=100000, adr=500000)]},
    )

    # Second envelope with updated/revised data
    env2 = NetworkRawEnvelope(
        schema_version=1,
        run_id="run-002",
        source="coin_metrics",
        endpoint="timeseries_asset_metrics",
        asset="btc",
        metrics="TxCnt,AdrActCnt",
        frequency="1d",
        start_time="2026-01-01",
        end_time="2026-01-02",
        retrieved_at_utc=t2,
        http_status=200,
        payload_sha256="def",
        record_count=1,
        payload={"data": [_make_sample_record(1, tx=120000, adr=550000)]},
    )

    metrics = normalize_network_envelopes([env1, env2])
    assert len(metrics) == 1
    assert metrics[0].transaction_count == 120000
    assert metrics[0].active_addresses_count == 550000
    assert metrics[0].source_run_id == "run-002"


def test_deterministic_sorting() -> None:
    """Normalized metrics are sorted strictly by metric_date_utc ascending."""
    t = datetime(2026, 1, 5, 0, 0, tzinfo=UTC)
    raw_unordered = [
        _make_sample_record(3),
        _make_sample_record(1),
        _make_sample_record(2),
    ]

    env = NetworkRawEnvelope(
        schema_version=1,
        run_id="run-sort",
        source="coin_metrics",
        endpoint="timeseries_asset_metrics",
        asset="btc",
        metrics="TxCnt,AdrActCnt",
        frequency="1d",
        start_time="2026-01-01",
        end_time="2026-01-03",
        retrieved_at_utc=t,
        http_status=200,
        payload_sha256="hash",
        record_count=3,
        payload={"data": raw_unordered},
    )

    metrics = normalize_network_envelopes([env])
    assert len(metrics) == 3
    assert [m.metric_date_utc.day for m in metrics] == [1, 2, 3]


def test_write_and_read_network_raw_envelope(tmp_path: Path) -> None:
    """Test raw gzip envelope serialization, atomic write, and deserialization."""
    retrieved = datetime(2026, 1, 2, 10, 0, tzinfo=UTC)
    payload = {"data": [_make_sample_record(1), _make_sample_record(2)]}

    env_dict = create_network_raw_envelope(
        run_id="test-run-123",
        asset="btc",
        metrics="TxCnt,AdrActCnt",
        frequency="1d",
        start_time="2026-01-01",
        end_time="2026-01-02",
        retrieved_at_utc=retrieved,
        http_status=200,
        payload=payload,
        provider_request_id="req-abc",
    )

    file_path = write_network_raw_envelope(tmp_path, env_dict)
    assert file_path.exists()
    assert file_path.name.endswith(".json.gz")

    envelopes = read_network_raw_envelopes(tmp_path)
    assert len(envelopes) == 1
    env = envelopes[0]
    assert env.run_id == "test-run-123"
    assert env.asset == "btc"
    assert env.record_count == 2
    assert env.http_status == 200


def test_normalized_network_metric_includes_mvrv() -> None:
    """15. NormalizedNetworkMetric includes mvrv_ratio from CoinMetricsRecord and raw dict."""
    from decimal import Decimal

    ingested = datetime(2026, 9, 18, 0, 0, tzinfo=UTC)

    # From raw dict
    raw = {
        "asset": "btc",
        "time": "2026-09-17T00:00:00.000000000Z",
        "TxCnt": "345612",
        "AdrActCnt": "890140",
        "CapMVRVCur": "1.435849",
    }
    m1 = normalize_network_record(raw, ingested_at_utc=ingested, source_run_id="run-1")
    assert m1.mvrv_ratio == Decimal("1.435849")

    # From CoinMetricsRecord
    rec = CoinMetricsRecord(
        asset="btc",
        time_utc=datetime(2026, 9, 17, 0, 0, tzinfo=UTC),
        tx_count=345612,
        active_addresses=890140,
        mvrv_ratio=Decimal("2.105"),
    )
    m2 = normalize_network_record(rec, ingested_at_utc=ingested, source_run_id="run-2")
    assert m2.mvrv_ratio == Decimal("2.105")
