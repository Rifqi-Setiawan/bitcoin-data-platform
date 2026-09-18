"""Tests for Coin Metrics Community API v4 contract definition and validation."""

from datetime import UTC, datetime
from typing import Any

from bitcoin_data_platform.sources.coin_metrics_contract import (
    validate_coin_metrics_payload,
    validate_record,
)


def _valid_payload() -> dict[str, Any]:
    return {
        "data": [
            {
                "asset": "btc",
                "time": "2026-01-01T00:00:00.000000000Z",
                "TxCnt": "345678",
                "AdrActCnt": "890123",
            },
            {
                "asset": "btc",
                "time": "2026-01-02T00:00:00Z",
                "TxCnt": 400000,
                "AdrActCnt": 950000,
            },
        ]
    }


def test_valid_payload_validation() -> None:
    """Validate a standard valid Coin Metrics response payload."""
    payload = _valid_payload()
    result = validate_coin_metrics_payload(payload)

    assert result.is_valid is True
    assert len(result.violations) == 0
    assert len(result.valid_records) == 2

    r0 = result.valid_records[0]
    assert r0.asset == "btc"
    assert r0.time_utc == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert r0.metric_date_utc == r0.time_utc
    assert r0.tx_count == 345678
    assert r0.active_addresses == 890123

    r1 = result.valid_records[1]
    assert r1.asset == "btc"
    assert r1.time_utc == datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
    assert r1.tx_count == 400000
    assert r1.active_addresses == 950000


def test_date_only_timestamp_accepted() -> None:
    """Validate that YYYY-MM-DD format is accepted and aligned to 00:00:00 UTC."""
    record, violations = validate_record(
        {"asset": "btc", "time": "2026-01-03", "TxCnt": "100", "AdrActCnt": "200"}
    )
    assert len(violations) == 0
    assert record is not None
    assert record.time_utc == datetime(2026, 1, 3, 0, 0, tzinfo=UTC)


def test_missing_data_field() -> None:
    """Validate that payload missing 'data' array is rejected."""
    result = validate_coin_metrics_payload({"error": "not found"})
    assert result.is_valid is False
    assert any("missing required 'data' field" in v for v in result.violations)


def test_data_field_not_list() -> None:
    """Validate that payload where 'data' is not a list is rejected."""
    result = validate_coin_metrics_payload({"data": "invalid"})
    assert result.is_valid is False
    assert any("must be a list" in v for v in result.violations)


def test_payload_not_dict_or_list() -> None:
    """Validate that scalar payload is rejected."""
    result = validate_coin_metrics_payload("invalid")
    assert result.is_valid is False
    assert len(result.violations) > 0


def test_record_not_dict() -> None:
    """Validate that non-dict record is rejected."""
    record, violations = validate_record(["btc", "2026-01-01", 100, 200])
    assert record is None
    assert any("must be a dictionary" in v for v in violations)


def test_missing_asset() -> None:
    """Validate rejection of missing or empty asset."""
    record, violations = validate_record(
        {"asset": "", "time": "2026-01-01T00:00:00Z", "TxCnt": "100", "AdrActCnt": "200"}
    )
    assert record is None
    assert any("asset must be a non-empty string" in v for v in violations)


def test_missing_time() -> None:
    """Validate rejection of missing or null time."""
    record, violations = validate_record(
        {"asset": "btc", "time": None, "TxCnt": "100", "AdrActCnt": "200"}
    )
    assert record is None
    assert any("time must be a non-empty string" in v for v in violations)


def test_misaligned_time_boundary() -> None:
    """Validate rejection of time not aligned to daily boundary (00:00:00 UTC)."""
    record, violations = validate_record(
        {
            "asset": "btc",
            "time": "2026-01-01T15:30:00Z",
            "TxCnt": "100",
            "AdrActCnt": "200",
        }
    )
    assert record is None
    assert any("aligned to daily boundary" in v for v in violations)


def test_negative_counts() -> None:
    """Validate rejection of negative transaction count and active addresses."""
    record, violations = validate_record(
        {
            "asset": "btc",
            "time": "2026-01-01T00:00:00Z",
            "TxCnt": "-1",
            "AdrActCnt": "-500",
        }
    )
    assert record is None
    assert any("TxCnt must be non-negative" in v for v in violations)
    assert any("AdrActCnt must be non-negative" in v for v in violations)


def test_non_integer_counts() -> None:
    """Validate rejection of fractional or unparseable counts."""
    record, violations = validate_record(
        {
            "asset": "btc",
            "time": "2026-01-01T00:00:00Z",
            "TxCnt": "123.45",
            "AdrActCnt": "not-a-number",
        }
    )
    assert record is None
    assert any("TxCnt must be an integer" in v for v in violations)
    assert any("AdrActCnt must be numeric integer" in v for v in violations)


def test_boolean_counts() -> None:
    """Validate rejection of boolean values passed as counts."""
    record, violations = validate_record(
        {
            "asset": "btc",
            "time": "2026-01-01T00:00:00Z",
            "TxCnt": True,
            "AdrActCnt": False,
        }
    )
    assert record is None
    assert any("TxCnt must be an integer, got boolean" in v for v in violations)
    assert any("AdrActCnt must be an integer, got boolean" in v for v in violations)


def test_null_missing_counts() -> None:
    """Validate rejection of null or missing metric fields."""
    record, violations = validate_record(
        {"asset": "btc", "time": "2026-01-01T00:00:00Z", "TxCnt": None}
    )
    assert record is None
    assert any("TxCnt must not be null or missing" in v for v in violations)
    assert any("AdrActCnt must not be null or missing" in v for v in violations)


def test_coin_metrics_record_includes_mvrv() -> None:
    """14. CoinMetricsRecord parses CapMVRVCur as Decimal when present and None when absent."""
    from decimal import Decimal

    # Case 1: CapMVRVCur present
    record, violations = validate_record(
        {
            "asset": "btc",
            "time": "2026-09-17T00:00:00.000000000Z",
            "TxCnt": "345612",
            "AdrActCnt": "890140",
            "CapMVRVCur": "1.435849433216295172",
        }
    )
    assert len(violations) == 0
    assert record is not None
    assert record.mvrv_ratio == Decimal("1.435849433216295172")

    # Case 2: CapMVRVCur absent (historical backward compatibility)
    record_old, violations_old = validate_record(
        {
            "asset": "btc",
            "time": "2026-09-17T00:00:00.000000000Z",
            "TxCnt": "345612",
            "AdrActCnt": "890140",
        }
    )
    assert len(violations_old) == 0
    assert record_old is not None
    assert record_old.mvrv_ratio is None
