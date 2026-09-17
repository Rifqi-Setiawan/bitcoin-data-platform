"""Tests for Coinbase candle contract validation."""

from datetime import UTC, datetime
from decimal import Decimal

from bitcoin_data_platform.sources.coinbase_contract import (
    CoinbaseCandle,
    validate_candle,
    validate_candle_payload,
)


def test_valid_candle() -> None:
    """1. Valid candle parses correctly into CoinbaseCandle with UTC timestamp and Decimals."""
    raw = [1767225600, 95000.50, 96500.00, 95200.00, 96000.25, 123.456789]
    candle, violations = validate_candle(raw, index=0)

    assert violations == []
    assert candle is not None
    assert isinstance(candle, CoinbaseCandle)
    assert candle.timestamp_utc == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert candle.timestamp_epoch == 1767225600
    assert candle.low == Decimal("95000.50")
    assert candle.high == Decimal("96500.00")
    assert candle.open == Decimal("95200.00")
    assert candle.close == Decimal("96000.25")
    assert candle.volume == Decimal("123.456789")


def test_wrong_length() -> None:
    """2. Candle with fewer or more than 6 elements fails validation."""
    candle_5, violations_5 = validate_candle([1767225600, 95000, 96000, 95200, 96000], index=0)
    assert candle_5 is None
    assert any("exactly 6 elements" in v for v in violations_5)

    candle_7, violations_7 = validate_candle(
        [1767225600, 95000, 96000, 95200, 96000, 10, 999], index=1
    )
    assert candle_7 is None
    assert any("exactly 6 elements" in v for v in violations_7)


def test_non_numeric() -> None:
    """3. Non-numeric values for price or volume fail validation."""
    raw = [1767225600, "not_a_number", 96000, 95200, 96000, 10]
    candle, violations = validate_candle(raw, index=0)
    assert candle is None
    assert any("must be numeric" in v for v in violations)

    raw_none = [1767225600, 95000, 96000, None, 96000, 10]
    candle_none, violations_none = validate_candle(raw_none, index=0)
    assert candle_none is None
    assert any("must be numeric" in v for v in violations_none)


def test_negative_price() -> None:
    """4. Negative or zero prices fail validation."""
    # Negative low
    raw_neg = [1767225600, -100, 96000, 95200, 96000, 10]
    candle, violations = validate_candle(raw_neg, index=0)
    assert candle is None
    assert any("low price must be positive" in v for v in violations)

    # Zero open
    raw_zero = [1767225600, 95000, 96000, 0, 96000, 10]
    candle_z, violations_z = validate_candle(raw_zero, index=0)
    assert candle_z is None
    assert any("open price must be positive" in v for v in violations_z)


def test_negative_volume() -> None:
    """5. Negative volume fails validation."""
    raw = [1767225600, 95000, 96000, 95200, 96000, -0.01]
    candle, violations = validate_candle(raw, index=0)
    assert candle is None
    assert any("volume must be non-negative" in v for v in violations)


def test_zero_volume_ok() -> None:
    """6. Zero volume is explicitly permitted."""
    raw = [1767225600, 95000, 96000, 95200, 96000, 0]
    candle, violations = validate_candle(raw, index=0)
    assert violations == []
    assert candle is not None
    assert candle.volume == Decimal("0")


def test_high_less_than_low() -> None:
    """7. High price less than low price fails validation."""
    raw = [1767225600, 96000, 95000, 95500, 95500, 10]
    candle, violations = validate_candle(raw, index=0)
    assert candle is None
    assert any("high price" in v and "greater than or equal to low price" in v for v in violations)


def test_bad_timestamp() -> None:
    """8. Bad timestamp (boolean, string, float, zero, negative) fails validation."""
    # Boolean timestamp
    candle_b, violations_b = validate_candle([True, 95000, 96000, 95200, 96000, 10], index=0)
    assert candle_b is None
    assert any("time must be an integer unix epoch" in v for v in violations_b)

    # String timestamp
    candle_s, violations_s = validate_candle(
        ["1767225600", 95000, 96000, 95200, 96000, 10], index=0
    )
    assert candle_s is None
    assert any("time must be an integer unix epoch" in v for v in violations_s)

    # Float timestamp
    candle_f, violations_f = validate_candle(
        [1767225600.5, 95000, 96000, 95200, 96000, 10], index=0
    )
    assert candle_f is None
    assert any("time must be an integer unix epoch" in v for v in violations_f)

    # Non-positive timestamp
    candle_z, violations_z = validate_candle([0, 95000, 96000, 95200, 96000, 10], index=0)
    assert candle_z is None
    assert any("positive integer unix epoch" in v for v in violations_z)

    candle_n, violations_n = validate_candle([-100, 95000, 96000, 95200, 96000, 10], index=0)
    assert candle_n is None
    assert any("positive integer unix epoch" in v for v in violations_n)


def test_mixed_batch() -> None:
    """9. Batch containing both valid and invalid records separates valid and reports violations."""
    payload = [
        [1767225600, 95000, 96000, 95200, 96000, 10],  # Valid
        [1767229200, 96000, 95000, 95500, 95500, 10],  # Invalid: high < low
        [1767232800, 94000, 97000, 95000, 96500, 25],  # Valid
    ]
    result = validate_candle_payload(payload)
    assert not result.is_valid
    assert len(result.valid_candles) == 2
    assert len(result.violations) == 1
    assert "high price" in result.violations[0]


def test_empty_payload() -> None:
    """10. Empty payload list is valid with 0 candles and 0 violations."""
    result = validate_candle_payload([])
    assert result.is_valid
    assert result.valid_candles == []
    assert result.violations == []


def test_invalid_payload_structure() -> None:
    """11. Non-list/tuple payload is invalid and reports violation."""
    result_dict = validate_candle_payload({"message": "Not Found"})
    assert not result_dict.is_valid
    assert len(result_dict.valid_candles) == 0
    assert any("Payload must be a list or tuple" in v for v in result_dict.violations)

    result_str = validate_candle_payload("invalid string")
    assert not result_str.is_valid

    # Single candle passed instead of list of candles
    result_single = validate_candle_payload([[1767225600, "bad"]])
    assert not result_single.is_valid
