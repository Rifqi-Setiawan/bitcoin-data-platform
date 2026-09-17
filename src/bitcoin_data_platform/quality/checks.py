"""Data quality assertion and contract checks for market data."""

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, TypeVar


class QualityCheckError(ValueError):
    """Raised when data quality assertions fail."""


T = TypeVar("T")


def _get_candle_attr(candle: Any, *attr_names: str) -> Any:
    """Retrieve the first matching attribute from a candle object."""
    for attr in attr_names:
        if hasattr(candle, attr):
            return getattr(candle, attr)
    return None


def check_candle(
    candle: Any,
    *,
    now_utc: datetime | None = None,
) -> list[str]:
    """Check a candle object against blocking quality checks.

    Returns a list of violation messages. An empty list signifies a valid candle.
    Checks:
    - No null/missing required fields.
    - Positive prices: open, high, low, close > 0.
    - Non-negative volume: volume >= 0.
    - OHLC invariants: high >= max(open, close, low) and low <= min(open, close, high).
    - Hour-aligned UTC timestamp: tzinfo=UTC, minute=0, second=0, microsecond=0.
    - No future timestamp: candle_start_utc <= now_utc (or current UTC time).
    """
    violations: list[str] = []

    # 1. Null / missing field checks
    ts = _get_candle_attr(candle, "candle_start_utc", "timestamp_utc")
    open_p = _get_candle_attr(candle, "open")
    high_p = _get_candle_attr(candle, "high")
    low_p = _get_candle_attr(candle, "low")
    close_p = _get_candle_attr(candle, "close")
    vol = _get_candle_attr(candle, "volume_base", "volume")

    if ts is None:
        violations.append("Missing or null timestamp")
    if open_p is None:
        violations.append("Missing or null open price")
    if high_p is None:
        violations.append("Missing or null high price")
    if low_p is None:
        violations.append("Missing or null low price")
    if close_p is None:
        violations.append("Missing or null close price")
    if vol is None:
        violations.append("Missing or null volume")

    if violations:
        return violations

    # 2. Timestamp checks
    if not isinstance(ts, datetime):
        violations.append(f"Timestamp must be a datetime object, got {type(ts).__name__}")
    else:
        if ts.tzinfo is None:
            violations.append("Timestamp must be timezone-aware UTC, got naive datetime")
        elif ts.utcoffset() != UTC.utcoffset(None):
            violations.append(f"Timestamp timezone offset must be UTC, got {ts.tzinfo}")

        if ts.minute != 0 or ts.second != 0 or ts.microsecond != 0:
            violations.append(
                f"Timestamp must be hour-aligned (minute=0, second=0, microsecond=0), "
                f"got {ts.isoformat()}"
            )

        ref_now = now_utc if now_utc is not None else datetime.now(UTC)
        if ts > ref_now:
            violations.append(
                f"Timestamp is in the future ({ts.isoformat()} > {ref_now.isoformat()})"
            )

    # 3. Price checks: must be positive Decimal
    for name, p in [("open", open_p), ("high", high_p), ("low", low_p), ("close", close_p)]:
        if not isinstance(p, Decimal):
            try:
                p_dec = Decimal(str(p))
            except Exception:
                violations.append(f"{name} price must be a valid Decimal, got {p!r}")
                continue
        else:
            p_dec = p

        if p_dec <= 0:
            violations.append(f"{name} price must be positive, got {p_dec}")

    # 4. Volume check: must be non-negative Decimal
    if not isinstance(vol, Decimal):
        try:
            vol_dec = Decimal(str(vol))
        except Exception:
            violations.append(f"volume must be a valid Decimal, got {vol!r}")
            vol_dec = None
    else:
        vol_dec = vol

    if vol_dec is not None and vol_dec < 0:
        violations.append(f"volume must be non-negative, got {vol_dec}")

    # 5. OHLC invariant checks
    try:
        o_val = Decimal(str(open_p))
        h_val = Decimal(str(high_p))
        l_val = Decimal(str(low_p))
        c_val = Decimal(str(close_p))
    except Exception:
        return violations

    if h_val < l_val:
        violations.append(f"OHLC violation: high ({h_val}) must be >= low ({l_val})")
    if h_val < o_val:
        violations.append(f"OHLC violation: high ({h_val}) must be >= open ({o_val})")
    if h_val < c_val:
        violations.append(f"OHLC violation: high ({h_val}) must be >= close ({c_val})")
    if l_val > o_val:
        violations.append(f"OHLC violation: low ({l_val}) must be <= open ({o_val})")
    if l_val > c_val:
        violations.append(f"OHLC violation: low ({l_val}) must be <= close ({c_val})")

    return violations


def validate_candle_quality(
    candle: Any,
    *,
    now_utc: datetime | None = None,
) -> None:
    """Validate candle invariants, raising QualityCheckError if violations are found."""
    violations = check_candle(candle, now_utc=now_utc)
    if violations:
        raise QualityCheckError("; ".join(violations))


def filter_valid_candles(
    candles: Sequence[T],
    *,
    now_utc: datetime | None = None,
) -> tuple[list[T], list[str]]:
    """Filter a sequence of candles, promoting only valid records.

    Returns:
        A tuple of (valid_candles, all_violations).
    """
    valid: list[T] = []
    violations: list[str] = []

    for idx, candle in enumerate(candles):
        c_violations = check_candle(candle, now_utc=now_utc)
        if c_violations:
            for v in c_violations:
                violations.append(f"Candle index {idx}: {v}")
        else:
            valid.append(candle)

    return valid, violations
