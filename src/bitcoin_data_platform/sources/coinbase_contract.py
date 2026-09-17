"""Contract definition and validation for Coinbase Exchange candle responses."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


class ContractViolationError(ValueError):
    """Raised when data violates the Coinbase candle contract specification."""


@dataclass(frozen=True)
class CoinbaseCandle:
    """Represents a validated hourly candle from Coinbase Exchange.

    Contract:
    - timestamp_utc: UTC datetime corresponding to the candle's start epoch.
    - low: lowest traded price in interval (Decimal > 0).
    - high: highest traded price in interval (Decimal >= low > 0).
    - open: opening price in interval (Decimal > 0).
    - close: closing price in interval (Decimal > 0).
    - volume: base asset traded volume (Decimal >= 0).
    """

    timestamp_utc: datetime
    low: Decimal
    high: Decimal
    open: Decimal
    close: Decimal
    volume: Decimal

    @property
    def timestamp_epoch(self) -> int:
        """Unix epoch timestamp in seconds."""
        return int(self.timestamp_utc.timestamp())


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validating a batch of Coinbase candle records."""

    valid_candles: list[CoinbaseCandle]
    violations: list[str]
    is_valid: bool


def _parse_decimal(val: Any, field_name: str, prefix: str) -> tuple[Decimal | None, str | None]:
    """Parse a numeric value to a finite Decimal, rejecting booleans and invalid types."""
    if isinstance(val, bool):
        return None, f"{prefix}{field_name} must be numeric, got boolean {val}"
    if not isinstance(val, int | float | str | Decimal):
        return None, f"{prefix}{field_name} must be numeric, got {type(val).__name__} ({val!r})"

    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None, f"{prefix}{field_name} must be numeric, got unparseable {val!r}"

    if not dec.is_finite():
        return None, f"{prefix}{field_name} must be finite, got {val!r}"

    return dec, None


def validate_candle(
    raw_candle: Any,
    index: int | None = None,
) -> tuple[CoinbaseCandle | None, list[str]]:
    """Validate a single candle record [time, low, high, open, close, volume].

    Returns:
        A tuple of (CoinbaseCandle or None, list of violation messages).
    """
    prefix = f"Candle at index {index}: " if index is not None else "Candle: "
    violations: list[str] = []

    if not isinstance(raw_candle, list | tuple):
        return None, [f"{prefix}must be a list or tuple, got {type(raw_candle).__name__}"]

    if len(raw_candle) != 6:
        return None, [f"{prefix}must have exactly 6 elements, got {len(raw_candle)}"]

    raw_time, raw_low, raw_high, raw_open, raw_close, raw_volume = raw_candle

    # 1. Validate timestamp (idx 0): positive integer unix epoch
    candle_time: datetime | None = None
    if isinstance(raw_time, bool) or not isinstance(raw_time, int):
        time_type = type(raw_time).__name__
        violations.append(
            f"{prefix}time must be an integer unix epoch, got {time_type} ({raw_time!r})"
        )
    elif raw_time <= 0:
        violations.append(f"{prefix}time must be a positive integer unix epoch, got {raw_time}")
    else:
        try:
            candle_time = datetime.fromtimestamp(raw_time, tz=UTC)
        except (ValueError, OverflowError, OSError) as exc:
            violations.append(f"{prefix}time cannot be converted to UTC datetime: {exc}")

    # 2. Validate price fields (idx 1..4): Decimal > 0
    low_dec, low_err = _parse_decimal(raw_low, "low", prefix)
    if low_err:
        violations.append(low_err)
    elif low_dec is not None and low_dec <= 0:
        violations.append(f"{prefix}low price must be positive, got {low_dec}")

    high_dec, high_err = _parse_decimal(raw_high, "high", prefix)
    if high_err:
        violations.append(high_err)
    elif high_dec is not None and high_dec <= 0:
        violations.append(f"{prefix}high price must be positive, got {high_dec}")

    open_dec, open_err = _parse_decimal(raw_open, "open", prefix)
    if open_err:
        violations.append(open_err)
    elif open_dec is not None and open_dec <= 0:
        violations.append(f"{prefix}open price must be positive, got {open_dec}")

    close_dec, close_err = _parse_decimal(raw_close, "close", prefix)
    if close_err:
        violations.append(close_err)
    elif close_dec is not None and close_dec <= 0:
        violations.append(f"{prefix}close price must be positive, got {close_dec}")

    # Cross-field check: high >= low
    if (
        low_dec is not None
        and high_dec is not None
        and low_dec > 0
        and high_dec > 0
        and high_dec < low_dec
    ):
        violations.append(
            f"{prefix}high price ({high_dec}) must be "
            f"greater than or equal to low price ({low_dec})"
        )

    # 3. Validate volume (idx 5): Decimal >= 0
    vol_dec, vol_err = _parse_decimal(raw_volume, "volume", prefix)
    if vol_err:
        violations.append(vol_err)
    elif vol_dec is not None and vol_dec < 0:
        violations.append(f"{prefix}volume must be non-negative, got {vol_dec}")

    has_none = (
        candle_time is None
        or low_dec is None
        or high_dec is None
        or open_dec is None
        or close_dec is None
        or vol_dec is None
    )
    if violations or has_none:
        return None, violations

    candle = CoinbaseCandle(
        timestamp_utc=candle_time,  # type: ignore[arg-type]
        low=low_dec,  # type: ignore[arg-type]
        high=high_dec,  # type: ignore[arg-type]
        open=open_dec,  # type: ignore[arg-type]
        close=close_dec,  # type: ignore[arg-type]
        volume=vol_dec,  # type: ignore[arg-type]
    )
    return candle, []


def validate_candle_payload(payload: Any) -> ValidationResult:
    """Validate a raw JSON payload from Coinbase Exchange product candles endpoint.

    Args:
        payload: Expected to be a list/tuple of candle tuples.

    Returns:
        ValidationResult containing parsed candles, violations, and validity flag.
    """
    if not isinstance(payload, list | tuple):
        return ValidationResult(
            valid_candles=[],
            violations=[f"Payload must be a list or tuple, got {type(payload).__name__}"],
            is_valid=False,
        )

    valid_candles: list[CoinbaseCandle] = []
    violations: list[str] = []

    for idx, raw_candle in enumerate(payload):
        candle, candle_violations = validate_candle(raw_candle, index=idx)
        if candle_violations:
            violations.extend(candle_violations)
        elif candle is not None:
            valid_candles.append(candle)

    return ValidationResult(
        valid_candles=valid_candles,
        violations=violations,
        is_valid=len(violations) == 0,
    )
