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
    prices: dict[str, Decimal] = {}
    for field, raw_val in (
        ("low", raw_low),
        ("high", raw_high),
        ("open", raw_open),
        ("close", raw_close),
    ):
        dec, err = _parse_decimal(raw_val, field, prefix)
        if err:
            violations.append(err)
        elif dec is not None and dec <= 0:
            violations.append(f"{prefix}{field} price must be positive, got {dec}")
        elif dec is not None:
            prices[field] = dec

    # Cross-field check: high >= low
    if "low" in prices and "high" in prices and prices["high"] < prices["low"]:
        violations.append(
            f"{prefix}high price ({prices['high']}) must be "
            f"greater than or equal to low price ({prices['low']})"
        )

    # 3. Validate volume (idx 5): Decimal >= 0
    vol_dec, vol_err = _parse_decimal(raw_volume, "volume", prefix)
    if vol_err:
        violations.append(vol_err)
    elif vol_dec is not None and vol_dec < 0:
        violations.append(f"{prefix}volume must be non-negative, got {vol_dec}")

    if violations or candle_time is None or vol_dec is None or len(prices) != 4:
        return None, violations

    candle = CoinbaseCandle(
        timestamp_utc=candle_time,
        low=prices["low"],
        high=prices["high"],
        open=prices["open"],
        close=prices["close"],
        volume=vol_dec,
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
