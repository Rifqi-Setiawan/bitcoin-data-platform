"""Contract definition and validation for Coin Metrics Community API v4 responses."""

import re
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any

from bitcoin_data_platform.time_range import parse_iso_utc


class CoinMetricsContractViolationError(ValueError):
    """Raised when data violates the Coin Metrics contract specification."""


@dataclass(frozen=True)
class CoinMetricsRecord:
    """Represents a validated daily on-chain network metric record from Coin Metrics.

    Contract:
    - asset: Non-empty string identifier (e.g. "btc").
    - time_utc: UTC datetime aligned to daily boundary (00:00:00 UTC).
    - tx_count: Total daily transaction count (TxCnt), non-negative integer >= 0.
    - active_addresses: Daily active address count (AdrActCnt), non-negative integer >= 0.
    """

    asset: str
    time_utc: datetime
    tx_count: int
    active_addresses: int

    @property
    def metric_date_utc(self) -> datetime:
        """Semantic alias for daily conformed modeling."""
        return self.time_utc


@dataclass(frozen=True)
class CoinMetricsValidationResult:
    """Outcome of validating a Coin Metrics response payload."""

    valid_records: list[CoinMetricsRecord]
    violations: list[str]
    is_valid: bool


def _parse_timestamp(raw_time: Any, prefix: str) -> tuple[datetime | None, str | None]:
    """Parse raw timestamp into UTC datetime aligned to 00:00:00 UTC."""
    if not isinstance(raw_time, str) or not raw_time.strip():
        t_name = type(raw_time).__name__
        return None, f"{prefix}time must be a non-empty string, got {t_name} ({raw_time!r})"

    val = raw_time.strip()
    # Normalize nanoseconds if present (e.g. .000000000Z -> .000000Z)
    val_norm = re.sub(r"(\.\d{6})\d+Z?$", r"\1Z", val)

    if not val_norm.endswith("Z") and not val_norm.endswith("+00:00"):
        val_norm = f"{val_norm}T00:00:00Z" if "T" not in val_norm else f"{val_norm}Z"

    try:
        dt = parse_iso_utc(val_norm, "time")
    except Exception as exc:
        return None, f"{prefix}time cannot be converted to UTC datetime: {exc}"

    if dt.time() != time(0, 0, 0, 0):
        return (
            None,
            f"{prefix}time must be aligned to daily boundary (00:00:00 UTC), got {raw_time!r}",
        )

    return dt, None


def _parse_non_negative_int(
    val: Any, field_name: str, prefix: str
) -> tuple[int | None, str | None]:
    """Parse a value into a non-negative integer."""
    if isinstance(val, bool):
        return None, f"{prefix}{field_name} must be an integer, got boolean {val}"
    if val is None:
        return None, f"{prefix}{field_name} must not be null or missing"
    if not isinstance(val, int | float | str):
        return None, f"{prefix}{field_name} must be numeric, got {type(val).__name__} ({val!r})"

    try:
        num = float(val)
    except (ValueError, TypeError):
        return None, f"{prefix}{field_name} must be numeric integer, got unparseable {val!r}"

    if not num.is_integer():
        return None, f"{prefix}{field_name} must be an integer, got {val!r}"

    int_val = int(num)
    if int_val < 0:
        return None, f"{prefix}{field_name} must be non-negative (>= 0), got {int_val}"

    return int_val, None


def validate_record(
    raw_record: Any, index: int | None = None
) -> tuple[CoinMetricsRecord | None, list[str]]:
    """Validate a single raw Coin Metrics record dictionary.

    Returns:
        Tuple of (CoinMetricsRecord or None, list of violation messages).
    """
    prefix = f"Record at index {index}: " if index is not None else "Record: "
    violations: list[str] = []

    if not isinstance(raw_record, dict):
        return None, [f"{prefix}must be a dictionary, got {type(raw_record).__name__}"]

    raw_asset = raw_record.get("asset")
    if not isinstance(raw_asset, str) or not raw_asset.strip():
        violations.append(f"{prefix}asset must be a non-empty string, got {raw_asset!r}")
        asset_str = ""
    else:
        asset_str = raw_asset.strip()

    raw_time = raw_record.get("time")
    dt, time_err = _parse_timestamp(raw_time, prefix)
    if time_err:
        violations.append(time_err)

    raw_tx = raw_record.get("TxCnt")
    tx_count, tx_err = _parse_non_negative_int(raw_tx, "TxCnt", prefix)
    if tx_err:
        violations.append(tx_err)

    raw_adr = raw_record.get("AdrActCnt")
    adr_count, adr_err = _parse_non_negative_int(raw_adr, "AdrActCnt", prefix)
    if adr_err:
        violations.append(adr_err)

    if violations or dt is None or tx_count is None or adr_count is None or not asset_str:
        return None, violations

    record = CoinMetricsRecord(
        asset=asset_str,
        time_utc=dt,
        tx_count=tx_count,
        active_addresses=adr_count,
    )
    return record, []


def validate_coin_metrics_payload(payload: Any) -> CoinMetricsValidationResult:
    """Validate a raw JSON payload from Coin Metrics timeseries/asset-metrics endpoint.

    Args:
        payload: Expected to be a dict with a 'data' array, or a list of record dicts.

    Returns:
        CoinMetricsValidationResult containing parsed records, violations, and validity flag.
    """
    if isinstance(payload, dict):
        if "data" not in payload:
            return CoinMetricsValidationResult(
                valid_records=[],
                violations=["Payload missing required 'data' field"],
                is_valid=False,
            )
        raw_data = payload["data"]
        if not isinstance(raw_data, list):
            return CoinMetricsValidationResult(
                valid_records=[],
                violations=[f"Payload 'data' field must be a list, got {type(raw_data).__name__}"],
                is_valid=False,
            )
    elif isinstance(payload, list):
        raw_data = payload
    else:
        return CoinMetricsValidationResult(
            valid_records=[],
            violations=[
                f"Payload must be a dictionary with 'data' list, got {type(payload).__name__}"
            ],
            is_valid=False,
        )

    valid_records: list[CoinMetricsRecord] = []
    violations: list[str] = []

    for idx, item in enumerate(raw_data):
        rec, item_violations = validate_record(item, index=idx)
        if item_violations:
            violations.extend(item_violations)
        elif rec is not None:
            valid_records.append(rec)

    return CoinMetricsValidationResult(
        valid_records=valid_records,
        violations=violations,
        is_valid=len(violations) == 0,
    )
