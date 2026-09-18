"""Contract definition and validation for Alternative.me Crypto Fear & Greed Index."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

VALID_CLASSIFICATIONS = {
    "Extreme Fear",
    "Fear",
    "Neutral",
    "Greed",
    "Extreme Greed",
}


class SentimentContractViolationError(ValueError):
    """Raised when data violates the Sentiment contract specification."""


@dataclass(frozen=True)
class SentimentRecord:
    """Represents a validated daily sentiment index record.

    Contract:
    - date_utc: UTC datetime aligned to date (00:00:00 UTC).
    - value: Integer sentiment score between 0 and 100 inclusive.
    - classification: One of 'Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed'.
    - ingested_at_utc: UTC datetime when the record was ingested.
    """

    date_utc: datetime
    value: int
    classification: str
    ingested_at_utc: datetime


@dataclass(frozen=True)
class SentimentValidationResult:
    """Outcome of validating a sentiment response payload."""

    valid_records: list[SentimentRecord]
    violations: list[str]
    is_valid: bool


def _parse_timestamp(raw_ts: Any, prefix: str) -> tuple[datetime | None, str | None]:
    """Parse Unix epoch timestamp string/int into date-aligned UTC datetime."""
    if raw_ts is None:
        return None, f"{prefix}timestamp must not be null or missing"
    try:
        ts_int = int(str(raw_ts).strip())
    except (ValueError, TypeError):
        return None, f"{prefix}timestamp must be numeric epoch seconds, got {raw_ts!r}"

    if ts_int < 0:
        return None, f"{prefix}timestamp must be non-negative, got {ts_int}"

    try:
        dt = datetime.fromtimestamp(ts_int, tz=UTC).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return dt, None
    except (ValueError, OverflowError) as exc:
        return None, f"{prefix}timestamp is invalid or out of range: {exc}"


def _parse_fng_value(raw_val: Any, prefix: str) -> tuple[int | None, str | None]:
    """Parse Fear & Greed value into integer between 0 and 100."""
    if raw_val is None:
        return None, f"{prefix}value must not be null or missing"
    if isinstance(raw_val, bool):
        return None, f"{prefix}value must be an integer, got boolean {raw_val}"

    try:
        val_int = int(str(raw_val).strip())
    except (ValueError, TypeError):
        return None, f"{prefix}value must be numeric integer, got {raw_val!r}"

    if not 0 <= val_int <= 100:
        return None, f"{prefix}value must be between 0 and 100, got {val_int}"

    return val_int, None


def validate_sentiment_record(
    raw_record: Any,
    *,
    ingested_at_utc: datetime | None = None,
    index: int | None = None,
) -> tuple[SentimentRecord | None, list[str]]:
    """Validate a single raw Fear & Greed record dictionary."""
    prefix = f"Record at index {index}: " if index is not None else "Record: "
    violations: list[str] = []

    if not isinstance(raw_record, dict):
        return None, [f"{prefix}must be a dictionary, got {type(raw_record).__name__}"]

    val, val_err = _parse_fng_value(raw_record.get("value"), prefix)
    if val_err:
        violations.append(val_err)

    classification = raw_record.get("value_classification")
    if not isinstance(classification, str) or not classification.strip():
        violations.append(
            f"{prefix}value_classification must be a non-empty string, got {classification!r}"
        )
        clean_classification = ""
    else:
        clean_classification = classification.strip()
        if clean_classification not in VALID_CLASSIFICATIONS:
            allowed = sorted(VALID_CLASSIFICATIONS)
            violations.append(
                f"{prefix}value_classification must be one of {allowed}, "
                f"got {clean_classification!r}"
            )

    ts_dt, ts_err = _parse_timestamp(raw_record.get("timestamp"), prefix)
    if ts_err:
        violations.append(ts_err)

    if violations or val is None or ts_dt is None or not clean_classification:
        return None, violations

    ingested = ingested_at_utc or datetime.now(UTC)
    if ingested.tzinfo is None:
        ingested = ingested.replace(tzinfo=UTC)

    return SentimentRecord(
        date_utc=ts_dt,
        value=val,
        classification=clean_classification,
        ingested_at_utc=ingested,
    ), []


def validate_sentiment_payload(
    payload: Any,
    *,
    ingested_at_utc: datetime | None = None,
) -> SentimentValidationResult:
    """Validate a raw JSON payload from Alternative.me /fng endpoint."""
    if isinstance(payload, dict):
        if "data" not in payload:
            return SentimentValidationResult(
                valid_records=[],
                violations=["Payload missing required 'data' field"],
                is_valid=False,
            )
        raw_data = payload["data"]
        if not isinstance(raw_data, list):
            return SentimentValidationResult(
                valid_records=[],
                violations=[f"Payload 'data' field must be a list, got {type(raw_data).__name__}"],
                is_valid=False,
            )
    elif isinstance(payload, list):
        raw_data = payload
    else:
        return SentimentValidationResult(
            valid_records=[],
            violations=[f"Payload must be a dictionary or list, got {type(payload).__name__}"],
            is_valid=False,
        )

    valid_records: list[SentimentRecord] = []
    violations: list[str] = []

    for idx, item in enumerate(raw_data):
        rec, item_violations = validate_sentiment_record(
            item,
            ingested_at_utc=ingested_at_utc,
            index=idx,
        )
        if item_violations:
            violations.extend(item_violations)
        elif rec is not None:
            valid_records.append(rec)

    return SentimentValidationResult(
        valid_records=valid_records,
        violations=violations,
        is_valid=len(violations) == 0,
    )
