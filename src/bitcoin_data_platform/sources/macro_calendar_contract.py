"""Contract definition and validation for ForexFactory macro economic calendar events."""

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


class MacroContractViolationError(ValueError):
    """Raised when data violates the Macro Calendar contract specification."""


@dataclass(frozen=True)
class MacroEvent:
    """Represents a validated scheduled macroeconomic event.

    Contract:
    - event_id: Deterministic SHA-256 hash (title + scheduled_utc) for idempotent deduplication.
    - country: Currency/country code string (e.g. "USD").
    - title: Human-readable event title (e.g. "FOMC Statement").
    - impact: Impact level (e.g. "High", "Medium", "Low", "Holiday").
    - scheduled_utc: Scheduled release datetime in UTC.
    - forecast: Forecasted metric value as string, or None if empty.
    - previous: Prior period metric value as string, or None if empty.
    - ingested_at_utc: UTC datetime when the event was ingested.
    """

    event_id: str
    country: str
    title: str
    impact: str
    scheduled_utc: datetime
    forecast: str | None
    previous: str | None
    ingested_at_utc: datetime


@dataclass(frozen=True)
class MacroValidationResult:
    """Outcome of validating a macroeconomic calendar payload."""

    valid_events: list[MacroEvent]
    violations: list[str]
    is_valid: bool


def compute_event_id(title: str, scheduled_utc: datetime) -> str:
    """Deterministic event_id derived from title and scheduled UTC timestamp."""
    utc_str = scheduled_utc.astimezone(UTC).isoformat()
    raw_key = f"{title.strip().lower()}_{utc_str}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def _parse_macro_date(raw_date: Any, prefix: str) -> tuple[datetime | None, str | None]:
    """Parse ISO-8601 date string with timezone offset to UTC datetime."""
    if not isinstance(raw_date, str) or not raw_date.strip():
        return None, f"{prefix}date must be a non-empty ISO-8601 string, got {raw_date!r}"

    val = raw_date.strip()
    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            return None, f"{prefix}date must contain timezone offset, got {val!r}"
        return dt.astimezone(UTC), None
    except Exception as exc:
        return None, f"{prefix}date failed to parse as ISO-8601 datetime: {exc}"


def validate_macro_event(
    raw_event: Any,
    *,
    ingested_at_utc: datetime | None = None,
    index: int | None = None,
) -> tuple[MacroEvent | None, list[str]]:
    """Validate a single raw macro calendar event dictionary."""
    prefix = f"Event at index {index}: " if index is not None else "Event: "
    violations: list[str] = []

    if not isinstance(raw_event, dict):
        return None, [f"{prefix}must be a dictionary, got {type(raw_event).__name__}"]

    title = raw_event.get("title")
    if not isinstance(title, str) or not title.strip():
        violations.append(f"{prefix}title must be a non-empty string, got {title!r}")
        clean_title = ""
    else:
        clean_title = title.strip()

    country = raw_event.get("country")
    if not isinstance(country, str) or not country.strip():
        violations.append(f"{prefix}country must be a non-empty string, got {country!r}")
        clean_country = ""
    else:
        clean_country = country.strip()

    impact = raw_event.get("impact")
    if not isinstance(impact, str) or not impact.strip():
        violations.append(f"{prefix}impact must be a non-empty string, got {impact!r}")
        clean_impact = ""
    else:
        clean_impact = impact.strip()

    sched_utc, date_err = _parse_macro_date(raw_event.get("date"), prefix)
    if date_err:
        violations.append(date_err)

    if violations or not clean_title or not clean_country or not clean_impact or sched_utc is None:
        return None, violations

    raw_forecast = raw_event.get("forecast")
    clean_forecast = (
        str(raw_forecast).strip()
        if raw_forecast is not None and str(raw_forecast).strip()
        else None
    )

    raw_previous = raw_event.get("previous")
    clean_previous = (
        str(raw_previous).strip()
        if raw_previous is not None and str(raw_previous).strip()
        else None
    )

    ingested = ingested_at_utc or datetime.now(UTC)
    if ingested.tzinfo is None:
        ingested = ingested.replace(tzinfo=UTC)

    event_id = compute_event_id(clean_title, sched_utc)

    return MacroEvent(
        event_id=event_id,
        country=clean_country,
        title=clean_title,
        impact=clean_impact,
        scheduled_utc=sched_utc,
        forecast=clean_forecast,
        previous=clean_previous,
        ingested_at_utc=ingested,
    ), []


def validate_macro_payload(
    payload: Any,
    *,
    ingested_at_utc: datetime | None = None,
) -> MacroValidationResult:
    """Validate a raw JSON payload from ForexFactory calendar feed."""
    if isinstance(payload, list):
        raw_items = payload
    elif isinstance(payload, dict) and "events" in payload and isinstance(payload["events"], list):
        raw_items = payload["events"]
    else:
        return MacroValidationResult(
            valid_events=[],
            violations=[f"Payload must be a list of event objects, got {type(payload).__name__}"],
            is_valid=False,
        )

    valid_events: list[MacroEvent] = []
    violations: list[str] = []

    for idx, item in enumerate(raw_items):
        evt, item_violations = validate_macro_event(
            item,
            ingested_at_utc=ingested_at_utc,
            index=idx,
        )
        if item_violations:
            violations.extend(item_violations)
        elif evt is not None:
            valid_events.append(evt)

    return MacroValidationResult(
        valid_events=valid_events,
        violations=violations,
        is_valid=len(violations) == 0,
    )
