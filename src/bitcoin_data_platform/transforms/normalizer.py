"""Normalizer and deduplicator mapping raw payloads to typed NormalizedCandle records."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from bitcoin_data_platform.quality.checks import QualityCheckError, check_candle
from bitcoin_data_platform.transforms.raw_reader import RawEnvelope


@dataclass(frozen=True)
class NormalizedCandle:
    """Typed, validated candle ready for Parquet."""

    source: str
    product_id: str
    granularity_seconds: int
    candle_start_utc: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_base: Decimal
    ingested_at_utc: datetime
    source_run_id: str


def _parse_decimal_field(val: Any, field_name: str) -> Decimal:
    """Parse a numeric value into a finite Decimal."""
    if isinstance(val, bool):
        raise QualityCheckError(f"{field_name} must be numeric, got boolean {val}")
    try:
        dec = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise QualityCheckError(
            f"{field_name} must be a valid numeric Decimal, got {val!r}"
        ) from exc
    if not dec.is_finite():
        raise QualityCheckError(f"{field_name} must be finite, got {val!r}")
    return dec


def normalize_candle(
    raw_candle: Any,
    *,
    source: str,
    product_id: str,
    granularity_seconds: int,
    ingested_at_utc: datetime,
    source_run_id: str,
    now_utc: datetime | None = None,
) -> NormalizedCandle:
    """Normalize and validate a single raw candle tuple [time, low, high, open, close, volume].

    Raises:
        QualityCheckError: If any contract or quality invariant is violated.
    """
    if not isinstance(raw_candle, Sequence) or isinstance(raw_candle, str | bytes):
        raise QualityCheckError(f"Candle must be a sequence, got {type(raw_candle).__name__}")

    if len(raw_candle) != 6:
        raise QualityCheckError(f"Candle must have exactly 6 elements, got {len(raw_candle)}")

    raw_time, raw_low, raw_high, raw_open, raw_close, raw_volume = raw_candle

    # 1. Timestamp validation
    if isinstance(raw_time, bool) or not isinstance(raw_time, int):
        t_name = type(raw_time).__name__
        raise QualityCheckError(
            f"Candle time must be an integer unix epoch, got {t_name} ({raw_time!r})"
        )
    if raw_time <= 0:
        raise QualityCheckError(
            f"Candle time must be a positive integer unix epoch, got {raw_time}"
        )

    try:
        candle_start_utc = datetime.fromtimestamp(raw_time, tz=UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise QualityCheckError(f"Candle time cannot be converted to UTC datetime: {exc}") from exc

    # 2. Decimal prices and volume
    low_dec = _parse_decimal_field(raw_low, "low")
    high_dec = _parse_decimal_field(raw_high, "high")
    open_dec = _parse_decimal_field(raw_open, "open")
    close_dec = _parse_decimal_field(raw_close, "close")
    volume_dec = _parse_decimal_field(raw_volume, "volume")

    candle = NormalizedCandle(
        source=source,
        product_id=product_id,
        granularity_seconds=granularity_seconds,
        candle_start_utc=candle_start_utc,
        open=open_dec,
        high=high_dec,
        low=low_dec,
        close=close_dec,
        volume_base=volume_dec,
        ingested_at_utc=ingested_at_utc,
        source_run_id=source_run_id,
    )

    # 3. Comprehensive blocking quality checks
    violations = check_candle(candle, now_utc=now_utc)
    if violations:
        raise QualityCheckError("; ".join(violations))

    return candle


def normalize_envelopes(
    envelopes: list[RawEnvelope],
    *,
    now_utc: datetime | None = None,
) -> list[NormalizedCandle]:
    """Normalize raw payloads to typed candles. Applies blocking quality checks.

    Deduplicates by natural key (source, product_id, granularity_seconds, candle_start_utc).
    Conflict resolution: latest run_id / ingested_at_utc wins.
    Returns list of NormalizedCandle sorted by candle_start_utc.
    """
    deduped: dict[tuple[str, str, int, datetime], NormalizedCandle] = {}

    for env in envelopes:
        for raw_candle in env.payload:
            candle = normalize_candle(
                raw_candle,
                source=env.source,
                product_id=env.product_id,
                granularity_seconds=env.granularity_seconds,
                ingested_at_utc=env.retrieved_at_utc,
                source_run_id=env.run_id,
                now_utc=now_utc,
            )

            # Coinbase API returns candles for closed window [start, end].
            # Filter out any end boundary candle to enforce [start_utc, end_utc).
            if candle.candle_start_utc < env.start_utc or candle.candle_start_utc >= env.end_utc:
                continue

            natural_key = (
                candle.source,
                candle.product_id,
                candle.granularity_seconds,
                candle.candle_start_utc,
            )

            if natural_key in deduped:
                existing = deduped[natural_key]
                # Latest ingested_at_utc or higher source_run_id wins
                if (candle.ingested_at_utc, candle.source_run_id) >= (
                    existing.ingested_at_utc,
                    existing.source_run_id,
                ):
                    deduped[natural_key] = candle
            else:
                deduped[natural_key] = candle

    # Sort deterministically by candle_start_utc
    sorted_candles = sorted(deduped.values(), key=lambda c: c.candle_start_utc)
    return sorted_candles
