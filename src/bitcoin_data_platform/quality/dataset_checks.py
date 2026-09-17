"""Dataset-level data quality rules, boundary reconciliation, and anomaly detection."""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class QualityCheckRecord:
    """Record representing the evaluation outcome of a dataset quality rule."""

    check_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str = ""
    rule_name: str = ""
    severity: str = "INFO"  # BLOCK, WARN, INFO
    status: str = "PASSED"  # PASSED, FAILED
    metric_value: float | None = None
    threshold_value: float | None = None
    details: str | None = None
    evaluated_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert record to dictionary representation."""
        return {
            "check_id": self.check_id,
            "run_id": self.run_id,
            "rule_name": self.rule_name,
            "severity": self.severity,
            "status": self.status,
            "metric_value": self.metric_value,
            "threshold_value": self.threshold_value,
            "details": self.details,
            "evaluated_at_utc": self.evaluated_at_utc.isoformat(),
        }


def check_natural_key_uniqueness(
    candles: Sequence[Any],
    run_id: str = "",
    *,
    evaluated_at_utc: datetime | None = None,
) -> QualityCheckRecord:
    """Verify natural key uniqueness across the candle dataset.

    Natural key: (source, product_id, granularity_seconds, candle_start_utc).
    Severity: BLOCK.
    Fails if duplicate natural keys are present in the batch.
    """
    eval_time = evaluated_at_utc or datetime.now(UTC)
    seen: set[tuple[str, str, int, Any]] = set()
    duplicates: list[tuple[str, str, int, Any]] = []

    for c in candles:
        source = str(getattr(c, "source", "coinbase_exchange"))
        prod = str(getattr(c, "product_id", "BTC-USD"))
        gran = int(getattr(c, "granularity_seconds", 3600))
        ts = getattr(c, "candle_start_utc", None) or getattr(c, "timestamp_utc", None)
        if ts is not None and ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        key = (source, prod, gran, ts)
        if key in seen:
            duplicates.append(key)
        else:
            seen.add(key)

    if duplicates:
        return QualityCheckRecord(
            check_id=str(uuid.uuid4()),
            run_id=run_id,
            rule_name="natural_key_uniqueness",
            severity="BLOCK",
            status="FAILED",
            metric_value=float(len(duplicates)),
            threshold_value=0.0,
            details=f"Found {len(duplicates)} duplicate natural key(s) in dataset.",
            evaluated_at_utc=eval_time,
        )

    return QualityCheckRecord(
        check_id=str(uuid.uuid4()),
        run_id=run_id,
        rule_name="natural_key_uniqueness",
        severity="BLOCK",
        status="PASSED",
        metric_value=0.0,
        threshold_value=0.0,
        details="All natural keys are unique.",
        evaluated_at_utc=eval_time,
    )


def check_boundary_reconciliation(
    candles: Sequence[Any],
    requested_start: datetime,
    requested_end: datetime,
    run_id: str = "",
    *,
    tolerance_hours: int = 0,
    evaluated_at_utc: datetime | None = None,
) -> QualityCheckRecord:
    """Reconcile candle timestamps against requested time interval boundaries.

    Tolerance window: [requested_start - tolerance, requested_end + tolerance).
    Severity: BLOCK.
    Fails if any candle timestamp lies outside the window boundaries.
    """
    eval_time = evaluated_at_utc or datetime.now(UTC)
    req_start = (
        requested_start
        if requested_start.tzinfo is not None
        else requested_start.replace(tzinfo=UTC)
    )
    req_end = (
        requested_end if requested_end.tzinfo is not None else requested_end.replace(tzinfo=UTC)
    )

    win_start = req_start - timedelta(hours=tolerance_hours)
    win_end = req_end + timedelta(hours=tolerance_hours)

    outliers: list[datetime] = []
    for c in candles:
        ts = getattr(c, "candle_start_utc", None) or getattr(c, "timestamp_utc", None)
        if ts is not None:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            if ts < win_start or ts >= win_end:
                outliers.append(ts)

    if outliers:
        return QualityCheckRecord(
            check_id=str(uuid.uuid4()),
            run_id=run_id,
            rule_name="boundary_reconciliation",
            severity="BLOCK",
            status="FAILED",
            metric_value=float(len(outliers)),
            threshold_value=0.0,
            details=(
                f"Found {len(outliers)} candle(s) outside requested window "
                f"[{req_start.isoformat()}, {req_end.isoformat()}). "
                f"Sample outlier: {outliers[0].isoformat()}"
            ),
            evaluated_at_utc=eval_time,
        )

    return QualityCheckRecord(
        check_id=str(uuid.uuid4()),
        run_id=run_id,
        rule_name="boundary_reconciliation",
        severity="BLOCK",
        status="PASSED",
        metric_value=0.0,
        threshold_value=0.0,
        details=(
            f"All {len(candles)} candle(s) within window boundaries "
            f"[{req_start.isoformat()}, {req_end.isoformat()})."
        ),
        evaluated_at_utc=eval_time,
    )


def _extract_candle_timestamp(c: Any) -> datetime:
    ts = getattr(c, "candle_start_utc", None) or getattr(c, "timestamp_utc", None)
    if isinstance(ts, datetime):
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=UTC)
    return datetime.min.replace(tzinfo=UTC)


def _extract_volume(c: Any) -> float:
    vol = getattr(c, "volume_base", None)
    if vol is None:
        vol = getattr(c, "volume", 0)
    if vol is None:
        return 0.0
    try:
        return float(vol)
    except (TypeError, ValueError):
        return 0.0


def check_price_return_anomaly(
    candles: Sequence[Any],
    threshold_pct: float = 0.15,
    run_id: str = "",
    *,
    evaluated_at_utc: datetime | None = None,
) -> QualityCheckRecord:
    """Check for anomalous hourly price movements exceeding threshold_pct (default 15%).

    Severity: WARN.
    Evaluates both intrabar return |close - open| / open and consecutive bar return
    |close_t - close_{t-1}| / close_{t-1}.
    """
    eval_time = evaluated_at_utc or datetime.now(UTC)
    if not candles:
        return QualityCheckRecord(
            check_id=str(uuid.uuid4()),
            run_id=run_id,
            rule_name="price_return_anomaly",
            severity="WARN",
            status="PASSED",
            metric_value=0.0,
            threshold_value=threshold_pct,
            details="No candles to evaluate for price return anomaly.",
            evaluated_at_utc=eval_time,
        )

    sorted_candles = sorted(candles, key=_extract_candle_timestamp)

    max_return = 0.0
    max_return_detail = ""

    for i, c in enumerate(sorted_candles):
        open_p = float(getattr(c, "open", 0))
        close_p = float(getattr(c, "close", 0))
        if open_p > 0:
            intra_ret = abs(close_p - open_p) / open_p
            if intra_ret > max_return:
                max_return = intra_ret
                ts_str = str(getattr(c, "candle_start_utc", ""))
                max_return_detail = f"intrabar return {intra_ret:.2%} at {ts_str}"

        if i > 0:
            prev_close = float(getattr(sorted_candles[i - 1], "close", 0))
            if prev_close > 0:
                bar_ret = abs(close_p - prev_close) / prev_close
                if bar_ret > max_return:
                    max_return = bar_ret
                    ts_str = str(getattr(c, "candle_start_utc", ""))
                    max_return_detail = f"interbar return {bar_ret:.2%} at {ts_str}"

    if max_return > threshold_pct:
        return QualityCheckRecord(
            check_id=str(uuid.uuid4()),
            run_id=run_id,
            rule_name="price_return_anomaly",
            severity="WARN",
            status="FAILED",
            metric_value=round(max_return, 4),
            threshold_value=threshold_pct,
            details=(
                f"Price return anomaly detected: max return {max_return:.2%} "
                f"exceeds threshold {threshold_pct:.2%} ({max_return_detail})"
            ),
            evaluated_at_utc=eval_time,
        )

    return QualityCheckRecord(
        check_id=str(uuid.uuid4()),
        run_id=run_id,
        rule_name="price_return_anomaly",
        severity="WARN",
        status="PASSED",
        metric_value=round(max_return, 4),
        threshold_value=threshold_pct,
        details=f"Price returns within normal bounds (max return: {max_return:.2%})",
        evaluated_at_utc=eval_time,
    )


def check_volume_spike_anomaly(
    candles: Sequence[Any],
    spike_multiplier: float = 5.0,
    run_id: str = "",
    *,
    baseline_volume: float | None = None,
    evaluated_at_utc: datetime | None = None,
) -> QualityCheckRecord:
    """Check for anomalous volume spikes exceeding spike_multiplier * average volume (default 5.0x).

    Severity: WARN.
    """
    eval_time = evaluated_at_utc or datetime.now(UTC)
    if len(candles) < 2 and baseline_volume is None:
        return QualityCheckRecord(
            check_id=str(uuid.uuid4()),
            run_id=run_id,
            rule_name="volume_spike_anomaly",
            severity="WARN",
            status="PASSED",
            metric_value=0.0,
            threshold_value=spike_multiplier,
            details="Insufficient candles (< 2) and no baseline for volume spike check.",
            evaluated_at_utc=eval_time,
        )

    volumes = [_extract_volume(c) for c in candles]
    if baseline_volume is not None:
        avg_vol = baseline_volume
    elif len(volumes) >= 3:
        # Exclude maximum outlier from baseline to prevent contamination
        non_max = sorted(volumes)[:-1]
        avg_vol = (
            sum(non_max) / len(non_max)
            if any(v > 0 for v in non_max)
            else (sum(volumes) / len(volumes))
        )
    else:
        avg_vol = sum(volumes) / len(volumes) if volumes else 0.0

    if avg_vol <= 0:
        max_ratio = 0.0
    else:
        max_vol = max(volumes) if volumes else 0.0
        max_ratio = max_vol / avg_vol

    if max_ratio > spike_multiplier:
        return QualityCheckRecord(
            check_id=str(uuid.uuid4()),
            run_id=run_id,
            rule_name="volume_spike_anomaly",
            severity="WARN",
            status="FAILED",
            metric_value=round(max_ratio, 2),
            threshold_value=spike_multiplier,
            details=(
                f"Volume spike anomaly detected: max volume ratio {max_ratio:.2f}x "
                f"exceeds threshold {spike_multiplier:.2f}x (avg: {avg_vol:.2f})"
            ),
            evaluated_at_utc=eval_time,
        )

    return QualityCheckRecord(
        check_id=str(uuid.uuid4()),
        run_id=run_id,
        rule_name="volume_spike_anomaly",
        severity="WARN",
        status="PASSED",
        metric_value=round(max_ratio, 2),
        threshold_value=spike_multiplier,
        details=f"Volume within normal bounds (max ratio: {max_ratio:.2f}x)",
        evaluated_at_utc=eval_time,
    )


def run_dataset_quality_checks(
    candles: Sequence[Any],
    run_id: str = "",
    *,
    requested_start: datetime | None = None,
    requested_end: datetime | None = None,
    price_threshold_pct: float = 0.15,
    volume_spike_multiplier: float = 5.0,
    evaluated_at_utc: datetime | None = None,
) -> list[QualityCheckRecord]:
    """Run all dataset-level quality checks on a candle sequence."""
    eval_time = evaluated_at_utc or datetime.now(UTC)
    records: list[QualityCheckRecord] = []

    records.append(check_natural_key_uniqueness(candles, run_id=run_id, evaluated_at_utc=eval_time))
    if requested_start is not None and requested_end is not None:
        records.append(
            check_boundary_reconciliation(
                candles,
                requested_start,
                requested_end,
                run_id=run_id,
                evaluated_at_utc=eval_time,
            )
        )
    records.append(
        check_price_return_anomaly(
            candles,
            threshold_pct=price_threshold_pct,
            run_id=run_id,
            evaluated_at_utc=eval_time,
        )
    )
    records.append(
        check_volume_spike_anomaly(
            candles,
            spike_multiplier=volume_spike_multiplier,
            run_id=run_id,
            evaluated_at_utc=eval_time,
        )
    )
    return records
