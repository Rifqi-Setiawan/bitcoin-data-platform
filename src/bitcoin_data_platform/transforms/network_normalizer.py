"""Normalizer and deduplicator for Coin Metrics on-chain network metrics."""

import gzip
import json
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from bitcoin_data_platform.sources.coin_metrics_contract import (
    CoinMetricsContractViolationError,
    CoinMetricsRecord,
    validate_record,
)
from bitcoin_data_platform.storage.raw_writer import compute_payload_sha256
from bitcoin_data_platform.time_range import parse_iso_utc

NETWORK_SCHEMA_VERSION = 1
NETWORK_SOURCE_NAME = "coin_metrics"
NETWORK_ENDPOINT_NAME = "timeseries_asset_metrics"


@dataclass(frozen=True)
class NormalizedNetworkMetric:
    """Typed, validated daily on-chain network metric ready for Parquet."""

    source: str  # "coin_metrics"
    asset: str  # "btc"
    metric_date_utc: datetime  # Aligned to YYYY-MM-DD 00:00:00 UTC
    transaction_count: int  # Parsed from TxCnt (non-negative)
    active_addresses_count: int  # Parsed from AdrActCnt (non-negative)
    ingested_at_utc: datetime
    source_run_id: str
    mvrv_ratio: Decimal | None = None


@dataclass(frozen=True)
class NetworkRawEnvelope:
    """Parsed raw gzip JSON envelope containing on-chain network metrics."""

    schema_version: int
    run_id: str
    source: str
    endpoint: str
    asset: str
    metrics: str
    frequency: str
    start_time: str
    end_time: str
    retrieved_at_utc: datetime
    http_status: int
    payload_sha256: str
    record_count: int
    payload: dict[str, Any] | list[Any]


compute_network_payload_sha256 = compute_payload_sha256


def create_network_raw_envelope(
    *,
    run_id: str,
    asset: str = "btc",
    metrics: str = "TxCnt,AdrActCnt,CapMVRVCur",
    frequency: str = "1d",
    start_time: str,
    end_time: str,
    retrieved_at_utc: datetime,
    http_status: int,
    payload: dict[str, Any] | list[Any],
    provider_request_id: str | None = None,
    source: str = NETWORK_SOURCE_NAME,
    endpoint: str = NETWORK_ENDPOINT_NAME,
) -> dict[str, Any]:
    """Construct a validated raw envelope dictionary for network metrics."""
    retrieved_str = retrieved_at_utc.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    records = payload.get("data", []) if isinstance(payload, dict) else payload

    return {
        "schema_version": NETWORK_SCHEMA_VERSION,
        "run_id": run_id,
        "source": source,
        "endpoint": endpoint,
        "request": {
            "asset": asset,
            "metrics": metrics,
            "frequency": frequency,
            "start_time": start_time,
            "end_time": end_time,
        },
        "retrieved_at_utc": retrieved_str,
        "http": {
            "status": http_status,
            "provider_request_id": provider_request_id,
        },
        "payload_sha256": compute_network_payload_sha256(payload),
        "record_count": len(records),
        "payload": payload,
    }


def write_network_raw_envelope(output_dir: Path | str, envelope: dict[str, Any]) -> Path:
    """Atomically write a raw gzip JSON network envelope to disk."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    run_id = envelope["run_id"]
    req = envelope.get("request", {})
    start_str = req.get("start_time", "start").replace("-", "").replace(":", "")
    end_str = req.get("end_time", "end").replace("-", "").replace(":", "")
    filename = f"{run_id}_{start_str}_{end_str}.json.gz"

    final_path = out_dir / filename
    temp_path = out_dir / f"{filename}.{uuid.uuid4().hex}.tmp"

    try:
        raw_json = json.dumps(envelope, indent=2)
        compressed_bytes = gzip.compress(raw_json.encode("utf-8"))

        with open(temp_path, "wb") as f:
            f.write(compressed_bytes)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_path, final_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise

    return final_path


def parse_network_raw_envelope_dict(
    envelope_dict: dict[str, Any], source_path: Path | None = None
) -> NetworkRawEnvelope:
    """Parse raw envelope dict into NetworkRawEnvelope dataclass."""
    path_suffix = f" in {source_path}" if source_path is not None else ""
    schema_ver = envelope_dict.get("schema_version")
    if schema_ver != NETWORK_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported schema version {schema_ver}{path_suffix} "
            f"(expected {NETWORK_SCHEMA_VERSION})"
        )

    try:
        run_id = envelope_dict["run_id"]
        source = envelope_dict.get("source", NETWORK_SOURCE_NAME)
        endpoint = envelope_dict.get("endpoint", NETWORK_ENDPOINT_NAME)
        req = envelope_dict["request"]
        asset = req.get("asset", "btc")
        metrics = req.get("metrics", "TxCnt,AdrActCnt")
        frequency = req.get("frequency", "1d")
        start_time = str(req.get("start_time", ""))
        end_time = str(req.get("end_time", ""))
        retrieved_at_utc = parse_iso_utc(envelope_dict["retrieved_at_utc"])
        http_status = int(envelope_dict.get("http", {}).get("status", 200))
        payload_sha256 = envelope_dict["payload_sha256"]
        record_count = int(envelope_dict.get("record_count", 0))
        payload = envelope_dict.get("payload", {})
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Malformed envelope data{path_suffix}: {exc}") from exc

    return NetworkRawEnvelope(
        schema_version=schema_ver,
        run_id=run_id,
        source=source,
        endpoint=endpoint,
        asset=asset,
        metrics=metrics,
        frequency=frequency,
        start_time=start_time,
        end_time=end_time,
        retrieved_at_utc=retrieved_at_utc,
        http_status=http_status,
        payload_sha256=payload_sha256,
        record_count=record_count,
        payload=payload,
    )


def read_network_raw_envelopes(raw_dir: Path | str) -> list[NetworkRawEnvelope]:
    """Read all .json.gz network envelopes from raw_dir."""
    raw_path = Path(raw_dir)
    if not raw_path.is_dir():
        return []

    envelopes: list[NetworkRawEnvelope] = []
    for file_path in raw_path.glob("*.json.gz"):
        if not file_path.is_file():
            continue
        try:
            decompressed = gzip.decompress(file_path.read_bytes()).decode("utf-8")
            env = parse_network_raw_envelope_dict(json.loads(decompressed), source_path=file_path)
            envelopes.append(env)
        except Exception:
            continue

    envelopes.sort(key=lambda env: (env.start_time, env.run_id))
    return envelopes


def normalize_network_record(
    raw_record: Any,
    *,
    source: str = NETWORK_SOURCE_NAME,
    ingested_at_utc: datetime,
    source_run_id: str,
) -> NormalizedNetworkMetric:
    """Normalize a raw record or CoinMetricsRecord into NormalizedNetworkMetric."""
    if isinstance(raw_record, CoinMetricsRecord):
        return NormalizedNetworkMetric(
            source=source,
            asset=raw_record.asset,
            metric_date_utc=raw_record.time_utc,
            transaction_count=raw_record.tx_count,
            active_addresses_count=raw_record.active_addresses,
            ingested_at_utc=ingested_at_utc,
            source_run_id=source_run_id,
            mvrv_ratio=raw_record.mvrv_ratio,
        )

    rec, violations = validate_record(raw_record)
    if violations or rec is None:
        raise CoinMetricsContractViolationError("; ".join(violations))

    return NormalizedNetworkMetric(
        source=source,
        asset=rec.asset,
        metric_date_utc=rec.time_utc,
        transaction_count=rec.tx_count,
        active_addresses_count=rec.active_addresses,
        ingested_at_utc=ingested_at_utc,
        source_run_id=source_run_id,
        mvrv_ratio=rec.mvrv_ratio,
    )


def normalize_network_envelopes(
    envelopes: Sequence[NetworkRawEnvelope | dict[str, Any]],
    *,
    now_utc: datetime | None = None,
) -> list[NormalizedNetworkMetric]:
    """Normalize raw network payloads into typed, deduplicated NormalizedNetworkMetric records.

    Deduplicates by natural key (source, asset, metric_date_utc).
    Conflict resolution: latest ingested_at_utc / source_run_id wins.
    Returns list of NormalizedNetworkMetric sorted by metric_date_utc ascending.
    """
    deduped: dict[tuple[str, str, datetime], NormalizedNetworkMetric] = {}

    for env in envelopes:
        if isinstance(env, NetworkRawEnvelope):
            run_id = env.run_id
            source = env.source
            ingested_at = env.retrieved_at_utc
            payload = env.payload
        else:
            run_id = str(env.get("run_id", ""))
            source = str(env.get("source", NETWORK_SOURCE_NAME))
            retrieved_str = env.get("retrieved_at_utc")
            ingested_at = (
                parse_iso_utc(retrieved_str) if retrieved_str else (now_utc or datetime.now(UTC))
            )
            payload = env.get("payload", {})

        records: list[Any] = payload.get("data", []) if isinstance(payload, dict) else payload

        for raw_item in records:
            metric = normalize_network_record(
                raw_item,
                source=source,
                ingested_at_utc=ingested_at,
                source_run_id=run_id,
            )

            natural_key = (metric.source, metric.asset, metric.metric_date_utc)
            if natural_key in deduped:
                existing = deduped[natural_key]
                if (metric.ingested_at_utc, metric.source_run_id) >= (
                    existing.ingested_at_utc,
                    existing.source_run_id,
                ):
                    deduped[natural_key] = metric
            else:
                deduped[natural_key] = metric

    return sorted(deduped.values(), key=lambda m: (m.metric_date_utc, m.source, m.asset))
