"""Atomic storage writer for immutable raw gzip JSON response envelopes."""

import gzip
import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.time_range import parse_iso_utc

SCHEMA_VERSION = 1
SOURCE_NAME = "coinbase_exchange"
ENDPOINT_NAME = "product_candles"


class StorageError(Exception):
    """Raised when an error occurs during raw envelope persistence or verification."""


def compute_payload_sha256(payload: Any) -> str:
    """Compute the SHA-256 hex digest of a canonical JSON payload.

    Canonical JSON is serialized with sorted keys and compact separators (',', ':').
    """
    canonical_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def format_envelope_filename(run_id: str, start_utc: datetime, end_utc: datetime) -> str:
    """Format envelope filename following the pattern:

    {run_id}_{YYYYMMDDTHHZ}_{YYYYMMDDTHHZ}.json.gz
    """
    start_fmt = start_utc.astimezone(UTC).strftime("%Y%m%dT%HZ")
    end_fmt = end_utc.astimezone(UTC).strftime("%Y%m%dT%HZ")
    return f"{run_id}_{start_fmt}_{end_fmt}.json.gz"


def create_raw_envelope(
    *,
    run_id: str,
    product_id: str,
    granularity_seconds: int,
    start_utc: datetime,
    end_utc: datetime,
    retrieved_at_utc: datetime,
    http_status: int,
    payload: list[Any],
    provider_request_id: str | None = None,
) -> dict[str, Any]:
    """Construct a validated raw envelope dictionary adhering to the Phase 1B schema."""
    start_str = start_utc.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_utc.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    retrieved_str = retrieved_at_utc.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    http_dict: dict[str, Any] = {
        "status": http_status,
        "provider_request_id": provider_request_id,
    }

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "source": SOURCE_NAME,
        "endpoint": ENDPOINT_NAME,
        "request": {
            "product_id": product_id,
            "granularity_seconds": granularity_seconds,
            "start_utc": start_str,
            "end_utc": end_str,
        },
        "retrieved_at_utc": retrieved_str,
        "http": http_dict,
        "payload_sha256": compute_payload_sha256(payload),
        "candle_count": len(payload),
        "payload": payload,
    }


def write_raw_envelope(
    output_dir: Path | str,
    envelope: dict[str, Any],
) -> Path:
    """Atomically write a raw gzip JSON envelope to disk.

    Steps:
    1. Ensures target directory exists.
    2. Writes compressed gzip bytes to a temporary file ({filename}.{uuid}.tmp).
    3. Flushes and syncs to disk.
    4. Atomically renames temporary file to the final .json.gz path.

    Returns:
        The final Path to the written envelope.

    Raises:
        StorageError: If disk write or file rename fails.
    """
    out_dir = Path(output_dir)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise StorageError(f"Failed to create output directory {out_dir}: {exc}") from exc

    try:
        run_id = envelope["run_id"]
        start_utc = parse_iso_utc(envelope["request"]["start_utc"])
        end_utc = parse_iso_utc(envelope["request"]["end_utc"])
    except Exception as exc:
        raise StorageError(f"Invalid envelope metadata: {exc}") from exc

    filename = format_envelope_filename(run_id, start_utc, end_utc)
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
    except Exception as exc:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise StorageError(f"Failed writing raw envelope {final_path}: {exc}") from exc

    return final_path


def read_raw_envelope(path: Path | str) -> dict[str, Any]:
    """Read and decompress a raw gzip JSON envelope from disk.

    Raises:
        StorageError: If file cannot be read, decompressed, or parsed as JSON.
    """
    file_path = Path(path)
    try:
        compressed_bytes = file_path.read_bytes()
        decompressed_text = gzip.decompress(compressed_bytes).decode("utf-8")
        envelope = json.loads(decompressed_text)
        if not isinstance(envelope, dict):
            raise StorageError(f"Envelope at {file_path} is not a JSON object")
        return envelope
    except Exception as exc:
        raise StorageError(f"Failed to read raw envelope {file_path}: {exc}") from exc


def verify_raw_envelope(envelope: dict[str, Any]) -> bool:
    """Verify schema version, candle count, and SHA-256 checksum integrity of an envelope."""
    try:
        if envelope.get("schema_version") != SCHEMA_VERSION:
            return False
        payload = envelope.get("payload")
        if not isinstance(payload, list):
            return False
        if envelope.get("candle_count") != len(payload):
            return False
        expected_sha = compute_payload_sha256(payload)
        return envelope.get("payload_sha256") == expected_sha
    except Exception:
        return False
