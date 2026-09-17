"""Reader and parser for raw gzip JSON response envelopes."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.storage.raw_writer import SCHEMA_VERSION, read_raw_envelope
from bitcoin_data_platform.time_range import parse_iso_utc


class RawReaderError(ValueError):
    """Raised when an error occurs during raw envelope reading or schema validation."""


@dataclass(frozen=True)
class RawEnvelope:
    """Parsed raw gzip JSON envelope."""

    schema_version: int
    run_id: str
    source: str
    endpoint: str
    product_id: str
    granularity_seconds: int
    start_utc: datetime
    end_utc: datetime
    retrieved_at_utc: datetime
    http_status: int
    payload_sha256: str
    candle_count: int
    payload: list[Any]


def parse_raw_envelope_dict(
    envelope_dict: dict[str, Any], source_path: Path | None = None
) -> RawEnvelope:
    """Parse a raw envelope dictionary into a typed RawEnvelope dataclass.

    Raises:
        RawReaderError: If schema_version is unsupported or required fields are missing.
    """
    path_suffix = f" in {source_path}" if source_path is not None else ""

    schema_ver = envelope_dict.get("schema_version")
    if schema_ver != SCHEMA_VERSION:
        raise RawReaderError(
            f"Unsupported schema version {schema_ver}{path_suffix} (expected {SCHEMA_VERSION})"
        )

    try:
        run_id = envelope_dict["run_id"]
        source = envelope_dict.get("source", "coinbase_exchange")
        endpoint = envelope_dict.get("endpoint", "product_candles")
        request = envelope_dict["request"]
        product_id = request["product_id"]
        granularity_seconds = int(request["granularity_seconds"])
        start_utc = parse_iso_utc(request["start_utc"])
        end_utc = parse_iso_utc(request["end_utc"])
        retrieved_at_utc = parse_iso_utc(envelope_dict["retrieved_at_utc"])
        http = envelope_dict.get("http", {})
        http_status = int(http.get("status", 200))
        payload_sha256 = envelope_dict["payload_sha256"]
        candle_count = int(envelope_dict["candle_count"])
        payload = envelope_dict.get("payload", [])
    except (KeyError, TypeError, ValueError) as exc:
        raise RawReaderError(f"Malformed envelope data{path_suffix}: {exc}") from exc

    return RawEnvelope(
        schema_version=schema_ver,
        run_id=run_id,
        source=source,
        endpoint=endpoint,
        product_id=product_id,
        granularity_seconds=granularity_seconds,
        start_utc=start_utc,
        end_utc=end_utc,
        retrieved_at_utc=retrieved_at_utc,
        http_status=http_status,
        payload_sha256=payload_sha256,
        candle_count=candle_count,
        payload=payload,
    )


def read_raw_envelopes(raw_dir: Path | str) -> list[RawEnvelope]:
    """Read and parse all .json.gz envelopes from raw_dir.

    Skips non-.json.gz files gracefully.
    Returns a list of RawEnvelope instances sorted by start_utc ascending.
    """
    raw_path = Path(raw_dir)
    if not raw_path.is_dir():
        return []

    envelopes = [
        parse_raw_envelope_dict(read_raw_envelope(file_path), source_path=file_path)
        for file_path in raw_path.glob("*.json.gz")
        if file_path.is_file()
    ]
    envelopes.sort(key=lambda env: (env.start_utc, env.run_id))
    return envelopes
