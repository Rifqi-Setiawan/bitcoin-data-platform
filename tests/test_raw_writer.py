"""Tests for raw gzip JSON envelope storage writer."""

import gzip
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from bitcoin_data_platform.storage.raw_writer import (
    StorageError,
    compute_payload_sha256,
    create_raw_envelope,
    format_envelope_filename,
    read_raw_envelope,
    verify_raw_envelope,
    write_raw_envelope,
)


def _sample_envelope(run_id: str = "run-test-123") -> dict[str, Any]:
    payload = [
        [1767225600, 95000, 96000, 95200, 96000, 10],
        [1767229200, 96000, 97000, 96000, 96800, 15],
    ]
    return create_raw_envelope(
        run_id=run_id,
        product_id="BTC-USD",
        granularity_seconds=3600,
        start_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 2, 0, tzinfo=UTC),
        retrieved_at_utc=datetime(2026, 1, 1, 2, 5, tzinfo=UTC),
        http_status=200,
        payload=payload,
        provider_request_id="cb-req-999",
    )


def test_valid_gzip(tmp_path: Path) -> None:
    """1. Written file is valid gzip and decompresses to matching JSON object."""
    envelope = _sample_envelope()
    file_path = write_raw_envelope(tmp_path, envelope)

    assert file_path.exists()
    assert file_path.suffix == ".gz"

    # Manually decompress with stdlib gzip
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        loaded = json.load(f)

    assert loaded["schema_version"] == 1
    assert loaded["run_id"] == "run-test-123"
    assert loaded["source"] == "coinbase_exchange"
    assert loaded["candle_count"] == 2

    # Also test via read_raw_envelope helper
    read_back = read_raw_envelope(file_path)
    assert read_back == loaded


def test_checksum_match() -> None:
    """2. SHA-256 payload checksum matches canonical JSON payload serialization."""
    payload = [[1767225600, 95000, 96000, 95200, 96000, 10]]
    envelope = create_raw_envelope(
        run_id="run-sha",
        product_id="BTC-USD",
        granularity_seconds=3600,
        start_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
        retrieved_at_utc=datetime(2026, 1, 1, 1, 5, tzinfo=UTC),
        http_status=200,
        payload=payload,
    )

    expected_sha = compute_payload_sha256(payload)
    assert envelope["payload_sha256"] == expected_sha
    assert verify_raw_envelope(envelope) is True

    # Tamper payload and verify checksum failure
    tampered = dict(envelope)
    tampered["payload"] = [[1767225600, 95000, 96000, 95200, 96000, 999]]
    assert verify_raw_envelope(tampered) is False


def test_atomic_success(tmp_path: Path) -> None:
    """3. Atomic write succeeds: temp file cleaned up, only final .json.gz file exists."""
    envelope = _sample_envelope()
    final_path = write_raw_envelope(tmp_path, envelope)

    # Check that only the final file exists in tmp_path (no .tmp files left)
    all_files = list(tmp_path.iterdir())
    assert all_files == [final_path]
    assert not any(f.name.endswith(".tmp") for f in all_files)


def test_atomic_failure(tmp_path: Path) -> None:
    """4. On write failure, temp file is cleaned up and no final file is created."""
    envelope = _sample_envelope()

    # Simulate failure during os.replace
    with patch("os.replace", side_effect=OSError("Disk full simulation")):
        with pytest.raises(StorageError) as exc_info:
            write_raw_envelope(tmp_path, envelope)

        assert "Disk full simulation" in str(exc_info.value)

    # No files left behind (neither final nor temp)
    remaining_files = list(tmp_path.iterdir())
    assert len(remaining_files) == 0


def test_no_in_place_edit(tmp_path: Path) -> None:
    """5. Re-writing does not edit file in-place, uses temp file then atomic os.replace."""
    envelope = _sample_envelope()
    final_path = write_raw_envelope(tmp_path, envelope)
    assert final_path.exists()

    replace_called = False
    original_replace = os.replace

    def spy_replace(src: Any, dst: Any) -> None:
        nonlocal replace_called
        replace_called = True
        assert str(src).endswith(".tmp")
        original_replace(src, dst)

    with patch("os.replace", side_effect=spy_replace):
        write_raw_envelope(tmp_path, envelope)

    assert replace_called is True


def test_filename_format() -> None:
    """6. Filename matches {run_id}_{YYYYMMDDTHHZ}_{YYYYMMDDTHHZ}.json.gz pattern."""
    run_id = "abc-123-xyz"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 13, 12, 0, tzinfo=UTC)

    name = format_envelope_filename(run_id, start, end)
    assert name == "abc-123-xyz_20260101T00Z_20260113T12Z.json.gz"


def test_mkdir(tmp_path: Path) -> None:
    """7. Output directory and parent directories are automatically created if missing."""
    deep_dir = tmp_path / "deep" / "nested" / "raw"
    assert not deep_dir.exists()

    envelope = _sample_envelope()
    final_path = write_raw_envelope(deep_dir, envelope)

    assert deep_dir.exists()
    assert final_path.exists()


def test_schema_fields() -> None:
    """8. Envelope contains all required Phase 1B schema fields with correct types."""
    envelope = _sample_envelope()

    assert envelope["schema_version"] == 1
    assert isinstance(envelope["run_id"], str)
    assert envelope["source"] == "coinbase_exchange"
    assert envelope["endpoint"] == "product_candles"

    req = envelope["request"]
    assert req["product_id"] == "BTC-USD"
    assert req["granularity_seconds"] == 3600
    assert req["start_utc"] == "2026-01-01T00:00:00Z"
    assert req["end_utc"] == "2026-01-01T02:00:00Z"

    assert envelope["retrieved_at_utc"] == "2026-01-01T02:05:00Z"
    assert envelope["http"] == {"status": 200, "provider_request_id": "cb-req-999"}
    assert isinstance(envelope["payload_sha256"], str)
    assert len(envelope["payload_sha256"]) == 64
    assert envelope["candle_count"] == 2
    assert isinstance(envelope["payload"], list)
