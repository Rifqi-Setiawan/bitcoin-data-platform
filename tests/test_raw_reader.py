"""Tests for raw envelope reader."""

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from bitcoin_data_platform.storage.raw_writer import create_raw_envelope, write_raw_envelope
from bitcoin_data_platform.transforms.raw_reader import (
    RawEnvelope,
    RawReaderError,
    read_raw_envelopes,
)


def _make_test_envelope(
    run_id: str = "run-001",
    start_utc: datetime = datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
    end_utc: datetime = datetime(2026, 1, 1, 5, 0, tzinfo=UTC),
    payload: list | None = None,
    schema_version: int = 1,
) -> dict:
    if payload is None:
        payload = [
            [1767225600, 95000, 96000, 95200, 95800, 10.5],
        ]
    env = create_raw_envelope(
        run_id=run_id,
        product_id="BTC-USD",
        granularity_seconds=3600,
        start_utc=start_utc,
        end_utc=end_utc,
        retrieved_at_utc=datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        http_status=200,
        payload=payload,
    )
    if schema_version != 1:
        env["schema_version"] = schema_version
    return env


def test_read_single_valid_envelope(tmp_path: Path) -> None:
    """1. Read single valid envelope."""
    envelope_dict = _make_test_envelope(run_id="test-run-1")
    file_path = write_raw_envelope(tmp_path, envelope_dict)
    assert file_path.exists()

    envelopes = read_raw_envelopes(tmp_path)
    assert len(envelopes) == 1
    env = envelopes[0]
    assert isinstance(env, RawEnvelope)
    assert env.run_id == "test-run-1"
    assert env.product_id == "BTC-USD"
    assert env.granularity_seconds == 3600
    assert env.start_utc == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert env.end_utc == datetime(2026, 1, 1, 5, 0, tzinfo=UTC)
    assert env.candle_count == 1
    assert len(env.payload) == 1


def test_read_multiple_envelopes_sorted_by_start_utc(tmp_path: Path) -> None:
    """2. Read multiple envelopes, sorted by start_utc."""
    # Write in non-chronological order
    env2 = _make_test_envelope(
        run_id="run-2",
        start_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
    )
    env1 = _make_test_envelope(
        run_id="run-1",
        start_utc=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
    )
    env3 = _make_test_envelope(
        run_id="run-3",
        start_utc=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 1, 4, 0, 0, tzinfo=UTC),
    )

    write_raw_envelope(tmp_path, env2)
    write_raw_envelope(tmp_path, env1)
    write_raw_envelope(tmp_path, env3)

    envelopes = read_raw_envelopes(tmp_path)
    assert len(envelopes) == 3
    assert [e.run_id for e in envelopes] == ["run-1", "run-2", "run-3"]
    assert envelopes[0].start_utc < envelopes[1].start_utc < envelopes[2].start_utc


def test_skip_non_json_gz_files_gracefully(tmp_path: Path) -> None:
    """3. Skip non-.json.gz files gracefully."""
    # Create valid envelope
    env = _make_test_envelope(run_id="run-valid")
    write_raw_envelope(tmp_path, env)

    # Create dummy extraneous files
    (tmp_path / "notes.txt").write_text("not an envelope")
    (tmp_path / "data.json").write_text('{"uncompressed": true}')
    (tmp_path / "some.tmp").write_bytes(b"temp binary")
    (tmp_path / "sub_dir").mkdir()

    envelopes = read_raw_envelopes(tmp_path)
    assert len(envelopes) == 1
    assert envelopes[0].run_id == "run-valid"


def test_handle_empty_directory(tmp_path: Path) -> None:
    """4. Handle empty directory (returns empty list)."""
    envelopes = read_raw_envelopes(tmp_path)
    assert envelopes == []

    # Non-existent directory should also return [] gracefully
    non_existent = tmp_path / "does_not_exist"
    assert read_raw_envelopes(non_existent) == []


def test_validate_envelope_schema_version(tmp_path: Path) -> None:
    """5. Validate envelope schema_version."""
    env = _make_test_envelope(run_id="run-bad-ver", schema_version=99)

    # Write directly since write_raw_envelope doesn't restrict schema_version
    filename = "run-bad-ver_20260101T00Z_20260101T05Z.json.gz"
    file_path = tmp_path / filename
    raw_bytes = gzip.compress(json.dumps(env).encode("utf-8"))
    file_path.write_bytes(raw_bytes)

    with pytest.raises(RawReaderError) as exc_info:
        read_raw_envelopes(tmp_path)

    assert "Unsupported schema version 99" in str(exc_info.value)
