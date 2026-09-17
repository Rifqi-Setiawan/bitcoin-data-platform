"""CLI tests for backfill command."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.sources.coinbase_client import CoinbaseClient
from bitcoin_data_platform.storage.raw_writer import read_raw_envelope


def _fixed_clock_feb() -> datetime:
    return datetime(2026, 2, 1, 0, 0, tzinfo=UTC)


def _make_mock_client(
    payload: list[list[Any]] | None = None,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
) -> CoinbaseClient:
    candles = (
        payload
        if payload is not None
        else [
            [1767225600, 95000, 96000, 95200, 96000, 10],
            [1767229200, 96000, 97000, 96000, 96800, 15],
        ]
    )
    hdrs = headers or {"cb-request-id": "cb-test-123"}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=candles, headers=hdrs)

    transport = httpx.MockTransport(handler)
    return CoinbaseClient(transport=transport, min_request_interval_seconds=0.0)


def test_backfill_success_exit_0(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """1. Successful backfill executes plan -> fetch -> validate -> write and exits 0."""
    client = _make_mock_client()
    args = [
        "backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-01T02:00:00Z",
        "--output-dir",
        str(tmp_path),
    ]

    code = main(args, clock=_fixed_clock_feb, client=client)
    assert code == 0

    captured = capsys.readouterr()
    summary = json.loads(captured.out)

    assert summary["status"] == "success"
    assert summary["windows_planned"] == 1
    assert summary["windows_succeeded"] == 1
    assert summary["windows_failed"] == 0
    assert summary["candles_ingested"] == 2
    assert len(summary["files_written"]) == 1

    file_written = Path(summary["files_written"][0])
    assert file_written.exists()
    envelope = read_raw_envelope(file_written)
    assert envelope["schema_version"] == 1
    assert envelope["candle_count"] == 2


def test_backfill_invalid_input_exit_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """2. Invalid input timestamps exit 2, emit stderr, and leave stdout empty."""
    args = [
        "backfill",
        "--start",
        "2026-01-01T00:15:00Z",  # Misaligned boundary
        "--end",
        "2026-01-01T02:00:00Z",
        "--output-dir",
        str(tmp_path),
    ]

    code = main(args, clock=_fixed_clock_feb)
    assert code == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "aligned to an exact hour" in captured.err.lower()


def test_backfill_source_unavailable_exit_3(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """3. When Coinbase returns 503 and retries exhaust, command exits 3."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="Service Unavailable")

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        max_retries=2,
        base_backoff_seconds=0.001,
        min_request_interval_seconds=0.0,
        jitter=False,
    )

    args = [
        "backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-01T02:00:00Z",
        "--output-dir",
        str(tmp_path),
    ]

    code = main(args, clock=_fixed_clock_feb, client=client)
    assert code == 3

    captured = capsys.readouterr()
    assert "coinbase source unavailable" in captured.err.lower()

    summary = json.loads(captured.out)
    assert summary["status"] == "failure"
    assert summary["windows_failed"] == 1
    assert summary["windows_succeeded"] == 0


def test_backfill_contract_violation_exit_4(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """4. When response payload violates contract (e.g. high < low), command exits 4."""
    bad_payload = [
        [1767225600, 96000, 95000, 95500, 95500, 10],  # Invalid: high < low
    ]
    client = _make_mock_client(payload=bad_payload)

    args = [
        "backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-01T02:00:00Z",
        "--output-dir",
        str(tmp_path),
    ]

    code = main(args, clock=_fixed_clock_feb, client=client)
    assert code == 4

    captured = capsys.readouterr()
    assert "contract violation" in captured.err.lower()

    summary = json.loads(captured.out)
    assert summary["status"] == "failure"
    assert summary["windows_failed"] == 1


def test_backfill_storage_failure_exit_5(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """5. When raw envelope write fails (disk/filesystem error), command exits 5."""
    client = _make_mock_client()
    args = [
        "backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-01T02:00:00Z",
        "--output-dir",
        str(tmp_path),
    ]

    mock_err = OSError("Read-only filesystem")
    with patch("bitcoin_data_platform.cli.write_raw_envelope", side_effect=mock_err):
        code = main(args, clock=_fixed_clock_feb, client=client)
        assert code == 5

    captured = capsys.readouterr()
    assert "storage failure" in captured.err.lower()

    summary = json.loads(captured.out)
    assert summary["status"] == "failure"
    assert summary["windows_failed"] == 1


def test_backfill_partial_failure(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """6. Multi-window backfill persists completed windows prior to a failure."""
    # 301 hours = 2 windows (300 hours + 1 hour)
    window_call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal window_call_count
        window_call_count += 1
        if window_call_count == 1:
            # Window 0 succeeds
            return httpx.Response(
                200,
                json=[[1767225600, 95000, 96000, 95200, 96000, 10]],
                headers={"cb-request-id": "req-win-0"},
            )
        # Window 1 fails with 503
        return httpx.Response(503, text="Service Unavailable")

    transport = httpx.MockTransport(handler)
    client = CoinbaseClient(
        transport=transport,
        max_retries=2,
        base_backoff_seconds=0.001,
        min_request_interval_seconds=0.0,
        jitter=False,
    )

    args = [
        "backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-13T13:00:00Z",  # 301 hours = 2 windows
        "--output-dir",
        str(tmp_path),
    ]

    code = main(args, clock=_fixed_clock_feb, client=client)
    assert code == 3  # Exits with source unavailable code

    captured = capsys.readouterr()
    summary = json.loads(captured.out)

    assert summary["windows_planned"] == 2
    assert summary["windows_succeeded"] == 1
    assert summary["windows_failed"] == 1
    assert len(summary["files_written"]) == 1

    # First window's envelope file was successfully written and preserved
    persisted_file = Path(summary["files_written"][0])
    assert persisted_file.exists()
    envelope = read_raw_envelope(persisted_file)
    assert envelope["candle_count"] == 1


def test_backfill_custom_output_dir(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """7. Custom output directory creates specified tree and stores envelopes there."""
    custom_dir = tmp_path / "custom" / "storage" / "raw"
    assert not custom_dir.exists()

    client = _make_mock_client()
    args = [
        "backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-01T01:00:00Z",
        "--output-dir",
        str(custom_dir),
    ]

    code = main(args, clock=_fixed_clock_feb, client=client)
    assert code == 0

    assert custom_dir.exists()
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["output_dir"] == str(custom_dir)
    assert len(summary["files_written"]) == 1
    assert str(custom_dir) in summary["files_written"][0]


def test_backfill_summary_counts(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """8. Summary output contains all required tracking counts and schema attributes."""
    client = _make_mock_client()
    args = [
        "backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-01T02:00:00Z",
        "--output-dir",
        str(tmp_path),
    ]

    code = main(args, clock=_fixed_clock_feb, client=client)
    assert code == 0

    captured = capsys.readouterr()
    summary = json.loads(captured.out)

    assert "run_id" in summary
    assert summary["status"] == "success"
    assert summary["requested_start_utc"] == "2026-01-01T00:00:00Z"
    assert summary["requested_end_utc"] == "2026-01-01T02:00:00Z"
    assert summary["windows_planned"] == 1
    assert summary["windows_succeeded"] == 1
    assert summary["windows_failed"] == 0
    assert summary["candles_ingested"] == 2
    assert summary["output_dir"] == str(tmp_path)
    assert isinstance(summary["files_written"], list)
