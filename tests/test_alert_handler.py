"""Tests for failure alerting, credential sanitization, and deduplication throttling."""

import io
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.infra.alert_handler import (
    dispatch_failure_alert,
    scrub_message,
)


def test_scrub_message_redacts_credentials_and_urls() -> None:
    raw_text = (
        "Failed connecting to https://user:secret123@api.exchange.coinbase.com "
        "with api_key=cb_secret_xyz and token: bearer_abc999. "
        "Log at /home/rifqisetiawan/logs/error.log"
    )
    clean = scrub_message(raw_text)

    assert "secret123" not in clean
    assert "cb_secret_xyz" not in clean
    assert "bearer_abc999" not in clean
    assert "rifqisetiawan" not in clean
    assert "[REDACTED]" in clean
    assert "/home/[USER]" in clean


def test_scrub_message_handles_empty_or_clean_text() -> None:
    assert scrub_message("") == ""
    clean = "Unit bitcoin-data.service exited with status 1"
    assert scrub_message(clean) == clean


def test_alert_dispatcher_first_alert_emitted(tmp_path: Path) -> None:
    state_file = tmp_path / "alert_state.json"
    stream = io.StringIO()
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    payload = dispatch_failure_alert(
        failed_unit="bitcoin-data.service",
        state_file=state_file,
        message="Coinbase HTTP 503 Service Unavailable",
        now_utc=now,
        output_stream=stream,
    )

    assert payload["throttled"] is False
    assert payload["failed_unit"] == "bitcoin-data.service"
    assert payload["consecutive_failures"] == 1
    assert state_file.exists()

    # Check output JSON in stream
    out_json = json.loads(stream.getvalue().strip())
    assert out_json["failed_unit"] == "bitcoin-data.service"
    assert out_json["throttled"] is False


def test_alert_dispatcher_throttles_within_two_hour_window(tmp_path: Path) -> None:
    state_file = tmp_path / "alert_state.json"
    stream1 = io.StringIO()
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    # First failure
    p1 = dispatch_failure_alert(
        failed_unit="bitcoin-data.service",
        state_file=state_file,
        message="Incremental run crashed",
        now_utc=t0,
        output_stream=stream1,
    )
    assert p1["throttled"] is False

    # Second failure 30 minutes later with same message
    t1 = t0 + timedelta(minutes=30)
    stream2 = io.StringIO()
    p2 = dispatch_failure_alert(
        failed_unit="bitcoin-data.service",
        state_file=state_file,
        message="Incremental run crashed",
        now_utc=t1,
        output_stream=stream2,
    )
    assert p2["throttled"] is True
    assert p2["consecutive_failures"] == 2


def test_alert_dispatcher_emits_when_message_changes(tmp_path: Path) -> None:
    state_file = tmp_path / "alert_state.json"
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    dispatch_failure_alert(
        failed_unit="bitcoin-data.service",
        state_file=state_file,
        message="Error A: Timeout",
        now_utc=t0,
        output_stream=io.StringIO(),
    )

    # 10 minutes later with different error message
    t1 = t0 + timedelta(minutes=10)
    p2 = dispatch_failure_alert(
        failed_unit="bitcoin-data.service",
        state_file=state_file,
        message="Error B: Contract violation",
        now_utc=t1,
        output_stream=io.StringIO(),
    )
    assert p2["throttled"] is False


def test_alert_dispatcher_emits_after_window_expires(tmp_path: Path) -> None:
    state_file = tmp_path / "alert_state.json"
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    dispatch_failure_alert(
        failed_unit="bitcoin-data.service",
        state_file=state_file,
        message="Connection dropped",
        now_utc=t0,
        output_stream=io.StringIO(),
    )

    # 2 hours and 5 minutes later (> 7200s)
    t2 = t0 + timedelta(hours=2, minutes=5)
    p2 = dispatch_failure_alert(
        failed_unit="bitcoin-data.service",
        state_file=state_file,
        message="Connection dropped",
        now_utc=t2,
        output_stream=io.StringIO(),
    )
    assert p2["throttled"] is False


def test_cli_alert_subcommand(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    state_file = tmp_path / "alert_cli_state.json"

    exit_code = main(
        [
            "alert",
            "--failed-unit",
            "bitcoin-data.service",
            "--state-file",
            str(state_file),
            "--message",
            "Service failed with token=secret_token_123",
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    err_output = captured.err.strip()
    assert err_output != ""

    payload = json.loads(err_output)
    assert payload["event"] == "service_failure_alert"
    assert payload["failed_unit"] == "bitcoin-data.service"
    assert "secret_token_123" not in payload["message"]
    assert "[REDACTED]" in payload["message"]
