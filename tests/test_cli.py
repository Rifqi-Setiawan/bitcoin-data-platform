"""CLI tests for bitcoin-data and plan-backfill command."""

import json
import subprocess
import sys
from datetime import UTC, datetime

import pytest

from bitcoin_data_platform.cli import main


def _fixed_clock_feb() -> datetime:
    return datetime(2026, 2, 1, 0, 0, tzinfo=UTC)


def _fixed_clock_sep() -> datetime:
    return datetime(2026, 9, 17, 14, 30, tzinfo=UTC)


def test_cli_help_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--help"])
    assert code == 0
    captured = capsys.readouterr()
    assert "bitcoin-data" in captured.out
    assert "plan-backfill" in captured.out


def test_cli_plan_backfill_help_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["plan-backfill", "--help"])
    assert code == 0
    captured = capsys.readouterr()
    assert "--start" in captured.out
    assert "--end" in captured.out


def test_cli_user_story_601_hours(capsys: pytest.CaptureFixture[str]) -> None:
    args = [
        "plan-backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-26T01:00:00Z",
    ]
    code = main(args, clock=_fixed_clock_feb)
    assert code == 0

    captured = capsys.readouterr()
    assert captured.err == ""

    data = json.loads(captured.out)
    assert data["schema_version"] == 1
    assert data["source"] == "coinbase_exchange"
    assert data["product_id"] == "BTC-USD"
    assert data["granularity_seconds"] == 3600
    assert data["requested_start_utc"] == "2026-01-01T00:00:00Z"
    assert data["requested_end_utc"] == "2026-01-26T01:00:00Z"
    assert data["expected_candle_count"] == 601
    assert data["window_count"] == 3

    windows = data["windows"]
    assert len(windows) == 3

    assert windows[0]["index"] == 0
    assert windows[0]["start_utc"] == "2026-01-01T00:00:00Z"
    assert windows[0]["end_utc"] == "2026-01-13T12:00:00Z"
    assert windows[0]["expected_candle_count"] == 300

    assert windows[1]["index"] == 1
    assert windows[1]["start_utc"] == "2026-01-13T12:00:00Z"
    assert windows[1]["end_utc"] == "2026-01-26T00:00:00Z"
    assert windows[1]["expected_candle_count"] == 300

    assert windows[2]["index"] == 2
    assert windows[2]["start_utc"] == "2026-01-26T00:00:00Z"
    assert windows[2]["end_utc"] == "2026-01-26T01:00:00Z"
    assert windows[2]["expected_candle_count"] == 1


def test_cli_deterministic_byte_equivalent_output(capsys: pytest.CaptureFixture[str]) -> None:
    args = [
        "plan-backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-26T01:00:00Z",
    ]

    code1 = main(args, clock=_fixed_clock_feb)
    out1 = capsys.readouterr().out

    code2 = main(args, clock=_fixed_clock_feb)
    out2 = capsys.readouterr().out

    assert code1 == 0
    assert code2 == 0
    assert out1 == out2
    assert out1.encode("utf-8") == out2.encode("utf-8")


def test_cli_normalizes_z_and_plus_zero_to_identical_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    args_z = [
        "plan-backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-05T00:00:00Z",
    ]
    code_z = main(args_z, clock=_fixed_clock_feb)
    out_z = capsys.readouterr().out

    args_offset = [
        "plan-backfill",
        "--start",
        "2026-01-01T00:00:00+00:00",
        "--end",
        "2026-01-05T00:00:00+00:00",
    ]
    code_offset = main(args_offset, clock=_fixed_clock_feb)
    out_offset = capsys.readouterr().out

    assert code_z == 0
    assert code_offset == 0
    assert out_z == out_offset


@pytest.mark.parametrize(
    "invalid_args,expected_err_snippet",
    [
        ([], "error: a command is required"),
        (["unknown-cmd"], "invalid choice"),
        (["plan-backfill"], "the following arguments are required: --start, --end"),
        (
            ["plan-backfill", "--start", "2026-01-01T00:00:00Z"],
            "the following arguments are required: --end",
        ),
        (
            ["plan-backfill", "--start", "2026-01-01T00:00:00", "--end", "2026-01-02T00:00:00Z"],
            "explicit UTC",
        ),
        (
            [
                "plan-backfill",
                "--start",
                "2026-01-01T00:00:00+07:00",
                "--end",
                "2026-01-02T00:00:00Z",
            ],
            "explicit UTC",
        ),
        (
            ["plan-backfill", "--start", "invalid-ts", "--end", "2026-01-02T00:00:00Z"],
            "Invalid ISO-8601",
        ),
        (
            ["plan-backfill", "--start", "2026-01-01T00:15:00Z", "--end", "2026-01-02T00:00:00Z"],
            "aligned to an exact hour",
        ),
        (
            ["plan-backfill", "--start", "2026-01-01T00:00:00Z", "--end", "2026-01-01T00:00:01Z"],
            "aligned to an exact hour",
        ),
        (
            ["plan-backfill", "--start", "2026-01-01T00:00:00Z", "--end", "2026-01-01T00:00:00Z"],
            "strictly before",
        ),
        (
            ["plan-backfill", "--start", "2026-01-02T00:00:00Z", "--end", "2026-01-01T00:00:00Z"],
            "strictly before",
        ),
        (
            ["plan-backfill", "--start", "2026-09-17T10:00:00Z", "--end", "2026-09-17T15:00:00Z"],
            "later than the start of the current UTC hour",
        ),
    ],
)
def test_cli_invalid_input_exits_2_with_stderr_and_empty_stdout(
    invalid_args: list[str],
    expected_err_snippet: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(invalid_args, clock=_fixed_clock_sep)
    assert code == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert expected_err_snippet.lower() in captured.err.lower()


def test_cli_env_var_clock_override(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BITCOIN_DATA_OVERRIDE_NOW_UTC", "2026-02-01T00:00:00Z")
    args = [
        "plan-backfill",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-02T00:00:00Z",
    ]
    code = main(args)
    assert code == 0
    captured = capsys.readouterr()
    assert "2026-01-01T00:00:00Z" in captured.out


def test_cli_unexpected_error_handling(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def crashing_plan(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated crash")

    monkeypatch.setattr("bitcoin_data_platform.cli.plan_backfill", crashing_plan)
    code = main(
        [
            "plan-backfill",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-02T00:00:00Z",
        ],
        clock=_fixed_clock_feb,
    )
    assert code == 2
    captured = capsys.readouterr()
    assert "error: unexpected failure: simulated crash" in captured.err


def test_cli_module_invocation() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "bitcoin_data_platform", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "bitcoin-data" in result.stdout
    assert "plan-backfill" in result.stdout
