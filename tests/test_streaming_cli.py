"""Unit and CLI integration tests for bitcoin-data stream command."""

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.sources.coinbase_client import CoinbaseResponse
from bitcoin_data_platform.sources.coinbase_contract import CoinbaseCandle
from bitcoin_data_platform.streaming.connection import WebSocketConnectionError
from bitcoin_data_platform.streaming.runner import (
    StreamingSessionError,
    run_streaming_session,
)


class MockCliWebSocket:
    """Mock WebSocket for streaming CLI testing."""

    def __init__(self, messages: list[str]) -> None:
        self.messages = list(messages)
        self._wait_forever = asyncio.Event()

    async def send(self, data: str) -> None:
        await asyncio.sleep(0)

    def __aiter__(self) -> "MockCliWebSocket":
        return self

    async def __anext__(self) -> str:
        if self.messages:
            await asyncio.sleep(0)
            return self.messages.pop(0)
        await self._wait_forever.wait()
        raise StopAsyncIteration


class MockCliConnectContextManager:
    def __init__(
        self, ws: MockCliWebSocket | None = None, connect_error: Exception | None = None
    ) -> None:
        self.ws = ws
        self.connect_error = connect_error

    async def __aenter__(self) -> MockCliWebSocket:
        if self.connect_error:
            raise self.connect_error
        assert self.ws is not None
        return self.ws

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


def _create_mock_factory(messages: list[str]) -> Any:
    def connect_factory(url: str) -> MockCliConnectContextManager:
        return MockCliConnectContextManager(ws=MockCliWebSocket(messages))

    return connect_factory


def test_cli_stream_help(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["stream", "--help"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "--duration" in captured.out
    assert "--output-dir" in captured.out
    assert "--reconcile" in captured.out


def test_cli_stream_invalid_duration(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code_zero = main(["stream", "--duration", "0"])
    assert exit_code_zero == 2
    captured = capsys.readouterr()
    assert "error: --duration must be a positive integer" in captured.err

    exit_code_neg = main(["stream", "--duration", "-5"])
    assert exit_code_neg == 2


def test_runner_invalid_duration_raises() -> None:
    import asyncio

    with pytest.raises(StreamingSessionError, match="duration_seconds must be a positive integer"):
        asyncio.run(run_streaming_session(duration_seconds=0))


def test_cli_stream_execution_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    t_now = datetime.now(UTC).isoformat()
    raw_match = json.dumps(
        {
            "type": "match",
            "trade_id": 5001,
            "sequence": 10001,
            "product_id": "BTC-USD",
            "price": "65000.00",
            "size": "0.5",
            "side": "buy",
            "time": t_now,
        }
    )
    raw_hb = json.dumps(
        {
            "type": "heartbeat",
            "sequence": 10002,
            "last_trade_id": 5001,
            "product_id": "BTC-USD",
            "time": t_now,
        }
    )

    connect_factory = _create_mock_factory([raw_match, raw_hb])
    out_dir = tmp_path / "streaming_out"

    exit_code = main(
        ["stream", "--duration", "1", "--output-dir", str(out_dir)],
        connect_factory=connect_factory,
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["status"] == "success"
    assert summary["product_id"] == "BTC-USD"
    assert summary["metrics"]["total_trades_captured"] >= 1
    assert summary["metrics"]["total_heartbeats"] >= 1

    # Verify physical file layout
    runs_dir = out_dir / "runs"
    assert runs_dir.exists()
    run_folders = list(runs_dir.iterdir())
    assert len(run_folders) == 1
    run_folder = run_folders[0]

    assert (run_folder / "manifest.json").exists()
    assert (run_folder / "reports" / "summary.json").exists()
    assert (run_folder / "derived" / "candles_1m.parquet").exists()
    assert (run_folder / "derived" / "candles_1h.parquet").exists()


def test_cli_stream_execution_with_reconcile(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    t_now = datetime.now(UTC)
    t_iso = t_now.isoformat()
    raw_match = json.dumps(
        {
            "type": "match",
            "trade_id": 6001,
            "sequence": 20001,
            "product_id": "BTC-USD",
            "price": "65500.00",
            "size": "1.0",
            "side": "buy",
            "time": t_iso,
        }
    )

    connect_factory = _create_mock_factory([raw_match])
    out_dir = tmp_path / "streaming_reconcile_out"

    mock_client = MagicMock()
    candle_start = t_now.replace(second=0, microsecond=0)
    mock_candle = CoinbaseCandle(
        timestamp_utc=candle_start,
        open=Decimal("65500.00"),
        high=Decimal("65500.00"),
        low=Decimal("65500.00"),
        close=Decimal("65500.00"),
        volume=Decimal("1.0"),
    )
    mock_client.fetch_candles.return_value = CoinbaseResponse(
        start_utc=candle_start,
        end_utc=candle_start,
        candles=[mock_candle],
        raw_payload=[],
        http_status=200,
        retrieved_at_utc=t_now,
    )

    exit_code = main(
        ["stream", "--duration", "1", "--output-dir", str(out_dir), "--reconcile"],
        connect_factory=connect_factory,
        client=mock_client,
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["reconciliation"] is not None
    assert summary["reconciliation"]["windows_evaluated"] >= 1


def test_cli_stream_connection_failure(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def failing_factory(url: str) -> MockCliConnectContextManager:
        err = WebSocketConnectionError("Handshake failed")
        return MockCliConnectContextManager(connect_error=err)

    out_dir = tmp_path / "streaming_fail"
    exit_code = main(
        ["stream", "--duration", "1", "--output-dir", str(out_dir)],
        connect_factory=failing_factory,
    )
    assert exit_code == 3
    captured = capsys.readouterr()
    assert "Connection failure:" in captured.err
    assert "Handshake failed" in captured.err
