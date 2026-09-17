"""Unit tests for WebSocket connection lifecycle, backoff, and reconnection."""

import asyncio
import json
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed

from bitcoin_data_platform.streaming.connection import (
    ConnectionState,
    WebSocketConnectionError,
    WebSocketConnectionManager,
)


class MockWebSocket:
    """Mock WebSocket for deterministic testing of handshake and messaging."""

    def __init__(
        self,
        messages: list[str],
        raise_after: Exception | None = None,
    ) -> None:
        self.messages = messages
        self.raise_after = raise_after
        self.sent_messages: list[str] = []

    async def send(self, data: str) -> None:
        self.sent_messages.append(data)

    def __aiter__(self) -> "MockWebSocket":
        self._iter = iter(self.messages)
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._iter)
        except StopIteration as exc:
            if self.raise_after:
                raise self.raise_after from exc
            raise StopAsyncIteration from exc


class MockConnectContextManager:
    """Async context manager mocking websockets.connect."""

    def __init__(self, ws: MockWebSocket | None = None, connect_error: Exception | None = None):
        self.ws = ws
        self.connect_error = connect_error

    async def __aenter__(self) -> MockWebSocket:
        if self.connect_error:
            raise self.connect_error
        assert self.ws is not None
        return self.ws

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


def test_build_subscription_payload() -> None:
    manager = WebSocketConnectionManager(
        product_ids=["BTC-USD"],
        channels=["matches", "heartbeat"],
    )
    payload = manager.build_subscription_payload()
    assert payload == {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["matches", "heartbeat"],
    }


def test_compute_backoff_delay_bounds() -> None:
    # Deterministic jitter (1.0)
    manager = WebSocketConnectionManager(
        base_backoff_seconds=1.0,
        max_backoff_seconds=16.0,
        jitter_func=lambda: 1.0,
    )
    assert manager.compute_backoff_delay(0) == 1.0
    assert manager.compute_backoff_delay(1) == 2.0
    assert manager.compute_backoff_delay(2) == 4.0
    assert manager.compute_backoff_delay(3) == 8.0
    assert manager.compute_backoff_delay(4) == 16.0
    assert manager.compute_backoff_delay(5) == 16.0  # Capped at max_backoff


def test_connection_handshake_and_stream() -> None:
    async def _run() -> None:
        mock_ws = MockWebSocket(
            messages=['{"type": "subscriptions"}', '{"type": "match", "trade_id": 1}'],
        )

        def connect_factory(url: str) -> MockConnectContextManager:
            return MockConnectContextManager(ws=mock_ws)

        manager = WebSocketConnectionManager(
            product_ids=["BTC-USD"],
            connect_factory=connect_factory,
        )

        received: list[str | bytes] = []
        async for msg in manager.stream_messages():
            received.append(msg)
            if len(received) == 2:
                manager.stop()

        assert len(received) == 2
        assert len(mock_ws.sent_messages) == 1
        sent_dict = json.loads(mock_ws.sent_messages[0])
        assert sent_dict["type"] == "subscribe"
        assert sent_dict["product_ids"] == ["BTC-USD"]
        assert manager.state == ConnectionState.STOPPED

    asyncio.run(_run())


def test_reconnection_backoff_lifecycle() -> None:
    async def _run() -> None:
        sleeps: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleeps.append(duration)

        ws1 = MockWebSocket(
            messages=['{"type": "match", "trade_id": 101}'],
            raise_after=ConnectionClosed(None, None),  # type: ignore[arg-type]
        )
        ws2 = MockWebSocket(
            messages=['{"type": "match", "trade_id": 102}'],
        )

        sockets = [ws1, ws2]
        idx = 0

        def connect_factory(url: str) -> MockConnectContextManager:
            nonlocal idx
            ws = sockets[idx]
            idx += 1
            return MockConnectContextManager(ws=ws)

        manager = WebSocketConnectionManager(
            base_backoff_seconds=2.0,
            max_backoff_seconds=10.0,
            connect_factory=connect_factory,
            sleeper=fake_sleep,
            jitter_func=lambda: 1.0,
        )

        received: list[str | bytes] = []
        async for msg in manager.stream_messages():
            received.append(msg)
            if len(received) == 2:
                manager.stop()

        assert len(received) == 2
        assert manager.reconnect_count == 1
        assert len(sleeps) == 1
        assert sleeps[0] == 2.0  # 2.0 * 2^0
        assert manager.state == ConnectionState.STOPPED

    asyncio.run(_run())


def test_max_reconnect_attempts_exceeded() -> None:
    async def _run() -> None:
        def connect_factory(url: str) -> MockConnectContextManager:
            return MockConnectContextManager(connect_error=OSError("Network unreachable"))

        manager = WebSocketConnectionManager(
            max_reconnect_attempts=2,
            base_backoff_seconds=0.1,
            connect_factory=connect_factory,
            sleeper=lambda d: asyncio.sleep(0),
        )

        with pytest.raises(
            WebSocketConnectionError, match="WebSocket reconnect failed after 3 attempts"
        ):
            async for _ in manager.stream_messages():
                pass

        assert manager.state == ConnectionState.DISCONNECTED

    asyncio.run(_run())


def test_clean_shutdown_while_streaming() -> None:
    async def _run() -> None:
        mock_ws = MockWebSocket(messages=['{"type": "heartbeat"}'] * 10)

        def connect_factory(url: str) -> MockConnectContextManager:
            return MockConnectContextManager(ws=mock_ws)

        manager = WebSocketConnectionManager(
            connect_factory=connect_factory,
        )

        count = 0
        async for _ in manager.stream_messages():
            count += 1
            if count == 3:
                manager.stop()

        assert count == 3
        assert manager.state == ConnectionState.STOPPED

    asyncio.run(_run())
