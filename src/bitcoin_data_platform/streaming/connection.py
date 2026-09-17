"""Async WebSocket connection manager with subscription handshake and backoff reconnect."""

import asyncio
import enum
import json
import random
import time
from collections.abc import AsyncIterator, Callable
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

DEFAULT_WS_URL = "wss://ws-feed.exchange.coinbase.com"
DEFAULT_PRODUCT_ID = "BTC-USD"
DEFAULT_CHANNELS = ["matches", "heartbeat"]
DEFAULT_BASE_BACKOFF_SECONDS = 1.0
DEFAULT_MAX_BACKOFF_SECONDS = 30.0


class ConnectionState(enum.Enum):
    """Lifecycle states of the WebSocket connection."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"


class WebSocketConnectionError(Exception):
    """Raised when connection cannot be established or max retries exceeded."""


class WebSocketConnectionManager:
    """Manages WebSocket lifecycle for Coinbase market data feeds.

    Features:
    - Automatic subscription to requested channels upon connection.
    - Exponential backoff with full jitter to prevent thundering herd.
    - Pluggable connect_factory for offline deterministic testing.
    - State tracking and graceful termination.
    """

    def __init__(
        self,
        url: str = DEFAULT_WS_URL,
        *,
        product_ids: list[str] | None = None,
        channels: list[str] | None = None,
        base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
        max_reconnect_attempts: int | None = None,
        connect_factory: Callable[..., Any] | None = None,
        sleeper: Callable[[float], Any] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        jitter_func: Callable[[], float] = random.random,
    ) -> None:
        self.url = url
        self.product_ids = product_ids or [DEFAULT_PRODUCT_ID]
        self.channels = channels or list(DEFAULT_CHANNELS)
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.max_reconnect_attempts = max_reconnect_attempts

        self._connect_factory = connect_factory or websockets.connect
        self._sleeper = sleeper
        self._clock = clock
        self._jitter_func = jitter_func

        self._state: ConnectionState = ConnectionState.DISCONNECTED
        self._stop_event: asyncio.Event = asyncio.Event()
        self._reconnect_count: int = 0
        self._consecutive_failures: int = 0
        self._active_ws: Any = None

    @property
    def state(self) -> ConnectionState:
        """Current connection state."""
        return self._state

    @property
    def reconnect_count(self) -> int:
        """Total number of reconnection attempts performed."""
        return self._reconnect_count

    def compute_backoff_delay(self, attempt: int) -> float:
        """Calculate exponential backoff with full jitter.

        Formula: U(0, min(max_delay, base * 2^attempt)).
        """
        cap = min(self.max_backoff_seconds, self.base_backoff_seconds * (2**attempt))
        return float(self._jitter_func() * cap)

    def build_subscription_payload(self) -> dict[str, Any]:
        """Construct the Coinbase subscribe JSON payload."""
        return {
            "type": "subscribe",
            "product_ids": self.product_ids,
            "channels": self.channels,
        }

    def stop(self) -> None:
        """Signal the manager to cease reconnecting and close active socket."""
        self._state = ConnectionState.STOPPED
        self._stop_event.set()

    async def stream_messages(self) -> AsyncIterator[str | bytes]:
        """Connect to WebSocket feed and yield incoming messages.

        Automatically reconnects on disconnects until stopped or max attempts reached.
        """
        while not self._stop_event.is_set():
            self._state = ConnectionState.CONNECTING
            try:
                connect_cm = self._connect_factory(self.url)
                async with connect_cm as ws:
                    self._active_ws = ws
                    self._state = ConnectionState.CONNECTED
                    self._consecutive_failures = 0

                    sub_payload = json.dumps(self.build_subscription_payload())
                    await ws.send(sub_payload)

                    async for message in ws:
                        if self._stop_event.is_set():
                            break
                        yield message

            except (TimeoutError, ConnectionClosed, OSError) as exc:
                if self._stop_event.is_set():
                    break

                self._consecutive_failures += 1
                self._reconnect_count += 1

                if (
                    self.max_reconnect_attempts is not None
                    and self._consecutive_failures > self.max_reconnect_attempts
                ):
                    self._state = ConnectionState.DISCONNECTED
                    err_msg = (
                        f"WebSocket reconnect failed after {self._consecutive_failures} "
                        f"attempts: {exc}"
                    )
                    raise WebSocketConnectionError(err_msg) from exc

                self._state = ConnectionState.RECONNECTING
                delay = self.compute_backoff_delay(self._consecutive_failures - 1)
                await self._sleeper(delay)

            except Exception as exc:
                if self._stop_event.is_set():
                    break
                self._state = ConnectionState.DISCONNECTED
                raise WebSocketConnectionError(f"Unexpected WebSocket error: {exc}") from exc
            finally:
                self._active_ws = None

        self._state = ConnectionState.STOPPED
