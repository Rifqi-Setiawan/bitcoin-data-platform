"""Bounded in-memory FIFO buffer with drop-newest overflow protection."""

import collections
from collections.abc import Sequence

from bitcoin_data_platform.streaming.models import StreamTrade

DEFAULT_MAX_ITEMS = 20_000
DEFAULT_MAX_BYTES = 32 * 1024 * 1024  # 32 MiB
TRADE_ESTIMATED_BYTES = 256


class StreamBuffer:
    """Bounded FIFO buffer for trade events with explicit loss accounting.

    Enforces both item count and memory byte limits.
    When capacity is reached, applies 'drop-newest' overflow policy:
    incoming events are discarded, queue contents remain intact, and
    dropped_events counter is incremented.
    """

    def __init__(
        self,
        max_items: int = DEFAULT_MAX_ITEMS,
        max_bytes: int = DEFAULT_MAX_BYTES,
        trade_byte_estimate: int = TRADE_ESTIMATED_BYTES,
    ) -> None:
        if max_items <= 0:
            raise ValueError("max_items must be a positive integer")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")

        self.max_items = max_items
        self.max_bytes = max_bytes
        self.trade_byte_estimate = trade_byte_estimate

        self._queue: collections.deque[StreamTrade] = collections.deque()
        self._dropped_events: int = 0
        self._total_pushed: int = 0
        self._current_bytes: int = 0

    @property
    def size(self) -> int:
        """Current number of items in the buffer."""
        return len(self._queue)

    @property
    def bytes_size(self) -> int:
        """Estimated memory usage of buffered items in bytes."""
        return self._current_bytes

    @property
    def dropped_events(self) -> int:
        """Cumulative count of events dropped due to buffer capacity exhaustion."""
        return self._dropped_events

    @property
    def total_pushed(self) -> int:
        """Cumulative count of successfully buffered events."""
        return self._total_pushed

    @property
    def is_empty(self) -> bool:
        """True if the buffer contains no items."""
        return len(self._queue) == 0

    @property
    def is_full(self) -> bool:
        """True if either item count or byte limit is reached."""
        return (
            len(self._queue) >= self.max_items
            or self._current_bytes + self.trade_byte_estimate > self.max_bytes
        )

    def push(self, trade: StreamTrade) -> bool:
        """Push a trade into the buffer.

        Returns True if buffered, False if dropped due to capacity limits.
        """
        if self.is_full:
            self._dropped_events += 1
            return False

        self._queue.append(trade)
        self._current_bytes += self.trade_byte_estimate
        self._total_pushed += 1
        return True

    def push_many(self, trades: Sequence[StreamTrade]) -> int:
        """Push multiple trades into the buffer.

        Returns the count of successfully accepted trades.
        """
        accepted = 0
        for trade in trades:
            if self.push(trade):
                accepted += 1
        return accepted

    def pop_batch(self, max_batch_size: int = 1000) -> list[StreamTrade]:
        """Pop up to max_batch_size trades from the front of the queue (FIFO)."""
        if max_batch_size <= 0:
            return []

        popped: list[StreamTrade] = []
        count = min(max_batch_size, len(self._queue))
        for _ in range(count):
            trade = self._queue.popleft()
            popped.append(trade)

        self._current_bytes = max(0, self._current_bytes - (len(popped) * self.trade_byte_estimate))
        return popped

    def drain(self) -> list[StreamTrade]:
        """Drain all remaining trades from the buffer."""
        all_items = list(self._queue)
        self._queue.clear()
        self._current_bytes = 0
        return all_items

    def clear(self) -> None:
        """Discard all buffered items without resetting dropped_events counter."""
        self._queue.clear()
        self._current_bytes = 0
