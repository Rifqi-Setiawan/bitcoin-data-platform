"""Atomic micro-batch JSON Lines writer with fsync and disk capacity protection."""

import contextlib
import hashlib
import json
import os
import shutil
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.streaming.models import MicroBatchReceipt, StreamTrade

DEFAULT_MIN_FREE_BYTES = 50 * 1024 * 1024  # 50 MiB


class StorageError(Exception):
    """Base exception for streaming storage failures."""


class DiskFullError(StorageError):
    """Raised when available disk space is below safety threshold."""


class MicroBatchWriter:
    """Writes micro-batches of trades to immutable JSON Lines segments.

    Enforces crash-resilient atomic commits:
    1. Pre-flight check on available disk capacity.
    2. Writes data to a temporary '.partial' file.
    3. Flushes and calls os.fsync() to guarantee durability on disk.
    4. Performs atomic rename (os.replace) to final segment path.
    5. Writes an atomic commit receipt JSON with SHA-256 checksum and bounds.
    """

    def __init__(
        self,
        output_dir: Path | str,
        *,
        min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
        disk_usage_func: Callable[[str | Path], Any] = shutil.disk_usage,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.min_free_bytes = min_free_bytes
        self._disk_usage_func = disk_usage_func
        self._clock = clock or (lambda: datetime.now(UTC))

        self.events_dir = self.output_dir / "events"
        self.commits_dir = self.output_dir / "commits"

        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.commits_dir.mkdir(parents=True, exist_ok=True)

        self._batch_counter: int = 0

    def _check_disk_capacity(self) -> None:
        """Ensure destination filesystem has sufficient headroom."""
        try:
            usage = self._disk_usage_func(self.events_dir)
            free_bytes = getattr(usage, "free", 0)
        except Exception as exc:
            raise StorageError(f"Failed to check disk capacity: {exc}") from exc

        if free_bytes < self.min_free_bytes:
            raise DiskFullError(
                f"Disk space critical: {free_bytes} bytes free, "
                f"requires at least {self.min_free_bytes} bytes"
            )

    def write_batch(
        self,
        trades: Sequence[StreamTrade],
        batch_id: str | None = None,
    ) -> MicroBatchReceipt | None:
        """Write a sequence of trades to a durable JSON Lines segment.

        Returns MicroBatchReceipt on success, or None if trades sequence is empty.
        """
        if not trades:
            return None

        self._check_disk_capacity()

        if batch_id is None:
            self._batch_counter += 1
            batch_id = f"{self._batch_counter:06d}"

        segment_name = f"part-{batch_id}.jsonl"
        segment_path = self.events_dir / segment_name
        partial_path = self.events_dir / f"{segment_name}.partial"

        hasher = hashlib.sha256()
        byte_size = 0

        try:
            with open(partial_path, "wb") as f:
                for trade in trades:
                    line_str = json.dumps(trade.to_dict()) + "\n"
                    line_bytes = line_str.encode("utf-8")
                    f.write(line_bytes)
                    hasher.update(line_bytes)
                    byte_size += len(line_bytes)

                f.flush()
                os.fsync(f.fileno())

            os.replace(partial_path, segment_path)
        except DiskFullError:
            raise
        except Exception as exc:
            if partial_path.exists():
                with contextlib.suppress(OSError):
                    partial_path.unlink()
            raise StorageError(
                f"Failed to write micro-batch segment {segment_path}: {exc}"
            ) from exc

        sha256_checksum = hasher.hexdigest()
        min_seq = min(t.sequence for t in trades)
        max_seq = max(t.sequence for t in trades)
        min_tid = min(t.trade_id for t in trades)
        max_tid = max(t.trade_id for t in trades)
        start_time = min(t.time_utc for t in trades)
        end_time = max(t.time_utc for t in trades)
        committed_at = self._clock()

        receipt = MicroBatchReceipt(
            batch_id=batch_id,
            segment_path=str(segment_path),
            trade_count=len(trades),
            byte_size=byte_size,
            sha256_checksum=sha256_checksum,
            min_sequence=min_seq,
            max_sequence=max_seq,
            min_trade_id=min_tid,
            max_trade_id=max_tid,
            start_time_utc=start_time,
            end_time_utc=end_time,
            committed_at_utc=committed_at,
        )

        receipt_path = self.commits_dir / f"{batch_id}.json"
        tmp_receipt_path = self.commits_dir / f"{batch_id}.json.tmp"

        try:
            with open(tmp_receipt_path, "w", encoding="utf-8") as f:
                json.dump(receipt.to_dict(), f, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_receipt_path, receipt_path)
        except Exception as exc:
            if tmp_receipt_path.exists():
                with contextlib.suppress(OSError):
                    tmp_receipt_path.unlink()
            raise StorageError(f"Failed to write commit receipt {receipt_path}: {exc}") from exc

        return receipt
