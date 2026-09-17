"""Unit tests for atomic micro-batch JSON Lines writer and disk capacity protection."""

import hashlib
import json
from collections import namedtuple
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from bitcoin_data_platform.streaming.models import MicroBatchReceipt, StreamTrade
from bitcoin_data_platform.streaming.writer import (
    DiskFullError,
    MicroBatchWriter,
    StorageError,
)

DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


def _make_trade(trade_id: int) -> StreamTrade:
    ts = datetime(2026, 9, 18, 10, 0, trade_id, tzinfo=UTC)
    return StreamTrade(
        source="coinbase_exchange",
        product_id="BTC-USD",
        trade_id=trade_id,
        sequence=1000 + trade_id,
        price=Decimal("65000.50"),
        size=Decimal("0.25"),
        side="buy" if trade_id % 2 == 0 else "sell",
        time_utc=ts,
        received_at_utc=ts,
        received_monotonic_ns=10000 + trade_id,
    )


def test_writer_creates_directories(tmp_path: Path) -> None:
    out = tmp_path / "streaming_run"
    writer = MicroBatchWriter(out)
    assert (out / "events").exists()
    assert (out / "commits").exists()
    assert writer.output_dir == out


def test_write_batch_atomic_and_checksum(tmp_path: Path) -> None:
    out = tmp_path / "run_1"
    fixed_now = datetime(2026, 9, 18, 10, 5, 0, tzinfo=UTC)
    writer = MicroBatchWriter(out, clock=lambda: fixed_now)

    trades = [_make_trade(1), _make_trade(2), _make_trade(3)]
    receipt = writer.write_batch(trades, batch_id="000001")

    assert receipt is not None
    assert receipt.batch_id == "000001"
    assert receipt.trade_count == 3
    assert receipt.min_sequence == 1001
    assert receipt.max_sequence == 1003
    assert receipt.min_trade_id == 1
    assert receipt.max_trade_id == 3
    assert receipt.committed_at_utc == fixed_now

    segment_path = Path(receipt.segment_path)
    assert segment_path.exists()
    assert not (segment_path.parent / "part-000001.jsonl.partial").exists()

    receipt_path = out / "commits" / "000001.json"
    assert receipt_path.exists()
    assert not (out / "commits" / "000001.json.tmp").exists()

    # Validate checksum against segment contents
    with open(segment_path, "rb") as f:
        file_bytes = f.read()
    expected_hash = hashlib.sha256(file_bytes).hexdigest()
    assert receipt.sha256_checksum == expected_hash
    assert receipt.byte_size == len(file_bytes)

    # Validate JSON Lines contents
    lines = file_bytes.decode("utf-8").strip().split("\n")
    assert len(lines) == 3
    first_dict = json.loads(lines[0])
    assert first_dict["trade_id"] == 1
    assert first_dict["price"] == "65000.50"


def test_write_batch_empty_trades_returns_none(tmp_path: Path) -> None:
    out = tmp_path / "run_empty"
    writer = MicroBatchWriter(out)
    receipt = writer.write_batch([])
    assert receipt is None
    assert len(list((out / "events").glob("*"))) == 0
    assert len(list((out / "commits").glob("*"))) == 0


def test_write_batch_disk_full_guard(tmp_path: Path) -> None:
    out = tmp_path / "run_disk_full"

    def mock_disk_usage(path: str | Path) -> DiskUsage:
        # Simulate 10 MiB free, while min_free_bytes requires 50 MiB
        return DiskUsage(total=100 * 1024 * 1024, used=90 * 1024 * 1024, free=10 * 1024 * 1024)

    writer = MicroBatchWriter(
        out,
        min_free_bytes=50 * 1024 * 1024,
        disk_usage_func=mock_disk_usage,
    )

    trades = [_make_trade(1)]
    with pytest.raises(DiskFullError, match="Disk space critical"):
        writer.write_batch(trades)

    # No files should have been created
    assert len(list((out / "events").glob("*"))) == 0


def test_write_batch_storage_error_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "run_err"
    writer = MicroBatchWriter(out)
    trades = [_make_trade(1)]

    # Mock os.replace to raise an OSError
    def fail_replace(src: str | Path, dst: str | Path) -> None:
        raise OSError("Permission denied")

    monkeypatch.setattr("os.replace", fail_replace)

    with pytest.raises(StorageError, match="Failed to write micro-batch segment"):
        writer.write_batch(trades)


def test_micro_batch_receipt_roundtrip() -> None:
    now = datetime(2026, 9, 18, 10, 0, 0, tzinfo=UTC)
    receipt = MicroBatchReceipt(
        batch_id="batch_01",
        segment_path="/tmp/part-01.jsonl",
        trade_count=10,
        byte_size=1024,
        sha256_checksum="abcdef123456",
        min_sequence=1,
        max_sequence=10,
        min_trade_id=100,
        max_trade_id=109,
        start_time_utc=now,
        end_time_utc=now,
        committed_at_utc=now,
    )
    d = receipt.to_dict()
    restored = MicroBatchReceipt.from_dict(d)
    assert restored == receipt
