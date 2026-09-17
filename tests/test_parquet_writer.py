"""Tests for curated Parquet writer and partition management."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from bitcoin_data_platform.storage.parquet_writer import (
    PARQUET_SCHEMA,
    PartitionResult,
    read_partition_candles,
    write_parquet_partitions,
)
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _make_candle(
    year: int = 2026,
    month: int = 1,
    day: int = 1,
    hour: int = 0,
    source_run_id: str = "run-1",
    price: str = "95000.00",
    volume: str = "10.5",
    ingested_at: datetime | None = None,
) -> NormalizedCandle:
    c_time = datetime(year, month, day, hour, 0, tzinfo=UTC)
    if ingested_at is None:
        ingested_at = datetime(year, month, day, hour + 1, 0, tzinfo=UTC)
    p = Decimal(price)
    v = Decimal(volume)
    return NormalizedCandle(
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        candle_start_utc=c_time,
        open=p,
        high=p + Decimal("100"),
        low=p - Decimal("100"),
        close=p + Decimal("50"),
        volume_base=v,
        ingested_at_utc=ingested_at,
        source_run_id=source_run_id,
    )


def test_write_new_partition_creates_correct_directory_structure(tmp_path: Path) -> None:
    """22. Write new partition creates correct directory structure."""
    candles = [_make_candle(year=2026, hour=0), _make_candle(year=2026, hour=1)]
    results = write_parquet_partitions(candles, curated_dir=tmp_path)

    assert len(results) == 1
    res = results[0]
    assert isinstance(res, PartitionResult)
    assert res.year == 2026
    assert res.row_count == 2
    assert res.is_new is True

    expected_path = (
        tmp_path
        / "market"
        / "candles_hourly"
        / "source=coinbase_exchange"
        / "year=2026"
        / "data.parquet"
    )
    assert res.file_path == expected_path
    assert expected_path.exists()


def test_parquet_schema_matches_spec(tmp_path: Path) -> None:
    """23. Parquet schema matches spec (decimal types, timestamps)."""
    candles = [_make_candle(year=2026)]
    results = write_parquet_partitions(candles, curated_dir=tmp_path)
    file_path = results[0].file_path

    parquet_file = pq.ParquetFile(file_path)
    schema = parquet_file.schema_arrow

    assert schema.field("source").type == pa.string()
    assert schema.field("product_id").type == pa.string()
    assert schema.field("granularity_seconds").type == pa.int32()
    assert schema.field("candle_start_utc").type == pa.timestamp("us", tz="UTC")
    assert schema.field("open").type == pa.decimal128(38, 18)
    assert schema.field("high").type == pa.decimal128(38, 18)
    assert schema.field("low").type == pa.decimal128(38, 18)
    assert schema.field("close").type == pa.decimal128(38, 18)
    assert schema.field("volume_base").type == pa.decimal128(38, 18)
    assert schema.field("ingested_at_utc").type == pa.timestamp("us", tz="UTC")
    assert schema.field("source_run_id").type == pa.string()
    assert schema == PARQUET_SCHEMA


def test_merge_with_existing_partition_deduplicates_correctly(tmp_path: Path) -> None:
    """24. Merge with existing partition deduplicates correctly."""
    # Write initial partition with hour 0 and hour 1
    candles_batch1 = [
        _make_candle(hour=0, source_run_id="run-1", price="95000", volume="10"),
        _make_candle(hour=1, source_run_id="run-1", price="96000", volume="15"),
    ]
    write_parquet_partitions(candles_batch1, curated_dir=tmp_path)

    # Second batch: hour 1 (updated run-2) and hour 2 (new)
    candles_batch2 = [
        _make_candle(
            hour=1,
            source_run_id="run-2",
            price="96500",
            volume="20",
            ingested_at=datetime(2026, 1, 1, 5, 0, tzinfo=UTC),
        ),
        _make_candle(hour=2, source_run_id="run-2", price="97000", volume="25"),
    ]
    results = write_parquet_partitions(candles_batch2, curated_dir=tmp_path)

    assert len(results) == 1
    assert results[0].is_new is False
    assert results[0].row_count == 3  # hours 0, 1, 2

    # Read back and inspect
    candles_merged = read_partition_candles(results[0].file_path)
    assert len(candles_merged) == 3
    # Hour 1 should have taken the run-2 updated values
    h1 = candles_merged[1]
    assert h1.candle_start_utc == datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    assert h1.source_run_id == "run-2"
    assert h1.volume_base == Decimal("20")
    assert h1.open == Decimal("96500")


def test_atomic_write_uses_temp_file_and_replace(tmp_path: Path) -> None:
    """25. Atomic write (temp → replace)."""
    import os as _os_mod

    candles = [_make_candle(hour=0)]
    called_args: list[tuple[str, str]] = []
    real_replace = _os_mod.replace

    def spy_replace(src: _os_mod.PathLike | str, dst: _os_mod.PathLike | str) -> None:
        called_args.append((str(src), str(dst)))
        real_replace(src, dst)

    with patch("bitcoin_data_platform.storage.parquet_writer.os.replace", side_effect=spy_replace):
        write_parquet_partitions(candles, curated_dir=tmp_path)

    assert len(called_args) == 1
    src, dst = called_args[0]
    assert ".tmp" in src
    assert dst.endswith("data.parquet")
    assert Path(dst).exists()


def test_verify_row_count_matches_input(tmp_path: Path) -> None:
    """26. Verify row count matches input."""
    candles = [_make_candle(hour=h) for h in range(10)]
    results = write_parquet_partitions(candles, curated_dir=tmp_path)

    assert results[0].row_count == 10
    table = pq.read_table(results[0].file_path)
    assert table.num_rows == 10


def test_partition_by_year_multi_year_splits(tmp_path: Path) -> None:
    """27. Partition by year correctly (multi-year data splits)."""
    candles = [
        _make_candle(year=2024, hour=0),
        _make_candle(year=2025, hour=0),
        _make_candle(year=2026, hour=0),
    ]
    results = write_parquet_partitions(candles, curated_dir=tmp_path)

    assert len(results) == 3
    years = [res.year for res in results]
    assert years == [2024, 2025, 2026]

    for year in (2024, 2025, 2026):
        p = (
            tmp_path
            / "market"
            / "candles_hourly"
            / "source=coinbase_exchange"
            / f"year={year}"
            / "data.parquet"
        )
        assert p.exists()


def test_read_back_with_pyarrow_matches_written_data(tmp_path: Path) -> None:
    """28. Read back with pyarrow matches written data."""
    c = _make_candle(
        year=2026,
        hour=0,
        source_run_id="run-exact-match",
        price="95123.456789",
        volume="42.12345678",
    )
    results = write_parquet_partitions([c], curated_dir=tmp_path)
    file_path = results[0].file_path

    candles_read = read_partition_candles(file_path)
    assert len(candles_read) == 1
    r = candles_read[0]

    assert r.source == c.source
    assert r.product_id == c.product_id
    assert r.granularity_seconds == c.granularity_seconds
    assert r.candle_start_utc == c.candle_start_utc
    assert r.open == c.open
    assert r.high == c.high
    assert r.low == c.low
    assert r.close == c.close
    assert r.volume_base == c.volume_base
    assert r.source_run_id == c.source_run_id
    assert r.ingested_at_utc == c.ingested_at_utc
