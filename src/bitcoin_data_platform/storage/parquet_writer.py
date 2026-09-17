"""Parquet storage writer for curated market candle partitions."""

import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


class ParquetStorageError(Exception):
    """Raised when writing or merging Parquet partitions fails."""


PARQUET_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("product_id", pa.string()),
        ("granularity_seconds", pa.int32()),
        ("candle_start_utc", pa.timestamp("us", tz="UTC")),
        ("open", pa.decimal128(38, 18)),
        ("high", pa.decimal128(38, 18)),
        ("low", pa.decimal128(38, 18)),
        ("close", pa.decimal128(38, 18)),
        ("volume_base", pa.decimal128(38, 18)),
        ("ingested_at_utc", pa.timestamp("us", tz="UTC")),
        ("source_run_id", pa.string()),
    ]
)


@dataclass(frozen=True)
class PartitionResult:
    """Result of writing one annual partition."""

    year: int
    file_path: Path
    row_count: int
    is_new: bool


def read_partition_candles(file_path: Path) -> list[NormalizedCandle]:
    """Read an existing Parquet partition back into a list of NormalizedCandle objects."""
    if not file_path.exists():
        return []
    try:
        table = pq.read_table(file_path)
        candles: list[NormalizedCandle] = []
        for row in table.to_pylist():
            c_start = row["candle_start_utc"]
            if c_start.tzinfo is None:
                c_start = c_start.replace(tzinfo=UTC)
            else:
                c_start = c_start.astimezone(UTC)

            ingested = row["ingested_at_utc"]
            if ingested.tzinfo is None:
                ingested = ingested.replace(tzinfo=UTC)
            else:
                ingested = ingested.astimezone(UTC)

            candles.append(
                NormalizedCandle(
                    source=row["source"],
                    product_id=row["product_id"],
                    granularity_seconds=int(row["granularity_seconds"]),
                    candle_start_utc=c_start,
                    open=Decimal(str(row["open"])),
                    high=Decimal(str(row["high"])),
                    low=Decimal(str(row["low"])),
                    close=Decimal(str(row["close"])),
                    volume_base=Decimal(str(row["volume_base"])),
                    ingested_at_utc=ingested,
                    source_run_id=str(row["source_run_id"]),
                )
            )
        return candles
    except Exception as exc:
        raise ParquetStorageError(
            f"Failed to read Parquet partition at {file_path}: {exc}"
        ) from exc


def candles_to_table(candles: list[NormalizedCandle]) -> pa.Table:
    """Convert NormalizedCandles to a PyArrow Table with explicit decimal schema."""
    arrays = [
        pa.array([c.source for c in candles], type=pa.string()),
        pa.array([c.product_id for c in candles], type=pa.string()),
        pa.array([c.granularity_seconds for c in candles], type=pa.int32()),
        pa.array([c.candle_start_utc for c in candles], type=pa.timestamp("us", tz="UTC")),
        pa.array([c.open for c in candles], type=pa.decimal128(38, 18)),
        pa.array([c.high for c in candles], type=pa.decimal128(38, 18)),
        pa.array([c.low for c in candles], type=pa.decimal128(38, 18)),
        pa.array([c.close for c in candles], type=pa.decimal128(38, 18)),
        pa.array([c.volume_base for c in candles], type=pa.decimal128(38, 18)),
        pa.array([c.ingested_at_utc for c in candles], type=pa.timestamp("us", tz="UTC")),
        pa.array([c.source_run_id for c in candles], type=pa.string()),
    ]
    return pa.Table.from_arrays(arrays, schema=PARQUET_SCHEMA)


def write_parquet_partitions(
    candles: list[NormalizedCandle],
    curated_dir: Path | str,
    existing_dir: Path | str | None = None,
) -> list[PartitionResult]:
    """Write annual Parquet partitions. Merges with existing partitions if present.

    Directory structure:
        curated/market/candles_hourly/source={source}/year={year}/data.parquet

    Atomic write: writes to temporary file and replaces target atomically.
    Returns list of PartitionResult sorted by year ascending.
    """
    curated_path = Path(curated_dir)
    existing_base = Path(existing_dir) if existing_dir is not None else curated_path

    # Group candles by (source, year)
    grouped: dict[tuple[str, int], list[NormalizedCandle]] = defaultdict(list)
    for candle in candles:
        grouped[(candle.source, candle.candle_start_utc.year)].append(candle)

    results: list[PartitionResult] = []

    for (source, year), new_candles in grouped.items():
        partition_dir = (
            curated_path / "market" / "candles_hourly" / f"source={source}" / f"year={year}"
        )
        target_file = partition_dir / "data.parquet"

        # Check existing partition to merge
        existing_file = (
            existing_base
            / "market"
            / "candles_hourly"
            / f"source={source}"
            / f"year={year}"
            / "data.parquet"
        )
        is_new = not target_file.exists() and not (
            existing_file is not None and existing_file.exists()
        )

        # Collect existing candles if partition exists
        source_existing_file = (
            existing_file
            if (existing_file is not None and existing_file.exists())
            else (target_file if target_file.exists() else None)
        )
        existing_candles: list[NormalizedCandle] = []
        if source_existing_file is not None:
            existing_candles = read_partition_candles(source_existing_file)

        # Merge and deduplicate by natural key: latest ingested_at / source_run_id wins
        deduped: dict[tuple[str, str, int, datetime], NormalizedCandle] = {}
        for c in existing_candles:
            key = (c.source, c.product_id, c.granularity_seconds, c.candle_start_utc)
            deduped[key] = c

        for c in new_candles:
            key = (c.source, c.product_id, c.granularity_seconds, c.candle_start_utc)
            if key in deduped:
                existing_c = deduped[key]
                if (c.ingested_at_utc, c.source_run_id) >= (
                    existing_c.ingested_at_utc,
                    existing_c.source_run_id,
                ):
                    deduped[key] = c
            else:
                deduped[key] = c

        merged_candles = sorted(deduped.values(), key=lambda c: c.candle_start_utc)
        table = candles_to_table(merged_candles)

        # Atomic write: write to temp file, then os.replace
        partition_dir.mkdir(parents=True, exist_ok=True)
        temp_file = partition_dir / f"data.parquet.{uuid.uuid4().hex}.tmp"

        try:
            pq.write_table(table, temp_file)
            os.replace(temp_file, target_file)
        except Exception as exc:
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)
            raise ParquetStorageError(f"Failed to write partition {target_file}: {exc}") from exc

        results.append(
            PartitionResult(
                year=year,
                file_path=target_file,
                row_count=len(merged_candles),
                is_new=is_new,
            )
        )

    results.sort(key=lambda res: res.year)
    return results
