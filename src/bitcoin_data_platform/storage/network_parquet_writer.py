"""Parquet storage writer for curated on-chain network metrics partitions."""

import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from bitcoin_data_platform.transforms.network_normalizer import NormalizedNetworkMetric


class NetworkParquetStorageError(Exception):
    """Raised when writing or merging network Parquet partitions fails."""


NETWORK_PARQUET_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("asset", pa.string()),
        ("metric_date_utc", pa.timestamp("us", tz="UTC")),
        ("transaction_count", pa.int64()),
        ("active_addresses_count", pa.int64()),
        ("ingested_at_utc", pa.timestamp("us", tz="UTC")),
        ("source_run_id", pa.string()),
    ]
)


@dataclass(frozen=True)
class NetworkPartitionResult:
    """Result of writing one annual network metrics partition."""

    year: int
    file_path: Path
    row_count: int
    is_new: bool


def read_network_partition_metrics(file_path: Path) -> list[NormalizedNetworkMetric]:
    """Read an existing network Parquet partition back into NormalizedNetworkMetric objects."""
    if not file_path.exists():
        return []
    try:
        table = pq.read_table(file_path)
        metrics: list[NormalizedNetworkMetric] = []
        for row in table.to_pylist():
            m_date = row["metric_date_utc"]
            m_date = m_date.replace(tzinfo=UTC) if m_date.tzinfo is None else m_date.astimezone(UTC)

            ingested = row["ingested_at_utc"]
            ingested = (
                ingested.replace(tzinfo=UTC)
                if ingested.tzinfo is None
                else ingested.astimezone(UTC)
            )

            metrics.append(
                NormalizedNetworkMetric(
                    source=str(row["source"]),
                    asset=str(row["asset"]),
                    metric_date_utc=m_date,
                    transaction_count=int(row["transaction_count"]),
                    active_addresses_count=int(row["active_addresses_count"]),
                    ingested_at_utc=ingested,
                    source_run_id=str(row["source_run_id"]),
                )
            )
        return metrics
    except Exception as exc:
        raise NetworkParquetStorageError(
            f"Failed to read network Parquet partition at {file_path}: {exc}"
        ) from exc


def network_metrics_to_table(metrics: list[NormalizedNetworkMetric]) -> pa.Table:
    """Convert NormalizedNetworkMetrics to a PyArrow Table with explicit schema."""
    arrays = [
        pa.array([m.source for m in metrics], type=pa.string()),
        pa.array([m.asset for m in metrics], type=pa.string()),
        pa.array([m.metric_date_utc for m in metrics], type=pa.timestamp("us", tz="UTC")),
        pa.array([m.transaction_count for m in metrics], type=pa.int64()),
        pa.array([m.active_addresses_count for m in metrics], type=pa.int64()),
        pa.array([m.ingested_at_utc for m in metrics], type=pa.timestamp("us", tz="UTC")),
        pa.array([m.source_run_id for m in metrics], type=pa.string()),
    ]
    return pa.Table.from_arrays(arrays, schema=NETWORK_PARQUET_SCHEMA)


def write_network_parquet_partitions(
    metrics: list[NormalizedNetworkMetric],
    curated_dir: Path | str,
    existing_dir: Path | str | None = None,
) -> list[NetworkPartitionResult]:
    """Write annual network Parquet partitions. Merges with existing partitions if present.

    Directory structure:
        curated/onchain/network_metrics_daily/source={source}/year={year}/data.parquet

    Atomic write: writes to temporary file and replaces target atomically.
    Returns list of NetworkPartitionResult sorted by year ascending.
    """
    curated_path = Path(curated_dir)
    existing_base = Path(existing_dir) if existing_dir is not None else curated_path

    # Group metrics by (source, year)
    grouped: dict[tuple[str, int], list[NormalizedNetworkMetric]] = defaultdict(list)
    for m in metrics:
        grouped[(m.source, m.metric_date_utc.year)].append(m)

    results: list[NetworkPartitionResult] = []

    for (source, year), new_metrics in grouped.items():
        partition_dir = (
            curated_path / "onchain" / "network_metrics_daily" / f"source={source}" / f"year={year}"
        )
        target_file = partition_dir / "data.parquet"

        existing_file = (
            existing_base
            / "onchain"
            / "network_metrics_daily"
            / f"source={source}"
            / f"year={year}"
            / "data.parquet"
        )
        source_existing = (
            existing_file
            if existing_file.exists()
            else (target_file if target_file.exists() else None)
        )
        is_new = source_existing is None
        existing_metrics = (
            read_network_partition_metrics(source_existing) if source_existing else []
        )

        # Merge and deduplicate by natural key: (source, asset, metric_date_utc)
        deduped = {(m.source, m.asset, m.metric_date_utc): m for m in existing_metrics}
        for m in new_metrics:
            key = (m.source, m.asset, m.metric_date_utc)
            if key not in deduped or (m.ingested_at_utc, m.source_run_id) >= (
                deduped[key].ingested_at_utc,
                deduped[key].source_run_id,
            ):
                deduped[key] = m

        merged_metrics = sorted(deduped.values(), key=lambda m: m.metric_date_utc)
        table = network_metrics_to_table(merged_metrics)

        partition_dir.mkdir(parents=True, exist_ok=True)
        temp_file = partition_dir / f"data.parquet.{uuid.uuid4().hex}.tmp"

        try:
            pq.write_table(table, temp_file)
            os.replace(temp_file, target_file)
        except Exception as exc:
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)
            raise NetworkParquetStorageError(
                f"Failed to write network partition {target_file}: {exc}"
            ) from exc

        results.append(
            NetworkPartitionResult(
                year=year,
                file_path=target_file,
                row_count=len(merged_metrics),
                is_new=is_new,
            )
        )

    results.sort(key=lambda r: r.year)
    return results
