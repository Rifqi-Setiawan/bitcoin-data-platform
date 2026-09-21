"""Application service for market data ingestion and promotion."""

from __future__ import annotations

import contextlib
import logging
import sys
from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from bitcoin_data_platform.quality.checks import QualityCheckError
from bitcoin_data_platform.quality.dataset_checks import run_dataset_quality_checks
from bitcoin_data_platform.sources.coinbase_client import (
    CoinbaseClient,
    CoinbaseClientError,
    SourceUnavailableError,
)
from bitcoin_data_platform.sources.coinbase_contract import validate_candle_payload
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.parquet_writer import (
    ParquetStorageError,
    write_parquet_partitions,
)
from bitcoin_data_platform.storage.raw_writer import (
    StorageError,
    create_raw_envelope,
    write_raw_envelope,
)
from bitcoin_data_platform.time_range import format_canonical_utc
from bitcoin_data_platform.transforms.normalizer import normalize_envelopes
from bitcoin_data_platform.transforms.raw_reader import read_raw_envelopes
from bitcoin_data_platform.window_planner import PlannedWindow

logger = logging.getLogger(__name__)


def ingest_candle_windows(
    cb_client: CoinbaseClient,
    windows: Sequence[PlannedWindow],
    run_id: str,
    output_dir: Path,
    *,
    write_envelope_fn: Callable[..., Any] | None = None,
) -> tuple[int, int, int, int, list[str], str | None]:
    """Ingest candle windows sequentially, writing raw envelopes.

    Returns:
        (exit_code, windows_succeeded, windows_failed, candles_ingested, files_written, error_msg)
    """
    writer = write_envelope_fn if write_envelope_fn is not None else write_raw_envelope
    windows_succeeded = 0
    windows_failed = 0
    candles_ingested = 0
    files_written: list[str] = []
    exit_code = 0
    error_msg: str | None = None

    for window in windows:
        logger.info(
            "window_started: run_id=%s window_index=%d start=%s end=%s",
            run_id,
            window.index,
            format_canonical_utc(window.start_utc),
            format_canonical_utc(window.end_utc),
        )
        try:
            response = cb_client.fetch_candles(window.start_utc, window.end_utc)
        except (SourceUnavailableError, CoinbaseClientError) as exc:
            windows_failed += 1
            is_unavailable = isinstance(exc, SourceUnavailableError)
            prefix = "Coinbase source unavailable" if is_unavailable else "Coinbase client error"
            error_msg = f"{prefix}: {exc}"
            logger.error("coinbase_error: run_id=%s error=%s", run_id, str(exc))
            exit_code = 3
            break

        validation = validate_candle_payload(response.raw_payload)
        if not validation.is_valid:
            windows_failed += 1
            error_msg = f"Contract violation: {'; '.join(validation.violations)}"
            logger.error(
                "contract_violation: run_id=%s violations=%s", run_id, validation.violations
            )
            exit_code = 4
            break

        try:
            envelope = create_raw_envelope(
                run_id=run_id,
                product_id="BTC-USD",
                granularity_seconds=3600,
                start_utc=window.start_utc,
                end_utc=window.end_utc,
                retrieved_at_utc=response.retrieved_at_utc,
                http_status=response.http_status,
                payload=response.raw_payload,
                provider_request_id=response.provider_request_id,
            )
            file_path = writer(output_dir, envelope)
            files_written.append(str(file_path))
            windows_succeeded += 1
            candles_ingested += len(validation.valid_candles)
            logger.info(
                "window_persisted: path=%s candles=%d", file_path, len(validation.valid_candles)
            )
        except (StorageError, OSError) as exc:
            windows_failed += 1
            error_msg = f"Storage failure: {exc}"
            logger.error("storage_failure: run_id=%s error=%s", run_id, str(exc))
            exit_code = 5
            break

    return (
        exit_code,
        windows_succeeded,
        windows_failed,
        candles_ingested,
        files_written,
        error_msg,
    )


def promote_market_data(
    raw_dir: Path,
    curated_dir: Path,
    db_manager: DuckDBManager,
    run_id: str,
    now_utc: datetime,
    *,
    clock: Callable[[], datetime] | None = None,
    update_watermark: bool = True,
    clear_existing: bool = False,
) -> tuple[int, str | None, int, int, int]:
    """Promote raw envelopes to curated Parquet partitions and refresh DuckDB views.

    Returns:
        (exit_code, error_msg, envelopes_count, rows_promoted, partitions_count)
    """
    try:
        envelopes = read_raw_envelopes(raw_dir)
    except Exception as exc:
        logger.error("raw_read_failed: %s", exc)
        return 2, str(exc), 0, 0, 0

    if not envelopes:
        return 0, None, 0, 0, 0

    try:
        candles = normalize_envelopes(envelopes, now_utc=now_utc)
    except QualityCheckError as exc:
        logger.error("quality_failure: %s", exc)
        sys.stderr.write(f"error: quality failure: {exc}\n")
        return 4, str(exc), len(envelopes), 0, 0
    except Exception as exc:
        logger.error("normalization_failure: %s", exc)
        sys.stderr.write(f"error: normalization failure: {exc}\n")
        return 4, str(exc), len(envelopes), 0, 0

    req_start = min((e.start_utc for e in envelopes), default=None)
    req_end = max((e.end_utc for e in envelopes), default=None)
    quality_records = run_dataset_quality_checks(
        candles,
        run_id=run_id,
        requested_start=req_start,
        requested_end=req_end,
        evaluated_at_utc=now_utc,
    )
    try:
        db_manager.record_quality_checks(quality_records)
    except Exception as exc:
        logger.warning("quality_checks_persist_failed: %s", exc)

    blocking_failures = [
        r for r in quality_records if r.severity == "BLOCK" and r.status == "FAILED"
    ]
    if blocking_failures:
        failure_details = "; ".join(f"{r.rule_name}: {r.details}" for r in blocking_failures)
        logger.error("quality_check_blocked: %s", failure_details)
        sys.stderr.write(f"error: quality check violation: {failure_details}\n")
        return 4, f"Dataset quality check violation: {failure_details}", len(envelopes), 0, 0

    warn_failures = [r for r in quality_records if r.severity == "WARN" and r.status == "FAILED"]
    if warn_failures:
        warn_details = "; ".join(f"{r.rule_name}: {r.details}" for r in warn_failures)
        logger.warning("quality_check_warning: %s", warn_details)

    if clear_existing:
        for p_file in curated_dir.glob("market/candles_hourly/source=*/year=*/*.parquet"):
            with contextlib.suppress(OSError):
                p_file.unlink()

    try:
        partitions = write_parquet_partitions(candles, curated_dir=curated_dir)
    except (ParquetStorageError, OSError) as exc:
        logger.error("storage_failure: %s", exc)
        sys.stderr.write(f"error: storage failure: {exc}\n")
        return 5, str(exc), len(envelopes), 0, 0

    try:
        db_manager.initialize()
        completed_at_utc = clock() if clock is not None else now_utc
        if update_watermark and candles:
            max_candle_ts = max(c.candle_start_utc for c in candles)
            current_wm = db_manager.get_watermark()
            if current_wm is None or max_candle_ts > current_wm:
                db_manager.set_watermark(max_candle_ts, run_id=run_id, now_utc=completed_at_utc)
    except Exception as exc:
        logger.error("database_failure: %s", exc)
        sys.stderr.write(f"error: database failure: {exc}\n")
        return 5, str(exc), len(envelopes), len(candles), len(partitions)

    return 0, None, len(envelopes), len(candles), len(partitions)
