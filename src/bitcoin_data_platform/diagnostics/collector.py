"""Out-of-band, 100% read-only telemetry collector across storage, catalog, and system state."""

from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from bitcoin_data_platform.diagnostics.models import TelemetryBundle
from bitcoin_data_platform.time_range import format_canonical_utc, parse_iso_utc

SMALL_FILE_THRESHOLD_BYTES = 16 * 1024 * 1024  # 16 MiB


class TelemetryCollector:
    """Collects platform diagnostic telemetry strictly using read-only connections."""

    def __init__(
        self,
        *,
        db_path: Path | str | None = None,
        curated_dir: Path | str | None = None,
        lakehouse_catalog_dir: Path | str | None = None,
        alert_state_path: Path | str | None = None,
        target_run_id: str | None = None,
        now_utc: datetime | None = None,
    ) -> None:
        self.db_path = Path(db_path) if db_path else Path("./data/state/platform.duckdb")
        self.curated_dir = Path(curated_dir) if curated_dir else Path("./data/curated")
        self.lakehouse_catalog_dir = (
            Path(lakehouse_catalog_dir)
            if lakehouse_catalog_dir
            else Path("./data/lakehouse/catalog")
        )
        self.alert_state_path = (
            Path(alert_state_path) if alert_state_path else Path("./data/state/alert_state.json")
        )
        self.target_run_id = target_run_id
        self.now_utc = (
            now_utc.astimezone(UTC)
            if now_utc and now_utc.tzinfo
            else (now_utc.replace(tzinfo=UTC) if now_utc else datetime.now(UTC))
        )
        self.executed_queries: list[str] = []

    def _record_query(self, sql: str) -> None:
        """Append an executed SQL query to the audit trail."""
        self.executed_queries.append(sql.strip())

    def collect(self) -> TelemetryBundle:
        """Collect comprehensive read-only telemetry bundle."""
        last_run: dict[str, Any] | None = None
        recent_runs: list[dict[str, Any]] = []
        quality_checks: list[dict[str, Any]] = []
        watermark_utc: str | None = None
        watermark_age_hours: float | None = None
        gaps: list[dict[str, Any]] = []
        is_locked = False
        running_lock: dict[str, Any] | None = None

        # 1. Collect DuckDB Telemetry (read_only=True)
        if self.db_path.exists():
            con = None
            try:
                con = duckdb.connect(str(self.db_path), read_only=True)
                con.execute("SET TimeZone='UTC';")

                # Inspect existing tables
                tbl_sql = (
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';"
                )
                self._record_query(tbl_sql)
                existing_tables = {row[0] for row in con.execute(tbl_sql).fetchall()}

                # Read run_metadata
                if "run_metadata" in existing_tables:
                    # Lock status
                    lock_sql = (
                        "SELECT run_id, mode, status, "
                        "STRFTIME(started_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS started_at_utc, "
                        "STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS completed_at_utc, "
                        "rows_promoted, partitions_written, raw_envelopes_read, "
                        "error_message, error_class "
                        "FROM run_metadata WHERE UPPER(status) = 'RUNNING' "
                        "ORDER BY started_at_utc DESC LIMIT 1;"
                    )
                    self._record_query(lock_sql)
                    cur_lock = con.execute(lock_sql)
                    desc_lock = cur_lock.description
                    row_lock = cur_lock.fetchone()
                    if row_lock is not None and desc_lock is not None:
                        is_locked = True
                        cols = [d[0] for d in desc_lock]
                        running_lock = dict(zip(cols, row_lock, strict=True))

                    # Last run or target run
                    if self.target_run_id:
                        target_sql = (
                            "SELECT run_id, mode, status, "
                            "STRFTIME(started_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS started_at_utc, "
                            "STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS completed_at_utc, "
                            "rows_promoted, partitions_written, raw_envelopes_read, "
                            "error_message, error_class "
                            "FROM run_metadata WHERE run_id = ?;"
                        )
                        self._record_query(f"{target_sql} -- param: {self.target_run_id}")
                        cur_target = con.execute(target_sql, [self.target_run_id])
                        desc_target = cur_target.description
                        row_target = cur_target.fetchone()
                        if row_target is not None and desc_target is not None:
                            cols = [d[0] for d in desc_target]
                            last_run = dict(zip(cols, row_target, strict=True))

                    if last_run is None:
                        last_run_sql = (
                            "SELECT run_id, mode, status, "
                            "STRFTIME(started_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS started_at_utc, "
                            "STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS completed_at_utc, "
                            "rows_promoted, partitions_written, raw_envelopes_read, "
                            "error_message, error_class "
                            "FROM run_metadata ORDER BY started_at_utc DESC LIMIT 1;"
                        )
                        self._record_query(last_run_sql)
                        cur_last = con.execute(last_run_sql)
                        desc_last = cur_last.description
                        row_last = cur_last.fetchone()
                        if row_last is not None and desc_last is not None:
                            cols = [d[0] for d in desc_last]
                            last_run = dict(zip(cols, row_last, strict=True))

                    recent_sql = (
                        "SELECT run_id, mode, status, "
                        "STRFTIME(started_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS started_at_utc, "
                        "STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS completed_at_utc, "
                        "rows_promoted, partitions_written, raw_envelopes_read, "
                        "error_message, error_class "
                        "FROM run_metadata ORDER BY started_at_utc DESC LIMIT 10;"
                    )
                    self._record_query(recent_sql)
                    cur_rec = con.execute(recent_sql)
                    desc_rec = cur_rec.description
                    if desc_rec is not None:
                        cols = [d[0] for d in desc_rec]
                        recent_runs = [dict(zip(cols, r, strict=True)) for r in cur_rec.fetchall()]

                # Read quality_check_results
                if "quality_check_results" in existing_tables:
                    qc_sql = (
                        "SELECT check_id, run_id, rule_name, severity, status, "
                        "metric_value, threshold_value, details, "
                        "STRFTIME(evaluated_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS evaluated_at_utc "
                        "FROM quality_check_results ORDER BY evaluated_at_utc DESC LIMIT 20;"
                    )
                    self._record_query(qc_sql)
                    cur_qc = con.execute(qc_sql)
                    desc_qc = cur_qc.description
                    if desc_qc is not None:
                        cols = [d[0] for d in desc_qc]
                        quality_checks = [
                            dict(zip(cols, r, strict=True)) for r in cur_qc.fetchall()
                        ]

                # Read pipeline_watermark
                if "pipeline_watermark" in existing_tables:
                    wm_sql = (
                        "SELECT STRFTIME(watermark_utc, '%Y-%m-%dT%H:%M:%SZ') AS watermark_utc "
                        "FROM pipeline_watermark WHERE pipeline_id = 'btc_usd_hourly';"
                    )
                    self._record_query(wm_sql)
                    cur_wm = con.execute(wm_sql)
                    row_wm = cur_wm.fetchone()
                    if row_wm is not None and row_wm[0] is not None:
                        watermark_utc = str(row_wm[0])
                        try:
                            wm_dt = parse_iso_utc(watermark_utc, "watermark_utc")
                            watermark_age_hours = round(
                                (self.now_utc - wm_dt).total_seconds() / 3600.0, 2
                            )
                        except Exception:
                            watermark_age_hours = None

                # Detect gaps in fact_market_candle_hourly
                if "fact_market_candle_hourly" in existing_tables:
                    gap_sql = (
                        "SELECT DISTINCT STRFTIME(candle_start_utc, '%Y-%m-%dT%H:%M:%SZ') "
                        "FROM fact_market_candle_hourly "
                        "WHERE candle_start_utc IS NOT NULL "
                        "ORDER BY candle_start_utc ASC;"
                    )
                    self._record_query(gap_sql)
                    cur_gap = con.execute(gap_sql)
                    rows_gap = cur_gap.fetchall()
                    if len(rows_gap) >= 2:
                        ts_list = [parse_iso_utc(str(r[0]), "candle_start_utc") for r in rows_gap]
                        step = timedelta(hours=1)
                        for i in range(len(ts_list) - 1):
                            curr_ts = ts_list[i]
                            next_ts = ts_list[i + 1]
                            diff = next_ts - curr_ts
                            if diff > step:
                                missing_hours = int(diff.total_seconds() // 3600) - 1
                                gaps.append(
                                    {
                                        "start_utc": format_canonical_utc(curr_ts + step),
                                        "end_utc": format_canonical_utc(next_ts),
                                        "missing_hours": missing_hours,
                                    }
                                )
            except Exception:
                # Read-only failure or corrupt database handled safely
                pass
            finally:
                if con is not None:
                    con.close()

        # 2. Collect Lakehouse Telemetry (SQLite mode=ro)
        lakehouse_tables = self._collect_lakehouse_telemetry()

        # 3. Collect Disk Usage
        disk = self._collect_disk_usage()

        # 4. Collect Alert State
        alert_state = self._collect_alert_state()

        return TelemetryBundle(
            last_run=last_run,
            recent_runs=recent_runs,
            quality_checks=quality_checks,
            watermark_utc=watermark_utc,
            watermark_age_hours=watermark_age_hours,
            gaps=gaps,
            is_locked=is_locked,
            running_lock=running_lock,
            lakehouse_tables=lakehouse_tables,
            disk=disk,
            alert_state=alert_state,
            executed_queries=list(self.executed_queries),
            collected_at_utc=self.now_utc,
        )

    def _collect_lakehouse_telemetry(self) -> dict[str, Any]:
        """Inspect Lakehouse SQLite catalog strictly with read-only URI mode=ro."""
        catalog_db = self.lakehouse_catalog_dir / "platform_catalog.sqlite"
        if not catalog_db.exists():
            return {}

        lakehouse_tables: dict[str, Any] = {}
        conn = None
        try:
            # Connect strictly in read-only mode using SQLite URI
            conn = sqlite3.connect(f"file:{catalog_db.resolve()}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row

            tbl_sql = "SELECT table_name, table_uuid, location FROM tables ORDER BY table_name ASC;"
            self._record_query(f"[sqlite:ro] {tbl_sql}")
            cur = conn.execute(tbl_sql)
            tables = cur.fetchall()

            for t in tables:
                table_name = str(t["table_name"])
                table_location = Path(t["location"])

                branch_sql = (
                    "SELECT current_snapshot_id FROM table_branches "
                    "WHERE table_name = ? AND branch_name = 'main';"
                )
                self._record_query(f"[sqlite:ro] {branch_sql} -- param: {table_name}")
                cur_branch = conn.execute(branch_sql, (table_name,))
                branch_row = cur_branch.fetchone()
                current_snapshot_id = (
                    int(branch_row["current_snapshot_id"])
                    if branch_row and branch_row["current_snapshot_id"] is not None
                    else None
                )

                small_files_by_partition: dict[str, int] = {}
                manifest_files_count = 0
                total_small_files = 0

                if current_snapshot_id is not None:
                    snap_sql = (
                        "SELECT snapshot_id, manifest_files_json, summary_json, created_at_utc "
                        "FROM snapshots WHERE table_name = ? AND snapshot_id = ?;"
                    )
                    self._record_query(
                        f"[sqlite:ro] {snap_sql} -- params: {table_name}, {current_snapshot_id}"
                    )
                    cur_snap = conn.execute(snap_sql, (table_name, current_snapshot_id))
                    snap_row = cur_snap.fetchone()
                    if snap_row:
                        manifest_files = json.loads(snap_row["manifest_files_json"])
                        manifest_files_count = len(manifest_files)

                        for mf in manifest_files:
                            file_path = Path(mf)
                            if not file_path.is_absolute():
                                file_path = table_location / file_path

                            if file_path.exists():
                                f_size = file_path.stat().st_size
                                try:
                                    parent_rel = str(file_path.parent.relative_to(table_location))
                                    part_key = "root" if parent_rel == "." else parent_rel
                                except Exception:
                                    part_key = "root"

                                if f_size < SMALL_FILE_THRESHOLD_BYTES:
                                    small_files_by_partition[part_key] = (
                                        small_files_by_partition.get(part_key, 0) + 1
                                    )
                                    total_small_files += 1

                lakehouse_tables[table_name] = {
                    "table_name": table_name,
                    "location": str(table_location),
                    "current_snapshot_id": current_snapshot_id,
                    "manifest_files_count": manifest_files_count,
                    "small_files_count": total_small_files,
                    "small_files_by_partition": small_files_by_partition,
                }
        except Exception:
            pass
        finally:
            if conn is not None:
                conn.close()

        return lakehouse_tables

    def _collect_disk_usage(self) -> dict[str, Any]:
        """Inspect storage volume utilization."""
        target_path = (
            self.curated_dir
            if self.curated_dir.exists()
            else (self.db_path.parent if self.db_path.parent.exists() else Path("."))
        )
        try:
            usage = shutil.disk_usage(target_path)
            total_gb = round(usage.total / (1024**3), 2)
            used_gb = round(usage.used / (1024**3), 2)
            free_gb = round(usage.free / (1024**3), 2)
            percent_used = round((usage.used / usage.total) * 100.0, 2) if usage.total > 0 else 0.0
        except Exception:
            total_gb, used_gb, free_gb, percent_used = 0.0, 0.0, 0.0, 0.0

        return {
            "target_path": str(target_path),
            "total_gb": total_gb,
            "used_gb": used_gb,
            "free_gb": free_gb,
            "percent_used": percent_used,
            "disk_warning": percent_used > 70.0,
            "disk_critical": percent_used > 80.0,
        }

    def _collect_alert_state(self) -> dict[str, Any]:
        """Inspect failure alert state if present."""
        path = self.alert_state_path
        if not path.exists():
            # Fallback path
            fallback = Path("/srv/data/bitcoin-data-platform/state/alert_state.json")
            if fallback.exists():
                path = fallback
            else:
                return {}

        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}
