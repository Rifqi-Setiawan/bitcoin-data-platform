"""DuckDB manager for curated analytical views, pipeline watermark, run locking, and metadata."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb

from bitcoin_data_platform.time_range import format_canonical_utc, parse_iso_utc


class DuckDBManagerError(Exception):
    """Raised when an error occurs in DuckDB operations."""


class RunLockError(DuckDBManagerError):
    """Raised when a concurrent run is detected."""


class DuckDBManager:
    """Manages DuckDB database connection, analytical views, run locking, and execution."""

    def __init__(self, db_path: Path | str, curated_dir: Path | str) -> None:
        self.db_path_str = str(db_path)
        self.db_path = Path(db_path) if self.db_path_str != ":memory:" else None
        self.curated_dir = Path(curated_dir)
        self._connection: duckdb.DuckDBPyConnection | None = None

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create the active DuckDB connection."""
        if self._connection is None:
            if self.db_path is not None:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                con = duckdb.connect(self.db_path_str)
                con.execute("SET TimeZone='UTC';")
                self._connection = con
            except Exception as exc:
                raise DuckDBManagerError(
                    f"Failed to connect to DuckDB at {self.db_path_str}: {exc}"
                ) from exc
        return self._connection

    def close(self) -> None:
        """Close the active DuckDB connection if open."""
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def __enter__(self) -> "DuckDBManager":
        self.get_connection()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def create_metadata_table(self) -> None:
        """Create run_metadata table if not exists, and migrate columns if missing."""
        con = self.get_connection()
        sql = """
        CREATE TABLE IF NOT EXISTS run_metadata (
            run_id VARCHAR PRIMARY KEY,
            mode VARCHAR,
            started_at_utc TIMESTAMPTZ,
            completed_at_utc TIMESTAMPTZ,
            status VARCHAR,
            rows_promoted INTEGER,
            partitions_written INTEGER,
            raw_envelopes_read INTEGER,
            requested_start_utc TIMESTAMPTZ,
            requested_end_utc TIMESTAMPTZ,
            old_watermark_utc TIMESTAMPTZ,
            new_watermark_utc TIMESTAMPTZ,
            error_message VARCHAR
        );
        """
        con.execute(sql)

        # Handle schema migration for existing databases missing new Phase 3 columns
        existing_cols = {
            row[1] for row in con.execute("PRAGMA table_info('run_metadata');").fetchall()
        }
        migration_cols = [
            ("requested_start_utc", "TIMESTAMPTZ"),
            ("requested_end_utc", "TIMESTAMPTZ"),
            ("old_watermark_utc", "TIMESTAMPTZ"),
            ("new_watermark_utc", "TIMESTAMPTZ"),
        ]
        for col_name, col_type in migration_cols:
            if col_name not in existing_cols:
                con.execute(f"ALTER TABLE run_metadata ADD COLUMN {col_name} {col_type};")

    def create_watermark_table(self) -> None:
        """Create pipeline_watermark table if not exists."""
        con = self.get_connection()
        sql = """
        CREATE TABLE IF NOT EXISTS pipeline_watermark (
            pipeline_id VARCHAR PRIMARY KEY DEFAULT 'btc_usd_hourly',
            watermark_utc TIMESTAMPTZ NOT NULL,
            updated_at_utc TIMESTAMPTZ NOT NULL,
            updated_by_run_id VARCHAR NOT NULL
        );
        """
        con.execute(sql)

    def get_watermark(self, pipeline_id: str = "btc_usd_hourly") -> datetime | None:
        """Get current watermark timestamp in UTC for pipeline_id, or None if not set."""
        con = self.get_connection()
        self.create_watermark_table()
        try:
            sql = (
                "SELECT STRFTIME(watermark_utc, '%Y-%m-%dT%H:%M:%S.%fZ') "
                "FROM pipeline_watermark WHERE pipeline_id = ?"
            )
            cursor = con.execute(sql, [pipeline_id])
            row = cursor.fetchone()
            if row is None or row[0] is None:
                return None
            return parse_iso_utc(str(row[0]), "watermark_utc")
        except Exception:
            return None

    def set_watermark(
        self,
        watermark_utc: datetime,
        run_id: str,
        pipeline_id: str = "btc_usd_hourly",
        now_utc: datetime | None = None,
        force: bool = False,
    ) -> bool:
        """Set or advance watermark timestamp monotonically.

        Returns True if watermark was set or advanced; False if skipped (due to monotonicity).
        """
        con = self.get_connection()
        self.create_watermark_table()

        if watermark_utc.tzinfo is None:
            watermark_utc = watermark_utc.replace(tzinfo=UTC)
        else:
            watermark_utc = watermark_utc.astimezone(UTC)

        current = self.get_watermark(pipeline_id)
        if current is not None and watermark_utc <= current and not force:
            return False

        updated_at = now_utc if now_utc is not None else datetime.now(UTC)
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=UTC)
        else:
            updated_at = updated_at.astimezone(UTC)

        if current is None:
            sql = """
            INSERT INTO pipeline_watermark (
                pipeline_id, watermark_utc, updated_at_utc, updated_by_run_id
            ) VALUES (?, ?, ?, ?);
            """
            con.execute(sql, [pipeline_id, watermark_utc, updated_at, run_id])
        else:
            sql = """
            UPDATE pipeline_watermark
            SET watermark_utc = ?, updated_at_utc = ?, updated_by_run_id = ?
            WHERE pipeline_id = ?;
            """
            con.execute(sql, [watermark_utc, updated_at, run_id, pipeline_id])
        return True

    def check_lock(self) -> bool:
        """Check if any run is currently RUNNING."""
        con = self.get_connection()
        self.create_metadata_table()
        cursor = con.execute("SELECT COUNT(*) FROM run_metadata WHERE UPPER(status) = 'RUNNING';")
        row = cursor.fetchone()
        return bool(row is not None and row[0] > 0)

    def get_running_lock(self) -> dict[str, Any] | None:
        """Get details of current active running run, or None."""
        con = self.get_connection()
        self.create_metadata_table()
        cursor = con.execute(
            """
            SELECT
                run_id,
                mode,
                STRFTIME(started_at_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS started_at_utc,
                STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS completed_at_utc,
                status,
                rows_promoted,
                partitions_written,
                raw_envelopes_read,
                STRFTIME(requested_start_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS requested_start_utc,
                STRFTIME(requested_end_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS requested_end_utc,
                STRFTIME(old_watermark_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS old_watermark_utc,
                STRFTIME(new_watermark_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS new_watermark_utc,
                error_message
            FROM run_metadata
            WHERE UPPER(status) = 'RUNNING'
            ORDER BY started_at_utc DESC
            LIMIT 1;
            """
        )
        desc = cursor.description
        row = cursor.fetchone()
        if row is None or desc is None:
            return None
        col_names = [d[0] for d in desc]
        return dict(zip(col_names, row, strict=True))

    def acquire_lock(
        self,
        *,
        run_id: str,
        mode: str,
        started_at_utc: datetime,
        requested_start_utc: datetime | None = None,
        requested_end_utc: datetime | None = None,
        old_watermark_utc: datetime | None = None,
    ) -> bool:
        """Attempt to acquire run lock by inserting a RUNNING record.

        Returns True if acquired, False if already locked.
        """
        con = self.get_connection()
        self.create_metadata_table()
        if self.check_lock():
            return False

        if started_at_utc.tzinfo is None:
            started_at_utc = started_at_utc.replace(tzinfo=UTC)
        else:
            started_at_utc = started_at_utc.astimezone(UTC)

        sql = """
        INSERT INTO run_metadata (
            run_id,
            mode,
            started_at_utc,
            status,
            requested_start_utc,
            requested_end_utc,
            old_watermark_utc
        ) VALUES (?, ?, ?, 'RUNNING', ?, ?, ?);
        """
        con.execute(
            sql,
            [
                run_id,
                mode,
                started_at_utc,
                requested_start_utc,
                requested_end_utc,
                old_watermark_utc,
            ],
        )
        return True

    def release_lock(
        self,
        *,
        run_id: str,
        status: str = "SUCCEEDED",
        completed_at_utc: datetime | None = None,
        rows_promoted: int = 0,
        partitions_written: int = 0,
        raw_envelopes_read: int = 0,
        old_watermark_utc: datetime | None = None,
        new_watermark_utc: datetime | None = None,
        error_message: str | None = None,
    ) -> None:
        """Release run lock by updating status and execution details."""
        con = self.get_connection()
        self.create_metadata_table()

        if completed_at_utc is None:
            completed_at_utc = datetime.now(UTC)
        elif completed_at_utc.tzinfo is None:
            completed_at_utc = completed_at_utc.replace(tzinfo=UTC)
        else:
            completed_at_utc = completed_at_utc.astimezone(UTC)

        sql = """
        UPDATE run_metadata
        SET status = ?,
            completed_at_utc = ?,
            rows_promoted = ?,
            partitions_written = ?,
            raw_envelopes_read = ?,
            old_watermark_utc = COALESCE(?, old_watermark_utc),
            new_watermark_utc = ?,
            error_message = ?
        WHERE run_id = ?;
        """
        con.execute(
            sql,
            [
                status,
                completed_at_utc,
                rows_promoted,
                partitions_written,
                raw_envelopes_read,
                old_watermark_utc,
                new_watermark_utc,
                error_message,
                run_id,
            ],
        )

    def force_clear_lock(
        self,
        stale_threshold_seconds: int = 3600,
        now_utc: datetime | None = None,
    ) -> int:
        """Force-clear stale RUNNING locks older than stale_threshold_seconds.

        If stale_threshold_seconds <= 0, clears all RUNNING locks regardless of age.
        Returns number of locks cleared.
        """
        con = self.get_connection()
        self.create_metadata_table()

        if now_utc is None:
            now_utc = datetime.now(UTC)
        elif now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=UTC)
        else:
            now_utc = now_utc.astimezone(UTC)

        query = (
            "SELECT run_id, STRFTIME(started_at_utc, '%Y-%m-%dT%H:%M:%S.%fZ') "
            "FROM run_metadata WHERE UPPER(status) = 'RUNNING';"
        )
        rows = con.execute(query).fetchall()

        cleared_count = 0
        for run_id, started_at_str in rows:
            s_at = parse_iso_utc(str(started_at_str), "started_at_utc")
            age_seconds = (now_utc - s_at).total_seconds()
            if stale_threshold_seconds <= 0 or age_seconds >= stale_threshold_seconds:
                con.execute(
                    """
                    UPDATE run_metadata
                    SET status = 'FAILED',
                        completed_at_utc = ?,
                        error_message = 'Cleared by force_clear_lock'
                    WHERE run_id = ?;
                    """,
                    [now_utc, run_id],
                )
                cleared_count += 1
        return cleared_count

    def create_hourly_view(self) -> None:
        """Create or replace view fact_market_candle_hourly pointing to Parquet files."""
        con = self.get_connection()
        parquet_glob = str(
            self.curated_dir.resolve()
            / "market"
            / "candles_hourly"
            / "source=*"
            / "year=*"
            / "*.parquet"
        )
        existing_files = list(
            self.curated_dir.glob("market/candles_hourly/source=*/year=*/*.parquet")
        )

        if existing_files:
            sql = f"""
            CREATE OR REPLACE VIEW fact_market_candle_hourly AS
            SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true);
            """
        else:
            # Fallback view with full schema when no files are present yet
            sql = """
            CREATE OR REPLACE VIEW fact_market_candle_hourly AS
            SELECT
                CAST(NULL AS VARCHAR) AS source,
                CAST(NULL AS VARCHAR) AS product_id,
                CAST(NULL AS INTEGER) AS granularity_seconds,
                CAST(NULL AS TIMESTAMPTZ) AS candle_start_utc,
                CAST(NULL AS DECIMAL(38,18)) AS open,
                CAST(NULL AS DECIMAL(38,18)) AS high,
                CAST(NULL AS DECIMAL(38,18)) AS low,
                CAST(NULL AS DECIMAL(38,18)) AS close,
                CAST(NULL AS DECIMAL(38,18)) AS volume_base,
                CAST(NULL AS TIMESTAMPTZ) AS ingested_at_utc,
                CAST(NULL AS VARCHAR) AS source_run_id,
                CAST(NULL AS INTEGER) AS year
            WHERE 1=0;
            """
        con.execute(sql)

    def create_daily_mart_view(self) -> None:
        """Create or replace analytical view mart_btc_usd_daily derived from hourly fact."""
        con = self.get_connection()
        sql = """
        CREATE OR REPLACE VIEW mart_btc_usd_daily AS
        SELECT
            source,
            product_id,
            DATE_TRUNC('day', candle_start_utc) AS trade_date_utc,
            FIRST(open ORDER BY candle_start_utc) AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            LAST(close ORDER BY candle_start_utc) AS close,
            SUM(volume_base) AS volume_base,
            COUNT(*) AS observed_hour_count,
            COUNT(*) = 24 AS is_complete
        FROM fact_market_candle_hourly
        GROUP BY source, product_id, DATE_TRUNC('day', candle_start_utc);
        """
        con.execute(sql)

    def initialize(self) -> None:
        """Initialize database schema, tables, and views."""
        self.create_metadata_table()
        self.create_watermark_table()
        self.create_hourly_view()
        self.create_daily_mart_view()

    def record_run(
        self,
        *,
        run_id: str,
        mode: str,
        started_at_utc: datetime,
        completed_at_utc: datetime | None = None,
        status: str,
        rows_promoted: int = 0,
        partitions_written: int = 0,
        raw_envelopes_read: int = 0,
        requested_start_utc: datetime | None = None,
        requested_end_utc: datetime | None = None,
        old_watermark_utc: datetime | None = None,
        new_watermark_utc: datetime | None = None,
        error_message: str | None = None,
    ) -> None:
        """Record or update a pipeline promotion run in run_metadata."""
        con = self.get_connection()
        self.create_metadata_table()

        if started_at_utc.tzinfo is None:
            started_at_utc = started_at_utc.replace(tzinfo=UTC)
        else:
            started_at_utc = started_at_utc.astimezone(UTC)

        if completed_at_utc is not None:
            if completed_at_utc.tzinfo is None:
                completed_at_utc = completed_at_utc.replace(tzinfo=UTC)
            else:
                completed_at_utc = completed_at_utc.astimezone(UTC)

        sql = """
        INSERT OR REPLACE INTO run_metadata (
            run_id,
            mode,
            started_at_utc,
            completed_at_utc,
            status,
            rows_promoted,
            partitions_written,
            raw_envelopes_read,
            requested_start_utc,
            requested_end_utc,
            old_watermark_utc,
            new_watermark_utc,
            error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        con.execute(
            sql,
            [
                run_id,
                mode,
                started_at_utc,
                completed_at_utc,
                status,
                rows_promoted,
                partitions_written,
                raw_envelopes_read,
                requested_start_utc,
                requested_end_utc,
                old_watermark_utc,
                new_watermark_utc,
                error_message,
            ],
        )

    def get_last_run(self) -> dict[str, Any] | None:
        """Get metadata for latest pipeline run."""
        con = self.get_connection()
        self.create_metadata_table()
        cursor = con.execute(
            """
            SELECT
                run_id,
                mode,
                status,
                STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS comp_str,
                rows_promoted
            FROM run_metadata
            ORDER BY started_at_utc DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()
        if row is None:
            return None

        return {
            "run_id": row[0],
            "mode": row[1],
            "status": row[2],
            "completed_at_utc": row[3],
            "rows_promoted": row[4] if row[4] is not None else 0,
        }

    def get_last_success(self) -> dict[str, Any] | None:
        """Get metadata for latest successful pipeline run."""
        con = self.get_connection()
        self.create_metadata_table()
        cursor = con.execute(
            """
            SELECT
                run_id,
                STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS comp_str
            FROM run_metadata
            WHERE UPPER(status) IN ('SUCCEEDED', 'SUCCESS')
            ORDER BY completed_at_utc DESC, started_at_utc DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()
        if row is None:
            return None

        return {
            "run_id": row[0],
            "completed_at_utc": row[1],
        }

    def get_last_failure(self) -> dict[str, Any] | None:
        """Get metadata for latest failed pipeline run."""
        con = self.get_connection()
        self.create_metadata_table()
        cursor = con.execute(
            """
            SELECT
                run_id,
                STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS comp_str
            FROM run_metadata
            WHERE UPPER(status) IN ('FAILED', 'FAILURE')
            ORDER BY completed_at_utc DESC, started_at_utc DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()
        if row is None:
            return None

        return {
            "run_id": row[0],
            "completed_at_utc": row[1],
        }

    def get_curated_stats(self) -> dict[str, Any]:
        """Get summary statistics of curated Parquet data and disk usage."""
        con = self.get_connection()
        total_rows = 0
        min_candle_str: str | None = None
        max_candle_str: str | None = None

        try:
            cursor = con.execute(
                """
                SELECT
                    COUNT(*) AS total_rows,
                    STRFTIME(MIN(candle_start_utc), '%Y-%m-%dT%H:%M:%SZ') AS min_ts,
                    STRFTIME(MAX(candle_start_utc), '%Y-%m-%dT%H:%M:%SZ') AS max_ts
                FROM fact_market_candle_hourly;
                """
            )
            rows = cursor.fetchall()
            if rows:
                total_rows = rows[0][0] if rows[0][0] is not None else 0
                min_candle_str = rows[0][1]
                max_candle_str = rows[0][2]
        except Exception:
            pass

        parquet_files = list(
            self.curated_dir.glob("market/candles_hourly/source=*/year=*/*.parquet")
        )
        total_size_bytes = sum(f.stat().st_size for f in parquet_files if f.exists())

        return {
            "total_rows": total_rows,
            "min_candle_utc": min_candle_str,
            "max_candle_utc": max_candle_str,
            "partitions": len(parquet_files),
            "total_size_bytes": total_size_bytes,
        }

    def detect_gaps(self) -> list[dict[str, Any]]:
        """Detect any missing hours between min and max candle timestamps in curated fact view."""
        con = self.get_connection()
        try:
            cursor = con.execute(
                """
                SELECT DISTINCT STRFTIME(candle_start_utc, '%Y-%m-%dT%H:%M:%SZ')
                FROM fact_market_candle_hourly
                WHERE candle_start_utc IS NOT NULL
                ORDER BY candle_start_utc ASC;
                """
            )
            rows = cursor.fetchall()
        except Exception:
            return []

        if len(rows) < 2:
            return []

        timestamps: list[datetime] = [parse_iso_utc(str(r[0]), "candle_start_utc") for r in rows]

        gaps: list[dict[str, Any]] = []
        step = timedelta(hours=1)
        for i in range(len(timestamps) - 1):
            curr_ts = timestamps[i]
            next_ts = timestamps[i + 1]
            diff = next_ts - curr_ts
            if diff > step:
                gap_start = curr_ts + step
                gap_end = next_ts
                missing_hours = int(diff.total_seconds() // 3600) - 1
                gap_start_str = format_canonical_utc(gap_start)
                gap_end_str = format_canonical_utc(gap_end)
                gaps.append(
                    {
                        "start_utc": gap_start_str,
                        "gap_start_utc": gap_start_str,
                        "end_utc": gap_end_str,
                        "gap_end_utc": gap_end_str,
                        "missing_hours": missing_hours,
                    }
                )
        return gaps

    def get_status(self, now_utc: datetime | None = None) -> dict[str, Any]:
        """Compile comprehensive status dictionary as specified in Phase 3 spec."""
        if now_utc is None:
            now_utc = datetime.now(UTC)
        elif now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=UTC)
        else:
            now_utc = now_utc.astimezone(UTC)

        self.initialize()

        watermark = self.get_watermark()
        watermark_str = format_canonical_utc(watermark) if watermark is not None else None
        watermark_age_hours = (
            round((now_utc - watermark).total_seconds() / 3600.0, 2)
            if watermark is not None
            else None
        )

        last_run = self.get_last_run()
        last_success = self.get_last_success()
        last_failure = self.get_last_failure()
        curated_stats = self.get_curated_stats()
        gaps = self.detect_gaps()
        is_locked = self.check_lock()

        return {
            "watermark_utc": watermark_str,
            "watermark_age_hours": watermark_age_hours,
            "last_run": last_run,
            "last_success": last_success,
            "last_failure": last_failure,
            "curated_stats": curated_stats,
            "gaps": gaps,
            "is_locked": is_locked,
        }

    def execute_query(self, sql: str) -> list[dict[str, Any]]:
        """Execute arbitrary SQL and return results as a list of dictionaries."""
        con = self.get_connection()
        try:
            cursor = con.execute(sql)
            if cursor.description is None:
                return []
            try:
                if hasattr(cursor, "to_arrow_table"):
                    arrow_table = cursor.to_arrow_table()
                elif hasattr(cursor, "fetch_arrow_table"):
                    arrow_table = cursor.fetch_arrow_table()
                else:
                    reader = cursor.arrow()
                    arrow_table = reader.read_all() if hasattr(reader, "read_all") else reader
                res = arrow_table.to_pylist()
                return cast(list[dict[str, Any]], res)
            except Exception:
                col_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(col_names, row, strict=True)) for row in rows]
        except Exception as exc:
            raise DuckDBManagerError(f"SQL execution error: {exc}") from exc
