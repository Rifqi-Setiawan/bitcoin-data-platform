"""DuckDB manager for curated analytical views and pipeline run metadata."""

from datetime import datetime
from pathlib import Path
from typing import Any, cast

import duckdb


class DuckDBManagerError(Exception):
    """Raised when an error occurs in DuckDB operations."""


class DuckDBManager:
    """Manages DuckDB database connection, analytical views, and execution."""

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
        """Create run_metadata table if not exists."""
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
            error_message VARCHAR
        );
        """
        con.execute(sql)

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
        self.create_hourly_view()
        self.create_daily_mart_view()

    def record_run(
        self,
        *,
        run_id: str,
        mode: str,
        started_at_utc: datetime,
        completed_at_utc: datetime,
        status: str,
        rows_promoted: int,
        partitions_written: int,
        raw_envelopes_read: int,
        error_message: str | None = None,
    ) -> None:
        """Record a pipeline promotion run in run_metadata."""
        con = self.get_connection()
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
            error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                error_message,
            ],
        )

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
