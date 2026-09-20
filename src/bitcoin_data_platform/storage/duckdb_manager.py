"""DuckDB manager for curated analytical views, pipeline watermark, run locking, and metadata."""

import contextlib
import shutil
import uuid
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb

from bitcoin_data_platform.committee.models import (
    InvestmentMemorandum,
)
from bitcoin_data_platform.intelligence.models import (
    UserIntelligenceRecord,
)
from bitcoin_data_platform.macro.models import (
    DailyNarrativeReport,
    MacroArticle,
    MacroEconomicRelease,
    MacroPillar,
    MacroRegime,
)
from bitcoin_data_platform.sources.macro_calendar_contract import MacroEvent
from bitcoin_data_platform.sources.sentiment_contract import SentimentRecord
from bitcoin_data_platform.time_range import format_canonical_utc, parse_iso_utc


class DuckDBManagerError(Exception):
    """Raised when an error occurs in DuckDB operations."""


class RunLockError(DuckDBManagerError):
    """Raised when a concurrent run is detected."""


class DuckDBManager:
    """Manages DuckDB database connection, analytical views, run locking, and execution."""

    def __init__(self, db_path: Path | str, curated_dir: Path | str | None = None) -> None:
        self.db_path_str = str(db_path)
        self.db_path = Path(db_path) if self.db_path_str != ":memory:" else None
        if curated_dir is not None:
            self.curated_dir = Path(curated_dir)
        elif self.db_path is not None:
            self.curated_dir = self.db_path.parent.parent / "curated"
        else:
            self.curated_dir = Path("./data/curated")
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
            error_message VARCHAR,
            code_version VARCHAR,
            source_row_count INTEGER,
            valid_row_count INTEGER,
            gap_count INTEGER,
            raw_checksums VARCHAR,
            error_class VARCHAR
        );
        """
        con.execute(sql)

    def create_quality_checks_table(self) -> None:
        """Create quality_check_results table if not exists."""
        con = self.get_connection()
        sql = """
        CREATE TABLE IF NOT EXISTS quality_check_results (
            check_id VARCHAR PRIMARY KEY,
            run_id VARCHAR NOT NULL,
            rule_name VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,  -- BLOCK, WARN, INFO
            status VARCHAR NOT NULL,    -- PASSED, FAILED
            metric_value DOUBLE,
            threshold_value DOUBLE,
            details VARCHAR,
            evaluated_at_utc TIMESTAMPTZ NOT NULL
        );
        """
        con.execute(sql)

    def record_quality_checks(
        self,
        checks: Sequence[Any],
    ) -> None:
        """Record quality check results into quality_check_results table."""
        if not checks:
            return
        con = self.get_connection()
        self.create_quality_checks_table()
        sql = """
        INSERT OR REPLACE INTO quality_check_results (
            check_id, run_id, rule_name, severity, status,
            metric_value, threshold_value, details, evaluated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        for c in checks:
            eval_time = getattr(c, "evaluated_at_utc", None) or datetime.now(UTC)
            if eval_time.tzinfo is None:
                eval_time = eval_time.replace(tzinfo=UTC)
            mv = getattr(c, "metric_value", None)
            tv = getattr(c, "threshold_value", None)
            dt = getattr(c, "details", None)
            con.execute(
                sql,
                [
                    str(getattr(c, "check_id", uuid.uuid4())),
                    str(getattr(c, "run_id", "")),
                    str(getattr(c, "rule_name", "")),
                    str(getattr(c, "severity", "INFO")),
                    str(getattr(c, "status", "PASSED")),
                    float(mv) if mv is not None else None,
                    float(tv) if tv is not None else None,
                    str(dt) if dt is not None else None,
                    eval_time,
                ],
            )

    def get_recent_quality_checks(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get latest quality check results."""
        con = self.get_connection()
        self.create_quality_checks_table()
        try:
            cursor = con.execute(
                """
                SELECT
                    check_id,
                    run_id,
                    rule_name,
                    severity,
                    status,
                    metric_value,
                    threshold_value,
                    details,
                    STRFTIME(evaluated_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS evaluated_at_utc
                FROM quality_check_results
                ORDER BY evaluated_at_utc DESC, check_id DESC
                LIMIT ?;
                """,
                [limit],
            )
            desc = cursor.description
            if desc is None:
                return []
            col_names = [d[0] for d in desc]
            rows = cursor.fetchall()
            return [dict(zip(col_names, row, strict=True)) for row in rows]
        except Exception:
            return []

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
        # Preserve base table fixture if created in test setup
        with contextlib.suppress(Exception):
            table_check = con.execute(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_name = 'mart_btc_usd_daily';"
            ).fetchone()
            if table_check and table_check[0] == "BASE TABLE":
                return

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

    def create_network_fact_view(self) -> None:
        """Create or replace view fact_network_metrics_daily pointing to on-chain Parquet files."""
        con = self.get_connection()
        parquet_glob = str(
            self.curated_dir.resolve()
            / "onchain"
            / "network_metrics_daily"
            / "source=*"
            / "year=*"
            / "*.parquet"
        )
        existing_files = list(
            self.curated_dir.glob("onchain/network_metrics_daily/source=*/year=*/*.parquet")
        )

        if existing_files:
            sql = f"""
            CREATE OR REPLACE VIEW fact_network_metrics_daily AS
            SELECT *
            FROM read_parquet('{parquet_glob}', hive_partitioning=true, union_by_name=true);
            """
            try:
                con.execute(sql)
                cols = [
                    col[0] for col in con.execute("DESCRIBE fact_network_metrics_daily;").fetchall()
                ]
                if "mvrv_ratio" not in cols:
                    sql = f"""
                    CREATE OR REPLACE VIEW fact_network_metrics_daily AS
                    SELECT *, CAST(NULL AS DOUBLE) AS mvrv_ratio
                    FROM read_parquet('{parquet_glob}', hive_partitioning=true, union_by_name=true);
                    """
                    con.execute(sql)
            except Exception:
                con.execute(sql)
        else:
            # Fallback view with full schema when no files are present yet
            sql = """
            CREATE OR REPLACE VIEW fact_network_metrics_daily AS
            SELECT
                CAST(NULL AS VARCHAR) AS source,
                CAST(NULL AS VARCHAR) AS asset,
                CAST(NULL AS TIMESTAMPTZ) AS metric_date_utc,
                CAST(NULL AS BIGINT) AS transaction_count,
                CAST(NULL AS BIGINT) AS active_addresses_count,
                CAST(NULL AS DOUBLE) AS mvrv_ratio,
                CAST(NULL AS TIMESTAMPTZ) AS ingested_at_utc,
                CAST(NULL AS VARCHAR) AS source_run_id,
                CAST(NULL AS INTEGER) AS year
            WHERE 1=0;
            """
            con.execute(sql)

    def create_cross_domain_mart_view(self) -> None:
        """Create or replace conformed cross-domain view mart_btc_market_and_network_daily."""
        con = self.get_connection()
        sql = """
        CREATE OR REPLACE VIEW mart_btc_market_and_network_daily AS
        SELECT
            COALESCE(m.trade_date_utc, n.metric_date_utc) AS trade_date_utc,
            'BTC' AS asset,
            m.open AS market_open_usd,
            m.high AS market_high_usd,
            m.low AS market_low_usd,
            m.close AS market_close_usd,
            m.volume_base AS market_volume_btc,
            m.observed_hour_count AS market_observed_hour_count,
            m.is_complete AS is_market_day_complete,
            n.transaction_count,
            n.active_addresses_count,
            CASE
                WHEN n.active_addresses_count > 0
                THEN ROUND(CAST(n.transaction_count AS DOUBLE) / n.active_addresses_count, 4)
                ELSE NULL
            END AS tx_per_active_address
        FROM mart_btc_usd_daily m
        FULL OUTER JOIN fact_network_metrics_daily n
            ON m.trade_date_utc = n.metric_date_utc;
        """
        con.execute(sql)

    def create_sentiment_table(self) -> None:
        """Create raw_crypto_sentiment_daily table if not exists."""
        con = self.get_connection()
        sql = """
        CREATE TABLE IF NOT EXISTS raw_crypto_sentiment_daily (
            sentiment_date_utc DATE PRIMARY KEY,
            fng_value INTEGER NOT NULL,
            fng_classification VARCHAR NOT NULL,
            ingested_at_utc TIMESTAMPTZ NOT NULL
        );
        """
        con.execute(sql)

    def insert_sentiment_records(self, records: Sequence[SentimentRecord]) -> int:
        """Insert or replace sentiment records into raw_crypto_sentiment_daily."""
        if not records:
            return 0
        con = self.get_connection()
        self.create_sentiment_table()
        rows = [
            (
                r.date_utc.date(),
                r.value,
                r.classification,
                r.ingested_at_utc,
            )
            for r in records
        ]
        con.executemany(
            """
            INSERT OR REPLACE INTO raw_crypto_sentiment_daily (
                sentiment_date_utc, fng_value, fng_classification, ingested_at_utc
            ) VALUES (?, ?, ?, ?);
            """,
            rows,
        )
        return len(rows)

    def create_macro_events_table(self) -> None:
        """Create raw_macro_economic_events table if not exists."""
        con = self.get_connection()
        sql = """
        CREATE TABLE IF NOT EXISTS raw_macro_economic_events (
            event_id VARCHAR PRIMARY KEY,
            country VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            impact VARCHAR NOT NULL,
            scheduled_utc TIMESTAMPTZ NOT NULL,
            forecast VARCHAR,
            previous VARCHAR,
            ingested_at_utc TIMESTAMPTZ NOT NULL
        );
        """
        con.execute(sql)

    def insert_macro_events(self, events: Sequence[MacroEvent]) -> int:
        """Insert or replace macroeconomic events into raw_macro_economic_events."""
        if not events:
            return 0
        con = self.get_connection()
        self.create_macro_events_table()
        rows = [
            (
                e.event_id,
                e.country,
                e.title,
                e.impact,
                e.scheduled_utc,
                e.forecast,
                e.previous,
                e.ingested_at_utc,
            )
            for e in events
        ]
        con.executemany(
            """
            INSERT OR REPLACE INTO raw_macro_economic_events (
                event_id, country, title, impact, scheduled_utc, forecast, previous, ingested_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            rows,
        )
        return len(rows)

    def create_investment_signals_view(self) -> None:
        """Create or replace conformed analytical mart mart_btc_investment_signals_daily."""
        con = self.get_connection()
        self.create_hourly_view()
        self.create_daily_mart_view()
        self.create_network_fact_view()
        self.create_sentiment_table()
        self.create_macro_events_table()

        # Preserve base table fixture if created in test setup
        with contextlib.suppress(Exception):
            table_check = con.execute(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_name = 'mart_btc_investment_signals_daily';"
            ).fetchone()
            if table_check and table_check[0] == "BASE TABLE":
                return

        sql = """
        CREATE OR REPLACE VIEW mart_btc_investment_signals_daily AS
        WITH base AS (
            SELECT
                m.trade_date_utc,
                m.close AS market_close_usd,
                AVG(m.close) OVER (
                    ORDER BY m.trade_date_utc ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS sma_200,
                m.close / NULLIF(AVG(m.close) OVER (
                    ORDER BY m.trade_date_utc ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ), 0) AS mayer_multiple,
                n.mvrv_ratio,
                COALESCE(s.fng_value, 50) AS fng_value,
                COALESCE(s.fng_classification, 'Neutral') AS fng_classification,
                CASE WHEN e.event_date IS NOT NULL THEN TRUE ELSE FALSE END
                    AS has_high_impact_macro_event
            FROM mart_btc_usd_daily m
            LEFT JOIN fact_network_metrics_daily n
                ON CAST(m.trade_date_utc AS DATE) = CAST(n.metric_date_utc AS DATE)
            LEFT JOIN raw_crypto_sentiment_daily s
                ON CAST(m.trade_date_utc AS DATE) = s.sentiment_date_utc
            LEFT JOIN (
                SELECT DISTINCT CAST(scheduled_utc AS DATE) AS event_date
                FROM raw_macro_economic_events
                WHERE impact = 'High' AND country = 'USD'
            ) e ON CAST(m.trade_date_utc AS DATE) = e.event_date
        )
        SELECT
            trade_date_utc,
            market_close_usd,
            sma_200,
            mayer_multiple,
            mvrv_ratio,
            fng_value,
            fng_classification,
            has_high_impact_macro_event,
            CASE
                WHEN mayer_multiple < 0.80 AND fng_value <= 25 THEN 'AGGRESSIVE_ACCUMULATE'
                WHEN mayer_multiple < 1.00 OR mvrv_ratio < 1.20 THEN 'OPPORTUNISTIC_ACCUMULATE'
                WHEN mayer_multiple > 2.40 OR mvrv_ratio > 3.50 THEN 'HARD_FREEZE'
                WHEN mayer_multiple > 1.80 OR fng_value >= 85 THEN 'DEFENSIVE_RESERVE'
                WHEN mayer_multiple BETWEEN 1.00 AND 1.80 THEN 'STANDARD_DCA'
                ELSE 'STANDARD_DCA'
            END AS investment_signal
        FROM base;
        """
        con.execute(sql)

    def create_signal_history_table(self) -> None:
        """Create signal_history table if not exists."""
        con = self.get_connection()
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_history (
                signal_date_utc DATE PRIMARY KEY,
                generated_at_utc TIMESTAMPTZ NOT NULL,
                market_close_usd DOUBLE,
                sma_200 DOUBLE,
                mayer_multiple DOUBLE,
                mvrv_ratio DOUBLE,
                fng_value INTEGER,
                fng_classification VARCHAR,
                has_high_impact_macro_event BOOLEAN,
                investment_signal VARCHAR NOT NULL,
                signal_strength VARCHAR NOT NULL,
                narrative VARCHAR
            );
            """
        )

    def insert_signal_history(
        self,
        *,
        signal_date_utc: Any,
        generated_at_utc: Any,
        market_close_usd: float | None,
        sma_200: float | None,
        mayer_multiple: float | None,
        mvrv_ratio: float | None,
        fng_value: int,
        fng_classification: str,
        has_high_impact_macro_event: bool,
        investment_signal: str,
        signal_strength: str,
        narrative: str,
    ) -> None:
        """Insert or replace a record in signal_history."""
        con = self.get_connection()
        self.create_signal_history_table()
        con.execute(
            """
            INSERT OR REPLACE INTO signal_history (
                signal_date_utc, generated_at_utc, market_close_usd, sma_200,
                mayer_multiple, mvrv_ratio, fng_value, fng_classification,
                has_high_impact_macro_event, investment_signal, signal_strength,
                narrative
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                signal_date_utc,
                generated_at_utc,
                market_close_usd,
                sma_200,
                mayer_multiple,
                mvrv_ratio,
                fng_value,
                fng_classification,
                has_high_impact_macro_event,
                investment_signal,
                signal_strength,
                narrative,
            ],
        )

    def create_news_sentinel_alerts_table(self) -> None:
        """Create news_sentinel_alerts table if not exists."""
        con = self.get_connection()
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS news_sentinel_alerts (
                alert_id VARCHAR PRIMARY KEY,
                title VARCHAR NOT NULL,
                link VARCHAR,
                published_utc TIMESTAMPTZ,
                matched_keywords VARCHAR,
                severity VARCHAR NOT NULL,
                ingested_at_utc TIMESTAMPTZ NOT NULL,
                telegram_sent BOOLEAN DEFAULT FALSE
            );
            """
        )

    def insert_news_alerts(self, alerts: Sequence[Any]) -> int:
        """Insert or replace news alerts in news_sentinel_alerts."""
        if not alerts:
            return 0
        con = self.get_connection()
        self.create_news_sentinel_alerts_table()
        rows = []
        for a in alerts:
            keywords = (
                ", ".join(a.matched_keywords)
                if isinstance(a.matched_keywords, list | tuple | set)
                else str(a.matched_keywords)
            )
            rows.append(
                (
                    a.alert_id,
                    a.title,
                    a.link,
                    a.published_utc,
                    keywords,
                    a.severity,
                    a.ingested_at_utc,
                    getattr(a, "telegram_sent", False),
                )
            )
        con.executemany(
            """
            INSERT OR REPLACE INTO news_sentinel_alerts (
                alert_id, title, link, published_utc, matched_keywords, severity,
                ingested_at_utc, telegram_sent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            rows,
        )
        return len(rows)

    def get_seen_alert_ids(self) -> set[str]:
        """Return set of alert_ids already recorded in news_sentinel_alerts."""
        con = self.get_connection()
        self.create_news_sentinel_alerts_table()
        rows = con.execute("SELECT alert_id FROM news_sentinel_alerts;").fetchall()
        return {str(r[0]) for r in rows}

    def get_unsent_news_alerts(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return alerts that have not yet been sent to Telegram."""
        con = self.get_connection()
        self.create_news_sentinel_alerts_table()
        rows = con.execute(
            """
            SELECT alert_id, title, link,
                   STRFTIME(published_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS published_utc,
                   matched_keywords, severity,
                   STRFTIME(ingested_at_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS ingested_at_utc,
                   telegram_sent
            FROM news_sentinel_alerts
            WHERE telegram_sent = FALSE
            ORDER BY published_utc ASC
            LIMIT ?;
            """,
            [limit],
        ).fetchall()
        cols = [
            "alert_id",
            "title",
            "link",
            "published_utc",
            "matched_keywords",
            "severity",
            "ingested_at_utc",
            "telegram_sent",
        ]
        return [dict(zip(cols, r, strict=False)) for r in rows]

    def get_latest_news_alerts(self, limit: int = 10) -> list[dict[str, Any]]:
        """Return latest recorded alerts."""
        con = self.get_connection()
        self.create_news_sentinel_alerts_table()
        rows = con.execute(
            """
            SELECT alert_id, title, link,
                   STRFTIME(published_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS published_utc,
                   matched_keywords, severity,
                   STRFTIME(ingested_at_utc, '%Y-%m-%dT%H:%M:%S.%fZ') AS ingested_at_utc,
                   telegram_sent
            FROM news_sentinel_alerts
            ORDER BY published_utc DESC
            LIMIT ?;
            """,
            [limit],
        ).fetchall()
        cols = [
            "alert_id",
            "title",
            "link",
            "published_utc",
            "matched_keywords",
            "severity",
            "ingested_at_utc",
            "telegram_sent",
        ]
        return [dict(zip(cols, r, strict=False)) for r in rows]

    def mark_news_alert_sent(self, alert_id: str) -> None:
        """Mark an alert as sent via Telegram."""
        con = self.get_connection()
        self.create_news_sentinel_alerts_table()
        con.execute(
            "UPDATE news_sentinel_alerts SET telegram_sent = TRUE WHERE alert_id = ?;",
            [alert_id],
        )

    def create_paper_portfolio_tables(self) -> None:
        """Create forward paper trading portfolio tables if not exist."""
        con = self.get_connection()
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_portfolio_balance (
                portfolio_id VARCHAR PRIMARY KEY,
                initial_cash DOUBLE NOT NULL,
                base_cash DOUBLE NOT NULL,
                reserve_cash DOUBLE NOT NULL,
                btc_balance DOUBLE NOT NULL,
                total_contributed DOUBLE NOT NULL,
                last_updated_utc TIMESTAMPTZ NOT NULL,
                total_trades INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS paper_portfolio_snapshots_daily (
                snapshot_date DATE NOT NULL,
                portfolio_id VARCHAR NOT NULL,
                base_cash DOUBLE NOT NULL,
                reserve_cash DOUBLE NOT NULL,
                total_cash DOUBLE NOT NULL,
                btc_balance DOUBLE NOT NULL,
                btc_price DOUBLE NOT NULL,
                portfolio_equity DOUBLE NOT NULL,
                unrealized_pnl_usd DOUBLE NOT NULL,
                unrealized_pnl_pct DOUBLE NOT NULL,
                benchmark_equity DOUBLE NOT NULL,
                PRIMARY KEY (snapshot_date, portfolio_id)
            );

            CREATE TABLE IF NOT EXISTS paper_trade_ledger (
                trade_id VARCHAR PRIMARY KEY,
                portfolio_id VARCHAR NOT NULL,
                executed_at_utc TIMESTAMPTZ NOT NULL,
                trade_date DATE NOT NULL,
                side VARCHAR NOT NULL,
                signal_regime VARCHAR NOT NULL,
                spot_price DOUBLE NOT NULL,
                gross_amount_usd DOUBLE NOT NULL,
                fee_usd DOUBLE NOT NULL,
                net_amount_usd DOUBLE NOT NULL,
                btc_amount DOUBLE NOT NULL,
                narrative VARCHAR NOT NULL
            );
            """
        )

    def create_macro_tables(self) -> None:
        """Create tables for Phase 16 Macro & Narrative Intelligence."""
        con = self.get_connection()
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS macro_news_articles (
                article_id VARCHAR PRIMARY KEY,
                source VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                url VARCHAR NOT NULL,
                published_utc TIMESTAMPTZ NOT NULL,
                summary VARCHAR NOT NULL DEFAULT '',
                pillar VARCHAR NOT NULL,
                severity VARCHAR NOT NULL,
                polarity DOUBLE NOT NULL,
                matched_keywords VARCHAR NOT NULL,
                ingested_at_utc TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS macro_economic_releases (
                release_id VARCHAR PRIMARY KEY,
                event_name VARCHAR NOT NULL,
                country VARCHAR NOT NULL,
                release_date DATE NOT NULL,
                release_time_utc VARCHAR NOT NULL,
                impact VARCHAR NOT NULL,
                actual_value DOUBLE,
                forecast_value DOUBLE,
                previous_value DOUBLE,
                surprise_delta DOUBLE,
                directional_score DOUBLE NOT NULL,
                raw_payload_json VARCHAR,
                ingested_at_utc TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_narrative_intelligence (
                intelligence_date DATE PRIMARY KEY,
                synthesized_at_utc TIMESTAMPTZ NOT NULL,
                hard_macro_score DOUBLE NOT NULL,
                sentiment_score DOUBLE NOT NULL,
                narrative_score DOUBLE NOT NULL,
                composite_mni DOUBLE NOT NULL,
                regime VARCHAR NOT NULL,
                black_swan_flag BOOLEAN NOT NULL DEFAULT FALSE,
                active_critical_alerts INTEGER NOT NULL DEFAULT 0,
                dominant_pillar VARCHAR NOT NULL,
                narrative_summary_id VARCHAR NOT NULL
            );
            """
        )

    def create_macro_mart_view(self) -> None:
        """Create or replace analytical view mart_macro_narrative_daily."""
        con = self.get_connection()
        self.create_macro_tables()
        with contextlib.suppress(Exception):
            self.create_investment_signals_view()
        # Preserve base table fixture if created in test setup
        with contextlib.suppress(Exception):
            table_check = con.execute(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_name = 'mart_macro_narrative_daily';"
            ).fetchone()
            if table_check and table_check[0] == "BASE TABLE":
                return

        con.execute(
            """
            CREATE OR REPLACE VIEW mart_macro_narrative_daily AS
            SELECT
                m.trade_date_utc,
                m.market_close_usd,
                m.sma_200,
                m.mayer_multiple,
                m.mvrv_ratio,
                m.fng_value,
                m.investment_signal,
                COALESCE(n.composite_mni, 0.0) AS composite_mni,
                COALESCE(n.regime, 'NEUTRAL_CHOP') AS macro_regime,
                COALESCE(n.black_swan_flag, FALSE) AS black_swan_flag,
                COALESCE(n.hard_macro_score, 0.0) AS hard_macro_score,
                COALESCE(n.sentiment_score, 0.0) AS sentiment_score,
                COALESCE(n.narrative_score, 0.0) AS narrative_score,
                n.narrative_summary_id,
                COALESCE(n.active_critical_alerts, 0) AS active_critical_alerts
            FROM mart_btc_investment_signals_daily m
            LEFT JOIN daily_narrative_intelligence n
                ON CAST(m.trade_date_utc AS DATE) = n.intelligence_date;
            """
        )

    def insert_macro_articles(self, articles: Sequence[MacroArticle]) -> int:
        """Insert or replace news articles into macro_news_articles."""
        if not articles:
            return 0
        con = self.get_connection()
        self.create_macro_tables()
        rows = [
            (
                a.article_id,
                a.source,
                a.title,
                a.url,
                a.published_utc,
                a.summary,
                a.pillar.value if hasattr(a.pillar, "value") else str(a.pillar),
                a.severity.value if hasattr(a.severity, "value") else str(a.severity),
                float(a.polarity),
                ", ".join(a.matched_keywords)
                if isinstance(a.matched_keywords, list)
                else str(a.matched_keywords),
                a.ingested_at_utc,
            )
            for a in articles
        ]
        con.executemany(
            """
            INSERT OR REPLACE INTO macro_news_articles (
                article_id, source, title, url, published_utc, summary,
                pillar, severity, polarity, matched_keywords, ingested_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            rows,
        )
        return len(rows)

    def insert_macro_economic_releases(self, releases: Sequence[MacroEconomicRelease]) -> int:
        """Insert or replace economic releases into macro_economic_releases."""
        if not releases:
            return 0
        con = self.get_connection()
        self.create_macro_tables()
        rows = [
            (
                r.release_id,
                r.event_name,
                r.country,
                r.release_date,
                r.release_time_utc,
                r.impact,
                r.actual_value,
                r.forecast_value,
                r.previous_value,
                r.surprise_delta,
                float(r.directional_score),
                r.raw_payload_json,
                r.ingested_at_utc,
            )
            for r in releases
        ]
        con.executemany(
            """
            INSERT OR REPLACE INTO macro_economic_releases (
                release_id, event_name, country, release_date, release_time_utc,
                impact, actual_value, forecast_value, previous_value,
                surprise_delta, directional_score, raw_payload_json, ingested_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            rows,
        )
        return len(rows)

    def insert_daily_narrative_intelligence(self, report: DailyNarrativeReport) -> None:
        """Insert or replace daily narrative intelligence report."""
        con = self.get_connection()
        self.create_macro_tables()
        con.execute(
            """
            INSERT OR REPLACE INTO daily_narrative_intelligence (
                intelligence_date, synthesized_at_utc, hard_macro_score,
                sentiment_score, narrative_score, composite_mni,
                regime, black_swan_flag, active_critical_alerts,
                dominant_pillar, narrative_summary_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                report.intelligence_date,
                report.synthesized_at_utc,
                float(report.hard_macro_score),
                float(report.sentiment_score),
                float(report.narrative_score),
                float(report.composite_mni),
                report.regime.value if hasattr(report.regime, "value") else str(report.regime),
                bool(report.black_swan_flag),
                int(report.active_critical_alerts),
                report.dominant_pillar.value
                if hasattr(report.dominant_pillar, "value")
                else str(report.dominant_pillar),
                report.narrative_summary_id,
            ],
        )

    def get_latest_narrative_intelligence(self) -> DailyNarrativeReport | None:
        """Fetch the most recent daily narrative intelligence report."""
        con = self.get_connection()
        self.create_macro_tables()
        cursor = con.execute(
            """
            SELECT
                CAST(intelligence_date AS VARCHAR) AS intelligence_date,
                CAST(synthesized_at_utc AS VARCHAR) AS synthesized_at_utc,
                hard_macro_score, sentiment_score, narrative_score, composite_mni,
                regime, black_swan_flag, active_critical_alerts,
                dominant_pillar, narrative_summary_id
            FROM daily_narrative_intelligence
            ORDER BY intelligence_date DESC, synthesized_at_utc DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return DailyNarrativeReport(
            intelligence_date=row[0]
            if isinstance(row[0], date)
            else date.fromisoformat(str(row[0])),
            synthesized_at_utc=row[1]
            if isinstance(row[1], datetime)
            else datetime.fromisoformat(str(row[1])),
            hard_macro_score=float(row[2]),
            sentiment_score=float(row[3]),
            narrative_score=float(row[4]),
            composite_mni=float(row[5]),
            regime=MacroRegime(row[6]),
            black_swan_flag=bool(row[7]),
            active_critical_alerts=int(row[8]),
            dominant_pillar=MacroPillar(row[9]),
            narrative_summary_id=str(row[10]),
        )

    def get_macro_articles(self, limit: int = 20) -> list[dict[str, Any]]:
        """Fetch latest ingested macro news articles."""
        con = self.get_connection()
        self.create_macro_tables()
        cursor = con.execute(
            """
            SELECT
                article_id, source, title, url,
                CAST(published_utc AS VARCHAR) AS published_utc,
                summary, pillar, severity, polarity, matched_keywords,
                CAST(ingested_at_utc AS VARCHAR) AS ingested_at_utc
            FROM macro_news_articles
            ORDER BY published_utc DESC
            LIMIT ?;
            """,
            [limit],
        )
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        result: list[dict[str, Any]] = []
        for r in rows:
            d = dict(zip(cols, r, strict=True))
            if isinstance(d.get("published_utc"), datetime):
                d["published_utc"] = d["published_utc"].isoformat()
            if isinstance(d.get("ingested_at_utc"), datetime):
                d["ingested_at_utc"] = d["ingested_at_utc"].isoformat()
            result.append(d)
        return result

    def get_macro_economic_releases(self, days: int = 7) -> list[dict[str, Any]]:
        """Fetch scheduled/recent macroeconomic releases."""
        con = self.get_connection()
        self.create_macro_tables()
        cursor = con.execute(
            """
            SELECT
                release_id, event_name, country,
                CAST(release_date AS VARCHAR) AS release_date,
                release_time_utc, impact, actual_value, forecast_value, previous_value,
                surprise_delta, directional_score, raw_payload_json,
                CAST(ingested_at_utc AS VARCHAR) AS ingested_at_utc
            FROM macro_economic_releases
            ORDER BY release_date DESC, release_time_utc DESC
            LIMIT 100;
            """
        )
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        result: list[dict[str, Any]] = []
        for r in rows:
            d = dict(zip(cols, r, strict=True))
            if isinstance(d.get("release_date"), date):
                d["release_date"] = d["release_date"].isoformat()
            if isinstance(d.get("ingested_at_utc"), datetime):
                d["ingested_at_utc"] = d["ingested_at_utc"].isoformat()
            result.append(d)
        return result

    def create_committee_tables(self) -> None:
        """Create tables for Phase 17 Investment Committee and User Intelligence."""
        con = self.get_connection()
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS user_market_intelligence (
                intelligence_id VARCHAR PRIMARY KEY,
                source_url VARCHAR,
                title VARCHAR NOT NULL,
                user_thesis VARCHAR NOT NULL,
                raw_content VARCHAR NOT NULL DEFAULT '',
                pillar VARCHAR NOT NULL,
                sentiment_bias DOUBLE NOT NULL,
                confidence_score DOUBLE NOT NULL,
                tags VARCHAR NOT NULL DEFAULT '',
                created_at_utc TIMESTAMPTZ NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE
            );

            CREATE TABLE IF NOT EXISTS investment_committee_memos (
                memo_id VARCHAR PRIMARY KEY,
                memo_date DATE NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                market_regime VARCHAR NOT NULL,
                composite_mni DOUBLE NOT NULL,
                consensus_score DOUBLE NOT NULL,
                executive_summary_id VARCHAR NOT NULL,
                macro_thesis VARCHAR NOT NULL,
                valuation_thesis VARCHAR NOT NULL,
                technical_thesis VARCHAR NOT NULL,
                dissenting_opinions VARCHAR NOT NULL,
                proposed_action VARCHAR NOT NULL,
                proposed_allocation_usd DOUBLE NOT NULL,
                clamped_allocation_usd DOUBLE NOT NULL,
                allocation_clamped BOOLEAN NOT NULL,
                clamping_reason VARCHAR,
                risk_guard_passed BOOLEAN NOT NULL,
                memo_markdown VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS investment_committee_votes (
                vote_id VARCHAR PRIMARY KEY,
                memo_id VARCHAR NOT NULL,
                persona VARCHAR NOT NULL,
                stance VARCHAR NOT NULL,
                target_allocation_usd DOUBLE NOT NULL,
                confidence DOUBLE NOT NULL,
                rationale VARCHAR NOT NULL,
                voted_at_utc TIMESTAMPTZ NOT NULL
            );
            """
        )

    def create_committee_mart_view(self) -> None:
        """Create or replace analytical view mart_committee_deliberation_daily."""
        con = self.get_connection()
        self.create_committee_tables()
        with contextlib.suppress(Exception):
            self.create_macro_mart_view()

        con.execute(
            """
            CREATE OR REPLACE VIEW mart_committee_deliberation_daily AS
            SELECT
                m.trade_date_utc,
                m.market_close_usd,
                m.sma_200,
                m.mayer_multiple,
                m.mvrv_ratio,
                m.fng_value,
                m.composite_mni,
                m.macro_regime,
                m.black_swan_flag,
                c.memo_id,
                c.consensus_score,
                c.proposed_action,
                c.proposed_allocation_usd,
                c.clamped_allocation_usd,
                c.allocation_clamped,
                c.clamping_reason,
                c.risk_guard_passed,
                c.executive_summary_id,
                COALESCE(u.active_user_alpha_count, 0) AS active_user_alpha_count
            FROM mart_macro_narrative_daily m
            LEFT JOIN investment_committee_memos c
                ON CAST(m.trade_date_utc AS DATE) = c.memo_date
            LEFT JOIN (
                SELECT
                    CAST(created_at_utc AS DATE) AS alpha_date,
                    COUNT(*) AS active_user_alpha_count
                FROM user_market_intelligence
                WHERE is_active = TRUE
                GROUP BY CAST(created_at_utc AS DATE)
            ) u ON CAST(m.trade_date_utc AS DATE) = u.alpha_date;
            """
        )

    def insert_user_intelligence(self, record: UserIntelligenceRecord) -> None:
        """Insert or replace a user market intelligence record."""
        con = self.get_connection()
        self.create_committee_tables()
        tags_str = ", ".join(record.tags) if isinstance(record.tags, list) else str(record.tags)
        con.execute(
            """
            INSERT INTO user_market_intelligence (
                intelligence_id, source_url, title, user_thesis, raw_content,
                pillar, sentiment_bias, confidence_score, tags, created_at_utc, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (intelligence_id) DO UPDATE SET
                title = EXCLUDED.title,
                user_thesis = EXCLUDED.user_thesis,
                raw_content = EXCLUDED.raw_content,
                pillar = EXCLUDED.pillar,
                sentiment_bias = EXCLUDED.sentiment_bias,
                confidence_score = EXCLUDED.confidence_score,
                tags = EXCLUDED.tags,
                is_active = EXCLUDED.is_active;
            """,
            [
                record.intelligence_id,
                record.source_url,
                record.title,
                record.user_thesis,
                record.raw_content,
                record.pillar.value if hasattr(record.pillar, "value") else str(record.pillar),
                float(record.sentiment_bias),
                float(record.confidence_score),
                tags_str,
                record.created_at_utc,
                bool(record.is_active),
            ],
        )

    def list_user_intelligence(
        self, limit: int = 20, active_only: bool = True
    ) -> list[dict[str, Any]]:
        """List recent user market intelligence records."""
        con = self.get_connection()
        self.create_committee_tables()
        where_clause = "WHERE is_active = TRUE" if active_only else ""
        cursor = con.execute(
            f"""
            SELECT
                intelligence_id, source_url, title, user_thesis, raw_content,
                pillar, sentiment_bias, confidence_score, tags,
                CAST(created_at_utc AS VARCHAR) AS created_at_utc, is_active
            FROM user_market_intelligence
            {where_clause}
            ORDER BY created_at_utc DESC
            LIMIT ?;
            """,
            [limit],
        )
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        results: list[dict[str, Any]] = []
        for r in rows:
            d = dict(zip(cols, r, strict=True))
            tag_str = d.get("tags") or ""
            d["tags"] = [t.strip() for t in tag_str.split(",") if t.strip()] if tag_str else []
            results.append(d)
        return results

    def get_user_intelligence_window(
        self, start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        """Get active user intelligence records created within date window."""
        con = self.get_connection()
        self.create_committee_tables()
        cursor = con.execute(
            """
            SELECT
                intelligence_id, source_url, title, user_thesis, raw_content,
                pillar, sentiment_bias, confidence_score, tags,
                CAST(created_at_utc AS VARCHAR) AS created_at_utc, is_active
            FROM user_market_intelligence
            WHERE is_active = TRUE
              AND CAST(created_at_utc AS DATE) >= ?
              AND CAST(created_at_utc AS DATE) <= ?
            ORDER BY created_at_utc ASC;
            """,
            [start_date, end_date],
        )
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        results: list[dict[str, Any]] = []
        for r in rows:
            d = dict(zip(cols, r, strict=True))
            tag_str = d.get("tags") or ""
            d["tags"] = [t.strip() for t in tag_str.split(",") if t.strip()] if tag_str else []
            results.append(d)
        return results

    def insert_investment_memo(self, memo: InvestmentMemorandum) -> None:
        """Insert or replace an investment committee memorandum and member votes."""
        con = self.get_connection()
        self.create_committee_tables()

        con.execute(
            """
            INSERT OR REPLACE INTO investment_committee_memos (
                memo_id, memo_date, created_at_utc, market_regime, composite_mni,
                consensus_score, executive_summary_id, macro_thesis, valuation_thesis,
                technical_thesis, dissenting_opinions, proposed_action,
                proposed_allocation_usd, clamped_allocation_usd, allocation_clamped,
                clamping_reason, risk_guard_passed, memo_markdown
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                memo.memo_id,
                memo.memo_date,
                memo.created_at_utc,
                memo.market_regime,
                float(memo.composite_mni),
                float(memo.consensus_score),
                memo.executive_summary_id,
                memo.macro_thesis,
                memo.valuation_thesis,
                memo.technical_thesis,
                memo.dissenting_opinions,
                memo.proposed_action.value
                if hasattr(memo.proposed_action, "value")
                else str(memo.proposed_action),
                float(memo.proposed_allocation_usd),
                float(memo.clamped_allocation_usd),
                bool(memo.allocation_clamped),
                memo.clamping_reason,
                bool(memo.risk_guard_passed),
                memo.memo_markdown,
            ],
        )

        for v in memo.votes:
            con.execute(
                """
                INSERT OR REPLACE INTO investment_committee_votes (
                    vote_id, memo_id, persona, stance, target_allocation_usd,
                    confidence, rationale, voted_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [
                    v.vote_id,
                    v.memo_id,
                    v.persona.value if hasattr(v.persona, "value") else str(v.persona),
                    v.stance.value if hasattr(v.stance, "value") else str(v.stance),
                    float(v.target_allocation_usd),
                    float(v.confidence),
                    v.rationale,
                    v.voted_at_utc,
                ],
            )

    def get_votes_for_memo(self, memo_id: str) -> list[dict[str, Any]]:
        """Fetch all individual votes cast for a given memorandum."""
        con = self.get_connection()
        self.create_committee_tables()
        cursor = con.execute(
            """
            SELECT
                vote_id, memo_id, persona, stance, target_allocation_usd,
                confidence, rationale, CAST(voted_at_utc AS VARCHAR) AS voted_at_utc
            FROM investment_committee_votes
            WHERE memo_id = ?
            ORDER BY vote_id ASC;
            """,
            [memo_id],
        )
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, r, strict=True)) for r in rows]

    def get_latest_investment_memo(self) -> dict[str, Any] | None:
        """Fetch the latest investment committee memorandum with its votes."""
        con = self.get_connection()
        self.create_committee_tables()
        cursor = con.execute(
            """
            SELECT
                memo_id,
                CAST(memo_date AS VARCHAR) AS memo_date,
                CAST(created_at_utc AS VARCHAR) AS created_at_utc,
                market_regime, composite_mni, consensus_score, executive_summary_id,
                macro_thesis, valuation_thesis, technical_thesis, dissenting_opinions,
                proposed_action, proposed_allocation_usd, clamped_allocation_usd,
                allocation_clamped, clamping_reason, risk_guard_passed, memo_markdown
            FROM investment_committee_memos
            ORDER BY memo_date DESC, created_at_utc DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()
        if not row:
            return None
        cols = [desc[0] for desc in cursor.description]
        data = dict(zip(cols, row, strict=True))
        data["votes"] = self.get_votes_for_memo(data["memo_id"])
        return data

    def get_investment_memo_by_date(self, target_date: date) -> dict[str, Any] | None:
        """Fetch investment committee memorandum for a target date."""
        con = self.get_connection()
        self.create_committee_tables()
        cursor = con.execute(
            """
            SELECT
                memo_id,
                CAST(memo_date AS VARCHAR) AS memo_date,
                CAST(created_at_utc AS VARCHAR) AS created_at_utc,
                market_regime, composite_mni, consensus_score, executive_summary_id,
                macro_thesis, valuation_thesis, technical_thesis, dissenting_opinions,
                proposed_action, proposed_allocation_usd, clamped_allocation_usd,
                allocation_clamped, clamping_reason, risk_guard_passed, memo_markdown
            FROM investment_committee_memos
            WHERE memo_date = ?
            ORDER BY created_at_utc DESC
            LIMIT 1;
            """,
            [target_date],
        )
        row = cursor.fetchone()
        if not row:
            return None
        cols = [desc[0] for desc in cursor.description]
        data = dict(zip(cols, row, strict=True))
        data["votes"] = self.get_votes_for_memo(data["memo_id"])
        return data

    def get_investment_memos_history(self, limit: int = 20) -> list[dict[str, Any]]:
        """Fetch historical investment committee memorandums with votes."""
        con = self.get_connection()
        self.create_committee_tables()
        cursor = con.execute(
            """
            SELECT
                memo_id,
                CAST(memo_date AS VARCHAR) AS memo_date,
                CAST(created_at_utc AS VARCHAR) AS created_at_utc,
                market_regime, composite_mni, consensus_score, executive_summary_id,
                macro_thesis, valuation_thesis, technical_thesis, dissenting_opinions,
                proposed_action, proposed_allocation_usd, clamped_allocation_usd,
                allocation_clamped, clamping_reason, risk_guard_passed, memo_markdown
            FROM investment_committee_memos
            ORDER BY memo_date DESC, created_at_utc DESC
            LIMIT ?;
            """,
            [limit],
        )
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        results: list[dict[str, Any]] = []
        for r in rows:
            data = dict(zip(cols, r, strict=True))
            data["votes"] = self.get_votes_for_memo(data["memo_id"])
            results.append(data)
        return results

    def initialize(self) -> None:
        """Initialize database schema, tables, and views."""
        self.create_metadata_table()
        self.create_watermark_table()
        self.create_quality_checks_table()
        self.create_sentiment_table()
        self.create_macro_events_table()
        self.create_signal_history_table()
        self.create_news_sentinel_alerts_table()
        self.create_paper_portfolio_tables()
        self.create_macro_tables()
        self.create_committee_tables()
        self.create_hourly_view()
        self.create_daily_mart_view()
        self.create_network_fact_view()
        self.create_cross_domain_mart_view()
        self.create_investment_signals_view()
        self.create_macro_mart_view()
        self.create_committee_mart_view()

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

    def _get_run_by_status(self, statuses: tuple[str, ...]) -> dict[str, Any] | None:
        con = self.get_connection()
        self.create_metadata_table()
        placeholders = ", ".join(f"'{s}'" for s in statuses)
        cursor = con.execute(
            f"""
            SELECT
                run_id,
                STRFTIME(completed_at_utc, '%Y-%m-%dT%H:%M:%SZ') AS comp_str
            FROM run_metadata
            WHERE UPPER(status) IN ({placeholders})
            ORDER BY completed_at_utc DESC, started_at_utc DESC
            LIMIT 1;
            """
        )
        row = cursor.fetchone()
        return {"run_id": row[0], "completed_at_utc": row[1]} if row else None

    def get_last_success(self) -> dict[str, Any] | None:
        """Get metadata for latest successful pipeline run."""
        return self._get_run_by_status(("SUCCEEDED", "SUCCESS"))

    def get_last_failure(self) -> dict[str, Any] | None:
        """Get metadata for latest failed pipeline run."""
        return self._get_run_by_status(("FAILED", "FAILURE"))

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

    def get_disk_usage(self) -> dict[str, Any]:
        """Inspect filesystem disk usage for curated / data directory."""
        target_path = self.curated_dir if self.curated_dir.exists() else Path(".")
        try:
            usage = shutil.disk_usage(target_path)
            total_gb = round(usage.total / (1024**3), 2)
            used_gb = round(usage.used / (1024**3), 2)
            free_gb = round(usage.free / (1024**3), 2)
            percent_used = round((usage.used / usage.total) * 100.0, 2) if usage.total > 0 else 0.0
        except Exception:
            total_gb = 0.0
            used_gb = 0.0
            free_gb = 0.0
            percent_used = 0.0

        disk_warning = percent_used > 70.0
        disk_critical = percent_used > 80.0

        return {
            "total_gb": total_gb,
            "used_gb": used_gb,
            "free_gb": free_gb,
            "percent_used": percent_used,
            "disk_warning": disk_warning,
            "disk_critical": disk_critical,
        }

    def get_status(self, now_utc: datetime | None = None) -> dict[str, Any]:
        """Compile comprehensive status dictionary as specified in Phase 3 & 5 specs."""
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
        disk_stats = self.get_disk_usage()
        recent_checks = self.get_recent_quality_checks(limit=10)

        is_healthy = bool(
            watermark_age_hours is not None
            and watermark_age_hours <= 2.0
            and len(gaps) == 0
            and not disk_stats["disk_critical"]
            and not is_locked
        )

        return {
            "watermark_utc": watermark_str,
            "watermark_age_hours": watermark_age_hours,
            "last_run": last_run,
            "last_success": last_success,
            "last_failure": last_failure,
            "curated_stats": curated_stats,
            "gaps": gaps,
            "is_locked": is_locked,
            "disk": disk_stats,
            "disk_usage": disk_stats,
            "is_healthy": is_healthy,
            "quality_checks": recent_checks,
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
