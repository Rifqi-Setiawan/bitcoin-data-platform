"""Tests for mart_btc_investment_signals_daily view and MVRV parquet schema."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from bitcoin_data_platform.sources.macro_calendar_contract import MacroEvent
from bitcoin_data_platform.sources.sentiment_contract import SentimentRecord
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.network_parquet_writer import (
    NETWORK_PARQUET_SCHEMA,
    read_network_partition_metrics,
    write_network_parquet_partitions,
)
from bitcoin_data_platform.storage.parquet_writer import write_parquet_partitions
from bitcoin_data_platform.transforms.network_normalizer import NormalizedNetworkMetric
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _create_candle(
    dt: datetime,
    hour: int,
    price: Decimal,
) -> NormalizedCandle:
    candle_time = dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    return NormalizedCandle(
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        candle_start_utc=candle_time,
        open=price,
        high=price + Decimal("50.0"),
        low=price - Decimal("50.0"),
        close=price,
        volume_base=Decimal("10.0"),
        ingested_at_utc=candle_time + timedelta(minutes=5),
        source_run_id="run-candle-1",
    )


def _create_day_candles(dt: datetime, close_price: Decimal) -> list[NormalizedCandle]:
    """Generate 24 hourly candles for a given day with specified close price."""
    return [_create_candle(dt, h, close_price) for h in range(24)]


def test_parquet_schema_includes_mvrv_column(tmp_path: Path) -> None:
    """16. Parquet output contains mvrv_ratio column and verifies round-trip."""
    curated_dir = tmp_path / "curated"
    dt = datetime(2026, 9, 18, 0, 0, tzinfo=UTC)

    metric = NormalizedNetworkMetric(
        source="coin_metrics",
        asset="btc",
        metric_date_utc=dt,
        transaction_count=350000,
        active_addresses_count=850000,
        ingested_at_utc=dt,
        source_run_id="run-mvrv-1",
        mvrv_ratio=Decimal("1.854321"),
    )

    results = write_network_parquet_partitions([metric], curated_dir=curated_dir)
    assert len(results) == 1
    target_file = results[0].file_path

    # Verify PyArrow schema
    table = pq.read_table(target_file)
    assert "mvrv_ratio" in table.schema.names
    col_idx = table.schema.get_field_index("mvrv_ratio")
    assert table.schema.field(col_idx).type == NETWORK_PARQUET_SCHEMA.field("mvrv_ratio").type

    # Verify read-back
    read_back = read_network_partition_metrics(target_file)
    assert len(read_back) == 1
    assert read_back[0].mvrv_ratio == Decimal("1.854321")


def test_mart_view_computes_sma_200_and_mayer_multiple(tmp_path: Path) -> None:
    """17 & 18. Verify 200-day rolling SMA and Mayer Multiple calculation in mart view."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    # Create candles for 3 days with prices 100, 200, 300
    start_date = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    all_candles: list[NormalizedCandle] = []
    prices = [Decimal("100.0"), Decimal("200.0"), Decimal("300.0")]
    for idx, p in enumerate(prices):
        day_dt = start_date + timedelta(days=idx)
        all_candles.extend(_create_day_candles(day_dt, p))

    write_parquet_partitions(all_candles, curated_dir=curated_dir)

    db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    db_manager.initialize()

    rows = db_manager.execute_query(
        "SELECT trade_date_utc, market_close_usd, sma_200, mayer_multiple "
        "FROM mart_btc_investment_signals_daily ORDER BY trade_date_utc;"
    )
    assert len(rows) == 3

    # Day 1: close 100, sma 100, mayer 1.0
    assert rows[0]["market_close_usd"] == 100.0
    assert rows[0]["sma_200"] == 100.0
    assert rows[0]["mayer_multiple"] == 1.0

    # Day 2: close 200, sma (100 + 200)/2 = 150, mayer 200 / 150 = 1.3333...
    assert rows[1]["market_close_usd"] == 200.0
    assert rows[1]["sma_200"] == 150.0
    assert round(rows[1]["mayer_multiple"], 4) == round(200.0 / 150.0, 4)

    # Day 3: close 300, sma (100 + 200 + 300)/3 = 200, mayer 300 / 200 = 1.5
    assert rows[2]["market_close_usd"] == 300.0
    assert rows[2]["sma_200"] == 200.0
    assert rows[2]["mayer_multiple"] == 1.5


def test_mart_view_joins_sentiment(tmp_path: Path) -> None:
    """19. Verify sentiment score and classification join on date."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    day = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
    write_parquet_partitions(_create_day_candles(day, Decimal("65000.0")), curated_dir=curated_dir)

    db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    db_manager.initialize()

    sentiment = SentimentRecord(
        date_utc=day,
        value=22,
        classification="Extreme Fear",
        ingested_at_utc=day,
    )
    db_manager.insert_sentiment_records([sentiment])

    rows = db_manager.execute_query(
        "SELECT trade_date_utc, fng_value, fng_classification "
        "FROM mart_btc_investment_signals_daily;"
    )
    assert len(rows) == 1
    assert rows[0]["fng_value"] == 22
    assert rows[0]["fng_classification"] == "Extreme Fear"


def test_mart_view_joins_macro_events(tmp_path: Path) -> None:
    """20. Verify has_high_impact_macro_event flag is true when USD High impact event exists."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    day1 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    day2 = datetime(2026, 5, 2, 0, 0, tzinfo=UTC)
    candles = _create_day_candles(day1, Decimal("70000.0")) + _create_day_candles(
        day2, Decimal("71000.0")
    )
    write_parquet_partitions(candles, curated_dir=curated_dir)

    db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    db_manager.initialize()

    # Event scheduled on day 1
    event = MacroEvent(
        event_id="evt-fomc-1",
        country="USD",
        title="FOMC Statement",
        impact="High",
        scheduled_utc=day1.replace(hour=18, minute=0),
        forecast=None,
        previous=None,
        ingested_at_utc=day1,
    )
    db_manager.insert_macro_events([event])

    rows = db_manager.execute_query(
        "SELECT trade_date_utc, has_high_impact_macro_event "
        "FROM mart_btc_investment_signals_daily ORDER BY trade_date_utc;"
    )
    assert len(rows) == 2
    assert rows[0]["has_high_impact_macro_event"] is True
    assert rows[1]["has_high_impact_macro_event"] is False


def test_investment_signal_classification(tmp_path: Path) -> None:
    """21. Verify investment signal allocation classification bands."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    # Base date
    d0 = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    d1 = datetime(2026, 6, 2, 0, 0, tzinfo=UTC)
    d2 = datetime(2026, 6, 3, 0, 0, tzinfo=UTC)
    d3 = datetime(2026, 6, 4, 0, 0, tzinfo=UTC)
    d4 = datetime(2026, 6, 5, 0, 0, tzinfo=UTC)

    # Set up 5 separate scenario days
    # d0: AGGRESSIVE_ACCUMULATE (mayer < 0.80 and fng <= 25)
    # d1: OPPORTUNISTIC_ACCUMULATE (mayer < 1.00 or mvrv < 1.20)
    # d2: STANDARD_DCA (mayer 1.00..1.80)
    # d3: DEFENSIVE_RESERVE (mayer > 1.80 or fng >= 85)
    # d4: HARD_FREEZE (mayer > 2.40 or mvrv > 3.50)

    # To control mayer_multiple simply: single day in isolated partition or known prices
    candles = (
        _create_day_candles(d0, Decimal("50000.0"))  # close 50k -> mayer 1.0 initially
        + _create_day_candles(d1, Decimal("60000.0"))
        + _create_day_candles(d2, Decimal("70000.0"))
        + _create_day_candles(d3, Decimal("80000.0"))
        + _create_day_candles(d4, Decimal("90000.0"))
    )
    write_parquet_partitions(candles, curated_dir=curated_dir)

    db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    db_manager.initialize()

    # Insert network metrics with specific MVRV ratios
    net_metrics = [
        NormalizedNetworkMetric(
            source="coin_metrics",
            asset="btc",
            metric_date_utc=d0,
            transaction_count=300000,
            active_addresses_count=800000,
            ingested_at_utc=d0,
            source_run_id="r0",
            mvrv_ratio=Decimal("0.75"),
        ),
        NormalizedNetworkMetric(
            source="coin_metrics",
            asset="btc",
            metric_date_utc=d1,
            transaction_count=300000,
            active_addresses_count=800000,
            ingested_at_utc=d1,
            source_run_id="r1",
            mvrv_ratio=Decimal("1.10"),  # mvrv < 1.20 -> OPPORTUNISTIC_ACCUMULATE
        ),
        NormalizedNetworkMetric(
            source="coin_metrics",
            asset="btc",
            metric_date_utc=d2,
            transaction_count=300000,
            active_addresses_count=800000,
            ingested_at_utc=d2,
            source_run_id="r2",
            mvrv_ratio=Decimal("1.50"),  # mayer ~1.1..1.3 -> STANDARD_DCA
        ),
        NormalizedNetworkMetric(
            source="coin_metrics",
            asset="btc",
            metric_date_utc=d3,
            transaction_count=300000,
            active_addresses_count=800000,
            ingested_at_utc=d3,
            source_run_id="r3",
            mvrv_ratio=Decimal("2.00"),  # fng=90 -> DEFENSIVE_RESERVE
        ),
        NormalizedNetworkMetric(
            source="coin_metrics",
            asset="btc",
            metric_date_utc=d4,
            transaction_count=300000,
            active_addresses_count=800000,
            ingested_at_utc=d4,
            source_run_id="r4",
            mvrv_ratio=Decimal("3.80"),  # mvrv > 3.50 -> HARD_FREEZE
        ),
    ]
    write_network_parquet_partitions(net_metrics, curated_dir=curated_dir)
    db_manager.create_network_fact_view()

    # Insert sentiment
    db_manager.insert_sentiment_records(
        [
            SentimentRecord(d0, 20, "Extreme Fear", d0),
            SentimentRecord(d1, 40, "Fear", d1),
            SentimentRecord(d2, 50, "Neutral", d2),
            SentimentRecord(d3, 90, "Extreme Greed", d3),
            SentimentRecord(d4, 70, "Greed", d4),
        ]
    )

    db_manager.create_investment_signals_view()

    rows = db_manager.execute_query(
        "SELECT trade_date_utc, mayer_multiple, mvrv_ratio, fng_value, investment_signal "
        "FROM mart_btc_investment_signals_daily ORDER BY trade_date_utc;"
    )
    assert len(rows) == 5

    # d1 has mvrv 1.10 (< 1.20) -> OPPORTUNISTIC_ACCUMULATE
    assert rows[1]["investment_signal"] == "OPPORTUNISTIC_ACCUMULATE"

    # d2 has mayer in [1.0, 1.8], fng 50, mvrv 1.50 -> STANDARD_DCA
    assert rows[2]["investment_signal"] == "STANDARD_DCA"

    # d3 has fng 90 (>= 85) -> DEFENSIVE_RESERVE
    assert rows[3]["investment_signal"] == "DEFENSIVE_RESERVE"

    # d4 has mvrv 3.80 (> 3.50) -> HARD_FREEZE
    assert rows[4]["investment_signal"] == "HARD_FREEZE"


def test_fallback_when_no_sentiment_data(tmp_path: Path) -> None:
    """22. View defaults to Neutral sentiment and 50 FNG score when sentiment table is empty."""
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    day = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
    write_parquet_partitions(_create_day_candles(day, Decimal("60000.0")), curated_dir=curated_dir)

    db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    db_manager.initialize()

    rows = db_manager.execute_query(
        "SELECT fng_value, fng_classification, has_high_impact_macro_event, investment_signal "
        "FROM mart_btc_investment_signals_daily;"
    )
    assert len(rows) == 1
    assert rows[0]["fng_value"] == 50
    assert rows[0]["fng_classification"] == "Neutral"
    assert rows[0]["has_high_impact_macro_event"] is False
    assert rows[0]["investment_signal"] == "STANDARD_DCA"
