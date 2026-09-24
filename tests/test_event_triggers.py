"""Unit and contract tests for Phase 18 rolling drawdowns and event triggers views."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from bitcoin_data_platform.pacing.models import (
    CapitalPool,
    DecisionStatus,
    ExecutionMode,
    PacingState,
    TriggerObservation,
)
from bitcoin_data_platform.sources.sentiment_contract import SentimentRecord
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager
from bitcoin_data_platform.storage.network_parquet_writer import write_network_parquet_partitions
from bitcoin_data_platform.storage.parquet_writer import write_parquet_partitions
from bitcoin_data_platform.transforms.network_normalizer import NormalizedNetworkMetric
from bitcoin_data_platform.transforms.normalizer import NormalizedCandle


def _make_daily_candles(
    start_date: date,
    prices: list[tuple[float, float, float, float]],  # (open, high, low, close)
) -> list[NormalizedCandle]:
    """Generate 24 hourly candles per day matching the given daily OHLC prices."""
    candles: list[NormalizedCandle] = []
    for day_idx, (open_p, high_p, low_p, close_p) in enumerate(prices):
        cur_date = date.fromordinal(start_date.toordinal() + day_idx)
        for h in range(24):
            # Candle 0 has open, Candle 12 has high, Candle 6 has low, Candle 23 has close
            o = open_p if h == 0 else (open_p + close_p) / 2
            c = close_p if h == 23 else (open_p + close_p) / 2
            hi = high_p if h == 12 else max(o, c)
            lo = low_p if h == 6 else min(o, c)
            ts = datetime(cur_date.year, cur_date.month, cur_date.day, h, 0, tzinfo=UTC)
            ingested_at = datetime(cur_date.year, cur_date.month, cur_date.day, 23, 59, tzinfo=UTC)
            candles.append(
                NormalizedCandle(
                    source="coinbase_exchange",
                    product_id="BTC-USD",
                    granularity_seconds=3600,
                    candle_start_utc=ts,
                    open=Decimal(str(round(o, 2))),
                    high=Decimal(str(round(hi, 2))),
                    low=Decimal(str(round(lo, 2))),
                    close=Decimal(str(round(c, 2))),
                    volume_base=Decimal("10.0"),
                    ingested_at_utc=ingested_at,
                    source_run_id=f"run-{day_idx}",
                )
            )
    return candles


def test_mart_btc_usd_daily_drawdown_hand_computed(tmp_path: Path) -> None:
    """AC-9: Exact rolling-window drawdowns on hand-computed fixtures."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    # 4 consecutive days starting on Wednesday 2026-09-16:
    # Day 0: 2026-09-16 (Wed) - Open: 100, High: 100, Low: 95, Close: 98
    #   Peak 7d: 100, DD 7d: 98/100 - 1 = -0.02, Return 24h: None
    # Day 1: 2026-09-17 (Thu) - Open: 98, High: 105, Low: 96, Close: 102
    #   Peak 7d: 105, DD 7d: 102/105 - 1 = -0.028571, Return 24h: 102/98 - 1 = +0.040816
    # Day 2: 2026-09-18 (Fri) - Open: 102, High: 102, Low: 88, Close: 90
    #   Peak 7d: 105, DD 7d: 90/105 - 1 = -0.142857, Return 24h: 90/102 - 1 = -0.117647
    # Day 3: 2026-09-19 (Sat) - Open: 90, High: 91, Low: 85, Close: 86
    #   Peak 7d: 105, DD 7d: 86/105 - 1 = -0.180952, Return 24h: 86/90 - 1 = -0.044444
    start_d = date(2026, 9, 16)
    daily_ohlc = [
        (100.0, 100.0, 95.0, 98.0),
        (98.0, 105.0, 96.0, 102.0),
        (102.0, 102.0, 88.0, 90.0),
        (90.0, 91.0, 85.0, 86.0),
    ]
    candles = _make_daily_candles(start_d, daily_ohlc)
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        rows = manager.execute_query(
            "SELECT trade_date_utc, high, close, rolling_peak_7d, drawdown_7d, return_24h, "
            "sample_count_7d, sample_count_30d "
            "FROM mart_btc_usd_daily ORDER BY trade_date_utc ASC"
        )
        assert len(rows) == 4

        # Day 0
        assert rows[0]["rolling_peak_7d"] == Decimal("100.000000000000000000")
        assert rows[0]["sample_count_7d"] == 1
        assert rows[0]["drawdown_7d"] == pytest.approx(-0.02, abs=1e-4)
        assert rows[0]["return_24h"] is None

        # Day 1
        assert rows[1]["rolling_peak_7d"] == Decimal("105.000000000000000000")
        assert rows[1]["sample_count_7d"] == 2
        assert rows[1]["drawdown_7d"] == pytest.approx(102.0 / 105.0 - 1.0, abs=1e-4)
        assert rows[1]["return_24h"] == pytest.approx(102.0 / 98.0 - 1.0, abs=1e-4)

        # Day 2
        assert rows[2]["rolling_peak_7d"] == Decimal("105.000000000000000000")
        assert rows[2]["sample_count_7d"] == 3
        assert rows[2]["drawdown_7d"] == pytest.approx(90.0 / 105.0 - 1.0, abs=1e-4)
        assert rows[2]["return_24h"] == pytest.approx(90.0 / 102.0 - 1.0, abs=1e-4)

        # Day 3
        assert rows[3]["rolling_peak_7d"] == Decimal("105.000000000000000000")
        assert rows[3]["sample_count_7d"] == 4
        assert rows[3]["drawdown_7d"] == pytest.approx(86.0 / 105.0 - 1.0, abs=1e-4)
        assert rows[3]["return_24h"] == pytest.approx(86.0 / 90.0 - 1.0, abs=1e-4)


def test_drawdown_bounds_strictly_between_minus_one_and_zero(tmp_path: Path) -> None:
    """D-018: Decimal drawdowns must be strictly contained in [-1.0, 0.0]."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    start_d = date(2026, 1, 1)
    daily_ohlc = [
        (50000.0, 50000.0, 48000.0, 50000.0),  # Close == High -> DD = 0.0
        (50000.0, 60000.0, 45000.0, 46000.0),  # DD negative
    ]
    candles = _make_daily_candles(start_d, daily_ohlc)
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        rows = manager.execute_query("SELECT drawdown_7d, drawdown_30d FROM mart_btc_usd_daily")
        assert len(rows) == 2
        for r in rows:
            assert -1.0 <= r["drawdown_7d"] <= 0.0
            assert -1.0 <= r["drawdown_30d"] <= 0.0

        # Day 0 close == high -> 0.0
        assert rows[0]["drawdown_7d"] == 0.0


def test_no_future_lookahead_in_rolling_windows(tmp_path: Path) -> None:
    """AC-9: Inclusive trailing windows with zero future row lookahead."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    # Start with 3 days
    start_d = date(2026, 2, 1)
    ohlc_base = [
        (100.0, 100.0, 90.0, 95.0),
        (95.0, 105.0, 92.0, 100.0),
        (100.0, 102.0, 95.0, 98.0),
    ]
    candles = _make_daily_candles(start_d, ohlc_base)
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        before_rows = manager.execute_query(
            "SELECT trade_date_utc, rolling_peak_7d, drawdown_7d "
            "FROM mart_btc_usd_daily ORDER BY trade_date_utc"
        )

    # Now add day 4 with a gigantic peak of 500.0 in the future
    ohlc_with_future = list(ohlc_base) + [(98.0, 500.0, 95.0, 480.0)]
    candles_future = _make_daily_candles(start_d, ohlc_with_future)
    write_parquet_partitions(candles_future, curated_dir=curated_dir)

    with manager:
        manager.initialize()
        after_rows = manager.execute_query(
            "SELECT trade_date_utc, rolling_peak_7d, drawdown_7d "
            "FROM mart_btc_usd_daily ORDER BY trade_date_utc"
        )

        # Days 0, 1, 2 must have identical values before and after future row was added
        for i in range(3):
            assert before_rows[i]["rolling_peak_7d"] == after_rows[i]["rolling_peak_7d"]
            assert before_rows[i]["drawdown_7d"] == after_rows[i]["drawdown_7d"]


def test_return_24h_date_gap_returns_null(tmp_path: Path) -> None:
    """Return_24h must be NULL when preceding day is missing (gap > 1 day)."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    # Day 1: 2026-03-01, Day 2: 2026-03-10 (9-day gap)
    candles = _make_daily_candles(date(2026, 3, 1), [(100.0, 100.0, 90.0, 95.0)])
    candles += _make_daily_candles(date(2026, 3, 10), [(95.0, 105.0, 92.0, 100.0)])
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        rows = manager.execute_query(
            "SELECT trade_date_utc, return_24h FROM mart_btc_usd_daily ORDER BY trade_date_utc"
        )
        assert len(rows) == 2
        # Day 1 has no predecessor -> None
        assert rows[0]["return_24h"] is None
        # Day 2 is 9 days later, NOT 24 hours -> None
        assert rows[1]["return_24h"] is None


def test_event_triggers_weekly_cadence_day_flag(tmp_path: Path) -> None:
    """is_weekly_cadence_day must be True on Sunday UTC and False on all other weekdays."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    # 2026-09-20 is Sunday, 2026-09-21 is Monday, ..., 2026-09-26 is Saturday
    start_d = date(2026, 9, 20)  # Sunday
    daily_ohlc = [(100.0, 105.0, 95.0, 100.0) for _ in range(7)]
    candles = _make_daily_candles(start_d, daily_ohlc)
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        triggers = manager.get_event_triggers(product_id="BTC-USD")
        assert len(triggers) == 7

        # Day 0: 2026-09-20 (Sunday) -> True
        assert triggers[0]["trade_date_utc"] == date(2026, 9, 20)
        assert triggers[0]["is_weekly_cadence_day"] is True

        # Days 1 through 6: Mon through Sat -> False
        for i in range(1, 7):
            assert triggers[i]["is_weekly_cadence_day"] is False


def test_event_triggers_drawdown_event_flag(tmp_path: Path) -> None:
    """is_drawdown_event triggers when return_24h <= -5% OR drawdown_7d <= -12%."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    start_d = date(2026, 9, 14)
    # Day 0: 100 close (baseline)
    # Day 1: 94 close (-6% 24h return -> triggers via return_24h <= -0.05)
    # Day 2: 95 close (+1.06% 24h return, peak 100 -> DD -5% -> False)
    # Day 3: 87 close (peak 100 -> DD -13% <= -12% -> triggers via drawdown_7d)
    daily_ohlc = [
        (100.0, 100.0, 98.0, 100.0),
        (100.0, 100.0, 93.0, 94.0),
        (94.0, 96.0, 93.0, 95.0),
        (95.0, 95.0, 86.0, 87.0),
    ]
    candles = _make_daily_candles(start_d, daily_ohlc)
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()
        triggers = manager.get_event_triggers(product_id="BTC-USD")
        assert len(triggers) == 4

        # Day 0: no event
        assert triggers[0]["is_drawdown_event"] is False

        # Day 1: return_24h = -0.06 <= -0.05 -> True
        assert triggers[1]["return_24h"] == pytest.approx(-0.06, abs=1e-4)
        assert triggers[1]["is_drawdown_event"] is True

        # Day 2: return_24h > 0, drawdown_7d = -0.05 > -0.12 -> False
        assert triggers[2]["is_drawdown_event"] is False

        # Day 3: drawdown_7d = 87/100 - 1 = -0.13 <= -0.12 -> True
        assert triggers[3]["drawdown_7d"] == pytest.approx(-0.13, abs=1e-4)
        assert triggers[3]["is_drawdown_event"] is True


def test_event_triggers_capitulation_crossing_vs_persistent_level(tmp_path: Path) -> None:
    """Crossing predicate distinguishes threshold entry from a persistent low level."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    start_d = date(2026, 9, 14)
    daily_ohlc = [(100.0, 100.0, 95.0, 98.0) for _ in range(5)]
    candles = _make_daily_candles(start_d, daily_ohlc)
    write_parquet_partitions(candles, curated_dir=curated_dir)

    # Write network metrics with MVRV values:
    # Day 0: 1.20 (above 1.0)
    # Day 1: 0.95 (downward crossing below 1.0 -> True)
    # Day 2: 0.90 (persistent level below 1.0 -> False!)
    # Day 3: 1.05 (rebound above 1.0 -> False)
    # Day 4: 0.98 (downward crossing below 1.0 again -> True!)
    mvrv_values = [1.20, 0.95, 0.90, 1.05, 0.98]
    network_metrics = []
    for i, mvrv in enumerate(mvrv_values):
        d = date.fromordinal(start_d.toordinal() + i)
        network_metrics.append(
            NormalizedNetworkMetric(
                source="coin_metrics",
                asset="btc",
                metric_date_utc=datetime(d.year, d.month, d.day, 0, 0, tzinfo=UTC),
                transaction_count=300000,
                active_addresses_count=800000,
                ingested_at_utc=datetime(d.year, d.month, d.day, 2, 0, tzinfo=UTC),
                source_run_id=f"cm-run-{i}",
                mvrv_ratio=Decimal(str(mvrv)),
            )
        )
    write_network_parquet_partitions(network_metrics, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()

        triggers = manager.get_event_triggers(product_id="BTC-USD")
        assert len(triggers) == 5

        # Day 0: mvrv=1.20 -> False (no prior row, above threshold)
        assert triggers[0]["is_regime_capitulation"] is False

        # Day 1: mvrv=0.95 (prev 1.20) -> True! Entry into capitulation
        assert triggers[1]["mvrv_ratio"] == 0.95
        assert triggers[1]["is_regime_capitulation"] is True

        # Day 2: mvrv=0.90 (prev 0.95) -> False! Persistent level, not crossing
        assert triggers[2]["mvrv_ratio"] == 0.90
        assert triggers[2]["is_regime_capitulation"] is False

        # Day 3: mvrv=1.05 -> False (above threshold)
        assert triggers[3]["mvrv_ratio"] == 1.05
        assert triggers[3]["is_regime_capitulation"] is False

        # Day 4: mvrv=0.98 (prev 1.05) -> True! Re-crossed below 1.0
        assert triggers[4]["mvrv_ratio"] == 0.98
        assert triggers[4]["is_regime_capitulation"] is True


def test_event_triggers_froth_predicates(tmp_path: Path) -> None:
    """is_regime_froth triggers on (FNG >= 80 AND Mayer >= 2.0), or upward crossings."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    start_d = date(2026, 9, 14)
    # Day 0: Neutral market
    # Day 1: Overheated market: FNG = 85 and Mayer = 2.05 -> True
    # Day 2: Upward crossing of MVRV 2.5: MVRV goes from 2.4 to 2.6 -> True
    # Day 3: Persistent high MVRV 2.7 -> False for crossing!
    daily_ohlc = [
        (100.0, 100.0, 95.0, 100.0),
        (100.0, 210.0, 100.0, 205.0),
        (205.0, 210.0, 200.0, 205.0),
        (205.0, 210.0, 200.0, 205.0),
    ]
    candles = _make_daily_candles(start_d, daily_ohlc)
    write_parquet_partitions(candles, curated_dir=curated_dir)

    # MVRV for Days 2 & 3: crossing 2.5 on Day 2
    network_metrics = []
    for i, mvrv in [(0, 1.8), (1, 2.0), (2, 2.6), (3, 2.7)]:
        d = date.fromordinal(start_d.toordinal() + i)
        network_metrics.append(
            NormalizedNetworkMetric(
                source="coin_metrics",
                asset="btc",
                metric_date_utc=datetime(d.year, d.month, d.day, 0, 0, tzinfo=UTC),
                transaction_count=300000,
                active_addresses_count=800000,
                ingested_at_utc=datetime(d.year, d.month, d.day, 2, 0, tzinfo=UTC),
                source_run_id=f"cm-run-{i}",
                mvrv_ratio=Decimal(str(mvrv)),
            )
        )
    write_network_parquet_partitions(network_metrics, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()

        # Day 1 sentiment: FNG = 85
        manager.insert_sentiment_records(
            [
                SentimentRecord(
                    date_utc=datetime(2026, 9, 14, 0, 0, tzinfo=UTC),
                    value=50,
                    classification="Neutral",
                    ingested_at_utc=datetime(2026, 9, 14, 1, 0, tzinfo=UTC),
                ),
                SentimentRecord(
                    date_utc=datetime(2026, 9, 15, 0, 0, tzinfo=UTC),
                    value=85,
                    classification="Extreme Greed",
                    ingested_at_utc=datetime(2026, 9, 15, 1, 0, tzinfo=UTC),
                ),
            ]
        )

        triggers = manager.get_event_triggers(product_id="BTC-USD")
        assert len(triggers) == 4

        # Day 0: Neutral -> False
        assert triggers[0]["is_regime_froth"] is False

        # On Day 2: MVRV crossed from 2.0 to 2.6 (> 2.5) -> True!
        assert triggers[2]["is_regime_froth"] is True

        # On Day 3: MVRV is 2.7 (prev 2.6). Both > 2.5, so NOT a new upward crossing!
        assert triggers[3]["is_regime_froth"] is False


def test_idempotent_creation(tmp_path: Path) -> None:
    """Views must support idempotent re-creation without error or data mutation."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    candles = _make_daily_candles(date(2026, 9, 20), [(100.0, 100.0, 95.0, 98.0)])
    write_parquet_partitions(candles, curated_dir=curated_dir)

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        # First creation
        manager.initialize()
        r1 = manager.get_event_triggers()
        assert len(r1) == 1

        # Second creation (idempotency check)
        manager.create_daily_mart_view()
        manager.create_event_triggers_view()
        r2 = manager.get_event_triggers()
        assert len(r2) == 1
        assert r1[0]["trade_date_utc"] == r2[0]["trade_date_utc"]


def test_trigger_observation_domain_contract() -> None:
    """Test TriggerObservation dataclass validation and from_dict factory."""
    now = datetime(2026, 9, 20, 23, 59, tzinfo=UTC)
    raw_dict = {
        "product_id": "BTC-USD",
        "trade_date_utc": date(2026, 9, 20),
        "observed_at_utc": now,
        "close": 90000.0,
        "return_24h": -0.06,
        "rolling_peak_7d": 95000.0,
        "rolling_peak_30d": 98000.0,
        "drawdown_7d": -0.0526,
        "drawdown_30d": -0.0816,
        "mvrv_ratio": 1.15,
        "mayer_multiple": 0.95,
        "fng_value": 35,
        "is_weekly_cadence_day": True,
        "is_drawdown_event": True,
        "is_regime_capitulation": False,
        "is_regime_froth": False,
        "open": 92000.0,
        "high": 93000.0,
        "low": 89000.0,
        "volume_base": 1500.0,
        "sma_200": 85000.0,
        "sample_count_7d": 7,
        "sample_count_30d": 30,
        "observed_hour_count": 24,
        "is_complete": True,
        "source": "coinbase_exchange",
    }

    obs = TriggerObservation.from_dict(raw_dict)
    assert obs.product_id == "BTC-USD"
    assert obs.trade_date_utc == date(2026, 9, 20)
    assert obs.observed_at_utc == now
    assert obs.close == Decimal("90000.0")
    assert obs.return_24h == Decimal("-0.06")
    assert obs.rolling_peak_7d == Decimal("95000.0")
    assert obs.drawdown_7d == pytest.approx(-0.0526, abs=1e-4)
    assert obs.is_weekly_cadence_day is True
    assert obs.is_drawdown_event is True
    assert obs.is_regime_capitulation is False
    assert obs.is_regime_froth is False


def test_trigger_observation_validation_rejections() -> None:
    """TriggerObservation must fail closed on invalid numeric domains."""
    now = datetime(2026, 9, 20, 23, 59, tzinfo=UTC)

    # Empty product_id
    with pytest.raises(ValueError, match="product_id"):
        TriggerObservation(
            product_id="",
            trade_date_utc=date(2026, 9, 20),
            observed_at_utc=now,
            close=Decimal("100"),
            return_24h=None,
            rolling_peak_7d=Decimal("100"),
            rolling_peak_30d=Decimal("100"),
            drawdown_7d=0.0,
            drawdown_30d=0.0,
            mvrv_ratio=None,
            mayer_multiple=None,
            fng_value=None,
            is_weekly_cadence_day=False,
            is_drawdown_event=False,
            is_regime_capitulation=False,
            is_regime_froth=False,
        )

    # Positive drawdown (must be in [-1, 0])
    with pytest.raises(ValueError, match="drawdown_7d"):
        TriggerObservation(
            product_id="BTC-USD",
            trade_date_utc=date(2026, 9, 20),
            observed_at_utc=now,
            close=Decimal("100"),
            return_24h=None,
            rolling_peak_7d=Decimal("100"),
            rolling_peak_30d=Decimal("100"),
            drawdown_7d=0.05,  # Invalid: positive!
            drawdown_30d=0.0,
            mvrv_ratio=None,
            mayer_multiple=None,
            fng_value=None,
            is_weekly_cadence_day=False,
            is_drawdown_event=False,
            is_regime_capitulation=False,
            is_regime_froth=False,
        )

    # Drawdown < -1.0
    with pytest.raises(ValueError, match="drawdown_30d"):
        TriggerObservation(
            product_id="BTC-USD",
            trade_date_utc=date(2026, 9, 20),
            observed_at_utc=now,
            close=Decimal("100"),
            return_24h=None,
            rolling_peak_7d=Decimal("100"),
            rolling_peak_30d=Decimal("100"),
            drawdown_7d=-0.5,
            drawdown_30d=-1.5,  # Invalid: < -1.0!
            mvrv_ratio=None,
            mayer_multiple=None,
            fng_value=None,
            is_weekly_cadence_day=False,
            is_drawdown_event=False,
            is_regime_capitulation=False,
            is_regime_froth=False,
        )

    # Naive timestamp
    with pytest.raises(ValueError, match="timezone-aware"):
        TriggerObservation(
            product_id="BTC-USD",
            trade_date_utc=date(2026, 9, 20),
            observed_at_utc=datetime(2026, 9, 20, 12, 0),  # Naive!
            close=Decimal("100"),
            return_24h=None,
            rolling_peak_7d=Decimal("100"),
            rolling_peak_30d=Decimal("100"),
            drawdown_7d=0.0,
            drawdown_30d=0.0,
            mvrv_ratio=None,
            mayer_multiple=None,
            fng_value=None,
            is_weekly_cadence_day=False,
            is_drawdown_event=False,
            is_regime_capitulation=False,
            is_regime_froth=False,
        )


def test_empty_database_schema_contract(tmp_path: Path) -> None:
    """AC-9: Views must exist and expose correct column schema even when database has 0 rows."""
    db_path = tmp_path / "test.duckdb"
    curated_dir = tmp_path / "curated"

    manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    with manager:
        manager.initialize()

        # mart_btc_usd_daily schema check
        daily_cols = [
            c["column_name"] for c in manager.execute_query("DESCRIBE mart_btc_usd_daily")
        ]
        required_daily = [
            "source",
            "product_id",
            "trade_date_utc",
            "open",
            "high",
            "low",
            "close",
            "volume_base",
            "observed_hour_count",
            "is_complete",
            "observed_at_utc",
            "rolling_peak_7d",
            "rolling_peak_30d",
            "sample_count_7d",
            "sample_count_30d",
            "drawdown_7d",
            "drawdown_30d",
            "return_24h",
        ]
        for col in required_daily:
            assert col in daily_cols, f"Missing column {col} in mart_btc_usd_daily"

        # mart_btc_event_triggers schema check
        trigger_cols = [
            c["column_name"] for c in manager.execute_query("DESCRIBE mart_btc_event_triggers")
        ]
        required_triggers = [
            "product_id",
            "trade_date_utc",
            "observed_at_utc",
            "close",
            "return_24h",
            "rolling_peak_7d",
            "rolling_peak_30d",
            "drawdown_7d",
            "drawdown_30d",
            "mvrv_ratio",
            "mayer_multiple",
            "fng_value",
            "is_weekly_cadence_day",
            "is_drawdown_event",
            "is_regime_capitulation",
            "is_regime_froth",
        ]
        for col in required_triggers:
            assert col in trigger_cols, f"Missing column {col} in mart_btc_event_triggers"

        # Check that empty query returns 0 rows
        assert len(manager.get_event_triggers()) == 0


def test_pacing_domain_enums() -> None:
    """AC-1: Canonical domain enums must exist with required string values."""
    assert PacingState.IDLE_CHOP.value == "IDLE_CHOP"
    assert PacingState.WEEKLY_CORE.value == "WEEKLY_CORE"
    assert PacingState.SNIPER_DEPLOYMENT.value == "SNIPER_DEPLOYMENT"
    assert PacingState.FROTH_FREEZE.value == "FROTH_FREEZE"

    assert DecisionStatus.PROPOSED.value == "PROPOSED"
    assert DecisionStatus.AUTHORIZED.value == "AUTHORIZED"
    assert DecisionStatus.REJECTED.value == "REJECTED"
    assert DecisionStatus.SETTLED.value == "SETTLED"
    assert DecisionStatus.FAILED.value == "FAILED"

    assert CapitalPool.BASE.value == "00_BASE"
    assert CapitalPool.TACTICAL_RESERVE.value == "00_TACTICAL_RESERVE"

    assert ExecutionMode.AUTO.value == "AUTO"
    assert ExecutionMode.MANUAL_FORCE.value == "MANUAL_FORCE"
