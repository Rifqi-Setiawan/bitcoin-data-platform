"""Unit tests for Window Planner module."""

import os
import socket
from datetime import UTC, datetime, timedelta

import pytest

from bitcoin_data_platform.window_planner import (
    BackfillPlan,
    InvalidTimezoneError,
    SafetyLimitExceededError,
    plan_backfill,
    plan_windows,
)


def test_plan_windows_rejects_naive_or_non_utc() -> None:
    naive = datetime(2026, 1, 1, 0, 0)
    utc = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    with pytest.raises(InvalidTimezoneError, match="explicit UTC"):
        plan_windows(naive, utc)
    with pytest.raises(InvalidTimezoneError, match="explicit UTC"):
        plan_windows(utc, naive)


def test_ingestion_reexports_backward_compatibility() -> None:
    import bitcoin_data_platform.ingestion as ing
    import bitcoin_data_platform.ingestion.window_planner as ing_wp

    assert hasattr(ing, "plan_windows")
    assert hasattr(ing_wp, "plan_windows")
    assert hasattr(ing_wp, "BackfillPlan")


def test_one_hour_range_produces_one_window() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 1
    w = windows[0]
    assert w.index == 0
    assert w.start_utc == start
    assert w.end_utc == end
    assert w.expected_candle_count == 1
    assert w.expected_candles == 1
    assert w.start_iso == "2026-01-01T00:00:00Z"
    assert w.end_iso == "2026-01-01T01:00:00Z"
    assert w.to_dict() == {
        "index": 0,
        "start_utc": "2026-01-01T00:00:00Z",
        "end_utc": "2026-01-01T01:00:00Z",
        "expected_candle_count": 1,
    }


def test_300_hour_range_produces_exactly_one_300_hour_window() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=300)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 1
    w = windows[0]
    assert w.index == 0
    assert w.expected_candle_count == 300
    assert w.start_utc == start
    assert w.end_utc == end


def test_301_hour_range_splits_into_300_and_1() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=301)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 2
    assert windows[0].index == 0
    assert windows[0].expected_candle_count == 300
    assert windows[0].start_utc == start
    assert windows[0].end_utc == start + timedelta(hours=300)

    assert windows[1].index == 1
    assert windows[1].expected_candle_count == 1
    assert windows[1].start_utc == start + timedelta(hours=300)
    assert windows[1].end_utc == end


def test_601_hour_range_splits_into_300_300_and_1() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=601)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) == 3
    counts = [w.expected_candle_count for w in windows]
    assert counts == [300, 300, 1]
    assert sum(counts) == 601
    assert windows[0].start_utc == start
    assert windows[1].start_utc == windows[0].end_utc
    assert windows[2].start_utc == windows[1].end_utc
    assert windows[2].end_utc == end


def test_plan_exact_coverage_no_gaps_or_overlaps() -> None:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2025, 5, 1, 0, 0, tzinfo=UTC)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert windows[0].start_utc == start
    assert windows[-1].end_utc == end

    for i in range(len(windows)):
        assert windows[i].index == i
        assert windows[i].start_utc < windows[i].end_utc
        assert windows[i].expected_candle_count <= 300
        if i > 0:
            assert windows[i].start_utc == windows[i - 1].end_utc

    total_expected = sum(w.expected_candle_count for w in windows)
    total_requested_hours = int((end - start).total_seconds() // 3600)
    assert total_expected == total_requested_hours


@pytest.mark.parametrize(
    "hours",
    [1, 2, 50, 299, 300, 301, 599, 600, 601, 899, 900, 901, 1500, 8760],
)
def test_coinbase_window_limit_parameterized(hours: int) -> None:
    start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=hours)
    windows = plan_windows(start, end, allow_open_candle=True)

    assert len(windows) > 0
    assert windows[0].start_utc == start
    assert windows[-1].end_utc == end

    for i, w in enumerate(windows):
        assert w.index == i
        assert 1 <= w.expected_candle_count <= 300
        if i > 0:
            assert w.start_utc == windows[i - 1].end_utc

    assert sum(w.expected_candle_count for w in windows) == hours


def test_plan_backfill_domain_object() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 26, 1, 0, tzinfo=UTC)  # 601 hours
    plan = plan_backfill(start, end, allow_open_candle=True)

    assert isinstance(plan, BackfillPlan)
    assert plan.schema_version == 1
    assert plan.source == "coinbase_exchange"
    assert plan.product_id == "BTC-USD"
    assert plan.granularity_seconds == 3600
    assert plan.expected_candle_count == 601
    assert plan.window_count == 3
    assert len(plan.windows) == 3

    d = plan.to_dict()
    assert d["schema_version"] == 1
    assert d["source"] == "coinbase_exchange"
    assert d["product_id"] == "BTC-USD"
    assert d["granularity_seconds"] == 3600
    assert d["requested_start_utc"] == "2026-01-01T00:00:00Z"
    assert d["requested_end_utc"] == "2026-01-26T01:00:00Z"
    assert d["expected_candle_count"] == 601
    assert d["window_count"] == 3
    assert len(d["windows"]) == 3
    assert d["windows"][0]["index"] == 0
    assert d["windows"][0]["expected_candle_count"] == 300


def test_planning_performs_no_network_or_filesystem_access(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_connect(*args: object, **kwargs: object) -> None:
        pytest.fail("Network access was attempted during window planning!")

    monkeypatch.setattr(socket.socket, "connect", forbidden_connect)

    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    # Record directory state
    assert not os.path.exists("data")

    plan = plan_backfill(start, end, allow_open_candle=True)
    assert plan.expected_candle_count == 24
    assert not os.path.exists("data")


def test_safety_limit_enforced() -> None:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=100)
    with pytest.raises(SafetyLimitExceededError, match="exceeds safety limit"):
        plan_windows(start, end, max_total_hours=50, allow_open_candle=True)
