"""Tests for normalizer and deduplicator."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from bitcoin_data_platform.quality.checks import QualityCheckError
from bitcoin_data_platform.transforms.normalizer import (
    NormalizedCandle,
    normalize_candle,
    normalize_envelopes,
)
from bitcoin_data_platform.transforms.raw_reader import RawEnvelope


def _make_envelope(
    run_id: str = "run-001",
    start_utc: datetime = datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
    end_utc: datetime = datetime(2026, 1, 1, 5, 0, tzinfo=UTC),
    retrieved_at_utc: datetime = datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
    payload: list | None = None,
) -> RawEnvelope:
    if payload is None:
        payload = [
            [1767225600, 95000.5, 96500.0, 95200.0, 96000.25, 123.45],
        ]
    return RawEnvelope(
        schema_version=1,
        run_id=run_id,
        source="coinbase_exchange",
        endpoint="product_candles",
        product_id="BTC-USD",
        granularity_seconds=3600,
        start_utc=start_utc,
        end_utc=end_utc,
        retrieved_at_utc=retrieved_at_utc,
        http_status=200,
        payload_sha256="dummy_sha",
        candle_count=len(payload),
        payload=payload,
    )


def test_normalize_valid_candle_tuple() -> None:
    """6. Normalize valid candle tuple to NormalizedCandle with Decimal fields."""
    raw = [1767225600, 95000.50, 96500.00, 95200.00, 96000.25, 123.456789]
    ingested = datetime(2026, 1, 1, 6, 0, tzinfo=UTC)
    now = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    candle = normalize_candle(
        raw,
        source="coinbase_exchange",
        product_id="BTC-USD",
        granularity_seconds=3600,
        ingested_at_utc=ingested,
        source_run_id="run-1",
        now_utc=now,
    )

    assert isinstance(candle, NormalizedCandle)
    assert candle.candle_start_utc == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert candle.low == Decimal("95000.50")
    assert candle.high == Decimal("96500.00")
    assert candle.open == Decimal("95200.00")
    assert candle.close == Decimal("96000.25")
    assert candle.volume_base == Decimal("123.456789")
    assert candle.source == "coinbase_exchange"
    assert candle.product_id == "BTC-USD"
    assert candle.granularity_seconds == 3600
    assert candle.ingested_at_utc == ingested
    assert candle.source_run_id == "run-1"


def test_reject_candle_with_wrong_tuple_length() -> None:
    """7. Reject candle with wrong tuple length."""
    env_short = _make_envelope(payload=[[1767225600, 95000, 96000, 95200, 96000]])
    with pytest.raises(QualityCheckError) as exc:
        normalize_envelopes([env_short])
    assert "exactly 6 elements" in str(exc.value)

    env_long = _make_envelope(payload=[[1767225600, 95000, 96000, 95200, 96000, 10, 999]])
    with pytest.raises(QualityCheckError) as exc:
        normalize_envelopes([env_long])
    assert "exactly 6 elements" in str(exc.value)


def test_reject_negative_price() -> None:
    """8. Reject negative price."""
    env = _make_envelope(payload=[[1767225600, -100, 96000, 95200, 96000, 10]])
    with pytest.raises(QualityCheckError) as exc:
        normalize_envelopes([env])
    assert "low price must be positive" in str(exc.value)


def test_reject_high_less_than_low() -> None:
    """9. Reject high < low."""
    env = _make_envelope(payload=[[1767225600, 96000, 95000, 95500, 95500, 10]])
    with pytest.raises(QualityCheckError) as exc:
        normalize_envelopes([env])
    assert "high" in str(exc.value) and "low" in str(exc.value)


def test_accept_zero_volume() -> None:
    """10. Accept zero volume."""
    env = _make_envelope(payload=[[1767225600, 95000, 96000, 95200, 96000, 0]])
    candles = normalize_envelopes([env])
    assert len(candles) == 1
    assert candles[0].volume_base == Decimal("0")


def test_reject_negative_volume() -> None:
    """11. Reject negative volume."""
    env = _make_envelope(payload=[[1767225600, 95000, 96000, 95200, 96000, -0.01]])
    with pytest.raises(QualityCheckError) as exc:
        normalize_envelopes([env])
    assert "volume must be non-negative" in str(exc.value)


def test_deduplicate_by_natural_key_latest_run_id_wins() -> None:
    """12. Deduplicate by natural key (latest run_id wins)."""
    # Same candle time, different run_ids and ingested_at_utc
    env1 = _make_envelope(
        run_id="run-1",
        retrieved_at_utc=datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
        payload=[[1767225600, 95000, 96000, 95000, 96000, 10]],
    )
    env2 = _make_envelope(
        run_id="run-2",
        retrieved_at_utc=datetime(2026, 1, 1, 2, 0, tzinfo=UTC),
        payload=[[1767225600, 95100, 96100, 95100, 96100, 20]],
    )

    candles = normalize_envelopes([env1, env2])
    assert len(candles) == 1
    assert candles[0].source_run_id == "run-2"
    assert candles[0].volume_base == Decimal("20")

    # Order inverted in list: run-2 still wins because of latest ingested_at / run_id
    candles_inv = normalize_envelopes([env2, env1])
    assert len(candles_inv) == 1
    assert candles_inv[0].source_run_id == "run-2"
    assert candles_inv[0].volume_base == Decimal("20")


def test_multiple_envelopes_with_overlapping_candles_dedup_correctly() -> None:
    """13. Multiple envelopes with overlapping candles dedup correctly."""
    # Envelope 1 has hours 0 and 1
    env1 = _make_envelope(
        run_id="run-1",
        retrieved_at_utc=datetime(2026, 1, 1, 2, 0, tzinfo=UTC),
        payload=[
            [1767225600, 95000, 96000, 95000, 96000, 10],  # 00:00
            [1767229200, 96000, 97000, 96000, 97000, 15],  # 01:00
        ],
    )
    # Envelope 2 has hours 1 and 2 (hour 1 overlaps)
    env2 = _make_envelope(
        run_id="run-2",
        retrieved_at_utc=datetime(2026, 1, 1, 3, 0, tzinfo=UTC),
        payload=[
            [1767229200, 96000, 97000, 96000, 97000, 18],  # 01:00 updated
            [1767232800, 97000, 98000, 97000, 98000, 25],  # 02:00
        ],
    )

    candles = normalize_envelopes([env1, env2])
    assert len(candles) == 3
    # Check that hour 1 took run-2's value
    hour1_candle = candles[1]
    assert hour1_candle.candle_start_utc == datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    assert hour1_candle.source_run_id == "run-2"
    assert hour1_candle.volume_base == Decimal("18")


def test_output_sorted_by_candle_start_utc() -> None:
    """14. Output sorted by candle_start_utc."""
    env = _make_envelope(
        payload=[
            [1767232800, 97000, 98000, 97000, 98000, 25],  # 02:00
            [1767225600, 95000, 96000, 95000, 96000, 10],  # 00:00
            [1767229200, 96000, 97000, 96000, 97000, 15],  # 01:00
        ]
    )
    candles = normalize_envelopes([env])
    assert len(candles) == 3
    assert [c.candle_start_utc for c in candles] == [
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
        datetime(2026, 1, 1, 2, 0, tzinfo=UTC),
    ]


def test_attach_correct_lineage() -> None:
    """15. Attach correct lineage (source_run_id, ingested_at_utc)."""
    retrieved = datetime(2026, 1, 1, 12, 34, 56, tzinfo=UTC)
    env = _make_envelope(
        run_id="run-lineage-test-999",
        retrieved_at_utc=retrieved,
        payload=[[1767225600, 95000, 96000, 95000, 96000, 10]],
    )
    candles = normalize_envelopes([env])
    assert len(candles) == 1
    assert candles[0].source_run_id == "run-lineage-test-999"
    assert candles[0].ingested_at_utc == retrieved
