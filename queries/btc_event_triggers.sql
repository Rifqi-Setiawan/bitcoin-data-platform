-- Phase 18 Event Triggers Query
-- Selects canonical pacing trigger flags and peak-to-trough drawdowns for BTC-USD.

SELECT
    trade_date_utc,
    product_id,
    close,
    return_24h,
    rolling_peak_7d,
    rolling_peak_30d,
    drawdown_7d,
    drawdown_30d,
    mvrv_ratio,
    mayer_multiple,
    fng_value,
    is_weekly_cadence_day,
    is_drawdown_event,
    is_regime_capitulation,
    is_regime_froth,
    observed_at_utc
FROM mart_btc_event_triggers
ORDER BY trade_date_utc ASC;
