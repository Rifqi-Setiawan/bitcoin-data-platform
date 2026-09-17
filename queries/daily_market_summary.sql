-- Daily Market Summary Query
-- Analyzes daily BTC-USD price action, inter-day return, and intra-day high-low volatility spread.

SELECT
    trade_date_utc,
    product_id,
    open,
    high,
    low,
    close,
    volume_base,
    observed_hour_count,
    is_complete,
    -- Inter-day return percentage: (close - open) / open * 100
    ROUND(CAST((close - open) / NULLIF(open, 0) * 100 AS DOUBLE), 4) AS daily_return_pct,
    -- Intra-day High-Low volatility spread: (high - low) / low * 100
    ROUND(CAST((high - low) / NULLIF(low, 0) * 100 AS DOUBLE), 4) AS high_low_spread_pct
FROM mart_btc_usd_daily
WHERE is_complete = TRUE
ORDER BY trade_date_utc ASC;
