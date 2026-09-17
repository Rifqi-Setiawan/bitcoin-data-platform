-- Parameterized Cross-Domain Market and Network Analysis Query
-- Joins daily BTC-USD market dynamics with on-chain network activity on trade_date_utc.
-- Filters for complete market trading days and accepts :start_date and :end_date parameters.

SELECT
    trade_date_utc,
    asset,
    market_open_usd,
    market_high_usd,
    market_low_usd,
    market_close_usd,
    market_volume_btc,
    market_observed_hour_count,
    is_market_day_complete,
    transaction_count,
    active_addresses_count,
    tx_per_active_address,
    -- Daily market return percentage
    ROUND(CAST((market_close_usd - market_open_usd) / NULLIF(market_open_usd, 0) * 100 AS DOUBLE), 4) AS daily_return_pct,
    -- Daily high-low spread percentage
    ROUND(CAST((market_high_usd - market_low_usd) / NULLIF(market_low_usd, 0) * 100 AS DOUBLE), 4) AS high_low_spread_pct
FROM mart_btc_market_and_network_daily
WHERE is_market_day_complete = TRUE
  AND trade_date_utc >= CAST(:start_date AS TIMESTAMPTZ)
  AND trade_date_utc <= CAST(:end_date AS TIMESTAMPTZ)
ORDER BY trade_date_utc ASC;
