-- On-chain Network Activity Query
-- Analyzes daily Bitcoin network throughput, active participation, and transaction velocity.

SELECT
    metric_date_utc,
    asset,
    transaction_count,
    active_addresses_count,
    -- Transactions per active address ratio
    CASE
        WHEN active_addresses_count > 0
        THEN ROUND(CAST(transaction_count AS DOUBLE) / active_addresses_count, 4)
        ELSE NULL
    END AS tx_per_active_address,
    -- Day-over-day transaction count delta
    transaction_count - LAG(transaction_count) OVER (PARTITION BY asset ORDER BY metric_date_utc ASC) AS dod_tx_delta,
    -- Day-over-day active address count delta
    active_addresses_count - LAG(active_addresses_count) OVER (PARTITION BY asset ORDER BY metric_date_utc ASC) AS dod_active_addresses_delta
FROM fact_network_metrics_daily
ORDER BY metric_date_utc ASC;
