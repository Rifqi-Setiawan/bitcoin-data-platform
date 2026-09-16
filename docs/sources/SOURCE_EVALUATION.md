# Initial Source Evaluation

Checked against provider documentation on 2026-09-16. Provider contracts, limits, pricing, and terms can change; review them before implementation and record a decision if the source contract changes materially.

## Selected: Coinbase Exchange candles

| Attribute | Assessment |
|---|---|
| Data | Exchange-specific `BTC-USD` OHLCV candles |
| Provider status | Official Coinbase Exchange market-data API |
| Authentication | Public market data; no authentication required |
| Cost | No API fee documented for the public endpoint; use remains subject to Coinbase terms |
| Rate limit | Exchange public REST endpoints: 10 requests/second/IP, burst up to 15 |
| History | Explicit start/end windows; up to 300 candles/request; available history is retrieved through repeated bounded windows |
| API style | HTTPS REST, JSON array-of-arrays |
| Reliability posture | Official source with documented HTTP behavior; client must still handle timeouts, 429, 5xx, and contract changes |
| Limitations | Provider warns historical rates can be incomplete, publishes no interval with no ticks, discourages frequent historical polling, and may return points before the requested start |

Primary references:

- [Coinbase Exchange API overview](https://docs.cdp.coinbase.com/exchange/introduction/welcome)
- [Get product candles](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles)
- [Exchange REST rate limits](https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits)
- [Exchange REST requests and errors](https://docs.cdp.coinbase.com/exchange/rest-api/requests)

Implementation consequences:

1. Treat `(source, product, granularity, candle_start)` as the natural key.
2. Do not assume response order or strict requested boundaries.
3. Exclude the current open candle from committed data.
4. Use explicit windows no larger than 300 expected points.
5. Throttle well below the public limit and make 429 retryable.
6. Preserve raw responses and record gaps rather than silently fabricating candles.
7. Label outputs `coinbase_exchange`; never market them as a universal Bitcoin price.

## Candidate: Coin Metrics Community network data

| Attribute | Assessment |
|---|---|
| Data | Provider-derived Bitcoin network/asset metrics with definitions and availability catalog |
| Provider status | Coin Metrics is a third-party specialist, not the Bitcoin protocol/project |
| Authentication | Community endpoint requires no API key |
| Cost | Free for non-commercial use under the documented Creative Commons terms; verify public-portfolio/reuse obligations |
| Rate limit | Community plan: 10 requests per 6 seconds per IP and up to 10 parallel REST requests |
| History | Metric-specific min/max ranges are exposed by catalog endpoints; several BTC metrics reach early chain history |
| API style | HTTPS REST API v4, JSON, pagination tokens/URLs |
| Reliability posture | Documented catalog and metric definitions; community performance/coverage is not the paid product |
| Limitations | Coverage varies by metric/frequency; metrics are calculated by a provider and must be interpreted using its definitions |

Primary references:

- [Coin Metrics API conventions and Community access](https://docs.coinmetrics.io/api)
- [Coin Metrics API v4, catalog, pagination, and rate limits](https://docs.coinmetrics.io/api/v4/)
- [Example transaction-count definition](https://gitbook-docs.coinmetrics.io/network-data/network-data-overview/transactions/transactions)

Recommended use: second domain after the first pipeline is operational. Select only a small group of metrics that answer a documented research question.

## Candidate: Kraken OHLCVT

| Attribute | Assessment |
|---|---|
| Data | Exchange-specific market candles/trade aggregates |
| Provider status | Official Kraken exchange data |
| Authentication | Public market-data endpoints do not require a private trading key |
| Cost | Public API/download access; terms apply |
| Rate limit | Endpoint-specific limits apply; confirm the current REST limits before implementation |
| History | REST OHLC is limited; Kraken separately publishes downloadable historical OHLCVT archives |
| API style | REST JSON plus downloadable ZIP/CSV archives |
| Reliability posture | Official exchange source |
| Limitations | Combining archive bootstrap with REST increments creates two source contracts in the first pipeline |

Primary references:

- [Kraken public endpoint examples](https://support.kraken.com/articles/360000919986-public-endpoint-examples-you-can-try-them-directly-in-a-web-browser-)
- [Kraken downloadable historical OHLCVT](https://support.kraken.com/in/articles/360047124832-downloadable-historical-ohlcvt-open-high-low-close-volume-trades-data)

Recommended use: later cross-venue comparison/reconciliation or an alternative if Coinbase access becomes unsuitable.

## Candidate: Alternative.me Fear & Greed

| Attribute | Assessment |
|---|---|
| Data | Daily-ish sentiment index and classification |
| Provider status | Third-party derived index; methodology is not a protocol-level source |
| Authentication | No credential documented for the `/fng/` endpoint |
| Cost | Commercial use permitted with required attribution according to the provider page |
| Rate limit | Not stated on the referenced endpoint page; implement conservative request frequency |
| History | `limit=0` requests all available records |
| API style | REST JSON or CSV |
| Reliability posture | Very simple interface but a single proprietary index/provider |
| Limitations | Opaque composite signal, low data volume, attribution requirements, continuity risk |

Primary reference: [Alternative.me Fear & Greed API](https://alternative.me/crypto/fear-and-greed-index/)

Recommended use: lightweight later enrichment, not the foundation.

## Candidate: FRED / ALFRED macroeconomic data

| Attribute | Assessment |
|---|---|
| Data | US and global macroeconomic time series, revisions, and vintages |
| Provider status | Official Federal Reserve Bank of St. Louis service aggregating many named sources |
| Authentication | Registered API key required |
| Cost | Public API; terms and source-specific copyright/reuse conditions apply |
| Rate limit | Documentation states up to 120 requests/minute before HTTP 429 |
| History | Series-specific; rich date filtering and ALFRED vintages |
| API style | HTTPS REST, XML/JSON and endpoint-specific CSV/XLSX options |
| Reliability posture | Mature official service with explicit errors and revision semantics |
| Limitations | Not Bitcoin-native, credentials required, mixed frequencies and revisions need deliberate modeling |

Primary references:

- [FRED API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html)
- [FRED API keys](https://fred.stlouisfed.org/docs/api/api_key.html)
- [Series observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)
- [Errors and rate limiting](https://fred.stlouisfed.org/docs/api/fred/errors.html)
- [Series vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html)

Recommended use: later macro domain when revisions/vintages can be taught explicitly.

## Candidate: self-derived Bitcoin Core data

Bitcoin Core is the strongest provenance path but not a reasonable first source on the recorded VPS storage. A full archival chain plus indexes and operating headroom exceeds the roughly 61 GiB persistent root filesystem. A pruned node reduces disk but cannot independently answer arbitrary historical queries after old blocks are removed.

Reconsider only after persistent storage expansion, explicit node objectives, backup/reindex planning, and an honest comparison with structured public network-data APIs.
