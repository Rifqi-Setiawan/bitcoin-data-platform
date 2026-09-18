# Phase 13: Investment Signal Engine — Daily Signal Generation, News Sentinel & Telegram Alerts

**Owner:** Engineering Team  
**Verification:** Automated Test Suite & Peer Review  
**Branch:** `feature/P13-investment-signal-engine`  
**Status:** Draft  

---

## Objective

Build a production-ready Investment Signal Engine that executes daily signal generation from the `mart_btc_investment_signals_daily` analytical view, implements a CoinDesk RSS News Sentinel for black swan detection, and delivers investment signals and emergency alerts via Telegram Bot. This transforms the platform from passive data collection into an active investment advisory system.

## User Story

As an autonomous investment system operator, I need daily investment signals delivered to my phone via Telegram, plus emergency alerts when critical events (exchange hacks, major regulatory actions) are detected via RSS news feeds, so I can make informed investment decisions without manually checking dashboards.

---

## Scope

### In Scope
1. **Signal Generator CLI** (`bitcoin-data generate-signal`) — queries `mart_btc_investment_signals_daily`, produces structured JSON signal report
2. **News Sentinel** (`bitcoin-data news-sentinel`) — fetches CoinDesk RSS feed, regex keyword filter for black swan events, dedup by entry ID
3. **Telegram Alert Dispatcher** (`bitcoin-data send-alert`) — sends formatted investment signal or emergency alert to Telegram chat via Bot API
4. **Signal History Table** — DuckDB `signal_history` table for audit trail
5. **Automated tests** for all new components

### Out of Scope
- Backtest simulation engine (Phase 14)
- Coinbase order execution (Phase 16)
- Dashboard integration of signals (future enhancement)

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  DAILY CRON (systemd timer)              │
│                                                         │
│  1. bitcoin-data fetch (market data)                    │
│  2. bitcoin-data fetch-network (on-chain)               │
│  3. bitcoin-data promote (curate market)                │
│  4. bitcoin-data promote-network (curate on-chain)      │
│  5. bitcoin-data fetch-sentiment (Fear & Greed)         │
│  6. bitcoin-data fetch-macro-calendar (ForexFactory)    │
│  7. bitcoin-data generate-signal (NEW — Phase 13)       │
│  8. bitcoin-data send-alert (NEW — Phase 13)            │
│                                                         │
│  EVERY 30 MIN:                                          │
│  9. bitcoin-data news-sentinel (NEW — Phase 13)         │
└─────────────────────────────────────────────────────────┘
```

---

## Functional Contracts

### 1. Signal Generator (`src/bitcoin_data_platform/signals/generator.py`)

```python
@dataclass(frozen=True)
class InvestmentSignal:
    signal_date_utc: date
    generated_at_utc: datetime
    market_close_usd: float
    sma_200: float | None
    mayer_multiple: float | None
    mvrv_ratio: float | None
    fng_value: int
    fng_classification: str
    has_high_impact_macro_event: bool
    investment_signal: str  # AGGRESSIVE_ACCUMULATE | OPPORTUNISTIC_ACCUMULATE | STANDARD_DCA | DEFENSIVE_RESERVE | HARD_FREEZE
    signal_strength: str   # STRONG | MODERATE | WEAK
    narrative: str         # Human-readable explanation

class SignalGenerator:
    def __init__(self, db_path: Path): ...
    def generate_latest(self) -> InvestmentSignal: ...
    def generate_for_date(self, target_date: date) -> InvestmentSignal | None: ...
```

**Signal Narrative Logic:**
- `AGGRESSIVE_ACCUMULATE` → "🟢 AKUMULASI AGRESIF: Bitcoin di bawah nilai wajar (Mayer {mm:.2f}, MVRV {mvrv:.2f}) + pasar panik (FNG {fng}). Peluang beli terbaik."
- `OPPORTUNISTIC_ACCUMULATE` → "🔵 AKUMULASI OPORTUNISTIK: Bitcoin di zona diskon (Mayer {mm:.2f}). Beli lebih dari normal."
- `STANDARD_DCA` → "⚪ DCA STANDAR: Pasar di zona normal (Mayer {mm:.2f}). Lanjutkan investasi reguler."
- `DEFENSIVE_RESERVE` → "🟡 CADANGAN DEFENSIF: Pasar mulai panas (Mayer {mm:.2f}, FNG {fng}). Kurangi beli, sisihkan ke kas."
- `HARD_FREEZE` → "🔴 STOP TOTAL: Pasar bubble (Mayer {mm:.2f}, MVRV {mvrv:.2f}). Jangan beli, tunggu koreksi."

**Signal Strength:**
- `STRONG`: Multiple indicators align (e.g., Mayer < 0.8 AND MVRV < 1.0 AND FNG < 25)
- `MODERATE`: At least 2 indicators agree
- `WEAK`: Only 1 indicator triggers

### 2. News Sentinel (`src/bitcoin_data_platform/signals/news_sentinel.py`)

```python
@dataclass(frozen=True)
class NewsAlert:
    alert_id: str          # SHA-256(title + published)
    title: str
    link: str
    published_utc: datetime
    matched_keywords: list[str]
    severity: str          # CRITICAL | WARNING
    ingested_at_utc: datetime

class NewsSentinel:
    def __init__(self, *, feed_url: str = "https://www.coindesk.com/arc/outboundfeeds/rss/",
                 max_retries: int = 3, transport=None, sleeper=None, clock=None): ...
    def scan(self) -> list[NewsAlert]: ...
```

**Keyword Patterns (regex, case-insensitive):**
- CRITICAL: `hack(ed|ing)?`, `exploit`, `insolvency`, `insolvent`, `bankrupt`, `SEC (sue|charge|enforcement)`, `ban(ned|ning)?.*crypto`, `emergency`, `collapse`
- WARNING: `ETF (approv|reject|deny)`, `FOMC`, `rate (hike|cut)`, `CPI`, `regulation`, `stablecoin (depeg|crash)`

**Deduplication:** Store seen alert_ids in DuckDB `news_sentinel_alerts` table. Only return new (unseen) alerts.

### 3. Telegram Alert Dispatcher (`src/bitcoin_data_platform/alerts/telegram_dispatcher.py`)

```python
class TelegramDispatcher:
    def __init__(self, *, bot_token: str, chat_id: str,
                 base_url: str = "https://api.telegram.org",
                 transport=None, sleeper=None, clock=None): ...
    def send_signal(self, signal: InvestmentSignal) -> bool: ...
    def send_news_alert(self, alert: NewsAlert) -> bool: ...
    def send_text(self, text: str, parse_mode: str = "Markdown") -> bool: ...
```

**Signal Message Format (Telegram Markdown):**
```
📊 *SINYAL INVESTASI BITCOIN*
📅 {date}

💰 Harga: ${price:,.2f}
📈 Mayer Multiple: {mm:.2f}
🔗 MVRV: {mvrv:.2f}
😱 Fear & Greed: {fng} ({classification})
🗓️ Macro Event: {yes/no}

{signal_emoji} *{signal_name}*
_{narrative}_

Kekuatan: {strength}
```

**News Alert Message Format:**
```
🚨 *ALERT {severity}: {title}*

🔗 {link}
📅 {published}
🔍 Keywords: {matched}

⚠️ _Review sebelum mengambil keputusan investasi._
```

### 4. DuckDB Tables

```sql
-- Signal audit trail
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

-- News sentinel dedup & audit
CREATE TABLE IF NOT EXISTS news_sentinel_alerts (
    alert_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    link VARCHAR,
    published_utc TIMESTAMPTZ,
    matched_keywords VARCHAR,  -- comma-separated
    severity VARCHAR NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    telegram_sent BOOLEAN DEFAULT FALSE
);
```

---

## CLI Subcommands

### `bitcoin-data generate-signal`
```
Args:
  --db-path PATH     DuckDB database path (default: ./data/state/platform.duckdb)
  --date YYYY-MM-DD  Target date (default: latest available)
  --json             Output as JSON (default: human-readable)
  --save             Persist signal to signal_history table
```

### `bitcoin-data news-sentinel`
```
Args:
  --db-path PATH     DuckDB database path (default: ./data/state/platform.duckdb)
  --feed-url URL     RSS feed URL (default: CoinDesk RSS)
  --json             Output as JSON
```

### `bitcoin-data send-alert`
```
Args:
  --db-path PATH     DuckDB database path
  --type signal|news  Alert type to send
  --date YYYY-MM-DD   Signal date (for type=signal)
  --dry-run           Print message without sending
  --bot-token TOKEN   Telegram Bot token (or env TELEGRAM_BOT_TOKEN)
  --chat-id ID        Telegram chat ID (or env TELEGRAM_CHAT_ID)
```

---

## Module Layout

### NEW Files
| File | Purpose |
|------|---------|
| `src/bitcoin_data_platform/signals/__init__.py` | Signals package |
| `src/bitcoin_data_platform/signals/generator.py` | Investment signal generator |
| `src/bitcoin_data_platform/signals/news_sentinel.py` | CoinDesk RSS news scanner |
| `src/bitcoin_data_platform/alerts/__init__.py` | Alerts package |
| `src/bitcoin_data_platform/alerts/telegram_dispatcher.py` | Telegram Bot API dispatcher |
| `tests/test_signal_generator.py` | Signal generator tests |
| `tests/test_news_sentinel.py` | News sentinel tests |
| `tests/test_telegram_dispatcher.py` | Telegram dispatcher tests |

### MODIFIED Files
| File | Change |
|------|--------|
| `src/bitcoin_data_platform/storage/duckdb_manager.py` | Add `signal_history` and `news_sentinel_alerts` tables |
| `src/bitcoin_data_platform/cli.py` | Add `generate-signal`, `news-sentinel`, `send-alert` subcommands |
| `docs/data_dictionary/DATA_DICTIONARY.md` | Document new tables |

---

## Design Constraints

- **No new dependencies** — use stdlib `xml.etree.ElementTree` for RSS parsing, `urllib.request` for Telegram API
- **Telegram Bot token & chat_id from environment variables** — never hardcoded
- **Dependency injection** for all HTTP calls (testability)
- **Idempotent signal generation** — same date always produces same signal (deterministic from DuckDB data)
- **News sentinel dedup** — alert_id based on SHA-256(title + published), stored in DuckDB
- **Dry-run mode** for Telegram — print formatted message without sending
- **All tests use mocks** — no real API calls, no real Telegram sends

---

## Required Tests

### Group A: Signal Generator (test_signal_generator.py)
1. `test_generate_signal_aggressive_accumulate` — low Mayer + low MVRV + extreme fear
2. `test_generate_signal_opportunistic_accumulate` — below-average valuation
3. `test_generate_signal_standard_dca` — normal market conditions
4. `test_generate_signal_defensive_reserve` — overheated market
5. `test_generate_signal_hard_freeze` — bubble territory
6. `test_signal_strength_strong` — multiple indicators aligned
7. `test_signal_strength_weak` — single indicator trigger
8. `test_signal_narrative_contains_metrics` — narrative includes actual values
9. `test_signal_persists_to_history` — signal saved to signal_history table
10. `test_signal_idempotent` — same date produces same signal

### Group B: News Sentinel (test_news_sentinel.py)
11. `test_scan_detects_critical_keyword` — "hack" in title triggers CRITICAL
12. `test_scan_detects_warning_keyword` — "ETF approved" triggers WARNING
13. `test_scan_dedup_skips_seen_alerts` — already-ingested alert_ids filtered out
14. `test_scan_handles_empty_feed` — no items returns empty list
15. `test_scan_handles_malformed_rss` — graceful degradation
16. `test_keyword_regex_case_insensitive` — "HACKED" matches "hack"
17. `test_multiple_keywords_matched` — alert lists all matched keywords
18. `test_retries_on_transient_error` — network retry

### Group C: Telegram Dispatcher (test_telegram_dispatcher.py)
19. `test_send_signal_formats_correctly` — message contains all KPI fields
20. `test_send_news_alert_formats_correctly` — message contains severity + keywords
21. `test_send_dry_run_does_not_call_api` — dry run prints only
22. `test_send_retries_on_transient_error` — retry on 500/503
23. `test_send_fails_gracefully_on_bad_token` — returns False, no crash
24. `test_send_signal_bahasa_indonesia` — narrative in Indonesian

---

## Acceptance Criteria

- **AC-1:** `bitcoin-data generate-signal` produces correct signal classification from DuckDB mart view data
- **AC-2:** `bitcoin-data news-sentinel` scans CoinDesk RSS and detects keyword matches with dedup
- **AC-3:** `bitcoin-data send-alert --type signal --dry-run` prints correctly formatted Telegram message
- **AC-4:** `bitcoin-data send-alert --type news --dry-run` prints correctly formatted alert message
- **AC-5:** Signal history persisted to `signal_history` DuckDB table
- **AC-6:** News alerts persisted to `news_sentinel_alerts` DuckDB table with `telegram_sent` flag
- **AC-7:** All existing 514 tests pass without regression
- **AC-8:** All quality gates pass: `ruff check`, `ruff format --check`, `mypy src`
- **AC-9:** Data Dictionary updated

---

## Implementation Sequence

1. Create `signals/` package → `generator.py` with `SignalGenerator`
2. Create `signals/news_sentinel.py` with `NewsSentinel` (RSS parser + keyword filter)
3. Create `alerts/` package → `telegram_dispatcher.py` with `TelegramDispatcher`
4. Extend `duckdb_manager.py` → `signal_history` and `news_sentinel_alerts` tables
5. Add CLI subcommands `generate-signal`, `news-sentinel`, `send-alert`
6. Write all tests
7. Update Data Dictionary
8. Run full test suite + quality gates
