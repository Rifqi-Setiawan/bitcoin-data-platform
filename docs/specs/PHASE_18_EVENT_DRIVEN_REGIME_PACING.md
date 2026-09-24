# Phase 18: Event-Driven Regime Pacing and Asymmetric Drawdown Sniper

**Owner:** Architecture and Engineering Teams  
**Decision:** [D-018](../decisions/D-018_EVENT_DRIVEN_REGIME_PACING.md)  
**Research basis:** `/srv/hermes-control/reports/professor_bitcoin_execution_cadence_and_advanced_strategy_research_20260924.md`  
**Status:** Approved Technical Specification

---

## 1. Objective

Replace the fixed daily purchase loop with a deterministic event-driven execution path that:

1. batches routine accumulation into a weekly `00 Base` order;
2. preserves `00 Tactical Reserve` for drawdown or capitulation events;
3. freezes all buying in froth and hard-safety conditions;
4. prevents duplicate or repeated deployment from scheduler retries and persistent trigger levels;
5. remains subordinate to the existing symbolic pre-trade safety layer; and
6. supports chronological, zero-lookahead backtesting before forward-paper activation.

The research-derived $1,000 sandbox baseline is $400 in `00 Base` and $600 in `00 Tactical Reserve`. These are policy defaults, not hard-coded ledger semantics.

## 2. Scope

### In scope

- DuckDB rolling drawdown and event-trigger analytical contracts.
- Pure deterministic FSM and policy objects.
- Pacing integration into paper and backtest execution paths.
- Dual-pool authorization and settlement invariants.
- Idempotent decision, episode, and ledger persistence.
- Audit reason codes, policy versioning, and input fingerprints.
- Unit, contract, integration, replay, and failure-injection tests.

### Out of scope

- Live-money exchange orders and credentials.
- Intraday tick/order-book execution and resting limit-order management.
- Automatic profit-taking or sell-side rebalancing.
- LLM authority over state transitions or safety limits.
- Kafka, Redis, or another continuously running event broker.
- Promotion to forward paper trading before comparative backtest acceptance.

## 3. Logical Architecture

```text
Coinbase / Coin Metrics / Sentiment / Macro
                    |
                    v
        mart_btc_usd_daily + signal marts
                    |
                    v
         mart_btc_event_triggers
  (facts and flags only; no portfolio mutation)
                    |
                    v
       PacingPolicy + PacingStateMachine
       observation + prior checkpoint
                    |
                    v
        immutable ExecutionDecision
                    |
                    v
       RiskGuard / InvariantSolver clamp
                    |
                    v
        transactional paper ledger
     00 Base | 00 Tactical Reserve | BTC
```

The scheduled daily pipeline may continue to refresh data and evaluate the FSM every day. “Event-driven” means orders are emitted only when a qualifying edge or cadence event exists; it does not require a resident broker or daemon.

## 4. Canonical Domain Model

### 4.1 Enumerations

```text
PacingState = IDLE_CHOP | WEEKLY_CORE | SNIPER_DEPLOYMENT | FROTH_FREEZE
DecisionStatus = PROPOSED | AUTHORIZED | REJECTED | SETTLED | FAILED
CapitalPool = 00_BASE | 00_TACTICAL_RESERVE
ExecutionMode = AUTO | MANUAL_FORCE
```

`MANUAL_FORCE` may request evaluation but MUST NOT bypass freeze, solvency, price, macro, black-swan, pool-isolation, runway, velocity, or idempotency controls.

### 4.2 PacingPolicy

| Field | Type / unit | Initial default | Validation |
|---|---|---:|---|
| `policy_version` | non-empty string | deployment-defined | immutable |
| `effective_from_utc` | timestamp | deployment-defined | required |
| `weekly_weekday_utc` | integer 0-6 | Sunday | one value |
| `weekly_base_usd` | USD | 20.00 | positive |
| `initial_base_pct` | decimal | 0.40 | `[0,1]` |
| `initial_tactical_pct` | decimal | 0.60 | sums to 1 with Base |
| `sniper_return_24h_lte` | decimal return | -0.05 | `[-1,0)` |
| `sniper_drawdown_7d_lte` | decimal return | -0.12 | `[-1,0)` |
| `sniper_request_pct_min/max` | reserve fraction | 0.20 / 0.35 | `0 < min <= max <= 1` |
| `capitulation_mvrv_lt` | ratio | 1.0 | positive |
| `capitulation_mayer_lt` | ratio | 0.8 | positive |
| `froth_fng_gte` | index | 80 | `[0,100]` |
| `froth_mayer_gte` | ratio | 2.0 | positive |
| `preservation_mvrv_gt` | ratio | 2.5 | positive |
| `preservation_mayer_gt` | ratio | 2.2 | positive |
| `minimum_base_runway_weeks` | weeks | 8 | positive integer |
| `low_runway_buy_factor` | decimal | 0.50 | `[0,1]` |
| `max_tactical_48h_pct` | reserve fraction | 0.40 | `(0,1]` |
| `max_observation_age` | duration | deployment-defined | positive |
| re-arm/hysteresis fields | decimal / days | calibration-required | explicit, non-negative |

The 20%-35% sniper request from the research is reconciled with the existing 15% daily `InvariantSolver` limit as follows: policy creates the request, then authorization takes the minimum of every applicable cap. Thus current effective one-day deployment cannot exceed 15% of available Tactical Reserve without a separately reviewed invariant change.

### 4.3 TriggerObservation

One canonical observation MUST exist per `(product_id, trade_date_utc)`:

```text
product_id: str
trade_date_utc: date
observed_at_utc: timestamp
close: finite positive decimal
return_24h: finite decimal
rolling_peak_7d: finite positive decimal
rolling_peak_30d: finite positive decimal
drawdown_7d: finite decimal in [-1, 0]
drawdown_30d: finite decimal in [-1, 0]
mvrv_ratio: optional finite positive decimal
mayer_multiple: optional finite positive decimal
fng_value: optional integer [0, 100]
is_weekly_cadence_day: bool
is_drawdown_event: bool
is_regime_capitulation: bool
is_regime_froth: bool
```

Missing valuation fields are allowed only if policy can evaluate safely without them. Missing data MUST never evaluate as a favorable trigger. Required stale, null, duplicated, or non-finite inputs fail closed.

### 4.4 PacingCheckpoint

```text
product_id
policy_version
last_trade_date_utc
last_state
active_drawdown_episode_id: optional str
sniper_armed: bool
last_weekly_period_key: optional str
last_settled_decision_id: optional str
checkpoint_version: monotonic integer
updated_at_utc
```

Checkpoint writes use optimistic version comparison or an equivalent transaction lock so two evaluators cannot advance the same instrument concurrently.

### 4.5 ExecutionDecision

The persisted decision schema follows ADR D-018 and additionally records:

- `execution_mode`;
- requested and authorized amount by pool;
- all policy thresholds used;
- `input_fingerprint` (SHA-256 of canonical serialized observation, checkpoint version, and policy version);
- `reason_codes` from a stable registry;
- risk-clamping receipt reference;
- settlement ledger transaction ID.

Recommended reason codes include:

```text
NO_EVENT_CHOP
WEEKLY_CADENCE_DUE
DRAWNDOWN_EDGE_24H
DRAWDOWN_EDGE_7D
CAPITULATION_EDGE_MVRV
CAPITULATION_EDGE_MAYER
FROTH_FNG_MAYER
CAPITAL_PRESERVATION_VALUATION
HARD_RISK_FREEZE
STALE_OR_INVALID_INPUT
BASE_RUNWAY_REDUCTION
DAILY_TACTICAL_CAP
TACTICAL_48H_CAP
DUPLICATE_IDEMPOTENCY_KEY
```

Reason code spelling becomes a downstream contract after implementation; tests must freeze the final registry.

## 5. Analytical Data Contract

The data layer extends `mart_btc_usd_daily` using rows available at or before date `t`:

```text
rolling_peak_7d  = max(high) over [t-6, t]
rolling_peak_30d = max(high) over [t-29, t]
drawdown_7d      = close_t / rolling_peak_7d - 1
drawdown_30d     = close_t / rolling_peak_30d - 1
return_24h       = close_t / close_(t-1) - 1
```

Window definitions use ordered UTC dates and include the current row. For incomplete history, values may be calculated from available prior rows but must expose sample counts if consumers require full-window qualification. The event view is created idempotently and emits:

```text
is_weekly_cadence_day
is_drawdown_event
is_regime_capitulation
is_regime_froth
```

Initial predicates:

```text
is_drawdown_event = return_24h <= -0.05 OR drawdown_7d <= -0.12
is_regime_capitulation = downward crossing of MVRV 1.0
                         OR downward crossing of Mayer 0.8
is_regime_froth = (FNG >= 80 AND Mayer >= 2.0)
                  OR upward crossing of MVRV 2.5
                  OR upward crossing of Mayer 2.2
```

Crossing predicates compare only `t` with the latest earlier valid observation. A level that remains beyond a threshold is not a new transition.

## 6. FSM Evaluation Algorithm

For each observation in strictly increasing UTC date order:

1. Validate policy, observation uniqueness, chronology, freshness, and numeric domains.
2. Load the prior checkpoint and unsettled decisions for the same instrument/policy.
3. Derive hard-risk, froth, drawdown-edge, capitulation-edge, and weekly-due predicates.
4. Select exactly one state using precedence:
   `FROTH_FREEZE > SNIPER_DEPLOYMENT > WEEKLY_CORE > IDLE_CHOP`.
5. Build a pool-specific request:
   - freeze/chop: zero from both pools;
   - weekly: Base only;
   - sniper/capitulation: Tactical Reserve only.
6. Apply Base runway, tactical 48-hour velocity, daily tactical, solvency, macro, black-swan, price, and kill-switch controls.
7. Persist decision and checkpoint atomically before or with authorization, according to implementation transaction boundaries.
8. Settle an authorized non-zero decision exactly once in the portfolio ledger.
9. Persist terminal status and the risk receipt. A retry reads and returns the existing terminal decision.

### 6.1 Collision behavior

| Predicates | State | Disposition |
|---|---|---|
| froth + any opportunity | `FROTH_FREEZE` | zero order |
| hard safety + any opportunity | `FROTH_FREEZE` | zero order |
| drawdown/capitulation edge + weekly | `SNIPER_DEPLOYMENT` | tactical only; weekly period remains unspent and is not silently combined |
| drawdown level, episode already latched | `IDLE_CHOP` or weekly if due | no repeated sniper |
| weekly flag, period already settled | `IDLE_CHOP` | duplicate suppressed |

## 7. Dual-Pool Capital Contracts

### 7.1 Initialization

For initial capital `C0` under the default policy:

```text
00 Base = round_currency(C0 * 0.40)
00 Tactical Reserve = C0 - 00 Base
```

Rounding remainder is assigned to Tactical Reserve to guarantee exact conservation.

### 7.2 Base runway

Before a weekly authorization:

```text
runway_weeks = base_cash / weekly_base_usd
requested_base = weekly_base_usd
if runway_weeks < 8:
    requested_base *= 0.50
authorized_base = min(requested_base, base_cash)
```

A zero weekly budget or invalid balance is a contract failure, not an infinite runway.

### 7.3 Tactical authorization

```text
requested_tactical = tactical_cash * configured_sniper_pct
remaining_48h_cap = max(0, 0.40 * window_opening_tactical_cash - tactical_debits_48h)
daily_cap = 0.15 * tactical_cash
authorized_tactical = min(
    requested_tactical,
    tactical_cash,
    remaining_48h_cap,
    daily_cap,
    all_other_risk_caps,
)
```

Window semantics and boundary inclusion (`[now-48h, now]`) must be fixed in tests. Fees are included in the relevant pool debit so a nominal order cannot produce an overdraft after fees.

### 7.4 Conservation and isolation

Every settlement transaction proves:

```text
opening_base + opening_tactical + external_inflow
= closing_base + closing_tactical + gross_buy + fees + explicit_outflow
```

BTC quantity credit is:

```text
btc_credit = gross_buy / execution_price
```

if `gross_buy` excludes fees, or the existing platform fee convention otherwise. One convention must be selected from the current engine and tested end-to-end; implementation must not mix conventions.

No implicit pool fallback is permitted. Transfers use explicit balanced debit/credit entries with a transfer reason and idempotency key.

## 8. Persistence and Idempotency

Minimum durable records:

1. `pacing_checkpoints` — one current checkpoint per product/policy.
2. `pacing_decisions` — immutable decision and authorization facts.
3. `drawdown_episodes` — threshold entry, latch, re-arm, and consumed decision.
4. existing paper ledger entries, extended with `decision_id`, `capital_pool`, and `policy_version` where necessary.

Required unique constraints:

```text
UNIQUE(product_id, policy_version)                         -- checkpoint
UNIQUE(idempotency_key)                                   -- decision
UNIQUE(product_id, policy_version, cadence_period_key)    -- weekly settlement
UNIQUE(product_id, policy_version, episode_id, band_key)  -- sniper settlement
```

A crash after ledger commit but before status update is recovered by looking up the ledger transaction by decision ID and marking the decision settled; it must never create a second trade.

## 9. Interfaces and Module Boundaries

Recommended implementation boundaries (exact filenames may follow repository conventions):

```text
src/bitcoin_data_platform/pacing/models.py
    enums and immutable input/output contracts
src/bitcoin_data_platform/pacing/policy.py
    validated policy and version loading
src/bitcoin_data_platform/pacing/fsm.py
    pure transition and sizing request logic
src/bitcoin_data_platform/pacing/repository.py
    checkpoint, episode, and decision persistence
src/bitcoin_data_platform/paper/engine.py
    AUTO/MANUAL_FORCE orchestration and settlement adapter
src/bitcoin_data_platform/backtest/strategies.py
    EventDrivenRegimeStrategy and weekly benchmark adapter
src/bitcoin_data_platform/storage/duckdb_manager.py
    mart and persistence DDL
```

The pure FSM accepts plain contracts and returns a decision candidate without opening DuckDB, invoking an LLM, reading the clock, or mutating the ledger. Time and repositories are injected at orchestration boundaries.

## 10. Observability and Audit

Each evaluation emits structured fields:

```text
decision_id, idempotency_key, policy_version, product_id,
trade_date_utc, previous_state, next_state, reason_codes,
requested_base_usd, requested_tactical_usd,
authorized_base_usd, authorized_tactical_usd,
checkpoint_version, episode_id, input_fingerprint, status
```

Metrics include state counts, skipped-chop count, freeze count by reason, weekly/sniper settlement count, duplicate suppression count, Base runway, Tactical Reserve utilization, and clamp amount by rule. Logs and UI must distinguish `NO_ACTION` from `FAILED`.

## 11. Failure-Mode and Effects Analysis

| ID | Failure | Effect | Detection | Control | Residual risk |
|---|---|---|---|---|---|
| F-01 | stale/missing market input | wrong-state trade | freshness/schema validation | freeze and audit | missed trade |
| F-02 | duplicate scheduler call | double debit | unique decision key | return existing outcome | delayed status repair |
| F-03 | persistent drawdown level | repeated sniper | active episode latch | edge-trigger + hysteresis | calibration sensitivity |
| F-04 | concurrent evaluators | checkpoint race | version/unique conflict | transaction + retry read | short contention |
| F-05 | crash during settlement | ambiguous trade status | decision-ledger reconciliation | one transaction/idempotent recovery | manual repair if storage corrupt |
| F-06 | policy changed mid-run | irreproducible sizing | policy mismatch | immutable captured version | operational rollout complexity |
| F-07 | froth and drawdown collide | buy at overheat | predicate collision test | freeze precedence | missed reversal |
| F-08 | fees omitted from debit | negative cash | conservation check | fee-inclusive authorization | rounding differences |
| F-09 | LLM proposes override | invariant bypass | authority boundary | proposal-only LLM | misleading narrative |
| F-10 | corrupted threshold units | excessive trade | policy validation | decimal-return contract | configuration error before load |
| F-11 | late/corrected mart history | replay drift | fingerprint mismatch | immutable decision inputs | source correction not auto-applied |
| F-12 | no re-arm hysteresis | trigger chatter | event-rate alert | explicit re-arm policy | missed close events |

## 12. Security and Safety Properties

- No new network listener, credential, or live exchange integration.
- Least authority: analytical layer cannot mutate execution balances.
- Manual forcing cannot bypass hard invariants.
- Invalid configuration fails startup/evaluation closed.
- Decision and ledger records are append-only from the strategy’s perspective.
- Human-readable committee narratives are non-authoritative.

## 13. Verification Plan

### 13.1 Unit tests

- Every state/guard row and every collision in the transition table.
- Policy boundary and invalid-unit cases.
- Base runway exactly below, at, and above eight weeks.
- Tactical daily and rolling-48-hour cap boundaries.
- Episode entry, persistence, recovery, and re-arm.
- Fee-inclusive conservation and rounding.

### 13.2 Data-contract tests

- Exact rolling-window drawdowns on hand-computed fixtures.
- No future row usage.
- UTC weekday semantics.
- Threshold crossings versus persistent levels.
- Idempotent view creation and stable column types.
- Duplicate/null/NaN/stale row rejection.

### 13.3 Integration and fault-injection tests

- Duplicate AUTO invocation settles once.
- Concurrent evaluation resolves to one decision.
- Crash before commit produces no debit.
- Crash after ledger commit reconciles without another debit.
- RiskGuard kill switch, macro window, black swan, and price deviation dominate buy states.
- Pool balances never cross zero and never fall back implicitly.

### 13.4 Comparative backtest gate

Run chronological 2020-01-01 through 2026-09-01 comparison, subject to data availability, for:

1. existing daily `DynamicReserveDCAStrategy`;
2. weekly batch DCA;
3. event-driven weekly core plus asymmetric sniper.

Report CAGR, maximum drawdown, Sharpe, Sortino, transaction count, total fees, average acquisition price, Base runway, reserve utilization, and time without deployable cash. Claims from the research report are hypotheses until reproduced by this suite. Promotion requires zero accounting/invariant violations and an explicitly approved quantitative acceptance threshold; no performance result may be fabricated when source coverage is incomplete.

## 14. Acceptance Criteria

- **AC-1:** Exactly four states exist with precedence and transitions specified here.
- **AC-2:** Neutral/chop AUTO evaluations settle no trade.
- **AC-3:** Weekly events debit only `00 Base`; sniper/capitulation events debit only `00 Tactical Reserve`.
- **AC-4:** Froth/hard safety always authorizes zero, including collision days.
- **AC-5:** Duplicate, concurrent, and crash-recovery paths cannot settle twice.
- **AC-6:** One drawdown episode cannot repeatedly consume reserve without re-arm.
- **AC-7:** Eight-week Base runway reduction and 40%-per-48h tactical bound are enforced alongside the current 15% daily reserve bound.
- **AC-8:** Every decision is reproducible from policy version, persisted inputs, prior checkpoint, and input fingerprint.
- **AC-9:** Data views are idempotent, zero-lookahead, UTC-defined, and contract-tested.
- **AC-10:** Existing RiskGuard and symbolic invariants remain authoritative.
- **AC-11:** Comparative backtest results are generated before forward-paper default activation.
- **AC-12:** `pytest`, lint, formatting, and type-check gates pass with no regression.

## 15. Rollout and Rollback

1. Land marts and contracts behind no execution behavior change.
2. Run shadow FSM evaluations and persist decisions with settlement disabled.
3. Replay and compare shadow outputs; resolve trigger and policy anomalies.
4. Run the comparative backtest gate and independent review.
5. Enable new pacing for a fresh paper portfolio or a documented, reconciled migration; do not silently reinterpret old 70/30 balances as 40/60.
6. Keep the existing strategy selectable for rollback.
7. Rollback disables new decision emission, not historical records; pending decisions are terminally rejected with an audit reason, and settled ledger entries remain immutable.