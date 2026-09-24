# D-018: Event-Driven Regime Pacing with Dual-Pool Capital Isolation

## Status
Accepted

## Context
The platform currently supports periodic DCA, valuation and macro regimes, paper execution, and deterministic pre-trade clamping. A fixed daily cadence, however, can overtrade range-bound markets, spend dry powder before a material drawdown, and continue buying into overheated conditions. Conversely, a strategy driven only by drawdown magnitude can miss the long-horizon accumulation cadence and can repeatedly fire while one drawdown episode remains active.

The Phase 18 strategy requires four explicit operating states:

- `IDLE_CHOP`: no acquisition event is due and capital accumulates.
- `WEEKLY_CORE`: a scheduled weekly base-pool acquisition is due.
- `SNIPER_DEPLOYMENT`: a newly armed drawdown event permits bounded tactical-reserve deployment.
- `FROTH_FREEZE`: overheating or a hard safety condition suppresses all buys.

It also requires two independently accountable capital pools, named `00 Base` and `00 Tactical Reserve`. The design must preserve deterministic replay, prevent lookahead, avoid double deployment on repeated daily observations, and remain subordinate to the existing `RiskGuard` and `InvariantSolver` safety controls.

The foundational research input is maintained outside the repository at `/srv/hermes-control/reports/professor_bitcoin_execution_cadence_and_advanced_strategy_research_20260924.md`. It recommends replacing the fixed daily $10 burn rate with weekly batching, drawdown-triggered tactical tranches, valuation-regime transitions, and a 40/60 Base/Reserve allocation for the $1,000 sandbox. This ADR adopts those mechanisms as an initial policy baseline while requiring every threshold to remain versioned and empirically validated before forward-paper promotion.

## Decision

### 1. Use a deterministic daily finite-state machine

For each UTC trade date, the pacing evaluator consumes one immutable end-of-day trigger snapshot and the prior persisted FSM checkpoint. It emits exactly one decision record. It MUST NOT query future rows or revise a previously finalized decision in place.

State precedence is safety-first:

1. `FROTH_FREEZE`
2. `SNIPER_DEPLOYMENT`
3. `WEEKLY_CORE`
4. `IDLE_CHOP`

If several predicates are true on the same observation, the highest-precedence state wins. A macro proximity halt, black-swan halt, kill switch, invalid/stale price, or other hard `RiskGuard` rejection also forces effective execution to zero; these controls cannot be weakened by the FSM.

### 2. Separate observed state, intent, authorization, and settlement

The analytical mart reports facts and trigger flags. The FSM creates an intent. The symbolic risk layer authorizes or clamps that intent. The ledger performs settlement. No analytical view mutates portfolio balances, and no qualitative/LLM component may select a state or override an invariant.

An intent is not evidence of execution. Capital balances change only after an idempotent ledger settlement succeeds.

### 3. Preserve strict capital-pool isolation

`00 Base` funds scheduled core acquisitions only. `00 Tactical Reserve` funds drawdown sniper acquisitions only. A transfer between pools is a first-class balanced ledger event; execution code MUST NOT silently fall back from one pool to the other.

The following invariant holds at every committed ledger version:

`base_cash >= 0 AND tactical_reserve_cash >= 0`

For a buy settlement with fee-inclusive debits:

`base_debit + tactical_debit = gross_order_usd + fee_usd`

and:

`base_debit <= authorized_base_usd`

`tactical_debit <= authorized_tactical_usd`

A `WEEKLY_CORE` intent has `authorized_tactical_usd = 0`. A `SNIPER_DEPLOYMENT` intent has `authorized_base_usd = 0`, unless a later ADR explicitly permits a compound order. Phase 18 deliberately rejects compound orders to keep attribution and replay unambiguous.

### 4. Make sniper deployment edge-triggered and bounded

A drawdown level remaining true over multiple days is one episode, not one event per day. The tactical trigger is armed only when the prior observation was outside the configured band (or no active episode exists), and fires on entry into the band. It remains latched until re-armed by the configured recovery/hysteresis condition.

Every emitted decision and settlement uses deterministic idempotency keys. At minimum, a sniper key identifies policy version, instrument, trigger band, and episode identifier; a weekly key identifies policy version, instrument, and cadence period. Duplicate keys MUST return the original outcome without another debit.

Tactical authorization is the minimum of the policy tranche, available tactical balance, the existing daily reserve ceiling, and any stricter risk clamp. The current `InvariantSolver` ceiling of 15% of available reserve per UTC day remains an upper bound unless superseded by a separate reviewed ADR.

### 5. Treat thresholds as versioned policy, not embedded state-machine logic

The following are configuration fields with explicit units and validation: weekly cadence weekday/timezone, 24-hour, 7-day, and 30-day drawdown entry bands, re-arm hysteresis, valuation and froth thresholds, tactical tranche percentage, data freshness limit, and policy effective interval. Policy changes create a new `policy_version`; they do not retroactively rewrite decisions.

Initial research-derived defaults are: Sunday UTC weekly cadence (operational execution may occur at Monday 00:05 UTC), $20 Base weekly buy for the $400 Base pool, $600 initial Tactical Reserve, sniper entry at a 24-hour return of at most -5% or a 7-day drawdown of at most -12%, and a 20%-35% tactical request before safety clamping. Capitulation is MVRV crossing below 1.0 or Mayer Multiple crossing below 0.8. Froth lockout is deterministically active when FNG is at least 80 and Mayer Multiple is at least 2.0; the research also identifies MVRV above 2.5 or Mayer above 2.2 as capital-preservation regime thresholds.

These defaults are hypotheses, not proven production constants. A missing/invalid policy produces no buy intent and an auditable reason code.

## State Transition Contract

Let the evaluated predicates be:

- `hard_freeze`: any non-overridable safety halt or invalid/unavailable required input.
- `froth`: the configured froth predicate is true.
- `sniper_edge`: `is_drawdown_event` is true and the drawdown episode is armed.
- `weekly_due`: `is_weekly_cadence_day` is true and the cadence period has not settled.

The transition function is total and deterministic:

| Current state | Guard (in precedence order) | Next state | Permitted intent |
|---|---|---|---|
| Any | `hard_freeze OR froth` | `FROTH_FREEZE` | No buy; preserve both pools |
| Any non-frozen state or recovered freeze | `sniper_edge` | `SNIPER_DEPLOYMENT` | Tactical-only, bounded tranche |
| Any non-frozen state or recovered freeze | `weekly_due` | `WEEKLY_CORE` | Base-only, configured core amount |
| Any | none of the above | `IDLE_CHOP` | No buy; preserve/accumulate cash |

A state is evaluated for one decision cycle; it is not a license for repeated orders. After successful or terminally rejected processing, the next observation is evaluated afresh. `FROTH_FREEZE` exits only when both `hard_freeze` and `froth` are false. If freeze and sniper entry coincide, the sniper episode remains unspent but MUST NOT automatically fire later unless the configured episode/re-arm contract says it is still a valid edge; the decision record must expose that disposition.

## Input and Output Contracts

The canonical trigger input is one row per `trade_date_utc` and instrument from `mart_btc_event_triggers`, with at least:

- `trade_date_utc DATE NOT NULL`
- `product_id VARCHAR NOT NULL`
- `close DOUBLE NOT NULL CHECK (close > 0)`
- `drawdown_7d DOUBLE` and `drawdown_30d DOUBLE`, represented as decimal returns in `[-1, 0]`
- `return_24h DOUBLE`, represented as a decimal return
- `is_weekly_cadence_day BOOLEAN NOT NULL`
- `is_drawdown_event BOOLEAN NOT NULL`
- `is_regime_capitulation BOOLEAN NOT NULL`
- `is_regime_froth BOOLEAN NOT NULL`
- upstream observation timestamp or freshness evidence

The downstream FSM decision contract contains:

- `decision_id`, `idempotency_key`, `policy_version`
- `trade_date_utc`, `product_id`, `evaluated_at_utc`
- `previous_state`, `next_state`
- copied trigger values used in evaluation
- `drawdown_episode_id` when applicable
- `requested_base_usd`, `requested_tactical_usd`
- `authorized_base_usd`, `authorized_tactical_usd`
- `reason_codes` as a stable machine-readable set
- `input_fingerprint` over canonicalized inputs
- `status` in `PROPOSED | AUTHORIZED | REJECTED | SETTLED | FAILED`

Null or non-finite required prices, positive drawdown values, duplicate date/instrument rows, non-monotonic observation dates, stale observations, or unknown policy versions are contract violations and fail closed.

## Failure-Mode Controls

| Failure mode | Effect | Required control |
|---|---|---|
| Missing/stale market row | Decision based on obsolete price | `FROTH_FREEZE`, zero intent, freshness reason code |
| Duplicate scheduler delivery | Double purchase | Unique idempotency key and transactional ledger constraint |
| Drawdown flag true for many days | Reserve exhaustion | Episode latch plus recovery hysteresis |
| Froth and drawdown coincide | Buy into overheated regime | Freeze precedence |
| Weekly and sniper coincide | Ambiguous pool attribution | Sniper precedence; no compound order |
| Partial ledger write | Cash/asset mismatch | Single transaction; balanced journal; rollback on failure |
| Policy changes mid-run | Non-reproducible outcome | Immutable policy version captured in decision |
| Analytical view recomputation | Historical trigger drift | Persist decision inputs and fingerprint; do not rewrite settled decisions |
| Negative/NaN balance or amount | Capital corruption | Contract validation and transaction rejection |
| LLM/committee override attempt | Safety bypass | FSM and invariants remain deterministic and authoritative |

## Consequences

### Positive

- Strategy behavior is replayable, explainable, and naturally idempotent.
- Capital purpose and performance attribution remain auditable by pool.
- Safety signals dominate opportunity signals.
- Threshold calibration can evolve without changing state-machine semantics.
- The analytical mart and execution engine can be tested independently through a stable contract.

### Negative

- Episode latching and immutable policy versions add persistence and operational complexity.
- Strict pool isolation may leave capital idle even when the other pool has funds.
- Sniper precedence can defer a weekly core purchase on collision days.
- Fail-closed behavior can miss opportunities during data outages.

## Alternatives Considered

1. **Fixed weekly DCA only.** Rejected because it cannot exploit bounded asymmetric drawdowns or freeze in froth.
2. **Daily regime multiplier.** Rejected because persistent flags can cause repeated reserve deployment and obscure event semantics.
3. **One fungible cash pool.** Rejected because it permits accidental reserve exhaustion and weakens attribution.
4. **LLM-selected pacing.** Rejected because state selection and arithmetic safety must be deterministic.
5. **Streaming event broker.** Deferred; a daily DuckDB-driven evaluator is sufficient for the current single-host, end-of-day strategy and avoids unnecessary operational services.

## Verification and Compliance

The implementation is compliant only if automated tests prove:

- full transition-table coverage, including all predicate collisions;
- no lookahead and chronological replay determinism;
- one settlement per idempotency key under retries;
- no repeated tactical debit during one latched episode;
- non-negative, isolated pool balances and balanced fee-inclusive journal entries;
- freeze precedence over every buy state;
- policy-version and input-fingerprint persistence;
- fail-closed handling for stale, null, duplicated, non-finite, or invalid input;
- compatibility with existing `RiskGuard` and `InvariantSolver` clamps.
- Base runway protection: if `base_cash / weekly_budget < 8`, weekly authorization is reduced by 50%.
- Tactical velocity protection: aggregate Tactical Reserve debit is at most 40% of the window-opening reserve balance in any rolling 48-hour window; the existing 15% per-UTC-day clamp remains independently applicable, so the stricter bound wins.

## Reconsider When

Revisit this decision when intraday execution becomes necessary, multiple concurrent writers require an external transactional service, validated research approves compound base-plus-tactical orders, or live exchange integration introduces venue-specific order and reconciliation semantics.