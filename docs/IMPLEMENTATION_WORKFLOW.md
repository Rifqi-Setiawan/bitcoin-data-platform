# Hermes Implementation Workflow

## Purpose

Hermes is the implementation workforce for this repository. Codex supplies an approved plan,
architecture constraints, and final review. The workflow deliberately separates planning from
execution so an implementation model cannot quietly expand scope.

## Profiles

| Profile | Responsibility | Must not do |
|---|---|---|
| `btc-orchestrator` | Turn an already-approved implementation plan into ordered, bounded Kanban cards; track dependencies and blockers. | Choose a new architecture or implement code itself. |
| `btc-coder` | Implement one scoped card, add tests, run quality checks, and commit on a task branch/worktree. | Work directly on `main`, expand phases, or administer the VPS. |
| `btc-verifier` | Reproduce acceptance checks independently and return evidence or a precise defect report. | Approve architecture, hide failures, or silently redesign/fix unrelated code. |

All profiles default to `ag/gemini-3.8-flash-high` through the private named `9router` provider.
For an unusually difficult, explicitly approved card, the operator may override only that task to
`ag/claude-opus-4-6-thinking`; the default remains Gemini Flash High.

## Board and task lifecycle

Use the dedicated `bitcoin-data-platform` board. Its project directory is
`/srv/projects/bitcoin-data-platform`. Coding tasks should use a Git worktree so concurrent workers
cannot overwrite one another. Direct shared-directory tasks are reserved for read-only checks or
explicitly serialized repository maintenance.

1. Codex/owner approves a bounded implementation specification.
2. Create a card with an assignee, dependencies, exact scope, acceptance criteria, and prohibited
   changes. Keep automatic decomposition disabled.
3. `btc-coder` implements on its task worktree and supplies test evidence plus a commit.
4. Assign a separate verification card to `btc-verifier` when independent execution evidence is
   useful. Hermes verification is a pre-check, not final approval.
5. Codex reviews the diff and architecture compliance. The owner decides whether to merge.

## Minimum card contract

Every implementation card should state:

- objective and user-visible outcome;
- files/components in scope;
- inputs, outputs, invariants, and failure behavior;
- tests and exact acceptance criteria;
- dependencies/parent cards;
- explicit non-goals;
- whether network access is permitted;
- expected artifact: commit, report, or both.

If a card cannot answer those questions, keep it blocked for clarification instead of coding from
assumptions.

## Safety and quality gates

- The Hermes Linux user remains unprivileged and has project-only collaborative write access.
- No worker receives sudo or Docker access, and no new public listener is allowed.
- Secrets stay in private environment files outside Git.
- Automatic Kanban decomposition and automatic review dispatch stay disabled so implementation
  cannot bypass Codex planning/review.
- Delegation is flat and concurrency is intentionally small to control conflicting edits and model
  usage.
- Repository instructions live in `AGENTS.md`; profile `SOUL.md` files contain only stable role
  identity and communication behavior.

## Phase boundary

Phase 1 ends when a clean environment can install the project, offline tests pass, and an explicit
bounded `backfill --start --end` safely writes checksummed immutable raw envelopes. Do not add
Parquet/DuckDB transforms, timers, public services, or a second data source until a separately
approved phase begins.
