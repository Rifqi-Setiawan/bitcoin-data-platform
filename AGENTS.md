# Hermes Project Instructions

## Mission and authority

This repository is the implementation workspace for the Bitcoin Data Engineering Platform.
Hermes implements only work that is already approved in a scoped task. Codex remains responsible
for planning, architecture decisions, and final review. Do not silently redesign the system or
advance to a later roadmap phase.

The current approved implementation boundary is Phase 1 in `docs/roadmap/ROADMAP.md`: repository
bootstrap and deterministic raw ingestion of public Coinbase Exchange `BTC-USD` hourly candles.
Stop and report a blocker when a requested change conflicts with the architecture or lacks an
acceptance criterion.

## Read before editing

For every new task, read in this order:

1. The Kanban task body, comments, dependencies, and acceptance criteria.
2. `README.md` and this file.
3. `docs/MASTER_PLAN.md`.
4. `docs/architecture/ARCHITECTURE_V1.md`.
5. The current phase in `docs/roadmap/ROADMAP.md`.
6. Relevant decisions in `docs/decisions/README.md` and source notes under `docs/sources/`.

If documents disagree, do not guess. Preserve the safer/narrower behavior and report the conflict.

## Workspace boundary

- Work only under `/srv/projects/bitcoin-data-platform` or a Git worktree created from it.
- Do not modify `/srv/apps`, `/etc`, `/srv/infrastructure`, UFW, systemd, Docker, 9Router, or Hermes.
- Do not use sudo, Docker, privileged containers, or system service commands.
- Do not expose a port or add a long-running service during Phase 1.
- Do not put persistent project data on `/mnt`; it is ephemeral Azure storage.
- Do not add trading, wallet, private-key, order-execution, or financial-advice behavior.

The operating-system account already enforces most of this boundary. Treat it as policy too.

## Git workflow

- Start from a clean tree and inspect `git status` before changing files.
- Never work directly on `main` for an implementation task. Use a task branch named
  `hermes/<task-id>-<short-slug>` or the Kanban-provided worktree branch.
- Keep commits small, coherent, and free of generated data and secrets.
- Do not rewrite shared history, force-push, hard-reset, or delete another worker's branch/worktree.
- Do not create a public remote, push, or open a pull request unless the task explicitly authorizes it.
- Before handoff, show the diff, run the complete available quality gate, and ensure `git status`
  contains only intentional changes.

## Engineering defaults

- Target Python 3.12 with a `src/bitcoin_data_platform/` package layout.
- Use type hints on public functions and explicit UTC-aware datetimes.
- Keep source access, window planning, storage, configuration, and CLI orchestration separated.
- Use bounded HTTP timeouts, bounded exponential backoff with jitter, and explicit handling for
  429, transient 5xx, timeouts, and permanent 4xx responses.
- Preserve successful source responses as immutable gzip JSON envelopes before transformation.
- Use atomic write-then-rename and SHA-256 checksums. A failed response must never be stored as data.
- Never log secrets, full environment dumps, or sensitive headers.
- Keep live-network tests out of the default offline test suite. Use small sanitized fixtures.
- Prefer the simplest dependency that satisfies the approved contract. Do not introduce Kafka,
  Spark, Docker, dbt, Airflow, a database server, or a web API in Phase 1.

## Required verification

Add or update tests with every behavior change. Phase 1 must cover at least:

- half-open UTC range and hourly-boundary validation;
- Coinbase's maximum 300-candle request window;
- deterministic window planning without gaps or overlap errors;
- retry and terminal-error classification;
- malformed/partial source tuples;
- immutable envelope schema, checksum, and atomic promotion;
- safe reruns and secret-free logs.

Run the repository's documented formatter, linter, type checker, and tests. If a tool is not yet
configured, do not invent a passing result; establish it in the bootstrap task or report it missing.
Live smoke tests are supplementary and must never be the only evidence.

## Handoff contract

Every completed implementation task must report:

- task and branch name;
- files changed and the reason for each group;
- exact validation commands and results;
- assumptions, residual risks, and any intentionally deferred work;
- commit hash, or a clear reason no commit was made.

The implementation worker may mark work ready for review, but only Codex/the owner accepts
architecture changes and performs the final review.
