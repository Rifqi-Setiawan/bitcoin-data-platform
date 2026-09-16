# Bitcoin Data Engineering Platform

This repository is the architecture and implementation workspace for a production-style,
single-host Bitcoin data platform and public data-engineering portfolio project.

The current baseline contains approved architecture, roadmap, and Hermes implementation governance.
Application code has not been implemented yet. The next bounded phase is repository bootstrap plus
deterministic raw ingestion for public Coinbase Exchange `BTC-USD` hourly candles.

Start with [the master plan](docs/MASTER_PLAN.md).

## Current documents

- [Master plan](docs/MASTER_PLAN.md)
- [Architecture V1](docs/architecture/ARCHITECTURE_V1.md)
- [Decision log](docs/decisions/README.md)
- [Roadmap](docs/roadmap/ROADMAP.md)
- [Data Engineering concept map](docs/learning/DE_CONCEPT_MAP.md)
- [Source evaluation](docs/sources/SOURCE_EVALUATION.md)
- [Hermes implementation workflow](docs/IMPLEMENTATION_WORKFLOW.md)

## Safety boundary

This project is for data engineering and Bitcoin research. It does not place trades, provide automated buy/sell decisions, expose a database publicly, or depend on an AI agent for pipeline correctness.
