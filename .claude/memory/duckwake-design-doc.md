---
name: duckwake-design-doc
description: The DuckWake design documents in docs/design/ and keeping them in step with docs/roadmap.md
metadata:
  type: reference
---

The design documents live in the repository under `docs/design/`, one Markdown file each with diagrams in
`docs/design/images/`: the main design (`streaming-extension.md`: summary, SQL surface, user model, execution,
checkpointing, delivery plan, risks), then stateful operators, Arrow ingest, parallel processing, stream–stream
joins, CDC sources, change feed sources, database CDC sources, the performance backlog, the architecture review,
worker rebalancing and dbt-duckwake. `docs/design/README.md` indexes them.

The user also keeps a working copy as a Claude Doc. Never link it from the repository: no claude.ai links in any
committed file.

**How to apply:** `docs/roadmap.md` mirrors the main design's delivery plan; when one changes, change the other.
When a design step gets built, mark it built in its design document (date, branch, the tests that cover it).
Related: [[duckwake-dbt-not-ddl]].
