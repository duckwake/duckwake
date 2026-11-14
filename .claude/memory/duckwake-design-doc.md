---
name: duckwake-design-doc
description: The DuckWake design doc (a Claude Doc with tabs) and keeping it in step with docs/roadmap.md
metadata:
  type: reference
---

The design doc is the Claude Doc "DuckDB Streaming Extension — Design Doc":
https://github.com/duckwake/duckwake/tree/main/docs/design (edit it with the Claude Docs tools, never by web fetch).

Tabs: the main tab (summary, SQL surface, user model, execution, checkpointing, delivery plan, risks), then
Stateful operators, Arrow ingest plan, Parallel processing, Stream–stream joins, CDC sources, Change feed
sources, Database CDC sources, Performance backlog, Architecture review, Worker rebalancing, and dbt-duckwake
(https://github.com/duckwake/duckwake/tree/main/docs/design).

**How to apply:** `docs/roadmap.md` mirrors the doc's Delivery plan; when one changes, change the other. When a
design step gets built, mark it built in the doc (date, branch, the tests that cover it). Related:
[[duckwake-dbt-not-ddl]].
