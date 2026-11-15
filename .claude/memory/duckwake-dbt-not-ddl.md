---
name: duckwake-dbt-not-ddl
description: SQL authoring goes through a dbt package (dbt-duckwake), not CREATE CONTINUOUS VIEW DDL, which is long term only; the plan and its state
metadata:
  type: project
---

On 2026-11-13 the user decided DDL support is not worth building now and chose a dbt package, dbt-duckwake,
instead; `CREATE CONTINUOUS VIEW` moved to the "Long term" section of `docs/roadmap.md` and of the SQL surface in
`docs/design/streaming-extension.md`.

**Why:** DuckDB's C API has no parser hooks. A spike proved a C++ parser extension works against the DuckDB that
`libduckdb-sys` bundles (headers via `DEP_DUCKDB_INCLUDE`; build with the same DEBUG/NDEBUG as DuckDB), but it
uses DuckDB's unstable internal C++ API and works only in `duckwaked` and `duckwake sql`, not the loadable
extension. dbt gives files, review, CI, `ref()` order, tests and docs with no C++.

**How to apply:** the plan, in order, each its own PR:
1. `create_view` `if_exists` with the outcome reported - merged in PR #5 (2026-11-14).
2. Backfill of new views over lake inputs (F3) - built 2026-11-15 on branch `f3-backfill` (`start := 'earliest'`
   default, P_min retention, held-snapshot metrics, reset cascade; gate `scripts/backfill_test.sh`). The package
   can create and start each view as dbt builds it; the stopped-then-started workaround is no longer needed.
3. The package: a Jinja-only dbt package on dbt-duckdb in a `dbt/` folder, with `streaming_source` and
   `streaming_view` materializations, an example project and a CI suite. A plain `dbt run` never recomputes:
   unchanged does nothing, settings go through `alter_view`, a changed query fails until `--full-refresh`
   (stricter than dbt-risingwave, which ignores a changed query).
4. Later: zero-downtime replace (build beside, catch up, swap in one DuckLake transaction).

Open: whether dbt-duckdb can install our extension from a repository URL; how `ref()`'s quoted three-part
names reach DuckWake's compiler. Full design: `docs/design/dbt-duckwake.md` ([[duckwake-design-doc]]).
