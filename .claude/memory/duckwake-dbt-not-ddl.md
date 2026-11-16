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
2. Backfill of new views over lake inputs (F3) - merged 2026-11-15 (PR #2) (`start := 'earliest'`
   default, P_min retention, held-snapshot metrics, reset cascade; gate `scripts/backfill_test.sh`). The package
   can create and start each view as dbt builds it; the stopped-then-started workaround is no longer needed.
3. The package - merged 2026-11-15 (PR #4) in `dbt/`: Jinja-only on
   dbt-duckdb 1.11 / DuckDB 2.0.0, `streaming_source` and `streaming_view` materializations, `duckwake_verified`,
   `duckwake_wait_built`, `duckwake_drop_orphans`, an example project in `dbt/example`, CI suite
   `scripts/dbt_test.sh`. A plain `dbt run` never recomputes; a changed query fails until `--full-refresh`, which
   needs the readers selected (`-s view+`) and drops and recreates them.
4. Later: zero-downtime replace (build beside, catch up, swap in one DuckLake transaction).

Answered: dbt-duckdb installs the extension from a repository path or URL (`repo:` needs its own quotes;
`config_options: {allow_unsigned_extensions: true}`); the compiler accepts quoted `"lake"."schema"."table"`, and
the package rewrites `ref()` to a source model into the bare source name. On 2026-11-16 the user created
`duckwake/dbt-duckwake` and chose to keep writing the package in `dbt/` and mirror it there (branch `dbt-mirror`):
`.github/workflows/dbt-mirror.yml` on pushes to main, the release's `dbt-package` job tags `vX.Y.Z` there; both
need the secret `DBT_MIRROR_TOKEN`, which the user adds. Never edit dbt-duckwake by hand. The dbt Hub listing is a
PR to dbt-labs/hubcap after the first tag (needs the user's go-ahead). Full design: `docs/design/dbt-duckwake.md` ([[duckwake-design-doc]]).
