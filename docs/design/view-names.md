# View names

Date: Nov 22, 2026. Status: built (see the end).

## The problem

A view is named by the DuckLake table it maintains, such as `analytics.daily`. Up to 0.2 its job id was the
table name alone, `daily`. That id keys everything DuckWake keeps for the view: the `jobs`, `checkpoints`,
`dead_letter` and `dependencies` rows, the per-view tables `__duckwake.lease_<job>` and `__duckwake.state_<job>`,
the in-memory staging tables of an epoch (`in_<job>`, `agg_<job>`, `up_<job>`, ...), the `job` field of the
logs and the `view` label of the metrics. So two schemas could not each have a view of the same table name:
`marketing.daily_active` and `product.daily_active` were refused, although DuckDB, DuckLake and dbt all treat
them as two tables. dbt-duckwake already sent `schema.identifier` for every model.

## The decision

A view's job id is `schema.table`. Nothing else changes shape:

- The per-view tables keep their pattern with the dot inside a quoted name: `__duckwake."lease_analytics.daily"`,
  `__duckwake."state_analytics.daily"`, `memory.main."in_analytics.daily"`. Every place that builds such a name
  already quotes it (`ident`). `check_name` allows no dot in a schema or table name, so the id splits one way.
- An ingest job's id stays its source's name (`shop`), which has no dot, so a view and an ingest job never share
  an id.
- The deprecated `--sink` form, which named the job and the table separately, is gone: a view's id is always
  its table's name.

How a name is read:

| Where | `analytics.daily` / `lake.analytics.daily` | `daily` |
| --- | --- | --- |
| `create-view`, `duckwake_create_view` | that table | `main.daily` |
| Other commands and functions (`start`, `reset`, `verify`, ...) | that view | the view whose table is `daily`, when exactly one schema has one; several fail and list them; none falls back to a database source's ingest job |
| A view's SQL (`FROM daily`) | that table or view | `main.daily`, as DuckDB reads it (the worker runs `USE lake`) |

A bare name in SQL follows DuckDB rather than the command rule: the same text then means the same table in a
view and in an ad hoc query.

## No migration

DuckWake had no users when this changed, so a lake created by 0.2 is not migrated. A worker refuses to start on
a lake with a view whose id has no dot, and so does `create-view`, naming those views; `duckwake drop <view>`
still drops them, and the lake is usable again. This costs about twenty lines and fails loudly instead of
running with mixed ids.

A migration would also have been harder than it looks. Renaming `state_daily` to `"state_analytics.daily"`
keeps the DuckLake `table_id` (checked: 8 before and after), but time travel under the new name stops at the
rename: `SELECT ... FROM "state_analytics.daily" AT (VERSION => 11)` fails with "does not exist at version 11",
while the old name still reads that version. `reset` restores a view's state with
`INSERT INTO state SELECT * FROM state AT (VERSION => s)`, and a watermark is read from `checkpoints AT (VERSION
=> s)` by job id, so a renamed view could not be reset to an epoch before its migration. A migration, when one is
needed, has to separate an immutable internal key from the user-facing name instead.

## Status

Built on Nov 22, 2026 (branch `claude/focused-mendel-cbfmfj`):

- `catalog::view_names`, `resolve_view` and `view_read_by`, with unit tests for each row of the table above
  and for the 0.2 guard (`lake::check_view_ids`).
- The circuit tests run with the job id `analytics.v`, so every staging and state table name carries a dot.
- `scripts/names_test.sh`: `a.daily` and `b.daily` over one table, a view over a view across schemas, a view in
  `main` read by its bare name, crashes, a reset of one `daily` that leaves the other alone, a drop, metrics
  labels, and the 0.2 guard. Its first run found `reset` looking up an epoch's snapshot with the JSON path
  `$.jobs.<job>`, which a dot splits; the path now quotes the id.
- `scripts/dbt_test.sh`: the example project has `staging.game_totals` beside `analytics.game_totals`.
