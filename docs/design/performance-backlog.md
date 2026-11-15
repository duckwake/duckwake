# Performance backlog

Commits and small files · Nov 5, 2026

DuckLake inlines an insert only when it has at most 10 rows. DuckWake raises that limit for sinks and the checkpoint table; late tables and dead letters still write one small Parquet file per epoch, on purpose. Status (2026-11-05, commit `cc7bad3`): the checkpoint table is now inlined up to 1000 rows per insert, which took it from 27 files to 0 with commit p50 unchanged (326 ms). Inlining dead letters and late rows too was tried and dropped: commit p50 rose to 382 ms.

## How DuckLake handles frequent commits

- **A commit is cheap.** It is one SQL transaction on the catalog and rewrites no metadata files, unlike Iceberg manifests or Delta log files. Bench at 16 views and 20k ev/s: `commit_call` p50 about 80 ms, data `writes` p50 about 240 ms.
- **Data inlining.** An insert at or under the limit goes into catalog tables instead of a Parquet file. `ducklake_flush_inlined_data` moves it to Parquet later.
- **What DuckLake does not do.** It has no group commit, and snapshots stay until `ducklake_expire_snapshots` runs.
- **What DuckWake already does.** It commits every job on a worker in one transaction per round (`commit()` in `engine.rs`). It skips an epoch with no new rows and an unchanged watermark. Maintenance flushes inlined data, merges files, expires snapshots, deletes old files and prunes checkpoints.

So snapshot count scales with workers × rounds, not with views or sources: about 43k snapshots a day per worker at a 2 s round.

## Measured

Default limit on DuckDB v2.0.0 with DuckLake format 1.1, tested on a scratch lake: an insert of 10 rows is inlined and 11 rows writes a Parquet file. `ducklake_options` does not list the option, and the repo sets no global value.

Live Parquet files per table in the catalog Postgres:

| Lake | Table | Live files | Rows in files | Why |
| --- | --- | --- | --- | --- |
| `duckwake_bench` (16 views, `lean_commit`) | `__duckwake.checkpoints` | 27 | 432 | one INSERT of 16 rows per epoch, over 10 |
| `duckwake_bench` | `__duckwake.dead_letter` | 27 | 96,000 | no inlining option |
| `duckwake_bench` | each `bench.*_late` | 26 | 5,830–17,490 | no inlining option |
| `duckwake_bench` | sinks | 0 | 0 | `set_sink_inlining` |
| `duckwake_test` (suites) | `checkpoints`, `lease_*` | 0 | 0 | one row per write |
| `duckwake_test` | `state_active_users` | 14 | 29,681 | DISTINCT state is over 10 rows per epoch |

The bench run was short, so maintenance had not merged its files yet.

## Proposed fix (not done)

1. Set `data_inlining_row_limit` on the control tables (`checkpoints`, `dead_letter`, `lease_*`) and on `*_late` tables, as `set_sink_inlining` does for sinks. A simpler alternative is one global value set when the lake is created, overridden per table where needed.
2. Re-apply the option on each worker connection, since an attached lake does not see options set later (finding 5 in `docs/internals.md`).
3. Measure before inlining state tables. State rows are deleted and rewritten every epoch, and deletes of inlined rows conflict with concurrent writes to the same table ("another transaction has deleted inlined data", finding 1).
4. Inlined rows live in the catalog Postgres until a flush, so keep the maintenance flush enabled.
5. Check the fix by rerunning the bench and comparing live files per table and `commit_p50`.

## Decision: idle commits for CDC ingest

An ingest job commits every epoch, including empty epochs that only advance its progress time T. There is no idle throttling. Continuous data commits every epoch anyway, so throttling would save little. Once control tables are inlined, an empty epoch costs only a few catalog rows.

## Open questions

- Is the default limit of 10 the same with a Postgres catalog as with a DuckDB file catalog? The test lake used a DuckDB file catalog. The bench lake on Postgres shows the same behavior for checkpoints, but this was not tested directly.
- What limit suits `*_late` and `dead_letter`, whose rows per epoch vary with traffic?
