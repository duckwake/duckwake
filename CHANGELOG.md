# Changelog

All notable changes to DuckWake. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/) as described in [docs/releasing.md](docs/releasing.md).

## [Unreleased]

### Added

- PostgreSQL sources ([#44](https://github.com/duckwake/duckwake/issues/44)): `duckwake_create_source('shop',
  'postgres', '{"dsn": ..., "tables": [...]}')` (`create-source --kind postgres`) keeps a mirror table of each
  table in the lake, `lake.<source>.<table>`, with one row per primary key, read from a logical replication
  slot with no Kafka or Debezium. Its ingest job copies every table under one snapshot, then applies whole
  transactions (inserts, updates, deletes, primary key changes, `TRUNCATE`, and updates that leave a TOASTed
  value out) and commits the mirror rows with the slot position in one DuckLake transaction; the slot is
  advanced only after that, so it is never ahead of the lake. A slot that is gone, invalidated or ahead of the
  lake, a changed table, or a value its mirror column cannot hold stops the job with the reason; `reset
  --to-epoch 0` takes a new snapshot. `verify` compares the mirrors with the tables; views read the mirrors
  through their change feeds. Reads through DuckDB's postgres extension, with no new dependency. Gate:
  `scripts/pg_ingest_test.sh`, in order and pipelined.
- `duckwake_drop_source(name)` (`drop-source NAME`); for a PostgreSQL source it also drops the mirror tables,
  the slot, and the publication DuckWake created.
- Windowed views take the updates and deletes of their lake inputs ([#38](https://github.com/duckwake/duckwake/issues/38)), of a
  table or of a view whose rows change (an upsert view's, say), where they used to read them insert-only and
  stop. A change to a window still open revises it: `min`, `max`, `bool_and`/`bool_or`, `count(DISTINCT)`,
  `approx_count_distinct` and the quantiles keep each value's count, so a deleted extreme gives way to the next
  value. A change to a window already closed is a late correction: the late-data table gets it with a
  `_duckwake_weight` of -1 (or +1), or `rows_late` counts it, and `verify` adds it back. Views created before
  keep reading insert-only. A reset of a view empties the windowed views that read it, which backfill again, as
  before: its rewrite of closed windows would otherwise only be late corrections. Gate: `scripts/retract_test.sh`.
- Debezium sources ([#42](https://github.com/duckwake/duckwake/issues/42)): `"format": "debezium"` reads a topic of
  Debezium change events, with or without `schemas.enable`. Its views set `upsert_key` to the table's primary
  key: inserts, snapshot reads and updates replace a key's row from `after`, deletes remove it (keyed by the
  message key, or by `before` when there is none), and truncates and other events are dead letters that name
  them. Debezium's epoch numbers in `TIMESTAMP` and `DATE` columns are read as times. An event with a value
  Debezium left out (`__debezium_unavailable_value`, a PostgreSQL table without `REPLICA IDENTITY FULL`) stops
  the view with an error that says so. `duckwake produce --keys N --debezium` writes such a topic. Gates:
  `FORMAT=debezium scripts/upsert_test.sh`, in order and pipelined, and `scripts/debezium_e2e_test.sh`, end to end
  with PostgreSQL 16 and Debezium's connector 3.6.3 (`docker compose --profile debezium`).
- A source's `event_time` is optional when its format is `debezium`; a window over a source without one is
  refused.
- Upsert views over Kafka topics ([#41](https://github.com/duckwake/duckwake/issues/41)):
  `duckwake_create_view(..., upsert_key := 'order_id')` (`create-view --upsert-key`) reads a topic of changes as
  one row per key. The key comes from the message key (a JSON object's field, or the whole key), else the
  value; a later message replaces its key's row and a tombstone (no value) deletes it. With `upsert_version` a
  message applies only when its version is at least the row's, so an older message redelivered or replayed
  out of order changes nothing. The view's state holds each key's current row and nothing for deleted keys. The
  view's table is a plain DuckLake table; aggregates go in views that read it through the change feed. `verify` checks it, `reset` rewinds it, and dbt-duckwake
  takes both settings as model config. Gate: `scripts/upsert_test.sh`, in order and pipelined.
- `duckwake produce --keys N` sends keyed order updates for upsert views, with `--delete-every`,
  `--repeat-every` and `--stale-every`.
- `duckwake sql` shows each message's key (`_key`) when it queries a source.

- DuckWake is a DuckDB community extension: `INSTALL duckwake FROM community` installs it signed, with no
  `-unsigned`, on Linux (amd64, arm64) and macOS on Apple Silicon
  ([duckdb/community-extensions#3654](https://github.com/duckdb/community-extensions/pull/3654),
  [#54](https://github.com/duckwake/duckwake/issues/54)). The repository builds for DuckDB's extension CI: a root
  `Makefile`, which builds the library with `cargo build -p duckwake_extension`, and a sqllogictest in
  `test/sql` that runs without PostgreSQL or Kafka. Intel Macs and other platforms install unsigned from
  `https://duckwake.github.io/extensions`.

- More aggregates, in windowed and `GROUP BY` views ([#29](https://github.com/duckwake/duckwake/issues/29)):
  - `covar_pop`, `covar_samp`, `corr` and the `regr_*` functions (`regr_count`, `regr_avgx`, `regr_avgy`,
    `regr_sxx`, `regr_syy`, `regr_sxy`, `regr_slope`, `regr_intercept`, `regr_r2`). Each group keeps the count
    and the sums of y, x, y², x² and xy over the rows where neither is NULL, so they follow updates and deletes
    like `avg`, with DuckDB's NaN and NULL results for constant groups.
  - `median`, `quantile_cont` and `quantile_disc` (`quantile`), exact, with a constant fraction or a list of
    them.
  - `approx_count_distinct`, equal to the batch query's estimate.
  - `count(DISTINCT x)` in windowed views.

  In `GROUP BY` views the last three keep each group's values with their counts, as `count(DISTINCT)` does, and
  follow updates and deletes. In windowed views they keep each window's values as a list, over inputs that only
  insert.
- `arg_min` and `arg_max` (and `min_by`, `max_by`) are refused with the reason: when rows tie, DuckDB's batch
  result depends on row order, so `duckwake_verify` could not check the view.

### Changed

- `verify` lists up to five differing rows on each side when a view's output does not match, not only their counts.
- A Kafka message without a value (a tombstone) is a dead letter with the error `tombstone` instead of
  `invalid json`, in the views that do not read its source as upserts.

### Fixed

- `verify` failed now and then on views with `corr`, `regr_slope` or the variances (`bf_spread` in
  `backfill_test.sh`, about one run in six): it rounded DOUBLE and FLOAT columns to 10 significant digits on both
  sides, and two results 10⁻¹³ apart could round to different values across a rounding step. It now compares
  them within a relative 10⁻⁹, pairing the rows that differ by their other columns, which stay exact. Below 1
  the bound is 10⁻⁹ absolutely: a `covar_samp` that is truly 0 came out as 5×10⁻¹⁵ in a window that took deletes.
- A windowed view over another windowed view's `window_start` or `window_end` closed its window up to
  `--position-refresh` (5 minutes by default) late once the upstream view's inputs went idle: the upstream view,
  with no window left to close, stopped committing, so the watermark in its checkpoint, which the downstream
  view reads, stood still until a position refresh committed it. A windowed view
  over lake inputs now also commits each time its watermark has moved a whole slide since it last recorded it,
  at most once a slide while idle. Found by `chain_test.sh` on `main`; `chain_test.sh` now checks it every run.
- The dead-letter query in the `DuckWakeDeadLetters` alert's docs named a column `_err`; it is `error`.

- CI could fail to start the stack with "failed to bind host port for 0.0.0.0:55432 ... address already in use":
  the published ports (55432, 19092-19095, 19900) are inside Linux's ephemeral port range, and an outgoing
  connection on the runner could hold one when Docker bound it. CI now reserves them before starting the stack.
- The development and CI stack's Redpanda could fail to start with "Address already in use" on its internal
  RPC port, 33145, when an outgoing connection in its container had taken that port; the container's ephemeral
  ports now start at 40000.
- A group whose values are all equal has a variance and standard deviation of exactly 0, as in DuckDB's batch
  query. Before, rounding in `q − s²/n` could leave a tiny positive variance whose square root (about 10⁻⁸
  times the values) `duckwake_verify` reported as a difference.

- Release notes link to files in the repository at the release's tag; the relative links copied from the
  changelog led nowhere on the release page.

## [0.2.0] - 2026-11-16

The first release with published binaries, extension and image. Views over lake tables and other views, with
backfill; failover and rebalancing across workers; Kafka SASL/TLS and S3; more aggregates; changing views while
they run; limits; monitoring; and the dbt package, dbt-duckwake.

### Upgrading from 0.1.0

- Stop every 0.1.0 worker before the first 0.2.0 worker starts ([versioning](docs/releasing.md#versioning)). The
  first 0.2.0 process to open the lake adds what this version stores: the `pinned_to` column of the jobs table
  and the workers table, where workers heartbeat. A 0.1.0 lake with running and paused views was opened this way
  and its worker took the running views over; resuming views from 0.1.0 checkpoints is part of P3's upgrade
  tests ([roadmap](docs/roadmap.md#p3-proven-under-load-and-failure)).
- dbt-duckwake is versioned with DuckWake: install the package at the tag of the version your workers run
  (`revision: v0.2.0`).

### Known issues

- A view whose Kafka brokers cannot be reached for 10 s while its worker starts it is marked `failed` and must be
  started again (`duckwake start`) once Kafka is back; a missing topic, by contrast, is waited for. Kafka outages
  are among P3's failure drills.

### Added

- **More aggregates.** `var_samp` (`variance`), `var_pop`, `stddev_samp` (`stddev`) and `stddev_pop`, in windowed
  and `GROUP BY` views, from each group's count, sum and sum of squares, so they follow updates and deletes;
  `bool_and` and `bool_or`, wherever `min` and `max` work; `count(DISTINCT x)` in `GROUP BY` views, with the
  state counting each group's values, also over inputs that update or delete; and `HAVING` in windowed views,
  which emit only the closed windows that pass it. An aggregate still unsupported is refused when the view is
  created, with the list of those that are ([sql.md](docs/sql.md#aggregates)).
- `--max-spill` (`DUCKWAKE_MAX_SPILL`) bounds the disk a worker's DuckDB spills to; past it an epoch runs out of
  memory and the worker shrinks its epochs, as past `--memory-limit`.
- **dbt-duckwake**, in `dbt/`: Kafka sources and streaming views as dbt models on dbt-duckdb
  (`materialized='streaming_source'` and `'streaming_view'`). A plain `dbt run` creates and starts new views,
  leaves unchanged ones alone, alters changed settings in place and fails on a changed query until
  `--full-refresh`; sources gain columns. Also `duckwake_verified` (a generic test over `duckwake_verify`),
  `duckwake_wait_built` and `duckwake_drop_orphans`. It is published from `dbt/` as the root of
  [duckwake/dbt-duckwake](https://github.com/duckwake/dbt-duckwake) on every change, and each release tags it
  there with its own version (`packages: - git: .../dbt-duckwake.git, revision: vX.Y.Z`), ready for dbt Hub.
- **Backfill of views over lake tables and other views (F3).** A new view starts from the rows its inputs
  already hold (`start := 'earliest'`, the default; `--start latest` keeps the old behaviour), read at the
  snapshot current when the view was created, `max_rows` rows per epoch and then the changes after it; it continues where it stopped
  after a crash, and windows close only once it is done. Maintenance no longer expires a snapshot a view still
  needs, paused and stopped views included, and keeps the commit times of expired snapshots for `_commit_ts`.
  `duckwake_snapshots_held` and `duckwake_snapshots_held_seconds` show what each view holds, with the
  `DuckWakeSnapshotsHeld` alert. A reset empties the windowed views that read the view insert-only, which
  backfill again, and a reset to an expired epoch is refused. Running views move their position every
  `--position-refresh` (default `5m`) even when their inputs are idle.
- **Open-source release files.** The MIT `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`,
  issue and pull request templates, and the design documents under `docs/design/`. Release archives and the
  image carry `LICENSE` and `THIRD_PARTY_LICENSES.md`; CI checks every dependency's license (`cargo deny`) and
  that the notices match `Cargo.lock`.
- **Schema changes.** `create-view --replace` (`CREATE OR REPLACE`, `replace := true`) swaps a running view's
  definition in one transaction; the view recomputes from the start and keeps its status, worker and pin.
  `add-source-column` adds a column to a running source; views that read it keep their columns.
  `alter-view` changes a view's epoch interval, rows per epoch and limits without recomputing it.
  `create-view --if-exists update` (`if_exists := 'update'`) makes rerunning a deploy safe: nothing when the
  view is as requested, its settings altered in place when only they differ, and a refusal when its query
  changed. `create_view`'s result now starts with what happened (`created`, `unchanged`, `altered`,
  `replaced`).
- **Limits.** `max_state_rows` and `max_late_rows` per view fail that view alone, with the reason in the log.
  A worker whose epoch runs out of DuckDB memory starts again from its checkpoints with half as many rows per
  epoch instead of exiting. `--temp-dir` places each worker's spill files in a directory of its own; `--threads` sets DuckDB's
  threads, which what an epoch needs in memory grows with.
- **Monitoring.** `duckwake run --metrics ADDR` and `playground --metrics ADDR`. The worker process's CPU,
  memory, threads and open files (`process_*`), DuckDB's memory, spill and limit, and bytes read from Kafka;
  node_exporter in the monitoring profile for the hosts. Prometheus discovers the workers from the lake: each
  worker records its metrics address in its heartbeat row and serves every live worker's at `GET /sd`. New series: `duckwake_state_rows`, `duckwake_views_failed_total`,
  `duckwake_out_of_memory_total`, `duckwake_epoch_rows_cap`, `process_resident_memory_bytes`,
  `duckwake_build_info`, and each view's settings (`duckwake_epoch_interval_seconds`,
  `duckwake_watermark_delay_setting_seconds`, `duckwake_max_state_rows`). Prometheus alerting rules with unit
  tests, a Grafana dashboard and a compose `monitoring` profile in `deploy/`; [docs/alerts.md](docs/alerts.md)
  says what each alert means and what to do.
- **Failover.** Workers heartbeat into the lake; a dead worker's views move to the live workers after
  `--lease-timeout`. `drain` hands a worker's views away before it is taken out; a cleanly stopped worker keeps
  its views for `--stop-grace` (rolling restarts).
- **Rebalancing.** Views move from the busiest worker to the least loaded one by measured cost, with a
  cooldown; `start --worker W` pins a view.
- **Kafka security.** SASL (PLAIN, SCRAM-SHA-256/512) and TLS, including client certificates. Passwords are
  `env:` or `file:` references, never stored in the lake.
- **Object storage.** An `s3://` data path, with credentials from the standard AWS variables or the AWS chain.
- **Views over the lake.** Views read lake tables and other views through DuckLake's change feed, including
  tables whose rows are updated or deleted (F1, F2); weight-aware operators for sources that retract (C1).
- **CI.** Clippy, unit tests and every integration suite run on each pull request and push to `main`.
- Release binaries for Linux and macOS with SHA-256 checksums; the release checks the tag against the crate
  version.
  macOS arm64 builds run on GitHub's `macos-15` runners (`macos-14` was retired on 2026-11-02).

### Changed

- The test stack pins its Kafka and S3 images (Redpanda v26.2.7, MinIO by digest) instead of `latest`, so a
  broker release no longer changes what CI tests.
- `stop` and `pause` fence the view's worker at once, so `drop` and `reset` can follow straight away.
- Worker instances run with `preserve_insertion_order = false`, so inserts stream instead of buffering.

### Fixed

- A worker no longer exits when a view is dropped or replaced while one of its epochs stages. The epoch failed on
  the view's missing tables, and any staging error stopped the worker and every other view it ran; a `dbt run
  --full-refresh`, which drops and creates views while workers run them, set this off in CI. A view whose epoch
  fails to stage is now let go when the worker no longer holds it; a staging error on a view it still holds
  fails the worker as before.
- A view paused or stopped while its worker was starting it no longer keeps running. The worker starts views
  from a list of running views it read a moment before; it took a fresh lease without checking that the view
  still ran, undoing the fence the pause had set, so the paused view's epochs kept committing until the next
  refresh. After `stop` and then `reset` or `drop`, such an epoch could land on top of them. A worker now takes
  a lease only while the view is running, checked in the same transaction. CI's backfill suite with pipelined
  workers caught it: a paused view's position moved past snapshots maintenance had already expired.
- A worker no longer exits when a view it is starting is dropped meanwhile. It tried to mark the view failed,
  found no such view, and stopped with every other view it ran; a `dbt run --full-refresh`, which drops and
  creates views while workers run, set this off in CI. A view that fails to start is now marked failed only
  while it is still running on that worker; one dropped, stopped or taken over meanwhile is let go.
- A worker no longer hangs starting a view whose topic was just created. Kafka clients retry a partition
  without a leader for ever, and the worker waited on that in its own loop: it stopped heartbeating, other
  workers took its views, and it did not stop on SIGTERM. A reader now gives up after 5 s and the view waits
  for its topic, trying again at the next refresh.
- A view that stopped during staging (a limit, or an insert-only input that changed) made its worker drop the
  whole epoch, losing those events for the worker's other views.
- A view dropped while its worker had an epoch in flight failed that worker.
- A catalog stall that every worker shared (PostgreSQL slow or failing over) made workers declare each other
  dead and take over each other's views.
- A worker frozen inside its catalog commit blocked every other worker's commits until it was thawed.
- With `--pipeline`, a view over lake tables stopped and started again soon after could lose its staged rows to
  the drop queued when its earlier run left the worker, and fail the worker ("Table with name typed_l... does
  not exist"). Each run of a view now stages into tables of its own.
- A view dropped while its worker was staging an epoch that then failed (its input replaced, say) no longer
  stops the worker: there is no view left to mark failed, so the worker lets it go.
- `duckwake_verify` that failed (a view with no checkpoint yet, for one) left the SQL functions' connection
  inside a transaction, and every later call in that session failed with "cannot start a transaction within a
  transaction". It now rolls back.
- A worker whose view was reset, replaced, stopped or taken over while an epoch was staging no longer marks the
  view `failed` when that epoch hits an error: the error belonged to the fenced incarnation. Before, a reset
  that emptied a windowed view could leave it failed instead of recomputing.

## [0.1.0] - 2026-11-04

First release: continuous SQL views over Kafka into DuckLake, exactly once. Windowed and global aggregates,
filters, `DISTINCT` and `UNION ALL` as incremental circuits; late-data tables and dead letters; `verify` checks
a view against its batch recomputation; reset and replay. The `duckwake` CLI, the `duckwaked` server over Quack
with Prometheus metrics, the loadable extension, and the playground.

[Unreleased]: https://github.com/duckwake/duckwake/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/duckwake/duckwake/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/duckwake/duckwake/releases/tag/v0.1.0
