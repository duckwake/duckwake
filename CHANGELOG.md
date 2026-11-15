# Changelog

All notable changes to DuckWake. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/) as described in [docs/releasing.md](docs/releasing.md).

## [Unreleased]

### Added

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

### Changed

- `stop` and `pause` fence the view's worker at once, so `drop` and `reset` can follow straight away.
- Worker instances run with `preserve_insertion_order = false`, so inserts stream instead of buffering.

### Fixed

- A view that stopped during staging (a limit, or an insert-only input that changed) made its worker drop the
  whole epoch, losing those events for the worker's other views.
- A view dropped while its worker had an epoch in flight failed that worker.
- A catalog stall that every worker shared (PostgreSQL slow or failing over) made workers declare each other
  dead and take over each other's views.
- A worker frozen inside its catalog commit blocked every other worker's commits until it was thawed.
- A worker whose view was reset, replaced, stopped or taken over while an epoch was staging no longer marks the
  view `failed` when that epoch hits an error: the error belonged to the fenced incarnation. Before, a reset
  that emptied a windowed view could leave it failed instead of recomputing.

## [0.1.0] - 2026-11-04

First release: continuous SQL views over Kafka into DuckLake, exactly once. Windowed and global aggregates,
filters, `DISTINCT` and `UNION ALL` as incremental circuits; late-data tables and dead letters; `verify` checks
a view against its batch recomputation; reset and replay. The `duckwake` CLI, the `duckwaked` server over Quack
with Prometheus metrics, the loadable extension, and the playground.

[Unreleased]: https://github.com/duckwake/duckwake/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/duckwake/duckwake/releases/tag/v0.1.0
