# Roadmap

Where DuckWake stands and what it needs before production. The stages run in order, and each ends at a gate
that must pass before the next starts. The original plan is the
[design doc's delivery plan](design/streaming-extension.md#delivery-plan).

## Done (v0.1.0)

- Engine: micro-batch views over Kafka, one DuckLake transaction per epoch, exactly-once output; lease fencing,
  `reset`, maintenance, Arrow JSON decode (the default); parallel staging, pipelined commits, shared readers and
  lean commits behind worker flags. See [performance.md](performance.md).
- Front-ends: the `duckwaked` server (Quack + metrics), the loadable extension, the CLI and playground.
- Tests: crash, fencing, reset, retention, decode, catch-up, shared-reader, server, extension and composed suites,
  each verified against a batch recomputation. See [testing.md](testing.md).
- Release pipeline: extension repository for Linux and macOS (amd64, arm64) and the `duckwaked` image on each tag.

## P1. Safety net

- [x] CI runs clippy, the unit tests and every integration suite on each pull request and push to `main`, against
      PostgreSQL, Redpanda (open and authenticated listeners) and MinIO (`.github/workflows/ci.yml`, about
      12 minutes). Blocking merges on `ci-passed` needs branch protection, which GitHub's Free plan does not
      offer for private repositories: the org must upgrade or the repository go public.

**Gate:** every suite is green in CI, and a failing suite blocks merging.

## P2. Runs in a real environment

- [x] **Automatic failover.** Workers heartbeat into the lake; a view whose worker dies, freezes or stops
      moves to the live workers within the lease timeout and continues from its checkpoint
      (`scripts/failover_test.sh`).
- [x] **Rebalancing.** The busiest worker hands views to the least loaded, weighted by each view's measured
      cost and preferring workers that read the view's source, with a cooldown per view; a clean stop is a
      restart (its views wait `--stop-grace`), `drain` takes a worker out, `start --worker W` pins
      (`scripts/rebalance_test.sh`). Workers of different sizes are treated alike.
- [x] **Kafka SASL and TLS.** SASL PLAIN and SCRAM, TLS with custom CAs and client certificates; passwords
      are `env:` or `file:` references (`scripts/kafka_auth_test.sh`). OAUTHBEARER and Kerberos are not built.
- [x] **Object storage.** Data files on `s3://` (S3 and S3-compatible stores), credentials from the standard AWS
      variables or the AWS credential chain; the suites run against MinIO with `LAKE_STORAGE=s3`.
- [x] **Secrets.** Nothing secret is stored in the lake: Kafka passwords are `env:`/`file:` references, the
      catalog password comes from `PGPASSWORD` or `.pgpass` (a password in the connection string warns), S3
      keys from the AWS variables or roles, and the Quack token may be a `file:` reference. Still one shared
      Quack token and no TLS of its own (use a TLS proxy); no Vault or cloud secret manager integration.

**Gate:** kill a worker and its views move to another within a bounded time, with exact output; the suites
pass against authenticated Kafka and MinIO.

## P3. Proven under load and failure

- [ ] **Soak.** 24 h+ at production rates with the four worker flags and maintenance on, against managed
      PostgreSQL and S3. Memory, catalog size and file count must stay flat.
- [ ] **Failure drills** beyond `kill -9`: PostgreSQL down, a Kafka broker restart, S3 errors, disk full, a long
      catalog stall.
- [ ] **Upgrades.** A new version on a running lake: control-schema migrations, rolling restarts, pinned DuckDB
      and extension versions.
- [ ] **Sizing.** Views per vCPU and events/s per worker on the 4 vCPU / 16 GB reference machine; recovery time
      with 1 GB of state.

**Gate:** every drill recovers with exact output, the soak stays flat, and the four worker flags become the
default.

## P4. Operable

- [ ] **Runbook.** Deploy, scale, move views between workers, recover from a bad epoch (`reset`), handle a
      failed view, rotate tokens.
- [x] **Alerts and a dashboard** for the design's thresholds (freshness, lag, epoch duration, commit latency
      and conflicts, restarts) on top of `/metrics`. Prometheus rules with `promtool` unit tests, a Grafana
      dashboard and a compose profile in `deploy/`; [alerts.md](alerts.md) says what to do for each. To tune with
      the soak's numbers (P3).
- [x] **Schema changes.** `OR REPLACE` for views; adding source columns without recreating; `alter-view` for
      settings (`scripts/schema_test.sh`).
- [x] **Limits.** Per-view caps on state size and late-data growth; worker memory limits enforced and tested
      (`scripts/limits_test.sh`).
- [x] **Distribution.** Versioning and a changelog ([releasing.md](releasing.md)); release binaries with
      checksums, the multi-arch image and the extension repository. Making the image and extension public is
      the organization's call: ghcr.io package visibility, and `PUBLISH_EXTENSIONS` for the website.

**Gate:** an on-call engineer can run the drills from the runbook alone.

## After production, driven by feedback

- **SQL authoring through dbt, not DDL.** A `CREATE CONTINUOUS VIEW` parser works only in `duckwaked` and
  `duckwake sql`, through DuckDB's unstable C++ API (a spike proved it; DDL stays long term, below), so instead a dbt package,
  dbt-duckwake, on dbt-duckdb: `streaming_source` and `streaming_view` models become `duckwake_*` calls,
  with dbt's `ref()` order, tests and docs. A plain `dbt run` never recomputes a view: unchanged does nothing,
  settings go through `alter_view`, a changed query fails until `--full-refresh`. In order:
  1. `create_view` reports what changed (`if_exists := 'fail' | 'update' | 'replace'`), also in the CLI.
  2. Backfill of new views over lake inputs (F3, below). Built 2026-11-15 (`scripts/backfill_test.sh`).
  3. The package, an example project and a CI suite against the compose stack. Built 2026-11-15 in `dbt/`
     (`scripts/dbt_test.sh`). Later: the package in a repository of its own for dbt Hub.
  4. **Zero-downtime replace.** Today `--full-refresh` drops a view's table and recomputes it, so queries see
     it empty or partly filled until it catches up, minutes to hours for a large view. Instead, the old view
     keeps running and serving while the new definition is built beside it as `<view>__next`, backfilling its
     inputs; once it has caught up, one DuckLake transaction swaps them (table, state, checkpoints, lease) and
     drops the old copy, so readers go from the complete old table straight to the complete new one. The
     first version covers views no other view reads, the ones dashboards query: a reader's position in the
     old table does not carry over to the new one, so a view with readers keeps today's rule (select them
     with `-s view+`; they are dropped and created again). Engine: `create_view(..., if_exists := 'rebuild')`
     and the swap; dbt: a model config that picks it on `--full-refresh`. **Gate:** a suite replaces a running
     view under load while a reader polls its table; no poll ever sees it empty or partly built, the swap
     survives a worker killed before, during and after it, and the new table equals its batch recomputation.

  Design, and how dbt-risingwave does it, in [dbt-duckwake](design/dbt-duckwake.md).
- More aggregates. Built 2026-11-16: the variances and standard deviations, `bool_and`/`bool_or`,
  `count(DISTINCT)` in `GROUP BY` views and `HAVING` in windowed views. Built 2026-11-17
  ([#29](https://github.com/duckwake/duckwake/issues/29)): `approx_count_distinct`, `median` and the exact
  quantiles, `count(DISTINCT)` in windows, `covar_*`, `corr` and `regr_*` (`crates/circuit/src/tests/views.rs`,
  `scripts/backfill_test.sh`). Next, in order: stream–stream joins
  ([#31](https://github.com/duckwake/duckwake/issues/31)); top-k and `ROW_NUMBER`
  ([#35](https://github.com/duckwake/duckwake/issues/35)); `list`/`string_agg`/`mode` and grouping sets
  ([#30](https://github.com/duckwake/duckwake/issues/30)); `DISTINCT` over windows and aggregates; `MIN`/`MAX`
  in windows over deletes ([#38](https://github.com/duckwake/duckwake/issues/38)). `arg_min`/`arg_max` wait
  until a tie can be checked against the batch query
- DuckLake tables and other views as sources: views over views and insert-only tables (F1,
  `scripts/chain_test.sh`), over tables whose rows change (F2, `scripts/retract_test.sh`), and backfill,
  retention and reset cascade (F3, `scripts/backfill_test.sh`) are done; shared and fused reads (F4) remain. Windows over tables
  whose rows change need an upsert window sink that keeps state for `lateness`, as Flink and Feldera do;
  what arrives later goes to the late-data table (C4), and bulk reruns are the partition-overwrite refresh
  below.
- **Partition-overwrite refresh**, right after F2's weights, for lake tables whose recent partitions are
  rerun to correct data (for example, the last 3 days rewritten with delete + insert or `INSERT OVERWRITE`).
  A view whose table is partitioned like its input (its output carries the input's partition column, such as
  `date`) is refreshed by partition instead of incrementally:
  1. From the change feed in (P, S], find the partition values the epoch touched.
  2. In the epoch transaction, delete those partitions from the view's table and insert the view's batch
     query over the input `AT (VERSION => S)`, restricted to those partitions; the checkpoint moves to S.

  It runs any SQL the batch query can (window functions, any join), needs no window state kept for
  `lateness`, and stays exactly-once through the same snapshot position. It costs one batch query over the
  rerun partitions per rerun. A view picks it at create time; `create_view` refuses it when the view's
  output does not carry the input's partition column. **Gate:** a generator reruns the last 3 days of a
  partitioned table with changed, removed and added rows; after every rerun, through crashes, the view's
  table equals its batch query over the table at the checkpoint's snapshot.
- Change data ([CDC sources](design/cdc-sources.md)). Built 2026-11-20: upsert views over Kafka topics (C2,
  [#41](https://github.com/duckwake/duckwake/issues/41)): `upsert_key` and `upsert_version` on
  `create_view`; the message key, tombstones as deletes; a plain DuckLake table with one
  row per key that other views read through the change feed (`scripts/upsert_test.sh`). Next: Debezium topics
  (C3), then late changes to closed windows (C4).
- Avro and Protobuf with a schema registry; Kafka headers
- Splitting one topic's partitions across workers
- A Windows build
- A signed community extension (`INSTALL duckwake FROM community`). Built 2026-11-17
  ([duckdb/community-extensions#3654](https://github.com/duckdb/community-extensions/pull/3654)) for Linux (amd64,
  arm64) and `osx_arm64`; Windows waits for [#53](https://github.com/duckwake/duckwake/issues/53)

## Long term

- **`CREATE CONTINUOUS VIEW` DDL**, mapped 1:1 onto the `duckwake_*` functions (`CREATE SOURCE`,
  `CREATE [OR REPLACE] CONTINUOUS VIEW`, `EXPLAIN` and `DROP CONTINUOUS VIEW`), only if users still ask for it
  once dbt-duckwake exists. DuckDB's C API has no parser hooks, so it needs a C++ parser shim over DuckDB's
  internal API, which carries no stability promise, and it works only in `duckwaked` and `duckwake sql`, not
  the loadable extension. Syntax in the [design doc's SQL surface](design/streaming-extension.md#sql-surface).
