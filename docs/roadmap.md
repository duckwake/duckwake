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
      catalog stall. PostgreSQL down and a broker restart pass in CI (`scripts/catalog_outage_test.sh`,
      `scripts/broker_outage_test.sh`: workers wait them out, views verify); on the P3 box they run at soak rates.
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
- [x] **View names.** A view's id is its `schema.table`, so two schemas can each have a view of one table name;
      a lake from 0.2 is refused, not migrated ([design](design/view-names.md), `scripts/names_test.sh`).
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
  `scripts/backfill_test.sh`). (`MIN`/`MAX` in windows over deletes: built with C4,
  [#38](https://github.com/duckwake/duckwake/issues/38).) The rest of the SQL work is the SQL coverage plan below.
- **SQL coverage** ([SQL coverage](design/sql-coverage.md)): accept as much SQL in a view as can be kept exactly,
  and refuse the rest at create time with the reason and the way out. Each step its own PR, tested by the
  circuit harness over inserts, updates and deletes (in order and pipelined), and the operator steps also by
  `verify` in the crash suites. In order:
  1. **S0. Two holes, first.** Refuse functions whose value is not fixed by the rows (`stability` other than
     `CONSISTENT`: `now()`, `random()`, `uuid()`), which a replay or `verify` cannot reproduce. An error in a
     view's SQL caused by its data (a failed cast) fails that view, not the worker and every view on it.
     **Gate:** a suite runs a view that fails on a row beside a healthy one on one worker; the healthy one keeps
     committing and verifies, the failing one is `failed` with the error.
  2. **S1. Rewrites into SQL already accepted.** Subqueries in `FROM` and CTEs that project and filter,
     flattened into the outer query; `FILTER (WHERE ...)` inside aggregates as `CASE`; `UNION` as `UNION ALL`
     under `DISTINCT`; `ORDER BY` without `LIMIT` accepted and dropped; expressions over aggregates in windowed
     views. **Gate:** the harness over each rewritten form, which must equal its batch query.
  3. **S2. A coverage corpus**: queries that are accepted (with their results over fixed data) or refused (with
     their messages), run by `cargo test`, from which `docs/sql.md` lists what a view can contain.
  4. **S3. Joins.** Two lake inputs, through their snapshots (no state of their own); then two Kafka sources
     ([#31](https://github.com/duckwake/duckwake/issues/31)) and the interval join
     ([#32](https://github.com/duckwake/duckwake/issues/32)); outer, semi and anti joins, which `IN (SELECT ...)`
     and `EXISTS` become.
  5. **S4. Nested stateful queries in one view**: an aggregate over an aggregate, `UNION ALL` of aggregates or
     windows, `DISTINCT` over them, as stages of one circuit in one epoch and one transaction.
  6. **S5. Set operations and collecting aggregates.** `EXCEPT` and `INTERSECT`; `GROUPING SETS`, `ROLLUP`,
     `CUBE`, and `list`, `string_agg`, `mode` ([#30](https://github.com/duckwake/duckwake/issues/30));
     `arg_min`/`arg_max` once an `ORDER BY` breaks ties.
  7. **S6. `OVER` functions.** Top-k and `ROW_NUMBER`/`RANK` ([#35](https://github.com/duckwake/duckwake/issues/35)),
     `LAG`/`LEAD` and rolling aggregates ([#36](https://github.com/duckwake/duckwake/issues/36)), session windows
     ([#37](https://github.com/duckwake/duckwake/issues/37)).
- DuckLake tables and other views as sources: views over views and insert-only tables (F1,
  `scripts/chain_test.sh`), over tables whose rows change (F2, `scripts/retract_test.sh`), and backfill,
  retention and reset cascade (F3, `scripts/backfill_test.sh`) are done; shared and fused reads (F4) remain.
  Windows over tables whose rows change are built (C4, 2026-11-21, `scripts/retract_test.sh`): changes to open
  windows revise them, changes to closed ones are late corrections. An upsert window sink that keeps closed
  windows revisable for a `lateness`, as Flink and Feldera do, is the next step if users need it; bulk reruns
  are the partition-overwrite refresh below.
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
  row per key that other views read through the change feed (`scripts/upsert_test.sh`). Built 2026-11-21:
  Debezium topics (C3, [#42](https://github.com/duckwake/duckwake/issues/42)): `"format": "debezium"` sources
  read as upsert views, `event_time` optional (`FORMAT=debezium scripts/upsert_test.sh`, and end to end with
  PostgreSQL and Debezium's connector: `scripts/debezium_e2e_test.sh`). Built 2026-11-21: windows over updates and
  deletes (C4, [#38](https://github.com/duckwake/duckwake/issues/38)), with late corrections in the late-data table.
- Database sources without Kafka ([database CDC sources](design/database-cdc-sources.md)). Built 2026-11-21:
  PostgreSQL sources (D1, [#44](https://github.com/duckwake/duckwake/issues/44)): `create_source(name, 'postgres',
  ...)` mirrors tables into the lake from a logical replication slot, exactly once, with the snapshot, TOAST
  values, key changes and truncates (`scripts/pg_ingest_test.sh`). Built 2026-11-22: MySQL sources (D3,
  [#45](https://github.com/duckwake/duckwake/issues/45)): the binlog by GTID, a lock-free consistent snapshot
  (`scripts/mysql_ingest_test.sh`). Next, D4 ([#46](https://github.com/duckwake/duckwake/issues/46)), starting
  with **database sources as definitions** ([design](design/database-sources-as-views.md)): `create_source` holds
  only how to reach and read the database, and each table's copy is a view whose query only selects columns of
  one table (`SELECT order_id, status FROM shop.public.orders`), still read through one slot or binlog reader
  per source and written in one DuckLake transaction. In order:
  1. The catalog and compiler: sources without `tables` and `schema`, copy views and their rules.
  2. The ingest job writing copy views; a start that copies one table under a new snapshot and writes only the
     difference (which is also D4's "add a table to a running source"); a source reset that rejoins every copy
     by difference. **Gate:** `pg_ingest_test.sh` and `mysql_ingest_test.sh` create a copy view and stop and
     start another during the workload with the worker killed, and drop the slot and reset the source; the
     copies equal their tables' selected columns, and a view over a restarted copy sees only the changed rows.
  3. Lakes from 0.3 migrated in place, each mirror becoming a copy view, with a gate that upgrades a lake made
     by the 0.3.0 binary.
  4. Then the rest of D4: chunked backfill for large tables (which serves first copies and starts alike) and
     schema changes.
- **Kafka message formats** ([#49](https://github.com/duckwake/duckwake/issues/49),
  [design](design/kafka-formats.md)): a source's `format` (the encoding) and `envelope` (`none`, `debezium`)
  apart, with `"format": "debezium"` kept. In order:
  1. **K1.** `text`, `bytes` and `csv`; `_timestamp` (usable as event time), `_headers`, `_partition`,
     `_offset` and `_key` as columns a view can select. **Gate:** the decode suite with text and CSV sources
     over hostile payloads, and the crash suite with a text source windowed on `_timestamp`.
  2. **K2.** Avro with a schema registry (Redpanda's, in the compose stack): the registry's wire format, a
     reader schema fixed at `create_source` (FORWARD or FULL compatibility), schemas cached in the lake, a
     registry outage waited out; JSON with the registry's header. **Gate:** `FORMAT=avro` crash and upsert
     suites, a compatible schema change mid-stream, a corrupt message as a dead letter, an unresolvable schema
     failing the view, the registry stopped and restarted.
  3. **K3.** Protobuf, the same way.
  4. **K4.** Avro and Protobuf keys for upsert views, and Debezium with the Avro converter end to end.
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
