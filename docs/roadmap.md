# Roadmap

Where DuckWake stands and what it needs before production. The stages run in order, and each ends at a gate
that must pass before the next starts. The same plan is in the design doc's
[Delivery plan](https://github.com/duckwake/duckwake/tree/main/docs/design).

## Done (v0.1.0)

- Engine: micro-batch views over Kafka, one DuckLake transaction per epoch, exactly-once output; lease fencing,
  `reset`, maintenance, Arrow JSON decode (the default); parallel staging, pipelined commits, shared readers and
  lean commits behind worker flags. See [performance.md](performance.md).
- Front-ends: the `duckwaked` server (Quack + metrics), the loadable extension, the CLI and playground.
- Tests: crash, fencing, reset, retention, decode, catch-up, shared-reader, server, extension and composed suites,
  each verified against a batch recomputation. See [testing.md](testing.md).
- Release pipeline: extension repository for Linux and macOS (amd64, arm64) and the `duckwaked` image on each tag.

## P1. Safety net

- [ ] CI runs the unit tests and the integration suites on every push, with PostgreSQL, Redpanda and MinIO as
      services. Today CI only builds, and the suites run on a laptop.

**Gate:** every suite is green in CI, and a failing suite blocks merging.

## P2. Runs in a real environment

- [ ] **Automatic failover.** When a worker dies, its views stop until someone runs `start --worker` elsewhere.
      Add worker heartbeats, lease expiry and reassignment of orphaned views to live workers.
- [ ] **Kafka SASL and TLS.** Only plaintext brokers work today; most production clusters require auth.
- [ ] **Object storage.** Data files on S3 or GCS, tested against MinIO. Every test so far used local disk.
- [ ] **Secrets.** Catalog passwords live in connection strings, env vars and stored definitions; read them from
      a secret store or file instead. Quack has one shared token and no TLS of its own (use a TLS proxy).

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
- [ ] **Alerts and a dashboard** for the design's thresholds (freshness, lag, epoch duration, commit latency
      and conflicts, restarts) on top of `/metrics`.
- [ ] **Schema changes.** `OR REPLACE` for views; adding source columns without recreating.
- [ ] **Limits.** Per-view caps on state size and late-data growth; worker memory limits enforced and tested.
- [ ] **Distribution.** A public image and extension repository, versioning and a changelog.

**Gate:** an on-call engineer can run the drills from the runbook alone.

## After production, driven by feedback

- `CREATE CONTINUOUS VIEW` / `CREATE SOURCE` DDL (needs a C++ parser shim; DuckDB's C API has no parser hooks)
- More aggregates: `approx_count_distinct`, quantiles, `list` (DuckDB's exported aggregate states)
- `DISTINCT` over windows and aggregates, `count(DISTINCT)`, `MIN`/`MAX` in windows over deletes,
  stream–stream joins
- DuckLake tables and other views as sources: views over views and insert-only tables (F1,
  `scripts/chain_test.sh`) and over tables whose rows change (F2, `scripts/retract_test.sh`) are done;
  backfill, retention and reset cascade (F3), and shared and fused reads (F4) remain. Windows over tables
  whose rows change need windows that stay open for `lateness` (C4).
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
- Avro and Protobuf with a schema registry; Kafka keys and headers
- Splitting one topic's partitions across workers
- A Windows build
- A signed community extension (`INSTALL duckwake FROM community`), once DuckDB's Rust extension path supports it
