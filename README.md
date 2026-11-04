# DuckWake

Continuous SQL over Kafka into DuckLake, exactly once. DuckWake is a proof of concept of the
[DuckDB Streaming Extension design](https://github.com/duckwake/duckwake/tree/main/docs/design), where the extension is
called `duckwake` and its SQL functions are prefixed `duckwake_`. It runs continuous SQL over a Kafka topic in micro-batches. Each epoch commits sink rows, window state and
source offsets to DuckLake in one transaction, which is what gives exactly-once output.

It covers the Phase 0 spike and a working slice of Phases 1–2:
- Kafka source, stateless views, and tumbling and hopping window aggregates.
- Watermarks, late-data and dead-letter tables.
- DuckLake checkpoints, lease fencing, `reset` via time travel, and built-in maintenance.

It ships three front-ends over the same SQL function API (`duckwake_create_view`, `duckwake_jobs()`, ...):
- **`duckwaked`**, the server: a worker plus a DuckDB with the lake attached and the `duckwake_*` functions,
  served over [Quack](https://duckdb.org/docs/2.0/quack/overview), with Prometheus metrics. Also a Docker image.
- **`duckwake.duckdb_extension`**: `INSTALL` and `LOAD` it in your own DuckDB 2.0 for the same functions and
  a supervised worker in that process; add `quack_serve` and that DuckDB is a DuckWake server.
- **`duckwake`**, the CLI: a command per function, plus a playground UI.

`CREATE CONTINUOUS VIEW` DDL is not built: DuckDB's C API has no parser hooks, so it needs a C++ shim.

## Results

Run on a MacBook (Apple Silicon, 10 cores) against DuckDB v2.0.0 and DuckLake 1.1, with a PostgreSQL 16
catalog and Redpanda, all from `docker-compose.yml`.

| Gate in the design | Test | Result |
| --- | --- | --- |
| Commit is atomic (Phase 0) | `examples/phase0_*.rs` | One transaction over sink, state, lease and checkpoint tables = one snapshot. A zombie commit is rejected. Time travel to an epoch's snapshot works. |
| `kill -9` at any point, no duplicates or gaps | `scripts/crash_test.sh` | 120k events with late and malformed rows. 12–16 crashes: external `kill -9` and aborts injected at poll, transaction begin, mid-transaction, before and after COMMIT, and maintenance. Sinks match a batch recomputation exactly. |
| Zombie worker after failover | `scripts/fencing_test.sh` | w1 commits epochs 1–3, then stalls 15 s with its epoch-4 transaction open. The views move to w2, which continues from epoch 4. w1's commit is rejected. The result verifies exactly. |
| 5k events/s, p95 latency < 30 s | `scripts/throughput_test.sh` | 600k events of ~1 KB at 5,000/s, 10 s epochs. Per epoch (~50k rows): ~0.45 s poll+stage, ~0.15 s commit. Lag stays ~0. An event is committed at most one epoch interval plus ~0.6 s after it arrives; windowed results also wait for the window to close plus the watermark delay. |
| Matches batch SQL | `verify` in every script | Multiset equality of each sink with the view run as one batch query over everything committed. Late rows and dead letters are also checked. |
| Matches batch SQL after retention | `scripts/retention_test.sh` | The first quarter of every partition is trimmed. Windowed and append views verify over the retained range. The global aggregate is refused, because it depends on deleted events. A sink row deleted inside the range is caught. |
| `duckwake_reset` | `scripts/reset_test.sh` | Rewind to epoch 6, replay, and the sinks end identical (verified). |
| Parallel processing (worker flags) | `scripts/scale_bench.sh`, all suites with `WORKER_FLAGS` | 16 views on one worker drain a backlog at 40.5k events/s with all four flags, against 21.7k without (1.87×). At a steady 20k events/s, read + stage per epoch drops from 1.55 s to 0.30 s, the commit from 0.66 s to 0.33 s, and CPU from 89% to 46%. Every suite passes with the flags off, with `--lean-commit` alone, with the other three together, and with all four. |

Not measured: the 4 vCPU / 16 GB reference box, the 24 h soak, and recovery time for 1 GB of state.

## Quick start

```sh
docker compose up -d --wait            # PostgreSQL (catalog) on :55432, Redpanda on :19092
cargo build --release                  # first build compiles DuckDB (~6 min)
B=target/release/duckwake

$B init
$B create-source clicks --options '{"brokers":"localhost:19092","topic":"clicks","event_time":"ts",
  "watermark_delay":"5 seconds","columns":{"ts":"TIMESTAMP","game_id":"VARCHAR","user_id":"VARCHAR","amount":"DECIMAL(18,2)"}}'
$B create-view analytics.clicks_per_min --late-data lake.analytics.clicks_late \
  --epoch-interval 10s \
  --sql "SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
         FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL"
$B start clicks_per_min --worker w1
$B run --worker w1 &                   # the worker; one structured log line per job per epoch
$B produce --rate 2000 --late-every 500 --bad-every 700
$B jobs                                # status, epoch, watermark, freshness, lag, totals
$B sql "FROM lake.analytics.clicks_per_min ORDER BY window_start DESC LIMIT 5"
$B verify clicks_per_min               # differential exactly-once check
```

A view is named by the DuckLake table it maintains (`analytics.clicks_per_min`; a bare name means the
`main` schema), and `create-view` fails if that table already exists. Its job id is the table name, so the
other commands take either `clicks_per_min` or `analytics.clicks_per_min`. `drop` removes the view's table
too (`--keep-table` keeps it); a late-data table is kept. The old `--sink` flag still works but is
deprecated. Control data lives in `lake.__duckwake`, and output rows carry `_duckwake_epoch`. A lake created
before that rename (`__stream`, `_stream_epoch`) is migrated the first time a new binary opens it.

Sources decode JSON with Arrow (`"decode": "auto"`, the default) when every column is TIMESTAMP, VARCHAR,
BOOLEAN, an integer, DOUBLE or DECIMAL. A message whose JSON Arrow can't read exactly as DuckDB's JSON
functions would goes through the SQL decode instead, so the rows and dead letters are the same either way.
`"decode": "sql"` decodes everything in SQL; `"decode": "arrow"` refuses a source whose column types Arrow
can't decode.

`run` (and `playground`) take four flags from the design's
[parallel processing plan](https://github.com/duckwake/duckwake/tree/main/docs/design). They are off by
default until a 24 h soak with them on has passed:
- `--stage-threads N` stages views in parallel, each thread on its own DuckDB connection to the same
  instance (`0` = half the cores).
- `--pipeline` stages the next epoch while the previous one commits, on a thread and connection of its own.
  Commits stay in order and at most one epoch is staged ahead. A global aggregate reads the previous
  epoch's new state from memory until that epoch has committed.
- `--shared-readers` gives the views of one source that are at the same position one Kafka reader, one
  decode and one staged copy of the events. A view at another position (new, reset, or behind) reads on
  its own until it reaches the shared reader's position, then joins it.
- `--lean-commit` skips the epoch transaction's writes that have nothing to write (no window closing, no
  late rows, no dead letters, no changed groups), and writes all views' checkpoints and dead letters with
  one statement each.

### Server: `duckwaked`

```sh
cargo build --release
target/release/duckwaked --worker w1 --quack-token secret      # Quack on quack:localhost:9494, metrics on :9464
# or in Docker, with its own catalog database and data volume:
docker compose --profile server up -d --build                 # token: duckwake-dev-token
```

`duckwaked` (the same as `duckwake serve`) runs a worker and serves SQL over Quack. In the DuckDB 2.0 CLI,
attach it and `CONNECT`; every statement then runs on the server:

```sql
ATTACH 'quack:localhost:9494' AS dw (TOKEN 'secret');
CONNECT dw;
CALL duckwake_create_source('clicks', 'kafka', '{"brokers":"localhost:19092","topic":"clicks","event_time":"ts",
  "columns":{"ts":"TIMESTAMP","game_id":"VARCHAR","user_id":"VARCHAR","amount":"DECIMAL(18,2)"}}');
CALL duckwake_create_view('analytics.clicks_per_min',
  'SELECT window_start, game_id, count(*) AS n FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL',
  epoch_interval := '10s', late_data := 'analytics.clicks_late');
CALL duckwake_start('clicks_per_min');                         -- on the server's own worker
FROM duckwake_jobs();
FROM lake.analytics.clicks_per_min;
DISCONNECT;                                                    -- back to the local session
```

Clients that prepare every statement before running it (the Rust `duckdb` crate, for one) fail under
`CONNECT` with "PREPARE succeeded but the prepared statement was not registered". From those, send each
statement with `quack_query`, which runs locally and ships one SQL string to the server:
`FROM quack_query('quack:localhost:9494', 'FROM duckwake_jobs()', token = 'secret')`.
`duckwake sql --server quack:localhost:9494 --token secret "CALL ...; FROM duckwake_jobs()"` does that for you.

| Function | Does |
| --- | --- |
| `duckwake_create_source(name, kind, options_json)` | register a Kafka source |
| `duckwake_create_view(name, sql, late_data :=, epoch_interval :=, max_rows :=)` | create the table `name` and its job |
| `duckwake_start(view, worker :=)` / `duckwake_pause(view)` / `duckwake_stop(view)` | lifecycle; `start` defaults to the server's worker |
| `duckwake_reset(view, to_epoch)` / `duckwake_drop(view, keep_table :=)` | rewind and replay / remove |
| `duckwake_jobs()` / `duckwake_checkpoints(view, n :=)` / `duckwake_sources()` | status, history, sources |
| `duckwake_verify(view)` | the differential exactly-once check |
| `duckwake_wait(view, timeout_s :=)` | block until the view has committed what its topic holds now |
| `duckwake_metrics()` | the worker's numbers as rows: `(metric, worker, view, value)` |
| `duckwake_worker_status()` | the worker: running, restarts, last error and when |

Quack serves plain HTTP on localhost only: bound to another address it switches to HTTPS with a self-signed
certificate, so clients must use that address over TLS. To reach the server from other machines, keep it on
localhost and put a TLS reverse proxy in front, as [Quack's security notes](https://duckdb.org/docs/2.0/quack/security.html)
recommend; the image and the compose profile bind Quack and metrics to localhost.

Actions return `(view, result)`. `GET :9464/metrics` serves per-view freshness, watermark delay, lag, epoch,
stage time and row, late-row and dead-letter counters, plus commit time and conflicts, in the Prometheus
format. A worker that fails restarts from its last checkpoint after 5 s, and the server stays up
(`duckwake_worker_status()` shows the restarts and the last error). `--quack none`
or `--metrics none` turns either off; the worker flags (`--stage-threads`, `--pipeline`, ...) are those of `run`.

### Extension: `LOAD duckwake` in your own DuckDB

DuckWake is a DuckDB extension, and Quack is another: a DuckDB you run can load both and become a DuckWake
server. `duckwaked` (above) is that same setup packaged as one supervised binary.

```sh
scripts/package_extension.sh        # builds it and lays out dist/extensions/v2.0.0/<platform>/ as an extension repository
duckdb -unsigned                    # the DuckDB 2.0 CLI (the extension is not signed yet)
```

```sql
INSTALL duckwake FROM '/path/to/ducklake_streaming/dist/extensions';   -- or an HTTP(S) URL serving that folder
LOAD duckwake;
CALL duckwake_connect('ducklake:postgres:dbname=lake host=localhost port=55432 user=lake password=lake',
                      data_path := '.lake/data/');     -- or set DUCKWAKE_CATALOG / DUCKWAKE_DATA_PATH
CALL duckwake_worker_start(worker := 'w1', pipeline := true, metrics := '127.0.0.1:9464');
ATTACH 'ducklake:postgres:dbname=lake host=localhost port=55432 user=lake password=lake' AS lake (DATA_PATH '.lake/data/');

-- Use it here ...
CALL duckwake_start('clicks_per_min');
CALL duckwake_wait('clicks_per_min');
FROM lake.analytics.clicks_per_min;

-- ... or serve it: another DuckDB attaches with ATTACH 'quack:localhost:9494' ... ; CONNECT, as for duckwaked.
INSTALL quack; LOAD quack;
CALL quack_serve('quack:localhost:9494', token = 'secret');
```

The worker runs in that DuckDB's process for as long as it lives; keep the process running (a terminal,
tmux, systemd) to keep the views fresh. If the worker fails (a view's lookup table dropped, Kafka or the
catalog unreachable) it restarts from its last checkpoint 5 s later; a view that can no longer compile is
marked `failed` and the others carry on. `CALL duckwake_worker_stop()` stops it.

Extensions get no handle to the host database, so the functions run on a private DuckDB in the same process
that attaches the lake (`duckwake_connect`); attach the same lake in your session to read the tables, as
`duckwake_connect`'s result spells out. The extension is the engine built against the host's DuckDB
library (DuckDB's V1 C API, pinned to v1.5.6, which DuckDB 2.0 loads). `.github/workflows/release.yml`
builds the repository for Linux and macOS (amd64, arm64) on a tag; it has not run yet, since this
repository has no remote. Until DuckWake is a signed community extension, hosts need `-unsigned`.

| Extension-only function | Does |
| --- | --- |
| `duckwake_connect(catalog, data_path :=)` | pick the lake |
| `duckwake_worker_start(worker :=, stage_threads :=, pipeline :=, shared_readers :=, lean_commit :=, metrics :=)` | run a supervised worker in this process; `metrics` also serves `GET /metrics` |
| `duckwake_worker_stop()` | stop it after its current epoch |

### Playground

```sh
docker compose up -d --wait
cargo run --release -- playground      # then open http://localhost:8787 and click "Load demo"
```

A local web UI with an in-process worker (`playground`). It lets you:
- create sources (the Kafka topic is created for you), run a test-data generator with late and malformed events, or send your own JSON;
- create lake tables, e.g. dimension tables for lookup joins;
- create continuous views from templates (unsupported SQL is rejected with the compiler's message);
- start, pause, stop, reset and drop views, and run Verify;
- pick a source's JSON decode (auto, arrow or sql) and see, per epoch, how many events went through the
  SQL decode (with Arrow, only the ones it can't read exactly as DuckDB would);
- watch epochs, watermarks, open windows, late rows and dead letters;
- query anything in the SQL console (`schema.table` means the lake) and follow the Activity log;
- query a source by name (`SELECT * FROM clicks`): each query reads the newest 100,000 events of the
  topic into a temporary table with the source's columns plus `_partition`, `_offset`, `_error` and
  `_payload`. It's a snapshot for exploring and debugging, not a stream. `duckwake sql` does the same.

The demo creates a `clicks` source at 300 events/s, a `dim.games` table, and four views: a tumbling window with a lookup join, a hopping window, a filter, and a global aggregate with `HAVING` (an upsert sink: one row per group, replaced when it changes). The playground uses the same lake as the CLI, so `duckwake jobs` sees its views. The test scripts use their own catalog database (`duckwake_test`) and data path, and never touch the playground's lake.

### Tests

The test scripts reset the lake and the topic themselves:
`scripts/crash_test.sh`, `scripts/fencing_test.sh`, `scripts/throughput_test.sh`, then `scripts/reset_test.sh`.
`scripts/retention_test.sh`, `scripts/decode_test.sh` and `scripts/catchup_test.sh` are standalone.
`DECODE=sql|arrow|auto` picks the source's decode for every script that uses the `clicks` source
(default `auto`).
`cargo run --release -p duckwake_core --example phase_a_arrow_decode` is Phase A of the Arrow ingest plan.
It decodes the same messages with today's SQL decode and with `decode.rs` (Arrow, falling back to SQL for
any form it can't match exactly), checks the outcomes are identical on a hostile corpus, and times both
on 50k soak-shaped events.
`scripts/decode_test.sh` is the same differential in the engine: one topic of hostile messages read by a
source that decodes in SQL and one that decodes in Arrow, each feeding the same view; sinks and dead letters
must be equal row for row. `scripts/catchup_test.sh` times a worker draining a backlog in each mode.
`POINTS="before_commit after_commit" scripts/crash_test.sh` limits the crash test to chosen injection points.
`WORKER_FLAGS="--stage-threads 4 --pipeline --shared-readers --lean-commit"` runs any test's workers with
the parallel-processing flags; with `--pipeline` the crash test also aborts while an epoch is staged ahead
of a commit (`staged_ahead`). `scripts/shared_reader_test.sh` creates a view late and rewinds another while
events stream in; both must catch up, join the shared reader and verify.
`scripts/composed_test.sh` uses the DuckDB 2.0 CLI on both ends: a plain DuckDB loads the extension, runs a
worker and serves itself over Quack; a second one attaches with `CONNECT`, creates four views, drops a lookup
table to make the worker fail and restart, recreates it and verifies everything.
`scripts/serve_test.sh` drives a `duckwaked` entirely over Quack: create, start, pause, reset and verify the
three test views, and check `/metrics`. `scripts/extension_test.sh` does the same in one DuckDB session with
the extension loaded and a worker inside it.
`scripts/scale_bench.sh` runs 1, 2, 4, 8 and 16 views (the playground's mix) on one worker at a fixed
rate, or with `CATCHUP=1` through a backlog, and appends epoch period, freshness, stage and commit time
(statements vs `COMMIT`), CPU and memory to `.lake/bench/*.csv`; `FLAGS` and `LABEL` name the variant.

`scripts/soak_test.sh` runs the design's soak test: 24 h at 5,000 events/s of ~1 KB with maintenance
enabled. It uses its own catalog database (`soak`), data path (`.lake/soak`) and topic, so it does not
touch the playground. Every minute it samples lag, freshness, snapshot count, catalog size, inlined
rows, Postgres dead tuples, file counts and worker memory. `scripts/soak_report.sh` summarizes a run by
hour, during or after it; `BUCKET="10 minutes"` makes the buckets finer. To stop early, run
`kill "$(cat .lake/soak/run.pid)"`. `DURATION`, `RATE`, `EPOCH` and the maintenance cadences are
environment variables. Snapshots are kept for 12 h by default, so a 24 h run reaches steady state
halfway through. The topic is capped at 1 GiB per partition, because Docker's VM disk is small.

Once retention has deleted a topic's oldest events, `verify` checks the retained range only. It starts
from the oldest checkpoint whose offsets are all still retained, and compares the output that depends only
on later events. For an append sink, that is the rows of later epochs. For a windowed sink, it is the
windows that start after every event time the job had seen by that checkpoint. The report's `from_epoch`
says which checkpoint was used. A global aggregate depends on every event, so it cannot be verified once
its topic has been trimmed.

| SQL function (server, extension) | CLI |
| --- | --- |
| `duckwake_create_source(name, kind, options)` | `create-source NAME --kind kafka --options JSON` |
| `duckwake_create_view(name, sql, options)`, where `name` is the view's DuckLake table | `create-view SCHEMA.TABLE --sql [--late-data] [--checkpoint ducklake] [--epoch-interval] [--max-rows]` |
| `duckwake_start` / `duckwake_stop` / `duckwake_pause` | `start NAME --worker W` / `stop` / `pause` |
| `duckwake_jobs()` / `duckwake_checkpoints(view)` | `jobs` / `checkpoints NAME` |
| `duckwake_reset(view, to_epoch)` | `reset NAME --to-epoch N` |
| `TUMBLE(src, col, size)` / `HOP(src, col, size, slide)` | same, inside the view SQL |

## How it maps to the design

| Design component | Code |
| --- | --- |
| Worker with an embedded DuckDB 2.0 and the lake attached | `lake.rs` (embedded instance via the `duckdb` crate, DuckLake on PostgreSQL) |
| View compiler: SQL rewrite to delta + MERGE, rejection at create time | `plan.rs`. DuckDB parses the view (`json_serialize_sql`). The AST is checked against the operator matrix, the stream reference is swapped for the epoch's staging table, and aggregates are split into mergeable partials (`avg` → `sum` + `count`). `json_deserialize_sql` turns it back into SQL. Global aggregates compile into three operators over weighted rows (`__w`): pre-aggregate, aggregate with a DuckLake state table, and post (`HAVING` + select list) into an upsert sink; `GlobalPlan::stage_sql` / `commit_sql` are the per-epoch statements, shared with the unit tests. |
| Source readers, offsets owned by the lake | `kafka.rs` (`rskafka`, one prefetch task per partition, bounded channel for backpressure). Each fetch is cut into chunks; with Arrow decode a chunk is decoded on a blocking thread before it is queued. |
| JSON decode | `decode.rs` (Arrow, per chunk, in the readers) for the forms it can match exactly; `plan.rs::decode_sql` in staging for the rest, and for every message of a source with `"decode": "sql"`. |
| Epoch loop: poll, watermark, compute, late routing, commit | `engine.rs` |
| Control data in `lake.__duckwake` | `catalog.rs`: `sources`, `jobs`, `checkpoints`, `dead_letter`, `lease_<job>`, `state_<job>` |
| Maintenance between epochs | `maintenance.rs`: flush inlined data, merge files, expire snapshots, delete old files, prune checkpoints |
| Differential tests | `verify.rs` |

The epoch transaction (`engine.rs::commit`), one per worker per epoch, covers all of its jobs:

```sql
BEGIN;
CALL lake.set_commit_message('duckwake', ..., extra_info => '{"worker":"w1","jobs":{"clicks_per_min":42}}');
UPDATE lake.__duckwake.lease_clicks_per_min SET last_epoch = 42 WHERE owner_worker = 'w1' AND lease_epoch = 7;  -- 0 rows = fenced
MERGE INTO lake.__duckwake.state_clicks_per_min s USING memory.main.out_clicks_per_min p ON ...;                -- partials
INSERT INTO lake.analytics.clicks_per_min SELECT <final projection>, 42 FROM state WHERE __we <= <watermark>;  -- closed windows
DELETE FROM lake.__duckwake.state_clicks_per_min WHERE __we <= <watermark>;
INSERT INTO lake.analytics.clicks_late ...; INSERT INTO lake.__duckwake.dead_letter ...;
INSERT INTO lake.__duckwake.checkpoints VALUES ('clicks_per_min', 42, '{"clicks":{"0":1832,...}}', <watermark>, ...);
COMMIT;
```

## Findings that should change the design doc

1. **Fence on a per-job lease table, not on `jobs`.** In DuckLake, an UPDATE to one row conflicts with
   a concurrent UPDATE to any other row of the same table: "another transaction has deleted inlined data
   from it". The doc's per-epoch `UPDATE jobs ... WHERE lease_epoch = $lease` would make all workers
   collide every epoch, and also collide with every `start`/`pause`. The POC fences on
   `__duckwake.lease_<job>`, and the worker never writes `jobs` during an epoch. Zombie fencing works:
   the stale commit fails at COMMIT with a conflict, or its fence UPDATE matches 0 rows.
2. **A transaction can write only one attached database.** `Δ` temp tables cannot be filled inside the
   commit. Staging (decode, windows, late split, partials) runs in the in-memory catalog *before* `BEGIN`.
   The commit reads staging tables and writes only the lake.
3. **"The epoch commit always wins" is not something DuckLake offers.** The first committer wins. A
   checkpoint-pruning DELETE that removes rows conflicts with a concurrent epoch INSERT into the same
   table, so the epoch commit retries (up to 3 times). Concurrent appends to a shared table, such as
   checkpoints from two workers, do not conflict.
4. **Building on DuckDB 2.0 from Rust.** The classic `duckdb` crate 1.20000.1 embeds DuckDB v2.0.0 fine
   for the daemon; `duckdb-neo` is only needed for the loadable extension. `libduckdb-sys` must be built
   without `DEBUG`: in a Cargo dev build its allocator assertions abort when the downloaded release-build
   `ducklake` extension frees memory. See `[profile.dev.package.libduckdb-sys]`.
5. **Table options are not seen by connections that already attached the lake.** A sink's
   `data_inlining_row_limit` set by `create-view` was ignored by a worker already running, so every
   epoch wrote a tiny Parquet file. The worker now re-applies the option on its own connection when it
   starts a job.
6. **A DuckLake MERGE allows only one UPDATE or DELETE action** ("MERGE INTO with DuckLake only
   supports a single UPDATE/DELETE action currently"), so an upsert sink can't be maintained with one
   MERGE. DuckWake deletes the changed groups' rows and inserts their new ones in the epoch transaction.
7. **Smaller items.**
   - `set_option('data_inlining_row_limit', ...)` fails on a table created in the same transaction.
   - `AT` is now a reserved word (time travel).
   - `now() - INTERVAL` needs the ICU extension.
   - DuckDB does not short-circuit `AND`, so `json_type()` must be guarded with `CASE` against malformed
     payloads.
   - Skipping empty epochs (no new rows, watermark unchanged) avoids empty snapshots when a stream is idle.

## Simplifications and gaps

- **Front-ends.** No `CREATE CONTINUOUS VIEW` / `CREATE SOURCE` DDL: DuckDB's C API (v1 and v2) has no
  parser hooks, so it needs a C++ shim. The extension is unsigned, built per platform, and has no
  `duckwake_explain`. The server's Quack token is one shared secret; there are no users or roles.
- **Sources.** Kafka only, JSON only, with declared columns. Unknown fields are dropped; there is no
  VARIANT catch-all. Offsets and definitions are stored as `JSON`, not `VARIANT`.
- **Partitions and readers.** A job reads all partitions of its topic, so there is no partition split
  across workers. Without `--shared-readers` each job has its own readers: two jobs on one source read it
  twice.
- **Operators.** Filter, project and lookup joins (any lake table in `FROM`). TUMBLE/HOP windows and
  global `GROUP BY` (with `HAVING` and expressions over aggregates) with `count`, `sum`, `min`, `max`
  and `avg`. `HAVING` in windowed views, `DISTINCT`, `approx_count_distinct` and stream-stream joins
  are rejected at create time. `min`/`max` in global aggregates assume append-only input, which holds
  while sources are Kafka topics; retracting inputs need value counts (milestone 3).
- **State and maintenance.** DuckLake state backend only; object-storage state is not built.
  Maintenance has no time budget and no lag-based skip.
- **Watermark after restart.** Per-partition maxima are not checkpointed. After a restart the watermark
  holds at the checkpointed value until every active partition reports again.

## Layout

```
crates/core      duckwake_core: engine, compiler, catalog, Kafka, maintenance, verify
crates/core/examples/phase0_*.rs   the Phase 0 spikes (DuckLake commit and conflict semantics)
crates/core/src/sql_functions.rs   the duckwake_* SQL functions (server and extension); api.rs the shared calls
crates/duckwake   duckwake CLI, duckwaked server (src/serve.rs), playground (src/playground.rs, playground.html)
crates/extension  the loadable extension (built on its own: scripts/build_extension.sh)
Dockerfile       the duckwaked image (docker compose --profile server)
scripts/         crash, fencing, reset, retention, decode, shared-reader, server and extension tests; benchmark
```
