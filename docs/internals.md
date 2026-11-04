# Internals

## How it maps to the design

| Design component | Code |
| --- | --- |
| Worker with an embedded DuckDB 2.0 and the lake attached | `lake.rs` (embedded instance via the `duckdb` crate, DuckLake on PostgreSQL) |
| View compiler: SQL rewrite to delta + MERGE, rejection at create time | `plan.rs`. DuckDB parses the view (`json_serialize_sql`). The AST is checked against the operator matrix, the stream reference is swapped for the epoch's staging table, and aggregates are split into mergeable partials (`avg` → `sum` + `count`). `json_deserialize_sql` turns it back into SQL. Global aggregates compile into three operators over weighted rows (`__w`): pre-aggregate, aggregate with a DuckLake state table, and post (`HAVING` + select list) into an upsert sink; `GlobalPlan::stage_sql` / `commit_sql` are the per-epoch statements, shared with the unit tests. |
| Source readers, offsets owned by the lake | `kafka.rs` (`rskafka`, one prefetch task per partition, bounded channel for backpressure). Each fetch is cut into chunks; with Arrow decode a chunk is decoded on a blocking thread before it is queued. |
| JSON decode | `decode.rs` (Arrow, per chunk, in the readers) for the forms it can match exactly; `plan.rs::decode_sql` in staging for the rest, and for every message of a source with `"decode": "sql"`. |
| Views as DBSP circuits: operator graph, incrementalization, SQL per operator | `circuit.rs` (see below) |
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

## Views as DBSP circuits

Every view runs as a [DBSP](https://arxiv.org/abs/2203.16684) circuit, the model Feldera is built on
(`circuit.rs`). The compiled view becomes an operator graph Q over whole collections: `Source`, `Weigh` (weight
+1 per row), `Window`, `LateSplit`, `Map` (the stateless SQL fragments), `Aggregate`, `Sink`. `incrementalize`
wraps it as D ∘ Q ∘ I and rewrites with DBSP's rules until no integrator (I) or differentiator (D) is left:

- a linear operator L commutes with both: L ∘ I = I ∘ L, D ∘ L = L ∘ D;
- D ∘ I = id;
- D ∘ Aggregate ∘ I = AggregateΔ, the incremental aggregate, whose state is a DuckLake table.

What remains runs on changes, and every operator generates its own SQL: `stage_steps` (in-memory, before the
epoch's transaction) and `commit_steps` (inside it). Linear operators fuse into their consumer's statement as
subqueries; an operator's output is a table only where a fragment reads it by name or two operators read it.
The engine runs the steps generically: counts, lean-commit skips and crash points are step attributes.

`duckwake explain VIEW` (or `FROM duckwake_explain('VIEW')`) prints Q, each rewrite, the epoch circuit and one
epoch's SQL. For a windowed view:

```
Q: the view over whole collections            The circuit that runs every epoch
  n0  Source(clicks)                            n0  Source(clicks)
  n1  Window(TUMBLE ts, 10s)       ← n0         n1  Window(TUMBLE ts, 10s)        ← n0
  n2  LateSplit(on time | late)    ← n1         n2  LateSplit(on time | late)     ← n1
  n3  Aggregate(windowed)          ← n2.0       n3  AggregateΔ(windowed) [state]  ← n2.0
  n4  Sink(append)                 ← n3         n4  Sink(append)                  ← n3
  n5  Sink(late data)              ← n2.1       n5  Sink(late data)               ← n2.1
```

New operators (DISTINCT, stream–stream joins, more aggregates) are new graph nodes with their own SQL and an
incremental form; the rules and the engine stay as they are. Feldera's `dbsp` crate is the reference for each
operator's semantics and a test oracle, not a dependency.

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
   for the CLI and the server, and its `loadable-extension` feature builds the extension against the host's
   DuckDB (V1 C API, pinned to v1.5.6, which DuckDB 2.0 still loads); `duckdb-neo` was not needed. `libduckdb-sys` must be built
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
   - `now()` inside a transaction is the transaction's start, and DuckLake's `snapshot_time` is also taken
     before the commit lands (about 50 ms before the epoch's start, here). Neither tells when output became
     visible. A checkpoint's `committed_at` is the worker's clock once the view's writes are done, which
     leaves out only the COMMIT call; the epoch log's `freshness_ms` and the
     `duckwake_commit_freshness_seconds` metric measure to the moment COMMIT returned.

## Simplifications and gaps

- **Front-ends.** No `CREATE CONTINUOUS VIEW` / `CREATE SOURCE` DDL: DuckDB's C API (v1 and v2) has no
  parser hooks, so it needs a C++ shim. The extension is unsigned and built per platform. The server's Quack token is one shared secret; there are no users or roles.
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
crates/core/src/circuit.rs        views as DBSP circuits: graph, incrementalization, SQL per operator
crates/core/src/sql_functions.rs   the duckwake_* SQL functions (server and extension); api.rs the shared calls
crates/duckwake   duckwake CLI, duckwaked server (src/serve.rs), playground (src/playground.rs, playground.html)
crates/extension  the loadable extension (built on its own: scripts/build_extension.sh)
Dockerfile       the duckwaked image (docker compose --profile server)
scripts/         crash, fencing, reset, retention, decode, shared-reader, server and extension tests; benchmark
```
