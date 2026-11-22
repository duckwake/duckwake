# Internals

## How it maps to the design

| Design component | Code |
| --- | --- |
| Worker with an embedded DuckDB 2.0 and the lake attached | `lake.rs` (embedded instance via the `duckdb` crate, DuckLake on PostgreSQL) |
| View compiler: SQL rewrite to delta + MERGE, rejection at create time | `crates/circuit/src/frontend/plan.rs`. DuckDB parses the view (`json_serialize_sql`). The AST is checked against the operator matrix, the stream reference is swapped for the epoch's staging table, and aggregates are split into mergeable partials (`avg` → `sum` + `count`). `json_deserialize_sql` turns it back into SQL. Global aggregates compile into three operators over weighted rows (`__w`): pre-aggregate, aggregate with a DuckLake state table, and post (`HAVING` + select list) into an upsert sink; `GlobalPlan::stage_sql` / `commit_sql` are the per-epoch statements, shared with the unit tests. |
| Source readers, offsets owned by the lake | `kafka.rs` (`rskafka`, one prefetch task per partition, bounded channel for backpressure). Each fetch is cut into chunks; with Arrow decode a chunk is decoded on a blocking thread before it is queued. |
| JSON decode | `decode.rs` (Arrow, per chunk, in the readers) for the forms it can match exactly; `decode.rs::decode_sql` in staging for the rest, and for every message of a source with `"decode": "sql"`. |
| Views as DBSP circuits: operator graph, incrementalization, SQL per operator | the `duckwake_circuit` crate (see below) |
| Epoch loop: poll, watermark, compute, late routing, commit | `engine.rs` |
| Control data in `lake.__duckwake` | `catalog.rs`: `sources`, `jobs`, `checkpoints`, `dead_letter`, `lease_<job>`, `state_<job>`; `failover.rs`: `worker_<id>` |
| Worker heartbeats and failover | `failover.rs` (finding 12) |
| Maintenance between epochs | `maintenance.rs`: flush inlined data, merge files, expire snapshots, delete old files, prune checkpoints |
| Differential tests | `verify.rs` |
| PostgreSQL sources: decoding, definition, ingest job | `pgoutput.rs` (pgoutput messages, LSNs, `pg_snapshot` visibility), `pgsource.rs` (options, checks, publication and slot, type mapping, `verify`), `ingest.rs` (the job; see below) |

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

A PostgreSQL source's ingest job (`ingest.rs`) runs on a thread of its own with its own connection, so a long
snapshot never holds up the views' epochs; the worker starts and stops it like a view (lease, claims, failover,
handoffs) and marks it failed when it reports a broken rule. Its epoch, with no transaction of the views':

```sql
-- before: the slot's state, then the changes (DuckDB's postgres extension; nothing consumed)
SELECT * FROM postgres_query('__duckwake_pg_shop', 'SELECT pg_current_wal_flush_lsn(), ... FROM pg_replication_slots ...');
SELECT data FROM postgres_query('__duckwake_pg_shop', 'SELECT data FROM pg_logical_slot_peek_binary_changes(''duckwake_shop'', NULL, 100000, ...)');
-- decoded in Rust into each touched key's last state; staged in memory, cast, unchanged TOAST values filled from the mirror
BEGIN;
UPDATE lake.__duckwake.lease_shop SET last_epoch = 9 WHERE owner_worker = 'w1' AND lease_epoch = 3;  -- 0 rows = fenced
DELETE FROM lake.shop.orders m WHERE EXISTS (SELECT 1 FROM memory.main.ingest_shop_chg0 c WHERE m.id = CAST(c.k0 AS BIGINT));
INSERT INTO lake.shop.orders SELECT ... FROM memory.main.ingest_shop_row0;
INSERT INTO lake.__duckwake.checkpoints VALUES ('shop', 9, '{"shop":{"0":1620405840}}', NULL, 'pgoutput', <snapshot state>, ...);
COMMIT;
SELECT * FROM postgres_query('__duckwake_pg_shop', 'SELECT pg_replication_slot_advance(''duckwake_shop'', ''0/6094C350'')');
```

## Views as DBSP circuits

Every view runs as a [DBSP](https://arxiv.org/abs/2203.16684) circuit, the model Feldera is built on
(the `duckwake_circuit` crate). The compiled view becomes an operator graph Q over whole collections: `Source` (each row with weight
`__w`, +1 for Kafka), `Window`, `LateSplit`, `From` (the stream under the view's alias, joined with its lookup
tables), `Filter` (`WHERE`, `HAVING`), `Map` (a select list), `Aggregate`, `Upsert` (each key's current row, for upsert views), `Sink`. The frontend cuts the view
into these pieces from DuckDB's parse tree, each kept as SQL text in the view's own names. An analysis
marks which edges are append-only; a view whose operators assume append-only input (append sinks, late-data
tables) is refused when that input can retract. `incrementalize`
wraps it as D ∘ Q ∘ I and pushes every integrator (I) forward until it meets a differentiator (D), the way
Feldera's `OptimizeIncrementalVisitor` does:

- a linear operator L commutes with I: L ∘ I = I ∘ L;
- any other operator S becomes its incremental form SΔ = D ∘ S ∘ I, so S ∘ I = I ∘ SΔ; AggregateΔ keeps its
  state in a DuckLake table;
- D ∘ I = id.

What remains runs on changes, and every operator generates its own SQL: `stage_steps` (in-memory, before the
epoch's transaction) and `commit_steps` (inside it). Linear operators fuse: a chain of `From`, `Filter` and
`Map` is one `SELECT`, and an operator's output is a table only where a stateful operator or a sink needs it.
A global aggregate's output is its change as a Z-set, the touched groups' old rows with weight −1 and their
new rows with +1; the upsert sink consolidates it, so a group whose row did not change cancels out. The
engine runs the steps generically: counts, lean-commit skips and crash points are step attributes.

**Retractions (C1).** A source whose `SourceSchema::append_only` is false stages each row's weight, and every
operator then sums weights instead of counting rows, choosing the weighted form only when the input can
retract, as Feldera's `ExpandAggregates` does (Kafka views keep the cheaper partials):

| Operator | Over a retracting source |
| --- | --- |
| Stateless view | sink `Counted`: one row per distinct row with its multiplicity in `_duckwake_count`; changes move the count, and a row whose count reaches 0 is deleted |
| `DISTINCT` | the state sums weights; a row leaves with −1 when its weight returns to 0; sink `Upsert` by the whole row |
| Global `count`, `sum`, `avg` | already weighted |
| Global `MIN`/`MAX`, `bool_and`/`bool_or` | value rows in the state table (`__i` the aggregate, `__v<i>` the value, `__n` its count); the extreme is recomputed for the epoch's groups from the values still present. `bool_and`/`bool_or` are `MIN`/`MAX` of a BOOLEAN |
| Global `count(DISTINCT)`, `approx_count_distinct`, quantiles | the same value rows, always (also over inputs that only insert); the count is the number of values whose count is above 0, `approx_count_distinct` is DuckDB's estimate over the values present (the same as the batch query's: the sketch depends only on the set), and a quantile is DuckDB's over the touched group's values, each repeated by its count (`list_resize`) |
| Global and windowed variances | weighted count, sum and sum of squares (`__a<i>_n`, `_s`, `_q`), like `avg`. The second moment `q − s²/n` is taken as 0 below `n·q·10⁻¹⁵`, its rounding: DuckDB's Welford computation gives exactly 0 for a constant group |
| Global and windowed `covar_*`, `corr`, `regr_*` | weighted count and sums of y, x, y², x², xy over the rows where both are non-NULL (`__a<i>_n`, `_y`, `_x`, `_yy`, `_xx`, `_xy`), with DuckDB's NaN and NULL at the edges (`paired_finalize`) |
| Windowed `count`, `sum`, `avg` | partials `sum(__w)`, `sum(x * __w)` and a weighted non-NULL count; a window and group whose weight returns to 0 is not emitted |
| Windowed `MIN`/`MAX`, `count(DISTINCT)`, `approx_count_distinct`, quantiles | refused for now (over inputs that only insert, the last three keep each window's values as a LIST partial: distinct values merged with `list_distinct(list_concat(...))`, all values with `list_concat`, finalized with `list_aggregate`) |
| Late-data table | refused for now: late rows would need their weights (C4) |

**Upsert views (C2).** A view created with `upsert_key` gets an `Upsert` operator after its `Source`
(`ops/upsert.rs`): non-linear, so it runs as UpsertΔ, with each key's current row in the view's state table
(the source's columns, nothing else). As a set, a key's current row is the best of its messages after its last
tombstone (`UpsertSpec::best_sql`: the highest version, then the latest position). Per epoch it stages that for
each key the epoch touches, with `__cut` when the epoch holds a tombstone for it; the stored rows of those keys
(`uold`; when pipelined, the previous epoch's uncommitted `unew` overlays the state); and the changed keys' new
rows (`unew`): the epoch's row when there was a tombstone, no stored row, or it is newer than the stored one
(`UpsertSpec::newer`; its messages come after the stored row's because a key stays in one partition). Its
output is a Z-set: a changed key's old row −1, its new row +1. Everything after it sees a retracting input, so a
filter or a projection is linear as usual and the sink is `Upsert` by the key columns. The commit replaces the
changed keys' state rows. The batch query the view must equal, used by `verify` and the tests, is
`UpsertSpec::batch_sql` over every message. Tombstones are staged as typed rows with `_err` 'tombstone' and the message key in `_key`; the
`Source` of an upsert view takes them with `__del`, and the commit leaves them out of its dead letters.

**Debezium sources (C3).** A source with `"format": "debezium"` changes only the decode
(`decode::debezium_sql`); the `Upsert` operator is C2's. The envelope is unwrapped from `schemas.enable`'s
`{schema, payload}`, and so is the key (`decode::key_sql`, also used for Kafka tombstones and by `duckwake sql`).
`c`, `r` and `u` events decode `after`; a `d` event decodes `before` and is staged as a tombstone, so a delete
without a message key still finds its key through C2's fallback to the value's key columns. Other operations
are dead letters. Numbers in `TIMESTAMP` and `DATE` columns are Debezium's epoch encodings
(`decode::debezium_cast`). An event carrying `__debezium_unavailable_value` is staged with `_err` 'unavailable
value'; `stage_source` counts them and `stage_view` stops every view of the source with `StopJob`, since the
row cannot be rebuilt. Such a source may have no event time: it then has no per-partition maxima, no watermark
and no freshness, and the compiler refuses windows over it (`SourceSchema::event_time` is `None`) and views
without `upsert_key` (`SourceSchema::changelog`).

A window is still emitted once, when it closes; a retraction into a closed window is a late row. The
harness's `changes` source deletes and updates rows of the previous epoch and cancels rows within an epoch,
and every view kind over it matches the batch query over the net rows after every epoch.

The tests in `crates/circuit/src/tests/` run each view kind epoch by epoch through its circuit's own SQL,
against an in-memory lake, and compare the view's table with the batch query after every epoch, with epochs
staged in order and pipelined.

A view may read several sources (`UNION ALL` today, joins next). The worker keeps one input per source, each
with its reader group, position and watermark; the view's watermark is the oldest of its sources'. One epoch
stages every source, and its checkpoint row holds all their offsets, so recovery seeks each reader to the
same epoch. With `--shared-readers`, each input joins the group of its source at its position.

Each operator is a file in `crates/circuit/src/ops/` implementing `Operator`: its class (source, sink, linear,
non-linear), output properties, input checks, and its stage and commit SQL; a non-linear one also implements
`Stateful`, its incremental form. The rewrite rules read only the class, so a new operator is a new file, a
variant in `Op`, its lowering and a test.

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

The design doc is [design/streaming-extension.md](design/streaming-extension.md); its findings from the first build
records the ones it took up.

1. **Fence on a per-job lease table, not on `jobs`.** In DuckLake, an UPDATE to one row conflicts with
   a concurrent UPDATE to any other row of the same table: "another transaction has deleted inlined data
   from it". The doc's per-epoch `UPDATE jobs ... WHERE lease_epoch = $lease` would make all workers
   collide every epoch, and also collide with every `start`/`pause`. DuckWake fences on
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
8. **DuckLake inlines an insert of at most 10 rows by default** (DuckDB v2.0.0, DuckLake format 1.1); one of
   11 rows writes a Parquet file. With `--lean-commit` one INSERT carries the checkpoint rows of every job on
   the worker, so a worker with more than 10 jobs wrote one small checkpoint file per epoch (27 files after
   the 16-view bench). Each worker now sets the checkpoint table's limit to 1000 at start. Inlining the dead
   letters and late rows as well removed their files too, but at thousands of rows per epoch it raised the
   bench's commit p50 from 321–329 ms to 382 ms (the COMMIT call from 82 to 113 ms): writing them into the
   Postgres catalog costs more than one local Parquet file. They keep the default, and maintenance merges
   their files.
9. **Reading a table's change feed by snapshot position is exact** (spike F0,
   `crates/core/examples/phase_f0_change_feed.rs`, Postgres catalog, one machine). What views over lake
   tables rely on:
   - With 4 writers committing 600 inserts concurrently (12s, inlined and file-backed), a reader that read
     `ducklake_table_changes` over (P, newest] about every 125 ms saw all 12,240 inserts exactly once, and
     the visible snapshot ids had no gap at any of 97 polls: an id is taken as the catalog's max + 1 under
     its primary key, so N + 1 is never visible before N.
   - `ducklake_current_snapshot('lake')` returns the newest snapshot another process committed, in 6–8 ms.
     `max(snapshot_id)` over `ducklake_snapshots` takes 20–38 ms and grows with the snapshot count.
   - Paging a table at an old snapshot by `rowid` (`AT (VERSION => s) WHERE rowid > r ORDER BY rowid LIMIT
     n`) returns exactly that version after later deletes, updates and inserts. The `rowid` filter is
     pushed into the scan, and every page of 200k rows takes 40–65 ms.
   - One snapshot's changes cost 45–55 ms warm whatever the table's file count (10 to 1000 files), against
     16–20 ms for a full scan of 100k–1M rows: a fixed cost of catalog round trips, not file reads. Cold, and
     on a table with 30 deletes, it was 150–170 ms. Every lake input of every view pays it each epoch, so
     views reading the same table should read its range once (F4).
   - `snapshot_time` never ran backwards against snapshot ids (0 of 599 pairs). Writers on other machines
     may still disagree on the clock, so the change time stays a running maximum.
10. **Views over lake tables and other views (F1)** need a few more DuckLake details, checked on the
   Postgres catalog:
   - `ducklake_table_changes(lake, schema, table, from, to)` fails when `to` is not a snapshot ("No snapshot
     found at version 99") and when `from > to`, so a job reads the current snapshot S first and skips an
     empty range (P = S) instead of querying it.
   - `ducklake_merge_adjacent_files` keeps every row's `snapshot_id`, and its own snapshot has no changes:
     3 files merged into 1, the change feed over the old range returned the same 4 inserts, and over the
     merge's snapshot none. Maintenance does not disturb a change feed.
   - Table ids come from `__ducklake_metadata_lake.ducklake_table` (joined to `ducklake_schema`, rows with
     `end_snapshot IS NULL`); `ducklake_table_info` has the id but no schema name.
   - A table alias goes before the version: `FROM t AS x AT (VERSION => n)`.
   - A view's own commits add snapshots that move every lake input's watermark (`_commit_ts`). Committing
     every time it moved would keep a worker writing an empty snapshot every epoch forever, so a view with
     lake inputs commits only for new rows or a window that closes. A windowed one also commits when its
     watermark has moved a whole slide past the one its last checkpoint recorded: a view windowing on its
     `window_start` or `window_end` reads that recorded watermark (`watermark_at`), and without these commits
     it waited for the next `position_refresh` (5 minutes by default) once the upstream view had no window
     left to close. When the whole lake is idle no
     snapshot comes, the watermark stands, and the last window stays open, as for an idle Kafka partition.
11. **Lake inputs that retract (F2)** weigh each change: `insert` and `update_postimage` +1, `delete` and
   `update_preimage` -1, times `_duckwake_count` for a counted view's table. To cancel the row the view took
   in, a retraction must carry that version's `_commit_ts`, not the time of the delete. DuckLake makes the
   version easy to find:
   - A row keeps its `rowid` through updates, and row ids are never reused; the `snapshot_id` column of a
     row is the snapshot that wrote its current version (probed on inlined and Parquet data, and after
     `ducklake_merge_adjacent_files`).
   - Within one snapshot, a row changes at most once: an update's old and new versions, or one delete, or
     one insert. A transaction that updates then deletes a row shows a single delete of the version before
     it; an insert then update, a single insert of the final version.
   - So a retraction's version is the newest earlier write of its `rowid` in the epoch's range, or else its
     version at P, the snapshot read up to: `t AT (VERSION => P) WHERE rowid BETWEEN lo AND hi AND rowid IN
     (...)`, which DuckLake prunes by row-id range. Its `_commit_ts` is the running maximum of
     `snapshot_time` up to that version's snapshot, as when the row was staged.
   - A view holds the rows written after it was created (snapshot S0), which is also what `verify` compares
     against (`snapshot_id > S0` at P). A retraction of a version written at or before S0 is dropped: the
     view never had that row. An update of such a row adds its new version.
   - A window is final once the watermark passes it, and a lake input's watermark is exact, so the delete
     of an old row usually arrives after its window closed (C4). A windowed view takes retractions as any
     view does: a window still open is revised (weighted partials; `min`, `max`, the distinct counts and the
     quantiles keep a `MAP(value, count)` partial, `MergeOp::Counts`), and a retraction for a closed window
     is late, routed by `LateSplit` to the late-data table with its weight (`_duckwake_weight`) or counted
     and dropped. `verify` builds the batch input from the rows at P plus the late corrections (weight −1),
     which the closed windows still count, and refuses a view without a late-data table only once a
     correction was dropped. Views created before C4 keep reading insert-only (`LakeInput::retracts` false)
     and stop on an update or delete.
   - `reset` of a view restores an upsert or counted table with time travel (delete all, insert the old
     version), which its retracting readers see as ordinary deletes and inserts: they follow the reset
     through the change feed. A windowed reader of an append-only view cannot take deletes, so the reset
     empties it in the same transaction and it backfills again from the table (F3).
12. **Failover needs heartbeats in tables of their own, and dead workers judged on the observer's clock.**
   A shared `workers` table would conflict the way `jobs` would (finding 1), so each worker owns
   `__duckwake.worker_<id>`, one row it updates every `lease_timeout / 5` (at most every 5 s), inlined in the
   catalog. A worker is dead when its `beat` counter has not moved for `--lease-timeout` (default 30 s) as
   timed by the worker watching it, so clocks need not agree, and only over time in which the watcher's own
   heartbeats landed: it must write half a timeout's worth of them (at least 2) meanwhile. A catalog stall
   stops every heartbeat at once; in CI a 12 s stall on a 2-core runner (every commit waited, none failed)
   made the worker that came out of it first claim the views of both its live peers, and in a larger
   cluster every worker would have claimed every other's. A stall everyone shares now kills no one, and a
   real death is still seen in the lease timeout. A worker that stops cleanly marks itself
   `stopped` and is dead once `--stop-grace` has passed (finding 15). In each refresh (every 2 s) a worker reads the heartbeats and the
   running views' leases, and claims its share of the orphans: views unassigned (`start` without a worker)
   or owned by a dead worker. Orphans go largest first, each to the live worker with the least load (finding
   15), so every worker computes the same plan. A claim is `UPDATE lease SET owner_worker = me, lease_epoch =
   lease_epoch + 1 WHERE owner_worker = <old> AND lease_epoch = <read>`; two workers claiming one view
   conflict and the first committer wins. The new owner starts from the last checkpoint as after any
   restart, and the bumped lease fences the old owner if it was only stalled (`scripts/failover_test.sh`
   freezes one with SIGSTOP and thaws it: its in-flight commit is rejected and it lets go). The heartbeat
   runs on the scheduler thread, so a worker whose epoch loop is stuck looks dead too; a commit that takes
   longer than the timeout moves views needlessly but never breaks exactly-once. Each heartbeat is one
   catalog snapshot (17,280 a day per worker at the default 5 s), expired with the epochs' snapshots.
   A frozen worker can block more than its own views: every DuckLake commit on PostgreSQL inserts the next
   `snapshot_id`, so a worker stopped inside its commit holds a row lock that every other commit waits on,
   claims and heartbeats included, for as long as it stays frozen (seen under load in CI: 16 minutes). A
   worker therefore adds `options='-c idle_in_transaction_session_timeout=<lease timeout>'` to its catalog
   connection (unless the string already passes `options`): PostgreSQL ends the stalled session, which rolls
   its commit back, and the lake moves again within the lease timeout. Thawed, that worker's commit fails and
   it exits (`duckwaked` restarts it). Workers that start together also race on `set_option` (each sets
   inlining on the shared checkpoint table, one catalog row): DuckLake reports the losers' failures as
   plain errors, not conflicts, and the worker died. A worker now skips an option its connection already
   sees, and retries the idempotent write.
13. **Kafka credentials stay out of the lake.** Source definitions live in `__duckwake.sources`, readable by
   anyone who can read the lake, so `sasl_password` must be `env:NAME` or `file:/path` (`secrets.rs`),
   resolved by each worker when it connects. TLS uses rustls with ring, the same stack rskafka uses, with the
   Mozilla roots unless `ssl_ca_location` names CAs. rskafka retries a failed connection forever, a refused
   SASL handshake included, so the first connection of a source is bounded (10 s): without it, one source
   with a wrong password hung its worker and every view on it. `scripts/kafka_auth_test.sh` runs the test
   views over SCRAM in plaintext, SCRAM over TLS and TLS with a client certificate against the local
   Redpanda's authenticated listeners (`docker/kafka`), and checks the refusals.
14. **Object storage needs only an S3 secret.** DuckLake writes, reads, time-travels, merges and deletes data
   files through DuckDB's httpfs, so a data path of `s3://bucket/prefix/` works once the instance has a
   secret: `lake.rs` creates a temporary one, scoped to the bucket, from the standard AWS variables (or the
   AWS credential chain), before attaching. Nothing about object storage reaches the lake, and the catalog
   password comes from libpq's `PGPASSWORD` or `.pgpass`. The suites run against MinIO with
   `LAKE_STORAGE=s3` (Chainguard's image: MinIO no longer publishes community images to Docker Hub).

15. **Rebalancing moves a view between epochs, from the busiest worker only.** Moving a view is cheap here:
   its state and checkpoint are in the lake, so the new owner restores nothing (Kafka Streams needs warm-up
   replicas for that) and the cost of a move is a refresh, the job start and one epoch of latency. Each
   worker publishes, per view, its cost (a moving average of staging, its share of the source read and of the
   commit, per second of wall time) and whether it is within the cooldown. Every `--rebalance-every` the
   busiest worker with a view it may move compares itself with the least loaded one: above the average by
   `ε` times it and by at least a tenth of a worker, for three rounds in a row (lightly loaded workers are not
   worth balancing, and a view catching up after a restart is a spike, not a trend), or two views ahead
   (a worker joined; counts are not noisy, so no wait), it gives the view that best
   evens out the pair, a little against targets that do not read the view's source (one reader serves all
   views of a source on a worker), and never one that only shifts the imbalance. Only one worker gives at a
   time, so two never pile onto one target, and a moved view stays put for the cooldown, so nothing
   ping-pongs. The handoff waits for the view's epoch in flight to commit, then moves the lease with the same
   compare-and-set as a claim: nothing is fenced, nothing staged is lost. Kafka Connect's lesson applies to
   restarts: a clean stop is a restart, so its views wait `--stop-grace` instead of moving away and back;
   `drain` is the explicit way out. Pins (`start --worker W`) are stored in `jobs.pinned_to`: never balanced
   away, failed over when W dies, handed back when W is live. `scripts/rebalance_test.sh` is the gate:
   joining workers, a settled cluster making no moves, a rolling restart keeping every view in place, drain,
   a pinned view failing over and going home, and a `kill -9` in the middle of a drain, with exact output.
16. **A view that stops must not take its epoch with it.** A view stops alone when an insert-only input changes
   or (since P4) when it goes over a limit. The worker used to throw the whole staged epoch away and stage it
   again: but its readers had already handed over those events, so its other views lost them
   (`scripts/limits_test.sh` caught it: 202 rows missing from a view no limit touched). The stopped view is now
   left out of the epoch and the others commit it. Replacing and dropping running views brought two more
   races: `stop` did not fence, so an epoch in flight could land after a `drop` began, and a commit that found
   its view's lease table dropped failed the worker. `stop` and `pause` now bump the lease, and a commit treats
   a vanished lease table as fenced.
17. **DuckDB does not always spill.** Under a tight memory limit DuckDB spills joins and aggregates, but an
   epoch's inserts into DuckLake still failed with Out of Memory (64 MB limit, 1M-row epochs), so a worker in
   that state would fail on the same epoch forever. Workers now run with `preserve_insertion_order = false`
   (nothing they run depends on it, and inserts then stream), and an epoch that still runs out of memory makes
   the worker start again from its checkpoints, in a fresh instance, with half as many rows per epoch. Each
   worker spills into its own directory, emptied at start: DuckDB's default `.tmp` is shared by every
   process started in the same directory.
18. **Adding a source column must not change running views.** A view's plan is compiled again at every start,
   from the source's columns as they are: a `SELECT *` view would grow a column its table does not have and
   be refused. Each view now stores the columns of each source it read when it was created, and is compiled
   against those; adding a column records them, in the same transaction, for views made before this.

## Simplifications and gaps

- **Front-ends.** No `CREATE CONTINUOUS VIEW` / `CREATE SOURCE` DDL: DuckDB's C API (v1 and v2) has no
  parser hooks, so it needs a C++ shim. The extension is a signed community extension for Linux (amd64, arm64) and `osx_arm64`; other platforms install it unsigned from an extension repository. The server's Quack token is one shared secret; there are no users or roles.
- **Sources.** Kafka only, JSON only, with declared columns. SASL (PLAIN, SCRAM) and TLS, with client
  certificates; no OAUTHBEARER or Kerberos yet. A source's first connection must succeed within 10 s or the
  view is marked failed, so a view starting while its brokers are down needs `start` again; once
  connected, readers retry through broker restarts. A view whose topic does not exist yet waits for it, as
  Kafka consumers do: its worker checks every refresh (before taking the lease, so waiting commits nothing)
  and logs once. Unknown fields are dropped; there is no
  VARIANT catch-all. Offsets and definitions are stored as `JSON`, not `VARIANT`.
- **Partitions and readers.** A job reads all partitions of its topic, so there is no partition split
  across workers. Without `--shared-readers` each job has its own readers: two jobs on one source read it
  twice.
- **Operators.** Filter, project and lookup joins (any lake table in `FROM`; a lookup table is read whole each
  epoch, so a change to it reaches only rows that arrive later). TUMBLE/HOP windows and
  global `GROUP BY` (with `HAVING` and expressions over aggregates) with `count`, `count(DISTINCT)`,
  `approx_count_distinct`, `sum`, `min`, `max`, `avg`, `bool_and`, `bool_or`, the variances and standard
  deviations, `median` and the exact quantiles, `covar_*`, `corr` and `regr_*`. Windowed views take `HAVING` too, applied as windows close (`Plan::window_having`). `UNION ALL` of plain views, each branch over any source: the view reads every source in one
  epoch and commits all their offsets in one checkpoint row. `SELECT DISTINCT` over plain views: its state keeps every distinct row with its count, and a
  row is written the first time its count turns positive. `DISTINCT` over windows and aggregates,
  `DISTINCT ON`, `list`/`string_agg`/`mode`, `approx_quantile`, `arg_min`/`arg_max` (a tie makes DuckDB's
  batch result depend on row order, so `verify` could not check them) and stream-stream joins are rejected at
  create time. Every operator handles retracting input (see Retractions above) except `MIN`/`MAX`,
  `count(DISTINCT)`, `approx_count_distinct` and the quantiles in windows, and late-data tables. Lake tables, and views whose tables change, are the
  retracting inputs; Kafka sources only insert (no CDC yet).
- **Storage.** Local disk or `s3://` (S3 and S3-compatible stores); not `gs://` or Azure natively (GCS through
  its S3 API works). Credentials come from the environment of each process; there is no secret store
  integration (Vault, cloud secret managers) beyond files and variables.
- **State and maintenance.** DuckLake state backend only; object-storage state is not built.
  Maintenance has no time budget and no lag-based skip.
- **Lake inputs (F1, F2).** A view reads a lake table, or another view's table, through its change feed when
  no Kafka source is in its `FROM`; next to a Kafka source the table is a lookup. A view without windows
  takes the input's updates and deletes as retractions (finding 11): a stateless view's table counts each
  row (`_duckwake_count`), `DISTINCT` and aggregates upsert. A windowed view revises the windows still open
  and records changes to closed ones as late corrections (C4); one created before C4 reads lake tables
  insert-only and stops on an update or delete.
- **Backfill and retention (F3).** A new view takes S0, the snapshot current when it is created (or, after a cascaded reset or a reset to
  epoch 0, the one current at its next start), and pages
  each input `AT (VERSION => S0)` in `rowid` order, `max_rows` per epoch; the cursor is in the checkpoint
  (offsets key `1`, next to the position at key `0`), so a crash resumes the page after the last committed
  one. Rows read this way carry the `_commit_ts` of their version, so the view equals its batch query at S0
  (`verify` restricts each input to `rowid <= cursor` while the backfill runs). Then it reads the changes after
  S0. The watermark stays at its start while any input backfills, so no window closes early. `start: latest`
  skips the backfill and drops retractions of versions at or before S0 (`lake_floor`); a reset or replace
  keeps that floor. Maintenance expires snapshots only before the oldest position of any view not dropped
  (P_min), and first records the expired snapshots' commit times in `__duckwake.snapshot_times`, since a row's
  `_commit_ts` is the running maximum of snapshot times. An idle running view moves its position every
  `--position-refresh`, so it does not hold P_min back. A view whose position has expired anyway fails with
  that snapshot, and a reset to an epoch whose snapshot expired is refused. `reset` reaches retracting readers
  through the change feed; windowed readers of the reset view are emptied and backfill again. Windows over a lake input use `_commit_ts`,
  or a windowed view's `window_start`/`window_end`; windows on another column of a lake input are not
  supported yet. Joins of two lake inputs, and fused chains on one worker (one hop per epoch now), come
  later. Columns are fixed at create time. `_commit_ts` is a `TIMESTAMP` in UTC, not `TIMESTAMPTZ`, so it
  compares with Kafka event times. A view reading a lake table polls its change feed every epoch even when
  the table is idle; views on one table should share that read (F4).
- **Placement.** Load assumes workers of equal size: a bigger machine is not given more views. A view's
  cost is measured on the worker that runs it, so a view that has not run yet counts as the average view.
  Every worker that ever ran keeps its `worker_<id>` table (state `stopped` after a clean stop); nothing
  drops old ones yet, and `workers` lists them all.
- **Watermark after restart.** Per-partition maxima are not checkpointed. After a restart the watermark
  holds at the checkpointed value until every active partition reports again.

## Layout

```
crates/circuit   duckwake_circuit: views as DBSP circuits. frontend/ (SQL -> plan -> Q), ops/ (one file per
                 operator), passes/ (analyze, incrementalize), codegen/ (SQL steps per epoch), ir/ (the graph)
crates/core      duckwake_core: engine, catalog, failover, Kafka, PostgreSQL sources (pgoutput, pgsource,
                 ingest), secrets, decode, maintenance, verify
crates/core/examples/phase0_*.rs   the Phase 0 spikes (DuckLake commit and conflict semantics)
crates/core/src/sql_functions.rs   the duckwake_* SQL functions (server and extension); api.rs the shared calls
crates/duckwake   duckwake CLI, duckwaked server (src/serve.rs), playground (src/playground.rs, playground.html)
crates/extension  the loadable extension (built on its own: scripts/build_extension.sh)
Dockerfile       the duckwaked image (docker compose --profile server)
docker/kafka     the local Redpanda's listeners (open, SASL, SASL over TLS, mTLS) and test-only certificates
deploy/          Prometheus alerting rules and their tests, scrape config, Grafana dashboard and provisioning
scripts/         crash, chain, retract, fencing, failover, rebalance, kafka auth, schema, limits, reset, retention, decode, shared-reader, PostgreSQL ingest, server and extension tests; benchmark
```
