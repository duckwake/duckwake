# Change feed sources

Nov 5, 2026

A view reads any DuckLake table or other view it names through the table's change feed, with nothing to declare. Inserts weigh +1, deletes −1, and an update is a −1/+1 pair. The table already holds the current rows, so views need no per-key state. One snapshot id per view is the position for all its lake tables, committed with the view's output in the epoch transaction.

This is milestone D2 of [Database CDC sources](database-cdc-sources.md), moved ahead of D1. It tests every retracting operator on plain lake tables before any Postgres connector exists, and it lets a view select from another view. Retracting inputs depend on C1 (every operator weight-aware) from [CDC sources](cdc-sources.md); append-only ones do not.

## SQL surface

No source is declared for a lake table. `create_view` resolves every name in FROM; a Kafka source stays declared, because brokers, topic and schema cannot be discovered, but a lake table describes itself.

```sql
-- over a lake table
SELECT status, count(*) AS n, sum(amount) AS total FROM shop.orders GROUP BY status;
-- over another view: its name resolves to its sink table
SELECT game, max(clicks) AS peak FROM clicks_per_window GROUP BY game;
```

| Name in FROM | How the view reads it |
| --- | --- |
| Kafka source | its topic, as today |
| Lake table, in a view with no Kafka source | change feed |
| View | change feed of its sink table |
| Lake table joined to a Kafka source | lookup at the current version, as today |

A lake table joined to a Kafka stream stays a lookup. Read as a change feed, every change to the table would have to be joined against the whole Kafka history kept in state, which has no bound. A lookup joins each event with the table as it is when the event is processed, like Flink's lookup join. The two give different results, so `explain` names each table's mode.

- **Columns** come from `DESCRIBE` when the view is created, plus `_commit_ts TIMESTAMPTZ`, the commit time of the change (see Time and watermark).
- **`start`** is a view option for all its lake tables. `earliest`, the default, reads each table's rows at the first snapshot S0 as inserts, then the changes after S0. `latest` reads only the changes after S0. Neither replays history from snapshot 0, which may be expired and would replay every past version.
- **No hints.** What a view needs from a table follows from the view (Operators and sinks). DuckWake knows the key and append-only property of tables it writes: view sinks and mirror tables.
- **Dependencies** are recorded in the catalog: a cycle (A reads B reads A) is rejected when the view is created, and dropping a view that another view reads is refused unless cascaded.

## Reading changes

Each epoch reads one range of snapshots with `ducklake_table_changes` and maps its change type to a weight:

| change\_type | `__w` |
| --- | --- |
| `insert` | +1 |
| `update_postimage` | +1 |
| `update_preimage` | −1 |
| `delete` | −1 |

```sql
SELECT c.id, c.status, c.amount, s.snapshot_time AS _commit_ts,
       CASE WHEN c.change_type IN ('insert', 'update_postimage') THEN 1 ELSE -1 END AS __w
FROM ducklake_table_changes('lake', 'shop', 'orders', $from, $to) c
JOIN ducklake_snapshots('lake') s USING (snapshot_id)
```

`rowid` and `snapshot_id` are dropped: they name row versions, not values. Summing weights over the values cancels a row inserted and deleted within one range.

Checked on a scratch lake (DuckDB v2.0.0, DuckLake format 1.1) on 2026-11-05:

- The range includes both ends, so reading after position P up to S means `$from = P + 1`, `$to = S`.
- An UPDATE keeps its `rowid` and returns a preimage/postimage pair. A DELETE plus INSERT in one transaction, which is how the upsert sink writes, returns a delete and an insert with a new `rowid`. Both give the same Z-set change.
- `ducklake_flush_inlined_data` keeps the history: the same changes read back after a flush.
- A range across `ADD COLUMN` returns the new schema, with NULL for older rows.
- Starting at an expired snapshot fails with "No snapshot found at version 11". A range whose start survives but whose middle snapshots expired still returns their changes while the files exist. Retention therefore must never pass a view's position (next section).
- A table scan exposes `rowid` and `snapshot_id` (the snapshot that wrote the version), and `AT (VERSION => n)` reads the table at a snapshot.

## Position and exactly-once

The position P is the last snapshot read, one per view for all its lake tables, kept in the checkpoint's `source_offsets` JSON beside Kafka offsets: `{"lake": {"snapshot": 1234}}`. It works like Kafka offsets, inside the same lake as the output.

1. Staging reads the current snapshot id, capped to S = P + `max_snapshots`, and stages each table's changes in (P, S\].
2. The epoch transaction writes the output and the checkpoint with `snapshot = S`.
3. After a crash the job restarts from the committed P and reads (P, S'\] again, so nothing is lost or doubled.

- **One cut across tables.** Every table is read to the same S, so a transaction that changed two tables upstream is seen whole or not at all. Separate positions per table could not promise that.
- **Tables by id.** Inputs are keyed by DuckLake `table_id`, not name: a renamed table keeps working, and a dropped and recreated one has a new id and fails the job with a clear error.
- **Snapshot ids must be gap-free as readers see them.** Each commit takes the next id under the catalog's primary key, so id N+1 should never be visible while N is still committing. To confirm on the Postgres catalog under concurrent writers (F0).
- **The view's own commits** add snapshots to every range. They touch none of its input tables, so they return no rows.
- **Backfill** (`start: earliest`) pages each table at S0 by `rowid`: `SELECT … FROM t AT (VERSION => S0) WHERE rowid > $r ORDER BY rowid LIMIT max_rows`, with position `{"snapshot": S0, "backfill": {"<table_id>": r}}`, then switches to changes after S0. To check that the `rowid` filter is pushed down at a version (F0).
- **Retention.** Maintenance takes the oldest position over every job with lake inputs, P\_min, including S0 during a backfill. It expires only snapshots older than both the configured age and P\_min: `older_than = least(now() - expire_older_than, snapshot_time(P_min))`. A paused job holds snapshots the way a slot holds WAL, so `/metrics` shows the snapshots and seconds each job holds.
- **`reset`** to a position that has expired fails with a clear error, and the only way on is a new backfill. Resetting a view also resets the views that read it, since they would otherwise receive its whole output again as changes.

## Time and watermark

A lake input that can retract takes no part in the view's watermark. An append-only one gives an exact watermark when DuckWake knows where its times come from.

`_commit_ts` is the time of a *change*, not of a row. An update's −1 carries the update's time, not the time the old version was written. A window over `_commit_ts` would put a row's +1 and its later −1 in different windows, so it would count changes, not rows. Windows over a retracting input therefore use a column of the row, and C1 with C4 of CDC sources handle its retractions: open windows take them, and closed ones send them to the late-data table.

| Input | Watermark |
| --- | --- |
| Retracting | none: it does not hold back other inputs |
| Append-only, windows on `_commit_ts` | `_commit_ts` of S: exact, no delay, no late rows |
| Windowed view, windows on its window columns | the upstream's watermark in its checkpoint at S: every later output row has `window_end` above it |
| Append-only, windows on another column | as for Kafka: newest event time minus `watermark_delay`, one partition |

`snapshot_time` is taken before a commit lands, so a writer that started earlier can commit a later id with an earlier time. `_commit_ts` is therefore the running maximum of `snapshot_time` by snapshot id, carried across epochs in the checkpoint. That makes it monotone in commit order, which the exact watermark needs.

## Operators and sinks

Whether a lake input can retract is known for tables DuckWake writes and assumed for every other table. A retracting input needs C1's weighted operator forms and sinks from CDC sources, which this plan reuses and does not repeat.

| Input | Retracts? | Key |
| --- | --- | --- |
| Sink of a stateless or windowed view (append sink) | no | none |
| Sink of a global aggregate or `DISTINCT` (upsert sink) | yes | the `GROUP BY` columns, or the whole row |
| Mirror table of a database source | yes | the source table's primary key |
| Any other lake table | assumed yes | none |

A mirror table holds the current rows, not a log of operations. The ingest job applies each insert, update or delete to it as an `INSERT`, `UPDATE` or `DELETE`, and the change feed turns those back into weights: +1, then −1/+1, then −1. Views never interpret an op column. A lake table that stores a log with an op column is read as an ordinary table; a changelog-table mode, C2 or C3 of CDC sources applied to a lake table, can come later if asked for.

Windowed views are the only ones that need insert-only input today, and only because of how the engine computes them. Their partials are `count(*)` and `sum(x)`, which ignore `__w`: the cheap form for Kafka, where every weight is 1, as Feldera's `ExpandAggregates` picks for append-only input. C1 replaces them with `sum(__w)` and `sum(x * __w)`, so open windows take deletes and updates correctly. Until then, `create_view` and `explain` print the requirement ("needs `shop.orders` to be insert-only"), and every epoch checks it. A stateless view over such a table uses an upsert or counted-rows sink instead.

After C1, only windows that already closed remain a problem: their result was written once and their state dropped. Two ways to handle a retraction into one:

| Option | What happens to window 10:00, emitted as 100, when one of its rows is deleted |
| --- | --- |
| C4: route to the late-data table | the sink keeps 100; the late table gets the row with `__w = -1` for reconciliation |
| Upsert window sink with `lateness`, as in Feldera | the sink is keyed by window and group, and a window's state is kept for `lateness` after it closes: the row becomes 99 within that time, and goes to the late table after it |

| View over a lake input | Retracting input | Append-only input |
| --- | --- | --- |
| Stateless (`Filter`, `Map`, lookup join) | upsert sink when the input has a key, else counted rows | append sink |
| Global aggregate, incl. `min` / `max` | yes, weighted forms | yes |
| `DISTINCT` | yes, weighted | yes |
| `UNION ALL` with a Kafka source | yes, upsert or counted-rows sink | yes |
| Windowed (`TUMBLE`, `HOP`) | after C1 and C4; before, the insert-only check | yes |
| Join of two lake inputs | later: ΔA ⋈ B@P + A@S ⋈ ΔB by time travel, no state | later |
| Stream–stream join (J2) | later | later |

## How other engines handle a change into a closed window

Every engine sits in one of four families, and none routes late retractions to a side table as its main mechanism. Surveyed 2026-11-11 from the engines' documentation as remembered, not re-read; version numbers are to confirm.

| Family | Engines | A change into a window that already closed… |
| --- | --- | --- |
| Windows never close | Materialize; RisingWave (default `EMIT ON UPDATE`); Feldera without `LATENESS` | is applied: the result row is retracted and re-emitted. Output is a changelog. State is unbounded unless a temporal filter or TTL bounds it. |
| State kept for a grace period, then dropped | Flink (`allowedLateness`); Kafka Streams and ksqlDB (grace period); Beam (allowed lateness); Feldera with `LATENESS`; RisingWave `EMIT ON WINDOW CLOSE` | re-fires the window with the corrected value inside the period, so downstream sees an update. Past it the change is dropped: a metric in Kafka Streams and Feldera, a side output in Flink. |
| Input refused | Flink SQL window aggregates over changelog input (for years); Kafka Streams (no windowed aggregate on a KTable); Spark Structured Streaming | cannot happen: windowed aggregates take append-only input only. The documented workaround is a plain `GROUP BY` on a truncated time column, bounded by state TTL. |
| Partition recomputed | BigQuery partitioned materialized views; Databricks Lakeflow/DLT (Enzyme); Snowflake dynamic tables (else a full refresh) | invalidates the matching output partition, which is recomputed from the base table at its new version. |

- **Flink** is the closest precedent for DuckWake today. Its error ("window aggregate doesn't support consuming update and delete changes") is F2's insert-only contract, and its advice is the same `GROUP BY time_bucket` workaround. Recent releases (1.19, to confirm) accept changelog input in window TVFs; a late change then follows the late-event rule: re-fire within the window's state lifetime, drop after.
- **Kafka Streams** splits where DuckWake does: windowed aggregates over streams only, and the adder/subtractor pattern for `GROUP BY` over tables, which is C1's weighted aggregate.
- **Feldera** is this design's second option: a windowed aggregate is an aggregate keyed by window, its output a Z-set change stream, and `LATENESS` says when state may go. Past lateness, late rows are discarded.
- **Materialize** never has the problem, at the cost of unbounded state, bounded by `mz_now()` filters.
- **ClickHouse** materialized views ignore updates and deletes (mutations do not fire them); many lakehouse "incremental" tools do the same.

What this settles:

1. **Side-table routing (C4) is everywhere the fallback past lateness, never the main mechanism.** The primary step is the `lateness` window sink: keyed by window and group, state kept for the period, the sink an upsert. C4 is what happens past it. This reverses the lean under Risks and open questions.
2. **`lateness` does not reach the lake-table cases that matter most.** A privacy erasure or a 3-day rerun touches rows older than any lateness. Every engine that handles those recomputes the partition: F5.
3. **Three regimes, then:** `lateness` for recent corrections, partition-overwrite refresh (F5) for reruns, and insert-only with a stop for the rest. DuckWake can offer all three in one engine, since it already has the change feed and time travel.

## Engine changes

- **Frontend.** `plan::compile` resolves FROM names in order: Kafka source, then view (to its sink table), then lake table. It decides change feed or lookup by the rules in SQL surface, and records each lake input's `table_id`, columns, key and append-only property. A view's sink properties come from its compiled circuit (`Props`, the upsert keys).
- **Catalog.** A dependency table records view → table and view → view. `create_view` rejects cycles; `drop_view` refuses while another view reads it, unless cascaded.
- **Circuit.** `Source` takes `append_only` from the input, true for Kafka. Today it adds `1 AS __w`; instead `SourceInput` says whether the staged table carries `__w`, and `Source` reads it. The snapshot id and `rowid` fill `_partition` and `_offset`, so the column layout stays the same.
- **Inputs.** `Input` gets a kind: Kafka (group, offsets, partitions) or lake (`table_id`, backfill cursor). The view's lake position and last `_commit_ts` live on the job. A lake input has no reader thread. Its staging runs the change query from the staging connection into `memory.typed_l<job>_<table_id>`; reading the lake while writing only the memory catalog keeps the one-database rule.
- **Epoch loop.** An epoch with no new snapshot touching an input table and no Kafka rows is skipped by the existing rule. `duckwake_wait` and catch-up treat lake inputs as caught up when the position is the current snapshot.
- **Checkpoint and commit.** The lake position joins `source_offsets` beside Kafka offsets, in the same transaction. Dead letters do not apply, since the rows are typed already.
- **Maintenance.** It computes P\_min across jobs before `ducklake_expire_snapshots` (Position and exactly-once).
- **`verify`.** It runs the view's batch query over each table `AT (VERSION => P)` and compares the result with the sink as multisets. That is exact and needs no topic retention.
- **`explain` and `/metrics`.** `explain` shows `ChangeFeed(shop.orders)` or `Lookup(shop.orders)` and any insert-only requirement. `/metrics` shows, per view, snapshots behind and lag seconds = now − `_commit_ts` of P.

## Milestones

F1 needs nothing from C1 and can start now. F2 waits for C1.

1. **F0. Spike.** On the Postgres catalog: snapshot ids gap-free for readers under concurrent writers; `rowid` paging at a version; cost of `ducklake_table_changes` per epoch on a table with many files and deletes; how far `snapshot_time` runs out of order. Gate: findings in `docs/internals.md`.
   - Status (2026-11-05, commit `04e1bc5`, internals item 9): F0 done. No gaps or lost changes under 4 concurrent writers. `ducklake_current_snapshot` (6–8 ms) gives the newest snapshot. `rowid` paging at a version is exact and pushed down. One snapshot's changes cost about 50 ms whatever the file count, so F4's shared reads matter. `snapshot_time` never ran backwards.
2. **F1. Views over append-only views and tables.** Name resolution and the dependency table, change feed by snapshot range, one position per view, `start: latest`, the insert-only check, windows on `_commit_ts` and on an upstream view's window columns with exact watermarks, `verify` by time travel. Gate: the epoch-by-epoch harness and the crash suite pass for a chain of three views over Kafka (stateless, then windowed, then windowed again), in order and pipelined; an update to an insert-only input stops the job; cycles and drops of read views are refused.
   - Status (2026-11-06, `scripts/chain_test.sh`, internals item 10): F1 done, uncommitted. The gate passes in order and pipelined: after each of 8 crashes the chain verifies (Kafka for the first view, time travel for the others), dropping a read view is refused, and an update to an insert-only table fails only the view reading it. Departures from this plan: `_commit_ts` is a UTC `TIMESTAMP`, not `TIMESTAMPTZ`, so it compares with Kafka event times; a view with lake inputs commits only for new rows or a closing window, since its own commits move its watermark; a window replaces an upstream view's `window_start` and `window_end`; one epoch reads at most 1,000 snapshots. Still open for F3: P\_min retention, `reset` cascading, `start: earliest`.
3. **F2. Retracting inputs** (after C1). Weights −1, keys from DuckWake-written tables, upsert and counted-rows sinks, a view over a global aggregate's or `DISTINCT`'s sink. Gate: a generator inserts, updates and deletes rows of a lake table, and every view kind matches the batch query `AT (VERSION => P)` after every epoch, through crashes.
   - Status (2026-11-11, `scripts/retract_test.sh`, internals item 11): F2 done. The gate passes in order and pipelined. A generator made 6.8k–7.8k updates and deletes to a lake table during 8 crashes, and after every crash seven views verified against their batch query at P: counted rows, a grouped aggregate with `min` / `max`, `DISTINCT`, an aggregate over a counted view, rows of the aggregate's table, and a `UNION ALL` of the table and the aggregate. A `reset` of the aggregate reaches its readers through the change feed. Departures from this plan: a retraction carries the `_commit_ts` of the version it removes, found by `rowid`, so `_commit_ts` belongs to the row and the batch query is defined; with `start: latest` a view holds the rows written after S0 and drops retractions of older versions; windowed views keep the insert-only check, because with an exact watermark every retraction of an old row lands in a closed window (C4 or `lateness` comes first); a stateless view over a keyed table uses counted rows, not keyed upserts. Still open: keys from DuckWake-written tables, then F3.
4. **F3. Backfill and retention.** `start: earliest` with `rowid` paging, maintenance that keeps P\_min, held-snapshot metrics, clear errors on expired positions, reset cascading to downstream views. Gate: a 10M-row table backfills while updates continue; with aggressive maintenance a paused view resumes exactly; a `reset` past retention fails with the expected error.
5. **F4. Shared and fused reads.** Views at the same position read a table's range once, as shared readers do for Kafka. A chain of views on one worker compiles into one circuit, so a downstream view takes its upstream's changes in memory in the same epoch. Then D1, the Postgres ingest job, writes mirror tables that views read this way.
6. **F5. Partition-overwrite refresh** (after F2's weights). For lake tables whose recent partitions are rerun to correct data, such as the last 3 days rewritten with delete + insert or `INSERT OVERWRITE`. A view whose table carries its input's partition column (such as `date`) is refreshed by partition instead of incrementally: from the change feed in (P, S\] find the partition values the epoch touched, then in the epoch transaction delete those partitions from the view's table and insert the view's batch query over the input `AT (VERSION => S)` restricted to them; the checkpoint moves to S. It runs any SQL the batch query can, keeps no window state for `lateness`, and stays exactly-once through the same position, at the cost of one batch query over the rerun partitions. A view picks it at create time, and `create_view` refuses it when the output lacks the partition column. Gate: a generator reruns the last 3 days of a partitioned table with changed, removed and added rows; after every rerun, through crashes, the view's table equals its batch query at the checkpoint's snapshot.

## Risks and open questions

- **Cost per epoch.** `ducklake_table_changes` may read many files on a table with heavy deletes. F0 measures it; the fallback is a larger `epoch_interval` for views with lake inputs.
- **Lookup or change feed surprises.** The same table is a change feed in one view and a lookup in another, depending on whether a Kafka source is joined. `explain` and `create_view` must say which, so nobody expects past output to update after a lookup table changes.
- **Schema changes.** Columns are fixed when the view is created. Added columns are ignored; a dropped or renamed column fails the job with a clear error. Following schema changes comes with D4.
- **Paused jobs pin snapshots.** Storage grows until the job resumes or is dropped. Is a maximum hold with an alert needed, like `max_slot_wal_keep_size`?
- **Chained views lag one round per hop** until F4 fuses chains on one worker. A downstream view reads only committed snapshots.
- **Insert-only check on foreign tables.** Until C1, a rare delete such as a privacy erasure stops every windowed view over that table. Should it go to the late-data table with its weight instead? That is C4's behavior for closed windows anyway, and it keeps the pipeline running; the leaning was yes, until the survey of other engines (How other engines handle a change into a closed window) settled it: lateness first, C4 only past it. After C1 the question remains only for closed windows: late table (C4) or an upsert window sink with `lateness` (Operators and sinks).
