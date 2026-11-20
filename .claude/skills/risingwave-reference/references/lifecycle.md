# Views over views, backfill, changing views, dbt

Contents: MV on MV · backfill strategies · DDL in the background, rate limits, parallelism · changing a running
view · dbt-risingwave · what to borrow

## MV on MV

An MV can read other MVs; cycles are refused. In the original design:
1. The upstream MV's dispatcher broadcasts its changes.
2. A new MV is wired in on a barrier at epoch e1.
3. A `Chain` operator reads a snapshot of the upstream at e1, then the buffered live changes.

An MV cannot be dropped while others read it.

**DuckWake:** views over views and lake tables (F1–F3) read the upstream table's DuckLake change feed:
- **Position:** each view's position is one snapshot id for all its lake inputs, committed in the epoch
  transaction.
- **Drops:** dropping a view another view reads is refused (`chain_test.sh`).
- **Coordination:** there is no wiring between jobs; the lake's snapshots are the interface.

## Backfill strategies

RisingWave has had three strategies:
- **No-shuffle backfill** (legacy, no longer used for new jobs):
  - Ran at the upstream fragment's parallelism.
  - Its progress was one primary-key offset shared by all vnodes, so it could not resume cleanly after
    rescaling.
- **Arrangement backfill** (default 1.8–2.7):
  - Hash-dispatched, with its own parallelism, reading the upstream's state through a replicated state table.
  - Each epoch it reads a bounded slice of the snapshot in key order, tracks the last key backfilled (per
    vnode), and applies live upstream changes only for keys at or below that position.
  - It resumes from the last checkpoint after a failure. Used for temporal joins and shared sources.
- **Snapshot backfill** (default from 2.8):
  - Reads one consistent snapshot at an epoch, then consumes the upstream's change log from that epoch until it
    has caught up.
  - The final merge waits until the lag is below `snapshot_backfill_finish_max_lagged_barriers`.

Other settings:
- `backfill_order = FIXED(t1 -> t2)` orders the backfill of several inputs (technical preview).
- Serverless backfill (RisingWave Cloud) runs it on separate nodes.

**DuckWake (F3, `docs/design/change-feed-sources.md`, `scripts/backfill_test.sh`):** close to snapshot backfill:
1. A new view over lake inputs records the snapshot S0 when it is created (`start := 'earliest'`) and holds it,
   even while stopped.
2. It reads each input as of S0, `max_rows` rows per epoch in `rowid` order, with a cursor in the checkpoint.
3. Then it reads the change feed after S0.
4. A windowed view closes no window until its backfill is done.

Retention: maintenance keeps every snapshot any view still needs.

Kafka views start at `earliest` or `latest` of the topic; there is no separate backfill.

## Background DDL, rate limits, parallelism during backfill

- **`SET BACKGROUND_DDL = true`:** `CREATE MATERIALIZED VIEW` returns at once. Progress is in
  `rw_catalog.rw_ddl_progress` as a percentage.
- **`max_concurrent_creating_streaming_jobs`:** default 1.
- **`backfill_rate_limit`:** rows per second per parallelism; 0 pauses, -1 means no limit. Change it later with
  `ALTER ... SET BACKFILL_RATE_LIMIT`.
- **`source_rate_limit`:** the same for sources.
- **`streaming_parallelism_for_backfill`, `ALTER ... SET BACKFILL_PARALLELISM [DEFERRED]`:** separate
  parallelism while backfilling.

**DuckWake:** `create_view` returns at once, and a worker picks the view up when it is started:
- **Pacing:** `max_rows` bounds each epoch, both for backfill and for Kafka catch-up.
- **Progress:** in `checkpoints`, metrics (`duckwake_snapshots_held_seconds`), `duckwake jobs`, and
  `duckwake_wait` (dbt's `duckwake_wait_built`).
- **Pause:** `pause`.

Not built: a separate rate limit in rows per second, a backfill progress percentage, and a backfill-only
setting.

## Changing a running view

RisingWave cannot change an MV's query in place:
- **Replace:** create a new MV, drop the old one, then `ALTER MATERIALIZED VIEW new RENAME TO old`. Dependent MVs
  are rebuilt bottom-up.
- **`ALTER MATERIALIZED VIEW ... SWAP WITH`:** exchanges two MVs' names.
- **`ALTER TABLE/SOURCE ADD COLUMN`:** works; existing rows get NULL.
- **Logic changes without downtime:** use `SINK INTO TABLE` with a version column. Run sink v1 and v2 side by
  side, drop v1, then delete the rows with `version < 2`.
- **Filter changes:** discarded rows are gone. To recompute, keep the raw data in a table.

**DuckWake:**
- `create_view(if_exists := 'fail' | 'update' | 'replace')` reports what changed. `update` alters settings only,
  and a changed query fails until `replace`, which recomputes from the start (Kafka retention permitting) or
  from the lake inputs.
- `add_source_column` adds a source column while views run.
- Zero-downtime replace (#51): build `<view>__next` beside, catch up, swap table, state, checkpoints and lease in
  one DuckLake transaction. This is RisingWave's create-new-then-swap, done atomically, which DuckLake allows
  and RisingWave's rename does not.

## dbt-risingwave

Materializations:
- `materialized_view`, the incremental one (`incremental` is deprecated in its favour).
- `source`, `table_with_connector` and `sink`: the model body is the full `CREATE ...` statement.
- `table`, `view`, `ephemeral`.

Indexes come from `indexes=[{'columns': [...]}]`. The docs give no zero-downtime rebuild in dbt.

**DuckWake (dbt-duckwake, `dbt/`, `docs/design/dbt-duckwake.md`):**
- `streaming_source` and `streaming_view` models call the `duckwake_*` functions; the model body is a `SELECT`,
  not DDL.
- A plain `dbt run` never recomputes a view; `--full-refresh` replaces it and its selected readers.
- `duckwake_verified` is a dbt test.
- Upsert settings go in model config.

The DuckWake design compared dbt-risingwave when it was written. Zero-downtime replace (#51) would put DuckWake
ahead here.

## What to borrow

| DuckWake item | From RisingWave |
| --- | --- |
| #51 zero-downtime replace | create beside, then swap: DuckWake swaps in one transaction; RisingWave's `SWAP WITH` names the user-facing operation |
| backfill controls | a rows-per-second limit next to `max_rows`, a progress percentage per view, a backfill-only setting |
| many views | RisingWave's advice past about 300 jobs (fixed parallelism, limit concurrent creation) matches DuckWake's sizing work (#23) |
| backfill order | `backfill_order` for views over several large lake inputs, if a user needs one input first |
