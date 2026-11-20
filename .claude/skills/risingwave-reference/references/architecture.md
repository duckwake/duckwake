# Architecture, checkpoints, state and scaling

Contents: nodes · streaming jobs and actors · barriers, epochs, checkpoints · consistency · recovery · state
store (Hummock) and state tables · vnodes and parallelism · what DuckWake takes from this

## Nodes

RisingWave runs four node types:
- **Frontend (serving)**: stateless, PostgreSQL wire protocol; parses, binds, optimizes, plans. Serves
  queries in local mode (point lookups) or distributed mode.
- **Compute (streaming)**: runs the streaming actors and holds their state. Compute nodes can be `hybrid` (the
  default), `streaming` or `serving`.
- **Compactor**: stateless, compacts Hummock's LSM (levels L0–L6). Suggested sizing is 2:1 compute to
  compactor, and 1:8 during write-heavy backfills.
- **Meta**: the control plane, with metadata in PostgreSQL, MySQL or SQLite and one active node plus a
  standby. Its managers:
  - ClusterManager: membership and heartbeats.
  - StreamManager: builds the stream graph and schedules it.
  - CatalogManager: DDL.
  - BarrierManager: injects and collects barriers, drives checkpoints.
  - HummockManager: the SST manifest and versions.
  - CompactionManager: hands out compaction tasks.

**DuckWake:** no cluster. One binary (`duckwake run`, or `duckwaked` with the SQL functions, or the DuckDB
extension) is the worker. PostgreSQL is DuckLake's catalog and DuckWake's control data (`lake.__duckwake`:
`jobs`, `checkpoints`, `dead_letter`, `lease_<job>`, `state_<job>`, `worker_<id>`), and the data files sit on
local disk or S3. DuckLake's own compaction and expiry run as worker maintenance (`maintenance.rs`). There is no
query layer: any DuckDB reads the lake.

## Streaming jobs, fragments, actors

`CREATE MATERIALIZED VIEW` goes through these steps:
1. The frontend's stream planner builds a stream plan.
2. Meta's fragmenter cuts it into fragments, the pieces between exchanges.
3. Each fragment runs as N actors (its parallelism), scheduled on compute nodes.
4. Meta starts the job.

An actor is a merger (which merges upstream channels and aligns barriers), a chain of executors (one per
relational operator), and a dispatcher (hash, broadcast, round-robin or no-shuffle). It runs on tokio. Actors
on one node talk through channels, and actors on different nodes through an RPC exchange service. Executors
pass stream chunks: columnar rows, each with an op (`+`, `-`, `U-`, `U+`).

**DuckWake:** a view is a DBSP circuit (`crates/circuit`): Q over whole collections, then incrementalized as
D∘Q∘I with integrators pushed forward. Each operator (`crates/circuit/src/ops/*.rs`) generates SQL text for
staging (in memory, before the epoch's transaction) and for commit (inside it). Weights `__w` (+1/−1) play the
part of RisingWave's ops, and an update is a −1/+1 pair. There are no actors, exchanges or dispatchers: one
worker runs a whole view, and DuckDB parallelizes each statement inside.

## Barriers, epochs, checkpoints

Every `barrier_interval_ms` (default 1000 ms; `checkpoint_frequency` 1) the meta node does the following:
1. It creates a barrier and sends it to every source actor.
2. Each actor forwards the barrier downstream. Fan-out operators copy it; fan-in operators (merge, join) wait
   until it has arrived from every input.
3. When an operator sees the barrier, it hands its dirty state to the node's shared buffer. The flush to S3
   is asynchronous: one SST per node per epoch, then the node tells meta.
4. When every node is done, meta commits the epoch in Hummock's version.

A checkpoint does two jobs: it is the recovery point, and it is the snapshot that reads see. Writes become
visible only after the checkpoint that commits them. `FLUSH` forces one; reading your own writes is not
guaranteed otherwise.

Only one checkpoint runs at a time. Epochs come from wall-clock time plus a serial number, so neighbouring
epochs are not +1/−1 apart. Unaligned joins (`streaming_enable_unaligned_join`) put a log store after a join
with high amplification, so barriers are not held up behind it.

**DuckWake:** a worker runs one epoch for all the views it holds (`engine.rs` header). Each view has its own
`epoch_interval` (default 10 s) and its own checkpoint row. The commit is one DuckLake transaction for all those
views, so it plays the part of RisingWave's checkpoint for one worker: one snapshot per worker per epoch. It
holds:
- the lease fence;
- the sink rows, and the windows that closed;
- the state changes;
- the late and dead-letter rows;
- one checkpoint row per view, with its Kafka offsets per partition or its lake snapshot position.

There is no barrier across workers. A view over a view reads the other view's committed snapshots through
DuckLake's change feed, so the lake's snapshots are the only coordination between them. With `--pipeline`, the next epoch is staged while the current one commits; the next
epoch's state reads go through an overlay of the previous epoch's staged changes (see `ops/upsert.rs` and the
aggregate operator). `--lean-commit` batches the control writes.

## Consistency and recovery

RisingWave promises consistency (a query sees the result as of some committed epoch) and monotonic reads. On a
compute-node failure, or a checkpoint that fails, meta rolls the whole graph back to the last committed epoch
and replays from the sources' offsets kept in that checkpoint. The rollback is global.

**DuckWake:** recovery is per view. A worker that dies loses its lease; another worker takes the view (after the
lease timeout) and resumes from the view's last checkpoint row: the Kafka offsets, or the lake snapshot (plus a
backfill cursor). A worker whose lease was taken over is fenced: its commit fails, because the lease epoch is
checked inside the transaction. Failures stay local: one failing view is marked `failed` and the others keep
running (limits like `max_state_rows`). The suites `crash_test.sh`, `fencing_test.sh`, `failover_test.sh`,
`chain_test.sh` and `upsert_test.sh` inject aborts at every step of the epoch.

## State store and state tables

**Hummock** is RisingWave's LSM key-value store on S3, built for the streaming engine rather than for general
use:
- **Workload:** each executor reads and writes only its own keys, partitioned by `table_id | vnode`, and
  commits serially by epoch.
- **Keys:** a full key is the user key plus an 8-byte epoch. Entries sort by key ascending, then by epoch
  descending.
- **SSTs:** blocks of about 64 KB with a meta file holding the index and a bloom filter.
- **Read path:** MemTable, then shared buffer, then block cache, then the Foyer disk cache, then S3.
- **Pinning:** reads pin a snapshot and a version so that compaction and vacuum keep what they need.
- **Version:** the SST list plus `max_committed_epoch`.

**State tables** are the operators' view of Hummock:
- **Row encoding:** one key-value pair per row, not per cell.
- **Layers:** a per-epoch MemTable merged over the committed storage table (the memory version wins), so an
  operator reads its own writes within an epoch.
- **Key layouts:**
  - MV: `table_id | sort key | pk`
  - Top-N: `table_id | sort key | pk`
  - Join side: `table_id | join key | pk`
  - Aggregate: `table_id | group key`

**Aggregate state:**
- Value aggregates (`sum`, `count`) keep one row per group.
- `min`/`max` over input that can retract keep a "materialized input" table per call:
  `group | sort key | upstream pk`, sorted ascending for min and descending for max, so a cache miss loads only
  the top slice.
- Over append-only input, `min`/`max` keep a single value per group.
- Groups are cached in memory (`AggGroupCache`).

**DuckWake:** state is DuckLake tables (`lake.__duckwake.state_<job>`), written by SQL in the epoch's
transaction, compacted and expired by DuckLake maintenance:
- **Aggregates:** partial sums (`avg` becomes sum and count, variances become count, sum and sum of squares).
- **`MIN`/`MAX` over retracting input:** value rows `(__i, __v<i>, __n)`. This is the same idea as RisingWave's
  materialized input; the extreme is recomputed for the epoch's groups.
- **`count(DISTINCT)`, quantiles:** value rows with counts.
- **Upsert views:** the current row per key.
- **Reads:** state is read by joining it to the epoch's touched keys. There is no cache: each epoch's SQL reads
  the rows it needs.
- **Small files:** DuckLake inlining keeps small inserts in the catalog
  (`docs/design/performance-backlog.md`).

The architecture review (`docs/design/architecture-review.md`) names state write amplification and the
catalog on the hot path as DuckWake's weak points: this is where Hummock's design spends its effort.

## Vnodes and parallelism

**Vnodes:**
- A distribution key hashes to a fixed set of vnodes, and meta maps vnodes to actors.
- The storage key starts with the vnode, so an actor's data is contiguous and storage does not depend on the
  mapping.
- Scaling changes only the mapping. In the doc's example, going from 3 to 4 units with a good mapping moves a
  quarter of the data.

**Parallelism settings:**
- **Values:** `n`, `adaptive`/`0`, `bounded(n)` or `ratio(r)`, set per job (`ALTER MATERIALIZED VIEW ... SET
  PARALLELISM`) or per fragment (`ALTER FRAGMENT`).
- **Defaults:** `default` resolves to `bounded(64)` (tables and sources to `bounded(4)`). Adaptive follows the
  cluster's cores.
- **Backfill:** has its own `BACKFILL_PARALLELISM`.

**Rescaling:**
- Reconfiguration rides on a barrier, and new actors read their vnodes' state from S3.
- Above about 300 jobs, the docs advise fixed parallelism (`disable_automatic_parallelism_control`,
  `default_parallelism`) to avoid actor explosion: watch `rw_actors`, with 50,000 as the warning level.

**DuckWake:** the unit of parallelism is the view:
- Leases spread views over workers; rebalancing moves views from the busiest worker when it is over (1+ε) times
  the average cost (`failover.rs`, `docs/design/worker-rebalancing.md`). `drain` empties a worker, and
  `start --worker` pins a view.
- Within a worker, `--stage-threads` stages views in parallel, and DuckDB parallelizes each statement.
- Splitting one topic's partitions across workers (#50) is the step that would need something like vnodes:
  state partitioned by a key hash, so that partitions of one view can run on different workers.

## What DuckWake takes from this

- **Taken:** epochs as the unit of atomic state and output commit; a source position stored with the
  checkpoint; recovery by replay from the checkpoint; aggregate state split into value and materialized-input
  forms; a log of per-epoch changes (DuckLake's change feed plays the part of Hummock's epochs for views over
  views).
- **Not taken:** a global barrier (DuckWake views commit independently, and readers see the committed snapshot of
  each view); its own LSM store (DuckLake on Parquet instead); actors and exchanges (one worker per view);
  in-memory state caches (each epoch is stateless SQL against state tables).
- **To borrow when needed:**
  - Vnode-style hashing if #50 (one topic's partitions across workers) is built.
  - Per-job backfill parallelism and rate limits, which map onto the existing `max_rows`.
  - The read-your-writes rule (no guarantee before the commit), which DuckWake already has: rows appear in the
    table when the epoch commits.
