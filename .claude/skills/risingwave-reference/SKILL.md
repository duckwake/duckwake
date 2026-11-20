---
name: risingwave-reference
description: >-
  How RisingWave (the streaming database) designs and implements each part of a streaming engine, mapped to
  DuckWake's code, design docs and roadmap issues. Use it whenever you design, plan, compare or build a
  DuckWake feature that RisingWave also has, even if RisingWave isn't named: sources and formats (upsert,
  Debezium, Avro, schema registry, Kafka keys/headers), CDC from PostgreSQL/MySQL, joins (stream-stream,
  interval, temporal/lookup, as-of), top-k and window functions, watermarks, emit-on-window-close and late
  data, TTL and state cleanup, backfill and views over views, checkpoints/epochs/exactly-once, state layout,
  scaling and parallelism, sinks, changing a running view, dbt. Also use it to answer "how does RisingWave do
  X" or "should DuckWake work like RisingWave here".
---

# RisingWave as a reference for DuckWake

RisingWave is the closest mature design to what DuckWake does: incremental SQL views kept current from Kafka
and CDC, with state in object storage. It has solved many problems DuckWake will meet next, and some of its
answers do not fit DuckWake. This skill holds what RisingWave does, sourced and dated, and where each piece sits in
DuckWake. Use it to design a feature from known answers rather than from scratch, and to say plainly where
DuckWake differs.

## The two engines in one paragraph each

**RisingWave**: a distributed cluster (frontend, compute, compactor and meta nodes). A view is planned into
fragments, run as many actors in parallel, connected by hash dispatchers. Every operator keeps its state in
Hummock, RisingWave's own LSM store on S3. The meta node injects a barrier every second (Chandy–Lamport); each
barrier is an epoch, and a checkpoint commits all state at once. Results are queried from RisingWave itself or
sent out through sinks.

**DuckWake**: a library and worker process around embedded DuckDB. A view compiles into a DBSP circuit of
operators, each producing plain SQL. A worker runs one epoch for all the views it holds: it reads Kafka or each
view's DuckLake change feed, stages SQL in memory, then commits every view's sink rows, state tables and
checkpoint row in **one DuckLake transaction**, one snapshot per worker per epoch. The result is an ordinary DuckLake table that any DuckDB can read, and `verify`
recomputes it as a batch query to prove it exact. Scale-out is by view: leases, failover and rebalancing.

## Workflow: designing a DuckWake feature with this skill

1. **Find the RisingWave design.** Open the reference for the area (table below) and read the RisingWave
   section and its DuckWake mapping. Check `references/duckwake-map.md` for the feature's status and its issue.
2. **Check it against DuckWake's invariants.** A RisingWave answer fits only if it keeps these:
   - One epoch, one DuckLake transaction: output, state and source positions commit together. That is how
     DuckWake is exactly-once, and nothing outside the transaction may be needed for recovery.
   - Every view equals a batch query over its inputs, which `verify` and the circuit tests check. A feature
     needs a batch definition before it gets an incremental one.
   - Nothing new to run: no cluster, no separate state store, no connector node.
   - State is DuckLake tables, written once per epoch in SQL. RisingWave's in-memory caches and LSM tricks
     do not carry over; what costs RisingWave a cache miss costs DuckWake a SQL join against a state table.
   - Prefer simple, well-tested designs (the user's standing rule): copy RisingWave's semantics where they
     are the industry's (as with equal versions in upsert), and drop machinery that exists for its cluster.
3. **Write down the differences.** In the design document (`docs/design/<topic>.md`), add a short "How
   RisingWave does it" section with what was adopted, what was not and why, and the source URLs with the date
   read. Earlier examples: `docs/design/cdc-sources.md` (Flink, Materialize, Feldera, RisingWave compared),
   `docs/design/dbt-duckwake.md`.
4. **Verify anything version-sensitive.** RisingWave changes fast: defaults and names moved several times
   between 1.x and 3.x (backfill strategies, parallelism settings, `DO UPDATE FULL` became `OVERWRITE`). This
   skill was written from the docs on 2026-11-20. Before relying on a detail in a design or PR, check the
   current page (`references/sources.md` lists them). Mark anything unconfirmed as such.

## References

| Area | File | Read it for |
| --- | --- | --- |
| Architecture, checkpoints, state, scaling | `references/architecture.md` | barriers and epochs, consistency and recovery, Hummock and state tables, vnodes and parallelism |
| Streaming model and operators | `references/operators.md` | stream keys, append-only, aggregates, joins, top-N, window functions, watermarks, EOWC, TTL, temporal filters |
| Sources, CDC and sinks | `references/sources-and-sinks.md` | source vs table, formats, `INCLUDE`, upsert and version columns, PostgreSQL CDC, sinks and delivery |
| Views over views, backfill, changing views, dbt | `references/lifecycle.md` | MV on MV, backfill strategies, background DDL, rate limits, altering jobs, swap, dbt-risingwave |
| Feature map | `references/duckwake-map.md` | each RisingWave feature: built, planned (issue), or not planned in DuckWake; what to borrow next |
| Sources read | `references/sources.md` | the URLs behind every claim, and what could not be confirmed |

## Things that are easy to get wrong

- **Barrier interval.** RisingWave's barrier and checkpoint interval defaults to 1 s (`barrier_interval_ms`). One
  2026 blog post says 10 s; the docs say 1 s. DuckWake's default epoch interval is 10 s, per view.
- **Sources hold no data in RisingWave.** `CREATE SOURCE` is a catalog entry; `CREATE TABLE ... WITH
  (connector=...)` stores the rows. DuckWake's sources are like RisingWave's sources, and a DuckWake view's
  table is like a RisingWave MV that is also a plain table in the lake.
- **Upsert and Debezium need a table in RisingWave.** An upsert stream cannot feed joins, aggregates or windows
  directly; RisingWave materializes it first to find each update's old row. DuckWake's upsert views (C2) work
  the same way: the view keeps the current rows, and aggregates go in a view that reads it.
- **RisingWave MVs cannot change their query in place.** You create a new MV, drop the old one and rename.
  Only `SINK INTO TABLE` with a version column changes logic without downtime. DuckWake's planned
  zero-downtime replace (#51) is the same build-beside-and-swap idea.
- **Exactly-once to external systems** in RisingWave holds only for Iceberg (with sink decoupling); most sinks
  are at-least-once. DuckWake's only output is its DuckLake table, written in the epoch transaction.
