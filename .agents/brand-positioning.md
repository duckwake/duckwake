# DuckWake — Positioning

## 01 — Category definition
- **Stated category**: stream processing / streaming SQL engine.
- **Actual category**: what a data engineer compares it with when a lake table must stay fresh from Kafka: a Flink (or Flink SQL) job, Spark Structured Streaming, a streaming database (Materialize, RisingWave, Feldera), or Kafka Connect plus a scheduled batch job.
- **Opportunity category**: *continuous views for the lake* — "a SQL view over a stream whose result is an ordinary DuckLake table". Not a database to migrate to and not a cluster to run: an add-on to a DuckDB/DuckLake setup the user already has.
- **Recommendation**: frame DuckWake as the **streaming layer for DuckLake**, not as one more streaming engine. That framing removes Flink and the streaming databases from the head-to-head and puts DuckWake next to the DuckDB tools people already like.

## 02 — Competitive landscape
Axis 1: **Run a platform ← → Add to what you have**. Axis 2: **Broad and general ← → Narrow and exact**.
- **Flink / Spark Streaming**: platform, broad. Own "the industry standard for stream processing"; cost is a cluster and a team.
- **Confluent**: platform, broad. Owns "data streaming platform" (Kafka + Flink as a service).
- **Materialize / RisingWave**: platform (a database you send queries to), fairly exact. Own "streaming database, Postgres-style".
- **Feldera**: closer to exact (incremental computation, DBSP), still a separate engine/service.
- **DuckWake**: *add to what you have* + *narrow and exact*. Unclaimed corner: no one owns "the lake table just stays fresh, exactly once, from your existing DuckDB".

## 03 — Positioning territory
- **The space**: the lightest way to keep a lake table exactly in step with a stream.
- **The audience owned**: data engineers on DuckDB/DuckLake who want fresh tables from Kafka without adopting a streaming platform.
- **The moat**: the guarantee is built into the storage (rows, state and offsets in one DuckLake transaction), and the incremental engine follows DBSP; a platform vendor cannot easily claim "no cluster", and a lightweight script cannot claim "exactly once, verified".

## 04 — Positioning statement
For data engineers who already use DuckDB and DuckLake, DuckWake is the streaming layer that keeps a lake table up to date from Kafka with a plain SQL view, exactly once, because every micro-batch commits its rows, the view's state and the Kafka offsets in one DuckLake transaction.
- *Strategic (internal)*: own "fresh lake tables" for the DuckDB ecosystem before streaming platforms add a DuckLake sink; win on operational lightness and verifiable correctness, not on scale or breadth.
- *Human (public)*: Write a view in SQL. DuckWake keeps the table fresh as events arrive — exactly once, no cluster to run.

## 05 — Proof points
1. Each epoch commits output, state and Kafka offsets in one DuckLake transaction; crash tests kill workers mid-commit and check for no loss or duplicates.
2. One process next to DuckDB; the result is an ordinary DuckLake table any DuckDB client can read.
3. Incremental views (filters, joins with lake tables, tumbling/hopping windows, running totals, DISTINCT, UNION ALL, retractions) on a DBSP operator graph, checked against dbsp as an oracle.
4. Measured: 16 views at 20,000 events/s on one worker, fresh within ~0.7 s at p95 (local benchmark).
5. Late events and undecodable messages land in their own tables instead of disappearing.

## 06 — What DuckWake refuses to be
- Not for teams that need a multi-node streaming platform for millions of events per second.
- Will never compete on "number of connectors" or "enterprise platform".
- Does not try to be a database you migrate to; the lake stays the source of truth.
- Will not claim guarantees the tests do not show.

## 07 — In one sentence
DuckWake keeps DuckLake tables fresh from Kafka with plain SQL views — exactly once, with no streaming cluster to run.
