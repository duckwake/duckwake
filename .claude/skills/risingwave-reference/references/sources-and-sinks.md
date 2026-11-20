# Sources, CDC and sinks

Contents: source vs table · formats and encodings · the `INCLUDE` clause · upsert, primary keys, `ON CONFLICT`
and version columns · shared sources · PostgreSQL CDC · sinks and delivery · what to borrow

## Source vs table

| | `CREATE SOURCE` | `CREATE TABLE ... WITH (connector=...)` |
| --- | --- | --- |
| Holds data | no: a catalog entry; each reading MV or sink runs a source executor | yes, in Hummock |
| Primary key | only for `FORMAT UPSERT` | yes; `INSERT` of an existing key overwrites by default |
| `UPDATE`/`DELETE` by users | no | yes |
| New MVs backfill from | the external system (Kafka offsets) | the table |
| Needed for | plain append streams | Debezium, upsert into a stored table, CDC (`FROM shared_source TABLE ...`) |
| `APPEND ONLY` | n/a | optional: no key, updates and deletes refused, cheaper operators downstream |

An upsert source cannot feed a join, an aggregate or a window ("upsert stream is not supported as input"). An
upsert carries no old value, so the stream must be materialized first, by a table or an MV, to emit the matching
delete.

**DuckWake:** a source is a Kafka topic with a schema (`duckwake_create_source`), holding no data, like a
RisingWave source. A view's table is the stored result. An upsert view (C2, `upsert_key` on `create_view`) plays
the part of RisingWave's upsert table. It keeps each key's current row as state, so the same topic can also feed
event views, and aggregates go in views that read it through the change feed. RisingWave's `FORMAT UPSERT`
needs `CREATE TABLE`; DuckWake chose the view setting (the user's decision while designing C2) so no new kind
of object was needed.

## Formats and encodings

- **FORMAT:** `PLAIN`, `UPSERT`, `DEBEZIUM`, `DEBEZIUM_MONGO`. Maxwell and Canal appear in older material; the
  current docs do not list them.
- **ENCODE:** `JSON`, `AVRO`, `PROTOBUF`, `CSV`, `BYTES` (one `BYTEA` column), `PARQUET` (file sources).
- **Combinations:** `PLAIN`/`UPSERT`/`DEBEZIUM` with JSON or Avro; `PLAIN` with the rest; `DEBEZIUM_MONGO` with
  JSON.
- **Schema registry:** `ENCODE AVRO (schema.registry = 'url1,url2', ...)`.
  - Several URLs are tried in turn.
  - The subject is TopicNameStrategy, `{topic}-value`, by default.
  - Username and password go together.
  - `schema.registry.ca_pem_path` takes a CA file.
- **Protobuf:** `message = 'pkg.Msg'` plus `schema.location` (a `FileDescriptorSet` built with `protoc
  --include_imports`) or a registry.
- **Timestamps:** `timestamptz.handling.mode` picks how JSON timestamps parse; the default `guess_number_unit`
  works for 1973–5138.
- **Avro maps:** `map.handling.mode = 'map' | 'jsonb'`.

**DuckWake:** JSON only:
- Arrow decode when the column types allow it, else SQL decode with identical results (`decode.rs`).
- A message that does not decode, or lacks its event time, is a dead letter with its error.
- Avro and Protobuf with a schema registry are #49.

When building #49, borrow:
- RisingWave's option names;
- several registry URLs tried in turn;
- the TopicNameStrategy default;
- the rule that the registry's schema decides the columns: in DuckWake, a source column list checked against
  the registry at create time.

## The `INCLUDE` clause

`INCLUDE key | header | offset | partition | timestamp | payload [AS col]` adds message metadata as columns:
- **Default names:** `_rw_{connector}_{col}`.
- **Headers:** `INCLUDE header` gives `List<Struct<varchar, bytea>>`; `INCLUDE header 'name'` gives one header.
- **Payload:** `INCLUDE payload` gives the raw JSON.
- **Upsert:** with `FORMAT UPSERT`, `INCLUDE KEY AS col` is required. The included key becomes the primary key,
  and a composite primary key is not allowed then.

**DuckWake:**
- **Staging:** every message's `_partition`, `_offset` and (since C2) `_key` are staged, and the dead letters
  keep the payload.
- **Ad hoc queries:** `duckwake sql` over a source shows `_partition`, `_offset`, `_error`, `_payload` and
  `_key`.
- **Not yet in views:** views cannot select those columns. Exposing keys and headers as columns is part of #49.
  `INCLUDE`'s explicit opt-in per column is a good model, because it keeps the source's schema stable.
- **Upsert keys:** DuckWake's upsert key reads the key without declaring it. It reads a JSON object key's
  fields, so composite keys work, which RisingWave refuses for included keys.

## Upsert, primary keys, `ON CONFLICT`, version columns

A table with a primary key resolves conflicts on insert, whether from `INSERT`, a connector or a sink into it:
- **`DO NOTHING`:** keep the old row.
- **`OVERWRITE`:** the default, replace the whole row (called `DO UPDATE FULL` in 1.x).
- **`DO UPDATE IF NOT NULL`:** replace only the new row's non-NULL fields, for partial updates. Defaults are not
  allowed with it.
- **`WITH VERSION COLUMN(v1[, v2...])`**, on `OVERWRITE` or `DO UPDATE IF NOT NULL`:
  - The new row applies only if its version is ≥ the existing one and not NULL.
  - Several columns compare left to right (v2.6+).
  - An update that only bumps the version is compacted to a no-op downstream.
  - Uses: event-time versions to ignore late data, or integer versions for zero-downtime logic changes.

The docs found no tombstone retention. With `FORMAT UPSERT`, a NULL value deletes the key.

**DuckWake (C2, PR #60):** `upsert_key` and `upsert_version` on the view:
- **Key:** from the message key (a JSON object field, or the whole key), else from the value.
- **Version:** a message applies when its version is ≥ the row's. Equal versions go to the later message, as in
  RisingWave. NULL versions count lowest (RisingWave ignores a NULL version).
- **Deletes:** a tombstone deletes the key, and the view keeps nothing of deleted keys, so a later message
  recreates the key.
- **State:** one row per live key.

Not built:
- partial updates (`DO UPDATE IF NOT NULL`);
- composite version columns.

Both are small additions to `UpsertSpec::newer`/`order`, and worth doing only on request.

## Shared sources

From v2.1, a Kafka source is shared by default (`streaming_use_shared_source`): one consumer per source, its
output dispatched to every MV that reads it, with each new MV backfilling up to the shared position.

**DuckWake:** `--shared-readers` gives the views of one source that are at the same position one Kafka reader. A
view that is behind reads on its own until it reaches the shared reader's position, then joins it
(`docs/performance.md`). J3 (#33) extends sharing across views and sources.

## PostgreSQL CDC

A shared CDC source, then one table per upstream table:

```sql
CREATE SOURCE pg WITH (connector='postgres-cdc', hostname='...', port='5432', username='...',
  password='...', database.name='db', schema.name='public', slot.name='rw_slot');
CREATE TABLE orders (id INT PRIMARY KEY, ...) WITH (snapshot='true') FROM pg TABLE 'public.orders';
CREATE TABLE t (*) FROM pg TABLE 'public.t';   -- all columns
```

**Connection and setup:**
- RisingWave creates and manages the publication (`rw_publication`) and the replication slot.
- `publish_via_partition_root` is set for partitioned tables.
- PostgreSQL 10–18 are supported.

**Initial snapshot:**
- `snapshot='true'` (the default) backfills existing rows in chunks of `snapshot.batch_size` (1000).
- Parallel backfill splits the table into ranges (`backfill.parallelism`, `backfill_num_rows_per_split`,
  `backfill_as_even_splits`; the docs spell these two ways).
- Then it streams changes, with the table as the consistent result.

**Change format:**
- Changes arrive in Debezium's JSON format, with `debezium.*` options passed through.
- TOAST columns are rebuilt from RisingWave's own state, so `REPLICA IDENTITY DEFAULT` is enough.

**Schema changes:** `auto.schema.change` (ADD and DROP COLUMN, a premium feature).

**Limits:**
- Unsupported types: `ltree`, `hstore`, `bit(n>1)`.
- `NUMERIC` with more than 28 digits becomes NULL unless the column is mapped to `rw_int256` or `varchar`.
- Lost WAL fails with "Last recorded offset is no longer available".

**DuckWake:** the plan is D1 (PostgreSQL, logical replication with `pgoutput`, #44), D3 (MySQL by GTID, #45) and
D4 (large tables and operations, #46), in `docs/design/database-cdc-sources.md`:
- Native Rust connectors, no Debezium and no JVM.
- One ingest job per database writes mirror tables in the lake, committing rows and the replication position
  in one DuckLake transaction.
- Views then read the mirror tables through the change feed, as with any lake table.

Points to take from RisingWave:
- **Managed setup:** manage the slot and publication for the user, with an option to bring your own.
- **Snapshot then stream:** chunked by primary-key ranges, with the snapshot's LSN as the start.
- **TOAST:** rebuild unchanged columns from the mirror table instead of requiring `REPLICA IDENTITY FULL`.
- **Type mapping:** a table, with a fallback to `VARCHAR`.
- **WAL loss:** a clear error when the WAL needed is gone.
- **Schema changes:** ADD and DROP COLUMN first.

## Sinks and delivery

**Creating sinks:** `CREATE SINK name [INTO table] AS SELECT ... WITH (connector=..., type='upsert' |
'append-only', ...) FORMAT ... ENCODE ...`.
- **Connectors:** about 25: Kafka, Iceberg, JDBC, ClickHouse, Snowflake, BigQuery, S3 and others.
- **Upsert:** upsert sinks need a primary key.
- **`force_append_only`:** drops deletes.
- **`EMIT ON WINDOW CLOSE`:** gives append-only output from windows.

**Delivery semantics:**
- **Exactly-once:** only Iceberg (`is_exactly_once`, the default), and only with sink decoupling. It commits a
  snapshot through a coordinator, by default every 60 checkpoints.
- **At-least-once:** the other sinks, Kafka included.

**Sink decoupling:**
- It is on by default: a log store buffers between the stream and the external system, so a slow sink does not
  hold up barriers.
- Commits happen every `commit_checkpoint_interval` (10 when decoupled).
- When the stream key differs from the sink's key, changes are buffered and compacted within each barrier.

**File sinks** (S3, GCS and others): append-only, rolling files by `rollover_seconds` (10) or `max_row_count`.

**`CREATE SINK INTO table`:** feeds a table from several streams.
- **Wide tables:** several sinks write different columns of one keyed table, with `ON CONFLICT DO UPDATE IF NOT
  NULL`.
- **Logic changes without downtime:** a second sink writes a new version beside the old one, then the old sink
  is dropped.

**DuckWake:** the view's DuckLake table is the only output, written in the epoch's transaction, so it is
exactly-once and readable by any DuckDB, Spark or Trino that reads DuckLake. There are no external sinks, and
none are planned: the positioning is "your lake is the result". If one is ever needed, RisingWave's lessons
apply:
- decouple with a log, which could be DuckLake's change feed itself;
- commit downstream idempotently by epoch;
- be honest about at-least-once where the target has no transactions.

Several views writing one table (sink-into-table) is listed in `docs/design/streaming-extension.md` as "a
continuous `INSERT INTO`, left for later".

## What to borrow

| DuckWake item | From RisingWave |
| --- | --- |
| #49 Avro/Protobuf, keys and headers | option names, several registry URLs, TopicNameStrategy, `INCLUDE`-style opt-in metadata columns |
| #42 Debezium topics (C3) | Debezium envelope handling, snapshot reads at the lowest version, `ignore_key`, deletes of unknown keys ignored |
| #44–#46 database CDC | managed slot and publication, chunked snapshot by key range, TOAST from own state, type table, lost-WAL error |
| upsert follow-ups | composite version columns, `DO UPDATE IF NOT NULL` partial updates, only on request |
| several views into one table | `SINK INTO TABLE` with `ON CONFLICT`, if users ask for wide tables |
