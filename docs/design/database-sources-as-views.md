# Database sources as definitions, copies as views

Date: Nov 23, 2026. Status: proposed, not built.

## The problem

A Kafka source is a definition only: `duckwake_create_source('clicks', 'kafka', ...)` names a topic and its
columns, and nothing reads the topic until a view does. A database source is more than that. Since D1 and D3,
`duckwake_create_source('shop', 'postgres', {"tables": [...]})` also creates a mirror table per listed table
(`lake.shop.orders`, every column) and an ingest job, named `shop`, that keeps them. So the same word means two
things:

| | Kafka source | Database source today |
| --- | --- | --- |
| What `create_source` makes | a definition | a definition, a mirror table per table, an ingest job |
| Where the data is | the views' tables | the mirror tables, then the views' tables |
| What you start, stop, reset, verify | each view | the source as a whole, then each view |
| Who picks a copy's name and columns | the view's SQL | the source: lake schema from its `schema` option (the source's name by default), the table's own name, every column |

The question this document answers: why is a database source not a definition too, with views that read it
and only select, so that "the source is the definition, the view is the data" holds for every kind of source?

## Why views cannot read the database the way they read Kafka

A view over a topic keeps its own offset, so ten views are ten independent readers, and Kafka does not notice.
A database's change stream is not a log anyone can read again:

| | Kafka topic | PostgreSQL slot / MySQL binlog |
| --- | --- | --- |
| Each reader costs the database | nothing | PostgreSQL: a replication slot, whose decoder reads the whole WAL on the primary. MySQL: a binlog dump thread and a `server_id`. |
| Data kept for a slow reader | the topic's retention, shared by all | PostgreSQL: each slot pins WAL until it advances, so one stopped reader fills the primary's disk. MySQL: purged after `binlog_expire_logs_seconds`, whoever has not read it. |
| Starting a reader | read from offset 0 | a consistent snapshot of the tables, on the production database |
| Reset, backfill, verify | read the topic again | the WAL cannot be read again: only a new snapshot |
| Limits | none in practice | PostgreSQL: `max_replication_slots`, 10 by default (and `max_wal_senders`, also 10, for readers on the replication protocol; DuckWake reads through SQL functions and uses none). MySQL: one `server_id` per reader. |

So views keep reading a copy in the lake, never the database. The copy plays the part a topic plays for Kafka:
one durable input that many views read at their own pace through DuckLake's change feed, with reset, backfill,
windows over updates and deletes, and `verify` working as for any lake table. This was the reason for one
ingest job per source in [database CDC sources](database-cdc-sources.md#architecture), and it stands.

What changes is who declares the copy.

## The proposal

A database source becomes a definition: where the database is and how to reach it. Each copy is a view the user
creates, whose query may only select columns of one table of the source. Call it a **copy view**.

```sql
CALL duckwake_create_source('shop', 'postgres', '{
  "dsn": "host=db.internal dbname=shop user=duckwake", "password": "env:SHOP_PG_PASSWORD"}');

CALL duckwake_create_view('raw.orders',
  'SELECT order_id, customer_id, status, amount, ordered_at FROM shop.public.orders');
CALL duckwake_create_view('raw.customers',
  'SELECT customer_id, country FROM shop.public.customers');          -- name and email stay out of the lake
CALL duckwake_start('raw.orders');
CALL duckwake_start('raw.customers');

-- everything else reads the copies, as today
CALL duckwake_create_view('analytics.orders_by_country',
  'SELECT c.country, count(*) AS n, sum(o.amount) AS amount
   FROM raw.orders o JOIN raw.customers c USING (customer_id) GROUP BY ALL');
```

What the user gets:

- **One model for every source.** `create_source` defines; `create_view` makes data; `start`, `stop`, `reset`,
  `verify` and `drop` act on views, the copies included.
- **Names and columns are the user's.** A copy goes in any schema under any name, and columns can be left out
  or renamed. A column left out never reaches the lake, which matters for personal data.
- **Per-table control.** One table's copy can be stopped, re-copied or dropped while the others keep following
  the database.
- **Fewer stops on schema changes.** A copy that names its columns does not care about a column added to, or
  dropped from, the others (see [Schema changes](#schema-changes)).
- **dbt fits with one change.** A copy view is a `streaming_view` model whose SQL selects from
  `{{ ref('shop') }}.public.orders` (`ref()` to a source model renders the source's name). The change:
  `streaming_source` creates only Kafka sources today, and needs a `kind` config for `postgres` and `mysql`.

### What a copy view's query may contain

The query reads exactly one table of one database source and lists columns:

- `FROM <source>.<schema>.<table>`, always in three parts; for MySQL the schema is the database
  (`FROM shop.shop.orders`). This is how DuckDB names a table of an attached PostgreSQL database. Two parts
  would be ambiguous: in a view's `FROM`, `x.y` is the lake table `lake.x.y`, and a lake schema may share a
  source's name. Source names `lake` and `memory` become reserved.
- Column names, each optionally renamed with `AS`, or `*`. Every primary key column must be selected.
- Nothing else: no expressions, casts, `WHERE`, joins, aggregates or `DISTINCT`. `create_view` refuses them with
  the next step in the message: create the copy, then a view over it.

Why so strict: a copy that is a projection of one table, keyed by the table's primary key, is what keeps the
ingest rules of D1 and D3 true. Updates and deletes apply by key, a changed key deletes the old row, unchanged
TOAST values come from the stored row, and `verify` compares the copy with the table read now, exactly. A
`WHERE` or an expression would turn the copy into a computed result, whose updates need the engine, not the
ingest job. Real transforms belong to the views over the copies, which already handle updates and deletes.

Any other view that names a database source's table in `FROM` is refused with the same message. Two copy views
may read one table, with different columns.

### One reader per source

Nothing about the database side changes: one slot (PostgreSQL) or one binlog reader (MySQL) per source, one
snapshot mechanism, one position. The source's ingest job stays, as an internal job named like the source:

- It runs on one worker under one lease, as today, from the source's creation until the source is stopped
  (see [Stopping the whole source](#stopping-the-whole-source)). Copy views have no lease of their own: they
  are written by the source's job, so they run where it runs and move with it when workers rebalance.
- Each epoch it reads the source's changes once and applies each table's changes to the copy views that select
  that table, projected to their columns. The copies, each copy view's checkpoint and the source's position go
  into one DuckLake transaction, so every copy of a source shows the same source transactions after every
  epoch, as the mirrors do now.
- Changes for tables no running copy view selects are read and dropped. For PostgreSQL the publication holds
  only the tables some copy view selects, so the database does not send the others at all.
- `duckwake_jobs()` lists the job, with its lag and status, and each copy view, with its own status
  (`running`, `joining`, `stopped`, `failed`). A failure of the job (slot lost, binlog purged) fails the copy
  views that read it, with the same message.

### Starting, stopping and resetting a copy view

The hard choice is what a stopped copy view holds. If each copy view had its own position in the stream, the
slot could advance only to the oldest of them, and one copy stopped for a weekend would pin the primary's WAL for
all the others. That is the failure the one-reader design exists to avoid.

So a copy view never holds the stream back. It is either following the source's position or not reading at all:

- **Stop** (or pause: for a copy view they are the same) leaves the copy as it was at that epoch, and lets go:
  the job no longer applies that table's changes to it.
- **Start** (the first one, or after a stop) **joins** the stream: the job copies that one table under a new
  snapshot while the others keep streaming, and writes it as a **difference** against what the copy holds. The
  rows that changed while it was stopped arrive as updates, inserts and deletes; the views over it read only
  those through the change feed. A first start writes every row, since the copy is empty.
- **Reset** (to epoch 0, the only reset a copy has) empties the copy, and the next start copies the table again.
  Most of the time a stop and a start is the better tool: the views over the copy see only what changed.
- **Drop** is refused while views read the copy, as for any view. It drops the copy's table, and when no copy
  view selects that table any more, removes it from a publication DuckWake owns. `duckwake_drop_source` is
  refused while the source has copy views.

The join keeps the guarantee that copies of one source agree. The table's snapshot is taken at a point ahead of
the job's position; its rows are staged and committed in the epoch whose position passes the snapshot's, together
with that table's changes from the stream after the snapshot. Until then the copy view is `joining`. A job far
behind its database (catching up after an outage) holds the staged rows that long, in the worker's staging
catalog, which spills to disk; a crash before the commit loses only the staging, and the join starts again. The
mechanisms are the ones D1 and D3 use for the whole source, applied to one table:

- **PostgreSQL.** Add the table to the publication if DuckWake owns it (`ALTER PUBLICATION ... ADD TABLE`); with
  a publication the user made, check the table is in it and refuse otherwise, naming the `ALTER`. The decoder
  sends a table's changes only for transactions after the `ALTER` committed, so the snapshot is taken after that
  commit: a REPEATABLE READ transaction records `pg_current_snapshot()` and copies the table. Transactions on
  that table the snapshot already saw are skipped by transaction id, as D1 skips them for the first snapshot;
  those it did not see (still running when it was taken) come from the stream.
- **MySQL.** The G1/G2 snapshot of D3 for one table: read `gtid_executed`, start a consistent snapshot, read it
  again, copy; transactions in G1 are skipped for that table, those between G1 and G2 apply over the copy (full
  row images make that safe).

A join reads the whole table on the database, so it costs what a first copy costs. For large tables the
chunked backfill of D4 applies to joins as it does to first copies.

### Stopping the whole source

When no copy view of a source runs, nothing needs its changes, but the slot still exists: if nothing advances
it, PostgreSQL keeps WAL from that point, as for a stopped source today. Two options:

1. Keep the job running, reading and dropping changes, so the slot advances. The next start of any copy is a
   join (a snapshot and a difference), so nothing is lost.
2. Stop the job with the last copy, and keep today's advice to set `max_slot_wal_keep_size`.

Proposal: option 1 by default, since a join makes it safe, with the job's status `idle` in `duckwake_jobs()`.
For MySQL, which keeps nothing for a reader, the choice changes only whether the binlog is read.
`duckwake_stop('shop')` on the source itself stops the job and every copy view, for maintenance of the
database; from then on PostgreSQL keeps WAL for the slot, as for a stopped source today. A worker that is down also pins WAL; that is
the existing risk, and the lag alert and `max_slot_wal_keep_size` cover it.

**Losing the stream is no longer a full restart.** Today a lost slot or purged binlog fails the job, and
`duckwake_reset('shop', 0)` empties every mirror, so the views over them take every row deleted and inserted
again. With joins, a reset of the source creates a new slot (PostgreSQL) or starts from the current GTID set
(MySQL), and every running copy view joins by difference: the views over the copies see only the rows that really
changed while the stream was lost.

### Schema changes

D4 applies schema changes to the copies. Copy views make most of them simpler:

- A copy that names its columns ignores a column added to its table, and a column dropped that it does not
  select. Only a change to a selected column (dropped, renamed, retyped) stops it.
- A copy with `*` takes an added nullable column (DuckLake can add a column), as D4 plans for mirrors.
- A dropped or renamed table stops its copy views only; the source's other copies keep going.

### Verify

`duckwake_verify('raw.orders')` compares the copy with its table's selected columns, read now under one snapshot,
through the same casts. `duckwake_verify('shop')` does it for every copy view of the source. As today, the result
is exact only once writes to the database stop.

## Compatibility and migration

- **No mirror tables any more, and no options for them.** A database source's options are only how to reach
  the database and how to read it: for PostgreSQL `dsn`, `password`, `slot`, `publication`, `epoch_interval`,
  `max_changes`; for MySQL the same with `url` and the replica's `server_id`. `tables` (which tables to mirror)
  and `schema` (the lake schema of the mirrors) go: copy views say which tables are read, and each copy view's
  own name says where its table is. `create_source` refuses both with the replacement in the message: a
  `create_view` per table.
- **0.3 lakes are migrated in place**, not refused: each mirror becomes a copy view of the same name, with `*`,
  following the source's stored position, and the source loses its `tables` and `schema`. No data moves, and
  views over mirrors keep reading the same tables, now copy views like any other.
  This is the first upgrade that resumes a lake; the gate runs the 0.3.0 binary (built from its tag) to make a
  lake, then this one on it.
- `duckwake_start('shop')`, `stop`, `reset` and `verify` on the source keep working and act on all its copy
  views; `reset` now rejoins them by difference instead of emptying them (above).

## What other engines do

Both engines with native database CDC have the same shape, as read from their docs on Nov 23, 2026 (check the
originals before relying on a detail):

- **RisingWave**: a shared CDC source per database (`CREATE SOURCE ... WITH (connector = 'postgres-cdc', ...)`),
  then a table per upstream table created from it (`CREATE TABLE orders (*) FROM pg_source TABLE
  'public.orders'`), with a primary key that matches the upstream table's. Its docs give the reason for sharing
  the source: transactions across tables stay atomic. A table's `snapshot` option can skip the initial copy.
- **Materialize** (the new syntax, v26 and later): `CREATE SOURCE` for the connection, then
  `CREATE TABLE t FROM SOURCE src (REFERENCE public.orders)`, read-only, with `EXCLUDE COLUMNS` to leave columns
  out; several tables may read one upstream table. A column added upstream is not picked up: a new table is
  created to get it, and dropping an ingested column puts the table in an error state.

Neither lets views read the database directly; both keep one reader per database and one stored copy per table,
which is what this proposal does.

## Costs and risks

- **More objects to create.** A copy view per table instead of one list in the source. dbt makes this a model
  per table, which is how dbt projects declare staging tables anyway.
- **A join reads the whole table** on the database: a stop and a start is not free. The docs say so, and the
  metrics show the join's rows and time.
- **The publication changes at run time** (PostgreSQL), which needs the role to own the table, as creating the
  publication does today. A user-made publication is never altered.
- **Copies of one source still share one job.** A table whose rows the cast refuses stops the job, so all its
  copies, as today. Failing only the copy view whose table broke is possible later, by dropping that table's
  changes, but it would let copies of one source disagree, so it is left out.
- **Latency is unchanged**: two hops, the ingest epoch and the downstream view's epoch.

## Open questions

- **Row filters.** `WHERE` on a copy's columns would keep it a function of one row; PostgreSQL 15 publications can
  filter rows on the server. It also turns an update that leaves the filter into a delete. Worth a second step
  once copy views exist.
- **Casts.** Changing a column's DuckDB type in the copy (a `numeric` to `DOUBLE`) is a projection too. Left out of
  the first step; the views over the copy can cast.
- **Columns that never leave the database.** A column a copy view leaves out still travels to the worker and is
  dropped there. PostgreSQL 15 publications take column lists, so the database would not send it at all; that
  needs every copy view of a table to agree, and PostgreSQL 15 where D1 needs 13.
- **Name.** "Copy view" here; the docs call the feature "copy database tables". Settle before building.

## Plan

Part of D4 ([#46](https://github.com/duckwake/duckwake/issues/46)), before chunked backfill, since a join is the
"add a table to a running source" item of D4 and the backfill then serves joins and first copies alike.

1. **Catalog and compiler.** A database source as a definition only (`tables` and `schema` refused); copy views
   recognised from `FROM` and checked (one table, columns only, the key selected); any other view naming a
   database table refused. Unit tests for the rules and the messages.
2. **The job over copy views.** Routing and projection per copy view, one commit, `idle` with no copy running.
   Joins with a difference, for PostgreSQL and MySQL. Gate: `pg_ingest_test.sh` and `mysql_ingest_test.sh` gain a
   step that creates a copy view and stops and starts another during the workload while the worker is killed;
   the copies equal their tables' selected columns once writes stop, the slot is never ahead of the lake, and a
   view over a restarted copy verifies having seen only the rows that changed. A second step drops the slot
   during the workload and resets the source: the copies rejoin by difference and equal their tables.
3. **Migration of 0.3 lakes**, with its gate.
4. **Docs**: [Copy database tables](../guides/databases.md), the PostgreSQL recipe, the SQL reference, the
   changelog, and the D4 status in [database CDC sources](database-cdc-sources.md).
