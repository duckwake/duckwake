# Copy database tables

A PostgreSQL or MySQL source keeps a copy of tables of a database in the lake, read from the database's own
change stream: a replication slot for PostgreSQL, the binlog for MySQL. No Kafka or Debezium is involved. Views
then read the copies like any lake table.

## How it works

1. `duckwake_create_source` checks the database's settings and, for PostgreSQL, creates a publication and a
   replication slot.
2. `duckwake_start` starts the source's ingest job, named like the source. Its first epoch copies every table
   under one consistent snapshot.
3. Then each epoch applies the changes after that snapshot: inserts, updates, deletes and, for PostgreSQL,
   primary key changes and `TRUNCATE`. It writes them and its position in one DuckLake transaction, so a crash
   never loses or repeats a change.

Each table gets a mirror table in the lake schema named like the source, `lake.shop.orders` for a source `shop`,
with the same columns and one row per primary key.

## PostgreSQL

```sql
CALL duckwake_create_source('shop', 'postgres', '{
  "dsn": "host=db.internal dbname=shop user=duckwake", "password": "env:SHOP_PG_PASSWORD",
  "tables": ["public.orders", "public.customers"]}');
CALL duckwake_start('shop');
```

The database needs:

- `wal_level = logical`, PostgreSQL 13 or later, and UTF8.
- A role with the `REPLICATION` attribute that can read the tables. It must own them when DuckWake creates the
  publication.
- A primary key on each table, and `REPLICA IDENTITY` `DEFAULT` or `FULL`. Partitioned tables are refused:
  mirror their partitions.

The password is a reference, `env:NAME` or `file:/path`, resolved by each process that connects. Without one,
libpq's `PGPASSWORD` or `.pgpass` apply.

## MySQL

```sql
CALL duckwake_create_source('shop', 'mysql', '{
  "url": "mysql://duckwake@db.internal:3306", "password": "env:SHOP_MYSQL_PASSWORD",
  "tables": ["shop.orders", "shop.customers"]}');
CALL duckwake_start('shop');
```

The server needs MySQL 8.0 or later, with `log_bin` on, `binlog_format = ROW`, `binlog_row_image = FULL` and
`gtid_mode = ON`. The user needs `SELECT`, `REPLICATION SLAVE` and `REPLICATION CLIENT`. Each table needs a
primary key and InnoDB. The full list, including the settings `create_source` checks, is in the
[SQL reference](../sql.md#mysql-sources).

## Read the copies

A view reads a mirror table through its change feed, so it takes each insert, update and delete without
rescanning the table:

```sql
CALL duckwake_create_view('analytics.orders_by_status',
  'SELECT status, count(*) AS n, sum(amount) AS amount FROM shop.orders GROUP BY ALL');
CALL duckwake_start('analytics.orders_by_status');
```

A new view starts from the rows the mirror already holds. The mirrors are also ordinary tables for ad hoc
queries: `FROM lake.shop.orders`.

## Watch it

| What | How |
| --- | --- |
| Is it running, how far behind | `FROM duckwake_jobs() WHERE job_id = 'shop'`: `lag` is bytes of WAL (PostgreSQL) or transactions (MySQL) the database has past the job. |
| Each epoch's position | `FROM duckwake_checkpoints('shop')` |
| Do the copies match | `FROM duckwake_verify('shop')` compares every mirror with its table as it is now. Run it when writes have stopped: changes in flight show up as differences. |
| Alerts | `duckwake_lag_events` is the same lag, in `/metrics`. |

For PostgreSQL, the database keeps every change after the slot's position. A source that is stopped or failing
makes it keep WAL until its disk fills: set `max_slot_wal_keep_size` on the server, and alert on the lag.

## When the job stops

The job's status becomes `failed`, with the reason in the worker's log:

- **The database lost the changes the lake needs.** A PostgreSQL slot was dropped, invalidated, or advanced by
  something else; a MySQL server purged binlog the lake had not read (`binlog_expire_logs_seconds`, 30 days by
  default). `duckwake_reset('shop', 0)` empties the mirrors, and the next start copies the tables again. The views
  that read them take the deletes and the new rows through their change feeds.
- **A table's columns changed.** Schema changes are not applied yet. Drop the source, and the views that read it,
  and create them again.

Errors that pass, such as the database restarting, are retried with backoff.

## Stop and drop

`duckwake_drop_source('shop')` is refused while views read a mirror table. Stop the source first:

```sql
CALL duckwake_stop('shop');
CALL duckwake_drop_source('shop');
```

It drops the mirror tables, and for PostgreSQL the slot and the publication if DuckWake created it.

## Limits

The first copy goes through memory and commits in one DuckLake transaction. DuckDB spills past the worker's
`--memory-limit`, but a table too large for that waits for chunked copies, planned in
[D4](../roadmap.md). Every type each database sends, and the type its mirror column gets, is in the SQL
reference: [PostgreSQL](../sql.md#postgresql-sources), [MySQL](../sql.md#mysql-sources).

## Related

- [Recipe: daily revenue from a PostgreSQL table](../recipes/postgres-revenue.md)
- [Views over lake tables and other views](views-over-views.md)
