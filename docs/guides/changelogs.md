# Changelogs and the latest row

Some topics carry changes to records rather than events: each message is the latest version of an order, an
account or a device, keyed by its id. An upsert view reads such a topic as a table with one row per key. Topics a
Debezium connector writes are read the same way.

## Upsert views

```sql
CALL duckwake_create_view('analytics.orders',
  'SELECT order_id, status, amount, ts FROM order_updates',
  upsert_key := 'order_id', upsert_version := 'ver');
```

- **A message replaces its key's row.** A tombstone, a message with a key and no value, deletes it.
- **`upsert_version` decides which message wins.** A message replaces the row only when its version is at least
  the row's, so a redelivered older message changes nothing. Of equal versions the later message wins, and NULL
  counts as the lowest. Without `upsert_version`, the message read last wins.
- **Keys stay in one partition.** Messages apply in their order in the partition. Kafka's default partitioner
  keeps a key's messages together.

The view's table is a plain DuckLake table. Its state holds each key's current row and nothing for deleted
keys, so it grows with the number of live keys, not with the topic.

## Where the key comes from

From the message key:

- **A JSON object** with a field of the key column's name: `{"order_id": 42}`.
- **For a single key column**, the whole key, as JSON or plain text: `42`.
- **A message without a key** takes the key from its value.

Several key columns are comma-separated: `upsert_key := 'tenant, order_id'`.

## What an upsert view can do

An upsert view filters, selects and computes columns, and can look up lake tables. The key columns must be
selected under their names. A filter applies to each key's current row: when an order's latest version no
longer matches, its row leaves the table.

Aggregates, `DISTINCT`, windows and `UNION ALL` are refused. Put them in a second view that reads the upsert
view's table. It reads the table's change feed, which carries every update and delete:

```sql
CALL duckwake_create_view('analytics.orders_by_status',
  'SELECT status, count(*) AS n, sum(amount) AS total FROM analytics.orders GROUP BY status');
```

## Debezium topics

A topic a Debezium connector writes, one table's changes, is a source with `"format": "debezium"`. Its views are
upsert views keyed by the table's primary key:

```sql
CALL duckwake_create_source('orders', 'kafka', '{
  "brokers": "localhost:19092", "topic": "pg.public.orders", "format": "debezium",
  "columns": {"order_id": "BIGINT", "status": "VARCHAR", "amount": "DECIMAL(18,2)", "updated_at": "TIMESTAMP"}}');
CALL duckwake_create_view('analytics.orders', 'SELECT * FROM orders',
  upsert_key := 'order_id', upsert_version := 'updated_at');
```

- **Events.** Inserts, snapshot reads and updates replace their key's row; deletes and tombstones remove it. Other
  events, such as truncates, are dead letters whose error names them.
- **No event time needed.** A Debezium source may leave out `event_time`.
- **Connector settings.** Set `decimal.handling.mode` to `string` or `double`: the default sends decimals as
  bytes, which DuckWake cannot read. Both JSON forms, with and without `schemas.enable`, work.
- **Large PostgreSQL values.** For a table without `REPLICA IDENTITY FULL`, Debezium leaves out a large (TOASTed)
  column that an update did not change. The view cannot rebuild that row and stops with an error naming the
  event. Set `REPLICA IDENTITY FULL` on the table, take a new Debezium snapshot, and replace the view.

To read a PostgreSQL or MySQL database without running Debezium, use a
[database source](databases.md) instead.

## Check it

`FROM duckwake_verify('analytics.orders')` recomputes the table from the topic with the same rules. In
`duckwake sql`, a query on the source shows each message's `_key` next to `_partition` and `_offset`.

## Related

- [Recipe: the current state of each order](../recipes/changelog-state.md)
- [Upsert views](../sql.md#upsert-views) and [Debezium sources](../sql.md#debezium-sources) in the SQL reference
