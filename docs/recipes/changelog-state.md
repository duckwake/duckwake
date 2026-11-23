# Recipe: the current state of each order from a changelog topic

**What this builds:** a Kafka topic carries every change to every order, keyed by order id. A view turns the
topic into a table with one row per order: its latest version. A second view reads that table and keeps a count
of orders per status, which follows each change.

**Use it for:** a topic of updates to records (orders, accounts, devices), compacted topics, and any source where
the latest message for a key replaces the earlier ones. For topics a Debezium connector writes, see
[changelogs and the latest row](../guides/changelogs.md).

## Before you start

Start the stack and the server, as in the quick start:

<!-- test: skip -->
```sh
git clone https://github.com/duckwake/duckwake.git && cd duckwake
docker compose up -d --wait
cargo build --release
target/release/duckwaked --worker w1 --quack-token secret
```

In another terminal, open the DuckDB 2.0 CLI and send everything to the server:

<!-- test: skip -->
```sql
ATTACH 'quack:localhost:9494' AS dw (TOKEN 'secret');
CONNECT dw;
```

## 1. Create the topic and the source

```sh
docker compose exec -T redpanda rpk topic create order_updates -p 1
```

Each message is the whole order as it is after a change, with a version that grows with each change:

```sql
CALL duckwake_create_source('order_updates', 'kafka', '{
  "brokers": "localhost:19092", "topic": "order_updates", "event_time": "ts",
  "columns": {"order_id": "BIGINT", "status": "VARCHAR", "amount": "DECIMAL(18,2)",
              "ver": "BIGINT", "ts": "TIMESTAMP"}}');
```

## 2. Create the upsert view

`upsert_key` makes the view keep one row per order id: a message replaces its key's row. `upsert_version` says
which message wins when they arrive out of order: a message replaces the row only when its `ver` is at least
the row's.

```sql
CALL duckwake_create_view('analytics.orders',
  'SELECT order_id, status, amount, ver FROM order_updates',
  upsert_key := 'order_id', upsert_version := 'ver', epoch_interval := '2s');
CALL duckwake_start('analytics.orders');
```

## 3. Send changes

Each line is a message key, a space, and the message. The last message is a redelivery of order 2's second
version, which arrives after its third:

```sh
docker compose exec -T redpanda rpk topic produce order_updates -f '%k %v\n' <<'EOF'
1 {"order_id": 1, "status": "placed", "amount": 20.00, "ver": 1, "ts": "2026-01-15 10:00:00"}
2 {"order_id": 2, "status": "placed", "amount": 35.50, "ver": 1, "ts": "2026-01-15 10:01:00"}
1 {"order_id": 1, "status": "paid", "amount": 20.00, "ver": 2, "ts": "2026-01-15 10:02:00"}
3 {"order_id": 3, "status": "placed", "amount": 12.00, "ver": 1, "ts": "2026-01-15 10:03:00"}
2 {"order_id": 2, "status": "shipped", "amount": 35.50, "ver": 3, "ts": "2026-01-15 10:05:00"}
2 {"order_id": 2, "status": "paid", "amount": 35.50, "ver": 2, "ts": "2026-01-15 10:04:00"}
EOF
```

```sql
SELECT order_id, status, amount, ver FROM lake.analytics.orders ORDER BY order_id;
```

<!-- test: expect -->
```text
┌──────────┬─────────┬───────────────┬───────┐
│ order_id │ status  │    amount     │  ver  │
│  int64   │ varchar │ decimal(18,2) │ int64 │
├──────────┼─────────┼───────────────┼───────┤
│        1 │ paid    │         20.00 │     2 │
│        2 │ shipped │         35.50 │     3 │
│        3 │ placed  │         12.00 │     1 │
└──────────┴─────────┴───────────────┴───────┘
```

Order 2 is `shipped`, not `paid`: the redelivered version 2 was older than the row's version 3, so it changed
nothing. Without `upsert_version`, the message read last would win.

## 4. Count orders by status

An upsert view only filters and computes columns. To aggregate, read its table from a second view. That view
reads the table's change feed, which carries each order's updates and deletes:

```sql
CALL duckwake_create_view('analytics.orders_by_status',
  'SELECT status, count(*) AS orders, sum(amount) AS amount FROM analytics.orders GROUP BY status',
  epoch_interval := '2s');
CALL duckwake_start('analytics.orders_by_status');
```

```sql
SELECT status, orders, amount FROM lake.analytics.orders_by_status ORDER BY status;
```

<!-- test: expect -->
```text
┌─────────┬────────┬───────────────┐
│ status  │ orders │    amount     │
│ varchar │ int64  │ decimal(38,2) │
├─────────┼────────┼───────────────┤
│ paid    │      1 │         20.00 │
│ placed  │      1 │         12.00 │
│ shipped │      1 │         35.50 │
└─────────┴────────┴───────────────┘
```

## 5. Delete an order

A tombstone, a message with a key and no value, deletes its key's row. Here, order 3 is cancelled:

```sh
echo '' | docker compose exec -T redpanda rpk topic produce order_updates -k 3 -Z
```

`-Z` sends the empty line as a tombstone. Order 3 leaves `analytics.orders`, and the count follows:

```sql
SELECT status, orders, amount FROM lake.analytics.orders_by_status ORDER BY status;
```

<!-- test: expect -->
```text
┌─────────┬────────┬───────────────┐
│ status  │ orders │    amount     │
│ varchar │ int64  │ decimal(38,2) │
├─────────┼────────┼───────────────┤
│ paid    │      1 │         20.00 │
│ shipped │      1 │         35.50 │
└─────────┴────────┴───────────────┘
```

## 6. Check both

```sql
FROM duckwake_verify('analytics.orders');
FROM duckwake_verify('analytics.orders_by_status');
```

Both results start with `VERIFY OK`: the first recomputes the table from the topic with the same rules, the
second runs its query over the rows it read.

## Key points

- **One row per key.** The view's state holds each key's current row and nothing for deleted keys, so it grows
  with the number of live keys, not with the topic.
- **Keep a key in one partition.** Messages apply in their order in the partition. Kafka's default partitioner
  keeps a key's messages together; `upsert_version` also guards against redeliveries.
- **Where the key comes from.** The message key: plain text as here, or a JSON object with the key's fields. A
  message without a key takes it from its value.
- **Aggregate in a second view.** The first view keeps the latest rows, and the second aggregates them through the
  change feed. Each view has its own checkpoints, and each can be verified and reset on its own.

## Clean up

```sql
CALL duckwake_stop('analytics.orders_by_status');
CALL duckwake_drop('analytics.orders_by_status');
CALL duckwake_stop('analytics.orders');
CALL duckwake_drop('analytics.orders');
CALL duckwake_drop_source('order_updates');
```

```sh
docker compose exec -T redpanda rpk topic delete order_updates
```

## Next

- [Changelogs and the latest row](../guides/changelogs.md): Debezium topics, JSON keys, what an upsert view can do.
- [Views over lake tables and other views](../guides/views-over-views.md): change feeds, backfill and resets.
