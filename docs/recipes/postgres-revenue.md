# Recipe: daily revenue from a PostgreSQL table

**What this builds:** an `orders` table in PostgreSQL is copied into the lake and kept in step with it, through a
replication slot. A view over the copy keeps daily revenue of paid orders up to date as orders are inserted,
updated and deleted. No Kafka or Debezium is involved.

**Use it for:** analytics over an application's database without querying it, and aggregates that must follow
updates and deletes.

## Before you start

You need the stack and the server from the quick start, plus the compose profile `debezium`. That profile adds a
PostgreSQL with logical replication on port 15432 (user, password and database `shop`); it stands in for your
application's database. The server connects to it, so start the server with the password in its environment:

<!-- test: skip -->
```sh
git clone https://github.com/duckwake/duckwake.git && cd duckwake
docker compose up -d --wait
docker compose --profile debezium up -d --wait pg-source
cargo build --release
SHOP_PG_PASSWORD=shop target/release/duckwaked --worker w1 --quack-token secret
```

In another terminal, open the DuckDB 2.0 CLI and send everything to the server:

<!-- test: skip -->
```sql
ATTACH 'quack:localhost:9494' AS dw (TOKEN 'secret');
CONNECT dw;
```

## 1. Create the orders table in PostgreSQL

```sh
docker compose --profile debezium exec -T pg-source psql -U shop -q <<'EOF'
CREATE TABLE orders (
  order_id   bigint PRIMARY KEY,
  customer   text NOT NULL,
  status     text NOT NULL,
  amount     numeric(10,2) NOT NULL,
  ordered_at timestamp NOT NULL
);
INSERT INTO orders VALUES
  (1, 'ana',  'paid',     25.00, '2026-01-14 10:12'),
  (2, 'bao',  'paid',     40.00, '2026-01-14 18:30'),
  (3, 'chi',  'pending',  15.50, '2026-01-15 08:05'),
  (4, 'ana',  'paid',     60.00, '2026-01-15 09:40'),
  (5, 'dung', 'refunded', 30.00, '2026-01-15 11:00');
EOF
```

Every table DuckWake copies needs a primary key.

## 2. Create the source and start it

A source of kind `postgres` names the tables to copy. The password is a reference, `env:SHOP_PG_PASSWORD`, which
the server reads from its environment; it is never stored in the lake.

```sql
CALL duckwake_create_source('shop', 'postgres', '{
  "dsn": "host=localhost port=15432 dbname=shop user=shop",
  "password": "env:SHOP_PG_PASSWORD",
  "tables": ["public.orders"]}');
CALL duckwake_start('shop');
```

`create_source` checks the database's settings and creates a publication and a replication slot,
`duckwake_shop`. Starting the source starts its ingest job, which is also named `shop`. The job copies the
table into `lake.shop.orders`, then applies every change the slot sends, every 2 seconds.

```sql
SELECT order_id, customer, status, amount FROM lake.shop.orders ORDER BY order_id;
```

<!-- test: expect -->
```text
┌──────────┬──────────┬──────────┬───────────────┐
│ order_id │ customer │  status  │    amount     │
│  int64   │ varchar  │ varchar  │ decimal(10,2) │
├──────────┼──────────┼──────────┼───────────────┤
│        1 │ ana      │ paid     │         25.00 │
│        2 │ bao      │ paid     │         40.00 │
│        3 │ chi      │ pending  │         15.50 │
│        4 │ ana      │ paid     │         60.00 │
│        5 │ dung     │ refunded │         30.00 │
└──────────┴──────────┴──────────┴───────────────┘
```

## 3. Create the view and start it

The view reads the copy like any lake table. It reads the table's change feed, the rows inserted, updated and
deleted since its last epoch, so it never rescans the table.

```sql
CALL duckwake_create_view('analytics.daily_revenue',
  'SELECT CAST(ordered_at AS DATE) AS day, count(*) AS orders, sum(amount) AS revenue
   FROM shop.orders WHERE status = ''paid'' GROUP BY ALL',
  epoch_interval := '2s');
CALL duckwake_start('analytics.daily_revenue');
```

A new view starts from the rows its input already holds, so the three paid orders are counted at once:

```sql
SELECT day, orders, revenue FROM lake.analytics.daily_revenue ORDER BY day;
```

<!-- test: expect -->
```text
┌────────────┬────────┬───────────────┐
│    day     │ orders │    revenue    │
│    date    │ int64  │ decimal(38,2) │
├────────────┼────────┼───────────────┤
│ 2026-01-14 │      2 │         65.00 │
│ 2026-01-15 │      1 │         60.00 │
└────────────┴────────┴───────────────┘
```

## 4. Change the orders

Order 3 is paid, order 2 is deleted, and a new paid order arrives:

```sh
docker compose --profile debezium exec -T pg-source psql -U shop -q <<'EOF'
UPDATE orders SET status = 'paid' WHERE order_id = 3;
DELETE FROM orders WHERE order_id = 2;
INSERT INTO orders VALUES (6, 'bao', 'paid', 22.00, '2026-01-15 20:15');
EOF
```

Within a few seconds the copy has the changes, and the view has taken them in: order 2 left January 14, and
orders 3 and 6 joined January 15.

```sql
SELECT day, orders, revenue FROM lake.analytics.daily_revenue ORDER BY day;
```

<!-- test: expect -->
```text
┌────────────┬────────┬───────────────┐
│    day     │ orders │    revenue    │
│    date    │ int64  │ decimal(38,2) │
├────────────┼────────┼───────────────┤
│ 2026-01-14 │      1 │         25.00 │
│ 2026-01-15 │      3 │         97.50 │
└────────────┴────────┴───────────────┘
```

## 5. Check both

`duckwake_verify` on the source compares the copy with the PostgreSQL table as it is now. On the view, it runs
the view's SQL as one batch query and compares the result with the view's table.

```sql
FROM duckwake_verify('shop');
FROM duckwake_verify('analytics.daily_revenue');
```

Both results start with `VERIFY OK`. Run the source's check when writes have stopped: changes still on their
way show up as differences.

## Key points

- **Exactly once.** Each epoch of the ingest job writes the copied rows and the slot's position in one DuckLake
  transaction, and the slot moves on only after that commit. A crash never loses or repeats a change.
- **Updates and deletes flow through.** The view keeps each day's count and sum and takes back the rows an
  update or a delete removes, so it needs no rescans.
- **WAL is kept for the slot.** PostgreSQL keeps every change after the slot's position. A source that is
  stopped for long makes the database's disk fill: set `max_slot_wal_keep_size`, and watch `duckwake_lag_events`.
- **MySQL works the same way** with a source of kind `mysql`, read from the binlog by GTID.

## Clean up

Stop the view and the source, then drop them. Dropping the source drops its copy, the slot and the publication.

```sql
CALL duckwake_stop('analytics.daily_revenue');
CALL duckwake_drop('analytics.daily_revenue');
CALL duckwake_stop('shop');
CALL duckwake_drop_source('shop');
```

```sh
docker compose --profile debezium exec -T pg-source psql -U shop -q -c 'DROP TABLE orders'
```

## Next

- [Copy database tables](../guides/databases.md): what PostgreSQL and MySQL need, types, and what to do when the
  job stops.
- [Views over lake tables and other views](../guides/views-over-views.md): change feeds and backfill.
