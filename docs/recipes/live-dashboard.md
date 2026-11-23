# Recipe: a live per-minute dashboard from Kafka

**What this builds:** purchase events arrive on a Kafka topic. A view counts purchases and sums revenue per game,
per minute, in a DuckLake table that a dashboard can query. Events that arrive after their minute has closed go
to a table of their own instead of being lost.

**Use it for:** live dashboards, per-minute metrics, anything that buckets events by their own time.

You'll create a Kafka source, a windowed view and a late-data table. You'll send eight events with known values
and read the exact result, then send a late event, and finally send 15,000 events and check the view with
`duckwake_verify`.

## Before you start

Start the stack and the server, as in the quick start: PostgreSQL holds the lake's catalog, Redpanda is Kafka,
and `duckwaked` runs a worker and serves SQL on port 9494.

<!-- test: skip -->
```sh
git clone https://github.com/duckwake/duckwake.git && cd duckwake
docker compose up -d --wait          # PostgreSQL on :55432, Redpanda on :19092
cargo build --release
target/release/duckwaked --worker w1 --quack-token secret
```

In another terminal, open the [DuckDB 2.0 CLI](https://duckdb.org/docs/installation/) and send everything to
the server. Every SQL block below runs in this session.

<!-- test: skip -->
```sql
ATTACH 'quack:localhost:9494' AS dw (TOKEN 'secret');
CONNECT dw;
```

## 1. Create the topic and the source

Create the `purchases` topic with one partition, so that the events below are read in the order they were
sent:

```sh
docker compose exec -T redpanda rpk topic create purchases -p 1
```

Then register the topic as a source. `event_time` names the column that holds each event's time; windows
close by that time, not by when the events arrive. `watermark_delay` is how long the view waits for
stragglers: a minute closes once an event 5 seconds past its end has been read.

```sql
CALL duckwake_create_source('purchases', 'kafka', '{
  "brokers": "localhost:19092", "topic": "purchases",
  "event_time": "ts", "watermark_delay": "5 seconds",
  "columns": {"ts": "TIMESTAMP", "game_id": "VARCHAR", "user_id": "VARCHAR", "amount": "DECIMAL(18,2)"}}');
```

## 2. Create the view and start it

`TUMBLE` splits time into one-minute windows and adds `window_start` and `window_end` to each row. The view's
table is `lake.dashboard.revenue_per_min`. Purchases that arrive after their minute has closed go to
`lake.dashboard.revenue_late`.

```sql
CALL duckwake_create_view('dashboard.revenue_per_min',
  'SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
   FROM TUMBLE(purchases, ts, INTERVAL 1 MINUTE) GROUP BY ALL',
  late_data := 'dashboard.revenue_late', epoch_interval := '2s');
CALL duckwake_start('dashboard.revenue_per_min');
```

`epoch_interval := '2s'` makes the view read and commit every 2 seconds instead of the default 10, so the
results below show up sooner.

## 3. Send eight purchases

```sh
docker compose exec -T redpanda rpk topic produce purchases <<'EOF'
{"ts": "2026-01-15 09:00:05", "game_id": "g1", "user_id": "u1", "amount": 4.99}
{"ts": "2026-01-15 09:00:20", "game_id": "g2", "user_id": "u2", "amount": 9.99}
{"ts": "2026-01-15 09:00:41", "game_id": "g1", "user_id": "u3", "amount": 1.99}
{"ts": "2026-01-15 09:00:58", "game_id": "g1", "user_id": "u1", "amount": 4.99}
{"ts": "2026-01-15 09:01:10", "game_id": "g2", "user_id": "u4", "amount": 19.99}
{"ts": "2026-01-15 09:01:15", "game_id": "g1", "user_id": "u2", "amount": 0.99}
{"ts": "2026-01-15 09:01:44", "game_id": "g2", "user_id": "u2", "amount": 9.99}
{"ts": "2026-01-15 09:02:07", "game_id": "g1", "user_id": "u5", "amount": 4.99}
EOF
```

## 4. Read the table

```sql
SELECT window_start, game_id, n, revenue
FROM lake.dashboard.revenue_per_min
ORDER BY window_start, game_id;
```

Within a few seconds:

<!-- test: expect -->
```text
┌─────────────────────┬─────────┬───────┬───────────────┐
│    window_start     │ game_id │   n   │    revenue    │
│      timestamp      │ varchar │ int64 │ decimal(38,2) │
├─────────────────────┼─────────┼───────┼───────────────┤
│ 2026-01-15 09:00:00 │ g1      │     3 │         11.97 │
│ 2026-01-15 09:00:00 │ g2      │     1 │          9.99 │
│ 2026-01-15 09:01:00 │ g1      │     1 │          0.99 │
│ 2026-01-15 09:01:00 │ g2      │     2 │         29.98 │
└─────────────────────┴─────────┴───────┴───────────────┘
```

The 09:02 minute is missing on purpose. Its one purchase, at 09:02:07, moved the watermark to 09:02:02: the
latest event time read, minus the 5-second delay. That closed 09:00 and 09:01, but 09:02 stays open until an
event at 09:03:05 or later arrives. A minute's row is written once, when it closes, and never changes after
that.

## 5. Send a late purchase

This purchase belongs to 09:00, which has already closed:

```sh
echo '{"ts": "2026-01-15 09:00:30", "game_id": "g2", "user_id": "u6", "amount": 2.99}' |
  docker compose exec -T redpanda rpk topic produce purchases
```

The 09:00 row keeps its count. The purchase goes to the late-data table, with the window it missed:

```sql
SELECT ts, game_id, amount, window_start FROM lake.dashboard.revenue_late;
```

<!-- test: expect -->
```text
┌─────────────────────┬─────────┬───────────────┬─────────────────────┐
│         ts          │ game_id │    amount     │    window_start     │
│      timestamp      │ varchar │ decimal(18,2) │      timestamp      │
├─────────────────────┼─────────┼───────────────┼─────────────────────┤
│ 2026-01-15 09:00:30 │ g2      │          2.99 │ 2026-01-15 09:00:00 │
└─────────────────────┴─────────┴───────────────┴─────────────────────┘
```

A late event is never added to a closed window: the window's row would change after a dashboard had read it.
To wait longer for stragglers, raise `watermark_delay`. Each minute is then written that much later.

## 6. Turn up the volume and check the result

DuckWake comes with a test producer. Send 15,000 purchases over 30 seconds, all stamped with the current time:

```sh
target/release/duckwake produce --topic purchases --rate 500 --count 15000
```

Wait for the view to read everything the topic holds now, then check it. `duckwake_verify` runs the view's SQL as
one batch query over every event the view has read, and compares the result with the view's table:

```sql
CALL duckwake_wait('dashboard.revenue_per_min', timeout_s := 120);
FROM duckwake_verify('dashboard.revenue_per_min');
```

The result starts with `VERIFY OK`. While the producer runs, `FROM duckwake_jobs()` shows the view's epoch,
watermark and lag.

## Key points

- **Event time.** Windows close by `ts`, the time in the event, so replaying a topic gives the same rows.
- **Final rows.** A window's row is written once, when the watermark passes its end. A dashboard never sees a
  minute change after the fact.
- **Late events are kept.** Events for a closed window go to the `late_data` table. Without that table they are
  counted (`rows_late` in `duckwake_checkpoints`) and dropped.
- **Plain tables.** `lake.dashboard.revenue_per_min` is an ordinary DuckLake table. Any DuckDB that attaches the
  lake can read it, with nothing from DuckWake installed.

## Clean up

```sql
CALL duckwake_stop('dashboard.revenue_per_min');
CALL duckwake_drop('dashboard.revenue_per_min');
DROP TABLE lake.dashboard.revenue_late;
CALL duckwake_drop_source('purchases');
```

```sh
docker compose exec -T redpanda rpk topic delete purchases
```

## Next

- [Windows and aggregates](../guides/windows.md): hopping windows, `HAVING`, and every aggregate a view can use.
- [Read a Kafka topic](../guides/kafka.md): formats, partitions, malformed messages and authentication.
- [A pipeline of views](layered-pipeline.md): views that read other views.
