# Recipe: a pipeline of views

**What this builds:** page views arrive on a Kafka topic. A staging view drops bot traffic and cleans the page
names. A second view reads the staging table and keeps per-page statistics, and a third reads those and keeps
the popular pages. Each layer is a DuckLake table that you can query, check and reset on its own.

**Use it for:** splitting a long query into steps that each have a table, sharing a cleaned stream between
several views, and staging and mart layers as in dbt.

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
docker compose exec -T redpanda rpk topic create page_views -p 1
```

```sql
CALL duckwake_create_source('page_views', 'kafka', '{
  "brokers": "localhost:19092", "topic": "page_views", "event_time": "ts",
  "columns": {"ts": "TIMESTAMP", "user_id": "VARCHAR", "page": "VARCHAR", "ms": "INTEGER", "is_bot": "BOOLEAN"}}');
```

## 2. Create the three views

The staging view filters and computes columns. It reads the topic:

```sql
CALL duckwake_create_view('staging.page_views',
  'SELECT ts, user_id, lower(page) AS page, ms FROM page_views WHERE NOT is_bot',
  epoch_interval := '2s');
```

The statistics view reads the staging view's table. A view in another schema is read by its full name,
`staging.page_views`; a bare name would mean the `main` schema.

```sql
CALL duckwake_create_view('analytics.page_stats',
  'SELECT page, count(*) AS views, count(DISTINCT user_id) AS users, avg(ms) AS avg_ms
   FROM staging.page_views GROUP BY page',
  epoch_interval := '2s');
```

The popular pages are those that at least two people viewed:

```sql
CALL duckwake_create_view('analytics.popular_pages',
  'SELECT page, views FROM analytics.page_stats WHERE users >= 2',
  epoch_interval := '2s');
```

Start all three. Each runs on its own, reads its input from where it last stopped, and commits its own epochs:

```sql
CALL duckwake_start('staging.page_views');
CALL duckwake_start('analytics.page_stats');
CALL duckwake_start('analytics.popular_pages');
```

## 3. Send page views

```sh
docker compose exec -T redpanda rpk topic produce page_views <<'EOF'
{"ts": "2026-01-15 12:00:01", "user_id": "u1", "page": "/Home", "ms": 120, "is_bot": false}
{"ts": "2026-01-15 12:00:02", "user_id": "bot7", "page": "/home", "ms": 5, "is_bot": true}
{"ts": "2026-01-15 12:00:04", "user_id": "u2", "page": "/home", "ms": 180, "is_bot": false}
{"ts": "2026-01-15 12:00:09", "user_id": "u1", "page": "/pricing", "ms": 300, "is_bot": false}
{"ts": "2026-01-15 12:00:15", "user_id": "u3", "page": "/Pricing", "ms": 250, "is_bot": false}
{"ts": "2026-01-15 12:00:21", "user_id": "u1", "page": "/docs", "ms": 90, "is_bot": false}
{"ts": "2026-01-15 12:00:30", "user_id": "u1", "page": "/home", "ms": 150, "is_bot": false}
EOF
```

## 4. Read each layer

```sql
SELECT page, views, users, avg_ms FROM lake.analytics.page_stats ORDER BY page;
```

<!-- test: expect -->
```text
┌──────────┬───────┬───────┬────────┐
│   page   │ views │ users │ avg_ms │
│ varchar  │ int64 │ int64 │ double │
├──────────┼───────┼───────┼────────┤
│ /docs    │     1 │     1 │   90.0 │
│ /home    │     3 │     2 │  150.0 │
│ /pricing │     2 │     2 │  275.0 │
└──────────┴───────┴───────┴────────┘
```

The bot's view is gone, and `/Home` and `/Pricing` count with their lower-case pages.

```sql
SELECT page, views FROM lake.analytics.popular_pages ORDER BY page;
```

<!-- test: expect -->
```text
┌──────────┬───────┐
│   page   │ views │
│ varchar  │ int64 │
├──────────┼───────┤
│ /home    │     3 │
│ /pricing │     2 │
└──────────┴───────┘
```

`/docs` comes in once a second person views it:

```sh
echo '{"ts": "2026-01-15 12:01:02", "user_id": "u2", "page": "/docs", "ms": 110, "is_bot": false}' |
  docker compose exec -T redpanda rpk topic produce page_views
```

```sql
SELECT page, views FROM lake.analytics.popular_pages ORDER BY page;
```

<!-- test: expect -->
```text
┌──────────┬───────┐
│   page   │ views │
│ varchar  │ int64 │
├──────────┼───────┤
│ /docs    │     2 │
│ /home    │     3 │
│ /pricing │     2 │
└──────────┴───────┘
```

The change went through all three layers: the staging view wrote the row, the statistics view updated `/docs`,
and the popular pages view saw `users` reach 2.

## 5. Check every layer

```sql
FROM duckwake_verify('staging.page_views');
FROM duckwake_verify('analytics.page_stats');
FROM duckwake_verify('analytics.popular_pages');
```

Each result starts with `VERIFY OK`. A view over another view is checked against the rows it read from that
view's table, so a check pinpoints the layer that is wrong.

## The same as dbt models

With [dbt-duckwake](../../dbt/README.md), each layer is a model, and `ref()` gives dbt the order to create them
in. The example project has the same shape: [`big_spend`](../../dbt/example/models/analytics/big_spend.sql)
filters the `clicks` source, and [`spend_by_game`](../../dbt/example/models/analytics/spend_by_game.sql) reads
it:

<!-- test: skip -->
```sql
-- models/analytics/spend_by_game.sql
SELECT game_id, count(*) AS n, sum(amount) AS total FROM {{ ref('big_spend') }} GROUP BY ALL
```

## Key points

- **Each layer is a table.** Query it, join it in ad hoc SQL, or point a dashboard at it. A view's table is an
  ordinary DuckLake table.
- **Change feeds connect the layers.** A view reads another view's table through its change feed: the rows
  inserted, updated and deleted since it last read. It never rescans the table.
- **Order of operations.** A view cannot be dropped while another view reads it, and a reset of a view is
  followed by the views that read it.
- **Windows over a view.** A windowed view over another view uses that view's `window_start` or `window_end`, or
  the commit time of each row, `_commit_ts`.

## Clean up

Drop the layers from the top down:

```sql
CALL duckwake_stop('analytics.popular_pages');
CALL duckwake_drop('analytics.popular_pages');
CALL duckwake_stop('analytics.page_stats');
CALL duckwake_drop('analytics.page_stats');
CALL duckwake_stop('staging.page_views');
CALL duckwake_drop('staging.page_views');
CALL duckwake_drop_source('page_views');
```

```sh
docker compose exec -T redpanda rpk topic delete page_views
```

## Next

- [Views over lake tables and other views](../guides/views-over-views.md): change feeds, backfill, and lookups.
- [Manage views](../guides/manage-views.md): start, pause, reset, verify and drop.
