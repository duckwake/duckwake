# Query a view's results

A view's table is an ordinary DuckLake table. Read it with plain SQL, from any DuckDB that attaches the lake.
Nothing from DuckWake needs to be installed to read it.

## Read it

Through the DuckWake server, as in the recipes:

```sql
SELECT window_start, game_id, n, revenue
FROM lake.analytics.clicks_per_min
WHERE window_start >= now() - INTERVAL 1 HOUR
ORDER BY window_start DESC;
```

From any DuckDB 2.0, attach the lake with its catalog and data path, the same ones the workers use:

```sql
INSTALL ducklake; INSTALL postgres;
ATTACH 'ducklake:postgres:dbname=lake host=localhost port=55432 user=lake' AS lake (DATA_PATH '.lake/data/');
FROM lake.analytics.clicks_per_min LIMIT 10;
```

That DuckDB needs the same access as a worker: the catalog's password (`PGPASSWORD` or `.pgpass`) and, for data
on S3, its own S3 secret (`CREATE SECRET (TYPE s3, ...)`).

## What a reader sees

- **Whole epochs.** Each epoch commits in one DuckLake transaction, so a query sees all of an epoch's changes or
  none of them, across every table it touches.
- **Extra columns.** Every row carries `_duckwake_epoch`, the epoch that wrote it. Select the columns you need
  rather than `*`.
- **Windowed tables are partitioned** by the year, month and day of `window_start`, so a filter on
  `window_start` reads only the files of those days.
- **How fresh.** `FROM duckwake_jobs()` shows each view's `last_commit`, and `freshness_s`: how far the newest
  event it has committed lags behind now.

## Read an earlier version

Each commit is a DuckLake snapshot, so the table can be read as it was:

```sql
FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 5;
FROM lake.analytics.clicks_per_min AT (VERSION => 1234);
FROM lake.analytics.clicks_per_min AT (TIMESTAMP => now() - INTERVAL 1 HOUR);
```

Snapshots last until maintenance expires them (`--expire-older-than` on the workers), except those a view still
needs.

## Join it with other tables

A view's table joins like any table. Ad hoc queries can combine views, database copies and your own tables:

```sql
SELECT g.studio, sum(r.revenue) AS revenue
FROM lake.analytics.clicks_per_min r JOIN lake.dim.games g USING (game_id)
WHERE r.window_start >= current_date
GROUP BY ALL ORDER BY revenue DESC;
```

To keep a join up to date as data arrives, make it a view: a Kafka source joined to a lake table is a lookup
([views over lake tables](views-over-views.md#lookups)).

## Use it from other tools

Anything that runs DuckDB with the DuckLake extension can read the tables: Python (`duckdb` package), a BI tool
with a DuckDB driver, or dbt models downstream of DuckWake's. The [dbt package](../../dbt/README.md) can wait for
views to catch up before tests run.

## Related

- [Manage views](manage-views.md): check a view's result against a batch query with `duckwake_verify`.
- [Extension](../extension.md): attach strings for the lake.
