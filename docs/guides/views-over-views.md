# Views over lake tables and other views

A view's `FROM` can name a lake table instead of a Kafka source: a table you load yourself, a database source's
copy, or another view's table. It needs no source:

```sql
CALL duckwake_create_view('analytics.spend_by_game',
  'SELECT game_id, count(*) AS n, sum(amount) AS total FROM analytics.big_spend GROUP BY ALL');
```

Names mean what they mean to DuckDB: `analytics.big_spend` is that table of the lake, and a bare name is a table
of the `main` schema.

## The change feed

A view reads a lake table through its DuckLake change feed: the rows inserted, updated and deleted since the
last snapshot the view read. It never rescans the table, however large it is.

- **Updates and deletes flow through.** A filter removes the rows its input no longer holds, and `GROUP BY` and
  `DISTINCT` views keep their rows up to date. A sum takes back the old value of an updated row and adds the new
  one.
- **`_commit_ts`.** Every lake input gets a `_commit_ts` column: the commit time of the row's version.
- **Epochs.** Each epoch reads up to `max_rows` changed rows and commits once.

## Lookups

Joined to a Kafka source, a lake table is a lookup instead: each event reads the table as it is when the event
is processed. Use it to add names or attributes to events:

```sql
SELECT c.ts, c.game_id, g.studio, c.amount
FROM clicks c JOIN dim.games g ON c.game_id = g.game_id
```

A change to `dim.games` applies to events read after it; rows already written keep the old value.
`FROM duckwake_explain('...')` shows `ChangeFeed(...)` or `Lookup(...)` for each input.

## Backfill

A new view starts from the rows its inputs already hold (`start := 'earliest'`, the default):

1. It reads each input as it was when the view was created, `max_rows` rows per epoch. Each row keeps the
   `_commit_ts` of its version.
2. Then it reads the changes since.

A backfill continues where it stopped after a crash, and a windowed view closes no window until its backfill is
done. `start := 'latest'` skips the rows the inputs hold and takes only new changes.

## Windows

A windowed view over a lake input uses `_commit_ts` as its time:

```sql
SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
FROM TUMBLE(analytics.big_spend, _commit_ts, INTERVAL 10 SECOND)
GROUP BY ALL
```

Over another windowed view, it can use that view's `window_end` instead, to roll small windows up into larger
ones. A 10-second window ending at 09:00:30 then counts in the 30-second window that starts at 09:00:30:

```sql
SELECT window_start, game_id, sum(n) AS n, sum(revenue) AS revenue
FROM TUMBLE(analytics.revenue_per_10s, window_end, INTERVAL 30 SECOND)
GROUP BY ALL
```

Its watermark is exact: commits come in time order, so a new row is never late. A change to a row whose window
is still open revises that window. A window is final once it closes, so a change to a row of a closed window,
such as a delete of an old row, is a late correction. It goes to the `late_data` table with `_duckwake_weight` -1 for a row
taken out (or +1 for one added), and the window's row keeps counting the old version.

To keep buckets current instead, group by a bucket rather than a window:
`GROUP BY time_bucket(INTERVAL 1 HOUR, _commit_ts)`.

## Chains of views

Views can read views that read views. Each has its own checkpoints, so each can be checked, paused or reset on
its own:

- **Drop from the top.** A view that another view reads cannot be dropped or replaced. Drop the readers first.
- **Resets follow.** A reset of a view is followed by the views that read it through its change feed. Windowed
  readers, and readers that only take inserts, are emptied and backfill again.
- **Retention.** Maintenance never expires a snapshot that a view still needs, paused and stopped views included.
  `duckwake_snapshots_held_seconds` says how far back that is: a view paused for a week holds a week of
  snapshots.

## Related

- [Recipe: a pipeline of views](../recipes/layered-pipeline.md)
- [Recipe: daily revenue from a PostgreSQL table](../recipes/postgres-revenue.md)
- [Views over lake tables and other views](../sql.md#views-over-lake-tables-and-other-views) in the SQL
  reference
