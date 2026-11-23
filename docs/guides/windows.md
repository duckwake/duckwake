# Windows and aggregates

A view aggregates in one of two ways:

| | Windowed view | `GROUP BY` view |
| --- | --- | --- |
| Groups by | time windows of the events' own time, plus any keys | keys only |
| A group's row | written once, when its window closes; final from then on | updated in place every epoch its group changes |
| Late events | go to the `late_data` table | none: every event counts |
| State | open windows only | one row per group, for ever |
| Use it for | dashboards per minute or hour, reports that must not change | running totals, current counts, leaderboards |

## Tumbling windows

`TUMBLE(source, time_column, size)` goes in the view's `FROM`. It splits time into windows of one size, one
after another, and adds `window_start` and `window_end` to each row:

```sql
CALL duckwake_create_view('analytics.clicks_per_min',
  'SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
   FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL',
  late_data := 'analytics.clicks_late');
```

## Hopping windows

`HOP(source, time_column, size, slide)` makes windows that overlap: a new window starts every `slide`, and each
lasts `size`. An event falls in `size / slide` windows. A one-minute window every 20 seconds:

```sql
SELECT window_end, game_id, count(*) AS n
FROM HOP(clicks, ts, INTERVAL 1 MINUTE, INTERVAL 20 SECOND)
GROUP BY ALL
```

## When a window is written

A window is written once the watermark passes its end. The watermark is the newest event time read on each
partition, the lowest of those, minus the source's `watermark_delay` ([read a Kafka topic](kafka.md)). So with
a delay of 5 seconds, the minute 09:00 is written once an event at 09:01:05 or later has been read on every
partition that is not idle.

The last window stays open while no newer events arrive. That is expected: closing it would mean writing a
window that may still receive events.

## Late events

An event for a window that has already closed is late. The window's row is not changed. With `late_data`, the
event goes to that table with the window it missed (`window_start`, `window_end`) and where it came from
(`_source_partition`, `_source_offset`):

```sql
SELECT window_start, count(*) AS late FROM lake.analytics.clicks_late GROUP BY ALL ORDER BY window_start;
```

Without `late_data`, late events are counted in `rows_late` of `duckwake_checkpoints` and dropped. Many late
events mean a watermark delay that is too short, or a producer whose clock is wrong. `max_late_rows` makes the
view fail instead when one epoch writes more late rows than that.

## Running totals

A `GROUP BY` view has no window. Each epoch updates the rows of the groups that changed:

```sql
CALL duckwake_create_view('analytics.game_totals',
  'SELECT game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS avg_amount
   FROM clicks GROUP BY ALL');
```

Its state holds one row per group for as long as the view lives. A key with many values, such as a user id over
years, keeps growing: set `max_state_rows` to make the view fail at a bound you choose rather than run out of
memory.

## Aggregates

| Aggregate | Notes |
| --- | --- |
| `count(*)`, `count(x)`, `sum`, `avg` | Everywhere. |
| `min`, `max`, `bool_and`, `bool_or` | Everywhere. Over inputs that update or delete, the state keeps a count per value. |
| `var_samp`, `var_pop`, `stddev_samp`, `stddev_pop`, `covar_*`, `corr`, `regr_*` | Everywhere; kept as sums, so they follow updates and deletes. |
| `count(DISTINCT x)`, `approx_count_distinct(x)` | The state keeps the distinct values. |
| `median(x)`, `quantile_cont(x, q)`, `quantile_disc(x, q)` | Exact, as DuckDB's. The state keeps the values. |

Other aggregates are refused when the view is created, with this list. The details, including precision, are in
the [SQL reference](../sql.md#aggregates).

## Filter groups with HAVING

```sql
SELECT window_start, game_id, count(*) AS n
FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE)
GROUP BY ALL HAVING count(*) >= 100
```

A windowed view writes only the closed windows that pass `HAVING`. A `GROUP BY` view keeps a group's row while it
passes, and removes it when it stops passing.

## Windows over lake tables

A windowed view can read a lake table or another view's table. Its time column is then `_commit_ts`, the commit
time of each row's version, or a windowed view's `window_start` or `window_end`. See
[views over lake tables and other views](views-over-views.md#windows).

## Related

- [Recipe: a live per-minute dashboard](../recipes/live-dashboard.md)
- [Windows](../sql.md#windows) in the SQL reference
