# Manage views

A view's status says whether a worker runs it: `running`, `paused`, `stopped` or `failed`. These functions
change it, show how a view is doing, and check its result. Each takes the view's name, `schema.table`, or the
bare table name when only one schema has a view of that name.

| Task | SQL | CLI |
| --- | --- | --- |
| Start or resume | `CALL duckwake_start('analytics.clicks_per_min')` | `duckwake start analytics.clicks_per_min` |
| Pause | `CALL duckwake_pause(...)` | `duckwake pause ...` |
| Stop | `CALL duckwake_stop(...)` | `duckwake stop ...` |
| Rewind and replay | `CALL duckwake_reset(..., 12)` | `duckwake reset ... --to-epoch 12` |
| Remove | `CALL duckwake_drop(...)` | `duckwake drop ...` |
| See every view | `FROM duckwake_jobs()` | `duckwake jobs` |
| Check the result | `FROM duckwake_verify(...)` | `duckwake verify ...` |

## Start, pause and stop

`duckwake_start` hands the view to a worker, which reads from the view's last checkpoint. A new view starts at
the beginning of its inputs (`start := 'earliest'`, the default).

`duckwake_pause` and `duckwake_stop` both stop the view at once: its worker is fenced, so an epoch in flight
does not land. Start it again and it continues from its last checkpoint, with nothing lost or counted twice.

While a view is paused or stopped, its inputs keep what it has not read yet: a Kafka topic up to its retention,
and lake snapshots, which maintenance keeps for it. A view paused for a week holds a week of snapshots.

On a server with several workers, `worker := 'w2'` pins a view to w2. Without it, any live worker may run the
view, and the workers move views between themselves to even out their load.

## See how views are doing

```sql
SELECT job_id, status, worker, epoch, watermark, freshness_s, lag, late, dead, waiting_for
FROM duckwake_jobs();
```

| Column | Means |
| --- | --- |
| `epoch` | The last epoch the view committed. |
| `watermark` | How far event time has got: windows up to here are written. |
| `freshness_s` | Seconds between now and the newest event the view has committed. |
| `lag` | What the view has not read yet: messages for Kafka, bytes or transactions for a database source. |
| `late`, `dead` | Late events and malformed messages so far. |
| `waiting_for` | For a running view its worker cannot start yet: `the topic` or `the Kafka brokers`. |

`FROM duckwake_checkpoints('analytics.clicks_per_min')` shows the last 10 epochs, with what each read and wrote.
The [alerts](../alerts.md) turn the same numbers into Prometheus alerts.

## Check a view's result

```sql
FROM duckwake_verify('analytics.clicks_per_min');
```

`duckwake_verify` runs the view's SQL as one batch query over everything the view has read, and compares the
result with the view's table. `VERIFY OK` means the incremental result is exactly the batch result. It reads all
of the view's input, so run it when you need to be sure, not on a schedule. On a database source,
`duckwake_verify('shop')` compares the copies with the database's tables.

## Rewind and replay

A view that computed something wrong, because of bad input or a bug since fixed, can go back to an earlier epoch
and replay from there:

```sql
FROM duckwake_checkpoints('analytics.clicks_per_min');
CALL duckwake_stop('analytics.clicks_per_min');
CALL duckwake_reset('analytics.clicks_per_min', 40);
CALL duckwake_start('analytics.clicks_per_min');
```

A running view must be paused or stopped first. The view's table, state and positions go back to epoch 40, and
once started it reads its inputs again from there.

- **Readers follow.** Views that read this view's table take the reset through its change feed. Windowed readers,
  and readers that only take inserts, are emptied and backfill again.
- **Expired positions.** A reset to an epoch whose snapshot has expired is refused.
- **`--to-epoch 0`** recomputes a view over lake tables from the rows they hold, and makes a database source copy
  its tables again.

## Drop

```sql
CALL duckwake_stop('analytics.clicks_per_min');
CALL duckwake_drop('analytics.clicks_per_min');
```

A view must be stopped first, and a view that another view reads cannot be dropped: drop the readers first.
`duckwake_drop` removes the view and its table; `keep_table := true` keeps the table. A late-data table is
always kept.

## When a view fails

A failed view stops alone: its worker and the other views carry on. The reason is in the worker's log and in
`duckwake_views_failed_total`. Common causes:

- **A limit.** `max_state_rows` or `max_late_rows` was passed. Raise it with `duckwake_alter_view`, or fix the
  cause.
- **Kafka refused the connection.** A wrong password or certificate.
- **A lookup table was dropped**, or a view's input changed so that its SQL no longer compiles.

Fix the cause and `duckwake_start` the view: it continues from its last checkpoint.

## Related

- [Change a view or a source](change-views.md)
- [SQL functions](../sql.md#running-views) and the [CLI](../cli.md)
