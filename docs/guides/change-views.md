# Change a view or a source

Views and sources change while workers run them. What a change costs depends on whether it changes what the
view computes:

| Change | How | The view's table |
| --- | --- | --- |
| How often it runs, rows per epoch, limits | `duckwake_alter_view` | Kept; the view continues where it was. |
| Its query, table or late-data table | `duckwake_create_view(..., replace := true)` | Recomputed from the start. |
| Run a deploy again with nothing changed | `duckwake_create_view(..., if_exists := 'update')` | Kept; nothing happens. |
| A new column in the source | `duckwake_add_source_column`, then replace the views that need it | Views that don't use it are untouched. |

## Change how a view runs

```sql
CALL duckwake_alter_view('analytics.clicks_per_min', epoch_interval := '30s', max_state_rows := 1000000);
```

`epoch_interval`, `max_rows`, `max_state_rows` and `max_late_rows` change without recomputing anything. A running
view restarts with the new settings and continues from where it was. A limit of 0 removes it.

## Change what a view computes

```sql
CALL duckwake_create_view('analytics.big_spend',
  'SELECT ts, game_id, country, amount FROM clicks WHERE amount >= 90',
  replace := true);
```

The new definition replaces the old one in one transaction, and starts over like a new view: Kafka sources from
their `start` position (`earliest` by default: everything the topic still holds). Its table, state, checkpoints
and dead letters are replaced. The view keeps its status and worker, and a worker still running the old
definition is stopped and starts the new one.

- **Readers first.** A view that other views read cannot be replaced: drop those first, and create them again
  after.
- **The table is rebuilt.** While the new definition catches up, its table holds only what it has computed so
  far. Building the new version beside the old one and swapping them is on the [roadmap](../roadmap.md).

## Deploy the same definitions again

A deploy script, or dbt, creates every view on each run. `if_exists := 'update'` makes that safe:

```sql
CALL duckwake_create_view('analytics.clicks_per_min',
  'SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
   FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL',
  epoch_interval := '30s', if_exists := 'update');
```

The result's first word says what happened:

| Result | When |
| --- | --- |
| `created` | The view did not exist. |
| `unchanged` | Nothing differs. Whitespace outside quotes and a trailing `;` do not count. |
| `altered` | Only how it runs differs, and that was changed as `duckwake_alter_view` would. |
| error | What it computes differs: the call fails and asks for `replace`. |

Options left out take their defaults, as for a new view, so a deploy must pass every option it sets.

## Add a column to a source

```sql
CALL duckwake_add_source_column('clicks', 'country', 'VARCHAR');
```

Views that already read the source keep the columns they were created with, and keep running. Replace a view to
use the new column. Events without the field read as NULL. Columns cannot be removed or retyped: create a new
source for that.

A database source does not follow schema changes yet: a table whose columns change stops its ingest job
([copy database tables](databases.md#when-the-job-stops)).

## With dbt

[dbt-duckwake](../../dbt/README.md) runs these for you: `dbt run` creates new views and applies setting changes
with `if_exists := 'update'`. A changed query fails until you run it with `--full-refresh`, selecting the view
and the views that read it (`-s my_view+`), which drops and creates them again.

## Related

- [Manage views](manage-views.md)
- [Changing views and sources](../cli.md#changing-views-and-sources) in the CLI guide
