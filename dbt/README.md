# dbt-duckwake

DuckWake streaming views and Kafka sources as dbt models, on [dbt-duckdb](https://github.com/duckdb/dbt-duckdb).
A model's `SELECT` becomes `duckwake_create_view`; dbt brings the files, review, `ref()` order, tests and docs.
dbt only writes definitions to the lake: the views run on DuckWake's workers (`duckwaked`, `duckwake run`, or
`LOAD duckwake` with `duckwake_worker_start`), which pick them up as they do any view.

The package is Jinja macros only. It needs dbt-core 1.8 or later, dbt-duckdb with DuckDB 2.0.0 (the extension
is built for it), and the DuckWake extension in an extension repository (`scripts/package_extension.sh` lays one
out in `dist/extensions`; releases attach one).

## Install

```yaml
# packages.yml
packages:
  - git: "https://github.com/duckwake/dbt-duckwake.git"
    revision: v0.2.0          # the version of DuckWake your workers run
```

Each DuckWake release tags the package with its own version in
[duckwake/dbt-duckwake](https://github.com/duckwake/dbt-duckwake); use the tag that matches your workers, since the
package calls the functions of that version. `revision: main` follows the latest unreleased engine. 0.2.0 is the
first version tagged there. The package is
written in [`dbt/`](https://github.com/duckwake/duckwake/tree/main/dbt) of the DuckWake repository, so
`git: "https://github.com/duckwake/duckwake.git"` with `subdirectory: dbt` installs the same files.

```yaml
# profiles.yml: DuckDB in memory, with the extension and the lake attached as `lake`
analytics:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: ':memory:'
      extensions:
        - ducklake
        - postgres
        - name: duckwake
          repo: community                          # INSTALL duckwake FROM community, signed
          # From an extension repository instead (unsigned): repo: "'https://duckwake.github.io/extensions'"
          # (keep the inner quotes) and config_options: {allow_unsigned_extensions: true}
      attach:
        - path: "ducklake:{{ env_var('DUCKWAKE_CATALOG') }}"   # postgres:dbname=lake host=... user=...
          alias: lake
          options: {data_path: "{{ env_var('DUCKWAKE_DATA_PATH') }}"}
```

```yaml
# dbt_project.yml
on-run-start: "{{ duckwake.duckwake_setup() }}"           # duckwake_connect to the lake
on-run-end: "{{ duckwake.duckwake_wait_built(results) }}" # with --vars '{duckwake_wait: true}': wait for the views
models:
  my_project:
    +database: lake
    sources:
      +materialized: streaming_source
    analytics:
      +materialized: streaming_view
      +schema: analytics
```

Add a `generate_schema_name` macro that returns the schema as named (the example's
[`macros/generate_schema_name.sql`](example/macros/generate_schema_name.sql)): a view is named by its lake
table, and dbt's default would make it `main_analytics`.

`duckwake_setup()` connects to the catalog in the `duckwake_catalog` var or the `DUCKWAKE_CATALOG` environment, and
the data path in `duckwake_data_path` or `DUCKWAKE_DATA_PATH`. The catalog's password comes from `PGPASSWORD`.

## Models

A source is config only, `models/sources/clicks.sql`:

```sql
{{ config(materialized='streaming_source', brokers='kafka-1:9094', topic='clicks', event_time='ts',
          watermark_delay='5 seconds',
          columns={'ts': 'TIMESTAMP', 'game_id': 'VARCHAR', 'amount': 'DECIMAL(18,2)'}) }}
```

It takes the source options of [`duckwake_create_source`](../docs/cli.md#views-and-sources) (`security_protocol`,
`sasl_password := 'env:KAFKA_PASSWORD'` and so on; a password is never written into the model).

A view is a `SELECT`, `models/analytics/clicks_per_min.sql`:

```sql
{{ config(epoch_interval='2s', max_state_rows=1000000) }}
SELECT window_start, game_id, count(*) AS n
FROM TUMBLE({{ ref('clicks') }}, ts, INTERVAL 1 MINUTE)
GROUP BY ALL
```

`ref()` to a source model becomes the source's name; `ref()` to a view, or `source()` for a lake table DuckWake
does not maintain, reads that table through its change feed. View config: `epoch_interval`, `max_rows`,
`late_data`, `checkpoint`, `inline_row_limit`, `max_state_rows`, `max_late_rows`, `start` (`earliest`, the
default, or `latest`), `upsert_key` (a column or a list; [upsert views](../docs/sql.md#upsert-views)),
`upsert_version`, `tombstone_retention`, `worker` (where a new view starts; default any live worker) and `start_on_create`
(default true).

## What `dbt run` does

A plain `dbt run` never recomputes a view; only `--full-refresh` does.

| The model | `dbt run` | `dbt run --full-refresh` |
| --- | --- | --- |
| A new view | Created and started; over lake tables and views it first fills from their rows | Same |
| Unchanged | Nothing | Replaced: it recomputes from its inputs |
| Only settings changed | Altered in place | Replaced |
| Query changed | Fails, asking for `--full-refresh` | Replaced; the views that read it must be selected too (`-s view+`), and are dropped and created again after it |
| Source gained columns | `duckwake_add_source_column` for each | Same |
| Source changed otherwise | Fails: create it under a new name | Same |
| Model removed | Nothing | Nothing; `dbt run-operation duckwake_drop_orphans` lists such views, and `--args '{dry_run: false}'` drops them |

## Tests

`duckwake.duckwake_verified` checks that a view's table equals its query run as one batch over everything it
has read (`duckwake_verify`); run it once the view has committed (`--vars '{duckwake_wait: true}'` on the run
before). A windowed view needs `late_data` set for it: verify cannot reconstruct late events it dropped. dbt's own tests read the views' tables in the attached lake.

```yaml
models:
  - name: clicks_per_min
    data_tests: [duckwake.duckwake_verified]
```

## The example

[`example/`](example/) is the test suite's views as a project: a Kafka source, a windowed aggregate, a filter, a
grouped aggregate, a view over a view and a view over a lake table. DuckWake's `scripts/dbt_test.sh` deploys and
changes it in CI, from the package as dbt-duckwake publishes it ([testing.md](../docs/testing.md)).

## Not yet

Zero-downtime replace (build beside, catch up, swap; next on the [roadmap](../docs/roadmap.md)); changing a source other than adding columns; `store_failures`
as a view; a dbt Hub listing (the package's repository is ready for one).
