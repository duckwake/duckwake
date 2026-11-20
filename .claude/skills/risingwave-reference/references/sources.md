# Sources read (2026-11-20)

Every claim in this skill comes from these pages, read on 2026-11-20 (RisingWave docs up to v3.0.2), unless it
is marked as unconfirmed. RisingWave's names and defaults change between versions: check the page again before
a design or PR relies on a detail.

## Developer guide (design)

- https://risingwavelabs.github.io/risingwave/design/architecture-design.html
- https://risingwavelabs.github.io/risingwave/design/streaming-overview.html
- https://risingwavelabs.github.io/risingwave/design/checkpoint.html
- https://risingwavelabs.github.io/risingwave/design/shared-buffer.html
- https://risingwavelabs.github.io/risingwave/design/state-store-overview.html
- https://risingwavelabs.github.io/risingwave/design/relational-table.html
- https://risingwavelabs.github.io/risingwave/design/keys.html
- https://risingwavelabs.github.io/risingwave/design/aggregation.html
- https://risingwavelabs.github.io/risingwave/design/consistent-hash.html
- https://risingwavelabs.github.io/risingwave/design/streaming-parallelism.html
- https://risingwavelabs.github.io/risingwave/design/backfill.html
- https://risingwavelabs.github.io/risingwave/design/mv-on-mv.html

## User docs

- Index of all pages: https://docs.risingwave.com/llms.txt
- https://docs.risingwave.com/get-started/architecture
- https://docs.risingwave.com/reference/key-concepts
- https://docs.risingwave.com/reference/data-persistence
- https://docs.risingwave.com/processing/watermarks
- https://docs.risingwave.com/processing/emit-on-window-close
- https://docs.risingwave.com/processing/backfill
- https://docs.risingwave.com/processing/deletes-and-updates
- https://docs.risingwave.com/processing/maintain-wide-table-with-table-sinks
- https://docs.risingwave.com/processing/sql/temporal-filters
- https://docs.risingwave.com/processing/sql/joins.md
- https://docs.risingwave.com/ingestion/formats-and-encoding-options
- https://docs.risingwave.com/ingestion/create-source-vs-create-table
- https://docs.risingwave.com/ingestion/extract-metadata-from-sources
- https://docs.risingwave.com/sql/commands/sql-create-source
- https://docs.risingwave.com/sql/commands/sql-create-table
- https://docs.risingwave.com/sql/commands/sql-create-mv
- https://docs.risingwave.com/sql/commands/sql-alter-materialized-view.md
- https://docs.risingwave.com/sql/commands/sql-alter-fragment.md
- https://docs.risingwave.com/sql/commands/sql-create-sink-into.md
- https://docs.risingwave.com/sql/functions/window-functions
- https://docs.risingwave.com/integrations/sources/postgresql-cdc
- https://docs.risingwave.com/delivery/overview
- https://docs.risingwave.com/operate/alter-streaming
- https://docs.risingwave.com/operate/manage-a-large-number-of-streaming-jobs.md
- https://docs.risingwave.com/integrations/other/dbt

## Blog

- https://risingwave.com/blog/architecture-distributed-streaming-database/ (2026; its "10 s" barrier conflicts with the docs' 1 s)
- https://risingwave.com/blog/deep-dive-into-the-risingwave-stream-processing-engine-part-2-computational-model/
- https://risingwave.com/blog/understanding-streaming-joins-in-risingwave/
- https://risingwave.com/blog/risingwave-process-time-temporal-joins
- https://risingwave.com/blog/risingwave-v2-8-highlights/ (snapshot backfill the default from 2.8)

## Not confirmed on 2026-11-20

- **Dynamic filters:** the 1.4 docs page is gone (404); the description comes from the joins blog.
- **Top-N:** the cache layout (low, middle, high) and the two over-window executors (emit-on-update and EOWC)
  come from RisingWave's source code as known before; not re-read. Unread pages:
  https://docs.risingwave.com/processing/sql/top-n-by-group.md and RFC 0033 (dynamic filter).
- **Aggregates:** `SimpleAgg` and the dedup tables of distinct aggregates come from the source code; not
  re-read.
- **Scaling:** how vnodes move during rescaling beyond the consistent-hash page ("reconfiguration rides on a
  barrier" is from the 2026 blog).
- **CDC connector:** whether it still runs Debezium in a Java connector node; the docs show only `debezium.*`
  options and Debezium's JSON format.
- **Formats:** Maxwell and Canal support (absent from the current formats page).
