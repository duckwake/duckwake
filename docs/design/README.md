# Design documents

The reasoning behind DuckWake: what was decided, what was measured, and what other engines do. Each document has a
date; the status lines inside record what was built since. The user-facing docs one level up describe what DuckWake
does today; when the two disagree, the code and those docs win.

| Document | Date | What it covers |
| --- | --- | --- |
| [DuckWake design](streaming-extension.md) | Oct 27, 2026 | The main design: goals, SQL surface, execution model, exactly-once, failure modes, delivery plan |
| [Arrow ingest plan](arrow-ingest.md) | Oct 29, 2026 | Decoding Kafka JSON once into Arrow, with an exact SQL fallback |
| [Stateful operators](stateful-operators.md) | Oct 29, 2026 | Why operator state lives in DuckLake, Feldera as the reference, the operator graph and six-stage path |
| [Parallel processing](parallel-processing.md) | Oct 31, 2026 | Parallel staging, pipelined commits, shared readers and a leaner commit |
| [CDC sources](cdc-sources.md) | Nov 5, 2026 | Debezium and upsert topics as keyed upserts, and weight-aware operators |
| [Change feed sources](change-feed-sources.md) | Nov 5, 2026 | Views over lake tables and other views through the DuckLake change feed |
| [Database CDC sources](database-cdc-sources.md) | Nov 5, 2026 | Native PostgreSQL and MySQL ingest into mirror tables |
| [Stream–stream joins](stream-stream-joins.md) | Nov 5, 2026 | Multi-source views and the interval join |
| [Performance backlog](performance-backlog.md) | Nov 5, 2026 | Commits, small files and DuckLake data inlining |
| [Architecture review](architecture-review.md) | Nov 11, 2026 | Where the architecture stands against other engines, its weaknesses, and proposals |
| [Worker rebalancing](worker-rebalancing.md) | Nov 12, 2026 | Sticky, incremental rebalancing of views across workers |
| [dbt-duckwake](dbt-duckwake.md) | Nov 13, 2026 | SQL authoring through a dbt package instead of new DDL |
| [View names](view-names.md) | Nov 22, 2026 | A view's id is `schema.table`, so two schemas can each have a view of one table name |
| [Database sources as views](database-sources-as-views.md) | Nov 23, 2026 | Proposed: a database source as a definition, and each table's copy a select-only view |
| [Kafka message formats](kafka-formats.md) | Nov 23, 2026 | Proposed: Avro, Protobuf, text, CSV and bytes values, a schema registry, metadata columns |

Comparisons with other engines come from their documentation and source as read on the dates given; check the
originals before relying on a detail.
