# Feature map: RisingWave → DuckWake

Status as of 2026-11-21 (main at `a3fce92`: upsert views #60, Debezium sources #62). Issues are on `duckwake/duckwake` with the
`roadmap` label. When a row's status changes, update it here and in `docs/roadmap.md`.

Legend:
- **built**: in `main`, with a CI gate.
- **PR**: open pull request.
- **planned #n**: roadmap issue.
- **differs**: DuckWake does it another way on purpose.
- **not planned**: no issue.

## Engine and operations

| RisingWave | DuckWake | Where |
| --- | --- | --- |
| Barriers and checkpoints every 1 s, global | differs: one epoch per worker for all its views, `epoch_interval` per view (default 10 s); one DuckLake transaction per worker epoch | `engine.rs`, `docs/internals.md` |
| Hummock (LSM on S3), compactor nodes | differs: DuckLake tables on Parquet plus the PostgreSQL catalog; DuckLake compaction and expiry in worker maintenance | `lake.rs`, `maintenance.rs` |
| Global rollback on failure | differs: per view; leases, fencing at commit, failover to another worker | `failover.rs`, `fencing_test.sh`, `failover_test.sh` |
| Actors, fragments, vnodes, parallelism | differs: a view runs on one worker; rebalancing by cost; staging threads | `docs/design/worker-rebalancing.md`, `parallel-processing.md` |
| One topic across many actors | planned #50 | |
| Shared source (one consumer per Kafka source) | built: `--shared-readers`; across views and sources: planned J3 #33 | `docs/performance.md` |
| Unaligned join (log store after joins) | not planned | |
| Background DDL, progress | differs: create returns at once; `duckwake jobs`, `checkpoints`, metrics | |
| Backfill rate limit, backfill parallelism | partly: `max_rows` per epoch; no rows/s limit | |
| System tables (`rw_catalog.*`) | `duckwake_jobs()`, `duckwake_checkpoints()`, `duckwake_metrics()`, `duckwake_worker_status()`, Prometheus `/metrics` | `docs/sql.md`, `docs/alerts.md` |
| Query serving (frontend) | differs: no query layer; any DuckDB reads the lake | |
| `FLUSH`, read-after-write not guaranteed | same: rows appear when the epoch commits; `duckwake_wait` waits for a position | |
| — (no equivalent) | DuckWake only: `verify`, which recomputes a view as one batch query and compares | `verify.rs` |

## SQL and operators

| RisingWave | DuckWake | Where |
| --- | --- | --- |
| Filter, project, `UNION ALL` | built | `ops/filter.rs`, `map.rs`, `union.rs` |
| `DISTINCT` | built (plain views) | `ops/distinct.rs` |
| Hash aggregate, value and materialized-input state | built: partials and value rows, over append-only and retracting input | `ops/aggregate.rs` |
| `string_agg`, `array_agg`, `mode`, grouping sets | planned #30 | |
| `TUMBLE`, `HOP` | built | `ops/window.rs` |
| `SESSION` windows | planned #37 | |
| Emit on window close | built: windows always emit on close | |
| Emit on update for windows | not planned | |
| Watermarks (`WATERMARK FOR ... AS ...`) | built as source options `event_time`, `watermark_delay`, `idle_timeout` | |
| Watermarks through MV on MV (a `Watermark` message in the stream) | differs: a view reads the upstream view's watermark from its checkpoint (`watermark_at`); a windowed view over lake inputs records it at least once a slide, even idle (#61) | `engine.rs` |
| Late data | built: `late_data` table or counted and dropped | `ops/late_split.rs` |
| Retractions into closed windows | planned C4 / #38 | `docs/design/cdc-sources.md` |
| Windowed `min`/`max` over retracting input | planned #38 | |
| Temporal (process-time) join | built as lookups of lake tables | `ops/from.rs` |
| Symmetric hash join (stream–stream) | planned #31 | `docs/design/stream-stream-joins.md` |
| Interval join | planned J2 #32 | |
| Window join | not planned separately (part of #31's design) | |
| As-of join | not planned | |
| Dynamic filter | not planned | |
| Top-N, `ROW_NUMBER`/`RANK` | planned #35 | |
| `LAG`/`LEAD`, `OVER` frames | planned #36 | |
| Temporal filter (`NOW()`), TTL | not planned (and `now()` in a view is not refused today) | |
| Stream key derivation | planned (`key` property, cdc-sources C1) | |

## Sources and sinks

| RisingWave | DuckWake | Where |
| --- | --- | --- |
| Kafka source, JSON | built (Arrow and SQL decode) | `kafka.rs`, `decode.rs` |
| Avro, Protobuf, schema registry | planned #49 | |
| `INCLUDE key/header/offset/partition/timestamp` | partly: staged `_partition`, `_offset`, `_key`; ad hoc `duckwake sql` shows them; columns in views planned #49 | |
| `FORMAT UPSERT` table, version column | built (C2, #60): `upsert_key`, `upsert_version` on the view | `ops/upsert.rs`, `scripts/upsert_test.sh` |
| `DO UPDATE IF NOT NULL`, composite versions | not planned (on request) | |
| `FORMAT DEBEZIUM` | built (C3, #62): `"format": "debezium"` sources read by upsert views; `event_time` optional; TOAST placeholder stops the view (RisingWave rebuilds it from state) | `decode::debezium_sql`, `debezium_e2e_test.sh` |
| PostgreSQL CDC (shared source, snapshot backfill) | planned D1 #44 (native, no Debezium) | `docs/design/database-cdc-sources.md` |
| MySQL CDC | planned D3 #45 | |
| CDC operations (state size, explain) | planned C5 #43 | |
| Tables, `INSERT`/`UPDATE`/`DELETE` | differs: any DuckLake table is an input; its change feed carries updates and deletes | `docs/design/change-feed-sources.md` |
| MV on MV | built (F1–F3): change feed, backfill from a held snapshot, reset cascade | `chain_test.sh`, `retract_test.sh`, `backfill_test.sh` |
| Sinks to external systems | differs: the DuckLake table is the output | |
| Iceberg exactly-once sink | differs: DuckLake output is exactly-once by construction | |
| `SINK INTO TABLE` (several streams into one table) | not planned (a continuous `INSERT INTO`, "left for later") | `docs/design/streaming-extension.md` |
| Partition overwrite / batch refresh MV (`refresh.interval.sec`) | planned #48 (partition-overwrite refresh) | |

## Lifecycle and tooling

| RisingWave | DuckWake | Where |
| --- | --- | --- |
| New MV, drop, rename, `SWAP WITH` | `create_view(if_exists := fail/update/replace)`, `drop` | `docs/cli.md` |
| No in-place query change | same; zero-downtime replace planned #51 (build beside, swap atomically) | `docs/roadmap.md` |
| `ALTER ... ADD COLUMN` on sources | built: `add_source_column` | |
| `ALTER ... SET PARALLELISM` | differs: `start --worker`, `drain`, rebalancing | |
| dbt-risingwave | built: dbt-duckwake; Hub listing #52 | `dbt/` |
| Cloud, serverless backfill | not planned | |
| — | DuckWake only: `reset --to-epoch` (time travel of state and output) | `catalog.rs::reset` |

## Next borrowings, in roadmap order

1. **#38 windows over updates and deletes.** Keep EOWC. Retractions into closed windows go to `late_data`
   (C4). Windowed `min`/`max` get materialized-input state, as in RisingWave's `HashAgg`.
2. **#31/#32 joins.** A symmetric hash join with DuckLake state per side, plus interval and window joins whose
   state the watermark cleans. Refuse an unbounded join without a limit.
3. **#35/#36 top-k and window functions.** Top-k first, with group-sorted state; then window functions in EOWC
   mode.
4. **#49 formats.** Avro and Protobuf with a registry, and opt-in `INCLUDE`-style metadata columns.
5. **#44 PostgreSQL CDC.** Managed slot and publication, chunked snapshot by primary key, TOAST from the mirror,
   ADD and DROP COLUMN.
