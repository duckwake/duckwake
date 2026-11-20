# CDC sources

Nov 5, 2026

A CDC source turns a database table's changes into weighted rows: an insert is +1, a delete −1, an update −1 for the old row and +1 for the new. The operators already carry `__w`; this plan makes every one of them correct for −1, adds a keyed upsert source that reads Debezium topics exactly once, and adds sinks that apply retractions. It is step 3 of the build order in [Stateful operators](stateful-operators.md).

## From change events to a Z-set

A CDC source is read as keyed upserts: the source keeps each key's current row as state, and every event becomes the difference between the old and the new current row. This is how Feldera reads tables with a primary key (the `dbsp` crate's `input_upsert` operator), and it is the only reading that stays exact with what Debezium actually delivers.

| Event (Debezium `op`) | New current row for the key | Rows the source emits |
| --- | --- | --- |
| `c` insert, `r` snapshot read | `after` | +1 `after`, and −1 the old row if the key had one |
| `u` update | `after` | −1 old row, +1 `after` |
| `d` delete | none | −1 old row, if the key had one |
| tombstone (null value) | unchanged | nothing |

Why not take Debezium's `before` and `after` as the −1 and +1 directly:

- **`before` is often incomplete.** With PostgreSQL's default `REPLICA IDENTITY`, an update's `before` holds only the key columns, so it is not the row to retract. The stored current row is.
- **Debezium delivers at least once.** After a connector restart it can send events again. Replaying `-before +after` twice counts a change twice; applying the same `after` to a key twice changes nothing the second time.
- **Kafka upsert topics** (key, latest value, null to delete) need the same operator, without any envelope.

The source declares its key (`"key": ["order_id"]` in the source options, or Debezium's message key). The `UpsertSource` operator is stateful: its state table `src_<source>` holds each key's current row and version, read for the epoch's keys only, like the global aggregate's. It is one table per source, shared by every view of that source (see How other engines read CDC). Its output is retracting, which the analysis on the operator graph propagates to every operator after it.

**Only CDC sources keep this state.** A source gets an `UpsertSource` only when its options say `"format": "upsert"` or `"debezium"`. An append-only Kafka source, today's `"format": "json"`, keeps the stateless `Source` (each message one row at +1): no copy of anything, and an append-only mark on every edge after it.

## Operators with retractions

Today the analysis refuses retracting input wherever an operator ignores weights. Each of these becomes weight-aware; the rewrite rules and the engine do not change.

| Operator | Today | With retractions |
| --- | --- | --- |
| `From`, `Filter`, `Map`, `Union` | linear, carry `__w` | no change; `Map` keeps `__w` in its select list where the view's input is weighted |
| Lookup join (`From` with lake tables) | a constant table, linear | no change: a row's weight passes through the join |
| Global aggregate: `count`, `sum`, `avg` | already sums `__w` | no change |
| Global aggregate: `min`, `max` | one value per group (`greatest` / `least`), append-only only | only where the input can retract: a count per (group, value) in its state, spike S4; the minimum is the smallest value whose count is positive, recomputed for the epoch's groups only. Append-only input keeps one value per group |
| Windowed aggregate | partials from `count(*)` and `sum(x)` | partials from `sum(__w)` and `sum(x * __w)`, `min` / `max` as above; a window whose total weight returns to 0 is dropped |
| `Distinct` | counts input rows | sums `__w`; a row leaves the output when its weight returns to 0, with weight −1 |
| Stream–stream join (J2) | weights multiply | no change |
| Append sink | inserts rows | refuses retracting input; see Sinks |

Two kinds of change follow, and only one of them costs anything. The weighted forms that replace `count(*)` with `CAST(sum(__w) AS BIGINT)` and `sum(x)` with `sum(x * __w)` replace the current forms in every view: with `__w = 1` they cost the same and keep the same state layout. The forms that need more state, value counts for `min` and `max` and the keyed upsert sink, are chosen only where the analysis marks an operator's input as retracting, as Feldera's `ExpandAggregates` chooses an aggregate's implementation by whether its input is append-only. A view that reads only append-only Kafka sources keeps the circuit and state it has today.

## Sinks for views that retract

A view's table must apply −1 rows, and DuckLake has no way to delete one of two identical rows. So a retracting view needs a key for its table, and the operator graph already knows where one exists.

| Sink | When | How it applies a change |
| --- | --- | --- |
| Append (today) | the view is append-only | inserts |
| Upsert by key (today for global aggregates) | the view's output has a key: a source key kept through `Filter` and `Map` (every key column selected), `GROUP BY` keys, or `DISTINCT` (the whole row) | consolidates the change, deletes the changed keys' rows, inserts their new ones |
| Counted rows | no key can be derived | one row per distinct output row with a `_duckwake_count` column, updated by summing weights; a row goes when its count reaches 0 |

v1 builds the keyed upsert for every retracting view and rejects the rest at create time with a message naming the missing key. The counted-rows sink changes what readers see (a count column instead of repeated rows), so it waits for a request. To find keys, the `key` property planned for the operator graph is filled in by the analysis: a source's declared key, kept by `Filter`, kept by `Map` when every key column is selected, replaced by the group keys at an aggregate.

## Event time, windows and late corrections

- **Event time.** A CDC row's event time is a column the source names (`"event_time": "updated_at"`), or, when the table has none, the change's commit time (Debezium's `source.ts_ms`). The watermark rule is unchanged.
- **Retractions of rows in closed windows.** An update or delete can retract a row whose window the watermark has already closed and emitted. v1 treats it like a late row: the window's emitted row stays, and the retraction goes to the late-data table, so nothing is lost silently. A later "emit corrections" mode can re-emit the window instead, through an upsert sink keyed by window and group.
- **Global aggregates, `DISTINCT` and joins** have no windows to close, so they apply every retraction exactly.
- **The initial snapshot** (Debezium `op = r`) arrives as inserts at the start of the topic; a view started from `earliest` reads it like any other event, and `verify` recomputes over it as usual.

## Milestones

The operators come first, tested with retractions the test harness makes up, so the source formats only have to produce correct weights.

1. **C1. Every operator weight-aware.** The weighted forms in the operator table, `min` / `max` with value counts (S4), the `key` property, and the keyed upsert sink for retracting views; the append-only checks stay only where a sink needs them. Gate: the epoch-by-epoch harness, fed random streams with inserts, updates and deletes, matches the batch query for every operator and view kind, in order and pipelined; every existing suite passes and the 16-view benchmark does not regress.
   - Status (2026-11-05, commit `e2d1c5b`): done except windowed `MIN` / `MAX` and late-data tables over retracting input, both still refused. Stateless views over a retracting source use a counted-rows sink (`_duckwake_count`); `DISTINCT` upserts by the whole row; global `MIN` / `MAX` keep value rows in the state table. The `key` property is not built yet: no source declares a key until lake inputs (F2).
2. **C2. Kafka upsert topics.** The `UpsertSource` operator and `"format": "upsert"` (message key, value or null), with a version per key, tombstones for deleted keys and one winner per key per epoch; one current-row table per source, shared by its views. Gate: a suite over an upsert topic with updates, deletes, messages sent twice and stale messages redelivered after newer ones passes exactly, through crashes.
   - Status (2026-11-20, branch `claude/focused-mendel-cbfmfj`, [#41](https://github.com/duckwake/duckwake/issues/41)): built, with two changes to the plan. The upsert is a setting of the view, not of the source: `duckwake_create_view(..., upsert_key := 'order_id', upsert_version := 'ver', tombstone_retention := '2 days')`, so the source stays a topic with a schema and the same topic can be read as events by one view and as upserts by another. And each upsert view keeps its own current rows (its state table) instead of one table per source: its table is that table, filtered and projected, and the views that need aggregates read it through the change feed. A shared table per source can come with shared reads (F4). The operator is `Upsert` (`crates/circuit/src/ops/upsert.rs`) after the `Source`; the batch query it must equal is `UpsertSpec::batch_sql`. The key comes from the message key (a JSON object's fields, or the whole key for one column), else the value; without a version the later message in the partition wins; with one the higher version wins, NULL lowest, the first of equals stays; a tombstone deletes the key when it follows the key's row, and with a version the deleted key's version stays for `tombstone_retention` (default 2 days), purged every 60th epoch. Tombstones are staged as typed rows with `_err` 'tombstone' and stay dead letters for views that do not read the source as upserts. Tests: `upsert_*` in `crates/circuit/src/tests/views.rs` (every epoch, in order and pipelined, against the batch query: object, text and missing keys, repeats, tombstones alone in an epoch, versions with NULLs, composite keys, filters, lookups), `decode::tests::tombstones_are_staged_with_their_keys`, and the gate `scripts/upsert_test.sh` (in order and pipelined): 40k order updates over 3,000 keys with every 13th a tombstone, every 17th sent twice and every 7th stale, 8 crashes with every view verified after each, the upsert views checked against the topic with SQL written in the script, and a reset replayed through a reader. The dbt package passes the three settings (`last_click` in the example).
3. **C3. Debezium.** `"format": "debezium"`: the JSON envelope's `op`, `after`, `source.ts_ms`, snapshot reads and tombstones, keyed by the message key. Gate: the same suite with a generator writing Debezium-shaped events; then end to end with PostgreSQL and a Debezium connector in a docker compose profile.
4. **C4. Windows over CDC.** Retractions in closed windows routed to the late-data table, and `verify` excluding them as it excludes late rows. Gate: a windowed view over a Debezium topic passes the crash suite exactly.
5. **C5. Operations.** Upsert-state size per source in `/metrics`, `explain` showing the upsert source, docs. Gate: a CDC view is as observable as a Kafka one.

## How other engines read CDC

Most engines read CDC over Kafka by keeping each key's current row as state and taking retractions from it rather than from Debezium's `before`: Kafka Streams, Materialize, RisingWave, Feldera and Databricks always, Flink once it is told duplicates can occur. Only Arroyo and Materialize's native PostgreSQL source trust the old row the database sends, and Materialize requires `REPLICA IDENTITY FULL` for it. The plan's core choice is the common one; ordering, duplicates and the cost of the state are where it needs changes (below). Researched November 5, 2026 from each project's docs and source.

| Engine | Current row per key | Debezium `before` | Duplicates and order | Key | State cleanup |
| --- | --- | --- | --- | --- | --- |
| Flink SQL | `ChangelogNormalize`, for upsert sources or with `table.exec.source.cdc-events-duplicate`; dropped when the sink needs no `-U` (FLIP-510) | plain format uses it and fails if null, asking for `REPLICA IDENTITY FULL` | identical row emits nothing; last arrival wins | required | TTL only; a delete after expiry is lost |
| Kafka Streams | KTable store, old value from the store | none (envelope flattened to `after`) | last arrival wins; versioned stores reject records older than a grace period | required | topic compaction |
| Materialize (Kafka) | upsert operator, RocksDB, rebuilt from its own output | ignored | highest offset wins within a timestamp | required | none; docs warn of memory and disk use |
| Materialize (native PostgreSQL) | none | old tuple trusted; `REPLICA IDENTITY FULL` required | snapshot changes rewound by LSN | — | — |
| RisingWave | the CDC table itself (`ON CONFLICT OVERWRITE`), LRU cache over its storage; lookups skipped when nothing reads it | ignored for updates; works with the default replica identity | optional version column | required | none |
| Feldera | `input_upsert`, an integral of the whole table | delete `before` by key, insert `after` | identical re-insert emits nothing; last arrival wins | required for upserts | none |
| Databricks `AUTO CDC` | the target table, plus tombstones | not used; `KEYS` and `SEQUENCE BY` | `SEQUENCE BY` drops stale events; tombstones resolve late events for deleted keys | required | tombstones dropped after 2 days by default |
| Arroyo | none at the source | trusted | not handled | required | `updating_ttl`, 1 day |

**What this changes in the plan.**

1. **A version per key.** Last-arrival-wins is not enough: a Debezium restart can redeliver an event older than the stored row, and Flink, Kafka Streams and Feldera would roll the key back. The state keeps each key's version (`source.lsn`, else `source.ts_ms` with the Kafka offset as tiebreak) and drops any event whose version is at or below it, as Databricks' `SEQUENCE BY` does. Redelivery becomes idempotent, not only for identical rows.
2. **Tombstones for deleted keys.** With versions, a deleted key keeps its last version as a tombstone, or a stale insert would bring it back. Tombstones are deleted after a bounded window (Databricks: 2 days), and that window bounds how late a redelivery may be.
3. **One winner per key per epoch.** An epoch holds several events per key; the source keeps only the highest version per key before comparing with state. Plain Z-set addition would hide an ordering bug here, as Feldera's Delta reader found.
4. **The CDC state is the source's table.** As in RisingWave, the current-row state is one DuckLake table per source, a mirror of the upstream table, not one copy per view: views read its changes. This removes the "state per view" risk below and gives users a queryable replica.
5. **Patch unchanged TOAST values** (Debezium's placeholder for large columns that did not change) from the stored row, as RisingWave and Materialize do.
6. **No TTL on this state.** Flink and Arroyo bound state with TTL and silently lose deletes and updates of expired keys. State here is bounded only by the table's size and the tombstone window.
7. **`trust_before` stays an opt-in**, the Materialize native PostgreSQL and Arroyo reading: no state, but `REPLICA IDENTITY FULL` and no redelivery required.

The initial snapshot (`op = r`) is read as upserts at the lowest version, so its overlap with the stream resolves itself, as in Flink CDC's and RisingWave's DBLog-style backfill; a delete of an unknown key is ignored, as everywhere above.

## Testing

- **What `verify` compares against.** For a CDC source the batch query runs over the table as of the checkpoint: for each key, its last event up to the checkpoint's offsets (keys stay in one partition, so offset order is change order), dropped if that event is a delete. The view's table must equal the query over that table.
- **The harness** gains random Z-set streams: inserts, then updates and deletes of earlier rows, some repeated, so every operator is checked against D ∘ Q ∘ I with −1 weights, the way `notes/dbsp_rules.py` checks the rules by hand.
- **The suites** gain an upsert topic (built: `produce --keys N` with `--delete-every`, `--repeat-every` and `--stale-every`) and a Debezium-shaped topic from the producer (with updates, deletes and redelivery), so crash, fencing, reset and pipelining cover CDC without a database. A docker compose profile runs PostgreSQL and Debezium for the end-to-end check.

## Risks and open questions

- **State per view.** Resolved by keeping the current rows in one table per source (change 4 above): ten views over one large table share one copy, and read its changes.
- **Large tables.** The current-row state is as large as the table. Reads are per epoch key, so the cost follows the changes, but the first snapshot writes the whole table into state in one long catch-up.
- **Keys that change.** An update that changes the primary key arrives from Debezium as a delete of the old key and an insert of the new one, possibly in different partitions and in either order. Keyed upserts stay exact, because each key's own events stay in order; for an epoch, a view may show both rows or neither.
- **Open:** should a source without a declared key be allowed, reading Debezium's `before` as the −1 row? It is exact only with `REPLICA IDENTITY FULL` and no redelivery, so v1 requires a key. The same reading could be an opt-in for a keyed source (`"trust_before": true`): it saves the copy of the table, at the price of exactness after a Debezium restart.
