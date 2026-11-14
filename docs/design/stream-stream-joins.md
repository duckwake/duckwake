# Stream–stream joins

Nov 5, 2026

A view will read several Kafka sources in one epoch, and a Join operator will keep both sides as DuckLake state. Each epoch still commits every source's offsets, the state and the output in one DuckLake transaction, so exactly-once holds unchanged. This is step 2 of the build order in [Stateful operators](stateful-operators.md), and spike S5.

## Scope

v1 joins two Kafka sources with an inner equi-join bounded in event time, and builds the multi-source engine behind it on a simpler view first.

|  | v1 | Later |
| --- | --- | --- |
| Join kinds | `INNER JOIN` on equality keys plus an event-time bound, e.g. `ON o.order_id = p.order_id AND p.ts BETWEEN o.ts AND o.ts + INTERVAL 1 HOUR` | `LEFT` / `RIGHT` / `FULL OUTER`, `ASOF`, joins of more than two streams |
| Unbounded joins (no time bound) | rejected: each side's state would grow forever | allowed once per-column lateness cleans state (spike S9) |
| Lookup tables in the same view | yes, as today | lookup tables that change, via `table_changes` (spike S6) |
| Downstream | `WHERE`, select list, `DISTINCT`, global aggregates | windows over the join's output |
| Sources | Kafka, append-only | CDC sources with retractions |

The first multi-source view is `UNION ALL` of two sources, not a join. `Union` is linear and needs no state, so it tests every engine change (several readers per view, one checkpoint across sources, recovery, fencing, pipelining) before the join adds its own state. Feldera's compiler has the same operator (`DBSPSumOperator`).

## The Join operator

Join is bilinear, so its incremental form is DBSP's three terms, the same expansion as Feldera's `JoinDeltaExpansion`:

```latex
\Delta(L \bowtie R) = \Delta L \bowtie z^{-1}R \;+\; z^{-1}L \bowtie \Delta R \;+\; \Delta L \bowtie \Delta R
```

z⁻¹L and z⁻¹R are each side's rows up to the previous epoch: two DuckLake state tables, `state_<view>_l` and `state_<view>_r`, holding the side's columns, the join keys and `__w`. The output weight of a matched pair is the product of the two weights.

**Stage.** Each side's change is the epoch's rows of its source after its own linear operators (`From`, `Filter`). The first term reads only the stored right rows whose keys appear in ΔL, and the second only the stored left rows whose keys appear in ΔR, so the cost follows the epoch's keys, not the state's size. With pipelined commits each side reads the previous epoch's rows through an overlay, as the aggregates do.

**Commit.** Each side's change is appended to its state table, and rows that can no longer match are deleted (below).

**Cleanup by the time bound.** A bound `r.ts BETWEEN l.ts - a AND l.ts + b` means a stored left row can match only right rows with `r.ts <= l.ts + b`. Once the right source's watermark passes `l.ts + b`, no new right row can match it, so it is deleted; the right side likewise with `a`. This is Feldera's `IntegrateTraceRetainKeys` driven by a waterline. State stays bounded by the interval times the event rate.

**SQL without a binder.** The view's `WHERE` and select list refer to both aliases (`o.amount`, `p.paid_at`). Rather than rename every column, codegen uses linearity: the linear operators after the join distribute over the three terms, L(A + B + C) = L(A) + L(B) + L(C). Each term is one `SELECT` with `FROM ΔL AS o JOIN z⁻¹R AS p ON …`, the downstream `WHERE` and select list applied inside it, and the three are combined with `UNION ALL`. The aliases stay valid in each term, and DuckDB binds them as it binds the view today.

**Class and checks.** A new `Bilinear` class: the rewrite rule fires when both inputs are integrated (Join ∘ (I, I) = I ∘ JoinΔ). The join's output is append-only when both inputs are, so an append sink can follow it.

## Engine changes

A view's runtime goes from one source to a list of them. Each source keeps its own position, partitions and watermark; everything else stays one per view, so an epoch is still one transaction.

| Area | Today | With several sources |
| --- | --- | --- |
| Plan (`plan.rs`) | `source: String`; the frontend finds exactly one stream reference | `sources: Vec<String>` (`source` kept for stored plans); one `Source` node per stream reference in Q |
| Runtime (`JobRuntime`) | one `offsets` map, one `part_max_ts`, one watermark | per source: offsets, newest event time per partition, last data per partition, watermark |
| Readers (`Group`) | a job belongs to one reader group | a job belongs to one group per source; groups stay per (source, position) |
| Staging (`stage_view`) | reads its group's `typed_g<id>` | each `Source` node reads its own group's table; the epoch context maps source to table |
| Checkpoint | `source_offsets` JSON is already keyed by source; one `watermark` column | the same JSON with every source; a new nullable `source_watermarks` JSON column, read as the old `watermark` when NULL |
| Recovery (`start_job`) | seeks one reader to `latest_checkpoint(job, source)` | seeks each source's reader to its offsets in the same checkpoint row |
| Fencing, lease, reset | per view | unchanged: per view, and reset rewinds every source to the chosen epoch |
| Metrics | lag per view | lag per view and source; freshness from the oldest source |
| Shared readers | a job joins a group at the same position | v1: a multi-source job reads with readers of its own; sharing per source comes in J3 |

The epoch boundary is cut independently in each source, which is safe: DBSP's join terms are exact whatever the split, because a row that arrives later meets the other side's stored rows in a later epoch. The `max_rows` cap applies per source, so one busy topic cannot starve the other.

## Watermarks and late rows

Each source keeps today's watermark rule: the minimum over its active partitions of their newest event time, minus its `watermark_delay`, ignoring partitions idle longer than `idle_timeout`. A view's watermark, for anything downstream that closes windows, is the minimum over its sources.

- **Join cleanup** uses the other side's watermark, not the view's: a left row goes once the right source can no longer send a match.
- **A row that arrives after the other side has cleaned up its possible matches is late.** v1 drops it from the join and routes it to the view's late-data table, the way windowed views route rows for closed windows. Without that, the join would miss its matches silently.
- **An idle source** stops holding back its own watermark after `idle_timeout`, as today, so a quiet topic does not freeze the other side's cleanup forever. Its stored rows wait until it sends again.
- **Recovery** reads each source's watermark from `source_watermarks`, so a restart does not reset cleanup to the beginning of time.

## Milestones

Each milestone ends at a gate, and the next starts only once it passes.

1. **J1. Views over several sources, with `UNION ALL`.** The engine changes in the table above, a `Union` operator, checkpoints and recovery across sources, and `verify` reading every source. Gate: a union view over two topics passes the crash, fencing, reset and decode suites in both flag modes, exactly.
2. **J2. Interval join.** The Join operator, its two state tables, cleanup by the other side's watermark, and late rows routed to the late-data table; unbounded joins rejected at create time. Gate: the epoch-by-epoch harness matches the batch query with both sides arriving out of step; a join view passes every suite; spike S5's bar, 5k events/s per side with a 10-minute interval, staging and commit inside the epoch, state flat over an hour.
3. **J3. Shared readers across views and sources.** A multi-source view joins a reader group per source when its position matches. Gate: the shared-reader suite with a join view, and no regression on the 16-view benchmark.
4. **J4. Operations.** Lag and freshness per source in `duckwake_jobs` and `/metrics`, `explain` showing each source, and the docs. Gate: a join view is as observable as a single-source one.

## Risks and open questions

- **Reading the other side's state each epoch.** The join reads stored rows for the epoch's keys from DuckLake every epoch; at high key counts this is the cost measured earlier (a 10M-row in-memory state took 8–15 ms, the DuckLake read is the expensive part). S5's benchmark decides whether J2 needs the cached state planned under "Making it fast".
- **Skewed keys.** One hot key on both sides multiplies rows in the ΔL ⋈ z⁻¹R term. The interval bound limits it; a per-view cap on output rows per epoch may still be needed.
- **Topics at very different rates.** A slow topic holds back the view's watermark and keeps the fast side's state longer. Per-source `idle_timeout` covers silence, not slowness.
- **Upgrading stored views.** `sources` replaces `source` in the stored plan, and the checkpoint gains `source_watermarks`. Views created before J1 must keep starting: the start-time recompile already checks state layout, and the new column is nullable.
- **Joins without a time bound.** Decided: v1 rejects them, with an error that shows how to add a bound. Lateness cannot clean their state; see below.

### Cleaning join state with lateness

In Feldera a column declared `LATENESS INTERVAL 10 MINUTES` promises that no later row is more than 10 minutes older than the newest one seen. That gives the column a waterline, the newest value minus the lateness, which only rises; a row below it is late and dropped. The compiler propagates waterlines through the operators (`MonotoneAnalyzer`) and deletes state that no future row can match (`IntegrateTraceRetainKeys`). DuckWake's per-source `watermark_delay` is the same promise on one column per source.

Whether it can clean a join depends on how the join condition involves time:

| Join condition | Example | Can state be cleaned? |
| --- | --- | --- |
| A key column derived from a time column with lateness | `o.order_id = p.order_id AND o.day = p.day` | Yes: once the other side's waterline passes a day, rows of earlier days can never match. Exact. |
| A time bound between the sides | `p.paid_at BETWEEN o.created_at AND o.created_at + INTERVAL 1 HOUR` | Yes: an order from 10:00 goes once the payments' watermark passes 11:00. Exact. This is v1's cleanup. |
| Keys only | `o.order_id = p.order_id` | No: lateness bounds a future payment's time, not its key, so it may still match any stored order. State grows forever. |

For the third kind there are three choices: reject the view (v1); allow it with a warning and a state-size limit, keeping exact results; or expire rows after a TTL, which bounds state but drops matches that arrive later than the TTL, so results differ from the batch query and `verify` must allow for it. A TTL changes the query's meaning, so if it is ever added it should be an option the user turns on explicitly. Per-column lateness (spike S9) widens the first kind to every column that declares one.
