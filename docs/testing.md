# Testing

## Results

Run on a MacBook (Apple Silicon, 10 cores) against DuckDB v2.0.0 and DuckLake 1.1, with a PostgreSQL 16
catalog and Redpanda, all from `docker-compose.yml`.

| Gate in the design | Test | Result |
| --- | --- | --- |
| Commit is atomic (Phase 0) | `examples/phase0_*.rs` | One transaction over sink, state, lease and checkpoint tables = one snapshot. A zombie commit is rejected. Time travel to an epoch's snapshot works. |
| `kill -9` at any point, no duplicates or gaps | `scripts/crash_test.sh` | 120k events with late and malformed rows. 12–16 crashes: external `kill -9` and aborts injected at poll, transaction begin, mid-transaction, before and after COMMIT, and maintenance. Sinks match a batch recomputation exactly. |
| Zombie worker after failover | `scripts/fencing_test.sh` | w1 commits epochs 1–3, then stalls 15 s with its epoch-4 transaction open. The views move to w2, which continues from epoch 4. w1's commit is rejected. The result verifies exactly. |
| 5k events/s, p95 latency < 30 s | `scripts/throughput_test.sh` | 600k events of ~1 KB at 5,000/s, 10 s epochs. Per epoch (~50k rows): ~0.45 s poll+stage, ~0.15 s commit. Lag stays ~0. An event is committed at most one epoch interval plus ~0.6 s after it arrives; windowed results also wait for the window to close plus the watermark delay. |
| Matches batch SQL | `verify` in every script | Multiset equality of each sink with the view run as one batch query over everything committed. Late rows and dead letters are also checked. |
| Matches batch SQL after retention | `scripts/retention_test.sh` | The first quarter of every partition is trimmed. Windowed and append views verify over the retained range. The global aggregate is refused, because it depends on deleted events. A sink row deleted inside the range is caught. |
| `duckwake_reset` | `scripts/reset_test.sh` | Rewind to epoch 6, replay, and the sinks end identical (verified). |
| Parallel processing (worker flags) | `scripts/scale_bench.sh`, all suites with `WORKER_FLAGS` | 16 views on one worker drain a backlog at 40.5k events/s with all four flags, against 21.7k without (1.87×). At a steady 20k events/s, read + stage per epoch drops from 1.55 s to 0.30 s, the commit from 0.66 s to 0.33 s, and CPU from 89% to 46%. Every suite passes with the flags off, with `--lean-commit` alone, with the other three together, and with all four. |

Not measured: the 4 vCPU / 16 GB reference box, the 24 h soak, and recovery time for 1 GB of state.

## Running the tests

The test scripts reset the lake and the topic themselves:
`scripts/crash_test.sh`, `scripts/fencing_test.sh`, `scripts/throughput_test.sh`, then `scripts/reset_test.sh`.
`scripts/retention_test.sh`, `scripts/decode_test.sh` and `scripts/catchup_test.sh` are standalone.
`DECODE=sql|arrow|auto` picks the source's decode for every script that uses the `clicks` source
(default `auto`).
`cargo run --release -p duckwake_core --example phase_a_arrow_decode` is Phase A of the Arrow ingest plan.
It decodes the same messages with today's SQL decode and with `decode.rs` (Arrow, falling back to SQL for
any form it can't match exactly), checks the outcomes are identical on a hostile corpus, and times both
on 50k soak-shaped events.
`scripts/decode_test.sh` is the same differential in the engine: one topic of hostile messages read by a
source that decodes in SQL and one that decodes in Arrow, each feeding the same view; sinks and dead letters
must be equal row for row. `scripts/catchup_test.sh` times a worker draining a backlog in each mode.
`POINTS="before_commit after_commit" scripts/crash_test.sh` limits the crash test to chosen injection points.
`WORKER_FLAGS="--stage-threads 4 --pipeline --shared-readers --lean-commit"` runs any test's workers with
the parallel-processing flags; with `--pipeline` the crash test also aborts while an epoch is staged ahead
of a commit (`staged_ahead`). `scripts/shared_reader_test.sh` creates a view late and rewinds another while
events stream in; both must catch up, join the shared reader and verify.
`scripts/composed_test.sh` uses the DuckDB 2.0 CLI on both ends: a plain DuckDB loads the extension, runs a
worker and serves itself over Quack; a second one attaches with `CONNECT`, creates four views, drops a lookup
table to make the worker fail and restart, recreates it and verifies everything.
`scripts/serve_test.sh` drives a `duckwaked` entirely over Quack: create, start, pause, reset and verify the
three test views, and check `/metrics`. `scripts/extension_test.sh` does the same in one DuckDB session with
the extension loaded and a worker inside it.
`scripts/scale_bench.sh` runs 1, 2, 4, 8 and 16 views (the playground's mix) on one worker at a fixed
rate, or with `CATCHUP=1` through a backlog, and appends epoch period, freshness, stage and commit time
(statements vs `COMMIT`), CPU and memory to `.lake/bench/*.csv`; `FLAGS` and `LABEL` name the variant.

`scripts/soak_test.sh` runs the design's soak test: 24 h at 5,000 events/s of ~1 KB with maintenance
enabled. It uses its own catalog database (`soak`), data path (`.lake/soak`) and topic, so it does not
touch the playground. Every minute it samples lag, freshness, snapshot count, catalog size, inlined
rows, Postgres dead tuples, file counts and worker memory. `scripts/soak_report.sh` summarizes a run by
hour, during or after it; `BUCKET="10 minutes"` makes the buckets finer. To stop early, run
`kill "$(cat .lake/soak/run.pid)"`. `DURATION`, `RATE`, `EPOCH` and the maintenance cadences are
environment variables. Snapshots are kept for 12 h by default, so a 24 h run reaches steady state
halfway through. The topic is capped at 1 GiB per partition, because Docker's VM disk is small.

Once retention has deleted a topic's oldest events, `verify` checks the retained range only. It starts
from the oldest checkpoint whose offsets are all still retained, and compares the output that depends only
on later events. For an append sink, that is the rows of later epochs. For a windowed sink, it is the
windows that start after every event time the job had seen by that checkpoint. The report's `from_epoch`
says which checkpoint was used. A global aggregate depends on every event, so it cannot be verified once
its topic has been trimmed.
