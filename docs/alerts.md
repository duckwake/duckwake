# Monitoring and alerts

Every worker can serve Prometheus metrics: `duckwake run --metrics 0.0.0.0:9464` (or `DUCKWAKE_METRICS`),
`duckwaked` on `:9464` unless `--metrics none`, `playground --metrics 127.0.0.1:9464`. `GET /healthz` answers `ok`
while the process runs.

**Run one worker per machine.** A worker uses every core it is given (`--stage-threads`, `--pipeline`), so more
workers on one machine add nothing but failover between processes; spread workers over machines instead. Then
every machine has one endpoint, on the conventional port 9464.

**Prometheus finds the workers itself.** A worker with `--metrics` records the address it is reached at in its
heartbeat row in the lake (`duckwake workers`, column `metrics`), and every worker serves the list of all live
ones at `GET /sd`, in Prometheus' HTTP service discovery format. So Prometheus needs one address, any worker's
`/sd`, and a worker that starts anywhere is scraped within 30 s, with no file to edit. A worker that stops cleanly
leaves the list (no alert); one that crashed stays for an hour, so its failed scrapes raise `DuckWakeWorkerDown`.
The address recorded is the one `--metrics` binds, with a wildcard host (`0.0.0.0`) replaced by the machine's name;
set `DUCKWAKE_METRICS_ADVERTISE=host:port` when Prometheus must use another. Workers bound to `127.0.0.1` are
listed under the host Prometheus used to reach `/sd`, so one machine needs no addresses at all. Add a second
`http_sd_configs` URL for another worker if the first may be down; Prometheus keeps the last list meanwhile.

`deploy/` holds what to run next to them:

| File | What |
| --- | --- |
| `deploy/prometheus/duckwake-alerts.yml` | the alerting rules below; scrape the workers with `job="duckwake"` |
| `deploy/prometheus/duckwake-alerts.test.yml` | their unit tests (`promtool test rules`, run in CI) |
| `deploy/prometheus/prometheus.yml` | job `duckwake` discovers the workers through `http://localhost:9464/sd`; job `node` scrapes the machines' node_exporter (`targets/node.yml`); `targets/duckwake.yml` is for workers discovery cannot reach |
| `deploy/prometheus/prometheus.mac.yml` | the same for Docker or Rancher Desktop on a Mac, through `host.docker.internal` |
| `deploy/grafana/dashboards/duckwake.json` | the dashboard: freshness, lag, throughput, late and dead rows, state, cost per view; commit and stage time, conflicts, load, failover and failures per worker; each worker process's CPU, memory against its DuckDB limit, spill, Kafka input, threads and files; each host's CPU, memory, network, disk and load |
| `deploy/grafana/provisioning/` | provisions the Prometheus data source and the dashboard |

`docker compose --profile monitoring up -d` runs Prometheus, Grafana and node_exporter with the host network
(Linux): Prometheus on `localhost:19090`, Grafana on `localhost:19300` (anonymous viewers see the DuckWake
dashboard), node_exporter on `:9100`. On a Mac, where Docker's host network is a VM's:

```sh
docker compose -f docker-compose.yml -f docker-compose.mac.yml --profile monitoring up -d prometheus grafana
target/release/duckwake playground --metrics 127.0.0.1:9464
```

The Host row stays empty there unless node_exporter runs on the Mac itself (`brew install node_exporter`) and
`prometheus.mac.yml` gets a `node` job for `host.docker.internal:9100`; the worker rows do not need it.

The series, per view (`worker` and `view` labels; `view` is the view's `schema.table`) unless marked per worker:

| Series | |
| --- | --- |
| `duckwake_freshness_seconds` | now minus the newest event time committed; grows on an idle topic too |
| `duckwake_lag_events` | events behind the end of the topic after the last commit; for a PostgreSQL source, bytes of WAL the server had flushed past its position; for a MySQL source, transactions it had executed past it |
| `duckwake_watermark_delay_seconds` | now minus the watermark |
| `duckwake_epoch`, `duckwake_stage_seconds`, `duckwake_commit_freshness_seconds` | last epoch, its staging time, event to visible |
| `duckwake_rows_in_total`, `_rows_out_total`, `_late_rows_total`, `_dead_letters_total` | counters |
| `duckwake_state_rows` | rows in the state as last committed, read every 10 s |
| `duckwake_view_cost` | share of a worker the view uses |
| `duckwake_snapshots_held`, `_snapshots_held_seconds` | views with lake inputs: snapshots after the view's position, which maintenance keeps, and how old the oldest is; a view no worker runs is reported by every worker |
| `duckwake_epoch_interval_seconds`, `duckwake_watermark_delay_setting_seconds`, `duckwake_max_state_rows` | the view's settings, for relative alerts |
| per worker: `duckwake_worker_up`, `_worker_restarts_total`, `_views`, `_worker_load` | |
| per worker: `duckwake_epochs_total`, `_commit_seconds_sum`, `_commit_seconds`, `_poll_stage_seconds`, `_commit_conflicts_total` | epochs and commits |
| per worker: `duckwake_failover_claims_total`, `_handoffs_total` | views taken over, views handed away |
| per worker: `duckwake_views_failed_total`, `_out_of_memory_total`, `_epoch_rows_cap` | failures and memory |
| per worker: `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_resident_memory_max_bytes`, `process_threads`, `process_open_fds` | the worker process (Linux and macOS) |
| per worker: `duckwake_duckdb_memory_bytes`, `_duckdb_spill_bytes`, `_duckdb_memory_limit_bytes` | DuckDB's memory, what it spilled, its limit |
| per worker: `duckwake_kafka_read_bytes_total`, `_kafka_read_messages_total` | what its readers fetched from Kafka |

Alerts marked **critical** should page; **warning** ones can wait for working hours. Each section says what the
alert means and what to look at first.

## DuckWakeWorkerDown

**critical.** Prometheus has not scraped a worker for 2 minutes. If the process is gone, its views move to the
other workers once its heartbeat has been still for `--lease-timeout` (30 s), and continue from their last
checkpoint: nothing is lost, but nothing runs them in between. With one worker, nothing runs at all.
Check the process and its log (`journalctl -u duckwake@<id>` or the container's), then the machine.

## DuckWakeNoWorkers

**critical.** No worker is up: no view is running. Same checks as above, for every worker; when they all
stopped at once, look at what they share: PostgreSQL (the catalog), Kafka, object storage.

## DuckWakeWorkerRestarting

**critical.** A supervised worker (`duckwaked`, the extension's) failed at least 3 times in 15 minutes, or is
waiting to restart. `FROM duckwake_worker_status()` shows the last error. A worker restarts from its
checkpoints, so restarts lose nothing, but a crash loop makes no progress: fix the cause (catalog or Kafka
unreachable, credentials, disk full).

## DuckWakeOutOfMemory

**warning.** An epoch did not fit in the DuckDB memory limit (`--memory-limit`). The worker started again from its
checkpoints with half as many rows per epoch (`duckwake_epoch_rows_cap`), and keeps that cap until it restarts:
output stays exact, but throughput drops. Raise `--memory-limit` if the machine has room, lower `--threads` (DuckDB
suggests 1 to 4 GB per thread), move views to other workers (`duckwake drain`, `start --worker`), or lower the
views' `max_rows`. A worker that runs out of memory
even at 1,000 rows per epoch exits.

## DuckWakeCommitSlow

**warning.** Epoch commits take more than 5 s on average. Each commit is one DuckLake transaction on the catalog:
look at PostgreSQL (CPU, I/O, locks, long transactions, autovacuum) and at the object store's latency, then at
how much one epoch writes (`duckwake checkpoints <view>`: `rows_out`).

## DuckWakeCommitConflicts

**warning.** More than 20% of epoch commits hit a DuckLake write conflict and were retried. They are retried
safely, but each costs a round trip. Usual causes: maintenance running too often (`--flush-every`, `--merge-every`),
or control commands in a loop.

## DuckWakeViewFailed

**critical.** A worker marked a view `failed`; the worker and its other views go on. `duckwake jobs` shows which,
and the worker log says why (`view stopped: marking it failed`, `cannot start job`):

- *over its max_late_rows* or *over its max_state_rows*: see [Limits](cli.md#limits). Fix the cause or raise the
  limit with `duckwake alter-view`, then `duckwake start` it: it continues from its last checkpoint.
- *an insert-only input changed*: a lake table a windowed view created before C4 (0.3) reads had rows updated or
  deleted. Replace the view (`create-view --if-exists replace`, with `--late-data`) to have it take updates and
  deletes as late corrections.
- *cannot start*: the view's definition no longer compiles against its inputs (a dropped table, a source gone).
- A PostgreSQL source's ingest job (named like the source): *replication slot ... is gone*, *was invalidated* or
  *has confirmed ..., past the lake's position*: changes were lost, so `duckwake reset <source> --to-epoch 0` and
  start it to take a new snapshot. *the columns of ... changed*: drop the source and create it again. *does not
  fit its mirror column*: a value the type mapping cannot hold ([PostgreSQL sources](sql.md#postgresql-sources)).
  While it is failed or stopped, the database keeps WAL for its slot.
- A MySQL source's ingest job: *the server purged binlog the mirrors had not read*: reset it (`--to-epoch 0`) and
  start it. *... changed in MySQL*: drop the source and create it again. *a value of ... cannot be read*: see
  [MySQL sources](sql.md#mysql-sources).

## DuckWakeViewStuck

**critical.** Events are waiting and the view has not committed an epoch for 15 minutes. Is it running
somewhere (`duckwake jobs`: worker, lease)? Is that worker up, and does its log show the view? A worker
`waiting for the topic` cannot see the topic; a view whose worker is gone moves after the lease timeout.

## DuckWakeViewFallingBehind

**warning.** The newest committed event is more than 3 epoch intervals old (and at least 30 s) while events are
waiting. The view is behind, not idle. Short: a backlog after a restart or failover, catching up. Long: the
worker is too slow for the input (see the dashboard's stage and commit time and the view's cost); move views
away, add a worker (rebalancing gives it views), or raise `max_rows` so each epoch does more.

## DuckWakeLagGrowing

**warning.** The view's lag has grown for 30 minutes without catching up once: it cannot keep up with the input
rate. Same remedies as above.

## DuckWakeWatermarkStuck

**warning.** The watermark is far behind the clock while events are waiting, so windows do not close. A
partition with old events (a producer's clock behind, a replay) or one that stopped receiving holds it back
until `idle_timeout`. Look at the view's checkpoints (`watermark`) and the topic's partitions.

## DuckWakeStateNearLimit

**warning.** The view's state is over 80% of its `max_state_rows`; at the limit the view fails. Raise it with
`duckwake alter-view <view> --max-state-rows N`, or bound the view (a window, fewer groups).

## DuckWakeDeadLetters

**warning.** More than 1% of a view's events cannot be decoded. They are in `lake.__duckwake.dead_letter` with
their payload and the error:
`SELECT error, count(*) FROM lake.__duckwake.dead_letter WHERE job_id = '<schema.table>' GROUP BY ALL`. A producer
changed its format, or a source column has the wrong type (add one with `duckwake add-source-column`).

## DuckWakeLateRows

**warning.** More than 5% of a windowed view's events arrive after their window closed and go to its late-data
table. The source's `watermark_delay` is shorter than the events' real disorder; a view with `max_late_rows` fails
first.

## DuckWakeSnapshotsHeld

**warning.** A view's position in its lake inputs is more than 6 hours old, so maintenance keeps every snapshot
since then: the catalog and the files of rows updated or deleted since grow. Find the view's status with
`duckwake list`. A paused or stopped view holds them on purpose; start it, or drop it if it is no longer needed.
A running view is falling behind its inputs: look at its epochs (`duckwake_stage_seconds`, `--max-rows`).
