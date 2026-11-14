# Worker rebalancing

Nov 12, 2026

Built (P2): DuckWake rebalances the way Kafka's cooperative assignors and Akka's least-shard strategy do, sticky and incremental. The busiest worker hands views to the least loaded one when it is over (1+ε) times the average, weighted by each view's measured cost. A clean stop is a restart, `drain` takes a worker out, and `start --worker W` pins a view. Gate: `scripts/rebalance_test.sh`, in CI.

## How other engines rebalance

All of them keep placement sticky, move only what has to move, cap moves per round and wait for an imbalance threshold; the ones with local state also warm the new owner up first.

| System | Who decides | How a unit moves | What keeps it stable |
| --- | --- | --- | --- |
| Kafka consumers, cooperative (KIP-429) | A leader client computes a sticky assignment | Two rounds: the owner gives up only the partitions that move, then they are assigned; the rest keep running | Partitions go back to their previous owners where possible |
| Kafka consumers, KIP-848 (Kafka 4.0) | The broker compares a target assignment with what each member owns | Per member: the broker asks one member to revoke, then assigns elsewhere; no stop-the-world | Server-side assignors are sticky by design |
| Kafka Connect (KIP-415) | Incremental cooperative rebalancing | Revoke and reassign only what changes | `scheduled.rebalance.max.delay.ms` (default 5 min): a departed worker's tasks wait, so a returning worker gets them back and rolling restarts shuffle nothing |
| Kafka Streams (KIP-441) | The assignor, plus a probing rebalance every 10 min | A task moves once a warm-up replica on the new instance is within `acceptable.recovery.lag` | `max.warmup.replicas` (default 2) caps how fast tasks migrate |
| Akka Cluster Sharding | A coordinator, `LeastShardAllocationStrategy` | Shards move from the most loaded node to the least loaded | Absolute and relative limits on shards per round; balanced in 1–2 rounds |
| Pulsar brokers | The load manager, `ThresholdShedder` | A bundle is unloaded from a broker above the cluster average | 10 % over the average, a minimum throughput, weighted CPU, memory and network |
| Consistent hashing with bounded loads (Google, SODA 2018) | No coordinator: a deterministic function | Each item goes to its first preferred node below ⌈(1+ε)·avg⌉ | Bounded imbalance; few items move when a node joins or leaves (Vimeo uses ε = 0.25) |

## What is different in DuckWake

A move is cheap and can be atomic, so DuckWake needs no warm-up replicas and no two-step revoke.

- **Moving a view is cheap.** State and checkpoint live in the lake, so the new owner starts from the last checkpoint and restores nothing. A move costs a refresh interval, the job start (about 1–3 s), one epoch of latency, and a re-formed shared reader.
- **There is no coordinator.** Every worker reads the same leases and heartbeats and computes the same plan, as failover does. Deterministic schemes fit; a leader protocol does not.
- **The owner hands off between epochs.** It waits for the view's epoch in flight to commit, then moves the lease to the target with the same compare-and-set as a failover claim. Nothing is fenced, no staged epoch is wasted, and the new owner starts from that checkpoint. (Writing the handoff into the epoch's own commit would save one catalog write but needs the commit path to know about placement; not worth it.)
- **Placement is not neutral.** Views of one source on one worker share a reader (one Kafka read, one decode); moving one away doubles those reads. Chains (F4) will want co-location too.

## Design, as built

The busiest worker that has a view it may move hands up to two views per round to the least loaded live worker, until no worker is above (1+ε) times the average or two views ahead (`failover.rs`).

- **Load.** A view's cost is the share of a worker it uses: a moving average of its staging, its share of the source read and of the commit, per second of wall time. Each worker publishes per-view costs and which views are within their cooldown in its heartbeat row; a view not measured yet counts as the average view.
- **When.** Every `--rebalance-every` (30 s): the busiest worker holds two views more than the least loaded worker (a worker joined), or, for three rounds in a row, is above the average by ε times it (ε = 0.2) and by at least a tenth of a worker. The margin keeps lightly loaded workers from churning; the three rounds keep a restart's catch-up spike from moving views.
- **Which view.** The one that best evens out the pair, with a small penalty for a target that does not read the view's source; never one that only shifts the imbalance. Only the busiest worker with a movable view gives, so two never pile onto one target.
- **Cooldown.** A view that just moved or started stays put for `--rebalance-cooldown` (5 min), so nothing ping-pongs.
- **Orphans and new views** (`start` without a worker, or a dead owner's views) go largest first, each to the least loaded live worker; every worker computes the same plan.
- **Restarts and drain.** A clean stop counts as a restart: its views wait `--stop-grace` (60 s) instead of moving away and back, as Kafka Connect's `scheduled.rebalance.max.delay.ms` does. `duckwake drain W` (or `duckwake_drain`) hands all of W's views away and takes none until it restarts. A crash still fails over after the lease timeout.
- **Pins.** `start --worker W` stores `jobs.pinned_to`: never balanced away, failed over when W dies, handed back when W is live again.
- **Operations.** `workers` shows each worker's state, views and load; `/metrics` has `duckwake_view_cost`, `duckwake_worker_load`, `duckwake_failover_claims_total` and `duckwake_handoffs_total`; `--rebalance-every off` turns it off.

### Gate results

| Check | Result |
| --- | --- |
| Second and third worker join (15 s cooldown) | Views spread in 14 s and 5 s; the pinned view stays |
| Balanced cluster | No handoffs; no view moved twice within its cooldown |
| Rolling restart of all three workers | No view moved, no claims |
| Drain | All views away in 2 s; nothing taken back |
| Pinned view's worker killed | Failed over in 13 s; home 3 s after the worker returned |
| `kill -9` in the middle of a drain | The rest failed over |
| Output | All five views exact over 240k events |

Found while building it: in a lightly loaded cluster, a plain (1+ε) rule moved views on measurement noise, and a restarted worker's catch-up triggered moves during a rolling restart; the absolute margin and the three-round rule removed both. Views started before their topic exists now wait for it instead of failing. Not built: workers of different sizes are treated alike, and a `rebalance --dry-run` command.

## Sources

KIP-429 · KIP-848 (Confluent) · KIP-415 · KIP-441 · Akka ShardCoordinator · Pulsar load balancing · PIP-364 · Consistent Hashing with Bounded Loads · Google Research blog
