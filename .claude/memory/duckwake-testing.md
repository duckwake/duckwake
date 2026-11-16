---
name: duckwake-testing
description: How to build and test DuckWake locally without false failures - cargo commands, which binary has the SQL functions, ports, test lake
metadata:
  type: feedback
---

- Run `cargo test --release` and `cargo clippy --release --all-targets` on the default members, as CI does.
  `cargo test --workspace` fails the circuit tests with "DuckDB API not initialized": feature unification with
  the loadable extension crate, not a real failure. Check the extension with `cargo check --release -p duckwake_extension`.
- rustfmt is not installed and CI does not run fmt; ignore `cargo fmt --check`.
- `duckwake sql` does not register the `duckwake_*` functions. Test them through `duckwaked` over Quack
  (`scripts/serve_test.sh`, or `duckwake sql --server quack:...`) or the extension.
- The integration suites use their own lake (`scripts/lib.sh`: database `$TEST_DB`, data under `.lake/test/`),
  so they are safe to run while the playground runs. The playground may hold port 9464; run `serve_test.sh`
  with `QUACK_PORT=9495 METRICS_PORT=9465`.
- `scripts/limits_test.sh` pins `--threads 12` and `--max-spill 32MB`: under a 64 MB limit DuckDB spills before it
  runs out, so without the spill cap a roomy runner never exercises the limit. Before that, an epoch only ran out of memory with many
  DuckDB threads (4-core CI runners otherwise spill and pass).
- Under `set -o pipefail`, put `|| true` at the end of a grep pipeline, not inside it.
- Without Docker (cloud sessions), suites that need only PostgreSQL run with `PG_LOCAL=1` against a local
  server on localhost:55432 with a `lake` role (`scripts/backfill_test.sh`). Read a view's position with the key
  `"0"` of its checkpoint offsets (`'$.*."0"'`); key `"1"` is a backfill cursor. Table names contain dots, so a
  JSON path must quote them. `set -e` applies inside an EXIT trap: end it with `|| true`.

**Why:** each of these cost a debugging round once.

**How to apply:** use these before trusting a red result locally. Related: [[duckwake-production-grade]].
