# Extension: `LOAD duckwake` in your own DuckDB

DuckWake is a DuckDB extension, and Quack is another: a DuckDB you run can load both and become a DuckWake
server. [`duckwaked`](server.md) is that same setup packaged as one supervised binary.

DuckWake is a [DuckDB community extension](https://github.com/duckdb/community-extensions/pull/3654), built
and signed by DuckDB's CI for DuckDB 2.0 on Linux (amd64, arm64) and macOS on Apple Silicon (`osx_arm64`), so
the DuckDB 2.0 CLI installs it with no flags:

```sql
INSTALL duckwake FROM community;
LOAD duckwake;
CALL duckwake_connect('ducklake:postgres:dbname=lake host=localhost port=55432 user=lake password=lake',
                      data_path := '.lake/data/');     -- or set DUCKWAKE_CATALOG / DUCKWAKE_DATA_PATH
CALL duckwake_worker_start(worker := 'w1', pipeline := true, metrics := '127.0.0.1:9464');
ATTACH 'ducklake:postgres:dbname=lake host=localhost port=55432 user=lake password=lake' AS lake (DATA_PATH '.lake/data/');

-- Use it here ...
CALL duckwake_start('clicks_per_min');
CALL duckwake_wait('clicks_per_min');
FROM lake.analytics.clicks_per_min;

-- ... or serve it: another DuckDB attaches with ATTACH 'quack:localhost:9494' ... ; CONNECT, as for duckwaked.
INSTALL quack; LOAD quack;
CALL quack_serve('quack:localhost:9494', token = 'secret');
```

On other platforms, or to run a build of your own, install from an extension repository instead (unsigned,
so the host needs `duckdb -unsigned`): the project's at `https://duckwake.github.io/extensions` (Linux and
macOS, amd64 and arm64, each release), or one you build with `scripts/package_extension.sh`, which lays out
`dist/extensions/v2.0.0/<platform>/`:

```sql
INSTALL duckwake FROM 'https://duckwake.github.io/extensions';   -- or '/path/to/dist/extensions'
```

The worker runs in that DuckDB's process for as long as it lives; keep the process running (a terminal,
tmux, systemd) to keep the views fresh. If the worker fails (a view's lookup table dropped, Kafka or the
catalog unreachable) it restarts from its last checkpoint 5 s later; a view that can no longer compile is
marked `failed` and the others carry on. `CALL duckwake_worker_stop()` stops it.

Extensions get no handle to the host database, so the functions run on a private DuckDB in the same process
that attaches the lake (`duckwake_connect`); attach the same lake in your session to read the tables, as
`duckwake_connect`'s result spells out. The extension is the engine built against the host's DuckDB
library (DuckDB's V1 C API, pinned to v1.5.6, which DuckDB 2.0 loads).

## Distribution

`scripts/package_extension.sh` lays the build out as a DuckDB extension repository
(`dist/extensions/v2.0.0/<platform>/duckwake.duckdb_extension.gz`); any static host (S3, GitHub Pages) can serve
that folder, and `INSTALL duckwake FROM '<url>'` installs from it. `.github/workflows/release.yml` builds it for
Linux and macOS (amd64, arm64) on a `v*` tag or a manual run, attaches the merged repository to the release,
and can publish it on the website ([releasing.md](releasing.md)).
**Community extension.** DuckDB's community extension CI builds, tests and signs DuckWake for DuckDB's latest
release (v2.0.0) and serves it with `INSTALL duckwake FROM community`. Its descriptor,
`extensions/duckwake/description.yml` in `duckdb/community-extensions`, names this repository and the commit to
build; each release moves it to the release's commit ([releasing.md](releasing.md)). This repository is laid
out for that CI:

- `Makefile` at the root: the CI checks out `duckdb/extension-ci-tools` into `./extension-ci-tools` and runs
  `make configure`, `make release` and `make test_release`. The Makefile builds the library with
  `cargo build --release -p duckwake_extension` (not the workspace's default members, which bundle DuckDB) and
  lets the CI's `append_extension_metadata.py` write the footer: the stable C API, pinned to v1.5.6, as
  `scripts/build_extension.sh` writes it locally.
- `test/sql/duckwake.test`: the CI's sqllogictest, with no PostgreSQL or Kafka. It checks that the extension
  loads, registers its 22 functions and that one needing a lake says so; `scripts/extension_test.sh` is the
  end-to-end test.
- Windows, WebAssembly and musl Linux are left out (`excluded_platforms`): Windows is not built yet, and the
  worker needs threads and network sockets. DuckDB's CI does not build `osx_amd64` (Intel Macs) for it; those
  install from the project's repository above.

The first version, accepted on 2026-11-17 ([duckdb/community-extensions#3654](https://github.com/duckdb/community-extensions/pull/3654)),
is the commit `16ec8d6` on `main`, after 0.2.0.

The extension-only functions (`duckwake_connect`, `duckwake_worker_start`, `duckwake_worker_stop`) are in
[sql.md](sql.md).
