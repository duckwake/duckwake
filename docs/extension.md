# Extension: `LOAD duckwake` in your own DuckDB

DuckWake is a DuckDB extension, and Quack is another: a DuckDB you run can load both and become a DuckWake
server. [`duckwaked`](server.md) is that same setup packaged as one supervised binary.

```sh
scripts/package_extension.sh        # builds it and lays out dist/extensions/v2.0.0/<platform>/ as an extension repository
duckdb -unsigned                    # the DuckDB 2.0 CLI (the extension is not signed yet)
```

```sql
INSTALL duckwake FROM '/path/to/ducklake_streaming/dist/extensions';   -- or an HTTP(S) URL serving that folder
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
Until DuckWake is a signed community extension, hosts need `-unsigned`. DuckDB's community extension CI builds
Rust extensions through `extension-template-rs`, which is still experimental and uses the V2 C API; DuckWake
uses the V1 API, so it would move to the V2 entry point before being submitted.

The extension-only functions (`duckwake_connect`, `duckwake_worker_start`, `duckwake_worker_stop`) are in
[sql.md](sql.md).
