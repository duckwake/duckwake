#!/usr/bin/env bash
# Lays out the extension as a DuckDB extension repository: dist/extensions/<duckdb version>/<platform>/
# duckwake.duckdb_extension(.gz). Serve that folder over HTTP (S3, GitHub Pages, any static host) or use it as a
# local path, and users install by name:
#   duckdb -unsigned
#   INSTALL duckwake FROM 'https://your-host/extensions';   -- or FROM '/path/to/dist/extensions'
#   LOAD duckwake;
# Builds for this machine's platform; run it on each platform (or in CI) to fill in the others.
set -euo pipefail
cd "$(dirname "$0")/.."
DUCKDB_VERSION=${DUCKDB_VERSION:-v2.0.0}
DIST=${DIST:-dist/extensions}
out=$(bash scripts/build_extension.sh | tail -1)
platform=$(sed -E 's/.*\(([a-z0-9_]+),.*/\1/' <<<"$out")
dir="$DIST/$DUCKDB_VERSION/$platform"
mkdir -p "$dir"
cp target/release/duckwake.duckdb_extension "$dir/"
gzip -kf "$dir/duckwake.duckdb_extension"
ls -l "$dir"
