#!/usr/bin/env bash
# Builds the loadable extension: target/release/duckwake.duckdb_extension for this platform. It uses DuckDB's
# stable V1 C API pinned to one version, which DuckDB 2.0 loads (the unstable or V2 API would need the V2
# entry point, which the Rust macros don't generate yet). It is unsigned, so the host must allow that:
# `duckdb -unsigned`, or `allow_unsigned_extensions` set when the database starts.
set -euo pipefail
cd "$(dirname "$0")/.."
# The stable V1 C API version the extension asks for; DuckDB 2.0 still loads V1 extensions pinned to one.
CAPI_VERSION=${CAPI_VERSION:-v1.5.6}
VERSION=v$(awk -F'"' '/^version/ {print $2; exit}' Cargo.toml)

cargo build --release -p duckwake_extension
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) PLATFORM=osx_arm64; LIB=libduckwake_extension.dylib ;;
  Darwin-x86_64) PLATFORM=osx_amd64; LIB=libduckwake_extension.dylib ;;
  Linux-x86_64) PLATFORM=linux_amd64; LIB=libduckwake_extension.so ;;
  Linux-aarch64) PLATFORM=linux_arm64; LIB=libduckwake_extension.so ;;
  *) echo "unsupported platform $(uname -s)-$(uname -m)"; exit 1 ;;
esac
OUT=target/release/duckwake.duckdb_extension
cp "target/release/$LIB" "$OUT"

# DuckDB's extension footer (as extension-ci-tools' append_extension_metadata.py writes it): a WebAssembly
# custom section header, eight 32-byte fields written last to first, and 256 bytes left for a signature.
python3 - "$OUT" "$PLATFORM" "$CAPI_VERSION" "$VERSION" <<'EOF'
import sys
out, platform, capi_version, version = sys.argv[1:]
field = lambda s: s.encode('ascii') + b'\0' * (32 - len(s))
footer = bytes([0, 0x93, 0x04, 16]) + b'duckdb_signature' + bytes([0x80, 0x04])
for f in ['', '', '', 'C_STRUCT', version, capi_version, platform, '4']:
    footer += field(f)
footer += b'\0' * 256
with open(out, 'ab') as fh:
    fh.write(footer)
EOF
echo "$OUT ($PLATFORM, C API $CAPI_VERSION, $VERSION)"
