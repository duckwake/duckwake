# Versions and releases

## Versioning

DuckWake follows [Semantic Versioning](https://semver.org/). The version is the workspace's in `Cargo.toml`;
`duckwake --version` prints it, every worker records it in its heartbeat row (`duckwake workers`, column
`version`) and exports it as `duckwake_build_info{version, duckdb}`.

Until 1.0:

- **Patch** (0.2.0 → 0.2.1): fixes only. Workers of both versions may run on one lake at the same time, so a
  rolling restart upgrades without stopping views.
- **Minor** (0.2 → 0.3): may add features, change defaults, and change what the lake stores (control tables,
  stored view definitions). A newer worker migrates a lake the first time it opens it and stays able to read what
  older ones wrote; older workers may not read what newer ones write. Stop every older worker before the first
  newer one starts, and check the changelog's notes for the version.
- A view whose state or table layout the new compiler would build differently is refused at start with a
  message to replace it (`create-view --replace`).

From 1.0, minor versions stay compatible with workers of the same major on one lake.

Each release pins the DuckDB it embeds (the `duckdb` crate, DuckDB 2.0.0) and the DuckDB extensions the image
installs (`ducklake`, `postgres`, `icu`, `quack`); the extension is built against DuckDB's C API v1.5.6, which
DuckDB 2.0 loads.

## What a release publishes

On a `v*` tag, `.github/workflows/release.yml`:

| Artifact | Where |
| --- | --- |
| `duckwake` and `duckwaked` for Linux (amd64, arm64; glibc 2.39 or newer) and macOS (arm64, amd64) | release assets `duckwake-<version>-<platform>.tar.gz` |
| The extension as a DuckDB extension repository (`extensions/v2.0.0/<platform>/duckwake.duckdb_extension.gz`) | release asset `duckwake-extensions-<version>.tar.gz`; optionally the website (below) |
| `SHA256SUMS` of every archive | release asset |
| The `duckwaked` image, amd64 and arm64 | `ghcr.io/duckwake/duckwake:<version>`, and `:<major>.<minor>` and `:latest` unless it is a pre-release |
| Release notes | the version's section of `CHANGELOG.md` |

A tag with a pre-release suffix (`v0.2.0-rc.1`) makes a GitHub pre-release and leaves `:latest` alone. A manual
run (Actions → release → Run workflow) builds everything and publishes nothing.

Verify a download: `sha256sum -c SHA256SUMS --ignore-missing`.

**The extension repository on the website.** When the repository variable `PUBLISH_EXTENSIONS` is `true` and
the secret `PAGES_TOKEN` (a fine-grained token with contents write access to `duckwake/duckwake.github.io`) is
set, a release also copies the extension repository into that site, so users install with
`INSTALL duckwake FROM 'https://duckwake.github.io/extensions'`. The site is public: turn this on only when the
extension may be public.

**The image's visibility.** Packages of a private repository start private on ghcr.io. To let anyone
`docker pull` it, an organization owner sets the `duckwake` package to public (Packages → duckwake → Package
settings → Change visibility).

## Making a release

1. Move the `[Unreleased]` entries in `CHANGELOG.md` under a new `## [X.Y.Z] - YYYY-MM-DD` heading, with notes
   on anything an upgrade needs (see Versioning), and update the links at the bottom.
2. Set `version = "X.Y.Z"` in `Cargo.toml` and run `cargo build` (it updates `Cargo.lock`).
3. Open a pull request; merge it when `ci-passed` is green.
4. Tag the merge commit and push the tag:
   `git tag -a vX.Y.Z -m "DuckWake X.Y.Z" && git push origin vX.Y.Z`.
5. The release workflow refuses a tag that does not match `Cargo.toml` or has no changelog section, runs clippy
   and the unit tests, then builds and publishes. Check the release page, the image tags, and
   `docker run --rm --entrypoint duckwake ghcr.io/duckwake/duckwake:X.Y.Z --version`.
