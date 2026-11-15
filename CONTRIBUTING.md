# Contributing to DuckWake

Thanks for your interest. DuckWake is on its way to production, so issues that report what you tried, what broke and
what you expected are as useful as code.

## Before you start

- **Bugs:** open an issue with the bug template. A failing script or SQL that reproduces it helps most.
- **Features:** open an issue first for anything larger than a small fix, so we can agree on the approach. The
  [roadmap](docs/roadmap.md) and the [design documents](docs/design/) say where things are headed.
- **Security problems:** do not open a public issue; see [SECURITY.md](SECURITY.md).

## Building

You need Rust (stable) and Docker.

```sh
docker compose up -d --wait            # PostgreSQL (the lake's catalog), Redpanda (Kafka), MinIO (S3)
cargo build --release                  # the first build compiles DuckDB, about 6 minutes
bash scripts/build_extension.sh        # the loadable extension, built on its own
```

[testing.md](docs/testing.md) describes every suite. The ones CI runs on each pull request:

```sh
cargo clippy --release --all-targets -- -D warnings
cargo test --release
scripts/crash_test.sh                  # or any suite under scripts/*_test.sh
```

Each integration suite checks its views against the same SQL run as one batch query and exits non-zero on any
difference. A change to the engine should keep every suite exact.

## Pull requests

- Keep a pull request to one change, with a description of what it does and how you tested it.
- Add or extend a test for behaviour you change: a unit test, or a suite under `scripts/` for anything that
  involves crashes, Kafka or the lake.
- Run `cargo clippy` and `cargo test` before pushing; CI runs those and every suite, and `ci-passed` must be
  green before merging.
- Add a line under `[Unreleased]` in [CHANGELOG.md](CHANGELOG.md) for anything a user would notice.
- New dependencies must use a license that `cargo deny check licenses` accepts (see `deny.toml`). If
  `Cargo.lock` changes, regenerate the third-party notices with
  `cargo about generate about.hbs -o THIRD_PARTY_LICENSES.md`.

## Writing style

Docs and comments are plain and concrete: say what the code does, with numbers where there are numbers, and say
what is not supported yet. Short sentences beat long ones.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE), the same as
the project.
