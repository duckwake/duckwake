# Security policy

## Reporting a vulnerability

Please do not open a public issue for a security problem. Report it privately through GitHub: on the
repository's **Security** tab, choose **Report a vulnerability**. Include what you found, how to reproduce it,
and the version (`duckwake --version`) or commit.

You will get an acknowledgement within a few days. Once a fix is ready, it ships in a release, and the advisory
is published with credit to you unless you prefer otherwise.

## Supported versions

Until 1.0, security fixes go into the latest release only.

## Scope

Things worth reporting include a way to read or change a lake without the Quack token, credentials written to the
lake or to logs, and a malformed Kafka message that crashes a worker or corrupts a view. The Quack server has one
shared token and no TLS of its own; serving it beyond localhost without a TLS proxy is a known limit, described
in [server.md](docs/server.md). The TLS keys under `docker/kafka/` are test material for the local stack, kept in
the repository on purpose.
