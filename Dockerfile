# duckwaked: a worker that serves the duckwake_* SQL functions and the lake over Quack, plus Prometheus metrics.
#   docker build -t duckwake .
#   docker run --network host -e DUCKWAKE_CATALOG=... -e DUCKWAKE_QUACK_TOKEN=... -v lake:/data duckwake
# The image also has the `duckwake` CLI (`docker run --entrypoint duckwake duckwake --help`).
FROM mirror.gcr.io/library/rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
# The first build compiles DuckDB from source (several minutes).
RUN cargo build --release -p duckwake --bin duckwake --bin duckwaked

FROM mirror.gcr.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10001 duckwake && mkdir /data && chown duckwake /data
COPY --from=build /src/target/release/duckwake /src/target/release/duckwaked /usr/local/bin/
USER duckwake
# The DuckDB extensions the server loads, installed now so it starts without downloading them.
RUN duckwake sql --bare "INSTALL ducklake; INSTALL postgres; INSTALL icu; INSTALL quack"
# Quack and metrics listen on localhost: with `--network host` that is the host's localhost. Quack serves plain
# HTTP only on local addresses (a non-local one switches it to HTTPS with a self-signed certificate); to serve
# other machines, put a TLS reverse proxy in front, as Quack's docs recommend.
ENV DUCKWAKE_DATA_PATH=/data \
    DUCKWAKE_WORKER=server \
    DUCKWAKE_QUACK=quack:localhost:9494 \
    DUCKWAKE_METRICS=127.0.0.1:9464 \
    RUST_LOG=info,rskafka=warn
VOLUME /data
EXPOSE 9494 9464
ENTRYPOINT ["duckwaked"]
