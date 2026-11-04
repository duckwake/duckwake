//! `duckwake serve` / `duckwaked`: the production front-end. One process runs
//! - a worker: the engine on its own embedded DuckDB, exactly as `duckwake run`;
//! - a serving DuckDB with the lake attached and the `duckwake_*` SQL functions registered, served over
//!   Quack, so any DuckDB can `ATTACH 'quack:host' AS dw (TOKEN '...')`, `CONNECT dw`, manage views with
//!   `CALL duckwake_create_view(...)` and read their tables;
//! - a Prometheus endpoint (`GET /metrics`) with per-view freshness, lag, epochs and row counts.
//!
//! A worker that fails (a view that cannot run, Kafka or the catalog down) restarts from its last checkpoint
//! after 5 s; the server stays up. `duckwake_worker_status()` and the metrics show the restarts.
use crate::cli::{shutdown_on_signal, WorkerArgs};
use anyhow::{Context, Result};
use duckwake_core::engine;
use duckwake_core::lake::{bootstrap, connect_again, lit, LakeConfig};
use duckwake_core::metrics::Metrics;
use duckwake_core::sql_functions::{self, Api};
use std::sync::Arc;
use tracing::info;

#[derive(clap::Args, Clone)]
pub struct ServeArgs {
    /// Where Quack listens (`quack:HOST[:PORT]`, default port 9494); `none` turns it off. Quack serves plain
    /// HTTP on localhost only; another host switches it to HTTPS with a self-signed certificate. To serve other
    /// machines, keep localhost and put a TLS reverse proxy in front.
    #[arg(long, env = "DUCKWAKE_QUACK", default_value = "quack:localhost:9494")]
    pub quack: String,
    /// Token Quack clients must send. Without one, Quack generates a token and the server prints it once.
    #[arg(long, env = "DUCKWAKE_QUACK_TOKEN", hide_env_values = true)]
    pub quack_token: Option<String>,
    /// Address of the HTTP endpoint (`GET /metrics`, `GET /healthz`); `none` turns it off.
    #[arg(long, env = "DUCKWAKE_METRICS", default_value = "127.0.0.1:9464")]
    pub metrics: String,
}

pub fn run(lake: &LakeConfig, rt: tokio::runtime::Runtime, w: &WorkerArgs, s: &ServeArgs) -> Result<()> {
    let shutdown = shutdown_on_signal(&rt);
    let conn = lake.open()?;
    bootstrap(&conn)?;
    let metrics = Arc::new(Metrics::default());
    let api = Api::new(connect_again(&conn)?, Some(w.worker.clone()), rt.handle().clone(), metrics.clone());
    sql_functions::register(&conn, &api)?;
    if s.quack != "none" {
        serve_quack(&conn, s)?;
    }
    if s.metrics != "none" {
        metrics.serve_http(&s.metrics, rt.handle())?;
        info!(addr = %s.metrics, "metrics at /metrics");
    }
    let lake = lake.clone();
    let w = w.clone();
    engine::supervise(move || w.config(&lake), rt.handle().clone(), shutdown, metrics)?;
    info!("server stopped");
    // `conn` (and with it the Quack server) lives until here.
    drop(conn);
    Ok(())
}

/// Starts Quack on the serving instance. A generated token is printed once, to stderr, with the line a
/// client runs to attach.
fn serve_quack(conn: &duckdb::Connection, s: &ServeArgs) -> Result<()> {
    let host = s.quack.trim_start_matches("quack:").rsplit_once(':').map_or(s.quack.trim_start_matches("quack:"), |(h, _)| h);
    let mut opts = String::new();
    if let Some(t) = &s.quack_token {
        opts.push_str(&format!(", token = {}", lit(t)));
    }
    if !matches!(host, "localhost" | "127.0.0.1" | "[::1]") {
        opts.push_str(", allow_other_hostname = true");
    }
    conn.execute_batch("INSTALL quack; LOAD quack;").context("loading the quack extension")?;
    let (uri, token): (String, Option<String>) = conn
        .query_row(&format!("SELECT listen_uri, auth_token FROM quack_serve({}{opts})", lit(&s.quack)), [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .with_context(|| format!("starting Quack on {}", s.quack))?;
    info!(uri = %uri, "serving the duckwake_* functions and the lake over Quack");
    if s.quack_token.is_none() {
        if let Some(t) = token {
            eprintln!("\n  Quack token: {t}\n  attach with: ATTACH '{uri}' AS dw (TOKEN '{t}'); CONNECT dw;\n");
        }
    }
    Ok(())
}
