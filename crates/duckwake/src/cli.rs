//! The `duckwake` CLI: worker, server, playground and a command per function of the control API.
//!
//! Subcommands mirror the SQL functions of the design (`duckwake_create_source`, `duckwake_create_view`,
//! `duckwake_start`, `duckwake_jobs()`, ...), which `serve` registers as SQL and serves over Quack. A view
//! is named by the DuckLake table it maintains (`analytics.clicks_per_min`); the other commands take that
//! name or the bare table name, which is the view's job id.
use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use duckwake_core::lake::{self, LakeConfig, CTL};
use duckwake_core::maintenance::MaintenanceConfig;
use duckwake_core::model::parse_duration;
use duckwake_core::{api, catalog, engine, kafka, verify};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::{playground, serve};

#[derive(Parser)]
#[command(version, about = "Continuous SQL over Kafka into DuckLake, exactly once")]
struct Cli {
    /// DuckLake metadata catalog (the part after `ducklake:` in ATTACH).
    #[arg(long, global = true, env = "DUCKWAKE_CATALOG", default_value = "postgres:dbname=lake host=localhost port=55432 user=lake password=lake")]
    catalog: String,
    /// Where DuckLake writes data files.
    #[arg(long, global = true, env = "DUCKWAKE_DATA_PATH", default_value = ".lake/data/")]
    data_path: String,
    /// DuckDB's memory limit per process, e.g. 2GB (default: 80% of RAM, too much with several workers on a
    /// machine). Past it, DuckDB spills to --temp-dir.
    #[arg(long, global = true, env = "DUCKWAKE_MEMORY_LIMIT")]
    memory_limit: Option<String>,
    /// Where DuckDB spills; a worker uses <temp-dir>/<worker id> and clears it at start (default .tmp).
    #[arg(long, global = true, env = "DUCKWAKE_TEMP_DIR")]
    temp_dir: Option<String>,
    /// DuckDB threads per process (default: one per core). The memory an epoch needs grows with them: with a
    /// small --memory-limit, fewer threads let more of it fit before DuckDB runs out.
    #[arg(long, global = true, env = "DUCKWAKE_THREADS")]
    threads: Option<usize>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create the __duckwake control schema.
    Init,
    /// duckwake_create_source(name, kind, options)
    CreateSource {
        name: String,
        #[arg(long, default_value = "kafka")]
        kind: String,
        /// JSON: brokers, topic, event_time, columns, watermark_delay, idle_timeout, start
        #[arg(long)]
        options: String,
    },
    /// duckwake_create_view(name, sql, options): NAME is the table the view maintains, e.g.
    /// analytics.clicks_per_min (a bare name means the main schema). The first word of the output says what
    /// happened: created, unchanged, altered or replaced.
    CreateView {
        name: String,
        #[arg(long)]
        sql: String,
        /// Deprecated: names the table separately, with NAME as the job id.
        #[arg(long, hide = true)]
        sink: Option<String>,
        #[arg(long)]
        late_data: Option<String>,
        #[arg(long, default_value = "ducklake")]
        checkpoint: String,
        #[arg(long, default_value = "10s")]
        epoch_interval: String,
        #[arg(long, default_value_t = 100_000)]
        max_rows: usize,
        #[arg(long, default_value_t = 10_000)]
        inline_row_limit: usize,
        /// Fail the view once its state holds more rows than this (open windows, groups or distinct rows).
        #[arg(long)]
        max_state_rows: Option<u64>,
        /// Fail the view when one epoch writes more late rows than this (a broken watermark or clock).
        #[arg(long)]
        max_late_rows: Option<u64>,
        /// When the view exists: fail (the default); update, which changes its settings without recomputing
        /// it, does nothing when nothing changed, and fails when its query changed; or replace.
        #[arg(long, value_parser = ["fail", "update", "replace"])]
        if_exists: Option<String>,
        /// Replace the view if it exists (CREATE OR REPLACE, --if-exists replace): it recomputes from the
        /// start, keeping its status and worker.
        #[arg(long)]
        replace: bool,
    },
    /// duckwake_add_source_column(source, column, type): views that read the source keep their columns.
    AddSourceColumn { source: String, column: String, r#type: String },
    /// duckwake_alter_view(view, ...): how a view runs, not what it computes; a limit of 0 removes it.
    AlterView {
        name: String,
        #[arg(long)]
        epoch_interval: Option<String>,
        #[arg(long)]
        max_rows: Option<usize>,
        #[arg(long)]
        max_state_rows: Option<u64>,
        #[arg(long)]
        max_late_rows: Option<u64>,
    },
    /// duckwake_start(view): assign to a worker (bumps the lease, fencing the previous owner).
    Start {
        name: String,
        /// The worker to pin it to (rebalancing never moves it; after a failover it goes back there); `any`
        /// (the default) leaves it to whichever live worker claims it, and lets rebalancing move it.
        #[arg(long, default_value = "any")]
        worker: String,
    },
    /// duckwake_drain(worker): the worker hands all its views to the others and takes none, before it is
    /// taken out for good. A restart ends the drain.
    Drain { worker: String },
    /// duckwake_pause(view)
    Pause { name: String },
    /// duckwake_stop(view)
    Stop { name: String },
    /// duckwake_reset(view, to_epoch): rewind to a checkpoint and replay from its offsets.
    Reset {
        name: String,
        #[arg(long)]
        to_epoch: i64,
    },
    /// Drop a stopped view and its table (a late-data table is kept).
    Drop {
        name: String,
        /// Keep the view's table.
        #[arg(long)]
        keep_table: bool,
    },
    /// duckwake_jobs(): status, epoch, watermark, freshness, lag.
    Jobs,
    /// duckwake_workers(): every worker that ran on this lake, its state, views and last heartbeat.
    Workers,
    /// duckwake_checkpoints(view)
    Checkpoints {
        name: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Run a worker.
    Run {
        #[command(flatten)]
        w: WorkerArgs,
        /// Serve Prometheus metrics (`GET /metrics`, `GET /healthz`) on this address, e.g. 0.0.0.0:9187.
        #[arg(long, env = "DUCKWAKE_METRICS")]
        metrics: Option<String>,
    },
    /// Run a worker and serve the duckwake_* SQL functions and the lake over Quack, plus Prometheus metrics
    /// (the `duckwaked` daemon).
    Serve {
        #[command(flatten)]
        w: WorkerArgs,
        #[command(flatten)]
        s: serve::ServeArgs,
    },
    /// Generate test events into a Kafka topic.
    Produce {
        #[arg(long, default_value = "localhost:19092")]
        brokers: String,
        #[arg(long, default_value = "clicks")]
        topic: String,
        #[arg(long, default_value_t = 3)]
        partitions: i32,
        /// Events per second.
        #[arg(long, default_value_t = 1000)]
        rate: u64,
        /// Total events (0 = until interrupted).
        #[arg(long, default_value_t = 0)]
        count: u64,
        /// Every Nth event is 5 minutes late (0 = never).
        #[arg(long, default_value_t = 0)]
        late_every: u64,
        /// Every Nth event is malformed (0 = never).
        #[arg(long, default_value_t = 0)]
        bad_every: u64,
        /// Random event-time disorder, in ms.
        #[arg(long, default_value_t = 2000)]
        disorder_ms: u64,
        /// Pad each event to about this many bytes.
        #[arg(long, default_value_t = 0)]
        pad_bytes: usize,
        /// At the end, send one far-future event per partition so every real window closes.
        #[arg(long)]
        eos: bool,
    },
    /// Differential exactly-once check of a view against a batch recomputation.
    Verify { name: String },
    /// duckwake_explain(view): the view's circuit, how it was made incremental, and one epoch's SQL.
    Explain { name: String },
    /// Run SQL against the embedded DuckDB with the lake attached.
    Sql {
        query: String,
        /// Print the last statement's result as headerless CSV (for scripts).
        #[arg(long)]
        csv: bool,
        /// Run the statements on a duckwake server over Quack (`quack:HOST[:PORT]`) instead of locally.
        #[arg(long, env = "DUCKWAKE_SERVER")]
        server: Option<String>,
        /// The server's Quack token.
        #[arg(long, env = "DUCKWAKE_QUACK_TOKEN", hide_env_values = true)]
        token: Option<String>,
        /// Run on a plain in-memory DuckDB without the lake that allows unsigned extensions (to test the
        /// loadable extension).
        #[arg(long, hide = true)]
        bare: bool,
    },
    /// Web playground: an in-process worker plus a UI for sources, tables, views and SQL.
    Playground {
        #[arg(long, default_value_t = 8787)]
        port: u16,
        #[arg(long, default_value = "playground")]
        worker: String,
        #[arg(long, default_value = "localhost:19092")]
        brokers: String,
        /// As for `run`.
        #[arg(long, default_value_t = 1)]
        stage_threads: usize,
        #[arg(long)]
        pipeline: bool,
        #[arg(long)]
        shared_readers: bool,
        #[arg(long)]
        lean_commit: bool,
        /// Serve the worker's Prometheus metrics (`GET /metrics`) on this address, e.g. 127.0.0.1:19187, for the
        /// Grafana dashboard (docs/alerts.md).
        #[arg(long, env = "DUCKWAKE_METRICS")]
        metrics: Option<String>,
    },
}

/// The worker's flags, shared by `run` and `serve`.
#[derive(clap::Args, Clone)]
pub struct WorkerArgs {
    #[arg(long, env = "DUCKWAKE_WORKER")]
    pub worker: String,
    #[arg(long)]
    pub max_epochs: Option<u64>,
    #[arg(long)]
    pub no_maintenance: bool,
    #[arg(long, default_value = "10m")]
    pub flush_every: String,
    #[arg(long, default_value = "1h")]
    pub merge_every: String,
    #[arg(long, default_value = "1h")]
    pub expire_every: String,
    #[arg(long, default_value = "24h")]
    pub expire_older_than: String,
    #[arg(long, default_value = "24h")]
    pub cleanup_every: String,
    #[arg(long, default_value = "1h")]
    pub prune_every: String,
    #[arg(long, default_value_t = 1000)]
    pub keep_checkpoints: i64,
    /// Failover: a worker whose heartbeat has not moved for this long is dead and its views move to live
    /// workers; `off` keeps this worker from taking over others' views.
    #[arg(long, default_value = "30s", env = "DUCKWAKE_LEASE_TIMEOUT")]
    pub lease_timeout: String,
    /// How long the views of a cleanly stopped worker wait for it to come back (a restart) before they move.
    /// Take a worker out for good with `drain` first.
    #[arg(long, default_value = "60s", env = "DUCKWAKE_STOP_GRACE")]
    pub stop_grace: String,
    /// How often a worker checks the balance; `off` moves views only on failover, drain and pins.
    #[arg(long, default_value = "30s", env = "DUCKWAKE_REBALANCE_EVERY")]
    pub rebalance_every: String,
    /// A view runs at least this long on a worker before rebalancing moves it again.
    #[arg(long, default_value = "5m", env = "DUCKWAKE_REBALANCE_COOLDOWN")]
    pub rebalance_cooldown: String,
    /// Most views the busiest worker gives away per round.
    #[arg(long, default_value_t = 2, env = "DUCKWAKE_REBALANCE_MAX_MOVES")]
    pub rebalance_max_moves: usize,
    /// Imbalance tolerated: the busiest worker may carry up to (1 + this) times the average load.
    #[arg(long, default_value_t = 0.2, env = "DUCKWAKE_REBALANCE_EPSILON")]
    pub rebalance_epsilon: f64,
    /// Stage views in parallel on this many threads, each with its own connection (0 = half the cores).
    #[arg(long, default_value_t = 1, env = "DUCKWAKE_STAGE_THREADS")]
    pub stage_threads: usize,
    /// Stage the next epoch while the previous one commits.
    #[arg(long, env = "DUCKWAKE_PIPELINE")]
    pub pipeline: bool,
    /// Views of one source at the same position share one Kafka reader and one decode.
    #[arg(long, env = "DUCKWAKE_SHARED_READERS")]
    pub shared_readers: bool,
    /// Skip epoch writes with nothing to write; one INSERT for all views' checkpoints and dead letters.
    #[arg(long, env = "DUCKWAKE_LEAN_COMMIT")]
    pub lean_commit: bool,
}

impl WorkerArgs {
    pub fn config(&self, lake: &LakeConfig) -> Result<engine::WorkerConfig> {
        Ok(engine::WorkerConfig {
            worker_id: self.worker.clone(),
            lake: lake.clone(),
            maintenance: MaintenanceConfig {
                enabled: !self.no_maintenance,
                flush_every: parse_duration(&self.flush_every)?,
                merge_every: parse_duration(&self.merge_every)?,
                expire_every: parse_duration(&self.expire_every)?,
                expire_older_than: parse_duration(&self.expire_older_than)?,
                cleanup_every: parse_duration(&self.cleanup_every)?,
                prune_every: parse_duration(&self.prune_every)?,
                keep_checkpoints: self.keep_checkpoints,
            },
            max_epochs: self.max_epochs,
            refresh_every: Duration::from_secs(2),
            lease_timeout: match self.lease_timeout.as_str() {
                "off" | "0" => None,
                t => Some(parse_duration(t)?),
            },
            stop_grace: parse_duration(&self.stop_grace)?,
            rebalance: match self.rebalance_every.as_str() {
                "off" | "0" => None,
                every => Some(duckwake_core::failover::RebalanceConfig {
                    every: parse_duration(every)?,
                    cooldown: parse_duration(&self.rebalance_cooldown)?,
                    max_moves: self.rebalance_max_moves.max(1),
                    epsilon: self.rebalance_epsilon.max(0.0),
                }),
            },
            stage_threads: self.stage_threads,
            pipeline: self.pipeline,
            shared_readers: self.shared_readers,
            lean_commit: self.lean_commit,
            metrics: None,
        })
    }
}

/// Sets a flag on Ctrl-C or SIGTERM.
pub fn shutdown_on_signal(rt: &tokio::runtime::Runtime) -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = shutdown.clone();
    rt.spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        tracing::info!("shutdown requested; finishing the current epoch");
        flag.store(true, Ordering::Relaxed);
    });
    shutdown
}

/// Runs the CLI on `args` (the program name first).
pub fn main_with(args: Vec<std::ffi::OsString>) -> Result<()> {
    let cli = Cli::parse_from(args);
    let logs = playground::LogBuf::default();
    let ui_layer = matches!(cli.cmd, Cmd::Playground { .. })
        .then(|| tracing_subscriber::fmt::layer().with_ansi(false).with_target(false).with_writer(logs.clone()));
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,rskafka=warn".into()))
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
                .with_writer(std::io::stderr),
        )
        .with(ui_layer)
        .init();
    let lake_cfg = LakeConfig {
        catalog: cli.catalog.clone(),
        data_path: cli.data_path.clone(),
        memory_limit: cli.memory_limit.clone(),
        temp_dir: cli.temp_dir.clone(),
        threads: cli.threads,
    };
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().worker_threads(4).build()?;
    let open = || -> Result<duckdb::Connection> {
        let c = lake_cfg.open()?;
        lake::bootstrap(&c)?;
        Ok(c)
    };

    match cli.cmd {
        Cmd::Init => {
            open()?;
            println!("ok: {CTL} ready");
        }
        Cmd::CreateSource { name, kind, options } => {
            println!("{}", api::create_source(&open()?, &name, &kind, &options)?);
        }
        Cmd::CreateView {
            name,
            sql,
            sink,
            late_data,
            checkpoint,
            epoch_interval,
            max_rows,
            inline_row_limit,
            max_state_rows,
            max_late_rows,
            if_exists,
            replace,
        } => {
            if sink.is_some() {
                eprintln!("warning: --sink is deprecated; name the view after its table: create-view analytics.<table>");
            }
            let o = api::ViewOptions {
                late_data,
                checkpoint: Some(checkpoint),
                epoch_interval: Some(epoch_interval),
                max_rows: Some(max_rows),
                inline_row_limit: Some(inline_row_limit),
                max_state_rows,
                max_late_rows,
                if_exists: api::if_exists(if_exists.as_deref(), replace)?,
            };
            let (def, outcome, msg) = api::create_view(&open()?, &name, &sql, &o, sink.as_deref())?;
            use duckwake_core::catalog::Outcome;
            if matches!(outcome, Outcome::Created | Outcome::Replaced) {
                println!("{msg} {:?}", def.plan.sink_columns);
                println!("  its circuit and per-epoch SQL: duckwake explain {}", def.job_id);
            } else {
                println!("{msg}");
            }
        }
        Cmd::AddSourceColumn { source, column, r#type } => {
            println!("{}", api::add_source_column(&open()?, &source, &column, &r#type)?)
        }
        Cmd::AlterView { name, epoch_interval, max_rows, max_state_rows, max_late_rows } => {
            let a = catalog::AlterView { epoch_interval, max_rows, max_state_rows, max_late_rows };
            println!("{}", api::alter_view(&open()?, &name, &a)?)
        }
        Cmd::Start { name, worker } => println!("{}", api::start(&open()?, &name, api::assign_named(&worker))?),
        Cmd::Drain { worker } => println!("{}", api::drain(&open()?, &worker)?),
        Cmd::Pause { name } => println!("{}", api::pause(&open()?, &name)?),
        Cmd::Stop { name } => println!("{}", api::stop(&open()?, &name)?),
        Cmd::Reset { name, to_epoch } => println!("{}", api::reset(&open()?, &name, to_epoch)?),
        Cmd::Drop { name, keep_table } => println!("{}", api::drop(&open()?, &name, keep_table)?),
        Cmd::Jobs => {
            let conn = open()?;
            match catalog::jobs_sql(&conn)? {
                Some(sql) => print_query(&conn, &sql)?,
                None => println!("no views"),
            }
        }
        Cmd::Workers => {
            let conn = open()?;
            print_query(&conn, &api::workers_sql(&conn)?)?;
        }
        Cmd::Checkpoints { name, limit } => {
            let conn = open()?;
            print_query(&conn, &api::checkpoints_sql(&conn, &name, limit)?)?;
        }
        Cmd::Run { w, metrics } => {
            let shutdown = shutdown_on_signal(&rt);
            let mut cfg = w.config(&lake_cfg)?;
            if let Some(addr) = metrics.filter(|a| a != "none") {
                let m = Arc::new(duckwake_core::metrics::Metrics::default());
                m.serve_http(&addr, rt.handle())?;
                tracing::info!(addr = %addr, "metrics at /metrics");
                cfg.metrics = Some(m);
            }
            engine::run_worker(cfg, rt.handle().clone(), shutdown)?;
        }
        Cmd::Serve { w, s } => {
            return serve::run(&lake_cfg, rt, &w, &s);
        }
        Cmd::Produce { brokers, topic, partitions, rate, count, late_every, bad_every, disorder_ms, pad_bytes, eos } => {
            rt.block_on(produce(&brokers, &topic, partitions, rate, count, late_every, bad_every, disorder_ms, pad_bytes, eos))?;
        }
        Cmd::Explain { name } => {
            print!("{}", api::explain(&open()?, &name)?);
        }
        Cmd::Verify { name } => {
            let conn = open()?;
            let name = catalog::resolve_view(&conn, &name)?;
            let r = verify::verify(&conn, rt.handle(), &name)?;
            println!("{r:#?}");
            if !r.ok() {
                bail!("VERIFY FAILED for {name}");
            }
            let scope = match r.from_epoch {
                Some(e) => format!("the retained {} (output after epoch {e})", r.inputs()),
                None => format!("all {}", r.inputs()),
            };
            println!("VERIFY OK: {name} output matches batch recomputation over {scope} (epoch {}, watermark {})", r.epoch, r.watermark);
        }
        Cmd::Playground { port, worker, brokers, stage_threads, pipeline, shared_readers, lean_commit, metrics } => {
            let opts = playground::WorkerOpts { stage_threads, pipeline, shared_readers, lean_commit, metrics };
            return playground::run(lake_cfg, rt, worker, opts, brokers, port, logs);
        }
        Cmd::Sql { query, csv, server: Some(server), token, .. } => {
            // Each statement runs on the server through quack_query; this side needs no lake.
            let conn = duckdb::Connection::open_in_memory()?;
            conn.execute_batch("INSTALL quack; LOAD quack;")?;
            let token = token.map(|t| format!(", token = {}", lake::lit(&t))).unwrap_or_default();
            let stmts = split_statements(&query);
            for (i, stmt) in stmts.iter().enumerate() {
                let remote = format!("FROM quack_query({}, {}{token})", lake::lit(&server), lake::lit(stmt));
                if csv && i + 1 == stmts.len() {
                    print_csv(&conn, &remote)?;
                } else if csv {
                    conn.execute_batch(&remote)?;
                } else {
                    print_query(&conn, &remote)?;
                }
            }
        }
        Cmd::Sql { query, csv, server: None, bare: true, .. } => {
            let conn = duckdb::Connection::open_in_memory_with_flags(duckdb::Config::default().allow_unsigned_extensions()?)?;
            let stmts = split_statements(&query);
            for (i, stmt) in stmts.iter().enumerate() {
                if csv && i + 1 == stmts.len() {
                    print_csv(&conn, stmt)?;
                } else {
                    print_query(&conn, stmt)?;
                }
            }
        }
        Cmd::Sql { query, csv, server: None, .. } => {
            let conn = open()?;
            let sources = catalog::load_sources(&conn)?;
            for src in duckwake_core::adhoc::referenced_sources(&query, &sources) {
                let n = duckwake_core::adhoc::load_snapshot(&conn, rt.handle(), src, duckwake_core::adhoc::DEFAULT_TAIL)?;
                eprintln!("{}: newest {n} events of topic {}", src.name, src.options.topic);
            }
            let stmts = split_statements(&query);
            for (i, stmt) in stmts.iter().enumerate() {
                if csv && i + 1 == stmts.len() {
                    print_csv(&conn, stmt)?;
                } else if csv {
                    conn.execute_batch(stmt)?;
                } else {
                    print_query(&conn, stmt)?;
                }
            }
        }
    }
    Ok(())
}

fn split_statements(q: &str) -> Vec<String> {
    q.split(';').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect()
}

/// Headerless CSV, NULL as an empty field.
fn print_csv(conn: &duckdb::Connection, sql: &str) -> Result<()> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let opts = FormatOptions::default().with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"));
    let mut stmt = conn.prepare(sql)?;
    for batch in stmt.query_arrow([])? {
        let fmts = batch.columns().iter().map(|c| ArrayFormatter::try_new(c.as_ref(), &opts)).collect::<Result<Vec<_>, _>>()?;
        for i in 0..batch.num_rows() {
            let fields: Vec<String> = batch
                .columns()
                .iter()
                .zip(&fmts)
                .map(|(c, f)| {
                    if c.is_null(i) {
                        return String::new();
                    }
                    let v = f.value(i).to_string();
                    if v.contains([',', '"', '\n']) { format!("\"{}\"", v.replace('"', "\"\"")) } else { v }
                })
                .collect();
            println!("{}", fields.join(","));
        }
    }
    Ok(())
}

fn print_query(conn: &duckdb::Connection, sql: &str) -> Result<()> {
    let mut stmt = conn.prepare(sql)?;
    let arrow = stmt.query_arrow([])?;
    let schema = arrow.get_schema();
    // DDL and DML report a single "Count" column; nothing to show for those.
    if schema.fields().is_empty() || (schema.fields().len() == 1 && schema.field(0).name() == "Count") {
        arrow.for_each(drop);
        return Ok(());
    }
    let batches: Vec<arrow::record_batch::RecordBatch> = arrow.collect();
    if batches.is_empty() {
        println!("(no rows)");
    } else {
        println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn produce(
    brokers: &str,
    topic: &str,
    partitions: i32,
    rate: u64,
    count: u64,
    late_every: u64,
    bad_every: u64,
    disorder_ms: u64,
    pad_bytes: usize,
    eos: bool,
) -> Result<()> {
    use rand::Rng;
    let client = kafka::plain_client(brokers).await?;
    kafka::ensure_topic(&client, topic, partitions).await?;
    let parts = kafka::partitions(&client, topic).await?;
    let tick = Duration::from_millis(100);
    let per_tick = (rate / 10).max(1);
    let started = Instant::now();
    let mut sent = 0u64;
    let mut rng = rand::rng();
    let pad = "x".repeat(pad_bytes.saturating_sub(120));
    let fmt = |t: chrono::DateTime<chrono::Utc>| t.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    while count == 0 || sent < count {
        let n = if count == 0 { per_tick } else { per_tick.min(count - sent) };
        let mut by_part: Vec<Vec<Vec<u8>>> = vec![Vec::new(); parts.len()];
        for k in 0..n {
            let i = sent + k + 1;
            let mut ts = chrono::Utc::now() - chrono::Duration::milliseconds(rng.random_range(0..=disorder_ms as i64));
            if late_every > 0 && i.is_multiple_of(late_every) {
                ts -= chrono::Duration::minutes(5);
            }
            let payload = if bad_every > 0 && i.is_multiple_of(bad_every) {
                if (i / bad_every).is_multiple_of(2) {
                    format!("{{\"ts\": \"{}\", \"game_id\": \"g1\", \"amount\": ", fmt(ts))
                } else {
                    format!("{{\"ts\": \"{}\", \"game_id\": \"g1\", \"amount\": \"not-a-number\"}}", fmt(ts))
                }
            } else {
                format!(
                    "{{\"ts\": \"{}\", \"game_id\": \"g{}\", \"user_id\": \"u{}\", \"amount\": {}.{:02}, \"seq\": {i}{}}}",
                    fmt(ts),
                    rng.random_range(0..20),
                    rng.random_range(0..5000),
                    rng.random_range(0..100),
                    rng.random_range(0..100),
                    if pad.is_empty() { String::new() } else { format!(", \"pad\": \"{pad}\"") }
                )
            };
            by_part[(i as usize) % parts.len()].push(payload.into_bytes());
        }
        for (idx, batch) in by_part.into_iter().enumerate() {
            // Keep each request under the broker's default 1 MB batch limit.
            let mut piece: Vec<Vec<u8>> = Vec::new();
            let mut bytes = 0;
            for payload in batch {
                if bytes + payload.len() > 512 * 1024 && !piece.is_empty() {
                    kafka::produce(&client, topic, parts[idx], std::mem::take(&mut piece)).await?;
                    bytes = 0;
                }
                bytes += payload.len();
                piece.push(payload);
            }
            if !piece.is_empty() {
                kafka::produce(&client, topic, parts[idx], piece).await?;
            }
        }
        sent += n;
        let due = tick * (sent / per_tick) as u32;
        if let Some(wait) = due.checked_sub(started.elapsed()) {
            tokio::time::sleep(wait).await;
        }
        if sent.is_multiple_of(per_tick * 50) {
            eprintln!("produced {sent} events ({:.0}/s)", sent as f64 / started.elapsed().as_secs_f64());
        }
    }
    if eos {
        let ts = chrono::Utc::now() + chrono::Duration::days(1);
        for p in &parts {
            let payload = format!("{{\"ts\": \"{}\", \"game_id\": \"__eos\", \"user_id\": \"eos\", \"amount\": 0}}", fmt(ts));
            kafka::produce(&client, topic, *p, vec![payload.into_bytes()]).await?;
        }
    }
    eprintln!("produced {sent} events in {:.1}s{}", started.elapsed().as_secs_f64(), if eos { " + end-of-stream markers" } else { "" });
    Ok(())
}
