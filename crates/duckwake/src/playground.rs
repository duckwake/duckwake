//! `duckwake playground`: a local web UI over the engine. It runs one worker in-process
//! (restarted with backoff if it fails), test-data generators per source, and a JSON API for creating
//! sources, lake tables and continuous views, running SQL and following the activity log.
use anyhow::{anyhow, bail, Result};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use duckdb::Connection;
use duckwake_core::lake::{lit, LakeConfig, CTL, LAKE};
use duckwake_core::maintenance::MaintenanceConfig;
use duckwake_core::model::{JobStatus, SourceOptions};
use duckwake_core::{adhoc, catalog, engine, kafka, verify};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

const PAGE: &str = include_str!("playground.html");
const MAX_ROWS: usize = 1000;

/// Log lines captured for the Activity panel (the tracing fmt layer writes here as well as to stderr).
#[derive(Clone, Default)]
pub struct LogBuf(Arc<Mutex<LogInner>>);

#[derive(Default)]
struct LogInner {
    lines: VecDeque<(u64, String)>,
    next: u64,
}

pub struct LogWriter(LogBuf);

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.0 .0.lock().unwrap();
        for line in String::from_utf8_lossy(buf).lines().filter(|l| !l.trim().is_empty()) {
            let id = inner.next;
            inner.next += 1;
            inner.lines.push_back((id, line.to_string()));
            if inner.lines.len() > 2000 {
                inner.lines.pop_front();
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(self.clone())
    }
}

struct Generator {
    rate: u64,
    late_pct: f64,
    bad_pct: f64,
    produced: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

#[derive(Default)]
struct WorkerState {
    running: bool,
    restarts: u64,
    last_error: Option<String>,
}

/// How the in-process worker stages and commits (the `run` flags of the same names).
#[derive(Clone, Copy)]
pub struct WorkerOpts {
    pub stage_threads: usize,
    pub pipeline: bool,
    pub shared_readers: bool,
    pub lean_commit: bool,
}

struct App {
    lake: LakeConfig,
    conn: Mutex<Connection>,
    rt: Handle,
    worker_id: String,
    opts: WorkerOpts,
    brokers: String,
    worker: Mutex<WorkerState>,
    gens: Mutex<HashMap<String, Generator>>,
    logs: LogBuf,
    shutdown: Arc<AtomicBool>,
}

type Shared = Arc<App>;

struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, Json(json!({ "error": format!("{:#}", self.0) }))).into_response()
    }
}

type ApiResult = std::result::Result<Json<Value>, ApiError>;

/// Runs blocking DuckDB work off the async runtime with the API connection.
async fn with_conn<F>(app: &Shared, f: F) -> ApiResult
where
    F: FnOnce(&App, &Connection) -> Result<Value> + Send + 'static,
{
    let app = app.clone();
    tokio::task::spawn_blocking(move || {
        let conn = app.conn.lock().unwrap();
        f(&app, &conn)
    })
    .await
    .map_err(|e| ApiError(anyhow!(e)))?
    .map(Json)
    .map_err(ApiError)
}

pub fn run(
    lake: LakeConfig,
    rt: tokio::runtime::Runtime,
    worker_id: String,
    opts: WorkerOpts,
    brokers: String,
    port: u16,
    logs: LogBuf,
) -> Result<()> {
    let conn = lake.open()?;
    duckwake_core::lake::bootstrap(&conn)?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let app = Arc::new(App {
        lake,
        conn: Mutex::new(conn),
        rt: rt.handle().clone(),
        worker_id,
        opts,
        brokers,
        worker: Mutex::new(WorkerState::default()),
        gens: Mutex::new(HashMap::new()),
        logs,
        shutdown: shutdown.clone(),
    });
    let supervisor = spawn_worker(app.clone());

    let router = Router::new()
        .route("/", get(|| async { Html(PAGE) }))
        .route("/api/state", get(state))
        .route("/api/log", get(log))
        .route("/api/sql", post(sql))
        .route("/api/demo", post(demo))
        .route("/api/sources", post(create_source))
        .route("/api/sources/{name}", get(source_detail).delete(drop_source))
        .route("/api/sources/{name}/generator", post(set_generator))
        .route("/api/sources/{name}/events", post(send_events))
        .route("/api/tables/{schema}/{table}", get(table_detail))
        .route("/api/views", post(create_view))
        .route("/api/views/{name}", get(view_detail))
        .route("/api/views/{name}/{action}", post(view_action))
        .with_state(app.clone());

    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        eprintln!("\n  DuckWake playground: http://localhost:{port}\n");
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
        anyhow::Ok(())
    })?;
    shutdown.store(true, Ordering::Relaxed);
    for (_, g) in app.gens.lock().unwrap().drain() {
        g.task.abort();
    }
    let _ = supervisor.join();
    Ok(())
}

/// Keeps one worker running; if an epoch fails (bad view, Kafka down) it restarts from the last checkpoint.
fn spawn_worker(app: Shared) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !app.shutdown.load(Ordering::Relaxed) {
            app.worker.lock().unwrap().running = true;
            let cfg = engine::WorkerConfig {
                worker_id: app.worker_id.clone(),
                lake: app.lake.clone(),
                maintenance: MaintenanceConfig {
                    flush_every: Duration::from_secs(60),
                    merge_every: Duration::from_secs(300),
                    expire_every: Duration::from_secs(600),
                    expire_older_than: Duration::from_secs(3600),
                    cleanup_every: Duration::from_secs(600),
                    prune_every: Duration::from_secs(600),
                    ..Default::default()
                },
                max_epochs: None,
                refresh_every: Duration::from_secs(1),
                lease_timeout: Some(Duration::from_secs(30)),
                stop_grace: Duration::from_secs(60),
                rebalance: Some(duckwake_core::failover::RebalanceConfig::default()),
                stage_threads: app.opts.stage_threads,
                pipeline: app.opts.pipeline,
                shared_readers: app.opts.shared_readers,
                lean_commit: app.opts.lean_commit,
                metrics: None,
            };
            let res = engine::run_worker(cfg, app.rt.clone(), app.shutdown.clone());
            let mut w = app.worker.lock().unwrap();
            w.running = false;
            if let Err(e) = res {
                let msg = format!("{e:#}");
                tracing::error!(error = %msg, "worker failed; restarting from the last checkpoint in 3 s");
                w.last_error = Some(msg);
                w.restarts += 1;
                drop(w);
                std::thread::sleep(Duration::from_secs(3));
            }
        }
    })
}

/// Result of a query as `{columns, types, rows}`, every value rendered as text (NULL stays null).
fn query_json(conn: &Connection, sql: &str, limit: usize) -> Result<Value> {
    let mut stmt = conn.prepare(sql)?;
    let arrow = stmt.query_arrow([])?;
    let schema = arrow.get_schema();
    let opts = FormatOptions::default()
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
        .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f%:z"));
    let mut rows = Vec::new();
    let mut total = 0usize;
    for batch in arrow {
        total += batch.num_rows();
        if rows.len() >= limit {
            continue;
        }
        let fmts: Vec<ArrayFormatter> =
            batch.columns().iter().map(|c| ArrayFormatter::try_new(c.as_ref(), &opts)).collect::<std::result::Result<_, _>>()?;
        for i in 0..batch.num_rows().min(limit - rows.len()) {
            let row: Vec<Value> = batch
                .columns()
                .iter()
                .zip(&fmts)
                .map(|(c, f)| if c.is_null(i) { Value::Null } else { Value::String(f.value(i).to_string()) })
                .collect();
            rows.push(Value::Array(row));
        }
    }
    Ok(json!({
        "columns": schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>(),
        "types": schema.fields().iter().map(|f| f.data_type().to_string()).collect::<Vec<_>>(),
        "rows": rows,
        "total": total,
    }))
}

/// Splits a script on `;` outside quotes and comments.
fn split_sql(s: &str) -> Vec<String> {
    let (mut out, mut cur) = (Vec::new(), String::new());
    let mut chars = s.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    cur.push(c);
                }
                '-' if chars.peek() == Some(&'-') => {
                    for c in chars.by_ref() {
                        if c == '\n' {
                            cur.push('\n');
                            break;
                        }
                    }
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    let mut prev = ' ';
                    for c in chars.by_ref() {
                        if prev == '*' && c == '/' {
                            break;
                        }
                        prev = c;
                    }
                }
                ';' => {
                    if !cur.trim().is_empty() {
                        out.push(cur.trim().to_string());
                    }
                    cur.clear();
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

#[derive(Deserialize)]
struct SqlReq {
    sql: String,
}

async fn sql(State(app): State<Shared>, Json(req): Json<SqlReq>) -> ApiResult {
    with_conn(&app, move |app, conn| {
        let stmts = split_sql(&req.sql);
        let Some((last, rest)) = stmts.split_last() else { bail!("nothing to run") };
        let t = Instant::now();
        // Sources named in the script become temp tables holding the newest events of their topic.
        let sources = catalog::load_sources(conn)?;
        let mut snapshots = Vec::new();
        for src in adhoc::referenced_sources(&req.sql, &sources) {
            let n = adhoc::load_snapshot(conn, &app.rt, src, adhoc::DEFAULT_TAIL)?;
            snapshots.push(json!({ "source": src.name, "topic": src.options.topic, "events": n }));
        }
        for s in rest {
            conn.execute_batch(s)?;
        }
        let mut res = query_json(conn, last, MAX_ROWS)?;
        res["elapsed_ms"] = json!(t.elapsed().as_millis() as u64);
        res["statements"] = json!(stmts.len());
        res["snapshots"] = json!(snapshots);
        Ok(res)
    })
    .await
}

async fn state(State(app): State<Shared>) -> ApiResult {
    with_conn(&app, |app, conn| {
        let gens = app.gens.lock().unwrap();
        let sources: Vec<Value> = catalog::load_sources(conn)?
            .into_values()
            .map(|s| {
                let g = gens.get(&s.name);
                json!({
                    "name": s.name, "topic": s.options.topic, "brokers": s.options.brokers,
                    "event_time": s.options.event_time, "watermark_delay": s.options.watermark_delay,
                    "idle_timeout": s.options.idle_timeout, "columns": s.options.columns,
                    "decode": s.options.decode,
                    "decodes_in": if s.options.arrow_decoder().ok().flatten().is_some() { "arrow" } else { "sql" },
                    "generator": g.map(|g| json!({
                        "rate": g.rate, "late_pct": g.late_pct, "bad_pct": g.bad_pct,
                        "produced": g.produced.load(Ordering::Relaxed),
                    })),
                })
            })
            .collect();
        drop(gens);
        let mut views = Vec::new();
        if let Some(q) = catalog::jobs_sql(conn)? {
            let r = query_json(conn, &q, MAX_ROWS)?;
            let cols: Vec<String> = serde_json::from_value(r["columns"].clone())?;
            for row in r["rows"].as_array().unwrap() {
                let mut v: serde_json::Map<String, Value> = cols.iter().cloned().zip(row.as_array().unwrap().iter().cloned()).collect();
                let (def, _) = catalog::load_job(conn, v["job_id"].as_str().unwrap())?;
                v.insert("sql".into(), json!(def.sql));
                v.insert("sink".into(), json!(def.sink));
                v.insert("late_data".into(), json!(def.late_data));
                v.insert("source".into(), json!(def.plan.sources().join(", ")));
                v.insert("epoch_interval".into(), json!(def.epoch_interval));
                v.insert("kind".into(), json!(def.plan.kind_name()));
                v.insert("explain".into(), json!(duckwake_core::api::explain(conn, &def.job_id).unwrap_or_else(|e| format!("{e:#}"))));
                v.insert("sink_columns".into(), json!(def.plan.sink_columns));
                views.push(Value::Object(v));
            }
        }
        let tables = query_json(
            conn,
            &format!(
                "SELECT t.schema_name, t.table_name, list(c.column_name || ' ' || c.data_type ORDER BY c.column_index) AS cols
                 FROM duckdb_tables() t JOIN duckdb_columns() c USING (database_name, schema_name, table_name)
                 WHERE t.database_name = {} AND t.schema_name <> '__duckwake'
                 GROUP BY ALL ORDER BY 1, 2",
                lit(LAKE)
            ),
            MAX_ROWS,
        )?;
        let w = app.worker.lock().unwrap();
        Ok(json!({
            "worker": { "id": app.worker_id, "running": w.running, "restarts": w.restarts, "last_error": w.last_error },
            "brokers": app.brokers,
            "sources": sources,
            "views": views,
            "tables": tables["rows"],
        }))
    })
    .await
}

#[derive(Deserialize)]
struct LogQuery {
    since: Option<u64>,
}

async fn log(State(app): State<Shared>, Query(q): Query<LogQuery>) -> Json<Value> {
    let inner = app.logs.0.lock().unwrap();
    let since = q.since.unwrap_or(0);
    let lines: Vec<Value> = inner.lines.iter().filter(|(id, _)| *id >= since).map(|(id, l)| json!([id, l])).collect();
    Json(json!({ "lines": lines, "next": inner.next }))
}

#[derive(Deserialize)]
struct CreateSourceReq {
    name: String,
    topic: Option<String>,
    partitions: Option<i32>,
    columns: Vec<(String, String)>,
    event_time: String,
    watermark_delay: Option<String>,
    idle_timeout: Option<String>,
    start: Option<String>,
    decode: Option<String>,
}

async fn create_source(State(app): State<Shared>, Json(r): Json<CreateSourceReq>) -> ApiResult {
    let topic = r.topic.clone().filter(|t| !t.is_empty()).unwrap_or_else(|| r.name.clone());
    let brokers = app.brokers.clone();
    let t2 = topic.clone();
    let parts = r.partitions.unwrap_or(3);
    app.rt
        .spawn(async move {
            let c = kafka::plain_client(&brokers).await?;
            kafka::ensure_topic(&c, &t2, parts).await
        })
        .await
        .map_err(|e| ApiError(anyhow!(e)))?
        .map_err(ApiError)?;
    let options = json!({
        "brokers": app.brokers, "topic": topic, "event_time": r.event_time,
        "watermark_delay": r.watermark_delay.unwrap_or_else(|| "5 seconds".into()),
        "idle_timeout": r.idle_timeout.unwrap_or_else(|| "30 seconds".into()),
        "columns": r.columns, "start": r.start.unwrap_or_else(|| "earliest".into()),
        "decode": r.decode.unwrap_or_else(|| "auto".into()),
    });
    with_conn(&app, move |_, conn| {
        let def = catalog::create_source(conn, &r.name, "kafka", &options)?;
        Ok(json!({ "ok": true, "name": def.name }))
    })
    .await
}

async fn drop_source(State(app): State<Shared>, Path(name): Path<String>) -> ApiResult {
    with_conn(&app, move |app, conn| {
        catalog::drop_source(conn, &name)?;
        if let Some(g) = app.gens.lock().unwrap().remove(&name) {
            g.task.abort();
        }
        Ok(json!({ "ok": true }))
    })
    .await
}

async fn source_detail(State(app): State<Shared>, Path(name): Path<String>) -> ApiResult {
    let opts = with_conn(&app, {
        let name = name.clone();
        move |_, conn| {
            let s = catalog::load_sources(conn)?.remove(&name).ok_or_else(|| anyhow!("no such source {name:?}"))?;
            Ok(serde_json::to_value(s.options)?)
        }
    })
    .await?
    .0;
    let opts: SourceOptions = serde_json::from_value(opts).map_err(|e| ApiError(e.into()))?;
    let offsets = app
        .rt
        .spawn(async move {
            let c = kafka::client(&opts).await?;
            let mut out = Vec::new();
            for p in kafka::partitions(&c, &opts.topic).await? {
                let pc = c.partition_client(&opts.topic, p, rskafka::client::partition::UnknownTopicHandling::Retry).await?;
                let hw = pc.get_offset(rskafka::client::partition::OffsetAt::Latest).await?;
                out.push(json!({ "partition": p, "end_offset": hw }));
            }
            anyhow::Ok(out)
        })
        .await
        .map_err(|e| ApiError(anyhow!(e)))?
        .map_err(ApiError)?;
    Ok(Json(json!({ "partitions": offsets })))
}

#[derive(Deserialize)]
struct GeneratorReq {
    running: bool,
    rate: Option<u64>,
    late_pct: Option<f64>,
    bad_pct: Option<f64>,
}

async fn set_generator(State(app): State<Shared>, Path(name): Path<String>, Json(r): Json<GeneratorReq>) -> ApiResult {
    let src = with_conn(&app, {
        let name = name.clone();
        move |_, conn| {
            let s = catalog::load_sources(conn)?.remove(&name).ok_or_else(|| anyhow!("no such source {name:?}"))?;
            Ok(serde_json::to_value(s.options)?)
        }
    })
    .await?
    .0;
    let opts: SourceOptions = serde_json::from_value(src).map_err(|e| ApiError(e.into()))?;
    let mut gens = app.gens.lock().unwrap();
    let produced = gens.remove(&name).map(|g| {
        g.task.abort();
        g.produced
    });
    if r.running {
        let produced = produced.unwrap_or_default();
        let rate = r.rate.unwrap_or(100).clamp(1, 20_000);
        let (late_pct, bad_pct) = (r.late_pct.unwrap_or(0.0), r.bad_pct.unwrap_or(0.0));
        let task = app.rt.spawn(generate(opts.clone(), rate, late_pct, bad_pct, produced.clone()));
        gens.insert(name, Generator { rate, late_pct, bad_pct, produced, task });
    }
    Ok(Json(json!({ "ok": true })))
}

/// One synthetic event for the source's declared columns.
fn fake_event(opts: &SourceOptions, late: bool) -> Value {
    use rand::Rng;
    let mut rng = rand::rng();
    let mut obj = serde_json::Map::new();
    for (c, t) in &opts.columns {
        let t = t.to_ascii_uppercase();
        let v = if c == &opts.event_time || t.starts_with("TIMESTAMP") {
            let mut ts = chrono::Utc::now() - chrono::Duration::milliseconds(rng.random_range(0..1500));
            if late && c == &opts.event_time {
                ts -= chrono::Duration::minutes(5);
            }
            json!(ts.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        } else if t.starts_with("DECIMAL") || t == "DOUBLE" || t == "FLOAT" || t == "REAL" {
            json!((rng.random_range(0..10_000) as f64) / 100.0)
        } else if t.contains("INT") {
            json!(rng.random_range(0..1000))
        } else if t == "BOOLEAN" {
            json!(rng.random_bool(0.5))
        } else if t == "DATE" {
            json!(chrono::Utc::now().date_naive().to_string())
        } else {
            let card = if c.contains("user") { 500 } else { 10 };
            let prefix = c.strip_suffix("_id").unwrap_or(c).chars().next().unwrap_or('v');
            json!(format!("{prefix}{}", rng.random_range(0..card)))
        };
        obj.insert(c.clone(), v);
    }
    Value::Object(obj)
}

async fn generate(opts: SourceOptions, rate: u64, late_pct: f64, bad_pct: f64, produced: Arc<AtomicU64>) {
    let run = async {
        let c = kafka::client(&opts).await?;
        let parts = kafka::partitions(&c, &opts.topic).await?;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let mut carry = 0.0;
        let mut rr = 0usize;
        loop {
            tick.tick().await;
            carry += rate as f64 / 10.0;
            let n = carry as usize;
            carry -= n as f64;
            let mut by_part: Vec<Vec<Vec<u8>>> = vec![Vec::new(); parts.len()];
            for _ in 0..n {
                let roll: f64 = rand::random::<f64>() * 100.0;
                let payload = if roll < bad_pct {
                    "{\"broken\": ".to_string()
                } else {
                    fake_event(&opts, roll < bad_pct + late_pct).to_string()
                };
                by_part[rr % parts.len()].push(payload.into_bytes());
                rr += 1;
            }
            for (i, batch) in by_part.into_iter().enumerate() {
                if !batch.is_empty() {
                    kafka::produce(&c, &opts.topic, parts[i], batch).await?;
                }
            }
            produced.fetch_add(n as u64, Ordering::Relaxed);
        }
        #[allow(unreachable_code)]
        anyhow::Ok(())
    };
    if let Err(e) = run.await {
        tracing::error!(topic = %opts.topic, error = %format!("{e:#}"), "generator stopped");
    }
}

#[derive(Deserialize)]
struct EventsReq {
    /// A JSON array of events, or one raw payload per line (malformed lines are sent as-is).
    events: String,
}

async fn send_events(State(app): State<Shared>, Path(name): Path<String>, Json(r): Json<EventsReq>) -> ApiResult {
    let src = with_conn(&app, {
        let name = name.clone();
        move |_, conn| {
            let s = catalog::load_sources(conn)?.remove(&name).ok_or_else(|| anyhow!("no such source {name:?}"))?;
            Ok(serde_json::to_value(s.options)?)
        }
    })
    .await?
    .0;
    let opts: SourceOptions = serde_json::from_value(src).map_err(|e| ApiError(e.into()))?;
    let payloads: Vec<String> = match serde_json::from_str::<Value>(r.events.trim()) {
        Ok(Value::Array(a)) => a.iter().map(|v| v.to_string()).collect(),
        Ok(v @ Value::Object(_)) => vec![v.to_string()],
        _ => r.events.lines().map(str::trim).filter(|l| !l.is_empty()).map(String::from).collect(),
    };
    let n = payloads.len();
    app.rt
        .spawn(async move {
            let c = kafka::client(&opts).await?;
            let parts = kafka::partitions(&c, &opts.topic).await?;
            let mut by_part: Vec<Vec<Vec<u8>>> = vec![Vec::new(); parts.len()];
            for (i, p) in payloads.into_iter().enumerate() {
                by_part[i % parts.len()].push(p.into_bytes());
            }
            for (i, b) in by_part.into_iter().enumerate() {
                if !b.is_empty() {
                    kafka::produce(&c, &opts.topic, parts[i], b).await?;
                }
            }
            anyhow::Ok(())
        })
        .await
        .map_err(|e| ApiError(anyhow!(e)))?
        .map_err(ApiError)?;
    Ok(Json(json!({ "ok": true, "sent": n })))
}

async fn table_detail(State(app): State<Shared>, Path((schema, table)): Path<(String, String)>) -> ApiResult {
    with_conn(&app, move |_, conn| {
        let t = format!("{LAKE}.{}.{}", duckwake_core::lake::ident(&schema), duckwake_core::lake::ident(&table));
        let count: i64 = conn.query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))?;
        let files = query_json(
            conn,
            &format!("SELECT count(*) AS files, coalesce(sum(data_file_size_bytes), 0) AS bytes FROM ducklake_list_files({}, {}, schema => {})", lit(LAKE), lit(&table), lit(&schema)),
            1,
        )?;
        let preview = query_json(conn, &format!("SELECT * FROM {t} LIMIT 100"), 100)?;
        Ok(json!({ "rows": count, "files": files["rows"][0], "preview": preview }))
    })
    .await
}

#[derive(Deserialize)]
struct CreateViewReq {
    /// The table the view maintains; a bare name means the `analytics` schema here.
    name: String,
    sql: String,
    /// Deprecated: a table named separately from the view.
    sink: Option<String>,
    late_data: Option<String>,
    epoch_interval: Option<String>,
    start: Option<bool>,
}

async fn create_view(State(app): State<Shared>, Json(r): Json<CreateViewReq>) -> ApiResult {
    with_conn(&app, move |app, conn| {
        let name = r.name.trim();
        let name = if name.contains('.') { name.to_string() } else { format!("analytics.{name}") };
        let (job_id, table) = catalog::view_names(&name, r.sink.as_deref().map(str::trim).filter(|s| !s.is_empty()))?;
        let late = r.late_data.filter(|s| !s.trim().is_empty());
        let def = catalog::create_view(
            conn,
            &catalog::CreateView {
                job_id: &job_id,
                sql: &r.sql,
                sink: &table,
                late_data: late.as_deref(),
                checkpoint: "ducklake",
                epoch_interval: r.epoch_interval.as_deref().unwrap_or("5s"),
                max_rows: 100_000,
                inline_row_limit: 10_000,
            },
        )?;
        if r.start.unwrap_or(true) {
            catalog::set_status(conn, &job_id, JobStatus::Running, Some(catalog::Assign::Prefer(&app.worker_id)))?;
        }
        Ok(json!({ "ok": true, "job_id": job_id, "kind": def.plan.kind_name(), "explain": duckwake_core::api::explain(conn, &job_id).unwrap_or_else(|e| format!("{e:#}")) }))
    })
    .await
}

async fn view_detail(State(app): State<Shared>, Path(name): Path<String>) -> ApiResult {
    with_conn(&app, move |_, conn| {
        let (def, _) = catalog::load_job(conn, &name)?;
        let sink = duckwake_core::lake::quote_table(&def.sink)?;
        let order = if def.plan.sink_columns.iter().any(|(c, _)| c == "window_start") {
            "_duckwake_epoch DESC, window_start DESC"
        } else {
            "_duckwake_epoch DESC"
        };
        let ckpts = query_json(
            conn,
            &format!(
                "SELECT epoch, rows_in, rows_sql AS decoded_in_sql, rows_out, rows_late, rows_dead, watermark, lag, worker,
                        strftime(committed_at, '%H:%M:%S') AS committed
                 FROM {CTL}.checkpoints WHERE job_id = {} ORDER BY epoch DESC LIMIT 40",
                lit(&name)
            ),
            40,
        )?;
        let sink_rows: i64 = conn.query_row(&format!("SELECT count(*) FROM {sink}"), [], |r| r.get(0))?;
        let preview = query_json(conn, &format!("SELECT * FROM {sink} ORDER BY {order} LIMIT 50"), 50)?;
        let state = if def.plan.is_windowed() {
            query_json(
                conn,
                &format!(
                    "SELECT __ws AS window_start, __we AS window_end, count(*) AS keys FROM {} GROUP BY ALL ORDER BY 1",
                    catalog::state_table(&name)
                ),
                50,
            )?
        } else {
            Value::Null
        };
        let dead = query_json(
            conn,
            &format!(
                "SELECT epoch, source_partition AS p, source_offset AS \"offset\", error, payload
                 FROM {CTL}.dead_letter WHERE job_id = {} ORDER BY epoch DESC LIMIT 20",
                lit(&name)
            ),
            20,
        )?;
        let late = match &def.late_data {
            Some(l) => query_json(
                conn,
                &format!("SELECT * FROM {} ORDER BY _duckwake_epoch DESC LIMIT 20", duckwake_core::lake::quote_table(l)?),
                20,
            )?,
            None => Value::Null,
        };
        Ok(json!({ "checkpoints": ckpts, "sink_rows": sink_rows, "preview": preview, "open_windows": state, "dead": dead, "late": late }))
    })
    .await
}

#[derive(Deserialize, Default)]
struct ActionReq {
    to_epoch: Option<i64>,
}

async fn view_action(
    State(app): State<Shared>,
    Path((name, action)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> ApiResult {
    let req: ActionReq = if body.is_empty() { ActionReq::default() } else { serde_json::from_slice(&body).map_err(|e| ApiError(e.into()))? };
    if action == "verify" {
        return verify_view(app, name).await;
    }
    with_conn(&app, move |app, conn| {
        match action.as_str() {
            "start" => catalog::set_status(conn, &name, JobStatus::Running, Some(catalog::Assign::Prefer(&app.worker_id)))?,
            "pause" => catalog::set_status(conn, &name, JobStatus::Paused, None)?,
            "stop" => catalog::set_status(conn, &name, JobStatus::Stopped, None)?,
            "reset" => catalog::reset(conn, &name, req.to_epoch.ok_or_else(|| anyhow!("to_epoch is required"))?)?,
            "drop" => catalog::drop_view(conn, &name, false)?,
            other => bail!("unknown action {other:?}"),
        }
        Ok(json!({ "ok": true }))
    })
    .await
}

/// Verify reads the view's whole topic and can run for minutes on a big one. It runs on a DuckDB of its own (the
/// lake attached afresh), not the API connection, so the UI keeps working, and two verifies don't share
/// scratch tables.
async fn verify_view(app: Shared, name: String) -> ApiResult {
    tokio::task::spawn_blocking(move || -> Result<Value> {
        let conn = app.lake.open()?;
        let r = verify::verify(&conn, &app.rt, &name)?;
        Ok(json!({
            "ok": r.ok(), "from_epoch": r.from_epoch, "epoch": r.epoch, "watermark": r.watermark, "kafka_rows": r.kafka_rows,
            "expected_rows": r.expected_rows, "sink_rows": r.sink_rows,
            "missing_from_sink": r.missing_from_sink, "unexpected_in_sink": r.unexpected_in_sink,
            "dead_expected": r.dead_expected, "dead_mismatch": r.dead_mismatch,
            "late_rows": r.late_rows, "late_duplicates": r.late_duplicates,
            "closed_windows_left_in_state": r.closed_windows_left_in_state,
        }))
    })
    .await
    .map_err(|e| ApiError(anyhow!(e)))?
    .map(Json)
    .map_err(ApiError)
}

/// Creates a demo: a clicks source with a generator, a games dimension table and three views.
async fn demo(State(app): State<Shared>) -> ApiResult {
    let exists = with_conn(&app, |_, conn| Ok(json!(catalog::load_sources(conn)?.contains_key("clicks")))).await?.0;
    if exists == json!(false) {
        let _ = create_source(
            State(app.clone()),
            Json(CreateSourceReq {
                name: "clicks".into(),
                topic: Some("playground_clicks".into()),
                partitions: Some(3),
                columns: vec![
                    ("ts".into(), "TIMESTAMP".into()),
                    ("game_id".into(), "VARCHAR".into()),
                    ("user_id".into(), "VARCHAR".into()),
                    ("amount".into(), "DECIMAL(18,2)".into()),
                ],
                event_time: "ts".into(),
                watermark_delay: Some("3 seconds".into()),
                idle_timeout: Some("30 seconds".into()),
                start: Some("earliest".into()),
                decode: None,
            }),
        )
        .await?;
    }
    let _ = with_conn(&app, |_, conn| {
        conn.execute_batch(
            "CREATE SCHEMA IF NOT EXISTS dim;
             CREATE TABLE IF NOT EXISTS dim.games AS
               SELECT 'g' || i AS game_id,
                      ['Aurora', 'Basalt', 'Cobalt'][i % 3 + 1] || ' Studio' AS studio,
                      ['RPG', 'Puzzle', 'Shooter', 'Racing'][i % 4 + 1] AS genre
               FROM range(10) t(i);",
        )?;
        Ok(json!(null))
    })
    .await?;
    let views = [
        (
            "revenue_per_studio",
            "SELECT c.window_start, g.studio, count(*) AS clicks, sum(c.amount) AS revenue, max(c.amount) AS top_spend\n\
             FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) c\n\
             JOIN dim.games g ON g.game_id = c.game_id\nGROUP BY ALL",
            Some("analytics.revenue_per_studio_late"),
        ),
        (
            "game_activity_hop",
            "SELECT window_start, window_end, game_id, count(*) AS clicks, avg(amount) AS avg_amount\n\
             FROM HOP(clicks, ts, INTERVAL 30 SECOND, INTERVAL 10 SECOND)\nGROUP BY ALL",
            Some("analytics.game_activity_hop_late"),
        ),
        ("big_spends", "SELECT ts, game_id, user_id, amount\nFROM clicks\nWHERE amount >= 95", None),
        (
            "studio_totals",
            "SELECT g.studio, g.genre, count(*) AS clicks, sum(c.amount) AS revenue, avg(c.amount) AS avg_amount\n\
             FROM clicks c\nJOIN dim.games g ON g.game_id = c.game_id\nGROUP BY ALL\nHAVING count(*) > 100",
            None,
        ),
    ];
    for (name, sql, late) in views {
        let r = create_view(
            State(app.clone()),
            Json(CreateViewReq {
                name: name.into(),
                sql: sql.into(),
                sink: None,
                late_data: late.map(String::from),
                epoch_interval: Some("3s".into()),
                start: Some(true),
            }),
        )
        .await;
        if let Err(ApiError(e)) = r {
            if !format!("{e:#}").contains("already exists") {
                return Err(ApiError(e));
            }
        }
    }
    let _ = set_generator(
        State(app.clone()),
        Path("clicks".into()),
        Json(GeneratorReq { running: true, rate: Some(300), late_pct: Some(1.0), bad_pct: Some(0.5) }),
    )
    .await?;
    Ok(Json(json!({ "ok": true })))
}

#[cfg(test)]
mod tests {
    use super::split_sql;

    #[test]
    fn splits_outside_quotes_and_comments() {
        let s = split_sql("SELECT ';' AS a; -- c;\nSELECT 2 /* ; */; ;\n SELECT \"x;y\" FROM t");
        assert_eq!(s, ["SELECT ';' AS a", "SELECT 2", "SELECT \"x;y\" FROM t"]);
    }
}
