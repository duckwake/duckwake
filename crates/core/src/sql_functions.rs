//! The design's SQL surface as DuckDB table functions, all prefixed `duckwake_`. A server registers them in
//! its own instance and serves that instance over Quack, so a remote DuckDB can run
//! `CALL duckwake_create_view(...)` or `FROM duckwake_jobs()` against it.
//!
//! The functions run on a connection of their own to a DuckDB with the lake attached (`Api`), never on the
//! calling connection. Ones that change something do it once, when the statement executes, and return
//! `(view, result)` rows. Ones that read return their query's own columns, so they run when the statement
//! is bound.
//!
//! In the loadable extension the host database is out of reach, so `Api::local` opens a private DuckDB with
//! the lake attached on first use (`duckwake_connect` picks the lake) and can run a worker in the process
//! (`duckwake_worker_start` / `duckwake_worker_stop`), for development.
use crate::api::{self, ViewOptions};
use crate::lake::LakeConfig;
use crate::metrics::Metrics;
use anyhow::{anyhow, bail, Result};
use duckdb::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::compute::concat_batches;
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type_for_field, BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::Connection;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;

/// What the functions run on: a lake connection, the worker views start on by default, and the runtime
/// `duckwake_verify` (and a local worker) reads Kafka with.
#[derive(Clone)]
pub struct Api {
    inner: Arc<Mutex<Inner>>,
    rt: Handle,
    /// The worker's live numbers (`duckwake_metrics()`, `duckwake_worker_status()`).
    metrics: Arc<Metrics>,
    /// Whether this API opens its own lake and can run a worker in the process (the extension).
    local: bool,
}

struct Inner {
    conn: Option<Connection>,
    /// The lake a local API opens; None for one given a connection.
    lake: Option<LakeConfig>,
    default_worker: Option<String>,
    worker: Option<LocalWorker>,
    /// Address of the metrics endpoint a local worker started, if any (it serves until the process ends).
    metrics_addr: Option<String>,
}

/// A worker running in this process (`duckwake_worker_start`).
struct LocalWorker {
    id: String,
    shutdown: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<Result<()>>,
}

impl Api {
    /// Functions on `conn`, a connection with the lake attached; views start on `default_worker`, whose numbers
    /// are in `metrics`.
    pub fn new(conn: Connection, default_worker: Option<String>, rt: Handle, metrics: Arc<Metrics>) -> Self {
        let inner = Inner { conn: Some(conn), lake: None, default_worker, worker: None, metrics_addr: None };
        Api { inner: Arc::new(Mutex::new(inner)), rt, metrics, local: false }
    }

    /// Functions on a private DuckDB that attaches `lake` on first use, with a worker that can run in this process.
    pub fn local(lake: LakeConfig, rt: Handle) -> Self {
        let inner = Inner { conn: None, lake: Some(lake), default_worker: None, worker: None, metrics_addr: None };
        Api { inner: Arc::new(Mutex::new(inner)), rt, metrics: Arc::new(Metrics::default()), local: true }
    }

    fn with<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.conn.is_none() {
            let lake = inner.lake.clone().ok_or_else(|| anyhow!("no lake connection"))?;
            let conn = lake.open()?;
            crate::lake::bootstrap(&conn)?;
            inner.conn = Some(conn);
        }
        f(inner.conn.as_ref().unwrap())
    }

    fn default_worker(&self) -> Option<String> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.worker.as_ref().filter(|w| !w.thread.is_finished()).map(|w| w.id.clone()).or_else(|| inner.default_worker.clone())
    }
}

/// Registers every `duckwake_*` function on `conn` (and so on every connection to its database).
pub fn register(conn: &Connection, api: &Api) -> Result<()> {
    fn action<S: Spec>(conn: &Connection, api: &Api) -> Result<()> {
        Ok(conn.register_table_function_with_extra_info::<ActionFn<S>, Api>(S::NAME, api)?)
    }
    fn query<S: Spec>(conn: &Connection, api: &Api) -> Result<()> {
        Ok(conn.register_table_function_with_extra_info::<QueryFn<S>, Api>(S::NAME, api)?)
    }
    action::<CreateSource>(conn, api)?;
    action::<CreateView>(conn, api)?;
    action::<Start>(conn, api)?;
    action::<Pause>(conn, api)?;
    action::<Stop>(conn, api)?;
    action::<Reset>(conn, api)?;
    action::<DropView>(conn, api)?;
    action::<Verify>(conn, api)?;
    action::<Wait>(conn, api)?;
    query::<Explain>(conn, api)?;
    query::<Jobs>(conn, api)?;
    query::<Workers>(conn, api)?;
    query::<Checkpoints>(conn, api)?;
    query::<Sources>(conn, api)?;
    query::<MetricsFn>(conn, api)?;
    query::<WorkerStatus>(conn, api)?;
    if api.local {
        action::<Connect>(conn, api)?;
        action::<WorkerStart>(conn, api)?;
        action::<WorkerStop>(conn, api)?;
    }
    Ok(())
}

/// A call's arguments as text: positional ones in order, named ones by name (NULL ones left out).
struct Args {
    name: &'static str,
    pos: Vec<String>,
    named: HashMap<&'static str, String>,
}

impl Args {
    fn from_bind(name: &'static str, bind: &BindInfo, named: &[(&'static str, LogicalTypeId)]) -> Result<Self> {
        let mut pos = Vec::new();
        for i in 0..bind.get_parameter_count() {
            let v = bind.get_parameter(i);
            if v.is_null() {
                bail!("{name}: argument {} must not be NULL", i + 1);
            }
            pos.push(v.to_string());
        }
        let named = named
            .iter()
            .filter_map(|(n, _)| bind.get_named_parameter(n).filter(|v| !v.is_null()).map(|v| (*n, v.to_string())))
            .collect();
        Ok(Args { name, pos, named })
    }

    fn arg(&self, i: usize) -> &str {
        &self.pos[i]
    }

    fn opt(&self, n: &str) -> Option<&str> {
        self.named.get(n).map(String::as_str)
    }

    fn opt_num<T: std::str::FromStr>(&self, n: &str) -> Result<Option<T>> {
        self.opt(n).map(|v| v.parse().map_err(|_| anyhow!("{}: {n} must be a number, not {v:?}", self.name))).transpose()
    }
}

/// One `duckwake_*` function: its signature and what it does.
trait Spec: Send + Sync + 'static {
    const NAME: &'static str;
    /// Positional parameter types.
    fn params() -> Vec<LogicalTypeId>;
    fn named() -> Vec<(&'static str, LogicalTypeId)> {
        Vec::new()
    }
    /// An action returns `(view, result)` rows; a query returns the SQL whose rows the function returns.
    fn run(api: &Api, args: &Args) -> Result<Out>;
}

enum Out {
    /// An action's `(view, result)` rows.
    Rows(Vec<(String, String)>),
    /// A query to run on the lake connection.
    Query(String),
    /// Rows computed here.
    Batch(RecordBatch),
}

fn one(view: &str, result: String) -> Result<Out> {
    Ok(Out::Rows(vec![(view.to_string(), result)]))
}

fn err(e: anyhow::Error) -> Box<dyn std::error::Error> {
    format!("{e:#}").into()
}

/// The rows of one call and how many have been returned so far.
struct Rows {
    batch: RecordBatch,
    next: AtomicUsize,
}

impl Rows {
    fn new(batch: RecordBatch) -> Self {
        Rows { batch, next: AtomicUsize::new(0) }
    }

    /// Writes the next vector's worth of rows into `out` (none once all are out).
    fn emit(&self, out: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let size = unsafe { duckdb::ffi::duckdb_vector_size() } as usize;
        let total = self.batch.num_rows();
        let from = self.next.fetch_add(size, Ordering::Relaxed).min(total);
        let len = size.min(total - from);
        if len == 0 {
            out.set_len(0);
            return Ok(());
        }
        record_batch_to_duckdb_data_chunk(&self.batch.slice(from, len), out)?;
        Ok(())
    }
}

/// Actions: fixed `(view, result)` columns; the action runs once, when the statement executes.
struct ActionFn<S>(PhantomData<S>);

impl<S: Spec> VTab for ActionFn<S> {
    type BindData = Args;
    type InitData = Rows;

    fn bind(bind: &BindInfo) -> Result<Args, Box<dyn std::error::Error>> {
        bind.add_result_column("view", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("result", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Args::from_bind(S::NAME, bind, &S::named()).map_err(err)
    }

    fn init(init: &InitInfo) -> Result<Rows, Box<dyn std::error::Error>> {
        let api = unsafe { &*init.get_extra_info::<Api>() };
        let args = unsafe { &*init.get_bind_data::<Args>() };
        let rows = match S::run(api, args).map_err(err)? {
            Out::Rows(rows) => rows,
            _ => return Err(format!("{}: an action returned a query", S::NAME).into()),
        };
        let (views, results): (Vec<String>, Vec<String>) = rows.into_iter().unzip();
        let schema = Schema::new(vec![Field::new("view", DataType::Utf8, false), Field::new("result", DataType::Utf8, false)]);
        let cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(views)), Arc::new(StringArray::from(results))];
        Ok(Rows::new(RecordBatch::try_new(Arc::new(schema), cols)?))
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        func.get_init_data().emit(output)
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(S::params().into_iter().map(LogicalTypeHandle::from).collect())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(S::named().into_iter().map(|(n, t)| (n.to_string(), LogicalTypeHandle::from(t))).collect())
    }
}

/// Queries: the columns of the query `S::run` returns, read when the statement is bound.
struct QueryFn<S>(PhantomData<S>);

impl<S: Spec> VTab for QueryFn<S> {
    type BindData = RecordBatch;
    type InitData = Rows;

    fn bind(bind: &BindInfo) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let api = unsafe { &*bind.get_extra_info::<Api>() };
        let args = Args::from_bind(S::NAME, bind, &S::named()).map_err(err)?;
        let batch = (|| -> Result<RecordBatch> {
            let sql = match S::run(api, &args)? {
                Out::Query(sql) => sql,
                Out::Batch(b) => return Ok(b),
                Out::Rows(_) => bail!("{}: a query returned action rows", S::NAME),
            };
            api.with(|conn| {
                let mut stmt = conn.prepare(&sql)?;
                let arrow = stmt.query_arrow([])?;
                let schema = arrow.get_schema();
                let batches: Vec<RecordBatch> = arrow.collect();
                Ok(concat_batches(&schema, &batches)?)
            })
        })()
        .map_err(err)?;
        for f in batch.schema().fields() {
            bind.add_result_column(f.name(), to_duckdb_logical_type_for_field(f)?);
        }
        Ok(batch)
    }

    fn init(init: &InitInfo) -> Result<Rows, Box<dyn std::error::Error>> {
        let batch = unsafe { &*init.get_bind_data::<RecordBatch>() };
        Ok(Rows::new(batch.clone()))
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        func.get_init_data().emit(output)
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(S::params().into_iter().map(LogicalTypeHandle::from).collect())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(S::named().into_iter().map(|(n, t)| (n.to_string(), LogicalTypeHandle::from(t))).collect())
    }
}

use LogicalTypeId::{Bigint, Boolean, Varchar};

/// `duckwake_create_source(name, kind, options)`: options as JSON (brokers, topic, event_time, columns, ...).
struct CreateSource;
impl Spec for CreateSource {
    const NAME: &'static str = "duckwake_create_source";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar, Varchar, Varchar]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        one(a.arg(0), api.with(|c| api::create_source(c, a.arg(0), a.arg(1), a.arg(2)))?)
    }
}

/// `duckwake_create_view(name, sql, late_data :=, epoch_interval :=, max_rows :=, checkpoint :=,
/// inline_row_limit :=)`: the table `name` and the job that maintains it.
struct CreateView;
impl Spec for CreateView {
    const NAME: &'static str = "duckwake_create_view";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar, Varchar]
    }
    fn named() -> Vec<(&'static str, LogicalTypeId)> {
        vec![
            ("late_data", Varchar),
            ("epoch_interval", Varchar),
            ("max_rows", Bigint),
            ("checkpoint", Varchar),
            ("inline_row_limit", Bigint),
        ]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let o = ViewOptions {
            late_data: a.opt("late_data").map(String::from),
            checkpoint: a.opt("checkpoint").map(String::from),
            epoch_interval: a.opt("epoch_interval").map(String::from),
            max_rows: a.opt_num("max_rows")?,
            inline_row_limit: a.opt_num("inline_row_limit")?,
        };
        let (def, msg) = api.with(|c| api::create_view(c, a.arg(0), a.arg(1), &o, None))?;
        one(&def.job_id, msg)
    }
}

/// `duckwake_start(view, worker :=)`: without a worker, the server's own; `worker := 'any'` leaves the view to
/// whichever live worker claims it.
struct Start;
impl Spec for Start {
    const NAME: &'static str = "duckwake_start";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn named() -> Vec<(&'static str, LogicalTypeId)> {
        vec![("worker", Varchar)]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let worker = match a.opt("worker").map(String::from).or_else(|| api.default_worker()) {
            Some(w) => w,
            None => bail!("duckwake_start: name the worker, e.g. duckwake_start('{}', worker := 'w1')", a.arg(0)),
        };
        one(a.arg(0), api.with(|c| api::start(c, a.arg(0), &worker))?)
    }
}

struct Pause;
impl Spec for Pause {
    const NAME: &'static str = "duckwake_pause";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        one(a.arg(0), api.with(|c| api::pause(c, a.arg(0)))?)
    }
}

struct Stop;
impl Spec for Stop {
    const NAME: &'static str = "duckwake_stop";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        one(a.arg(0), api.with(|c| api::stop(c, a.arg(0)))?)
    }
}

/// `duckwake_reset(view, to_epoch)`.
struct Reset;
impl Spec for Reset {
    const NAME: &'static str = "duckwake_reset";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar, Bigint]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let to: i64 = a.arg(1).parse().map_err(|_| anyhow!("duckwake_reset: to_epoch must be a number"))?;
        one(a.arg(0), api.with(|c| api::reset(c, a.arg(0), to))?)
    }
}

/// `duckwake_drop(view, keep_table := false)`.
struct DropView;
impl Spec for DropView {
    const NAME: &'static str = "duckwake_drop";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn named() -> Vec<(&'static str, LogicalTypeId)> {
        vec![("keep_table", Boolean)]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let keep = a.opt("keep_table").is_some_and(|v| v.eq_ignore_ascii_case("true"));
        one(a.arg(0), api.with(|c| api::drop(c, a.arg(0), keep))?)
    }
}

/// `duckwake_verify(view)`: the differential exactly-once check (reads the view's whole topic).
struct Verify;
impl Spec for Verify {
    const NAME: &'static str = "duckwake_verify";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let r = api.with(|c| {
            let job = crate::catalog::resolve_view(c, a.arg(0))?;
            crate::verify::verify(c, &api.rt, &job)
        })?;
        let msg = if r.ok() {
            format!("VERIFY OK: output matches batch recomputation over {} (epoch {})", r.inputs(), r.epoch)
        } else {
            format!("VERIFY FAILED: {r:?}")
        };
        one(&r.job, msg)
    }
}

/// `duckwake_explain(view)`: the view's circuit, how it was made incremental, and one epoch's SQL, a row per line.
struct Explain;
impl Spec for Explain {
    const NAME: &'static str = "duckwake_explain";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let text = api.with(|c| api::explain(c, a.arg(0)))?;
        let lines: Vec<&str> = text.lines().collect();
        let schema = Schema::new(vec![Field::new("explain", DataType::Utf8, false)]);
        Ok(Out::Batch(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(lines))])?))
    }
}

/// `duckwake_wait(view, timeout_s := 60)`: blocks until the view has committed everything its topic held when
/// called (for notebooks and tests: SQL has no sleep).
struct Wait;
impl Spec for Wait {
    const NAME: &'static str = "duckwake_wait";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn named() -> Vec<(&'static str, LogicalTypeId)> {
        vec![("timeout_s", Bigint)]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let timeout = std::time::Duration::from_secs(a.opt_num("timeout_s")?.unwrap_or(60));
        // Every input's end now: a Kafka source's offsets, a lake input's current snapshot.
        let (job, def, ends) = api.with(|c| {
            let job = crate::catalog::resolve_view(c, a.arg(0))?;
            let (def, _) = crate::catalog::load_job(c, &job)?;
            let mut all = crate::catalog::load_sources(c)?;
            let snapshot = crate::lake::current_snapshot(c)?;
            let mut ends = Vec::new();
            for n in def.plan.sources() {
                if def.lake_inputs.iter().any(|l| l.name == n) {
                    ends.push((n.clone(), std::collections::BTreeMap::from([(0, snapshot)])));
                } else {
                    let s = all.remove(&n).ok_or_else(|| anyhow!("source {n} of {job} is gone"))?;
                    ends.push((n.clone(), api.rt.block_on(crate::kafka::latest_offsets(&s.options))?));
                }
            }
            Ok((job, def, ends))
        })?;
        let t = std::time::Instant::now();
        loop {
            let mut ckpt = None;
            let mut done = true;
            for (name, end) in &ends {
                let ck = api.with(|c| crate::catalog::latest_checkpoint(c, &job, name))?;
                done &= match def.lake_inputs.iter().find(|l| &l.name == name) {
                    // A view skips epochs with no changes, so its position can stay behind the lake's newest
                    // snapshot: caught up once no change to the table lies between the two.
                    Some(l) => {
                        let p = ck.as_ref().and_then(|ck| ck.offsets.get(&0).copied()).or(def.start_snapshot).unwrap_or(0);
                        let s = end[&0];
                        p >= s || api.with(|c| {
                            let Some((schema, table)) = crate::lake::table_by_id(c, l.table_id)? else { return Ok(false) };
                            let n: i64 = c.query_row(
                                &format!("SELECT count(*) FROM ducklake_table_changes({}, ?, ?, ?, ?)", crate::lake::lit(crate::lake::LAKE)),
                                duckdb::params![schema, table, p + 1, s],
                                |r| r.get(0),
                            )?;
                            Ok(n == 0)
                        })?
                    }
                    None => ck.as_ref().is_some_and(|ck| end.iter().all(|(p, o)| ck.offsets.get(p).is_some_and(|c| c >= o))),
                };
                ckpt = ck;
            }
            if done {
                return one(&job, format!("{job}: caught up at epoch {}", ckpt.map_or(0, |c| c.epoch)));
            }
            if t.elapsed() > timeout {
                bail!("duckwake_wait: {job} did not catch up within {}s (at epoch {})", timeout.as_secs(), ckpt.map_or(0, |c| c.epoch));
            }
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
    }
}

/// `duckwake_jobs()`: status, worker, epoch, watermark, freshness, lag and totals per view.
struct Jobs;
impl Spec for Jobs {
    const NAME: &'static str = "duckwake_jobs";
    fn params() -> Vec<LogicalTypeId> {
        vec![]
    }
    fn run(api: &Api, _: &Args) -> Result<Out> {
        Ok(Out::Query(api.with(crate::catalog::jobs_query)?))
    }
}

/// `duckwake_workers()`: every worker that ran on this lake, its state, views and last heartbeat.
struct Workers;
impl Spec for Workers {
    const NAME: &'static str = "duckwake_workers";
    fn params() -> Vec<LogicalTypeId> {
        vec![]
    }
    fn run(api: &Api, _: &Args) -> Result<Out> {
        Ok(Out::Query(api.with(api::workers_sql)?))
    }
}

/// `duckwake_checkpoints(view, n := 10)`: the newest n checkpoints.
struct Checkpoints;
impl Spec for Checkpoints {
    const NAME: &'static str = "duckwake_checkpoints";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn named() -> Vec<(&'static str, LogicalTypeId)> {
        vec![("n", Bigint)]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let limit = a.opt_num("n")?.unwrap_or(10);
        Ok(Out::Query(api.with(|c| api::checkpoints_sql(c, a.arg(0), limit))?))
    }
}

/// `duckwake_sources()`.
struct Sources;
impl Spec for Sources {
    const NAME: &'static str = "duckwake_sources";
    fn params() -> Vec<LogicalTypeId> {
        vec![]
    }
    fn run(_: &Api, _: &Args) -> Result<Out> {
        Ok(Out::Query(api::sources_sql()))
    }
}

/// `duckwake_connect(catalog, data_path :=)`: the lake the extension's functions and worker use (the
/// `DUCKWAKE_CATALOG` / `DUCKWAKE_DATA_PATH` environment, or the docker-compose lake, until called).
struct Connect;
impl Spec for Connect {
    const NAME: &'static str = "duckwake_connect";
    fn params() -> Vec<LogicalTypeId> {
        vec![Varchar]
    }
    fn named() -> Vec<(&'static str, LogicalTypeId)> {
        vec![("data_path", Varchar)]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        {
            let mut inner = api.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.worker.as_ref().is_some_and(|w| !w.thread.is_finished()) {
                bail!("duckwake_connect: stop the worker first (CALL duckwake_worker_stop())");
            }
            let mut lake = inner.lake.clone().ok_or_else(|| anyhow!("duckwake_connect: this server's lake is fixed"))?;
            lake.catalog = a.arg(0).trim_start_matches("ducklake:").to_string();
            if let Some(p) = a.opt("data_path") {
                lake.data_path = p.to_string();
            }
            inner.lake = Some(lake);
            inner.conn = None;
        }
        // Open it now, so a wrong catalog fails here.
        api.with(|_| Ok(()))?;
        let lake = api.inner.lock().unwrap_or_else(|e| e.into_inner()).lake.clone().unwrap();
        one(
            "",
            format!(
                "connected; to read the views' tables in this session too: ATTACH 'ducklake:{}' AS lake (DATA_PATH '{}')",
                lake.catalog, lake.data_path
            ),
        )
    }
}

/// `duckwake_worker_start(worker := 'local', stage_threads :=, pipeline :=, shared_readers :=, lean_commit :=,
/// metrics := 'HOST:PORT')`: runs a worker in this process, restarting it from its last checkpoint 5 s after
/// it fails; `duckwake_start(view)` then assigns views to it. With `metrics`, also serves `GET /metrics`.
struct WorkerStart;
impl Spec for WorkerStart {
    const NAME: &'static str = "duckwake_worker_start";
    fn params() -> Vec<LogicalTypeId> {
        vec![]
    }
    fn named() -> Vec<(&'static str, LogicalTypeId)> {
        vec![
            ("worker", Varchar),
            ("stage_threads", Bigint),
            ("pipeline", Boolean),
            ("shared_readers", Boolean),
            ("lean_commit", Boolean),
            ("metrics", Varchar),
        ]
    }
    fn run(api: &Api, a: &Args) -> Result<Out> {
        let flag = |n: &str| a.opt(n).is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let mut inner = api.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = inner.worker.as_ref().filter(|w| !w.thread.is_finished()) {
            bail!("duckwake_worker_start: worker {} is already running", w.id);
        }
        let id = a.opt("worker").unwrap_or("local").to_string();
        let lake = inner.lake.clone().ok_or_else(|| anyhow!("duckwake_worker_start: not available here"))?;
        let mut note = String::new();
        if let Some(addr) = a.opt("metrics") {
            match &inner.metrics_addr {
                Some(old) if old == addr => {}
                Some(old) => bail!("duckwake_worker_start: metrics are already served on {old}"),
                None => {
                    api.metrics.serve_http(addr, &api.rt)?;
                    inner.metrics_addr = Some(addr.to_string());
                }
            }
            note = format!("; metrics at http://{addr}/metrics");
        }
        let (stage_threads, pipeline, shared_readers, lean_commit) =
            (a.opt_num("stage_threads")?.unwrap_or(1), flag("pipeline"), flag("shared_readers"), flag("lean_commit"));
        let worker_id = id.clone();
        let make_cfg = move || {
            Ok(crate::engine::WorkerConfig {
                worker_id: worker_id.clone(),
                lake: lake.clone(),
                maintenance: crate::maintenance::MaintenanceConfig::default(),
                max_epochs: None,
                refresh_every: std::time::Duration::from_secs(1),
                lease_timeout: Some(std::time::Duration::from_secs(30)),
                stage_threads,
                pipeline,
                shared_readers,
                lean_commit,
                metrics: None,
            })
        };
        let shutdown = Arc::new(AtomicBool::new(false));
        let (rt, stop, metrics) = (api.rt.clone(), shutdown.clone(), api.metrics.clone());
        let thread = std::thread::Builder::new()
            .name(format!("duckwake-worker-{id}"))
            .spawn(move || crate::engine::supervise(make_cfg, rt, stop, metrics))?;
        inner.default_worker = Some(id.clone());
        inner.worker = Some(LocalWorker { id: id.clone(), shutdown, thread });
        one("", format!("worker {id} running in this process; duckwake_start(view) assigns views to it{note}"))
    }
}

/// `duckwake_worker_stop()`: lets the local worker finish its epoch and stops it.
struct WorkerStop;
impl Spec for WorkerStop {
    const NAME: &'static str = "duckwake_worker_stop";
    fn params() -> Vec<LogicalTypeId> {
        vec![]
    }
    fn run(api: &Api, _: &Args) -> Result<Out> {
        let w = api.inner.lock().unwrap_or_else(|e| e.into_inner()).worker.take();
        let Some(w) = w else { return one("", "no worker is running in this process".into()) };
        w.shutdown.store(true, Ordering::Relaxed);
        let res = w.thread.join().map_err(|_| anyhow!("worker {} panicked", w.id))?;
        match res {
            Ok(()) => one("", format!("worker {} stopped", w.id)),
            Err(e) => one("", format!("worker {} had stopped with an error: {e:#}", w.id)),
        }
    }
}

/// `duckwake_metrics()`: the worker's numbers as rows (the same series `GET /metrics` serves): per-view series
/// carry the view, worker-wide ones a NULL view.
struct MetricsFn;
impl Spec for MetricsFn {
    const NAME: &'static str = "duckwake_metrics";
    fn params() -> Vec<LogicalTypeId> {
        vec![]
    }
    fn run(api: &Api, _: &Args) -> Result<Out> {
        let samples = api.metrics.samples();
        let worker = api.metrics.status().0;
        let schema = Schema::new(vec![
            Field::new("metric", DataType::Utf8, false),
            Field::new("worker", DataType::Utf8, false),
            Field::new("view", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(samples.iter().map(|s| s.name).collect::<Vec<_>>())),
            Arc::new(StringArray::from(vec![worker.as_str(); samples.len()])),
            Arc::new(StringArray::from(samples.iter().map(|s| s.view.clone()).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(samples.iter().map(|s| s.value).collect::<Vec<_>>())),
        ];
        Ok(Out::Batch(RecordBatch::try_new(Arc::new(schema), cols)?))
    }
}

/// `duckwake_worker_status()`: this process's worker, whether it runs, how often it restarted and why it last failed.
struct WorkerStatus;
impl Spec for WorkerStatus {
    const NAME: &'static str = "duckwake_worker_status";
    fn params() -> Vec<LogicalTypeId> {
        vec![]
    }
    fn run(api: &Api, _: &Args) -> Result<Out> {
        let (worker, up, restarts, last) = api.metrics.status();
        let schema = Schema::new(vec![
            Field::new("worker", DataType::Utf8, true),
            Field::new("running", DataType::Boolean, false),
            Field::new("restarts", DataType::Int64, false),
            Field::new("last_error_at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("last_error", DataType::Utf8, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![(!worker.is_empty()).then_some(worker)])),
            Arc::new(BooleanArray::from(vec![up])),
            Arc::new(Int64Array::from(vec![restarts as i64])),
            Arc::new(TimestampMicrosecondArray::from(vec![last.as_ref().map(|(t, _)| t.timestamp_micros())])),
            Arc::new(StringArray::from(vec![last.map(|(_, e)| e)])),
        ];
        Ok(Out::Batch(RecordBatch::try_new(Arc::new(schema), cols)?))
    }
}
