//! Control data in `lake.__duckwake`: sources, jobs, leases, checkpoints. Everything shares the lake's
//! transactions, so registering a view and creating its tables is one atomic step.
//!
//! Fencing uses one lease table per job (`__duckwake.lease_<job>`), not a column of the shared `jobs` table:
//! in DuckLake, an UPDATE of one row in a table conflicts with a concurrent UPDATE of any other row of the
//! same table, so per-epoch fencing on `jobs` would make every worker conflict with every other one.
use crate::lake::{ident, lit, quote_table, CTL, LAKE};
use crate::model::{parse_duration, Input, JobDef, JobStatus, LakeInput, SourceDef, SourceOptions, COMMIT_TS};
use crate::plan;
use anyhow::{anyhow, bail, Context, Result};
use duckdb::Connection;
use std::collections::{BTreeMap, HashMap};

pub fn lease_table(job: &str) -> String {
    format!("{CTL}.{}", ident(&format!("lease_{job}")))
}

pub use duckwake_circuit::sql::state_table;

/// A view with its plan compiled again by this version's compiler, and the sources. Its state and output tables
/// were created from the stored plan, so their columns must be what this compiler would create.
pub fn load_compiled(conn: &Connection, job: &str) -> Result<(JobDef, HashMap<String, SourceDef>)> {
    let (mut def, _) = load_job(conn, job)?;
    let sources = load_sources(conn)?;
    let plan = compile(conn, job, &def.sql, &sources, &def.lake_inputs)?;
    if plan.state_columns != def.plan.state_columns || plan.sink_columns != def.plan.sink_columns {
        bail!(
            "view {job} was created by a DuckWake version whose state or table layout differs from this one's; \
             drop and create it again"
        );
    }
    def.plan = plan;
    Ok((def, sources))
}

/// Compiles a view whose FROM names Kafka sources, and lake tables and views resolved to `lakes`.
pub fn compile(conn: &Connection, job: &str, sql: &str, sources: &HashMap<String, SourceDef>, lakes: &[LakeInput]) -> Result<plan::Plan> {
    let mut inputs: HashMap<String, Input> = sources.iter().map(|(n, s)| (n.clone(), Input::Kafka(s))).collect();
    inputs.extend(lakes.iter().map(|l| (l.name.clone(), Input::Lake(l))));
    plan::compile(conn, job, sql, &inputs)
}

/// Compiles a new view and settles how it reads each lake input it streams. A window is final once the
/// watermark passes it, so a later change to one of its rows could not be applied: a windowed view reads lake
/// tables insert-only (an update or delete then stops it), and refuses a view's table whose rows change.
/// Every other view takes updates and deletes as retractions.
fn compile_new(conn: &Connection, job: &str, sql: &str, sources: &HashMap<String, SourceDef>, lakes: &mut [LakeInput]) -> Result<plan::Plan> {
    // The view's shape first, every input insert-only: over retracting ones some windowed aggregates are refused.
    let inserts: Vec<LakeInput> = lakes.iter().map(|l| LakeInput { retracts: false, ..l.clone() }).collect();
    let plan = compile(conn, job, sql, sources, &inserts)?;
    let streams = plan.sources();
    let retracting = |l: &LakeInput| l.retracts && streams.contains(&l.name);
    if plan.is_windowed() {
        if let Some(l) = lakes.iter().find(|l| retracting(l) && l.view.is_some()) {
            bail!(
                "{} is the table of view {}, whose rows are replaced or removed as they change; windows over it are \
                 not supported yet (a window is final once the watermark passes it). Group by a time_bucket of a \
                 column instead: the view then keeps every bucket up to date",
                l.name,
                l.view.as_deref().unwrap_or_default()
            );
        }
        lakes.iter_mut().for_each(|l| l.retracts = false);
        return Ok(plan);
    }
    if !lakes.iter().any(retracting) {
        return Ok(plan);
    }
    compile(conn, job, sql, sources, lakes)
}

/// The columns of a view input named `name`: a Kafka source's or a lake input's.
pub fn input_columns<'a>(name: &str, sources: &'a HashMap<String, SourceDef>, lakes: &'a [LakeInput]) -> Result<&'a [(String, String)]> {
    if let Some(s) = sources.get(name) {
        return Ok(&s.options.columns);
    }
    lakes.iter().find(|l| l.name == name).map(|l| l.columns.as_slice()).with_context(|| format!("input {name} of the view is gone"))
}

/// Resolves the names a view's FROM uses that are not Kafka sources: first to a view (by job id, or by the
/// table it writes), then to a lake table. Names that are neither are left to DuckDB, which binds them or
/// fails. Every such table becomes a lake input of the view; the compiler decides which of them it reads as
/// a change feed and which it joins as a lookup.
fn resolve_lake_inputs(conn: &Connection, job: &str, sql: &str, sources: &HashMap<String, SourceDef>) -> Result<Vec<LakeInput>> {
    let mut out = Vec::new();
    for name in plan::table_names(conn, sql)? {
        if sources.contains_key(&name) {
            continue;
        }
        let upstream = if name.contains('.') { resolve_view(conn, &name).ok() } else { job_status(conn, &name)?.map(|_| name.clone()) };
        if let Some(up) = upstream {
            if up == job || reads_transitively(conn, &up, job)? {
                bail!("view {job} would read itself through {name}: cycles between views are not allowed");
            }
            let (def, _) = load_job(conn, &up)?;
            if def.plan.sink_columns.iter().any(|(c, _)| c == COMMIT_TS) {
                bail!("view {up} has a column named {COMMIT_TS}, which DuckWake adds to every lake input; rename it in {up}");
            }
            let (_, schema, table) = crate::lake::qualified_table(&def.sink)?;
            let table_id = crate::lake::table_id(conn, &schema, &table)?.with_context(|| format!("the table of view {up} is missing"))?;
            let windowed = def.plan.window.as_ref().filter(|_| {
                ["window_start", "window_end"].iter().all(|w| def.plan.sink_columns.iter().any(|(c, _)| c == w))
            });
            // An append-only view's table only ever gains rows; any other has rows replaced or removed.
            let retracts = def.plan.sink_mode() != plan::SinkMode::Append || def.plan.retracts();
            out.push(LakeInput {
                name,
                table: def.sink.clone(),
                table_id,
                columns: def.plan.sink_columns.iter().cloned().chain([(COMMIT_TS.to_string(), "TIMESTAMP".to_string())]).collect(),
                view: Some(up),
                window_size_us: windowed.map(|w| w.size_us),
                retracts,
                counted: crate::circuit::Circuit::compile(&def.view())?.counts_rows(),
            });
            continue;
        }
        let (catalog, schema, table) = match name.split('.').collect::<Vec<_>>().as_slice() {
            [t] => (LAKE.to_string(), "main".to_string(), t.to_string()),
            _ => match crate::lake::qualified_table(&name) {
                Ok(parts) => parts,
                Err(_) => continue,
            },
        };
        if schema == CTL.rsplit('.').next().unwrap_or(CTL) {
            bail!("views cannot read DuckWake's control tables ({name})");
        }
        let Some(table_id) = crate::lake::table_id(conn, &schema, &table)? else { continue };
        let qualified = format!("{catalog}.{schema}.{table}");
        let mut columns = crate::lake::describe(conn, &quote_table(&qualified)?)?;
        if columns.iter().any(|(c, _)| c == COMMIT_TS) {
            bail!("{name} has a column named {COMMIT_TS}, which DuckWake adds to every lake input");
        }
        columns.push((COMMIT_TS.to_string(), "TIMESTAMP".to_string()));
        // Any lake table may have rows updated or deleted; `create_view` holds a windowed view to insert-only.
        out.push(LakeInput { name, table: qualified, table_id, columns, view: None, window_size_us: None, retracts: true, counted: false });
    }
    Ok(out)
}

/// Whether view `from` reads view `to`, directly or through other views.
fn reads_transitively(conn: &Connection, from: &str, to: &str) -> Result<bool> {
    let mut stack = vec![from.to_string()];
    let mut seen = std::collections::HashSet::new();
    while let Some(v) = stack.pop() {
        if !seen.insert(v.clone()) {
            continue;
        }
        let mut stmt = conn.prepare(&format!("SELECT upstream_job FROM {CTL}.dependencies WHERE job_id = ? AND upstream_job IS NOT NULL"))?;
        for up in stmt.query_map([&v], |r| r.get::<_, String>(0))? {
            let up = up?;
            if up == to {
                return Ok(true);
            }
            stack.push(up);
        }
    }
    Ok(false)
}

/// The views that read `job`'s table.
pub fn readers(conn: &Connection, job: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT DISTINCT job_id FROM {CTL}.dependencies WHERE upstream_job = ? AND job_id <> ? ORDER BY job_id"
    ))?;
    let rows = stmt.query_map([job, job], |r| r.get::<_, String>(0))?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

pub fn create_source(conn: &Connection, name: &str, kind: &str, options: &serde_json::Value) -> Result<SourceDef> {
    crate::lake::check_name("source", name)?;
    if kind != "kafka" {
        bail!("only kind 'kafka' is supported (HTTP push and S3 polling come later)");
    }
    let options = SourceOptions::from_json(options)?;
    if load_sources(conn)?.contains_key(name) {
        bail!("source {name:?} already exists");
    }
    let def = SourceDef { name: name.into(), kind: kind.into(), options };
    conn.execute(
        &format!("INSERT INTO {CTL}.sources VALUES (?, ?, ?, now())"),
        [name, kind, &serde_json::to_string(&def.options)?],
    )?;
    Ok(def)
}

pub fn load_sources(conn: &Connection) -> Result<HashMap<String, SourceDef>> {
    let mut stmt = conn.prepare(&format!("SELECT name, kind, options::VARCHAR FROM {CTL}.sources"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?;
    let mut out = HashMap::new();
    for row in rows {
        let (name, kind, opts) = row?;
        let options: SourceOptions = serde_json::from_str(&opts)?;
        out.insert(name.clone(), SourceDef { name, kind, options });
    }
    Ok(out)
}

/// A view is named by the DuckLake table it maintains (`analytics.clicks_per_min`, or `lake.analytics...`;
/// a bare name means the `main` schema). Its job id is the table name, which keys the lease, state and
/// checkpoints, so table names must be unique across schemas. `sink` is the deprecated form that named the
/// job and the table separately. Returns (job id, `lake.schema.table`).
pub fn view_names(name: &str, sink: Option<&str>) -> Result<(String, String)> {
    if let Some(sink) = sink {
        let (c, s, t) = crate::lake::qualified_table(sink)?;
        return Ok((name.to_string(), format!("{c}.{s}.{t}")));
    }
    let (c, s, t) = if name.contains('.') { crate::lake::qualified_table(name)? } else { (LAKE.into(), "main".into(), name.into()) };
    Ok((t.clone(), format!("{c}.{s}.{t}")))
}

/// The job id of a view given by job id or by its table name (`analytics.clicks_per_min`).
pub fn resolve_view(conn: &Connection, name: &str) -> Result<String> {
    if !name.contains('.') {
        return Ok(name.to_string());
    }
    let (_, schema, table) = crate::lake::qualified_table(name)?;
    let mut stmt = conn.prepare(&format!("SELECT job_id, definition::VARCHAR FROM {CTL}.jobs ORDER BY job_id"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (job, def) = row?;
        let def: JobDef = serde_json::from_str(&def)?;
        let (_, s, t) = crate::lake::qualified_table(&def.sink)?;
        if s == schema && t == table {
            return Ok(job);
        }
    }
    bail!("no view writes {name:?}")
}

pub struct CreateView<'a> {
    pub job_id: &'a str,
    pub sql: &'a str,
    pub sink: &'a str,
    pub late_data: Option<&'a str>,
    pub checkpoint: &'a str,
    pub epoch_interval: &'a str,
    pub max_rows: usize,
    pub inline_row_limit: usize,
}

/// Compiles the view, creates its sink / late-data / state / lease tables and registers the job, in one
/// transaction. The job starts in status `created`; `start` assigns it to a worker.
pub fn create_view(conn: &Connection, v: &CreateView) -> Result<JobDef> {
    crate::lake::check_name("view", v.job_id)?;
    if v.checkpoint != "ducklake" {
        bail!("CHECKPOINT {:?}: only the ducklake backend is implemented (object storage state is phase 3)", v.checkpoint);
    }
    parse_duration(v.epoch_interval)?;
    if job_status(conn, v.job_id)?.is_some() {
        bail!("view {:?} already exists", v.job_id);
    }
    let sources = load_sources(conn)?;
    let mut lake_inputs = resolve_lake_inputs(conn, v.job_id, v.sql, &sources)?;
    let plan = compile_new(conn, v.job_id, v.sql, &sources, &mut lake_inputs)?;
    if v.late_data.is_some() && !plan.is_windowed() {
        bail!("only windowed views have late data; drop --late-data for a {} view", plan.kind_name());
    }
    // Tables the view joins to a Kafka source are lookups, read whole every epoch; only the streams are inputs.
    let streams = plan.sources();
    lake_inputs.retain(|l| streams.contains(&l.name));
    let start_snapshot = (!lake_inputs.is_empty()).then(|| crate::lake::current_snapshot(conn)).transpose()?;
    let src_columns = input_columns(&plan.source, &sources, &lake_inputs)?.to_vec();
    let def = JobDef {
        job_id: v.job_id.into(),
        sql: v.sql.into(),
        sink: v.sink.into(),
        late_data: v.late_data.map(String::from),
        checkpoint: v.checkpoint.into(),
        epoch_interval: v.epoch_interval.into(),
        max_rows: v.max_rows,
        inline_row_limit: v.inline_row_limit,
        plan,
        lake_inputs,
        start_snapshot,
    };
    // Refuse at create time what the worker would refuse at start: operators fed rows they cannot handle.
    let circuit = crate::circuit::Circuit::compile(&def.view())?;
    let sink = quote_table(&def.sink)?;
    let (_, sink_schema, sink_name) = crate::lake::qualified_table(&def.sink)?;

    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        conn.execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {LAKE}.{}", ident(&sink_schema)))?;
        let mut cols: Vec<String> =
            def.plan.sink_columns.iter().chain(&circuit.sink_extra_columns()).map(|(c, t)| format!("{} {t}", ident(c))).collect();
        cols.push("_duckwake_epoch BIGINT".into());
        // The view owns its table: creating one over an existing table would mix in rows no epoch wrote.
        if table_exists(conn, &sink_schema, &sink_name)? {
            bail!("table {} already exists; drop it or name the view after a new table", def.sink);
        }
        conn.execute_batch(&format!("CREATE TABLE {sink} ({})", cols.join(", ")))?;
        if def.plan.is_windowed() && def.plan.sink_columns.iter().any(|(c, _)| c == "window_start") {
            conn.execute_batch(&format!(
                "ALTER TABLE {sink} SET PARTITIONED BY (year(window_start), month(window_start), day(window_start))"
            ))?;
        }
        if let Some(late) = &def.late_data {
            let (_, ls, lt) = crate::lake::qualified_table(late)?;
            conn.execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {LAKE}.{}", ident(&ls)))?;
            if !table_exists(conn, &ls, &lt)? {
                let mut cols: Vec<String> = src_columns
                    .iter()
                    .filter(|(c, _)| !crate::circuit::codegen::is_window_column(c))
                    .map(|(c, t)| format!("{} {t}", ident(c)))
                    .collect();
                cols.extend(
                    ["window_start TIMESTAMP", "window_end TIMESTAMP", "_source_partition INTEGER", "_source_offset BIGINT", "_duckwake_epoch BIGINT"]
                        .map(String::from),
                );
                conn.execute_batch(&format!("CREATE TABLE {} ({})", quote_table(late)?, cols.join(", ")))?;
            }
        }
        if def.plan.has_state() {
            let cols: Vec<String> = def.plan.state_columns.iter().map(|(c, t)| format!("{} {t}", ident(c))).collect();
            conn.execute_batch(&format!("CREATE TABLE {} ({})", state_table(v.job_id), cols.join(", ")))?;
        }
        conn.execute_batch(&format!(
            "CREATE TABLE {lt} (owner_worker VARCHAR, lease_epoch BIGINT, last_epoch BIGINT);
             INSERT INTO {lt} VALUES (NULL, 0, 0);",
            lt = lease_table(v.job_id)
        ))?;
        conn.execute(
            &format!("INSERT INTO {CTL}.jobs VALUES (?, ?, 1, 'created', now(), now())"),
            [v.job_id, &serde_json::to_string(&def)?],
        )?;
        for l in &def.lake_inputs {
            conn.execute(
                &format!("INSERT INTO {CTL}.dependencies VALUES (?, ?, ?, ?)"),
                duckdb::params![v.job_id, l.name, l.table_id, l.view],
            )?;
        }
        Ok(())
    })();
    finish(conn, res)?;
    set_sink_inlining(conn, &def)?;
    Ok(def)
}

/// Sized so a typical epoch's sink rows land in the catalog instead of a tiny Parquet file (DuckLake's
/// default inlines at most 10 rows). Must run after the sink is committed (DuckLake rejects options on tables
/// created in the same transaction), and the worker re-applies it on its own connection: an attached lake
/// does not see options set later by another connection, so its epochs would keep writing one small file each.
pub fn set_sink_inlining(conn: &Connection, def: &JobDef) -> Result<()> {
    let (_, schema, table) = crate::lake::qualified_table(&def.sink)?;
    set_inlining(conn, &schema, &table, def.inline_row_limit)
}

/// Rows per insert the checkpoint table inlines. With `lean_commit` one INSERT carries the checkpoint rows of
/// every job on the worker, more than DuckLake's default of 10 once a worker runs more than 10 jobs.
const CHECKPOINT_INLINE_ROWS: usize = 1_000;

/// Inlining for the checkpoint table, set by each worker on its own connection at start (see
/// `set_sink_inlining`). Dead letters and late rows keep DuckLake's default: they come in thousands per
/// epoch, which cost more as catalog rows than as one Parquet file (16-view bench, `docs/internals.md`).
/// Lease and state tables keep it too: a lease write is one row, and state is rewritten every epoch.
pub fn set_control_inlining(conn: &Connection) -> Result<()> {
    let schema = CTL.rsplit('.').next().unwrap_or(CTL);
    set_inlining(conn, schema, "checkpoints", CHECKPOINT_INLINE_ROWS)
}

/// Skips the write when this connection already sees the option: several workers starting at once would
/// otherwise all update the same catalog row, and DuckLake reports the losers' failures as errors that are not
/// conflicts. The write is idempotent, so a failed one is retried.
fn set_inlining(conn: &Connection, schema: &str, table: &str, rows: usize) -> Result<()> {
    let set: i64 = conn.query_row(
        &format!(
            "SELECT count(*) FROM ducklake_options({}) WHERE option_name = 'data_inlining_row_limit'
               AND scope = 'TABLE' AND scope_entry = ? AND value = ?",
            lit(LAKE)
        ),
        [format!("{schema}.{table}"), rows.to_string()],
        |r| r.get(0),
    )?;
    if set > 0 {
        return Ok(());
    }
    let sql = format!(
        "CALL {LAKE}.set_option('data_inlining_row_limit', {rows}, schema => {}, table_name => {})",
        lit(schema),
        lit(table)
    );
    let mut attempt = 0;
    loop {
        attempt += 1;
        match conn.execute_batch(&sql) {
            Ok(()) => return Ok(()),
            Err(_) if attempt < 5 => std::thread::sleep(std::time::Duration::from_millis(100 * attempt)),
            Err(e) => return Err(e).with_context(|| format!("setting inlining on {schema}.{table}")),
        }
    }
}

fn finish(conn: &Connection, res: Result<()>) -> Result<()> {
    match res {
        Ok(()) => conn.execute_batch("COMMIT").map_err(Into::into),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn table_exists(conn: &Connection, schema: &str, table: &str) -> Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [LAKE, schema, table],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

pub fn job_status(conn: &Connection, job: &str) -> Result<Option<JobStatus>> {
    let mut stmt = conn.prepare(&format!("SELECT status FROM {CTL}.jobs WHERE job_id = ?"))?;
    let mut rows = stmt.query([job])?;
    match rows.next()? {
        Some(r) => Ok(Some(JobStatus::parse(&r.get::<_, String>(0)?)?)),
        None => Ok(None),
    }
}

pub fn load_job(conn: &Connection, job: &str) -> Result<(JobDef, JobStatus)> {
    let (def, status): (String, String) = conn
        .query_row(&format!("SELECT definition::VARCHAR, status FROM {CTL}.jobs WHERE job_id = ?"), [job], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .map_err(|_| anyhow!("no such view {job:?}"))?;
    Ok((serde_json::from_str(&def)?, JobStatus::parse(&status)?))
}

/// Running jobs and their current lease owner.
pub fn running_jobs(conn: &Connection) -> Result<Vec<(String, Option<String>, i64)>> {
    let mut stmt = conn.prepare(&format!("SELECT job_id FROM {CTL}.jobs WHERE status = 'running' ORDER BY job_id"))?;
    let ids = stmt.query_map([], |r| r.get::<_, String>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
    let mut out = Vec::new();
    for id in ids {
        let (owner, lease) = lease(conn, &id)?;
        out.push((id, owner, lease));
    }
    Ok(out)
}

pub fn lease(conn: &Connection, job: &str) -> Result<(Option<String>, i64)> {
    Ok(conn.query_row(&format!("SELECT owner_worker, lease_epoch FROM {}", lease_table(job)), [], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?)
}

/// Bumps the lease epoch for a job this worker owns, fencing any older incarnation of the worker.
pub fn take_lease(conn: &Connection, job: &str, worker: &str) -> Result<Option<i64>> {
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<Option<i64>> {
        let n = conn.execute(
            &format!("UPDATE {} SET lease_epoch = lease_epoch + 1 WHERE owner_worker = ?", lease_table(job)),
            [worker],
        )?;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(lease(conn, job)?.1))
    })();
    match res {
        Ok(v) => {
            conn.execute_batch("COMMIT")?;
            Ok(v)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Retries a lake transaction that lost a write-write race (DuckLake: first committer wins).
pub fn retry_conflicts<T>(mut f: impl FnMut() -> Result<T>) -> Result<T> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match f() {
            Err(e) if crate::lake::is_conflict(&e) && attempt < 5 => {
                std::thread::sleep(std::time::Duration::from_millis(100 * attempt));
            }
            other => return other,
        }
    }
}

/// `start` assigns the job to `worker` (`any`: to whichever live worker claims it) and bumps the lease, which
/// fences the previous owner's next commit.
pub fn set_status(conn: &Connection, job: &str, status: JobStatus, worker: Option<&str>) -> Result<()> {
    job_status(conn, job)?.ok_or_else(|| anyhow!("no such view {job:?}"))?;
    retry_conflicts(|| set_status_once(conn, job, status, worker))
}

fn set_status_once(conn: &Connection, job: &str, status: JobStatus, worker: Option<&str>) -> Result<()> {
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        conn.execute(
            &format!("UPDATE {CTL}.jobs SET status = ?, updated_at = now() WHERE job_id = ?"),
            [status.as_str(), job],
        )?;
        if let Some(w) = worker {
            // `any` leaves the view unassigned, for any live worker to claim (see `failover`).
            let owner = (w != crate::failover::ANY_WORKER).then_some(w);
            conn.execute(
                &format!("UPDATE {} SET owner_worker = ?, lease_epoch = lease_epoch + 1", lease_table(job)),
                [owner],
            )?;
        }
        Ok(())
    })();
    finish(conn, res)
}

#[derive(Debug, Clone, Default)]
pub struct Checkpoint {
    pub epoch: i64,
    pub offsets: BTreeMap<i32, i64>,
    pub watermark_us: Option<i64>,
    pub max_event_time_us: Option<i64>,
}

pub fn latest_checkpoint(conn: &Connection, job: &str, source: &str) -> Result<Option<Checkpoint>> {
    Ok(checkpoints(conn, job, source, "ORDER BY epoch DESC LIMIT 1")?.pop())
}

/// The job's checkpoints in epoch order (only the newest ones survive pruning).
pub fn all_checkpoints(conn: &Connection, job: &str, source: &str) -> Result<Vec<Checkpoint>> {
    checkpoints(conn, job, source, "ORDER BY epoch")
}

fn checkpoints(conn: &Connection, job: &str, source: &str, order: &str) -> Result<Vec<Checkpoint>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT epoch, source_offsets::VARCHAR, epoch_us(watermark), epoch_us(max_event_time)
         FROM {CTL}.checkpoints WHERE job_id = ? {order}"
    ))?;
    let mut rows = stmt.query([job])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let offsets: serde_json::Value = serde_json::from_str(&r.get::<_, String>(1)?)?;
        let mut parts = BTreeMap::new();
        if let Some(m) = offsets.get(source).and_then(|m| m.as_object()) {
            for (p, o) in m {
                parts.insert(p.parse()?, o.as_i64().context("offset")?);
            }
        }
        out.push(Checkpoint {
            epoch: r.get(0)?,
            offsets: parts,
            watermark_us: r.get(2)?,
            max_event_time_us: r.get(3)?,
        });
    }
    Ok(out)
}

/// Rewinds a stopped/paused job to `to_epoch`: deletes sink, late and dead-letter rows written after it,
/// restores the state table from the epoch's DuckLake snapshot (time travel) and drops later checkpoints.
/// The next start replays from that checkpoint's offsets.
pub fn reset(conn: &Connection, job: &str, to_epoch: i64) -> Result<()> {
    let (def, status) = load_job(conn, job)?;
    if status == JobStatus::Running {
        bail!("pause or stop {job:?} before reset");
    }
    let snapshot: Option<i64> = if to_epoch > 0 {
        let s = conn
            .query_row(
                &format!(
                    "SELECT max(snapshot_id) FROM ducklake_snapshots({}) WHERE json_extract_string(commit_extra_info, {})::BIGINT = ?",
                    lit(LAKE),
                    lit(&format!("$.jobs.{job}"))
                ),
                [to_epoch],
                |r| r.get::<_, Option<i64>>(0),
            )?
            .ok_or_else(|| anyhow!("no snapshot for {job} epoch {to_epoch}: it was never committed or has been expired"))?;
        Some(s)
    } else {
        None
    };
    // Its deletes from the shared control tables can lose to a running job's checkpoint insert.
    retry_conflicts(|| reset_once(conn, job, &def, to_epoch, snapshot))
}

fn reset_once(conn: &Connection, job: &str, def: &JobDef, to_epoch: i64, snapshot: Option<i64>) -> Result<()> {
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        let sink = quote_table(&def.sink)?;
        match (def.plan.sink_mode(), snapshot) {
            // Append sinks: drop what later epochs added.
            (plan::SinkMode::Append, _) => {
                conn.execute(&format!("DELETE FROM {sink} WHERE _duckwake_epoch > ?"), [to_epoch])?;
            }
            // Upsert sinks: later epochs also replaced rows, so restore the table as of the epoch.
            (plan::SinkMode::Upsert, Some(s)) => {
                conn.execute_batch(&format!("DELETE FROM {sink}; INSERT INTO {sink} SELECT * FROM {sink} AT (VERSION => {s});"))?;
            }
            (plan::SinkMode::Upsert, None) => conn.execute_batch(&format!("DELETE FROM {sink}"))?,
        }
        if let Some(l) = &def.late_data {
            conn.execute(&format!("DELETE FROM {} WHERE _duckwake_epoch > ?", quote_table(l)?), [to_epoch])?;
        }
        conn.execute(&format!("DELETE FROM {CTL}.dead_letter WHERE job_id = ? AND epoch > ?"), duckdb::params![job, to_epoch])?;
        if def.plan.has_state() {
            let st = state_table(job);
            conn.execute_batch(&format!("DELETE FROM {st}"))?;
            if let Some(s) = snapshot {
                conn.execute_batch(&format!("INSERT INTO {st} SELECT * FROM {st} AT (VERSION => {s})"))?;
            }
        }
        conn.execute(&format!("DELETE FROM {CTL}.checkpoints WHERE job_id = ? AND epoch > ?"), duckdb::params![job, to_epoch])?;
        Ok(())
    })();
    finish(conn, res)
}

/// Removes a stopped view: its table (unless `keep_table`), job row, lease and state tables, checkpoints
/// and dead letters. A late-data table is named by the user and may be shared, so it is kept.
pub fn drop_view(conn: &Connection, job: &str, keep_table: bool) -> Result<()> {
    let (def, status) = load_job(conn, job)?;
    if status == JobStatus::Running {
        bail!("stop {job:?} before dropping it");
    }
    let readers = readers(conn, job)?;
    if !readers.is_empty() {
        bail!("view {job} is read by {}; drop those views first", readers.join(", "));
    }
    // Like `reset`, its deletes from the shared control tables can lose to a running job's inserts.
    retry_conflicts(|| drop_view_once(conn, job, &def, keep_table))
}

fn drop_view_once(conn: &Connection, job: &str, def: &JobDef, keep_table: bool) -> Result<()> {
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        conn.execute(&format!("DELETE FROM {CTL}.jobs WHERE job_id = ?"), [job])?;
        conn.execute(&format!("DELETE FROM {CTL}.checkpoints WHERE job_id = ?"), [job])?;
        conn.execute(&format!("DELETE FROM {CTL}.dead_letter WHERE job_id = ?"), [job])?;
        conn.execute(&format!("DELETE FROM {CTL}.dependencies WHERE job_id = ?"), [job])?;
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {};", lease_table(job), state_table(job)))?;
        if !keep_table {
            conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", quote_table(&def.sink)?))?;
        }
        Ok(())
    })();
    finish(conn, res)
}

/// Removes a source no view reads from.
pub fn drop_source(conn: &Connection, name: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("SELECT job_id, definition::VARCHAR FROM {CTL}.jobs"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (job, def) = row?;
        let def: JobDef = serde_json::from_str(&def)?;
        if def.plan.sources().iter().any(|s| s == name) {
            bail!("view {job:?} reads from {name:?}; drop it first");
        }
    }
    if conn.execute(&format!("DELETE FROM {CTL}.sources WHERE name = ?"), [name])? == 0 {
        bail!("no such source {name:?}");
    }
    Ok(())
}

/// One row per view: status, lease owner, latest checkpoint and running totals; None when there are no views.
pub fn jobs_sql(conn: &Connection) -> Result<Option<String>> {
    let any: i64 = conn.query_row(&format!("SELECT count(*) FROM {CTL}.jobs"), [], |r| r.get(0))?;
    if any == 0 {
        return Ok(None);
    }
    jobs_query(conn).map(Some)
}

/// The query behind `duckwake_jobs()`: `jobs_sql`, returning no rows (with the same columns) when there are no views.
pub fn jobs_query(conn: &Connection) -> Result<String> {
    let mut stmt = conn.prepare(&format!("SELECT job_id FROM {CTL}.jobs ORDER BY job_id"))?;
    let ids = stmt.query_map([], |r| r.get::<_, String>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
    let mut leases: Vec<String> = ids
        .iter()
        .map(|id| format!("SELECT {} AS job_id, owner_worker, lease_epoch FROM {}", lit(id), lease_table(id)))
        .collect();
    if leases.is_empty() {
        leases.push("SELECT NULL::VARCHAR AS job_id, NULL::VARCHAR AS owner_worker, NULL::BIGINT AS lease_epoch WHERE false".into());
    }
    Ok(format!(
        "WITH leases AS ({}),
         last AS (SELECT * FROM {CTL}.checkpoints QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY epoch DESC) = 1),
         totals AS (SELECT job_id, sum(rows_in) AS rows_in, sum(rows_out) AS rows_out, sum(rows_late) AS late, sum(rows_dead) AS dead
                    FROM {CTL}.checkpoints GROUP BY ALL)
         SELECT j.job_id, j.status, l.owner_worker AS worker, l.lease_epoch AS lease, c.epoch, c.watermark,
                round(epoch(now()::TIMESTAMP - c.max_event_time), 1) AS freshness_s, c.lag,
                t.rows_in, t.rows_out, t.late, t.dead, c.committed_at::TIMESTAMP AS last_commit
         FROM {CTL}.jobs j LEFT JOIN leases l USING (job_id) LEFT JOIN last c USING (job_id) LEFT JOIN totals t USING (job_id)
         ORDER BY j.job_id",
        leases.join(" UNION ALL ")
    ))
}
