//! Control data in `lake.__duckwake`: sources, jobs, leases, checkpoints. Everything shares the lake's
//! transactions, so registering a view and creating its tables is one atomic step.
//!
//! Fencing uses one lease table per job (`__duckwake.lease_<job>`), not a column of the shared `jobs` table:
//! in DuckLake, an UPDATE of one row in a table conflicts with a concurrent UPDATE of any other row of the
//! same table, so per-epoch fencing on `jobs` would make every worker conflict with every other one.
use crate::lake::{ident, lit, quote_table, CTL, LAKE};
use crate::model::{parse_duration, Input, JobDef, JobStatus, LakeInput, Limits, SourceDef, SourceOptions, COMMIT_TS};
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
    let sources = view_sources(&def, load_sources(conn)?);
    let plan = compile(conn, job, &def.sql, &sources, &def.lake_inputs, def.upsert.as_ref())?;
    if plan.state_columns != def.plan.state_columns || plan.sink_columns != def.plan.sink_columns {
        bail!(
            "view {job} was created by a DuckWake version whose state or table layout differs from this one's; \
             drop and create it again"
        );
    }
    def.plan = plan;
    Ok((def, sources))
}

/// The sources as `def` sees them: each Kafka source it reads with the columns it was created with, so a
/// column added to a source later changes nothing in the views that already read it.
pub fn view_sources(def: &JobDef, mut sources: HashMap<String, SourceDef>) -> HashMap<String, SourceDef> {
    for (name, columns) in &def.source_columns {
        if let Some(s) = sources.get_mut(name) {
            s.options.columns = columns.clone();
        }
    }
    sources
}

/// Compiles a view whose FROM names Kafka sources, and lake tables and views resolved to `lakes`; with `upsert`,
/// a view that reads its Kafka source as upserts.
pub fn compile(
    conn: &Connection,
    job: &str,
    sql: &str,
    sources: &HashMap<String, SourceDef>,
    lakes: &[LakeInput],
    upsert: Option<&plan::UpsertSpec>,
) -> Result<plan::Plan> {
    let mut inputs: HashMap<String, Input> = sources.iter().map(|(n, s)| (n.clone(), Input::Kafka(s))).collect();
    inputs.extend(lakes.iter().map(|l| (l.name.clone(), Input::Lake(l))));
    plan::compile_with(conn, job, sql, &inputs, upsert)
}

/// Compiles a new view and settles how it reads each lake input it streams: updates and deletes as
/// retractions, from any lake table and from a view's table whose rows change. A windowed view applies a
/// retraction to a window still open; one for a window the watermark has passed is a late correction (its
/// late-data table, or counted and dropped), as a late row is.
fn compile_new(
    conn: &Connection,
    job: &str,
    sql: &str,
    sources: &HashMap<String, SourceDef>,
    lakes: &mut [LakeInput],
    upsert: Option<&plan::UpsertSpec>,
) -> Result<plan::Plan> {
    // The view's shape first, every input insert-only: over retracting ones some windowed aggregates are refused.
    let inserts: Vec<LakeInput> = lakes.iter().map(|l| LakeInput { retracts: false, ..l.clone() }).collect();
    let plan = compile(conn, job, sql, sources, &inserts, upsert)?;
    let streams = plan.sources();
    let retracting = |l: &LakeInput| l.retracts && streams.contains(&l.name);
    if !lakes.iter().any(retracting) {
        return Ok(plan);
    }
    compile(conn, job, sql, sources, lakes, upsert)
}

/// The columns of a view input named `name`: a Kafka source's or a lake input's.
pub fn input_columns<'a>(name: &str, sources: &'a HashMap<String, SourceDef>, lakes: &'a [LakeInput]) -> Result<&'a [(String, String)]> {
    if let Some(s) = sources.get(name) {
        return Ok(&s.options.columns);
    }
    lakes.iter().find(|l| l.name == name).map(|l| l.columns.as_slice()).with_context(|| format!("input {name} of the view is gone"))
}

/// Resolves the names a view's FROM uses that are not Kafka sources: first to a view (by the table it writes),
/// then to a lake table. Names that are neither are left to DuckDB, which binds them or
/// fails. Every such table becomes a lake input of the view; the compiler decides which of them it reads as
/// a change feed and which it joins as a lookup.
fn resolve_lake_inputs(conn: &Connection, job: &str, sql: &str, sources: &HashMap<String, SourceDef>) -> Result<Vec<LakeInput>> {
    let mut out = Vec::new();
    for name in plan::table_names(conn, sql)? {
        if sources.contains_key(&name) {
            continue;
        }
        let upstream = view_read_by(conn, &name)?;
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
    if kind != crate::sources::kafka::KIND {
        bail!("kind {kind:?}: catalog::create_source takes Kafka sources; database sources go through sources::create");
    }
    if source_kind(conn, name)?.is_some() {
        bail!("source {name:?} already exists");
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

/// Adds a column to a Kafka source. Views that read the source keep the columns they were created with (their
/// definitions record them, in the same transaction); a view created or replaced afterwards can use the new
/// one. Events without the field get NULL, as for any missing field.
pub fn add_source_column(conn: &Connection, source: &str, column: &str, ty: &str) -> Result<SourceDef> {
    crate::model::check_column(column, ty)?;
    // The type must be one DuckDB knows; `prepare` also refuses anything but one statement.
    conn.prepare(&format!("SELECT NULL::{ty}")).with_context(|| format!("column {column}: type {ty:?}"))?;
    retry_conflicts(|| {
        let mut sources = load_sources(conn)?;
        let mut def = sources.remove(source).ok_or_else(|| anyhow!("no such source {source:?}"))?;
        if def.options.columns.iter().any(|(c, _)| c.eq_ignore_ascii_case(column)) {
            bail!("source {source} already has a column {column}");
        }
        let before = def.options.columns.clone();
        def.options.columns.push((column.to_string(), ty.to_string()));
        // Every rule a new source must meet, the decoder's included.
        def.options = SourceOptions::from_json(&serde_json::to_value(&def.options)?)?;
        let jobs = jobs_reading(conn, source)?;
        conn.execute_batch("BEGIN")?;
        let res = (|| -> Result<()> {
            conn.execute(
                &format!("UPDATE {CTL}.sources SET options = ? WHERE name = ?"),
                [&serde_json::to_string(&def.options)?, source],
            )?;
            for (job, mut jd) in jobs {
                if jd.source_columns.contains_key(source) {
                    continue;
                }
                jd.source_columns.insert(source.to_string(), before.clone());
                conn.execute(
                    &format!("UPDATE {CTL}.jobs SET definition = ?, updated_at = now() WHERE job_id = ?"),
                    [&serde_json::to_string(&jd)?, &job],
                )?;
            }
            Ok(())
        })();
        finish(conn, res)?;
        Ok(def.clone())
    })
}

/// Every view whose definition reads Kafka source `source`, with its definition.
fn jobs_reading(conn: &Connection, source: &str) -> Result<Vec<(String, JobDef)>> {
    let mut stmt = conn.prepare(&format!("SELECT job_id, definition::VARCHAR FROM {CTL}.jobs ORDER BY job_id"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    let mut out = Vec::new();
    for row in rows {
        let (job, def) = row?;
        let Some(def) = view_def(&def)? else { continue };
        if def.plan.sources().iter().any(|s| s == source) && !def.lake_inputs.iter().any(|l| l.name == source) {
            out.push((job, def));
        }
    }
    Ok(out)
}

pub fn load_sources(conn: &Connection) -> Result<HashMap<String, SourceDef>> {
    // Database sources are not read in FROM: their mirror tables are (`sources::database::mirror`).
    let mut stmt = conn.prepare(&format!(
        "SELECT name, kind, options::VARCHAR FROM {CTL}.sources WHERE kind = {}",
        lit(crate::sources::kafka::KIND)
    ))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?;
    let mut out = HashMap::new();
    for row in rows {
        let (name, kind, opts) = row?;
        let options: SourceOptions = serde_json::from_str(&opts)?;
        out.insert(name.clone(), SourceDef { name, kind, options });
    }
    Ok(out)
}

/// A view is named by the DuckLake table it maintains: `analytics.clicks_per_min`, or `lake.analytics...`; a bare
/// name means the `main` schema. Its job id is `schema.table`, which keys its lease, state and checkpoints, so
/// two schemas can each have a view of the same table name. An ingest job's id is its source's name, which has
/// no dot, so the two never meet. Returns (job id, `lake.schema.table`).
pub fn view_names(name: &str) -> Result<(String, String)> {
    let (c, s, t) = if name.contains('.') { crate::lake::qualified_table(name)? } else { (LAKE.into(), "main".into(), name.into()) };
    crate::lake::check_name("schema", &s)?;
    crate::lake::check_name("view", &t)?;
    Ok((format!("{s}.{t}"), format!("{c}.{s}.{t}")))
}

/// Whether a job id is a view's (`schema.table`) rather than an ingest job's (a source name).
pub fn is_view_id(job: &str) -> bool {
    job.contains('.')
}

/// The job id of the view a command names: `schema.table` or `lake.schema.table`, or a bare table name when
/// exactly one schema has a view of that name. A bare name no view has is a database source's ingest job.
pub fn resolve_view(conn: &Connection, name: &str) -> Result<String> {
    if name.contains('.') {
        let (_, s, t) = crate::lake::qualified_table(name)?;
        let job = format!("{s}.{t}");
        if job_status(conn, &job)?.is_none() {
            bail!("no view writes {name:?}");
        }
        return Ok(job);
    }
    let mut stmt = conn.prepare(&format!("SELECT job_id FROM {CTL}.jobs WHERE ends_with(job_id, ?) ORDER BY job_id"))?;
    let views = stmt.query_map([format!(".{name}")], |r| r.get::<_, String>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
    match views.as_slice() {
        [job] => Ok(job.clone()),
        [] if job_status(conn, name)?.is_some() => Ok(name.to_string()),
        [] => bail!("no view or database source named {name:?}"),
        _ => bail!("{} views are named {name:?}: name one by its schema, one of {}", views.len(), views.join(", ")),
    }
}

/// The view a name in a view's SQL reads, if it is one: names there mean what they mean to DuckDB, so a bare
/// name is a table of the `main` schema.
fn view_read_by(conn: &Connection, name: &str) -> Result<Option<String>> {
    let job = if name.contains('.') {
        let (_, s, t) = crate::lake::qualified_table(name)?;
        format!("{s}.{t}")
    } else {
        format!("main.{name}")
    };
    Ok(job_status(conn, &job)?.map(|_| job))
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
    pub limits: Limits,
    /// For a view that reads lake tables or views through their change feeds: `earliest` (the default when
    /// None) backfills it from the rows they hold when it is created, then follows their changes; `latest`
    /// takes only the rows written after that. Kafka sources have their own `start`.
    pub start: Option<&'a str>,
    /// What to do when the view exists.
    pub if_exists: IfExists,
    /// Read the view's Kafka source as upserts: one row per key (`plan::UpsertSpec`).
    pub upsert: Option<plan::UpsertSpec>,
}

/// What `create_view` does when a view of that name exists.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum IfExists {
    /// Refuse.
    #[default]
    Fail,
    /// Make the view match the request without recomputing it: nothing when it already does, `alter_view` when
    /// only settings differ; refuse when what it computes differs (its query, table, late-data table,
    /// checkpoint or inlining), since only a replace recomputes.
    Update,
    /// Replace it (`CREATE OR REPLACE`): see `replace_view`.
    Replace,
}

impl IfExists {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "fail" => Ok(Self::Fail),
            "update" => Ok(Self::Update),
            "replace" => Ok(Self::Replace),
            _ => bail!("if_exists {s:?}: expected 'fail', 'update' or 'replace'"),
        }
    }
}

/// What `create_view` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Created,
    /// `IfExists::Update` found the view as requested.
    Unchanged,
    /// `IfExists::Update` changed its settings.
    Altered,
    Replaced,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Unchanged => "unchanged",
            Self::Altered => "altered",
            Self::Replaced => "replaced",
        }
    }
}

/// Compiles the view, creates its sink / late-data / state / lease tables and registers the job, in one
/// transaction. The job starts in status `created`; `start` assigns it to a worker. An existing view of that
/// name is refused, updated or replaced as `if_exists` says.
pub fn create_view(conn: &Connection, v: &CreateView) -> Result<(JobDef, Outcome)> {
    if !is_view_id(v.job_id) {
        bail!("view id {:?}: expected schema.table (catalog::view_names)", v.job_id);
    }
    crate::lake::check_view_ids(conn)?;
    if v.checkpoint != "ducklake" {
        bail!("CHECKPOINT {:?}: only the ducklake backend is implemented (object storage state is phase 3)", v.checkpoint);
    }
    parse_duration(v.epoch_interval)?;
    if v.max_rows == 0 {
        bail!("max_rows must be at least 1");
    }
    if let Some(s) = v.start.filter(|s| !matches!(*s, "earliest" | "latest")) {
        bail!("start {s:?}: expected 'earliest' or 'latest'");
    }
    let existing = job_status(conn, v.job_id)?;
    match (existing, v.if_exists) {
        (Some(_), IfExists::Fail) => bail!(
            "view {:?} already exists; update it in place with if_exists := 'update' (`--if-exists update`), or \
             replace it with CREATE OR REPLACE (`--replace`, `replace := true`)",
            v.job_id
        ),
        (Some(_), IfExists::Update) => return update_view(conn, v),
        _ => {}
    }
    let sources = load_sources(conn)?;
    let mut lake_inputs = resolve_lake_inputs(conn, v.job_id, v.sql, &sources)?;
    let plan = compile_new(conn, v.job_id, v.sql, &sources, &mut lake_inputs, v.upsert.as_ref())?;
    if v.late_data.is_some() && !plan.is_windowed() {
        bail!("only windowed views have late data; drop --late-data for a {} view", plan.kind_name());
    }
    // Tables the view joins to a Kafka source are lookups, read whole every epoch; only the streams are inputs.
    let streams = plan.sources();
    lake_inputs.retain(|l| streams.contains(&l.name));
    let start_snapshot = (!lake_inputs.is_empty()).then(|| crate::lake::current_snapshot(conn)).transpose()?;
    let start_commit_us = start_snapshot.map(|s| commit_us_at(conn, s)).transpose()?.flatten();
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
        backfill: !lake_inputs.is_empty() && v.start != Some("latest"),
        lake_floor: None,
        start_commit_us,
        lake_inputs,
        start_snapshot,
        source_columns: BTreeMap::new(),
        limits: v.limits,
        upsert: v.upsert.clone(),
    };
    let mut def = def;
    def.source_columns = def
        .plan
        .sources()
        .into_iter()
        .filter_map(|n| sources.get(&n).filter(|_| !def.lake_inputs.iter().any(|l| l.name == n)).map(|s| (n, s.options.columns.clone())))
        .collect();
    // Refuse at create time what the worker would refuse at start: operators fed rows they cannot handle.
    let circuit = crate::circuit::Circuit::compile(&def.view())?;
    let outcome = if existing.is_some() { Outcome::Replaced } else { Outcome::Created };
    match existing {
        None => {
            conn.execute_batch("BEGIN")?;
            let res = (|| -> Result<()> {
                create_tables(conn, &def, &circuit, &src_columns)?;
                conn.execute_batch(&format!(
                    "CREATE TABLE {lt} (owner_worker VARCHAR, lease_epoch BIGINT, last_epoch BIGINT);
                     INSERT INTO {lt} VALUES (NULL, 0, 0);",
                    lt = lease_table(v.job_id)
                ))?;
                conn.execute(
                    &format!(
                        "INSERT INTO {CTL}.jobs (job_id, definition, version, status, created_at, updated_at)
                         VALUES (?, ?, 1, 'created', now(), now())"
                    ),
                    [v.job_id, &serde_json::to_string(&def)?],
                )?;
                insert_dependencies(conn, &def)
            })();
            finish(conn, res)?;
        }
        Some(_) => replace_view(conn, &def, &circuit, &src_columns)?,
    }
    set_sink_inlining(conn, &def)?;
    Ok((def, outcome))
}

/// `IfExists::Update` on an existing view: compares the request with the stored definition.
fn update_view(conn: &Connection, v: &CreateView) -> Result<(JobDef, Outcome)> {
    let (old, _) = load_job(conn, v.job_id)?;
    let same_table = |a: &str, b: &str| -> Result<bool> {
        let (_, sa, ta) = crate::lake::qualified_table(a)?;
        let (_, sb, tb) = crate::lake::qualified_table(b)?;
        Ok(sa == sb && ta == tb)
    };
    let same_late = match (old.late_data.as_deref(), v.late_data) {
        (Some(a), Some(b)) => same_table(a, b)?,
        (a, b) => a.is_none() && b.is_none(),
    };
    let mut changed = Vec::new();
    if normalize_sql(&old.sql) != normalize_sql(v.sql) {
        changed.push("its query".to_string());
    }
    if !same_table(&old.sink, v.sink)? {
        changed.push(format!("its table ({} -> {})", old.sink, v.sink));
    }
    if !same_late {
        let show = |l: Option<&str>| l.unwrap_or("none").to_string();
        changed.push(format!("its late-data table ({} -> {})", show(old.late_data.as_deref()), show(v.late_data)));
    }
    if old.checkpoint != v.checkpoint {
        changed.push(format!("its checkpoint ({} -> {})", old.checkpoint, v.checkpoint));
    }
    if old.upsert != v.upsert {
        let show = |u: &Option<plan::UpsertSpec>| match u {
            Some(u) => format!(
                "key {}{}",
                u.key.join(", "),
                u.version.as_ref().map(|v| format!(", version {v}")).unwrap_or_default()
            ),
            None => "none".into(),
        };
        changed.push(format!("its upsert ({} -> {})", show(&old.upsert), show(&v.upsert)));
    }
    if old.inline_row_limit != v.inline_row_limit {
        changed.push(format!("inline_row_limit ({} -> {})", old.inline_row_limit, v.inline_row_limit));
    }
    // Only when the request names a start: a rerun that leaves it out keeps the view as it was made.
    if let Some(start) = v.start.filter(|_| !old.lake_inputs.is_empty()) {
        // A recomputed `latest` view backfills too, from its floor: the floor is what says it was made latest.
        let had = if old.floor().is_some() { "latest" } else { "earliest" };
        if start != had {
            changed.push(format!("its start ({had} -> {start})"));
        }
    }
    if !changed.is_empty() {
        bail!(
            "view {}: {} changed, and only a replace recomputes it: if_exists := 'replace' (`--replace`; dbt run \
             --full-refresh) recomputes it from the start",
            v.job_id,
            changed.join(", ")
        );
    }
    let limit = |old: Option<u64>, new: Option<u64>| (old != new).then_some(new.unwrap_or(0));
    let alter = AlterView {
        epoch_interval: (old.epoch_interval != v.epoch_interval).then(|| v.epoch_interval.to_string()),
        max_rows: (old.max_rows != v.max_rows).then_some(v.max_rows),
        max_state_rows: limit(old.limits.max_state_rows, v.limits.max_state_rows),
        max_late_rows: limit(old.limits.max_late_rows, v.limits.max_late_rows),
    };
    if alter.epoch_interval.is_none() && alter.max_rows.is_none() && alter.max_state_rows.is_none() && alter.max_late_rows.is_none() {
        return Ok((old, Outcome::Unchanged));
    }
    Ok((alter_view(conn, v.job_id, &alter)?, Outcome::Altered))
}

/// A query's text for comparing two definitions: runs of whitespace outside quotes become one space, and
/// surrounding whitespace and trailing semicolons go, so reformatting a model is not a change.
fn normalize_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut quote: Option<char> = None;
    let mut space = false;
    for c in sql.trim().trim_end_matches(|c: char| c == ';' || c.is_whitespace()).chars() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == q {
                    quote = None; // a doubled quote closes and reopens: same result
                }
            }
            None if c.is_whitespace() => space = true,
            None => {
                if space {
                    out.push(' ');
                    space = false;
                }
                if c == '\'' || c == '"' {
                    quote = Some(c);
                }
                out.push(c);
            }
        }
    }
    out
}

/// Creates a view's table, late-data table (unless it exists, with the columns this view writes) and state
/// table, inside the caller's transaction.
fn create_tables(conn: &Connection, def: &JobDef, circuit: &crate::circuit::Circuit, src_columns: &[(String, String)]) -> Result<()> {
    let sink = quote_table(&def.sink)?;
    let (_, sink_schema, sink_name) = crate::lake::qualified_table(&def.sink)?;
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
        conn.execute_batch(&format!("ALTER TABLE {sink} SET PARTITIONED BY (year(window_start), month(window_start), day(window_start))"))?;
    }
    if let Some(late) = &def.late_data {
        let (_, ls, lt) = crate::lake::qualified_table(late)?;
        conn.execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {LAKE}.{}", ident(&ls)))?;
        let mut cols: Vec<(String, String)> =
            src_columns.iter().filter(|(c, _)| !crate::circuit::codegen::is_window_column(c)).cloned().collect();
        cols.extend(
            [("window_start", "TIMESTAMP"), ("window_end", "TIMESTAMP"), ("_source_partition", "INTEGER"), ("_source_offset", "BIGINT"), ("_duckwake_epoch", "BIGINT")]
                .map(|(c, t)| (c.to_string(), t.to_string())),
        );
        // Over an input that updates and deletes, a late row is a change: +1 a row added to a closed window,
        // -1 one taken out of it (a late correction), which the window's row still counts.
        if def.plan.retracts() {
            cols.push((crate::circuit::ops::LATE_WEIGHT.to_string(), "INTEGER".to_string()));
        }
        if table_exists(conn, &ls, &lt)? {
            // Shared with another view, or kept from an earlier one: its rows must line up with this view's.
            let have: Vec<String> = crate::lake::describe(conn, &quote_table(late)?)?.into_iter().map(|(c, _)| c).collect();
            let want: Vec<String> = cols.iter().map(|(c, _)| c.clone()).collect();
            if have != want {
                bail!(
                    "late-data table {late} has columns ({}), but this view writes ({}): name another table",
                    have.join(", "),
                    want.join(", ")
                );
            }
        } else {
            let defs: Vec<String> = cols.iter().map(|(c, t)| format!("{} {t}", ident(c))).collect();
            conn.execute_batch(&format!("CREATE TABLE {} ({})", quote_table(late)?, defs.join(", ")))?;
        }
    }
    if def.plan.has_state() {
        let cols: Vec<String> = def.plan.state_columns.iter().map(|(c, t)| format!("{} {t}", ident(c))).collect();
        conn.execute_batch(&format!("CREATE TABLE {} ({})", state_table(&def.job_id), cols.join(", ")))?;
    }
    Ok(())
}

fn insert_dependencies(conn: &Connection, def: &JobDef) -> Result<()> {
    for l in &def.lake_inputs {
        conn.execute(
            &format!("INSERT INTO {CTL}.dependencies VALUES (?, ?, ?, ?)"),
            duckdb::params![def.job_id, l.name, l.table_id, l.view],
        )?;
    }
    Ok(())
}

/// `CREATE OR REPLACE`: swaps a view's definition in one transaction. The new definition starts over like a
/// new view (Kafka sources from their `start` position, lake inputs from now): its table, state, checkpoints
/// and dead letters are replaced, and so are the rows of a late-data table no other view writes. The view
/// keeps its status (a failed one becomes stopped), its worker and pin; its lease is bumped, so a worker
/// still running the old definition is fenced, then starts the new one. A view that other views read cannot
/// be replaced: their positions in its table would no longer mean anything.
fn replace_view(conn: &Connection, def: &JobDef, circuit: &crate::circuit::Circuit, src_columns: &[(String, String)]) -> Result<()> {
    let job = def.job_id.as_str();
    let readers = readers(conn, job)?;
    if !readers.is_empty() {
        bail!("view {job} is read by {}; drop those views before replacing it", readers.join(", "));
    }
    retry_conflicts(|| {
        let (old, status) = load_job(conn, job)?;
        // A late-data table is the user's and may be shared: only one this view alone writes is emptied.
        let late_alone = match &old.late_data {
            Some(l) => !all_jobs(conn)?.iter().any(|(id, d)| id != job && d.late_data.as_deref() == Some(l)),
            None => false,
        };
        let status = if status == JobStatus::Failed { JobStatus::Stopped } else { status };
        conn.execute_batch("BEGIN")?;
        let res = (|| -> Result<()> {
            conn.execute_batch(&format!(
                "DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {};",
                quote_table(&old.sink)?,
                state_table(job)
            ))?;
            // Its rows are the old definition's; the new one writes the same table again from the start.
            if let (Some(l), true) = (&old.late_data, late_alone && def.late_data == old.late_data) {
                conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", quote_table(l)?))?;
            }
            for t in ["checkpoints", "dead_letter", "dependencies"] {
                conn.execute(&format!("DELETE FROM {CTL}.{t} WHERE job_id = ?"), [job])?;
            }
            create_tables(conn, def, circuit, src_columns)?;
            conn.execute_batch(&format!("UPDATE {} SET lease_epoch = lease_epoch + 1, last_epoch = 0", lease_table(job)))?;
            conn.execute(
                &format!(
                    "UPDATE {CTL}.jobs SET definition = ?, version = version + 1, status = ?, updated_at = now() WHERE job_id = ?"
                ),
                [&serde_json::to_string(def)?, status.as_str(), job],
            )?;
            insert_dependencies(conn, def)
        })();
        finish(conn, res)
    })
}

/// `_commit_ts` at snapshot `at`, in UTC microseconds: the running maximum of `snapshot_time` up to it, the
/// times of snapshots maintenance expired included (`lake::commit_times_sql`, as every row gets it).
pub fn commit_us_at(conn: &Connection, at: i64) -> Result<Option<i64>> {
    Ok(conn.query_row(&format!("SELECT epoch_us(max(ts)) FROM ({})", crate::lake::commit_times_sql(at, &at.to_string())), [], |r| r.get(0))?)
}

/// A view that reads lake tables through their change feeds, and the snapshot it has read them up to.
#[derive(Debug, Clone, PartialEq)]
pub struct LakePosition {
    pub job: String,
    /// The last snapshot read (its start snapshot until its first commit), which its next epoch, `verify` and
    /// a retraction's lookup all read; None for a view waiting for its start snapshot after a cascaded reset.
    pub snapshot: Option<i64>,
    pub backfilling: bool,
}

/// The position of every view with lake inputs, whatever its status: maintenance keeps every snapshot from
/// the oldest of them on, the way a replication slot holds WAL.
pub fn lake_positions(conn: &Connection) -> Result<Vec<LakePosition>> {
    let mut latest: HashMap<String, serde_json::Value> = HashMap::new();
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT job_id, source_offsets::VARCHAR FROM {CTL}.checkpoints c
             WHERE epoch = (SELECT max(epoch) FROM {CTL}.checkpoints WHERE job_id = c.job_id)"
        ))?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            latest.insert(r.get(0)?, serde_json::from_str(&r.get::<_, String>(1)?)?);
        }
    }
    let mut out = Vec::new();
    for (job, def) in all_jobs(conn)?.into_iter().filter(|(_, d)| !d.lake_inputs.is_empty()) {
        let offsets = |l: &LakeInput, key: i32| latest.get(&job).and_then(|o| o.get(&l.name)).and_then(|o| o.get(key.to_string())).and_then(|v| v.as_i64());
        let read = def.lake_inputs.iter().filter_map(|l| offsets(l, crate::model::LAKE_POSITION)).min();
        let backfilling = match latest.get(&job) {
            Some(_) => def.lake_inputs.iter().any(|l| offsets(l, crate::model::LAKE_BACKFILL).is_some()),
            None => def.backfill,
        };
        out.push(LakePosition { job, snapshot: read.or(def.start_snapshot), backfilling });
    }
    Ok(out)
}

/// Every view with its definition.
fn all_jobs(conn: &Connection) -> Result<Vec<(String, JobDef)>> {
    let mut stmt = conn.prepare(&format!("SELECT job_id, definition::VARCHAR FROM {CTL}.jobs ORDER BY job_id"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    rows.map(|r| {
        let (id, d) = r?;
        Ok((id, serde_json::from_str(&d)?))
    })
    .collect()
}

/// Settings `alter_view` changes in place, without recomputing the view; None keeps the current value.
#[derive(Debug, Default, Clone)]
pub struct AlterView {
    pub epoch_interval: Option<String>,
    pub max_rows: Option<usize>,
    /// 0 removes the limit.
    pub max_state_rows: Option<u64>,
    /// 0 removes the limit.
    pub max_late_rows: Option<u64>,
}

/// Changes how a view runs, not what it computes: its epoch interval, rows per epoch and limits. A running
/// view's lease is bumped, so its worker starts it again with the new settings at its next refresh; its output
/// and position stay as they are.
pub fn alter_view(conn: &Connection, job: &str, a: &AlterView) -> Result<JobDef> {
    if let Some(i) = &a.epoch_interval {
        parse_duration(i)?;
    }
    if a.max_rows == Some(0) {
        bail!("max_rows must be at least 1");
    }
    retry_conflicts(|| {
        let (mut def, status) = load_job(conn, job)?;
        if let Some(i) = &a.epoch_interval {
            def.epoch_interval = i.clone();
        }
        if let Some(m) = a.max_rows {
            def.max_rows = m;
        }
        if let Some(m) = a.max_state_rows {
            def.limits.max_state_rows = (m > 0).then_some(m);
        }
        if let Some(m) = a.max_late_rows {
            def.limits.max_late_rows = (m > 0).then_some(m);
        }
        conn.execute_batch("BEGIN")?;
        let res = (|| -> Result<()> {
            conn.execute(
                &format!("UPDATE {CTL}.jobs SET definition = ?, version = version + 1, updated_at = now() WHERE job_id = ?"),
                [&serde_json::to_string(&def)?, job],
            )?;
            if status == JobStatus::Running {
                conn.execute_batch(&format!("UPDATE {} SET lease_epoch = lease_epoch + 1", lease_table(job)))?;
            }
            Ok(())
        })();
        finish(conn, res)?;
        Ok(def)
    })
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

pub(crate) fn finish(conn: &Connection, res: Result<()>) -> Result<()> {
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
    let def = view_def(&def)?.ok_or_else(|| {
        anyhow!("{job} is the ingest job of database source {job}, not a view: start, stop, pause and reset it, or drop-source it")
    })?;
    Ok((def, JobStatus::parse(&status)?))
}

/// The ingest job of a PostgreSQL source stores `{"ingest": <source>}` as its definition, not a view's.
pub fn ingest_definition(source: &str) -> String {
    serde_json::json!({ "ingest": source }).to_string()
}

/// A stored definition as a view's; None for an ingest job's.
fn view_def(def: &str) -> Result<Option<JobDef>> {
    let v: serde_json::Value = serde_json::from_str(def)?;
    if v.get("ingest").is_some() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(v)?))
}

/// The source a job ingests, when it is a PostgreSQL source's ingest job.
pub fn ingest_source(conn: &Connection, job: &str) -> Result<Option<String>> {
    let def = crate::lake::query_opt_string(
        conn,
        &format!("SELECT (SELECT definition->>'ingest' FROM {CTL}.jobs WHERE job_id = {} LIMIT 1)", lit(job)),
    )?;
    Ok(def)
}

pub fn source_kind(conn: &Connection, name: &str) -> Result<Option<String>> {
    crate::lake::query_opt_string(conn, &format!("SELECT (SELECT kind FROM {CTL}.sources WHERE name = {} LIMIT 1)", lit(name)))
}

/// The views that read any of `tables` (`lake.schema.table`): through the change feed, or as lookups.
pub fn views_reading_tables(conn: &Connection, tables: &[String]) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("SELECT job_id, definition::VARCHAR FROM {CTL}.jobs ORDER BY job_id"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    let norm = |t: &str| crate::lake::qualified_table(t).map(|(c, s, t)| format!("{c}.{s}.{t}")).unwrap_or_else(|_| t.to_string());
    let wanted: Vec<String> = tables.iter().map(|t| norm(t)).collect();
    let mut out = Vec::new();
    for row in rows {
        let (job, def) = row?;
        let Some(def) = view_def(&def)? else { continue };
        let reads = def.lake_inputs.iter().map(|l| norm(&l.table)).chain(def.plan.query.lookups.iter().chain(def.plan.union.iter().flat_map(|b| b.query.lookups.iter())).map(|l| norm(l)));
        if reads.into_iter().any(|t| wanted.contains(&t)) {
            out.push(job);
        }
    }
    Ok(out)
}

pub fn lake_table_exists(conn: &Connection, schema: &str, table: &str) -> Result<bool> {
    table_exists(conn, schema, table)
}

/// DuckLake inlining for a table this module did not create as a view's sink (a mirror table).
pub fn set_table_inlining(conn: &Connection, schema: &str, table: &str, rows: usize) -> Result<()> {
    set_inlining(conn, schema, table, rows)
}

/// A running view and where it runs.
#[derive(Debug, Clone, PartialEq)]
pub struct RunningJob {
    pub id: String,
    /// The lease owner; None while unassigned.
    pub owner: Option<String>,
    pub lease: i64,
    /// The worker `start --worker` pinned it to: never moved by rebalancing, and moved back there after a
    /// failover once that worker is live again.
    pub pinned_to: Option<String>,
}

/// Running jobs, their current lease owner and pin.
pub fn running_jobs(conn: &Connection) -> Result<Vec<RunningJob>> {
    let mut stmt =
        conn.prepare(&format!("SELECT job_id, pinned_to FROM {CTL}.jobs WHERE status = 'running' ORDER BY job_id"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)))?.collect::<duckdb::Result<Vec<_>>>()?;
    let mut out = Vec::new();
    for (id, pinned_to) in rows {
        let (owner, lease) = lease(conn, &id)?;
        out.push(RunningJob { id, owner, lease, pinned_to });
    }
    Ok(out)
}

/// The sources (Kafka sources, lake tables, views) each running view reads, by view: workers that already
/// read a source are the better place for another view of it (one reader serves both).
pub fn running_sources(conn: &Connection) -> Result<HashMap<String, Vec<String>>> {
    let mut stmt = conn.prepare(&format!("SELECT job_id, definition::VARCHAR FROM {CTL}.jobs WHERE status = 'running'"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?.collect::<duckdb::Result<Vec<_>>>()?;
    let mut out = HashMap::new();
    for (id, def) in rows {
        // A definition this version cannot read only loses its affinity, not the rebalance.
        if let Ok(def) = serde_json::from_str::<JobDef>(&def) {
            out.insert(id, def.plan.sources());
        }
    }
    Ok(out)
}

pub fn lease(conn: &Connection, job: &str) -> Result<(Option<String>, i64)> {
    Ok(conn.query_row(&format!("SELECT owner_worker, lease_epoch FROM {}", lease_table(job)), [], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?)
}

/// Bumps the lease epoch for a job this worker owns, fencing any older incarnation of the worker. None, changing
/// nothing, unless the view is running: a worker starts views from a list of running ones it read a moment
/// before, and a `pause` or `stop` committed since fences the view by bumping its lease. Taking a fresh lease
/// then would undo that fence, so the paused view's epochs would keep landing until the worker next looked;
/// after `stop` and a `reset` or `drop`, they would land on top of those. Check and bump are one transaction:
/// a `pause` committed while it is open conflicts with the bump (both write the lease table) and wins.
pub fn take_lease(conn: &Connection, job: &str, worker: &str) -> Result<Option<i64>> {
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<Option<i64>> {
        if job_status(conn, job)? != Some(JobStatus::Running) {
            return Ok(None);
        }
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
        Ok(None) => {
            conn.execute_batch("ROLLBACK")?;
            Ok(None)
        }
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

/// Where `start` puts a view.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Assign<'a> {
    /// On this worker, and kept there: rebalancing never moves it, and after a failover it goes back once the
    /// worker is live again.
    Pin(&'a str),
    /// On this worker for now; rebalancing may move it.
    Prefer(&'a str),
    /// Unassigned: the next live worker to claim it takes it (see `failover`).
    Any,
}

/// Whether `worker` still holds `job` at lease epoch `held`: the view exists and neither a stop, pause, reset,
/// replace nor takeover has moved its lease. A worker asks after a view's epoch fails to stage: a view dropped or
/// replaced while it staged fails on its own missing tables, and that is not the worker's error.
pub fn holds_lease(conn: &Connection, job: &str, worker: &str, held: i64) -> Result<bool> {
    if job_status(conn, job)?.is_none() {
        return Ok(false);
    }
    let (owner, lease_epoch) = lease(conn, job)?;
    Ok(owner.as_deref() == Some(worker) && lease_epoch == held)
}

/// Marks `job` failed for `worker`, and fences its lease. With `held`, only while the worker holds the lease at
/// that epoch; without (a view that failed to start, whose lease the start may have taken), only while the view
/// is running and the worker owns it. Returns false, changing nothing, when the view was dropped or has moved
/// on: reset, replaced, stopped or taken over meanwhile, and its new incarnation must not inherit an error its
/// old one hit (a reset's deletes reaching an insert-only reader that is about to be recomputed, say). A view
/// dropped meanwhile is not an error: the worker that failed it carries on with its other views. Check and
/// update are one transaction, so a concurrent reset either lands first (and this returns false on retry) or
/// after (and finds the view failed).
pub fn fail_if_held(conn: &Connection, job: &str, worker: &str, held: Option<i64>) -> Result<bool> {
    retry_conflicts(|| {
        conn.execute_batch("BEGIN")?;
        let res = (|| -> Result<bool> {
            // Dropped meanwhile (with its lease table): there is no view left to mark.
            let Some(status) = job_status(conn, job)? else { return Ok(false) };
            if held.is_none() && status != JobStatus::Running {
                return Ok(false);
            }
            let (owner, lease_epoch) = lease(conn, job)?;
            if owner.as_deref() != Some(worker) || held.is_some_and(|h| h != lease_epoch) {
                return Ok(false);
            }
            conn.execute(
                &format!("UPDATE {CTL}.jobs SET status = ?, updated_at = now() WHERE job_id = ?"),
                [JobStatus::Failed.as_str(), job],
            )?;
            conn.execute_batch(&format!("UPDATE {} SET lease_epoch = lease_epoch + 1", lease_table(job)))?;
            Ok(true)
        })();
        match res {
            Ok(true) => {
                conn.execute_batch("COMMIT")?;
                Ok(true)
            }
            Ok(false) => {
                conn.execute_batch("ROLLBACK")?;
                Ok(false)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    })
}

/// `start` assigns the job (`Some(assign)`) and bumps the lease, which fences the previous owner's next commit.
pub fn set_status(conn: &Connection, job: &str, status: JobStatus, assign: Option<Assign>) -> Result<()> {
    job_status(conn, job)?.ok_or_else(|| anyhow!("no such view {job:?}"))?;
    if let Some(Assign::Pin(w) | Assign::Prefer(w)) = assign {
        crate::failover::check_worker_id(w)?;
    }
    retry_conflicts(|| set_status_once(conn, job, status, assign))
}

fn set_status_once(conn: &Connection, job: &str, status: JobStatus, assign: Option<Assign>) -> Result<()> {
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        conn.execute(
            &format!("UPDATE {CTL}.jobs SET status = ?, updated_at = now() WHERE job_id = ?"),
            [status.as_str(), job],
        )?;
        if assign.is_none() && status != JobStatus::Running {
            // Fence the owner: an epoch it has in flight must not land after the view paused or stopped (a
            // `drop` or `reset` may follow at once).
            conn.execute_batch(&format!("UPDATE {} SET lease_epoch = lease_epoch + 1", lease_table(job)))?;
        }
        if let Some(a) = assign {
            let (owner, pin) = match a {
                Assign::Pin(w) => (Some(w), Some(w)),
                Assign::Prefer(w) => (Some(w), None),
                Assign::Any => (None, None),
            };
            conn.execute(&format!("UPDATE {CTL}.jobs SET pinned_to = ? WHERE job_id = ?"), duckdb::params![pin, job])?;
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
///
/// The views that read its table and take its changes (retractions) follow the reset through the change
/// feed. Those that read a table changed by the reset insert-only, or window it, directly or through other
/// views, are recomputed in the same transaction instead (`recompute_view`) and returned.
pub fn reset(conn: &Connection, job: &str, to_epoch: i64) -> Result<Vec<String>> {
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
                    lit(&format!("$.jobs.\"{job}\""))
                ),
                [to_epoch],
                |r| r.get::<_, Option<i64>>(0),
            )?
            .ok_or_else(|| anyhow!("no snapshot for {job} epoch {to_epoch}: it was never committed or has been expired"))?;
        Some(s)
    } else {
        None
    };
    // The snapshots its lake inputs resume from must still be there; maintenance keeps only those from the
    // views' newest checkpoints on. Checked again in the reset's transaction and after it commits, since
    // maintenance on another worker may expire them meanwhile.
    let positions = reset_positions(conn, job, &def, to_epoch)?;
    check_positions(conn, job, to_epoch, &positions)?;
    let recompute = insert_only_readers(conn, job)?;
    // Its deletes from the shared control tables can lose to a running job's checkpoint insert.
    retry_conflicts(|| reset_once(conn, job, &def, to_epoch, snapshot, &recompute, &positions))?;
    check_positions(conn, job, to_epoch, &positions).context(
        "the reset committed, but maintenance expired that snapshot while it ran: reset the view again, to a newer \
         epoch or to epoch 0 (which recomputes it from its inputs' current rows)",
    )?;
    Ok(recompute)
}

/// Where a reset of `job` to `to_epoch` leaves each lake input: its position in the newest checkpoint left,
/// as `(table, snapshot)`. None for epoch 0, which starts the view over from the snapshot current at its
/// next start.
fn reset_positions(conn: &Connection, job: &str, def: &JobDef, to_epoch: i64) -> Result<Vec<(String, i64)>> {
    if to_epoch == 0 {
        return Ok(vec![]);
    }
    let mut out = Vec::new();
    for l in &def.lake_inputs {
        let ck = checkpoints(conn, job, &l.name, &format!("AND epoch <= {to_epoch} ORDER BY epoch DESC LIMIT 1"))?.pop();
        if let Some(p) = ck.and_then(|c| c.offsets.get(&crate::model::LAKE_POSITION).copied()).or(def.start_snapshot) {
            out.push((l.table.clone(), p));
        }
    }
    Ok(out)
}

fn check_positions(conn: &Connection, job: &str, to_epoch: i64, positions: &[(String, i64)]) -> Result<()> {
    for (table, p) in positions {
        if !crate::lake::snapshot_exists(conn, *p)? {
            bail!(
                "view {job}: at epoch {to_epoch} it had read {table} up to snapshot {p}, which has expired, so the \
                 changes after it are gone. Reset it to a newer epoch, or to epoch 0 to recompute it from its \
                 inputs' current rows"
            );
        }
    }
    Ok(())
}

/// Makes a view with lake inputs start over: its next start takes the snapshot current then and backfills
/// from it, keeping a `start := 'latest'` view to the rows written after the snapshot it was created at.
fn start_over(def: &mut JobDef) {
    def.lake_floor = def.floor();
    def.backfill = true;
    def.start_snapshot = None;
    def.start_commit_us = None;
}

/// The views a reset of `job` changes the input of, that read that input insert-only or window it: among
/// `job`'s readers and their readers in turn (every one of whose tables can lose rows), each with such an input
/// from `job` or from one of those readers. A windowed view takes retractions (C4), but a reset rewrites rows of
/// windows it has closed, which it would only take as late corrections: recomputing it keeps it exact.
fn insert_only_readers(conn: &Connection, job: &str) -> Result<Vec<String>> {
    let mut changed = vec![job.to_string()];
    let mut i = 0;
    while i < changed.len() {
        for r in readers(conn, &changed[i].clone())? {
            if !changed.contains(&r) {
                changed.push(r);
            }
        }
        i += 1;
    }
    let mut out = Vec::new();
    for r in changed.iter().skip(1) {
        let (def, _) = load_job(conn, r)?;
        let windowed = def.plan.is_windowed();
        if def.lake_inputs.iter().any(|l| (!l.retracts || windowed) && l.view.as_ref().is_some_and(|v| changed.contains(v))) {
            out.push(r.clone());
        }
    }
    Ok(out)
}

/// Starts view `job` over, in the caller's transaction: its table is emptied (keeping its DuckLake table id,
/// so the views that read it go on and see the deletes), its state, checkpoints and dead letters go, and so
/// do the rows of a late-data table it alone writes. Its lease is bumped, so a worker still running it is
/// fenced and starts it again. It then backfills from its inputs' rows at the snapshot current at that start.
fn recompute_view(conn: &Connection, job: &str) -> Result<()> {
    let (mut def, _) = load_job(conn, job)?;
    conn.execute_batch(&format!("DELETE FROM {}", quote_table(&def.sink)?))?;
    if def.plan.has_state() {
        conn.execute_batch(&format!("DELETE FROM {}", state_table(job)))?;
    }
    if let Some(l) = &def.late_data {
        if !all_jobs(conn)?.iter().any(|(id, d)| id != job && d.late_data.as_deref() == Some(l)) {
            conn.execute_batch(&format!("DELETE FROM {}", quote_table(l)?))?;
        }
    }
    for t in ["checkpoints", "dead_letter"] {
        conn.execute(&format!("DELETE FROM {CTL}.{t} WHERE job_id = ?"), [job])?;
    }
    conn.execute_batch(&format!("UPDATE {} SET lease_epoch = lease_epoch + 1, last_epoch = 0", lease_table(job)))?;
    start_over(&mut def);
    conn.execute(
        &format!("UPDATE {CTL}.jobs SET definition = ?, version = version + 1, updated_at = now() WHERE job_id = ?"),
        [&serde_json::to_string(&def)?, job],
    )?;
    Ok(())
}

/// A view recomputed by a cascaded reset has no start snapshot until a worker starts it: takes the current
/// one now, and stores it with the view, so every later start and `verify` read the same.
pub fn take_start_snapshot(conn: &Connection, job: &str) -> Result<JobDef> {
    retry_conflicts(|| {
        let (mut def, _) = load_job(conn, job)?;
        if def.start_snapshot.is_some() || def.lake_inputs.is_empty() {
            return Ok(def);
        }
        let s = crate::lake::current_snapshot(conn)?;
        def.start_snapshot = Some(s);
        def.start_commit_us = commit_us_at(conn, s)?;
        conn.execute(&format!("UPDATE {CTL}.jobs SET definition = ?, updated_at = now() WHERE job_id = ?"), [&serde_json::to_string(&def)?, job])?;
        Ok(def)
    })
}

#[allow(clippy::too_many_arguments)]
fn reset_once(
    conn: &Connection,
    job: &str,
    def: &JobDef,
    to_epoch: i64,
    snapshot: Option<i64>,
    recompute: &[String],
    positions: &[(String, i64)],
) -> Result<()> {
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        check_positions(conn, job, to_epoch, positions)?;
        for r in recompute {
            recompute_view(conn, r)?;
        }
        // Back to epoch 0, a view with lake inputs starts over from the snapshot current at its next start:
        // its first start snapshot may be long expired.
        if to_epoch == 0 && !def.lake_inputs.is_empty() {
            let mut d = def.clone();
            start_over(&mut d);
            conn.execute(
                &format!("UPDATE {CTL}.jobs SET definition = ?, version = version + 1, updated_at = now() WHERE job_id = ?"),
                [&serde_json::to_string(&d)?, job],
            )?;
        }
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

/// Removes a Kafka source no view reads from. A database source is dropped by `sources::drop`, with its mirrors.
pub fn drop_source(conn: &Connection, name: &str) -> Result<()> {
    if let Some(k) = source_kind(conn, name)?.filter(|k| k != crate::sources::kafka::KIND) {
        bail!("{name:?} is a {k} source: drop it with drop-source (duckwake_drop_source), which also drops its mirror tables");
    }
    let mut stmt = conn.prepare(&format!("SELECT job_id, definition::VARCHAR FROM {CTL}.jobs"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (job, def) = row?;
        let Some(def) = view_def(&def)? else { continue };
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
         waits AS ({}),
         last AS (SELECT * FROM {CTL}.checkpoints QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY epoch DESC) = 1),
         totals AS (SELECT job_id, sum(rows_in) AS rows_in, sum(rows_out) AS rows_out, sum(rows_late) AS late, sum(rows_dead) AS dead
                    FROM {CTL}.checkpoints GROUP BY ALL)
         SELECT j.job_id, j.status, l.owner_worker AS worker, l.lease_epoch AS lease, c.epoch, c.watermark,
                round(epoch(now()::TIMESTAMP - c.max_event_time), 1) AS freshness_s, c.lag,
                t.rows_in, t.rows_out, t.late, t.dead, c.committed_at::TIMESTAMP AS last_commit, j.pinned_to,
                json_extract_string(w.waiting, '$.\"' || j.job_id || '\"') AS waiting_for
         FROM {CTL}.jobs j LEFT JOIN leases l USING (job_id) LEFT JOIN last c USING (job_id) LEFT JOIN totals t USING (job_id)
           LEFT JOIN waits w ON w.worker_id = l.owner_worker AND j.status = 'running'
         ORDER BY j.job_id",
        leases.join(" UNION ALL "),
        crate::failover::waiting_sql(conn)?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reformatting_a_query_is_not_a_change() {
        let a = "SELECT game_id, count(*) AS n\n       FROM clicks   GROUP BY ALL;\n";
        let b = "  SELECT game_id, count(*) AS n FROM clicks GROUP BY ALL";
        assert_eq!(normalize_sql(a), normalize_sql(b));
        assert_ne!(normalize_sql(a), normalize_sql("SELECT game_id, count(*) AS n FROM clicks WHERE true GROUP BY ALL"));
    }

    #[test]
    fn whitespace_inside_quotes_is_kept() {
        assert_ne!(normalize_sql("SELECT 'a  b' AS x"), normalize_sql("SELECT 'a b' AS x"));
        assert_ne!(normalize_sql(r#"SELECT 1 AS "a  b""#), normalize_sql(r#"SELECT 1 AS "a b""#));
        assert_eq!(normalize_sql("SELECT 'it''s'   AS x"), "SELECT 'it''s' AS x");
        assert_eq!(normalize_sql("SELECT ';' AS x;;"), "SELECT ';' AS x");
    }

    #[test]
    fn a_view_is_named_schema_dot_table() {
        assert_eq!(view_names("analytics.daily").unwrap(), ("analytics.daily".into(), "lake.analytics.daily".into()));
        assert_eq!(view_names("lake.analytics.daily").unwrap(), ("analytics.daily".into(), "lake.analytics.daily".into()));
        assert_eq!(view_names("daily").unwrap(), ("main.daily".into(), "lake.main.daily".into()));
        assert!(view_names("other.analytics.daily").is_err());
        assert!(view_names("Analytics.daily").is_err());
        assert!(view_names("analytics.Daily").is_err());
        assert!(is_view_id("main.daily") && !is_view_id("shop"));
    }

    /// Views `analytics.daily`, `staging.daily` and `main.solo`, and the ingest job of database source `shop`.
    fn catalog_with_names() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "ATTACH ':memory:' AS {LAKE}; CREATE SCHEMA {CTL};
             CREATE TABLE {CTL}.jobs (job_id VARCHAR, definition JSON, version INTEGER, status VARCHAR,
               created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);
             INSERT INTO {CTL}.jobs VALUES
               ('analytics.daily', '{{}}', 1, 'running', now(), now()),
               ('staging.daily', '{{}}', 1, 'running', now(), now()),
               ('main.solo', '{{}}', 1, 'running', now(), now()),
               ('shop', '{{\"ingest\": \"shop\"}}', 1, 'running', now(), now());"
        ))
        .unwrap();
        conn
    }

    #[test]
    fn a_command_names_a_view_by_schema_or_by_a_table_name_only_one_schema_has() {
        let conn = catalog_with_names();
        assert_eq!(resolve_view(&conn, "analytics.daily").unwrap(), "analytics.daily");
        assert_eq!(resolve_view(&conn, "lake.staging.daily").unwrap(), "staging.daily");
        assert_eq!(resolve_view(&conn, "solo").unwrap(), "main.solo");
        assert_eq!(resolve_view(&conn, "shop").unwrap(), "shop");
        let e = resolve_view(&conn, "daily").unwrap_err().to_string();
        assert!(e.contains("analytics.daily, staging.daily"), "{e}");
        assert!(resolve_view(&conn, "other.daily").unwrap_err().to_string().contains("no view writes"));
        assert!(resolve_view(&conn, "nope").unwrap_err().to_string().contains("no view or database source"));
        // A table name that only ends like a view's is not that view.
        assert!(resolve_view(&conn, "aily").is_err());
    }

    #[test]
    fn a_name_in_a_views_sql_means_what_it_means_to_duckdb() {
        let conn = catalog_with_names();
        assert_eq!(view_read_by(&conn, "solo").unwrap().as_deref(), Some("main.solo"));
        assert_eq!(view_read_by(&conn, "staging.daily").unwrap().as_deref(), Some("staging.daily"));
        assert_eq!(view_read_by(&conn, "lake.analytics.daily").unwrap().as_deref(), Some("analytics.daily"));
        // Bare, it is main.daily, which is no view (a lake table, if anything).
        assert_eq!(view_read_by(&conn, "daily").unwrap(), None);
        assert_eq!(view_read_by(&conn, "shop").unwrap(), None);
    }

    #[test]
    fn a_lake_with_views_named_by_0_2_is_refused() {
        let conn = catalog_with_names();
        crate::lake::check_view_ids(&conn).unwrap();
        conn.execute_batch(&format!("INSERT INTO {CTL}.jobs VALUES ('orders', '{{}}', 1, 'running', now(), now())")).unwrap();
        let e = crate::lake::check_view_ids(&conn).unwrap_err().to_string();
        assert!(e.contains("views orders were created by DuckWake 0.2"), "{e}");
    }

    /// A control schema with one view `v`, running, owned by `w1` at lease epoch 3.
    fn catalog_with_view() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "ATTACH ':memory:' AS {LAKE}; CREATE SCHEMA {CTL};
             CREATE TABLE {CTL}.jobs (job_id VARCHAR, definition JSON, version INTEGER, status VARCHAR,
               created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);
             INSERT INTO {CTL}.jobs VALUES ('v', '{{}}', 1, 'running', now(), now());
             CREATE TABLE {} (owner_worker VARCHAR, lease_epoch BIGINT, last_epoch BIGINT);
             INSERT INTO {} VALUES ('w1', 3, 0);",
            lease_table("v"),
            lease_table("v")
        ))
        .unwrap();
        conn
    }

    /// A worker marks a view failed only while the view is still its own: a view dropped, stopped or taken over
    /// while it failed is let go, and a dropped one is not an error (the worker would exit and stop its other
    /// views: a dbt --full-refresh drops and creates views while workers start them).
    #[test]
    fn a_view_is_marked_failed_only_while_its_worker_holds_it() {
        let held = |conn: &Connection| (job_status(conn, "v").unwrap(), lease(conn, "v").unwrap().1);

        let conn = catalog_with_view();
        assert!(!fail_if_held(&conn, "v", "w2", None).unwrap(), "another worker's view");
        assert!(!fail_if_held(&conn, "v", "w1", Some(2)).unwrap(), "an older lease");
        assert_eq!(held(&conn), (Some(JobStatus::Running), 3));
        assert!(fail_if_held(&conn, "v", "w1", Some(3)).unwrap());
        assert_eq!(held(&conn), (Some(JobStatus::Failed), 4), "failed and fenced");

        // Failing to start: whatever lease the start took, while the view runs and is the worker's.
        let conn = catalog_with_view();
        assert!(fail_if_held(&conn, "v", "w1", None).unwrap());
        assert_eq!(held(&conn), (Some(JobStatus::Failed), 4));

        let conn = catalog_with_view();
        conn.execute_batch(&format!("UPDATE {CTL}.jobs SET status = 'stopped'")).unwrap();
        assert!(!fail_if_held(&conn, "v", "w1", None).unwrap(), "stopped while it started");
        assert_eq!(held(&conn), (Some(JobStatus::Stopped), 3));

        let conn = catalog_with_view();
        conn.execute_batch(&format!("DELETE FROM {CTL}.jobs; DROP TABLE {}", lease_table("v"))).unwrap();
        assert!(!fail_if_held(&conn, "v", "w1", None).unwrap(), "dropped while it started");
        assert!(!fail_if_held(&conn, "v", "w1", Some(3)).unwrap(), "dropped while it ran");
    }

    /// A worker starts a view from a list of running views read before; a pause committed since must stay in
    /// force, so the worker gets no lease and the paused view no epoch.
    #[test]
    fn a_lease_is_taken_only_while_the_view_runs() {
        let conn = catalog_with_view();
        assert_eq!(take_lease(&conn, "v", "w2").unwrap(), None, "another worker's view");
        assert_eq!(take_lease(&conn, "v", "w1").unwrap(), Some(4));
        for status in ["paused", "stopped", "failed"] {
            let conn = catalog_with_view();
            conn.execute_batch(&format!("UPDATE {CTL}.jobs SET status = '{status}'")).unwrap();
            assert_eq!(take_lease(&conn, "v", "w1").unwrap(), None, "{status} while the worker started it");
            assert_eq!(lease(&conn, "v").unwrap(), (Some("w1".into()), 3), "{status}: the lease is untouched");
        }
        let conn = catalog_with_view();
        conn.execute_batch(&format!("DELETE FROM {CTL}.jobs; DROP TABLE {}", lease_table("v"))).unwrap();
        assert_eq!(take_lease(&conn, "v", "w1").unwrap(), None, "dropped while the worker started it");
    }

    /// A view staged by a worker that no longer holds it (dropped, stopped, taken over) is not its error.
    #[test]
    fn a_worker_holds_a_view_only_at_its_own_lease() {
        let conn = catalog_with_view();
        assert!(holds_lease(&conn, "v", "w1", 3).unwrap());
        assert!(!holds_lease(&conn, "v", "w1", 2).unwrap(), "an older lease: fenced");
        assert!(!holds_lease(&conn, "v", "w2", 3).unwrap(), "another worker's view");
        conn.execute_batch(&format!("DELETE FROM {CTL}.jobs; DROP TABLE {}", lease_table("v"))).unwrap();
        assert!(!holds_lease(&conn, "v", "w1", 3).unwrap(), "dropped");
    }

    #[test]
    fn if_exists_parses() {
        assert_eq!(IfExists::parse("update").unwrap(), IfExists::Update);
        assert!(IfExists::parse("upsert").is_err());
        let both = crate::api::if_exists;
        assert_eq!(both(None, false).unwrap(), IfExists::Fail);
        assert_eq!(both(None, true).unwrap(), IfExists::Replace);
        assert_eq!(both(Some("replace"), true).unwrap(), IfExists::Replace);
        assert!(both(Some("update"), true).is_err());
    }
}
