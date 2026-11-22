//! The control API every front-end shares: the CLI, the `duckwake_*` SQL functions a server registers (and
//! serves over Quack), and the loadable extension. Views may be named by job id or by their table
//! (`analytics.clicks_per_min`). Each call runs on the caller's lake connection and returns one line for people.
use crate::catalog;
use crate::lake::{lit, CTL};
use crate::model::{JobDef, JobStatus};
use anyhow::Result;
use duckdb::Connection;

/// Options of `duckwake_create_view`; `None` takes the default.
#[derive(Debug, Default, Clone)]
pub struct ViewOptions {
    /// Table for rows that arrive after their window closed (windowed views).
    pub late_data: Option<String>,
    /// State backend: `ducklake` (the only one built).
    pub checkpoint: Option<String>,
    /// How often an epoch runs, e.g. `10s`.
    pub epoch_interval: Option<String>,
    /// Most events one epoch reads.
    pub max_rows: Option<usize>,
    /// Sink rows kept inlined in the catalog before they go to Parquet.
    pub inline_row_limit: Option<usize>,
    /// Views over lake tables or views: `earliest` (the default) or `latest` (`catalog::CreateView::start`).
    pub start: Option<String>,
    /// Most rows the view's state may hold, and most late rows one epoch may write (`model::Limits`).
    pub max_state_rows: Option<u64>,
    pub max_late_rows: Option<u64>,
    /// What to do when the view exists: refuse, update it in place, or replace it (`CREATE OR REPLACE`).
    pub if_exists: catalog::IfExists,
    /// Read the Kafka source as upserts, one row per key: the key columns, comma-separated, and the column whose
    /// higher value wins (`plan::UpsertSpec`).
    pub upsert_key: Option<String>,
    pub upsert_version: Option<String>,
}

impl ViewOptions {
    /// The upsert spec the options give, if any.
    pub fn upsert(&self) -> Result<Option<crate::plan::UpsertSpec>> {
        let Some(key) = self.upsert_key.as_deref().filter(|k| !k.trim().is_empty()) else {
            if self.upsert_version.is_some() {
                anyhow::bail!("upsert_version needs upsert_key, the column(s) that identify a row");
            }
            return Ok(None);
        };
        Ok(Some(crate::plan::UpsertSpec {
            key: key.split(',').map(|k| k.trim().to_string()).filter(|k| !k.is_empty()).collect(),
            version: self.upsert_version.clone().filter(|v| !v.trim().is_empty()),
        }))
    }
}

/// `if_exists := ...` and the older `replace := true` (`--if-exists`, `--replace`) as one setting.
pub fn if_exists(named: Option<&str>, replace: bool) -> Result<catalog::IfExists> {
    use catalog::IfExists;
    match (named.map(IfExists::parse).transpose()?, replace) {
        (None, false) => Ok(IfExists::Fail),
        (None, true) | (Some(IfExists::Replace), _) => Ok(IfExists::Replace),
        (Some(m), false) => Ok(m),
        (Some(m), true) => anyhow::bail!("replace conflicts with if_exists {m:?}; give one of them"),
    }
}

pub fn create_source(conn: &Connection, name: &str, kind: &str, options: &str) -> Result<String> {
    let opts: serde_json::Value = serde_json::from_str(options).map_err(|e| anyhow::anyhow!("options must be JSON: {e}"))?;
    if kind == crate::pgsource::KIND {
        let o = crate::pgsource::create(conn, name, &opts)?;
        let mirrors: Vec<String> = o.tables.iter().map(|t| format!("{} -> {}", t.source_name(), t.mirror)).collect();
        return Ok(format!(
            "source {name}: {} (slot {}, publication {}); start {name} to take the snapshot and follow the changes",
            mirrors.join(", "),
            o.slot,
            o.publication
        ));
    }
    if kind != "kafka" {
        anyhow::bail!("kind {kind:?}: expected 'kafka' or 'postgres'");
    }
    let def = catalog::create_source(conn, name, kind, &opts)?;
    Ok(format!("source {} -> topic {} ({} columns)", def.name, def.options.topic, def.options.columns.len()))
}

/// Creates a continuous view: the table `name` (a bare name means the `main` schema) and its job. `sink` is the
/// deprecated form that names the table separately, with `name` as the job id. The message starts with what
/// happened (`created`, `unchanged`, `altered` or `replaced`), for scripts and dbt to act on.
pub fn create_view(
    conn: &Connection,
    name: &str,
    sql: &str,
    o: &ViewOptions,
    sink: Option<&str>,
) -> Result<(JobDef, catalog::Outcome, String)> {
    let (job_id, table) = catalog::view_names(name, sink)?;
    let (def, outcome) = catalog::create_view(
        conn,
        &catalog::CreateView {
            job_id: &job_id,
            sql,
            sink: &table,
            late_data: o.late_data.as_deref(),
            checkpoint: o.checkpoint.as_deref().unwrap_or("ducklake"),
            epoch_interval: o.epoch_interval.as_deref().unwrap_or("10s"),
            max_rows: o.max_rows.unwrap_or(100_000),
            inline_row_limit: o.inline_row_limit.unwrap_or(10_000),
            limits: crate::model::Limits {
                max_state_rows: o.max_state_rows.filter(|m| *m > 0),
                max_late_rows: o.max_late_rows.filter(|m| *m > 0),
            },
            start: o.start.as_deref(),
            if_exists: o.if_exists,
            upsert: o.upsert()?,
        },
    )?;
    match outcome {
        catalog::Outcome::Unchanged => {
            let msg = format!("unchanged view {}", def.job_id);
            return Ok((def, outcome, msg));
        }
        catalog::Outcome::Altered => {
            let msg = format!("altered view {}", settings(&def));
            return Ok((def, outcome, msg));
        }
        _ => {}
    }
    let kind = match def.plan.kind_name() {
        "windowed" => "windowed aggregate",
        "aggregate" => "global aggregate",
        k => k,
    };
    let circuit = crate::circuit::Circuit::compile(&def.view())?;
    let sink = if circuit.counts_rows() {
        "counted sink"
    } else if circuit.upserts() {
        "upsert sink"
    } else {
        "append sink"
    };
    // A lake table a windowed view reads must stay insert-only: say so up front.
    let insert_only: Vec<&str> = def.lake_inputs.iter().filter(|l| !l.retracts && l.view.is_none()).map(|l| l.name.as_str()).collect();
    let insert_only =
        if insert_only.is_empty() { String::new() } else { format!(" ({} must stay insert-only)", insert_only.join(", ")) };
    let msg = format!("{} view {}: {kind}, {sink}{insert_only}, table {}", outcome.as_str(), def.job_id, def.sink);
    let msg = if outcome == catalog::Outcome::Replaced { format!("{msg}; it recomputes from the start") } else { msg };
    Ok((def, outcome, msg))
}

/// A view's settings, as `alter_view` and an altering `create_view` report them.
fn settings(def: &JobDef) -> String {
    let limit = |v: Option<u64>| v.map_or("none".to_string(), |n| n.to_string());
    format!(
        "{}: epoch_interval {}, max_rows {}, max_state_rows {}, max_late_rows {}",
        def.job_id,
        def.epoch_interval,
        def.max_rows,
        limit(def.limits.max_state_rows),
        limit(def.limits.max_late_rows)
    )
}

/// Adds a column to a source; the views that read it keep their columns.
pub fn add_source_column(conn: &Connection, source: &str, column: &str, ty: &str) -> Result<String> {
    let def = catalog::add_source_column(conn, source, column, ty)?;
    Ok(format!(
        "source {source}: added {column} {ty} ({} columns); views that read it keep theirs until replaced",
        def.options.columns.len()
    ))
}

/// Changes a view's epoch interval, rows per epoch or limits, without recomputing it.
pub fn alter_view(conn: &Connection, name: &str, a: &catalog::AlterView) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    Ok(settings(&catalog::alter_view(conn, &job, a)?))
}

/// Starts the view where `assign` says and bumps its lease, which fences the previous owner's next commit.
pub fn start(conn: &Connection, name: &str, assign: catalog::Assign) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    catalog::set_status(conn, &job, JobStatus::Running, Some(assign))?;
    Ok(match assign {
        catalog::Assign::Any => format!("{job}: running on the next live worker to claim it"),
        catalog::Assign::Prefer(w) => format!("{job}: running on {w}"),
        catalog::Assign::Pin(w) => format!("{job}: running on {w} (pinned)"),
    })
}

/// How the CLI and SQL name a placement: `any` (or nothing, in the CLI) floats; a worker id pins.
pub fn assign_named(worker: &str) -> catalog::Assign<'_> {
    if worker == crate::failover::ANY_WORKER {
        catalog::Assign::Any
    } else {
        catalog::Assign::Pin(worker)
    }
}

/// The query behind `duckwake_workers()`.
pub fn workers_sql(conn: &Connection) -> Result<String> {
    crate::failover::workers_query(conn)
}

/// Asks `worker` to hand all its views to the other workers and take none, before it is taken out.
pub fn drain(conn: &Connection, worker: &str) -> Result<String> {
    crate::failover::drain(conn, worker)?;
    Ok(format!("{worker}: draining; its views move to the other workers within a refresh or two"))
}

pub fn pause(conn: &Connection, name: &str) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    catalog::set_status(conn, &job, JobStatus::Paused, None)?;
    Ok(format!("{job}: paused"))
}

pub fn stop(conn: &Connection, name: &str) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    catalog::set_status(conn, &job, JobStatus::Stopped, None)?;
    Ok(format!("{job}: stopped"))
}

/// Rewinds a paused or stopped view to a checkpoint; starting it again replays from that checkpoint's offsets.
pub fn reset(conn: &Connection, name: &str, to_epoch: i64) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    if catalog::ingest_source(conn, &job)?.is_some() {
        let opts = catalog::load_postgres_source(conn, &job)?;
        catalog::reset_postgres_source(conn, &job, &opts, to_epoch)?;
        return Ok(format!("{job}: reset; its mirror tables are empty, and starting it takes a new snapshot"));
    }
    let recomputed = catalog::reset(conn, &job, to_epoch)?;
    let also = if recomputed.is_empty() {
        String::new()
    } else {
        format!("; {} read it insert-only or window it, and recompute from its table when they next start", recomputed.join(", "))
    };
    Ok(format!("{job}: reset to epoch {to_epoch}; start it to replay{also}"))
}

/// Drops a stopped view and, unless `keep_table`, its table (a late-data table is kept).
pub fn drop(conn: &Connection, name: &str, keep_table: bool) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    catalog::drop_view(conn, &job, keep_table)?;
    Ok(format!("{job}: dropped{}", if keep_table { " (table kept)" } else { "" }))
}

/// `verify` of a PostgreSQL source's ingest job: its mirror tables against the source tables now. None for a view.
pub fn verify_ingest(conn: &Connection, job: &str) -> Result<Option<(bool, String)>> {
    if catalog::ingest_source(conn, job)?.is_none() {
        return Ok(None);
    }
    let opts = catalog::load_postgres_source(conn, job)?;
    crate::pgsource::verify(conn, job, &opts).map(Some)
}

/// Drops a source. A PostgreSQL source goes with its ingest job, mirror tables and replication slot.
pub fn drop_source(conn: &Connection, name: &str) -> Result<String> {
    if catalog::source_kind(conn, name)?.as_deref() == Some(crate::pgsource::KIND) {
        let opts = catalog::load_postgres_source(conn, name)?;
        return crate::pgsource::drop(conn, name, &opts);
    }
    catalog::drop_source(conn, name)?;
    Ok(format!("source {name}: dropped"))
}

/// The query behind `duckwake_checkpoints(view)`: the newest `limit` checkpoints.
pub fn checkpoints_sql(conn: &Connection, name: &str, limit: usize) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    Ok(format!(
        "SELECT epoch, source_offsets, watermark, rows_in, rows_sql, rows_out, rows_late, rows_dead, lag, worker,
                committed_at::TIMESTAMP AS committed_at
         FROM {CTL}.checkpoints WHERE job_id = {} ORDER BY epoch DESC LIMIT {limit}",
        lit(&job)
    ))
}

/// The query behind `duckwake_sources()`.
pub fn sources_sql() -> String {
    format!(
        "SELECT name, kind, options->>'topic' AS topic, options->>'brokers' AS brokers, options->>'event_time' AS event_time,
                options->>'decode' AS decode, created_at::TIMESTAMP AS created_at
         FROM {CTL}.sources ORDER BY name"
    )
}

/// The view's circuit: the query, how it was made incremental, and the SQL of one epoch.
pub fn explain(conn: &Connection, name: &str) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    let (def, sources) = catalog::load_compiled(conn, &job)?;
    let columns: Vec<(String, Vec<(String, String)>)> = def
        .plan
        .sources()
        .into_iter()
        .map(|n| Ok((n.clone(), catalog::input_columns(&n, &sources, &def.lake_inputs)?.to_vec())))
        .collect::<Result<_>>()?;
    crate::circuit::explain(def.view(), &columns)
}
