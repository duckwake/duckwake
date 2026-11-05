//! The control API every front-end shares: the CLI, the `duckwake_*` SQL functions a server registers (and
//! serves over Quack), and the loadable extension. Views may be named by job id or by their table
//! (`analytics.clicks_per_min`). Each call runs on the caller's lake connection and returns one line for people.
use crate::catalog;
use crate::lake::{lit, CTL};
use crate::model::{JobDef, JobStatus};
use anyhow::{anyhow, Result};
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
}

pub fn create_source(conn: &Connection, name: &str, kind: &str, options: &str) -> Result<String> {
    let opts: serde_json::Value = serde_json::from_str(options).map_err(|e| anyhow::anyhow!("options must be JSON: {e}"))?;
    let def = catalog::create_source(conn, name, kind, &opts)?;
    Ok(format!("source {} -> topic {} ({} columns)", def.name, def.options.topic, def.options.columns.len()))
}

/// Creates a continuous view: the table `name` (a bare name means the `main` schema) and its job. `sink` is the
/// deprecated form that names the table separately, with `name` as the job id.
pub fn create_view(conn: &Connection, name: &str, sql: &str, o: &ViewOptions, sink: Option<&str>) -> Result<(JobDef, String)> {
    let (job_id, table) = catalog::view_names(name, sink)?;
    let def = catalog::create_view(
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
        },
    )?;
    let kind = match def.plan.kind_name() {
        "windowed" => "windowed aggregate, append sink",
        "aggregate" => "global aggregate, upsert sink",
        "distinct" => "distinct, append sink",
        _ => "stateless, append sink",
    };
    let msg = format!("view {}: {kind}, table {}", def.job_id, def.sink);
    Ok((def, msg))
}

/// Assigns the view to `worker` and bumps its lease, which fences the previous owner's next commit.
pub fn start(conn: &Connection, name: &str, worker: &str) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    catalog::set_status(conn, &job, JobStatus::Running, Some(worker))?;
    Ok(format!("{job}: running on {worker}"))
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
    catalog::reset(conn, &job, to_epoch)?;
    Ok(format!("{job}: reset to epoch {to_epoch}; start it to replay"))
}

/// Drops a stopped view and, unless `keep_table`, its table (a late-data table is kept).
pub fn drop(conn: &Connection, name: &str, keep_table: bool) -> Result<String> {
    let job = catalog::resolve_view(conn, name)?;
    catalog::drop_view(conn, &job, keep_table)?;
    Ok(format!("{job}: dropped{}", if keep_table { " (table kept)" } else { "" }))
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
    let (def, mut sources) = catalog::load_compiled(conn, &job)?;
    let source = sources.remove(&def.plan.source).ok_or_else(|| anyhow!("source of {job} is gone"))?;
    crate::circuit::explain(def.view(), &source.options.columns)
}
