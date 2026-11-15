//! The embedded DuckDB instance and the DuckLake it attaches.
//!
//! The worker never runs SQL on a host database: it owns an in-memory DuckDB, attaches the lake as `lake`,
//! and stages each epoch's intermediate tables in the in-memory `memory` catalog. A DuckDB transaction may
//! write to only one attached database, so staging happens before `BEGIN` and the commit writes only the lake.
use anyhow::{bail, Context, Result};
use duckdb::Connection;

pub use duckwake_circuit::sql::{describe, ident, lit, qualified_table, quote_table, CTL, LAKE};
/// Column every output table carries: the epoch that wrote the row.
pub const EPOCH_COL: &str = "_duckwake_epoch";

#[derive(Debug, Clone)]
pub struct LakeConfig {
    /// DuckLake metadata connection, e.g. `postgres:dbname=lake host=localhost port=55432 user=lake password=lake`.
    pub catalog: String,
    /// Where DuckLake writes Parquet files (local path or s3://...).
    pub data_path: String,
    /// DuckDB memory limit for this instance, e.g. `4GB`. Past it, DuckDB spills to `temp_dir`.
    pub memory_limit: Option<String>,
    /// Where DuckDB spills; a worker uses `<temp_dir>/<worker id>` (default `.tmp/<worker id>`).
    pub temp_dir: Option<String>,
    /// DuckDB threads for this instance (default: one per core). What an epoch needs in memory grows with them.
    pub threads: Option<usize>,
}

impl LakeConfig {
    pub fn open(&self) -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        // icu is needed for TIMESTAMPTZ arithmetic used by snapshot expiry; json for the view compiler.
        conn.execute_batch(
            "INSTALL ducklake; INSTALL postgres; INSTALL icu;
             LOAD ducklake; LOAD postgres; LOAD icu;
             SET TimeZone = 'UTC';",
        )
        .context("loading ducklake/postgres/icu extensions")?;
        if let Some(limit) = &self.memory_limit {
            conn.execute_batch(&format!("SET memory_limit = {}", lit(limit)))?;
        }
        if let Some(dir) = &self.temp_dir {
            conn.execute_batch(&format!("SET temp_directory = {}", lit(dir)))?;
        }
        if let Some(n) = self.threads {
            conn.execute_batch(&format!("SET threads = {}", n.max(1)))?;
        }
        if self.data_path.starts_with("s3://") {
            conn.execute_batch(&s3_secret_sql(&self.data_path, &|k| std::env::var(k).ok())?)
                .context("setting up S3 access for the data path")?;
        } else if self.data_path.contains("://") {
            bail!("data path {}: only local paths and s3:// are supported", self.data_path);
        } else {
            std::fs::create_dir_all(&self.data_path).with_context(|| format!("creating {}", self.data_path))?;
        }
        conn.execute_batch(&format!(
            "ATTACH {} AS {LAKE} (DATA_PATH {})",
            lit(&format!("ducklake:{}", self.catalog)),
            lit(&self.data_path)
        ))
        .context("attaching DuckLake")?;
        // Unqualified names in user SQL (`dim.games`) mean the lake, in the CLI, the compiler and the
        // worker alike. Engine-internal staging tables are always qualified with `memory.main`.
        conn.execute_batch(&format!("USE {LAKE}"))?;
        Ok(conn)
    }
}

/// Loads httpfs and creates a temporary (never stored) DuckDB secret for the data path's bucket from the
/// standard AWS variables of this process: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
/// `AWS_SESSION_TOKEN`, `AWS_REGION` (or `AWS_DEFAULT_REGION`) and `AWS_ENDPOINT_URL` (or
/// `AWS_ENDPOINT_URL_S3`) for S3-compatible stores such as MinIO or GCS's S3 API, which also switches to
/// path-style URLs unless `DUCKWAKE_S3_URL_STYLE` says `vhost`. Without keys, the AWS credential chain
/// (environment, config files, instance and container roles) through DuckDB's `aws` extension.
pub fn s3_secret_sql(data_path: &str, env: &dyn Fn(&str) -> Option<String>) -> Result<String> {
    let rest = data_path.trim_start_matches("s3://");
    let bucket = rest.split('/').next().filter(|b| !b.is_empty()).context("data path s3://BUCKET/... has no bucket")?;
    let mut opts = vec!["TYPE s3".to_string(), format!("SCOPE {}", lit(&format!("s3://{bucket}")))];
    let mut sql = String::from("INSTALL httpfs; LOAD httpfs;");
    match (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
        (Some(k), Some(v)) => {
            opts.push(format!("KEY_ID {}", lit(&k)));
            opts.push(format!("SECRET {}", lit(&v)));
            if let Some(t) = env("AWS_SESSION_TOKEN") {
                opts.push(format!("SESSION_TOKEN {}", lit(&t)));
            }
        }
        (None, None) => {
            sql.push_str(" INSTALL aws; LOAD aws;");
            opts.push("PROVIDER credential_chain".into());
        }
        _ => bail!("set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither (to use the AWS credential chain)"),
    }
    if let Some(r) = env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION")) {
        opts.push(format!("REGION {}", lit(&r)));
    }
    if let Some(url) = env("AWS_ENDPOINT_URL_S3").or_else(|| env("AWS_ENDPOINT_URL")) {
        let (ssl, host) = match url.split_once("://") {
            Some(("http", h)) => (false, h),
            Some(("https", h)) => (true, h),
            Some((s, _)) => bail!("AWS_ENDPOINT_URL {url:?}: scheme {s:?} is not http or https"),
            None => (true, url.as_str()),
        };
        opts.push(format!("ENDPOINT {}", lit(host.trim_end_matches('/'))));
        opts.push(format!("USE_SSL {ssl}"));
        let style = env("DUCKWAKE_S3_URL_STYLE").unwrap_or_else(|| "path".into());
        opts.push(format!("URL_STYLE {}", lit(&style)));
    }
    sql.push_str(&format!(" CREATE OR REPLACE TEMPORARY SECRET duckwake_lake ({});", opts.join(", ")));
    Ok(sql)
}

/// A PostgreSQL catalog connection whose sessions PostgreSQL ends when they sit idle inside a transaction for
/// longer than `timeout`. Every DuckLake commit inserts the next snapshot id, so a worker frozen inside its
/// commit (a paused VM, a stopped process, a half-open connection) holds a lock every other writer waits on;
/// ending its session rolls the commit back and frees the lake. Unchanged for other catalogs, and when the
/// string already passes `options` (libpq takes only one).
pub fn bound_idle_transactions(catalog: &str, timeout: std::time::Duration) -> String {
    if !catalog.starts_with("postgres:") || catalog.contains("options=") {
        return catalog.to_string();
    }
    format!("{catalog} options='-c idle_in_transaction_session_timeout={}'", timeout.as_millis())
}

/// Another connection to the instance behind `conn`: it shares the attached lake and the in-memory `memory`
/// catalog, so it reads and writes the same staging tables. Repeats the per-connection settings of `open`.
pub fn connect_again(conn: &Connection) -> Result<Connection> {
    let c = conn.try_clone()?;
    c.execute_batch(&format!("SET TimeZone = 'UTC'; USE {LAKE}"))?;
    Ok(c)
}

/// Creates the control schema. Per-job lease and state tables are created by `catalog::create_view`.
pub fn bootstrap(conn: &Connection) -> Result<()> {
    migrate_names(conn)?;
    conn.execute_batch(&format!(
        "CREATE SCHEMA IF NOT EXISTS {CTL};
         CREATE TABLE IF NOT EXISTS {CTL}.sources (
           name VARCHAR, kind VARCHAR, options JSON, created_at TIMESTAMPTZ);
         CREATE TABLE IF NOT EXISTS {CTL}.jobs (
           job_id VARCHAR, definition JSON, version INTEGER, status VARCHAR,
           created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);
         CREATE TABLE IF NOT EXISTS {CTL}.checkpoints (
           job_id VARCHAR, epoch BIGINT, source_offsets JSON, watermark TIMESTAMP,
           state_backend VARCHAR, state_ref VARCHAR,
           rows_in BIGINT, rows_out BIGINT, rows_late BIGINT, rows_dead BIGINT,
           max_event_time TIMESTAMP, lag BIGINT, worker VARCHAR, committed_at TIMESTAMPTZ, rows_sql BIGINT);
         CREATE TABLE IF NOT EXISTS {CTL}.dead_letter (
           job_id VARCHAR, epoch BIGINT, source VARCHAR, source_partition INTEGER, source_offset BIGINT,
           payload VARCHAR, error VARCHAR, failed_at TIMESTAMPTZ);
         CREATE TABLE IF NOT EXISTS {CTL}.dependencies (
           job_id VARCHAR, input VARCHAR, table_id BIGINT, upstream_job VARCHAR);
         CREATE TABLE IF NOT EXISTS {CTL}.snapshot_times (snapshot_id BIGINT, commit_ts TIMESTAMP);"
    ))
    .context("bootstrapping __duckwake schema")?;
    // Lakes bootstrapped before the Arrow decode lack `rows_sql` (messages decoded in SQL per epoch).
    add_column(conn, "checkpoints", "rows_sql", "BIGINT")?;
    // Before rebalancing: the worker a view is pinned to (`start --worker W`), NULL when it may move.
    add_column(conn, "jobs", "pinned_to", "VARCHAR")?;
    Ok(())
}

/// Adds a column to a control table unless it has it. Several processes may bootstrap the same lake at
/// once: one that loses the race finds the column there and goes on.
pub fn add_column(conn: &Connection, table: &str, column: &str, ty: &str) -> Result<()> {
    for attempt in 1..=5u64 {
        if has_column(conn, table, column)? {
            return Ok(());
        }
        match conn.execute_batch(&format!("ALTER TABLE {CTL}.{} ADD COLUMN {} {ty}", ident(table), ident(column))) {
            Ok(()) => return Ok(()),
            Err(_) if attempt < 5 => std::thread::sleep(std::time::Duration::from_millis(100 * attempt)),
            Err(e) => return Err(e).with_context(|| format!("adding {column} to {CTL}.{table}")),
        }
    }
    Ok(())
}

/// Whether control table `table` has `column`.
pub fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let schema = CTL.rsplit('.').next().unwrap_or(CTL);
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM duckdb_columns() WHERE database_name = ? AND schema_name = ? AND table_name = ? AND column_name = ?",
        [LAKE, schema, table, column],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

/// Lakes created before the rename keep their control data in `__stream` and stamp output rows with
/// `_stream_epoch`. Moves every control table to `__duckwake` (DuckDB cannot move a table between schemas,
/// so each is copied and dropped) and renames the epoch column of every view's output and late-data table,
/// in one transaction. Time travel on a moved state table starts at the migration, so `reset` cannot
/// restore state to an epoch committed before it.
fn migrate_names(conn: &Connection) -> Result<()> {
    let old: Vec<String> = {
        let mut stmt = conn.prepare(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = '__stream' ORDER BY table_name",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<duckdb::Result<Vec<_>>>()?
    };
    let schema_left: i64 = conn.query_row(
        "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'lake' AND schema_name = '__stream'",
        [],
        |r| r.get(0),
    )?;
    if old.is_empty() && schema_left == 0 {
        return Ok(());
    }
    let mut outputs: Vec<String> = Vec::new();
    if old.iter().any(|t| t == "jobs") {
        let mut stmt = conn.prepare(&format!("SELECT definition::VARCHAR FROM {LAKE}.__stream.jobs"))?;
        let defs = stmt.query_map([], |r| r.get::<_, String>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
        for d in defs {
            let def: serde_json::Value = serde_json::from_str(&d)?;
            outputs.extend(["sink", "late_data"].iter().filter_map(|k| def[*k].as_str().map(String::from)));
        }
    }
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        conn.execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {CTL}"))?;
        for t in &old {
            let t = ident(t);
            conn.execute_batch(&format!(
                "CREATE TABLE {CTL}.{t} AS SELECT * FROM {LAKE}.__stream.{t}; DROP TABLE {LAKE}.__stream.{t};"
            ))?;
        }
        conn.execute_batch(&format!("DROP SCHEMA {LAKE}.__stream"))?;
        for table in &outputs {
            let (_, schema, name) = qualified_table(table)?;
            let n: i64 = conn.query_row(
                "SELECT count(*) FROM duckdb_columns() WHERE database_name = 'lake' AND schema_name = ? AND table_name = ?
                   AND column_name = '_stream_epoch'",
                [&schema, &name],
                |r| r.get(0),
            )?;
            if n > 0 {
                conn.execute_batch(&format!("ALTER TABLE {} RENAME COLUMN _stream_epoch TO {EPOCH_COL}", quote_table(table)?))?;
            }
        }
        Ok(())
    })();
    match res {
        Ok(()) => conn.execute_batch("COMMIT").context("committing the __stream -> __duckwake migration")?,
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e.context("migrating __stream to __duckwake"));
        }
    }
    tracing::info!(tables = old.len(), outputs = outputs.len(), "migrated __stream to __duckwake");
    Ok(())
}

/// Job and source names become parts of table names, so keep them to a safe identifier alphabet.
pub fn check_name(kind: &str, name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 48
        && name.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !ok {
        bail!("{kind} name {name:?} must match [a-z][a-z0-9_]{{0,47}}");
    }
    Ok(())
}

/// Runs a single-value query and returns it as a string (NULL becomes None).
pub fn query_opt_string(conn: &Connection, sql: &str) -> Result<Option<String>> {
    Ok(conn.query_row(sql, [], |r| r.get::<_, Option<String>>(0))?)
}

/// The lake's newest committed snapshot id. Cheaper than `max(snapshot_id)` over `ducklake_snapshots`, and
/// it always shows what other processes have just committed (F0, internals item 9).
pub fn current_snapshot(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(&format!("SELECT id FROM ducklake_current_snapshot({})", lit(LAKE)), [], |r| r.get(0))?)
}

/// Whether snapshot `id` is still in the lake (not expired).
pub fn snapshot_exists(conn: &Connection, id: i64) -> Result<bool> {
    let n: i64 = conn.query_row(&format!("SELECT count(*) FROM ducklake_snapshots({}) WHERE snapshot_id = ?", lit(LAKE)), [id], |r| r.get(0))?;
    Ok(n > 0)
}

/// The oldest snapshot the lake still has.
pub fn oldest_snapshot(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(&format!("SELECT min(snapshot_id) FROM ducklake_snapshots({})", lit(LAKE)), [], |r| r.get(0))?)
}

/// A query giving `(snapshot_id, ts)` for every snapshot up to `upto` whose commit time is known: `ts` is
/// `_commit_ts`, the running maximum of `snapshot_time` by snapshot id in UTC. Snapshots still in the lake
/// come from DuckLake; expired ones from `snapshot_times`, where maintenance records each snapshot before it
/// expires it. `needed` is a query of the snapshot ids the caller will look up, so only those are read from
/// the record. A snapshot expired by anything other than DuckWake's maintenance has no time here.
pub fn commit_times_sql(upto: i64, needed: &str) -> String {
    format!(
        "SELECT snapshot_id, greatest(max(timezone('UTC', snapshot_time)) OVER (ORDER BY snapshot_id),
                (SELECT max(commit_ts) FROM {CTL}.snapshot_times)) AS ts
         FROM ducklake_snapshots({lake}) WHERE snapshot_id <= {upto}
         UNION ALL
         SELECT snapshot_id, max(commit_ts) AS ts FROM {CTL}.snapshot_times
         WHERE snapshot_id <= {upto} AND snapshot_id IN ({needed})
           AND snapshot_id NOT IN (SELECT snapshot_id FROM ducklake_snapshots({lake}))
         GROUP BY snapshot_id",
        lake = lit(LAKE)
    )
}

/// The DuckLake metadata schema of the lake, which holds `ducklake_table` and `ducklake_schema`.
fn metadata() -> String {
    format!("__ducklake_metadata_{LAKE}")
}

/// The id of a live lake table, or None when there is no such table.
pub fn table_id(conn: &Connection, schema: &str, table: &str) -> Result<Option<i64>> {
    let m = metadata();
    let mut stmt = conn.prepare(&format!(
        "SELECT t.table_id FROM {m}.ducklake_table t JOIN {m}.ducklake_schema s USING (schema_id)
         WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL AND s.schema_name = ? AND t.table_name = ?"
    ))?;
    let mut rows = stmt.query([schema, table])?;
    Ok(rows.next()?.map(|r| r.get(0)).transpose()?)
}

/// The current schema and name of a live lake table by id, or None once it has been dropped.
pub fn table_by_id(conn: &Connection, id: i64) -> Result<Option<(String, String)>> {
    let m = metadata();
    let mut stmt = conn.prepare(&format!(
        "SELECT s.schema_name, t.table_name FROM {m}.ducklake_table t JOIN {m}.ducklake_schema s USING (schema_id)
         WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL AND t.table_id = ?"
    ))?;
    let mut rows = stmt.query([id])?;
    Ok(rows.next()?.map(|r| Ok::<_, duckdb::Error>((r.get(0)?, r.get(1)?))).transpose()?)
}

/// True when a DuckLake commit failed because another transaction touched the same table.
pub fn is_conflict(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}");
    msg.contains("Transaction conflict") || msg.contains("conflict")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_secret_from_env() {
        let env = |vars: &'static [(&'static str, &'static str)]| {
            move |k: &str| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())
        };
        let minio = env(&[("AWS_ACCESS_KEY_ID", "k"), ("AWS_SECRET_ACCESS_KEY", "s"), ("AWS_ENDPOINT_URL", "http://localhost:19900/")]);
        let sql = s3_secret_sql("s3://duckwake/lake/", &minio).unwrap();
        assert!(sql.contains("SCOPE 's3://duckwake'"), "{sql}");
        assert!(sql.contains("ENDPOINT 'localhost:19900', USE_SSL false, URL_STYLE 'path'"), "{sql}");
        let chain = s3_secret_sql("s3://b", &env(&[("AWS_REGION", "eu-west-1")])).unwrap();
        assert!(chain.contains("PROVIDER credential_chain") && chain.contains("REGION 'eu-west-1'") && !chain.contains("ENDPOINT"), "{chain}");
        assert!(s3_secret_sql("s3://b", &env(&[("AWS_ACCESS_KEY_ID", "k")])).is_err());
        assert!(s3_secret_sql("s3:///x", &env(&[])).is_err());
    }

    #[test]
    fn idle_transactions_bounded_on_postgres_only() {
        let t = std::time::Duration::from_secs(30);
        assert_eq!(
            bound_idle_transactions("postgres:dbname=lake host=pg", t),
            "postgres:dbname=lake host=pg options='-c idle_in_transaction_session_timeout=30000'"
        );
        assert_eq!(bound_idle_transactions("postgres:dbname=lake options='-c x=1'", t), "postgres:dbname=lake options='-c x=1'");
        assert_eq!(bound_idle_transactions("sqlite:lake.db", t), "sqlite:lake.db");
    }
}
