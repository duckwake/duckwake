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
    /// DuckDB memory limit for this instance, e.g. `4GB`.
    pub memory_limit: Option<String>,
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
        if !self.data_path.contains("://") {
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
           job_id VARCHAR, input VARCHAR, table_id BIGINT, upstream_job VARCHAR);"
    ))
    .context("bootstrapping __duckwake schema")?;
    // Lakes bootstrapped before the Arrow decode lack `rows_sql` (messages decoded in SQL per epoch).
    let has_rows_sql: i64 = conn.query_row(
        "SELECT count(*) FROM duckdb_columns() WHERE database_name = 'lake' AND schema_name = '__duckwake'
           AND table_name = 'checkpoints' AND column_name = 'rows_sql'",
        [],
        |r| r.get(0),
    )?;
    if has_rows_sql == 0 {
        conn.execute_batch(&format!("ALTER TABLE {CTL}.checkpoints ADD COLUMN rows_sql BIGINT"))?;
    }
    Ok(())
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
