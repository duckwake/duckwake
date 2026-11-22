//! The control data of database sources in `lake.__duckwake`: each one's row in `sources`, its ingest job in
//! `jobs` with a lease table, its checkpoints, and its mirror tables, all created, reset and dropped in one
//! DuckLake transaction each.
use super::mirror::MirrorTable;
use super::postgres;
use crate::catalog::{finish, ingest_definition, job_status, lease_table, retry_conflicts};
use crate::lake::{ident, lit, quote_table, CTL, LAKE};
use crate::model::JobStatus;
use anyhow::{anyhow, bail, Result};
use duckdb::Connection;

pub fn load_postgres(conn: &Connection, name: &str) -> Result<postgres::PgOptions> {
    Ok(serde_json::from_str(&load_options(conn, name, postgres::KIND)?)?)
}

/// The stored options of source `name` of `kind`, as JSON text.
pub fn load_options(conn: &Connection, name: &str, kind: &str) -> Result<String> {
    crate::lake::query_opt_string(
        conn,
        &format!("SELECT (SELECT options::VARCHAR FROM {CTL}.sources WHERE name = {} AND kind = {} LIMIT 1)", lit(name), lit(kind)),
    )?
    .ok_or_else(|| anyhow!("no {kind} source {name:?}"))
}

/// The mirror tables of a database source (PostgreSQL or MySQL), from whichever it is.
pub fn mirror_tables(conn: &Connection, name: &str) -> Result<Vec<MirrorTable>> {
    Ok(crate::sources::database::Spec::load(conn, name)?.tables().to_vec())
}

/// Registers a database source (PostgreSQL or MySQL) in one transaction: its mirror tables, the source and its
/// ingest job, created stopped like a view.
pub fn create(
    conn: &Connection,
    name: &str,
    kind: &str,
    options: &serde_json::Value,
    schema: &str,
    tables: &[MirrorTable],
) -> Result<()> {
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        conn.execute_batch(&format!("CREATE SCHEMA IF NOT EXISTS {LAKE}.{}", ident(schema)))?;
        for t in tables {
            let cols: Vec<String> = t.columns.iter().map(|c| format!("{} {}", ident(&c.name), c.duck_type)).collect();
            conn.execute_batch(&format!("CREATE TABLE {} ({})", quote_table(&t.mirror)?, cols.join(", ")))?;
        }
        conn.execute(
            &format!("INSERT INTO {CTL}.sources VALUES (?, ?, ?, now())"),
            [name, kind, &options.to_string()],
        )?;
        conn.execute_batch(&format!(
            "CREATE TABLE {lt} (owner_worker VARCHAR, lease_epoch BIGINT, last_epoch BIGINT);
             INSERT INTO {lt} VALUES (NULL, 0, 0);",
            lt = lease_table(name)
        ))?;
        conn.execute(
            &format!(
                "INSERT INTO {CTL}.jobs (job_id, definition, version, status, created_at, updated_at)
                 VALUES (?, ?, 1, 'created', now(), now())"
            ),
            [name, &ingest_definition(name)],
        )?;
        Ok(())
    })();
    finish(conn, res)
}

/// Removes a database source, its ingest job and its mirror tables in one transaction.
pub fn drop(conn: &Connection, name: &str, tables: &[MirrorTable]) -> Result<()> {
    retry_conflicts(|| {
        conn.execute_batch("BEGIN")?;
        let res = (|| -> Result<()> {
            for t in ["jobs", "checkpoints", "dead_letter"] {
                conn.execute(&format!("DELETE FROM {CTL}.{t} WHERE job_id = ?"), [name])?;
            }
            conn.execute(&format!("DELETE FROM {CTL}.sources WHERE name = ?"), [name])?;
            conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", lease_table(name)))?;
            for t in tables {
                conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", quote_table(&t.mirror)?))?;
            }
            Ok(())
        })();
        finish(conn, res)
    })
}

/// Starts a stopped database source over: its checkpoints and its mirror tables' rows go, in one transaction,
/// and its next start takes a new snapshot. The views that read the mirrors take the deletes and then the new
/// rows through the change feed.
pub fn reset(conn: &Connection, name: &str, tables: &[MirrorTable], to_epoch: i64) -> Result<()> {
    if job_status(conn, name)? == Some(JobStatus::Running) {
        bail!("pause or stop {name:?} before reset");
    }
    if to_epoch != 0 {
        bail!("a database source resets to epoch 0 only: its mirror tables start over from a new snapshot");
    }
    retry_conflicts(|| {
        conn.execute_batch("BEGIN")?;
        let res = (|| -> Result<()> {
            conn.execute(&format!("DELETE FROM {CTL}.checkpoints WHERE job_id = ?"), [name])?;
            for t in tables {
                conn.execute_batch(&format!("DELETE FROM {}", quote_table(&t.mirror)?))?;
            }
            Ok(())
        })();
        finish(conn, res)
    })
}
