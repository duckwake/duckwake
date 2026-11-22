//! Where views' rows come from, other than the lake itself (views over lake tables read DuckLake's change feed
//! in `engine`):
//!
//! - `kafka`: topics read by the views themselves, as JSON or Debezium change events (`kafka::decode`).
//! - `database`: PostgreSQL and MySQL tables, which an ingest job copies into mirror tables in the lake.
pub mod database;
pub mod kafka;

use crate::catalog;
use anyhow::{bail, Result};
use database::{mysql, postgres};
use duckdb::Connection;

/// Creates source `name` of `kind` (`kafka`, `postgres` or `mysql`) from its JSON options, and says what it made.
pub fn create(conn: &Connection, name: &str, kind: &str, options: &str) -> Result<String> {
    let opts: serde_json::Value = serde_json::from_str(options).map_err(|e| anyhow::anyhow!("options must be JSON: {e}"))?;
    let mirrors = |tables: &[database::mirror::MirrorTable]| -> String {
        tables.iter().map(|t| format!("{} -> {}", t.source_name(), t.mirror)).collect::<Vec<_>>().join(", ")
    };
    match kind {
        postgres::KIND => {
            let o = postgres::create(conn, name, &opts)?;
            Ok(format!(
                "source {name}: {} (slot {}, publication {}); start {name} to take the snapshot and follow the changes",
                mirrors(&o.tables),
                o.slot,
                o.publication
            ))
        }
        mysql::KIND => {
            let o = mysql::create(conn, name, &opts)?;
            Ok(format!(
                "source {name}: {} (binlog as replica {}); start {name} to take the snapshot and follow the changes",
                mirrors(&o.tables),
                o.server_id
            ))
        }
        kafka::KIND => {
            let def = catalog::create_source(conn, name, kind, &opts)?;
            Ok(format!("source {} -> topic {} ({} columns)", def.name, def.options.topic, def.options.columns.len()))
        }
        _ => bail!("kind {kind:?}: expected 'kafka', 'postgres' or 'mysql'"),
    }
}

/// Drops source `name`. A database source goes with its ingest job and mirror tables, and a PostgreSQL one with
/// its replication slot.
pub fn drop(conn: &Connection, name: &str) -> Result<String> {
    match catalog::source_kind(conn, name)?.as_deref() {
        Some(postgres::KIND) => postgres::drop(conn, name, &database::catalog::load_postgres(conn, name)?),
        Some(mysql::KIND) => mysql::drop(conn, name, &mysql::load(conn, name)?),
        _ => {
            catalog::drop_source(conn, name)?;
            Ok(format!("source {name}: dropped"))
        }
    }
}

/// Resets job `job` when it is a database source's ingest job (to epoch 0 only: a new snapshot); None for a view.
pub fn reset(conn: &Connection, job: &str, to_epoch: i64) -> Result<Option<String>> {
    if catalog::ingest_source(conn, job)?.is_none() {
        return Ok(None);
    }
    database::catalog::reset(conn, job, &database::catalog::mirror_tables(conn, job)?, to_epoch)?;
    Ok(Some(format!("{job}: reset; its mirror tables are empty, and starting it takes a new snapshot")))
}

/// `verify` of a database source's ingest job: its mirror tables against the source tables now. None for a view.
pub fn verify(conn: &Connection, job: &str) -> Result<Option<(bool, String)>> {
    if catalog::ingest_source(conn, job)?.is_none() {
        return Ok(None);
    }
    database::verify(conn, job).map(Some)
}
