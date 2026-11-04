//! SQL text helpers and the names the generated SQL uses.
use anyhow::{bail, Result};
use duckdb::Connection;

/// Catalog alias the lake is attached under. User SQL and sink names refer to it (`lake.analytics.t`).
pub const LAKE: &str = "lake";
/// Schema inside the lake that holds all control data.
pub const CTL: &str = "lake.__duckwake";

/// SQL string literal.
pub fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Quoted identifier.
pub fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Splits `lake.schema.table` (or `schema.table`, which is taken to be in the lake) into a fully quoted name.
pub fn qualified_table(name: &str) -> Result<(String, String, String)> {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.as_slice() {
        [c, s, t] if *c == LAKE => Ok((c.to_string(), s.to_string(), t.to_string())),
        [s, t] => Ok((LAKE.to_string(), s.to_string(), t.to_string())),
        _ => bail!("table {name:?} must be lake.<schema>.<table>"),
    }
}

pub fn quote_table(name: &str) -> Result<String> {
    let (c, s, t) = qualified_table(name)?;
    Ok(format!("{}.{}.{}", ident(&c), ident(&s), ident(&t)))
}

/// Column names and types of a query, via DESCRIBE.
pub fn describe(conn: &Connection, sql: &str) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(&format!("DESCRIBE {sql}"))?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

/// A view's state table.
pub fn state_table(job: &str) -> String {
    format!("{CTL}.{}", ident(&format!("state_{job}")))
}
