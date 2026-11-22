//! Mirror tables: the lake copies of a database source's tables (PostgreSQL, `postgres`; MySQL, `mysql`),
//! which the source's ingest job (`super`) keeps up to date. Every value reaches a mirror as a text form and is
//! cast to its column's DuckDB type here, from a source's snapshot and from its change stream alike, so the two
//! paths agree.
use crate::lake::ident;
use anyhow::Result;
use duckdb::Connection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MirrorColumn {
    pub name: String,
    /// The column's type in the source (`format_type` in PostgreSQL, `COLUMN_TYPE` in MySQL).
    pub source_type: String,
    /// PostgreSQL's type oid and typmod; 0 and -1 for MySQL.
    #[serde(default)]
    pub oid: u32,
    #[serde(default)]
    pub typmod: i32,
    pub duck_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MirrorTable {
    /// The source table's schema (PostgreSQL) or database (MySQL), and name.
    pub schema: String,
    pub name: String,
    /// The mirror table, `lake.<schema>.<name>` (unquoted: names are `[a-z][a-z0-9_]*`).
    pub mirror: String,
    pub columns: Vec<MirrorColumn>,
    /// The primary key's columns, in key order.
    pub key: Vec<String>,
}

impl MirrorTable {
    pub fn source_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Positions of the key columns among `columns`.
    pub fn key_positions(&self) -> Vec<usize> {
        self.key.iter().map(|k| self.columns.iter().position(|c| &c.name == k).expect("key column")).collect()
    }
}

/// SQL turning `v`, a column's text form, into the mirror column's type. Binary values come as `\x` and hex
/// digits (PostgreSQL's `bytea_output = hex`; the MySQL reader writes them the same way).
pub fn cast_sql(c: &MirrorColumn, v: &str) -> String {
    match c.duck_type.as_str() {
        "VARCHAR" => v.to_string(),
        "BLOB" => format!("unhex(substr({v}, 3))"),
        t => format!("CAST({v} AS {t})"),
    }
}

/// Compares each mirror table with the rows of its source table staged in `staged(i)` (cast like the mirror).
/// Returns whether they are all equal, and what was found for each table.
pub fn compare(conn: &Connection, name: &str, tables: &[MirrorTable], staged: &dyn Fn(usize) -> String) -> Result<(bool, String)> {
    let mut ok = true;
    let mut parts = Vec::new();
    for (i, t) in tables.iter().enumerate() {
        let (src, mirror) = (staged(i), crate::lake::quote_table(&t.mirror)?);
        let keys: Vec<String> = t.key.iter().map(|k| ident(k)).collect();
        let diff = |a: &str, b: &str| -> Result<(i64, Vec<String>)> {
            let n: i64 = conn.query_row(&format!("SELECT count(*) FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b})"), [], |r| r.get(0))?;
            let mut stmt = conn.prepare(&format!(
                "SELECT ({})::VARCHAR FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b}) ORDER BY ALL LIMIT 3",
                if keys.len() == 1 { keys[0].clone() } else { format!("row({})", keys.join(", ")) }
            ))?;
            let sample = stmt.query_map([], |r| r.get::<_, String>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
            Ok((n, sample))
        };
        let (only_src, s1) = diff(&src, &mirror)?;
        let (only_mirror, s2) = diff(&mirror, &src)?;
        let rows: i64 = conn.query_row(&format!("SELECT count(*) FROM {src}"), [], |r| r.get(0))?;
        if only_src == 0 && only_mirror == 0 {
            parts.push(format!("{} {rows} rows", t.name));
        } else {
            ok = false;
            parts.push(format!(
                "{}: {only_src} rows only in the source (keys {}), {only_mirror} only in the mirror (keys {})",
                t.name,
                s1.join(", "),
                s2.join(", ")
            ));
        }
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {src}"))?;
    }
    Ok((
        ok,
        if ok {
            format!("VERIFY OK: {name}: mirror tables equal their source tables ({})", parts.join(", "))
        } else {
            format!("VERIFY FAILED: {name}: {} (changes still in flight show up here too)", parts.join("; "))
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn casts() {
        let c = |t: &str| MirrorColumn { name: "x".into(), source_type: String::new(), oid: 0, typmod: -1, duck_type: t.into() };
        assert_eq!(cast_sql(&c("VARCHAR"), "v"), "v");
        assert_eq!(cast_sql(&c("BLOB"), "v"), "unhex(substr(v, 3))");
        assert_eq!(cast_sql(&c("DATE"), "v"), "CAST(v AS DATE)");
    }
}
