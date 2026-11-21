//! Ad-hoc queries over sources. A source is a Kafka topic, not a table, so a query that names one gets a
//! bounded snapshot: the newest events of the topic, decoded with the source's columns, as a temporary
//! table of the same name. It is read fresh for every query and never written to the lake; continuous
//! results are what views are for.
use crate::kafka;
use crate::lake::ident;
use crate::model::SourceDef;
use crate::decode::{decode_sql, IS_JSON_OBJECT};
use anyhow::Result;
use duckdb::Connection;
use std::collections::HashMap;
use tokio::runtime::Handle;

/// Events read per source and query, split across partitions.
pub const DEFAULT_TAIL: usize = 100_000;

/// Sources named in `sql` as whole, unqualified words (`clicks`, not `analytics.clicks`).
pub fn referenced_sources<'a>(sql: &str, sources: &'a HashMap<String, SourceDef>) -> Vec<&'a SourceDef> {
    let lower = sql.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out: Vec<&SourceDef> = sources
        .values()
        .filter(|s| {
            lower.match_indices(s.name.as_str()).any(|(i, m)| {
                let before = i.checked_sub(1).map(|j| bytes[j]);
                let after = bytes.get(i + m.len()).copied();
                !before.is_some_and(|b| is_word(b) || b == b'.' || b == b'"') && !after.is_some_and(is_word)
            })
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Replaces `temp.main.<source>` with the newest `limit` events: the source's columns plus `_partition`,
/// `_offset`, `_error` (why the event could not be decoded, else NULL: 'tombstone' for a message without a
/// value), `_payload` (the raw value) and `_key` (the message key).
/// Returns the number of events read.
pub fn load_snapshot(conn: &Connection, rt: &Handle, src: &SourceDef, limit: usize) -> Result<usize> {
    let msgs = rt.block_on(kafka::read_tail(&src.options, limit))?;
    let raw = format!("temp.main.{}", ident(&format!("__raw_{}", src.name)));
    let ok = format!("temp.main.{}", ident(&format!("__ok_{}", src.name)));
    conn.execute_batch(&format!("CREATE OR REPLACE TEMP TABLE {raw} (_partition INTEGER, _offset BIGINT, payload VARCHAR, _key VARCHAR)"))?;
    {
        let mut app = conn.appender_to_catalog_and_db(&format!("__raw_{}", src.name), "temp", "main")?;
        for m in &msgs {
            app.append_row(duckdb::params![m.partition, m.offset, m.payload_text().as_deref(), m.key_text()])?;
        }
        app.flush()?;
    }
    let cols: Vec<String> = src.options.columns.iter().map(|(c, _)| ident(c)).collect();
    let nulls: Vec<String> = src.options.columns.iter().map(|(c, t)| format!("NULL::{t} AS {}", ident(c))).collect();
    let name = format!("temp.main.{}", ident(&src.name));
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {ok} AS SELECT * FROM {raw} WHERE {IS_JSON_OBJECT};
         CREATE OR REPLACE TEMP TABLE {name} AS
           SELECT {cols}, _partition, _offset, _err AS _error, payload AS _payload, _key FROM ({decode})
           UNION ALL
           SELECT {nulls}, _partition, _offset,
                  CASE WHEN payload IS NULL THEN 'tombstone' ELSE 'invalid json' END AS _error, payload AS _payload,
                  {key} AS _key
           FROM {raw} WHERE NOT ({IS_JSON_OBJECT})
           ORDER BY _partition, _offset;
         DROP TABLE {raw}; DROP TABLE {ok};",
        cols = cols.join(", "),
        nulls = nulls.join(", "),
        decode = decode_sql(src, &ok),
        key = crate::decode::key_sql(src),
    ))?;
    Ok(msgs.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SourceOptions;
    use serde_json::json;

    #[test]
    fn finds_unqualified_source_names() {
        let opts = SourceOptions::from_json(&json!({"brokers": "x", "topic": "t", "event_time": "ts", "columns": {"ts": "TIMESTAMP"}})).unwrap();
        let src = |n: &str| (n.to_string(), SourceDef { name: n.into(), kind: "kafka".into(), options: opts.clone() });
        let sources = HashMap::from([src("clicks"), src("orders")]);
        let names = |sql: &str| referenced_sources(sql, &sources).iter().map(|s| s.name.clone()).collect::<Vec<_>>();
        assert_eq!(names("SELECT * FROM clicks c JOIN Orders o USING (id)"), ["clicks", "orders"]);
        assert!(names("SELECT * FROM analytics.clicks").is_empty());
        assert!(names("SELECT * FROM clicks_per_min, my_orders").is_empty());
    }
}
