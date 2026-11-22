//! MySQL values as text: what the binlog carries (`binlog_text`) and what the snapshot reads with the binary
//! protocol in a UTC session (`snapshot_text`) become one text per column kind (`Kind`), which the mirror then
//! casts like every other (`mirror::cast_sql`). Unit tests check that the two sides agree.
use crate::sources::database::mirror::MirrorColumn;
use anyhow::{bail, Context, Result};
use mysql_async::binlog::jsonb::{JsonContainer, JsonDom, JsonNumber, JsonScalar};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;

/// How a column's values are written as text (`text`).
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Kind {
    Number,
    Float,
    Double,
    Text,
    Binary,
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,
    Enum(Vec<String>),
    Set(Vec<String>),
    Bit,
    Json,
}

/// The labels of an `enum('a','b')` or `set(...)` column type.
fn labels(column_type: &str) -> Vec<String> {
    let inner = column_type.split_once('(').map(|(_, r)| r.trim_end_matches(')')).unwrap_or("");
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, quoted) {
            ('\'', false) => quoted = true,
            ('\'', true) if chars.peek() == Some(&'\'') => {
                cur.push('\'');
                chars.next();
            }
            ('\'', true) => {
                quoted = false;
                out.push(std::mem::take(&mut cur));
            }
            (c, true) => cur.push(c),
            _ => {}
        }
    }
    out
}

/// The kind and DuckDB type of a column, from `information_schema.COLUMNS` (`DATA_TYPE`, `COLUMN_TYPE`).
pub(super) fn kind_and_type(data_type: &str, column_type: &str, precision: Option<u32>, scale: Option<u32>) -> (Kind, String) {
    let unsigned = column_type.contains("unsigned");
    let int = |s: &str, u: &str| (Kind::Number, if unsigned { u } else { s }.to_string());
    match data_type {
        "tinyint" => int("TINYINT", "UTINYINT"),
        "smallint" => int("SMALLINT", "USMALLINT"),
        "mediumint" | "int" => int("INTEGER", "UINTEGER"),
        "bigint" => int("BIGINT", "UBIGINT"),
        "float" => (Kind::Float, "FLOAT".into()),
        "double" => (Kind::Double, "DOUBLE".into()),
        "decimal" => match (precision, scale) {
            (Some(p), Some(s)) if (1..=38).contains(&p) => (Kind::Number, format!("DECIMAL({p},{s})")),
            _ => (Kind::Number, "DOUBLE".into()),
        },
        "date" => (Kind::Date, "DATE".into()),
        "datetime" => (Kind::DateTime, "TIMESTAMP".into()),
        "timestamp" => (Kind::Timestamp, "TIMESTAMPTZ".into()),
        // Up to 838 hours either way: DuckDB's TIME holds a day.
        "time" => (Kind::Time, "VARCHAR".into()),
        "year" => (Kind::Year, "SMALLINT".into()),
        "enum" => (Kind::Enum(labels(column_type)), "VARCHAR".into()),
        "set" => (Kind::Set(labels(column_type)), "VARCHAR".into()),
        "bit" => (Kind::Bit, "UBIGINT".into()),
        "json" => (Kind::Json, "JSON".into()),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => (Kind::Text, "VARCHAR".into()),
        _ => (Kind::Binary, "BLOB".into()),
    }
}

/// A column's kind from what the source stored about it (`source_type` holds `DATA_TYPE COLUMN_TYPE`).
pub(super) fn kind(c: &MirrorColumn) -> Kind {
    let (data_type, column_type) = c.source_type.split_once(' ').unwrap_or((&c.source_type, ""));
    let (precision, scale) = match c.duck_type.strip_prefix("DECIMAL(") {
        Some(r) => {
            let (p, s) = r.trim_end_matches(')').split_once(',').unwrap_or(("0", "0"));
            (p.parse().ok(), s.parse().ok())
        }
        None => (None, None),
    };
    kind_and_type(data_type, column_type, precision, scale).0
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + 2 * b.len());
    s.push_str("\\x");
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

fn utf8(b: &[u8]) -> Result<String> {
    String::from_utf8(b.to_vec()).context("a text value is not UTF-8: the column's character set must be utf8mb4")
}

/// A MySQL date and time: year, month, day, hour, minute, second, microsecond.
type DateParts = (u16, u8, u8, u8, u8, u8, u32);

fn date_text(k: &Kind, (y, mo, d, h, mi, s, us): DateParts) -> Option<String> {
    // MySQL's zero date has no DuckDB counterpart: NULL, as Debezium maps it.
    if y == 0 || mo == 0 || d == 0 {
        return None;
    }
    Some(match k {
        Kind::Date => format!("{y:04}-{mo:02}-{d:02}"),
        Kind::Timestamp => format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}+00"),
        _ => format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"),
    })
}

fn time_text(neg: bool, days: u32, h: u8, m: u8, s: u8, us: u32) -> String {
    let hours = days * 24 + h as u32;
    let frac = if us > 0 { format!(".{us:06}") } else { String::new() };
    format!("{}{hours:02}:{m:02}:{s:02}{frac}", if neg { "-" } else { "" })
}

/// JSON as one text whichever way it was read: object keys sorted (the binlog's order and the server's text
/// differ; JSON gives the order no meaning), numbers as JSON numbers.
fn json_text(v: serde_json::Value) -> String {
    fn canonical(v: serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(m) => {
                let mut entries: Vec<(String, serde_json::Value)> = m.into_iter().map(|(k, v)| (k, canonical(v))).collect();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                serde_json::Value::Object(entries.into_iter().collect())
            }
            serde_json::Value::Array(a) => serde_json::Value::Array(a.into_iter().map(canonical).collect()),
            v => v,
        }
    }
    canonical(v).to_string()
}

/// A binlog JSON document as serde_json, as the server's text would read: a DECIMAL is a number (the library
/// makes it a string), a date or time the server's text form.
fn dom_json(d: JsonDom) -> serde_json::Value {
    use serde_json::Value as J;
    match d {
        JsonDom::Container(JsonContainer::Array(a)) => J::Array(a.into_iter().map(dom_json).collect()),
        JsonDom::Container(JsonContainer::Object(m)) => J::Object(m.into_iter().map(|(k, v)| (k, dom_json(v))).collect()),
        JsonDom::Scalar(JsonScalar::Number(JsonNumber::Decimal(d))) => {
            d.to_string().parse::<f64>().ok().and_then(serde_json::Number::from_f64).map_or(J::Null, J::Number)
        }
        JsonDom::Scalar(s) => s.into(),
    }
}

/// A value as the snapshot reads it (binary protocol, session time zone UTC), as text.
pub(super) fn snapshot_text(k: &Kind, v: Value) -> Result<Option<String>> {
    Ok(match v {
        Value::NULL => None,
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Double(f) => Some(f.to_string()),
        Value::Date(y, mo, d, h, mi, s, us) => date_text(k, (y, mo, d, h, mi, s, us)),
        Value::Time(neg, days, h, m, s, us) => Some(time_text(neg, days, h, m, s, us)),
        Value::Bytes(b) => Some(match k {
            Kind::Binary => hex(&b),
            Kind::Bit => bit_value(&b).to_string(),
            Kind::Json => json_text(serde_json::from_slice(&b).context("a JSON value")?),
            _ => utf8(&b)?,
        }),
    })
}

fn bit_value(b: &[u8]) -> u64 {
    b.iter().fold(0u64, |acc, x| (acc << 8) | *x as u64)
}

/// A value as the binlog carries it, as text: the same text `snapshot_text` gives for the same value.
pub(super) fn binlog_text(k: &Kind, v: Option<&BinlogValue<'_>>) -> Result<Option<String>> {
    let Some(v) = v else { return Ok(None) };
    Ok(match v {
        BinlogValue::Jsonb(j) => Some(json_text(dom_json(j.clone().parse().context("a JSON value")?))),
        BinlogValue::JsonDiff(_) => bail!("a partial JSON update: binlog_row_value_options must be empty"),
        BinlogValue::Value(v) => match (k, v) {
            (_, Value::NULL) => None,
            (Kind::Enum(labels), Value::Int(i)) => Some(if *i == 0 { String::new() } else { labels.get(*i as usize - 1).cloned().context("an ENUM index past its labels")? }),
            (Kind::Set(labels), Value::Bytes(b)) => {
                let bits = b.iter().rev().fold(0u64, |acc, x| (acc << 8) | *x as u64);
                Some(labels.iter().enumerate().filter(|(i, _)| bits & (1 << i) != 0).map(|(_, l)| l.as_str()).collect::<Vec<_>>().join(","))
            }
            // Seconds since 1970, and a fraction: the time in UTC.
            (Kind::Timestamp, Value::Bytes(b)) => {
                let s = utf8(b)?;
                let (sec, frac) = s.split_once('.').unwrap_or((&s, ""));
                let us: u32 = if frac.is_empty() { 0 } else { format!("{frac:0<6}")[..6].parse()? };
                let t = chrono::DateTime::from_timestamp(sec.parse()?, us * 1000).context("a TIMESTAMP out of range")?;
                if sec == "0" && us == 0 {
                    None
                } else {
                    Some(format!("{}+00", t.format("%Y-%m-%d %H:%M:%S%.6f")))
                }
            }
            (k, v) => snapshot_text(k, v.clone())?,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_and_text() {
        assert_eq!(kind_and_type("int", "int unsigned", Some(10), Some(0)).1, "UINTEGER");
        assert_eq!(kind_and_type("decimal", "decimal(10,2)", Some(10), Some(2)).1, "DECIMAL(10,2)");
        assert_eq!(kind_and_type("decimal", "decimal(65,2)", Some(65), Some(2)).1, "DOUBLE");
        assert_eq!(labels("enum('a','b''c','d')"), vec!["a", "b'c", "d"]);
        let e = Kind::Enum(labels("enum('a','b','c')"));
        // The binlog's ENUM index and SET bitmask, and the snapshot's labels, give the same text.
        assert_eq!(binlog_text(&e, Some(&BinlogValue::Value(Value::Int(2)))).unwrap(), Some("b".into()));
        assert_eq!(snapshot_text(&e, Value::Bytes(b"b".to_vec())).unwrap(), Some("b".into()));
        let s = Kind::Set(labels("set('x','y','z')"));
        assert_eq!(binlog_text(&s, Some(&BinlogValue::Value(Value::Bytes(vec![5])))).unwrap(), Some("x,z".into()));
        // TIMESTAMP: seconds since 1970 in the binlog, a UTC date in the snapshot.
        let ts = Kind::Timestamp;
        assert_eq!(
            binlog_text(&ts, Some(&BinlogValue::Value(Value::Bytes(b"1767323045.678".to_vec())))).unwrap(),
            snapshot_text(&ts, Value::Date(2026, 1, 2, 3, 4, 5, 678_000)).unwrap()
        );
        assert_eq!(snapshot_text(&Kind::DateTime, Value::Date(0, 0, 0, 0, 0, 0, 0)).unwrap(), None, "zero date");
        assert_eq!(snapshot_text(&Kind::Time, Value::Time(true, 34, 22, 59, 59, 0)).unwrap(), Some("-838:59:59".into()));
        assert_eq!(snapshot_text(&Kind::Bit, Value::Bytes(vec![0x01, 0x05])).unwrap(), Some("261".into()));
        assert_eq!(snapshot_text(&Kind::Binary, Value::Bytes(vec![0x00, 0xff])).unwrap(), Some("\\x00ff".into()));
        assert_eq!(snapshot_text(&Kind::Float, Value::Float(1.1)).unwrap(), Some("1.1".into()));
        assert_eq!(snapshot_text(&Kind::Json, Value::Bytes(br#"{"k": [1, 2.5], "a": null}"#.to_vec())).unwrap(), Some(r#"{"a":null,"k":[1,2.5]}"#.into()));
    }
}
