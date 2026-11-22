//! MySQL sources (D3 of `docs/design/database-cdc-sources.md`): a source of kind `mysql` names tables of one
//! server, and its ingest job (`ingest`) keeps a mirror table of each in the lake, read from the binlog by GTID.
//!
//! - **Position.** The GTID set of the transactions applied, kept in the checkpoint. Each epoch asks the server
//!   for the binlog after that set (`COM_BINLOG_DUMP_GTID`, non-blocking), so it sends no transaction twice and
//!   DuckWake acknowledges nothing. A server whose `gtid_purged` is not within the set has lost binlog the
//!   mirrors need: the job stops and asks for a reset.
//! - **Rows.** `binlog_row_image = FULL`: every change carries the whole row before and after, so a change is the
//!   row itself, and replaying one already applied changes nothing.
//! - **Snapshot.** The GTID set G1, then a `WITH CONSISTENT SNAPSHOT` transaction that copies every table, then
//!   the set G2. The copy holds every transaction of G1, some of those between G1 and G2, and nothing after.
//!   The first epoch applies the binlog after G1 up to G2 over the copy, in the same commit: full row images
//!   make the rows right whichever of them the copy held.
//! - **Values.** The binlog's values and the snapshot's (binary protocol, session time zone UTC) are turned into
//!   one text form per column kind (`Kind`), then cast like every mirror column (`mirror::cast_sql`).
use crate::ingest::{stop, Batch, Change, Position, Read, Reader};
use crate::lake::ident;
use crate::mirror::{cast_sql, MirrorColumn, MirrorTable};
use crate::pgoutput::Value as Text;
use anyhow::{bail, Context, Result};
use duckdb::Connection;
use futures_util::StreamExt;
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::binlog::jsonb::{JsonContainer, JsonDom, JsonNumber, JsonScalar};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn, GnoInterval, Opts, OptsBuilder, Sid, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const KIND: &str = "mysql";

/// A MySQL source as stored in `__duckwake.sources` (kind `mysql`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MyOptions {
    /// `mysql://user@host:port`, without the password; URL parameters such as `require_ssl=true` pass through.
    pub url: String,
    /// `env:NAME` or `file:/path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// The lake schema of the mirror tables.
    pub schema: String,
    /// The replica id the ingest job reads the binlog as; unique among a server's replicas.
    pub server_id: u32,
    pub epoch_interval: String,
    /// Most row changes one epoch reads (whole transactions: a larger one is read whole).
    pub max_changes: usize,
    pub tables: Vec<MirrorTable>,
}

impl MyOptions {
    fn opts(&self) -> Result<Opts> {
        let base = Opts::from_url(&self.url).with_context(|| format!("url {:?}", self.url))?;
        let mut b = OptsBuilder::from_opts(base).prefer_socket(false);
        if let Some(p) = &self.password {
            b = b.pass(Some(crate::secrets::resolve("password", p)?));
        }
        Ok(b.into())
    }
}

/// A runtime of its own: the ingest thread, the CLI and the SQL functions all call in from outside tokio.
fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread().enable_all().build()?)
}

pub fn load(conn: &Connection, name: &str) -> Result<MyOptions> {
    Ok(serde_json::from_str(&crate::catalog::load_source_options(conn, name, KIND)?)?)
}

/// How a column's values are written as text (`text`).
#[derive(Debug, Clone, PartialEq)]
enum Kind {
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
fn kind_and_type(data_type: &str, column_type: &str, precision: Option<u32>, scale: Option<u32>) -> (Kind, String) {
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
fn kind(c: &MirrorColumn) -> Kind {
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
fn snapshot_text(k: &Kind, v: Value) -> Result<Option<String>> {
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
fn binlog_text(k: &Kind, v: Option<&BinlogValue<'_>>) -> Result<Option<String>> {
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

/// A GTID set: per server UUID, intervals of transaction numbers (inclusive, sorted, merged).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GtidSet(BTreeMap<String, Vec<(u64, u64)>>);

impl GtidSet {
    pub fn parse(s: &str) -> Result<Self> {
        let mut set = GtidSet::default();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let mut it = part.split(':');
            let uuid = it.next().context("GTID set")?.to_lowercase();
            for iv in it {
                let (a, b) = iv.split_once('-').unwrap_or((iv, iv));
                let (a, b): (u64, u64) = match (a.parse(), b.parse()) {
                    (Ok(a), Ok(b)) => (a, b),
                    _ => bail!("GTID set {s:?}: tagged GTIDs (MySQL 8.4) are not supported"),
                };
                set.add_range(&uuid, a, b);
            }
        }
        Ok(set)
    }

    fn add_range(&mut self, uuid: &str, a: u64, b: u64) {
        let v = self.0.entry(uuid.to_string()).or_default();
        v.push((a, b));
        v.sort();
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (a, b) in v.drain(..) {
            match merged.last_mut() {
                Some(last) if a <= last.1 + 1 => last.1 = last.1.max(b),
                _ => merged.push((a, b)),
            }
        }
        *v = merged;
    }

    pub fn add(&mut self, uuid: &str, gno: u64) {
        self.add_range(uuid, gno, gno);
    }

    pub fn contains(&self, uuid: &str, gno: u64) -> bool {
        self.0.get(uuid).is_some_and(|v| v.iter().any(|(a, b)| *a <= gno && gno <= *b))
    }

    pub fn is_subset(&self, of: &GtidSet) -> bool {
        self.0.iter().all(|(u, v)| v.iter().all(|(a, b)| of.0.get(u).is_some_and(|w| w.iter().any(|(c, d)| c <= a && b <= d))))
    }

    /// How many transactions it holds.
    pub fn count(&self) -> u64 {
        self.0.values().flatten().map(|(a, b)| b - a + 1).sum()
    }

    fn sids(&self) -> Result<Vec<Sid<'static>>> {
        self.0
            .iter()
            .map(|(u, v)| {
                let hex: String = u.chars().filter(|c| *c != '-').collect();
                let mut bytes = [0u8; 16];
                for (i, b) in bytes.iter_mut().enumerate() {
                    *b = u8::from_str_radix(hex.get(2 * i..2 * i + 2).context("server UUID")?, 16)?;
                }
                // The protocol's intervals end one past the last transaction.
                Ok(Sid::new(bytes).with_intervals(v.iter().map(|(a, b)| GnoInterval::new(*a, b + 1)).collect()))
            })
            .collect()
    }
}

impl std::fmt::Display for GtidSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self
            .0
            .iter()
            .map(|(u, v)| {
                let ivs: Vec<String> = v.iter().map(|(a, b)| if a == b { a.to_string() } else { format!("{a}-{b}") }).collect();
                format!("{u}:{}", ivs.join(":"))
            })
            .collect();
        f.write_str(&parts.join(","))
    }
}

fn uuid_text(sid: [u8; 16]) -> String {
    let h: String = sid.iter().map(|b| format!("{b:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// The tables a DDL statement names (`ALTER TABLE`, `DROP TABLE`, `RENAME TABLE`, `TRUNCATE`), as
/// (database, table), and whether it is a TRUNCATE.
fn ddl_tables(query: &str, default_db: &str) -> (bool, Vec<(String, String)>) {
    let q: String = query.split("/*").map(|p| p.split_once("*/").map_or(p, |(_, r)| r)).collect::<Vec<_>>().join(" ");
    let words: Vec<&str> = q.split(|c: char| c.is_whitespace() || c == ',' || c == ';').filter(|w| !w.is_empty()).collect();
    let lower: Vec<String> = words.iter().map(|w| w.to_lowercase()).collect();
    let name = |w: &str| -> (String, String) {
        let w = w.trim_matches('`');
        match w.split_once("`.`").or_else(|| w.split_once('.')) {
            Some((d, t)) => (d.trim_matches('`').to_string(), t.trim_matches('`').to_string()),
            None => (default_db.to_string(), w.to_string()),
        }
    };
    let skip = ["table", "tables", "if", "exists", "temporary", "online", "offline", "ignore"];
    match lower.first().map(String::as_str) {
        Some("truncate") => (true, words.iter().zip(&lower).skip(1).filter(|(_, l)| !skip.contains(&l.as_str())).take(1).map(|(w, _)| name(w)).collect()),
        Some("alter") if lower.get(1).map(String::as_str) == Some("table") => (false, words.get(2).map(|w| vec![name(w)]).unwrap_or_default()),
        Some("drop") if lower.iter().take(3).any(|l| l == "table" || l == "tables") => {
            (false, words.iter().zip(&lower).skip(1).filter(|(_, l)| !skip.contains(&l.as_str()) && *l != "restrict" && *l != "cascade").map(|(w, _)| name(w)).collect())
        }
        Some("rename") => (false, words.iter().zip(&lower).skip(2).filter(|(_, l)| *l != "to").map(|(w, _)| name(w)).collect()),
        _ => (false, Vec::new()),
    }
}

/// A row of `information_schema.COLUMNS`: name, data type, column type, precision, scale, character set, extra.
type ColumnRow = (String, String, String, Option<u32>, Option<u32>, Option<String>, String);

/// The checks and table definitions of a new MySQL source, then the lake side (`catalog`).
pub fn create(conn: &Connection, name: &str, options: &serde_json::Value) -> Result<MyOptions> {
    crate::lake::check_name("source", name)?;
    let obj = options.as_object().context("options must be a JSON object")?;
    const KNOWN: [&str; 7] = ["url", "password", "tables", "schema", "server_id", "epoch_interval", "max_changes"];
    if let Some(k) = obj.keys().find(|k| !KNOWN.contains(&k.as_str())) {
        bail!("unknown option {k:?} for a mysql source (known: {})", KNOWN.join(", "));
    }
    let s = |k: &str| obj.get(k).map(|v| v.as_str().map(String::from).with_context(|| format!("option {k} must be a string"))).transpose();
    let url = s("url")?.context("option url is required, e.g. \"mysql://duckwake@db:3306\"")?;
    if !url.starts_with("mysql://") {
        bail!("url must start with mysql://");
    }
    if url.split_once("://").is_some_and(|(_, r)| r.split('@').next().is_some_and(|u| r.contains('@') && u.contains(':'))) {
        bail!("put the password in option password as env:NAME or file:/path, not in url, so it is never stored in the lake");
    }
    let password = s("password")?;
    if let Some(p) = &password {
        crate::secrets::check_reference("password", p)?;
    }
    let wanted: Vec<(String, String)> = match obj.get("tables") {
        Some(serde_json::Value::Array(a)) if !a.is_empty() => a
            .iter()
            .map(|t| {
                let t = t.as_str().context("tables must be names like \"shop.orders\"")?;
                t.split_once('.').map(|(d, n)| (d.to_string(), n.to_string())).context("tables must name their database: \"shop.orders\"")
            })
            .collect::<Result<_>>()?,
        _ => bail!("option tables is required: the tables to mirror, e.g. [\"shop.orders\"]"),
    };
    let schema = s("schema")?.unwrap_or_else(|| name.to_string());
    crate::lake::check_name("schema", &schema)?;
    let server_id = match obj.get("server_id") {
        None => 1_000_000 + (name.bytes().fold(2166136261u32, |h, b| (h ^ b as u32).wrapping_mul(16777619)) % 1_000_000_000),
        Some(v) => v.as_u64().filter(|n| *n > 0 && *n < u32::MAX as u64).context("server_id must be a positive integer")? as u32,
    };
    let epoch_interval = s("epoch_interval")?.unwrap_or_else(|| "2s".into());
    crate::model::parse_duration(&epoch_interval)?;
    let max_changes = match obj.get("max_changes") {
        None => 100_000,
        Some(v) => v.as_u64().filter(|n| *n > 0).context("max_changes must be a positive integer")? as usize,
    };
    if crate::catalog::source_kind(conn, name)?.is_some() {
        bail!("source {name:?} already exists");
    }
    if crate::catalog::job_status(conn, name)?.is_some() {
        bail!("a view named {name:?} exists; a mysql source's ingest job takes the source's name");
    }
    let mut opts = MyOptions { url, password, schema, server_id, epoch_interval, max_changes, tables: Vec::new() };
    let rt = runtime()?;
    opts.tables = rt.block_on(async {
        let mut c = Conn::new(opts.opts()?).await.with_context(|| format!("connecting to {}", opts.url))?;
        let vars: Vec<(String, String)> = c
            .query(
                "SELECT 'log_bin', @@GLOBAL.log_bin UNION ALL SELECT 'binlog_format', @@GLOBAL.binlog_format
                 UNION ALL SELECT 'binlog_row_image', @@GLOBAL.binlog_row_image UNION ALL SELECT 'gtid_mode', @@GLOBAL.gtid_mode
                 UNION ALL SELECT 'binlog_row_value_options', @@GLOBAL.binlog_row_value_options
                 UNION ALL SELECT 'binlog_transaction_compression', @@GLOBAL.binlog_transaction_compression",
            )
            .await?;
        let vars: BTreeMap<String, String> = vars.into_iter().collect();
        let want = [("log_bin", "1"), ("binlog_format", "ROW"), ("binlog_row_image", "FULL"), ("gtid_mode", "ON"), ("binlog_row_value_options", ""), ("binlog_transaction_compression", "0")];
        for (k, v) in want {
            if vars.get(k).map(|x| x.to_uppercase()) != Some(v.to_uppercase()) {
                bail!("the server's {k} is {:?}; reading its binlog needs {k} = {}", vars.get(k).cloned().unwrap_or_default(), if v.is_empty() { "''" } else { v });
            }
        }
        let mut tables = Vec::new();
        for (db, t) in &wanted {
            let what = format!("{db}.{t}");
            crate::lake::check_name("mirrored table", t).with_context(|| format!("{what}: its mirror table takes its name"))?;
            let engine: Option<String> =
                c.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'", (db, t)).await?;
            match engine.as_deref() {
                None => bail!("table {what} does not exist (or this user cannot see it)"),
                Some(e) if !e.eq_ignore_ascii_case("InnoDB") => bail!("{what} uses {e}; only InnoDB tables have a consistent snapshot"),
                _ => {}
            }
            let cols: Vec<ColumnRow> = c
                .exec(
                    "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_SET_NAME, EXTRA
                     FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                    (db, t),
                )
                .await?;
            let mut columns = Vec::new();
            for (cname, data_type, column_type, p, sc, charset, extra) in cols {
                if extra.to_uppercase().contains("GENERATED") {
                    bail!("{what}.{cname} is a generated column; tables with generated columns are not supported yet");
                }
                if let Some(cs) = charset.filter(|cs| !cs.starts_with("utf8") && cs != "ascii" && cs != "binary") {
                    bail!("{what}.{cname} uses character set {cs}; text columns must be utf8mb4 (or ascii)");
                }
                let (_, duck) = kind_and_type(&data_type.to_lowercase(), &column_type.to_lowercase(), p, sc);
                columns.push(MirrorColumn { name: cname, source_type: format!("{} {}", data_type.to_lowercase(), column_type.to_lowercase()), oid: 0, typmod: -1, duck_type: duck });
            }
            let key: Vec<String> = c
                .exec(
                    "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
                    (db, t),
                )
                .await?;
            if key.is_empty() {
                bail!("{what} has no primary key; a mirror needs one to apply updates and deletes");
            }
            if tables.iter().any(|x: &MirrorTable| &x.name == t) {
                bail!("two tables named {t}: their mirrors would collide in lake schema {}", opts.schema);
            }
            tables.push(MirrorTable { schema: db.clone(), name: t.clone(), mirror: format!("{}.{}.{t}", crate::lake::LAKE, opts.schema), columns, key });
        }
        c.disconnect().await?;
        Ok(tables)
    })?;
    for t in &opts.tables {
        if crate::catalog::lake_table_exists(conn, &opts.schema, &t.name)? {
            bail!("lake table {}.{} exists already; mirror into another schema (option schema)", opts.schema, t.name);
        }
    }
    crate::catalog::create_ingest_source(conn, name, KIND, &serde_json::to_value(&opts)?, &opts.schema, &opts.tables)?;
    for t in &opts.tables {
        crate::catalog::set_table_inlining(conn, &opts.schema, &t.name, 10_000)?;
    }
    Ok(opts)
}

/// The transaction a reader is in: its GTID, commit time, whether it is a row transaction (`BEGIN` seen), and
/// its changes.
struct Txn {
    uuid: String,
    gno: u64,
    commit_us: i64,
    dml: bool,
    changes: Vec<Change>,
}

pub(crate) struct MyReader {
    job: String,
    opts: MyOptions,
    kinds: Vec<Vec<Kind>>,
    rt: tokio::runtime::Runtime,
}

impl MyReader {
    pub(crate) fn open(job: &str, opts: &MyOptions) -> Result<Self> {
        let kinds = opts.tables.iter().map(|t| t.columns.iter().map(kind).collect()).collect();
        Ok(MyReader { job: job.into(), opts: opts.clone(), kinds, rt: runtime()? })
    }

    fn reset_hint(&self) -> String {
        format!("reset it to take a new snapshot: duckwake reset {} --to-epoch 0", self.job)
    }

    /// Copies every table in one consistent-snapshot transaction into `staging(i)`, between the GTID sets
    /// executed before and after it starts.
    fn copy_between(&self, conn: &Connection, staging: &dyn Fn(usize) -> String) -> Result<(GtidSet, GtidSet)> {
        self.rt.block_on(async {
            let mut c = Conn::new(self.opts.opts()?).await?;
            let g1: String = c.query_first("SELECT @@GLOBAL.gtid_executed").await?.unwrap_or_default();
            c.query_drop("SET SESSION time_zone = '+00:00'").await?;
            c.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ").await?;
            c.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY").await?;
            let g2: String = c.query_first("SELECT @@GLOBAL.gtid_executed").await?.unwrap_or_default();
            for (i, t) in self.opts.tables.iter().enumerate() {
                let raw = format!("memory.main.{}", ident(&format!("ingest_{}_raw{i}", self.job)));
                let defs: Vec<String> = (0..t.columns.len()).map(|k| format!("c{k} VARCHAR")).collect();
                conn.execute_batch(&format!("CREATE OR REPLACE TABLE {raw} ({})", defs.join(", ")))?;
                {
                    let mut app = conn.appender_to_catalog_and_db(&format!("ingest_{}_raw{i}", self.job), "memory", "main")?;
                    let cols: Vec<String> = t.columns.iter().map(|c| format!("`{}`", c.name.replace('`', "``"))).collect();
                    let sql = format!("SELECT {} FROM `{}`.`{}`", cols.join(", "), t.schema.replace('`', "``"), t.name.replace('`', "``"));
                    let mut result = c.exec_iter(sql, ()).await?;
                    let mut stream = result.stream::<mysql_async::Row>().await?.context("the copy returned no rows")?;
                    while let Some(row) = stream.next().await {
                        let row = row?;
                        let vals: Vec<Option<String>> = row
                            .unwrap()
                            .into_iter()
                            .zip(&self.kinds[i])
                            .map(|(v, k)| snapshot_text(k, v))
                            .collect::<Result<_>>()
                            .map_err(|e| stop(format!("a value of {} cannot be read: {e:#}", t.source_name())))?;
                        app.append_row(duckdb::appender_params_from_iter(vals))?;
                    }
                }
                let casts: Vec<String> = t.columns.iter().enumerate().map(|(k, c)| format!("{} AS {}", cast_sql(c, &format!("c{k}")), ident(&c.name))).collect();
                conn.execute_batch(&format!("CREATE OR REPLACE TABLE {} AS SELECT {} FROM {raw}; DROP TABLE {raw}", staging(i), casts.join(", ")))
                    .map_err(|e| crate::ingest::cast_error(e, t))?;
            }
            c.query_drop("COMMIT").await?;
            c.disconnect().await?;
            Ok((GtidSet::parse(&g1)?, GtidSet::parse(&g2)?))
        })
    }

    /// Reads the binlog after `set` into `batch`, whole transactions, until it ends, `max_changes` row changes
    /// were read, or `until` is within the set. Returns the set after, and the counts.
    async fn read_binlog(&self, mut set: GtidSet, batch: &mut Batch<'_>, until: Option<&GtidSet>) -> Result<(GtidSet, u64, i64, Option<i64>, bool)> {
        let c = Conn::new(self.opts.opts()?).await?;
        let req = BinlogStreamRequest::new(self.opts.server_id).with_gtid().with_gtid_set(set.sids()?).with_non_blocking();
        let mut stream = c.get_binlog_stream(req).await?;
        let (mut txns, mut changes, mut newest_us, mut full) = (0u64, 0i64, None, false);
        let mut txn: Option<Txn> = None;
        // Table ids of the binlog to the source's tables (None: one the source does not mirror).
        let mut tables: BTreeMap<u64, Option<usize>> = BTreeMap::new();
        while let Some(ev) = stream.next().await {
            let ev = ev?;
            let mut ended = false;
            match ev.read_data()? {
                Some(EventData::GtidEvent(g)) => {
                    txn = Some(Txn { uuid: uuid_text(g.sid()), gno: g.gno(), commit_us: g.original_commit_timestamp() as i64, dml: false, changes: Vec::new() });
                }
                Some(EventData::QueryEvent(q)) => {
                    let query = q.query();
                    let Some(t) = txn.as_mut() else { continue };
                    if query.eq_ignore_ascii_case("BEGIN") {
                        t.dml = true;
                    } else if query.eq_ignore_ascii_case("COMMIT") {
                        ended = true;
                    } else if !t.dml {
                        // A statement of its own (DDL): its own transaction.
                        let (truncate, named) = ddl_tables(&query, &q.schema());
                        let ours: Vec<usize> = named
                            .iter()
                            .filter_map(|(d, n)| self.opts.tables.iter().position(|x| x.schema.eq_ignore_ascii_case(d) && x.name.eq_ignore_ascii_case(n)))
                            .collect();
                        if truncate && !ours.is_empty() {
                            t.changes.push(Change::Truncate { tables: ours });
                        } else if let Some(&i) = ours.first() {
                            return Err(stop(format!(
                                "{} changed in MySQL ({}): schema changes are not applied yet; drop the source and create it again",
                                self.opts.tables[i].source_name(),
                                query.chars().take(200).collect::<String>()
                            )));
                        }
                        ended = true;
                    }
                }
                Some(EventData::TableMapEvent(tme)) => {
                    let i = self
                        .opts
                        .tables
                        .iter()
                        .position(|x| x.schema.eq_ignore_ascii_case(&tme.database_name()) && x.name.eq_ignore_ascii_case(&tme.table_name()));
                    if let Some(i) = i {
                        self.check_columns(i, &tme)?;
                    }
                    tables.insert(tme.table_id(), i);
                }
                Some(EventData::RowsEvent(r)) => {
                    let Some(&Some(i)) = tables.get(&r.table_id()) else { continue };
                    let tme = stream.get_tme(r.table_id()).context("a rows event without its table map")?;
                    let t = txn.as_mut().context("a rows event outside a transaction")?;
                    let (kinds, table) = (&self.kinds[i], &self.opts.tables[i]);
                    // A value that cannot be read now cannot be read on a retry either: the job stops.
                    let values = |row: &mysql_async::binlog::row::BinlogRow| -> Result<Vec<Text>> {
                        (0..kinds.len())
                            .map(|k| {
                                let v = binlog_text(&kinds[k], row.as_ref(k))
                                    .map_err(|e| stop(format!("a value of {}.{} cannot be read: {e:#}", table.source_name(), table.columns[k].name)))?;
                                Ok(v.map_or(Text::Null, Text::Text))
                            })
                            .collect()
                    };
                    for row in r.rows(tme) {
                        let (before, after) = row?;
                        changes += 1;
                        match (&r, before, after) {
                            (RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_), _, Some(a)) => {
                                t.changes.push(Change::Upsert { table: i, old_key: None, values: values(&a)? })
                            }
                            (RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_), Some(b), Some(a)) => {
                                let old_key = Some(batch.key(i, &values(&b)?)?);
                                t.changes.push(Change::Upsert { table: i, old_key, values: values(&a)? })
                            }
                            (RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_), Some(b), _) => {
                                t.changes.push(Change::Delete { table: i, key: batch.key(i, &values(&b)?)? })
                            }
                            (RowsEventData::PartialUpdateRowsEvent(_), _, _) => {
                                bail!("a partial row update: the server's binlog_row_value_options must be empty")
                            }
                            _ => bail!("a rows event without the row images it needs: binlog_row_image must be FULL"),
                        }
                    }
                }
                Some(EventData::XidEvent(_)) => ended = true,
                Some(EventData::TransactionPayloadEvent(_)) => bail!("a compressed transaction: the server's binlog_transaction_compression must be OFF"),
                _ => {}
            }
            if ended {
                let t = txn.take().context("a transaction end without its GTID")?;
                for c in t.changes {
                    batch.apply(c)?;
                }
                set.add(&t.uuid, t.gno);
                txns += 1;
                newest_us = Some(t.commit_us);
                if until.is_some_and(|u| u.is_subset(&set)) {
                    break;
                }
                if changes >= self.opts.max_changes as i64 && until.is_none() {
                    full = true;
                    break;
                }
            }
        }
        // Ending the dump closes its connection.
        std::mem::drop(stream);
        Ok((set, txns, changes, newest_us, full))
    }

    /// The columns a table map describes must be the ones the mirror was created with.
    fn check_columns(&self, i: usize, tme: &mysql_async::binlog::events::TableMapEvent<'_>) -> Result<()> {
        let t = &self.opts.tables[i];
        let mut names: Vec<String> = Vec::new();
        for f in tme.iter_optional_meta() {
            if let mysql_async::binlog::events::OptionalMetadataField::ColumnName(n) = f? {
                for name in n.iter_names() {
                    names.push(name?.name().to_string());
                }
            }
        }
        let same_count = tme.columns_count() as usize == t.columns.len();
        let same_names = names.is_empty() || names.iter().zip(&t.columns).all(|(a, b)| *a == b.name);
        if !same_count || !same_names {
            let was: Vec<&str> = t.columns.iter().map(|c| c.name.as_str()).collect();
            return Err(stop(format!(
                "the columns of {} changed in MySQL: now {} columns{}, the mirror has {}. Schema changes are not applied yet: \
                 drop the source and create it again",
                t.source_name(),
                tme.columns_count(),
                if names.is_empty() { String::new() } else { format!(" ({})", names.join(", ")) },
                was.join(", ")
            )));
        }
        Ok(())
    }

    /// The server's executed and purged GTID sets.
    async fn server_sets(&self) -> Result<(GtidSet, GtidSet)> {
        let mut c = Conn::new(self.opts.opts()?).await?;
        let (executed, purged): (String, String) =
            c.query_first("SELECT @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged").await?.context("gtid_executed")?;
        c.disconnect().await?;
        Ok((GtidSet::parse(&executed)?, GtidSet::parse(&purged)?))
    }
}

fn position(set: &GtidSet) -> Position {
    Position { offset: set.count(), state: serde_json::json!({ "gtid": set.to_string() }) }
}

impl Reader for MyReader {
    fn backend(&self) -> &'static str {
        "binlog"
    }

    fn describe(&self, pos: &Position) -> String {
        let s = pos.state.get("gtid").and_then(|v| v.as_str()).unwrap_or("");
        format!("GTID set {}", if s.len() > 120 { format!("{}...", &s[..120]) } else { s.to_string() })
    }

    fn snapshot(&mut self, conn: &Connection, staging: &dyn Fn(usize) -> String, batch: &mut Batch) -> Result<(Position, i64)> {
        let (g1, g2) = self.copy_between(conn, staging)?;
        // The copy holds G1 and some of what came after it, up to G2: the binlog from G1 up to G2, applied over
        // it, makes it the tables as of G2 (full row images: a change already in the copy changes nothing).
        let (set, _, changes, _, _) = self.rt.block_on(self.read_binlog(g1, batch, Some(&g2)))?;
        Ok((position(&set), changes))
    }

    fn copy(&mut self, conn: &Connection, staging: &dyn Fn(usize) -> String) -> Result<()> {
        self.copy_between(conn, staging).map(|_| ())
    }

    fn read(&mut self, _conn: &Connection, pos: &Position, batch: &mut Batch) -> Result<Read> {
        let set = GtidSet::parse(pos.state.get("gtid").and_then(|v| v.as_str()).unwrap_or(""))?;
        let (executed, purged) = self.rt.block_on(self.server_sets())?;
        if !purged.is_subset(&set) {
            return Err(stop(format!(
                "the server purged binlog the mirrors had not read (gtid_purged {purged} is not within the lake's position {set}), \
                 so those changes are lost; {}",
                self.reset_hint()
            )));
        }
        let (after, txns, changes, newest_us, full) = self.rt.block_on(self.read_binlog(set, batch, None))?;
        let lag = executed.count().saturating_sub(after.count()) as i64;
        Ok(Read { pos: position(&after), full, txns, skipped: 0, changes, newest_us, lag, due: false })
    }

    fn committed(&mut self, _conn: &Connection, _pos: &Position) {}
}

/// Drops a stopped MySQL source: its ingest job and mirror tables. Nothing is left on the server: a binlog reader
/// holds nothing there.
pub fn drop(conn: &Connection, name: &str, opts: &MyOptions) -> Result<String> {
    if crate::catalog::job_status(conn, name)? == Some(crate::model::JobStatus::Running) {
        bail!("stop {name:?} before dropping it");
    }
    let mirrors: Vec<String> = opts.tables.iter().map(|t| t.mirror.clone()).collect();
    let readers = crate::catalog::views_reading_tables(conn, &mirrors)?;
    if !readers.is_empty() {
        bail!("views {} read source {name}'s tables; drop them first", readers.join(", "));
    }
    crate::catalog::drop_ingest_source(conn, name, &opts.tables)?;
    Ok(format!("source {name}: dropped with its mirror tables"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gtid_sets() {
        let s = GtidSet::parse("B2EB3B02-E621-11F1-9FB5-02FC00000001:1-7:9,\n0a0a0a0a-0000-0000-0000-000000000000:3").unwrap();
        assert_eq!(s.to_string(), "0a0a0a0a-0000-0000-0000-000000000000:3,b2eb3b02-e621-11f1-9fb5-02fc00000001:1-7:9");
        assert_eq!(s.count(), 9);
        let mut t = s.clone();
        t.add("b2eb3b02-e621-11f1-9fb5-02fc00000001", 8);
        assert_eq!(t.to_string(), "0a0a0a0a-0000-0000-0000-000000000000:3,b2eb3b02-e621-11f1-9fb5-02fc00000001:1-9");
        assert!(s.is_subset(&t) && !t.is_subset(&s));
        assert!(GtidSet::default().is_subset(&s));
        assert!(t.contains("b2eb3b02-e621-11f1-9fb5-02fc00000001", 8));
        assert!(GtidSet::parse("").unwrap().0.is_empty());
        assert!(GtidSet::parse("b2eb3b02-e621-11f1-9fb5-02fc00000001:tag:1-3").is_err());
        let sids = t.sids().unwrap();
        assert_eq!(sids.len(), 2);
        assert_eq!(uuid_text([0xb2, 0xeb, 0x3b, 0x02, 0xe6, 0x21, 0x11, 0xf1, 0x9f, 0xb5, 0x02, 0xfc, 0, 0, 0, 1]), "b2eb3b02-e621-11f1-9fb5-02fc00000001");
    }

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

    #[test]
    fn ddl() {
        assert_eq!(ddl_tables("TRUNCATE TABLE `shop`.`orders`", "x"), (true, vec![("shop".into(), "orders".into())]));
        assert_eq!(ddl_tables("truncate orders", "shop"), (true, vec![("shop".into(), "orders".into())]));
        assert_eq!(ddl_tables("ALTER TABLE orders ADD COLUMN x int", "shop"), (false, vec![("shop".into(), "orders".into())]));
        assert_eq!(
            ddl_tables("DROP TABLE IF EXISTS `a`,`b` /* generated by server */", "shop"),
            (false, vec![("shop".into(), "a".into()), ("shop".into(), "b".into())])
        );
        assert_eq!(ddl_tables("RENAME TABLE a TO b", "shop").1.len(), 2);
        assert_eq!(ddl_tables("CREATE TABLE t (id int)", "shop"), (false, vec![]));
    }
}
