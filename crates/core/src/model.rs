//! Definitions stored as JSON in `__duckwake.sources` and `__duckwake.jobs`.
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceDef {
    pub name: String,
    pub kind: String,
    pub options: SourceOptions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceOptions {
    pub brokers: String,
    pub topic: String,
    /// `json` (each message one row) or `debezium` (Debezium change events, read as upserts by key).
    #[serde(default = "default_format")]
    pub format: String,
    /// Column carrying event time; must be one of `columns` with type TIMESTAMP. Required for `json`;
    /// optional for `debezium`, whose views keep one row per key and have no windows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time: Option<String>,
    #[serde(default = "default_watermark_delay")]
    pub watermark_delay: String,
    /// Partitions with no data for this long stop holding the watermark back.
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout: String,
    /// Ordered column name -> DuckDB type. JSON fields are cast to these types.
    pub columns: Vec<(String, String)>,
    /// `earliest` or `latest`, used when a job has no checkpoint yet.
    #[serde(default = "default_start")]
    pub start: String,
    /// `auto` (Arrow when every column type has an Arrow decoder, else SQL), `arrow` or `sql`.
    #[serde(default = "default_decode")]
    pub decode: String,
    /// How to reach the brokers, named as Kafka clients name it: `plaintext` (default), `ssl`,
    /// `sasl_plaintext` or `sasl_ssl`.
    #[serde(default = "default_security_protocol")]
    pub security_protocol: String,
    /// With SASL: `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_mechanism: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_username: Option<String>,
    /// `env:NAME` or `file:/path`, resolved by the worker when it connects (`secrets`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_password: Option<String>,
    /// With TLS: a PEM file of CA certificates to trust instead of the public roots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_ca_location: Option<String>,
    /// With TLS and a broker that wants client certificates: PEM files of the certificate chain and its key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_certificate_location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_key_location: Option<String>,
}

impl SourceOptions {
    /// Whether the topic carries Debezium change events.
    pub fn is_debezium(&self) -> bool {
        self.format == "debezium"
    }
}

fn default_format() -> String {
    "json".into()
}
fn default_watermark_delay() -> String {
    "30 seconds".into()
}
fn default_idle_timeout() -> String {
    "60 seconds".into()
}
fn default_start() -> String {
    "earliest".into()
}
fn default_decode() -> String {
    "auto".into()
}
fn default_security_protocol() -> String {
    "plaintext".into()
}

impl SourceOptions {
    /// Checks the connection settings: protocol and mechanism names, credentials with SASL, a password given
    /// as a reference, files only with TLS.
    fn check_security(&self) -> Result<()> {
        let (tls, sasl) = match self.security_protocol.to_ascii_lowercase().as_str() {
            "plaintext" => (false, false),
            "ssl" => (true, false),
            "sasl_plaintext" => (false, true),
            "sasl_ssl" => (true, true),
            p => bail!("security_protocol must be plaintext, ssl, sasl_plaintext or sasl_ssl, not {p:?}"),
        };
        if sasl {
            match self.sasl_mechanism.as_deref().map(str::to_ascii_uppercase).as_deref() {
                Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512") => {}
                Some(m) => bail!("sasl_mechanism must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512, not {m:?}"),
                None => bail!("security_protocol {} needs sasl_mechanism", self.security_protocol),
            }
            if self.sasl_username.as_deref().is_none_or(str::is_empty) {
                bail!("security_protocol {} needs sasl_username", self.security_protocol);
            }
            let Some(p) = &self.sasl_password else {
                bail!("security_protocol {} needs sasl_password (env:NAME or file:/path)", self.security_protocol)
            };
            crate::secrets::check_reference("sasl_password", p)?;
        } else if self.sasl_mechanism.is_some() || self.sasl_username.is_some() || self.sasl_password.is_some() {
            bail!("sasl_* options need security_protocol sasl_plaintext or sasl_ssl");
        }
        let files = [&self.ssl_ca_location, &self.ssl_certificate_location, &self.ssl_key_location];
        if !tls && files.iter().any(|f| f.is_some()) {
            bail!("ssl_* options need security_protocol ssl or sasl_ssl");
        }
        if self.ssl_certificate_location.is_some() != self.ssl_key_location.is_some() {
            bail!("ssl_certificate_location and ssl_key_location go together");
        }
        Ok(())
    }
}

impl SourceOptions {
    /// The Arrow decoder this source's readers use, or None to decode everything in SQL.
    pub fn arrow_decoder(&self) -> Result<Option<crate::decode::ArrowDecoder>> {
        if self.decode == "sql" {
            return Ok(None);
        }
        crate::decode::ArrowDecoder::new(self)
    }

    /// Accepts `columns` either as an ordered list of pairs or as a JSON object (key order is kept).
    pub fn from_json(v: &serde_json::Value) -> Result<Self> {
        let mut v = v.clone();
        if let Some(obj) = v.get("columns").and_then(|c| c.as_object()) {
            let pairs: Vec<(String, String)> = obj
                .iter()
                .map(|(k, t)| (k.clone(), t.as_str().unwrap_or_default().to_string()))
                .collect();
            v["columns"] = serde_json::to_value(pairs)?;
        }
        let o: SourceOptions = serde_json::from_value(v).context("parsing source options")?;
        match (o.format.as_str(), &o.event_time) {
            ("json", None) => bail!("event_time is required: the column holding each event's time"),
            ("json" | "debezium", _) => {}
            (f, _) => bail!("format must be 'json' or 'debezium', not {f:?}"),
        }
        if let Some(et) = &o.event_time {
            if !o.columns.iter().any(|(c, t)| c == et && t.eq_ignore_ascii_case("TIMESTAMP")) {
                bail!("event_time column {et:?} must be listed in columns with type TIMESTAMP");
            }
        }
        if o.is_debezium() && o.decode == "arrow" {
            bail!("decode 'arrow' reads flat JSON objects; a Debezium source decodes its envelope in SQL (decode 'auto' or 'sql')");
        }
        for (c, t) in &o.columns {
            check_column(c, t)?;
        }
        parse_duration(&o.watermark_delay)?;
        parse_duration(&o.idle_timeout)?;
        if o.start != "earliest" && o.start != "latest" {
            bail!("start must be 'earliest' or 'latest'");
        }
        match o.decode.as_str() {
            "auto" | "sql" => {}
            "arrow" if crate::decode::ArrowDecoder::new(&o)?.is_none() => {
                bail!("decode 'arrow' supports TIMESTAMP, VARCHAR, BOOLEAN, integer, DOUBLE and DECIMAL columns only")
            }
            "arrow" => {}
            d => bail!("decode must be 'auto', 'arrow' or 'sql', not {d:?}"),
        }
        o.check_security()?;
        Ok(o)
    }
}

/// A source column's name and type: names are `[A-Za-z0-9_]` and do not start with `_` (reserved for
/// DuckWake's own columns); a type is a DuckDB type name, such as `VARCHAR`, `DECIMAL(18,2)` or `INTEGER[]`.
pub fn check_column(name: &str, ty: &str) -> Result<()> {
    if name.is_empty() || name.starts_with('_') || !name.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        bail!("column {name:?}: use [A-Za-z0-9_] and do not start with '_' (reserved)");
    }
    if ty.trim().is_empty() || !ty.chars().all(|ch| ch.is_ascii_alphanumeric() || " _(),[]".contains(ch)) {
        bail!("column {name}: type {ty:?} is not a DuckDB type name");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDef {
    pub job_id: String,
    pub sql: String,
    pub sink: String,
    pub late_data: Option<String>,
    /// Only `ducklake` is implemented; `s3://...` (object storage state) is phase 3.
    pub checkpoint: String,
    pub epoch_interval: String,
    pub max_rows: usize,
    #[serde(default = "default_inline_row_limit")]
    pub inline_row_limit: usize,
    pub plan: crate::plan::Plan,
    /// The lake tables and views the view reads through their change feeds (not lookups), in the order
    /// `create_view` resolved them.
    #[serde(default)]
    pub lake_inputs: Vec<LakeInput>,
    /// With lake inputs: the snapshot current when the view was created (S0). The view reads the changes
    /// after it; with `backfill` it first reads the rows its inputs held at it. None after a cascaded reset,
    /// until the view's next start takes the snapshot current then.
    #[serde(default)]
    pub start_snapshot: Option<i64>,
    /// With lake inputs: whether the view starts from the rows its inputs hold at `start_snapshot`
    /// (`start := 'earliest'`, the default) before it follows their changes. False (`start := 'latest'`, and
    /// every view made before backfill existed): only rows written after `start_snapshot`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub backfill: bool,
    /// A backfill that takes only rows written after this snapshot: a `start := 'latest'` view recomputed by
    /// a cascaded reset keeps to the rows written after the snapshot it was created at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lake_floor: Option<i64>,
    /// `_commit_ts` at `start_snapshot`: what a row carries whose writing snapshot's time is no longer known
    /// (expired before DuckWake's maintenance recorded it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_commit_us: Option<i64>,
    /// The columns of each Kafka source the view reads, as they were when it was created: a column added to
    /// the source later (`add_source_column`) does not change the view's shape. Empty in views made before
    /// columns could be added, which read the source's columns as they are.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub source_columns: std::collections::BTreeMap<String, Vec<(String, String)>>,
    /// Limits that fail the view (status `failed`, the worker and its other views go on) instead of letting
    /// it grow without bound. None: no limit.
    #[serde(default, skip_serializing_if = "Limits::is_none")]
    pub limits: Limits,
    /// A view that reads its Kafka source as upserts (`upsert_key :=`): one row per key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upsert: Option<crate::plan::UpsertSpec>,
}

impl JobDef {
    /// The snapshot at or before which a lake input's rows are not the view's: none with a full backfill.
    pub fn floor(&self) -> Option<i64> {
        self.lake_floor.or(if self.backfill { None } else { self.start_snapshot })
    }
}

/// The key, in a lake input's offsets, of its position: the last snapshot read.
pub const LAKE_POSITION: i32 = 0;
/// The key, in a lake input's offsets, of its backfill cursor: the last row id read at the start snapshot.
/// There only while the backfill runs.
pub const LAKE_BACKFILL: i32 = 1;

/// A view's limits; `alter_view` changes them without recomputing the view.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Most rows the view's state may hold (open windows, groups or distinct rows).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_state_rows: Option<u64>,
    /// Most late rows one epoch may write: more means a broken watermark or clock, not late data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_late_rows: Option<u64>,
}

impl Limits {
    pub fn is_none(&self) -> bool {
        self.max_state_rows.is_none() && self.max_late_rows.is_none()
    }
}

/// The column every lake input adds: the commit time of the change, as the running maximum of
/// `snapshot_time` by snapshot id, in UTC. Monotone in commit order, so it gives an exact watermark.
pub const COMMIT_TS: &str = "_commit_ts";

/// A lake table, or another view's table, that a view reads through its DuckLake change feed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LakeInput {
    /// The name the view's SQL uses for it, and its key in the checkpoint's offsets.
    pub name: String,
    /// `lake.schema.table` when the view was created.
    pub table: String,
    /// DuckLake table id: a renamed table keeps working, and a dropped and recreated one has a new id and
    /// fails the job.
    pub table_id: i64,
    /// Its columns when the view was created, then `_commit_ts`.
    pub columns: Vec<(String, String)>,
    /// The view whose table this is, if any.
    pub view: Option<String>,
    /// A windowed view's table: its window size. Windows over its `window_start` or `window_end` then get
    /// an exact watermark from the view's own.
    pub window_size_us: Option<i64>,
    /// Whether the view takes its updates and deletes: each change then carries a weight `__w` (inserts and
    /// new versions +1, deletes and old versions -1) and the view retracts what it no longer holds.
    /// Otherwise the view needs it insert-only, and an update or delete stops the view.
    #[serde(default)]
    pub retracts: bool,
    /// A view's table that keeps each row once with its multiplicity in `_duckwake_count`: a change of such
    /// a row weighs that many rows.
    #[serde(default)]
    pub counted: bool,
}

impl LakeInput {
    /// The time column a window over this input may use, and the watermark source it implies.
    pub fn window_columns(&self) -> Vec<String> {
        let mut out = vec![COMMIT_TS.to_string()];
        if self.window_size_us.is_some() {
            out.extend(["window_start".into(), "window_end".into()]);
        }
        out
    }
}

impl crate::circuit::SourceSchema for LakeInput {
    fn event_time(&self) -> Option<&str> {
        Some(COMMIT_TS)
    }
    fn columns(&self) -> &[(String, String)] {
        &self.columns
    }
    fn append_only(&self) -> bool {
        !self.retracts
    }
    fn is_lake(&self) -> bool {
        true
    }
    fn time_columns(&self) -> Vec<String> {
        self.window_columns()
    }
}

/// Anything a view names in FROM that the compiler treats as a stream: a declared Kafka source, or a lake
/// table or view read through its change feed.
pub enum Input<'a> {
    Kafka(&'a SourceDef),
    Lake(&'a LakeInput),
}

impl crate::circuit::SourceSchema for Input<'_> {
    fn event_time(&self) -> Option<&str> {
        match self {
            Input::Kafka(s) => s.event_time(),
            Input::Lake(l) => l.event_time(),
        }
    }
    fn columns(&self) -> &[(String, String)] {
        match self {
            Input::Kafka(s) => s.columns(),
            Input::Lake(l) => l.columns(),
        }
    }
    fn append_only(&self) -> bool {
        match self {
            Input::Kafka(s) => s.append_only(),
            Input::Lake(l) => l.append_only(),
        }
    }
    fn is_lake(&self) -> bool {
        matches!(self, Input::Lake(_))
    }
    fn changelog(&self) -> bool {
        matches!(self, Input::Kafka(s) if s.changelog())
    }
    fn time_columns(&self) -> Vec<String> {
        match self {
            Input::Kafka(s) => s.time_columns(),
            Input::Lake(l) => l.time_columns(),
        }
    }
}

fn default_inline_row_limit() -> usize {
    10_000
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Created,
    Running,
    Paused,
    Stopped,
    Failed,
}

impl JobStatus {
    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "created" => Self::Created,
            "running" => Self::Running,
            "paused" => Self::Paused,
            "stopped" => Self::Stopped,
            "failed" => Self::Failed,
            _ => bail!("unknown job status {s:?}"),
        })
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

/// Parses `10s`, `500ms`, `30 seconds`, `1 minute`, `2h`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim().to_ascii_lowercase();
    let split = s.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: f64 = num.parse().with_context(|| format!("bad duration {s:?}"))?;
    let secs = match unit.trim() {
        "ms" | "millisecond" | "milliseconds" => n / 1000.0,
        "" | "s" | "sec" | "secs" | "second" | "seconds" => n,
        "m" | "min" | "mins" | "minute" | "minutes" => n * 60.0,
        "h" | "hour" | "hours" => n * 3600.0,
        "d" | "day" | "days" => n * 86400.0,
        u => bail!("bad duration unit {u:?} in {s:?}"),
    };
    Ok(Duration::from_secs_f64(secs))
}

impl JobDef {
    /// The view as the circuit compiler needs it.
    pub fn view(&self) -> crate::circuit::View<'_> {
        crate::circuit::View { id: &self.job_id, sink: &self.sink, late_data: self.late_data.as_deref(), plan: &self.plan }
    }
}

impl crate::circuit::SourceSchema for SourceDef {
    fn event_time(&self) -> Option<&str> {
        self.options.event_time.as_deref()
    }
    fn columns(&self) -> &[(String, String)] {
        &self.options.columns
    }
    fn changelog(&self) -> bool {
        self.options.is_debezium()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_formats_and_event_time() {
        let opts = |extra: serde_json::Value| {
            let mut v = serde_json::json!({"brokers": "x", "topic": "t", "columns": {"ts": "TIMESTAMP", "id": "BIGINT"}});
            v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            SourceOptions::from_json(&v).map_err(|e| format!("{e:#}"))
        };
        assert!(opts(serde_json::json!({"event_time": "ts"})).is_ok());
        assert!(opts(serde_json::json!({})).unwrap_err().contains("event_time is required"));
        let d = opts(serde_json::json!({"format": "debezium"})).unwrap();
        assert!(d.is_debezium() && d.event_time.is_none());
        assert!(opts(serde_json::json!({"format": "debezium", "event_time": "id"})).unwrap_err().contains("type TIMESTAMP"));
        assert!(opts(serde_json::json!({"format": "debezium", "decode": "arrow"})).unwrap_err().contains("decodes its envelope in SQL"));
        assert!(opts(serde_json::json!({"format": "avro", "event_time": "ts"})).unwrap_err().contains("'json' or 'debezium'"));
        // A source stored before event_time became optional reads back the same.
        let old: SourceOptions = serde_json::from_value(serde_json::json!({"brokers": "x", "topic": "t", "event_time": "ts",
            "columns": [["ts", "TIMESTAMP"]]})).unwrap();
        assert_eq!(old.event_time.as_deref(), Some("ts"));
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("30 seconds").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("1 minute").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert!(parse_duration("1 fortnight").is_err());
    }

    #[test]
    fn column_names_and_types() {
        check_column("country", "VARCHAR").unwrap();
        check_column("price", "DECIMAL(18, 2)").unwrap();
        check_column("tags", "VARCHAR[]").unwrap();
        assert!(check_column("_epoch", "BIGINT").is_err(), "reserved prefix");
        assert!(check_column("a-b", "BIGINT").is_err());
        assert!(check_column("", "BIGINT").is_err());
        assert!(check_column("x", "INT); DROP TABLE t; --").is_err());
        assert!(check_column("x", "INT'").is_err());
        assert!(check_column("x", " ").is_err());
    }

    #[test]
    fn limits_are_optional_in_stored_definitions() {
        // Definitions written before limits existed have no `limits` key.
        let l: Limits = serde_json::from_str("{}").unwrap();
        assert!(l.is_none());
        let l: Limits = serde_json::from_str(r#"{"max_state_rows": 10}"#).unwrap();
        assert_eq!(l, Limits { max_state_rows: Some(10), max_late_rows: None });
        // And nothing is written when there are none, so older versions read new definitions too.
        assert_eq!(serde_json::to_string(&Limits::default()).unwrap(), "{}");
    }
}
