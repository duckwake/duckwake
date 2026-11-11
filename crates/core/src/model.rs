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
    #[serde(default = "default_format")]
    pub format: String,
    /// Column carrying event time; must be one of `columns` with type TIMESTAMP.
    pub event_time: String,
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
        if o.format != "json" {
            bail!("only format 'json' is supported in this POC");
        }
        if !o.columns.iter().any(|(c, t)| c == &o.event_time && t.eq_ignore_ascii_case("TIMESTAMP")) {
            bail!("event_time column {:?} must be listed in columns with type TIMESTAMP", o.event_time);
        }
        for (c, _) in &o.columns {
            if c.starts_with('_') || !c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
                bail!("column {c:?}: use [A-Za-z0-9_] and do not start with '_' (reserved)");
            }
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
        Ok(o)
    }
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
    /// With lake inputs: the snapshot current when the view was created. The view reads the changes after it
    /// (`start: latest`); its first checkpoint starts from here.
    #[serde(default)]
    pub start_snapshot: Option<i64>,
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
    fn event_time(&self) -> &str {
        COMMIT_TS
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
    fn event_time(&self) -> &str {
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
    fn event_time(&self) -> &str {
        &self.options.event_time
    }
    fn columns(&self) -> &[(String, String)] {
        &self.options.columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("30 seconds").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("1 minute").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert!(parse_duration("1 fortnight").is_err());
    }
}
