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
