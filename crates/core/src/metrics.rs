//! Live numbers per view, updated by the worker after every committed epoch, plus whether the worker is up
//! and how often it restarted. Read as Prometheus text (`GET /metrics`, from `serve_http`) or as rows
//! (`duckwake_metrics()`). Freshness and watermark delay are computed when read, so they keep growing while
//! a view is stuck.
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct Metrics {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    worker: String,
    up: bool,
    restarts: u64,
    last_error: Option<(chrono::DateTime<chrono::Utc>, String)>,
    jobs: BTreeMap<String, Job>,
    epochs: u64,
    commit_conflicts: u64,
    commit_seconds_sum: f64,
    last_commit_seconds: f64,
    last_stage_seconds: f64,
}

#[derive(Default, Clone)]
struct Job {
    epoch: i64,
    lag: i64,
    watermark_us: Option<i64>,
    max_event_time_us: Option<i64>,
    rows_in: u64,
    rows_out: u64,
    rows_late: u64,
    rows_dead: u64,
    stage_seconds: f64,
    commit_freshness_seconds: Option<f64>,
}

/// One series' value: `view` is set for per-view series, None for worker-wide ones.
pub struct Sample {
    pub name: &'static str,
    pub kind: &'static str,
    pub help: &'static str,
    pub view: Option<String>,
    pub value: f64,
}

type PerView = (&'static str, &'static str, &'static str, fn(&Job, &dyn Fn(i64) -> f64) -> Option<f64>);

/// The per-view series: name, type, help, and how to read it off a view's numbers.
const PER_VIEW: [PerView; 10] = [
    ("duckwake_freshness_seconds", "gauge", "Now minus the newest event time committed (alert above 3x the epoch interval).", |j, secs| j.max_event_time_us.map(secs)),
    ("duckwake_commit_freshness_seconds", "gauge", "At the last commit: newest event time to the moment its epoch became visible.", |j, _| j.commit_freshness_seconds),
    ("duckwake_watermark_delay_seconds", "gauge", "Now minus the view's watermark (alert above 2x watermark_delay).", |j, secs| j.watermark_us.map(secs)),
    ("duckwake_lag_events", "gauge", "Events behind the end of the topic after the last commit.", |j, _| Some(j.lag as f64)),
    ("duckwake_epoch", "gauge", "Last committed epoch.", |j, _| Some(j.epoch as f64)),
    ("duckwake_stage_seconds", "gauge", "Staging time of the view in its last epoch.", |j, _| Some(j.stage_seconds)),
    ("duckwake_rows_in_total", "counter", "Source events read.", |j, _| Some(j.rows_in as f64)),
    ("duckwake_rows_out_total", "counter", "Rows written to the view's table.", |j, _| Some(j.rows_out as f64)),
    ("duckwake_late_rows_total", "counter", "Rows routed to the late-data table.", |j, _| Some(j.rows_late as f64)),
    ("duckwake_dead_letters_total", "counter", "Events that could not be decoded.", |j, _| Some(j.rows_dead as f64)),
];

/// What one view's committed epoch adds.
pub struct EpochStats {
    pub epoch: i64,
    pub lag: i64,
    pub watermark_us: Option<i64>,
    pub max_event_time_us: Option<i64>,
    pub rows_in: u64,
    pub rows_out: u64,
    pub rows_late: u64,
    pub rows_dead: u64,
    pub stage_ms: u128,
    /// Newest committed event time to the moment its epoch became visible.
    pub commit_freshness_s: Option<f64>,
}

impl Metrics {
    pub fn set_worker(&self, worker: &str) {
        self.lock().worker = worker.to_string();
    }

    pub fn record_job(&self, job: &str, s: EpochStats) {
        let mut st = self.lock();
        let j = st.jobs.entry(job.to_string()).or_default();
        j.epoch = s.epoch;
        j.lag = s.lag;
        j.watermark_us = s.watermark_us;
        j.max_event_time_us = s.max_event_time_us;
        j.rows_in += s.rows_in;
        j.rows_out += s.rows_out;
        j.rows_late += s.rows_late;
        j.rows_dead += s.rows_dead;
        j.stage_seconds = s.stage_ms as f64 / 1000.0;
        j.commit_freshness_seconds = s.commit_freshness_s;
    }

    /// One committed epoch of the whole worker; `attempts` above 1 were DuckLake write conflicts.
    pub fn record_commit(&self, poll_stage_ms: u128, commit_ms: u128, attempts: u64) {
        let mut st = self.lock();
        st.epochs += 1;
        st.commit_conflicts += attempts.saturating_sub(1);
        st.commit_seconds_sum += commit_ms as f64 / 1000.0;
        st.last_commit_seconds = commit_ms as f64 / 1000.0;
        st.last_stage_seconds = poll_stage_ms as f64 / 1000.0;
    }

    /// A view that left this worker (paused, stopped, reassigned or fenced).
    pub fn forget(&self, job: &str) {
        self.lock().jobs.remove(job);
    }

    /// The worker loop is running (true) or waiting to restart after a failure (false).
    pub fn set_up(&self, up: bool) {
        self.lock().up = up;
    }

    /// The worker failed with `error` and will restart.
    pub fn record_failure(&self, error: &str) {
        let mut st = self.lock();
        st.up = false;
        st.restarts += 1;
        st.last_error = Some((chrono::Utc::now(), error.to_string()));
    }

    /// Worker id, whether it runs, restarts so far, and the last failure (time, error).
    pub fn status(&self) -> (String, bool, u64, Option<(chrono::DateTime<chrono::Utc>, String)>) {
        let st = self.lock();
        (st.worker.clone(), st.up, st.restarts, st.last_error.clone())
    }

    /// Every series as (name, view, value): per-view ones carry the view, worker-wide ones None.
    pub fn samples(&self) -> Vec<Sample> {
        let st = self.lock();
        let now_us = chrono::Utc::now().timestamp_micros();
        let secs = |us: i64| (now_us - us) as f64 / 1e6;
        let mut out = Vec::new();
        for (name, kind, help, f) in PER_VIEW {
            for (id, j) in &st.jobs {
                if let Some(value) = f(j, &secs) {
                    out.push(Sample { name, kind, help, view: Some(id.clone()), value });
                }
            }
        }
        let worker: [(&'static str, &'static str, &'static str, f64); 8] = [
            ("duckwake_worker_up", "gauge", "1 while the worker runs, 0 while it waits to restart after a failure.", if st.up { 1.0 } else { 0.0 }),
            ("duckwake_worker_restarts_total", "counter", "Times the worker failed and restarted from its last checkpoint.", st.restarts as f64),
            ("duckwake_views", "gauge", "Views this worker runs.", st.jobs.len() as f64),
            ("duckwake_epochs_total", "counter", "Epochs committed (one transaction each).", st.epochs as f64),
            ("duckwake_commit_seconds_sum", "counter", "Time spent in epoch commits.", st.commit_seconds_sum),
            ("duckwake_commit_seconds", "gauge", "Duration of the last epoch commit.", st.last_commit_seconds),
            ("duckwake_poll_stage_seconds", "gauge", "Poll and stage time of the last epoch, all views.", st.last_stage_seconds),
            ("duckwake_commit_conflicts_total", "counter", "DuckLake write conflicts retried by epoch commits.", st.commit_conflicts as f64),
        ];
        out.extend(worker.into_iter().map(|(name, kind, help, value)| Sample { name, kind, help, view: None, value }));
        out
    }

    /// The Prometheus text format.
    pub fn render(&self) -> String {
        let w = escape(&self.lock().worker);
        let mut out = String::new();
        let mut last = "";
        for s in self.samples() {
            if s.name != last {
                let _ = writeln!(out, "# HELP {} {}\n# TYPE {} {}", s.name, s.help, s.name, s.kind);
                last = s.name;
            }
            match &s.view {
                Some(v) => {
                    let _ = writeln!(out, "{}{{worker=\"{w}\",view=\"{}\"}} {}", s.name, escape(v), s.value);
                }
                None => {
                    let _ = writeln!(out, "{}{{worker=\"{w}\"}} {}", s.name, s.value);
                }
            }
        }
        out
    }

    /// Serves `GET /metrics` (this registry) and `GET /healthz` on `addr`, on the runtime `rt`.
    pub fn serve_http(self: &Arc<Self>, addr: &str, rt: &tokio::runtime::Handle) -> anyhow::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = rt
            .block_on(tokio::net::TcpListener::bind(addr))
            .map_err(|e| anyhow::anyhow!("binding the metrics endpoint to {addr}: {e}"))?;
        let metrics = self.clone();
        rt.spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { continue };
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = match tokio::time::timeout(std::time::Duration::from_secs(5), sock.read(&mut buf)).await {
                        Ok(Ok(n)) => n,
                        _ => return,
                    };
                    let path = String::from_utf8_lossy(&buf[..n]).split_whitespace().nth(1).unwrap_or("").to_string();
                    let (status, ctype, body) = match path.as_str() {
                        "/metrics" => ("200 OK", "text/plain; version=0.0.4", metrics.render()),
                        "/healthz" => ("200 OK", "text/plain", "ok".to_string()),
                        _ => ("404 Not Found", "text/plain", "not found".to_string()),
                    };
                    let head = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(body.as_bytes()).await;
                });
            }
        });
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// A Prometheus label value.
fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_per_view_and_worker_series() {
        let m = Metrics::default();
        m.set_worker("w1");
        let now = chrono::Utc::now().timestamp_micros();
        m.record_job(
            "clicks",
            EpochStats {
                epoch: 7,
                lag: 12,
                watermark_us: Some(now - 5_000_000),
                max_event_time_us: Some(now - 1_000_000),
                rows_in: 100,
                rows_out: 3,
                rows_late: 1,
                rows_dead: 2,
                stage_ms: 40,
                commit_freshness_s: Some(0.8),
            },
        );
        m.record_commit(50, 120, 2);
        let text = m.render();
        assert!(text.contains("duckwake_epoch{worker=\"w1\",view=\"clicks\"} 7"), "{text}");
        assert!(text.contains("duckwake_dead_letters_total{worker=\"w1\",view=\"clicks\"} 2"), "{text}");
        assert!(text.contains("duckwake_commit_conflicts_total{worker=\"w1\"} 1"), "{text}");
        assert!(text.contains("# TYPE duckwake_freshness_seconds gauge"), "{text}");
        m.forget("clicks");
        assert!(!m.render().contains("view=\"clicks\""));
    }
}
