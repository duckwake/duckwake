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
    claims: u64,
    handoffs: u64,
    views_failed: u64,
    out_of_memory: u64,
    rows_cap: Option<usize>,
    duckdb_version: String,
    /// DuckDB's memory in use, what it has spilled to disk, and its limit, in bytes.
    duckdb_memory: Option<(u64, u64, u64)>,
    /// The address other machines reach this endpoint at, recorded in the worker's heartbeat row.
    advertised: Option<String>,
    /// Every live worker's metrics address, read from the lake, for `GET /sd`.
    targets: Vec<(String, String)>,
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
    cost: Option<f64>,
    state_rows: Option<i64>,
    /// The view's settings, so alerts can compare against them.
    interval_seconds: Option<f64>,
    watermark_delay_seconds: Option<f64>,
    max_state_rows: Option<u64>,
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
const PER_VIEW: [PerView; 15] = [
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
    ("duckwake_view_cost", "gauge", "Share of a worker the view uses (moving average of its staging, source and commit time per second).", |j, _| j.cost),
    ("duckwake_state_rows", "gauge", "Rows in the view's state as last committed (open windows, groups or distinct rows), read every 10 s.", |j, _| j.state_rows.map(|r| r as f64)),
    ("duckwake_epoch_interval_seconds", "gauge", "The view's epoch interval setting.", |j, _| j.interval_seconds),
    ("duckwake_watermark_delay_setting_seconds", "gauge", "The largest watermark_delay of the view's Kafka sources (0 for lake inputs).", |j, _| j.watermark_delay_seconds),
    ("duckwake_max_state_rows", "gauge", "The view's max_state_rows limit, when it has one.", |j, _| j.max_state_rows.map(|r| r as f64)),
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

    /// The view's cost, the share of the worker it uses.
    pub fn set_cost(&self, job: &str, cost: f64) {
        self.lock().jobs.entry(job.to_string()).or_default().cost = Some(cost);
    }

    /// The settings of a view this worker started.
    pub fn set_view_config(&self, job: &str, interval_seconds: f64, watermark_delay_seconds: f64, max_state_rows: Option<u64>) {
        let mut st = self.lock();
        let j = st.jobs.entry(job.to_string()).or_default();
        j.interval_seconds = Some(interval_seconds);
        j.watermark_delay_seconds = Some(watermark_delay_seconds);
        j.max_state_rows = max_state_rows;
    }

    /// Rows in the view's state as last committed.
    pub fn set_state_rows(&self, job: &str, rows: i64) {
        self.lock().jobs.entry(job.to_string()).or_default().state_rows = Some(rows);
    }

    /// This worker marked a view failed: a limit it went over, an input that broke its promise, or a start
    /// that failed.
    pub fn record_view_failed(&self) {
        self.lock().views_failed += 1;
    }

    /// An epoch ran out of memory; the worker went on with at most `cap` rows per epoch.
    pub fn record_out_of_memory(&self, cap: usize) {
        let mut st = self.lock();
        st.out_of_memory += 1;
        st.rows_cap = Some(cap);
    }

    /// This worker took over an orphaned view (failover).
    pub fn record_claim(&self) {
        self.lock().claims += 1;
    }

    /// This worker handed one of its views to another (rebalance, drain or pin).
    pub fn record_move(&self) {
        self.lock().handoffs += 1;
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

    /// The address this endpoint is reached at, as recorded in the lake for service discovery.
    pub fn advertised(&self) -> Option<String> {
        self.lock().advertised.clone()
    }

    /// Every live worker's metrics address (worker id, host:port), served at `GET /sd`.
    pub fn set_targets(&self, targets: Vec<(String, String)>) {
        self.lock().targets = targets;
    }

    /// DuckDB's memory in use, spilled to its temp directory, and its limit, in bytes.
    pub fn set_duckdb_memory(&self, used: u64, spilled: u64, limit: u64) {
        self.lock().duckdb_memory = Some((used, spilled, limit));
    }

    /// `duckwake_build_info`: this build's version and the DuckDB library's, as labels.
    pub fn set_build(&self, duckdb: &str) {
        self.lock().duckdb_version = duckdb.to_string();
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
        let load: f64 = st.jobs.values().filter_map(|j| j.cost).fold(0.0, |a, c| a + c);
        let worker: [(&'static str, &'static str, &'static str, f64); 14] = [
            ("duckwake_worker_up", "gauge", "1 while the worker runs, 0 while it waits to restart after a failure.", if st.up { 1.0 } else { 0.0 }),
            ("duckwake_worker_restarts_total", "counter", "Times the worker failed and restarted from its last checkpoint.", st.restarts as f64),
            ("duckwake_views", "gauge", "Views this worker runs.", st.jobs.len() as f64),
            ("duckwake_epochs_total", "counter", "Epochs committed (one transaction each).", st.epochs as f64),
            ("duckwake_commit_seconds_sum", "counter", "Time spent in epoch commits.", st.commit_seconds_sum),
            ("duckwake_commit_seconds", "gauge", "Duration of the last epoch commit.", st.last_commit_seconds),
            ("duckwake_poll_stage_seconds", "gauge", "Poll and stage time of the last epoch, all views.", st.last_stage_seconds),
            ("duckwake_commit_conflicts_total", "counter", "DuckLake write conflicts retried by epoch commits.", st.commit_conflicts as f64),
            ("duckwake_worker_load", "gauge", "Sum of the view costs: the share of the worker its views use.", load),
            ("duckwake_failover_claims_total", "counter", "Orphaned views this worker took over from dead or unassigned owners.", st.claims as f64),
            ("duckwake_handoffs_total", "counter", "Views this worker handed to another worker (rebalance, drain, pins).", st.handoffs as f64),
            ("duckwake_views_failed_total", "counter", "Views this worker marked failed (a limit, a broken input promise, a failed start).", st.views_failed as f64),
            ("duckwake_out_of_memory_total", "counter", "Epochs that ran out of memory; each halves the rows per epoch and restarts from the checkpoints.", st.out_of_memory as f64),
            ("duckwake_epoch_rows_cap", "gauge", "Most rows per epoch after running out of memory (0: no cap, each view's max_rows).", st.rows_cap.unwrap_or(0) as f64),
        ];
        out.extend(worker.into_iter().map(|(name, kind, help, value)| Sample { name, kind, help, view: None, value }));
        let read = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed) as f64;
        let p = crate::process::usage();
        let duck = st.duckdb_memory;
        let process: [(&'static str, &'static str, &'static str, Option<f64>); 10] = [
            ("process_cpu_seconds_total", "counter", "User and system CPU time of the worker process.", Some(p.cpu_seconds)),
            ("process_resident_memory_bytes", "gauge", "Resident memory of the worker process.", p.resident_bytes.map(|b| b as f64)),
            ("process_resident_memory_max_bytes", "gauge", "Peak resident memory of the worker process.", p.max_resident_bytes.map(|b| b as f64)),
            ("process_threads", "gauge", "Threads of the worker process.", p.threads.map(|t| t as f64)),
            ("process_open_fds", "gauge", "Open file descriptors of the worker process.", p.open_fds.map(|f| f as f64)),
            ("duckwake_duckdb_memory_bytes", "gauge", "Memory DuckDB holds (buffer manager), read every refresh.", duck.map(|d| d.0 as f64)),
            ("duckwake_duckdb_spill_bytes", "gauge", "What DuckDB has spilled to its temp directory.", duck.map(|d| d.1 as f64)),
            ("duckwake_duckdb_memory_limit_bytes", "gauge", "DuckDB's memory limit (--memory-limit, or 80% of RAM).", duck.map(|d| d.2 as f64)),
            ("duckwake_kafka_read_bytes_total", "counter", "Payload and key bytes this process's readers fetched from Kafka.", Some(read(&crate::kafka::READ_BYTES))),
            ("duckwake_kafka_read_messages_total", "counter", "Messages this process's readers fetched from Kafka.", Some(read(&crate::kafka::READ_MESSAGES))),
        ];
        out.extend(
            process
                .into_iter()
                .filter_map(|(name, kind, help, v)| v.map(|value| Sample { name, kind, help, view: None, value })),
        );
        out
    }

    /// The Prometheus text format.
    pub fn render(&self) -> String {
        let (w, duckdb) = {
            let st = self.lock();
            (escape(&st.worker), escape(&st.duckdb_version))
        };
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP duckwake_build_info The DuckWake version and the DuckDB library it runs.\n# TYPE duckwake_build_info gauge\n\
             duckwake_build_info{{worker=\"{w}\",version=\"{}\",duckdb=\"{duckdb}\"}} 1",
            env!("CARGO_PKG_VERSION")
        );
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

    /// Serves `GET /metrics` (this registry), `GET /healthz` and `GET /sd` (Prometheus HTTP service discovery:
    /// every live worker's endpoint) on `addr`, on the runtime `rt`. The address recorded for discovery is
    /// `DUCKWAKE_METRICS_ADVERTISE`, else `addr` with a wildcard host replaced by this machine's name.
    pub fn serve_http(self: &Arc<Self>, addr: &str, rt: &tokio::runtime::Handle) -> anyhow::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = rt
            .block_on(tokio::net::TcpListener::bind(addr))
            .map_err(|e| anyhow::anyhow!("binding the metrics endpoint to {addr}: {e}"))?;
        let port = listener.local_addr()?.port();
        self.lock().advertised = Some(std::env::var("DUCKWAKE_METRICS_ADVERTISE").unwrap_or_else(|_| advertise(addr, port)));
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
                    let request = String::from_utf8_lossy(&buf[..n]).to_string();
                    let path = request.split_whitespace().nth(1).unwrap_or("").to_string();
                    let (status, ctype, body) = match path.as_str() {
                        "/metrics" => ("200 OK", "text/plain; version=0.0.4", metrics.render()),
                        "/healthz" => ("200 OK", "text/plain", "ok".to_string()),
                        "/sd" => ("200 OK", "application/json", metrics.discovery(host_header(&request))),
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

    /// Prometheus HTTP service discovery: one target group per live worker, labelled with its id. Workers bound
    /// to a loopback address are on the machine Prometheus reached this endpoint at (`host`, from the request's
    /// Host header), so they are given that host: one box, any number of workers, no addresses to configure.
    pub fn discovery(&self, host: Option<String>) -> String {
        let st = self.lock();
        let groups: Vec<serde_json::Value> = st
            .targets
            .iter()
            .map(|(worker, addr)| {
                let addr = match (&host, addr.rsplit_once(':')) {
                    (Some(h), Some((a, port))) if is_loopback(a) => format!("{h}:{port}"),
                    _ => addr.clone(),
                };
                serde_json::json!({ "targets": [addr], "labels": { "__meta_duckwake_worker": worker } })
            })
            .collect();
        serde_json::Value::Array(groups).to_string()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The address to record for a metrics endpoint bound to `addr`: a wildcard host (`0.0.0.0`, `[::]`, none)
/// becomes this machine's host name.
fn advertise(addr: &str, port: u16) -> String {
    let host = addr.rsplit_once(':').map_or("", |(h, _)| h);
    if host.is_empty() || host == "0.0.0.0" || host == "[::]" || host == "::" {
        format!("{}:{port}", hostname().unwrap_or_else(|| "localhost".into()))
    } else {
        format!("{host}:{port}")
    }
}

fn hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes at most buf.len() bytes, NUL-terminated when it fits.
    if unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) } != 0 {
        return None;
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    Some(String::from_utf8_lossy(&buf[..end]).into_owned()).filter(|h| !h.is_empty())
}

fn is_loopback(host: &str) -> bool {
    host == "localhost" || host.starts_with("127.") || host == "[::1]" || host == "::1"
}

/// The host part of the request's Host header.
fn host_header(request: &str) -> Option<String> {
    let value = request.lines().find_map(|l| l.split_once(':').filter(|(k, _)| k.eq_ignore_ascii_case("host")).map(|(_, v)| v.trim()))?;
    let host = if value.starts_with('[') { value.split_inclusive(']').next()? } else { value.split(':').next()? };
    Some(host.to_string()).filter(|h| !h.is_empty())
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
        m.set_state_rows("clicks", 42);
        m.record_view_failed();
        m.record_out_of_memory(50_000);
        let text = m.render();
        assert!(text.contains("duckwake_state_rows{worker=\"w1\",view=\"clicks\"} 42"), "{text}");
        assert!(text.contains("duckwake_views_failed_total{worker=\"w1\"} 1"), "{text}");
        assert!(text.contains("duckwake_out_of_memory_total{worker=\"w1\"} 1"), "{text}");
        assert!(text.contains("duckwake_epoch_rows_cap{worker=\"w1\"} 50000"), "{text}");
        assert!(text.contains("duckwake_epoch{worker=\"w1\",view=\"clicks\"} 7"), "{text}");
        assert!(text.contains("duckwake_dead_letters_total{worker=\"w1\",view=\"clicks\"} 2"), "{text}");
        assert!(text.contains("duckwake_commit_conflicts_total{worker=\"w1\"} 1"), "{text}");
        assert!(text.contains("# TYPE duckwake_freshness_seconds gauge"), "{text}");
        m.forget("clicks");
        assert!(!m.render().contains("view=\"clicks\""));
    }

    #[test]
    fn discovery_lists_live_workers_from_the_lake() {
        let m = Metrics::default();
        m.set_targets(vec![("w1".into(), "127.0.0.1:9464".into()), ("w2".into(), "box-b:9464".into())]);
        let sd: serde_json::Value = serde_json::from_str(&m.discovery(Some("box-a".into()))).unwrap();
        assert_eq!(sd[0]["targets"][0], "box-a:9464", "a loopback worker is on the host Prometheus asked");
        assert_eq!(sd[0]["labels"]["__meta_duckwake_worker"], "w1");
        assert_eq!(sd[1]["targets"][0], "box-b:9464");
        assert_eq!(host_header("GET /sd HTTP/1.1\r\nHost: host.docker.internal:9464\r\n\r\n").as_deref(), Some("host.docker.internal"));
        assert_eq!(host_header("GET /sd HTTP/1.1\r\nhost: [::1]:9464\r\n").as_deref(), Some("[::1]"));
        assert_eq!(advertise("127.0.0.1:9464", 9464), "127.0.0.1:9464");
        assert!(!advertise("0.0.0.0:9464", 9464).starts_with("0.0.0.0"));
    }

    /// The alert rules and the dashboard (deploy/) name only series a worker exports.
    #[test]
    fn alerts_and_dashboard_name_real_series() {
        let m = Metrics::default();
        m.set_worker("w1");
        m.record_job(
            "v",
            EpochStats {
                epoch: 1,
                lag: 0,
                watermark_us: Some(0),
                max_event_time_us: Some(0),
                rows_in: 1,
                rows_out: 1,
                rows_late: 0,
                rows_dead: 0,
                stage_ms: 1,
                commit_freshness_s: Some(0.0),
            },
        );
        m.set_cost("v", 0.1);
        m.set_state_rows("v", 1);
        m.set_view_config("v", 2.0, 5.0, Some(10));
        m.set_duckdb_memory(1, 0, 2);
        let text = m.render();
        let known: std::collections::HashSet<&str> =
            text.lines().filter(|l| !l.starts_with('#')).filter_map(|l| l.split('{').next()).collect();
        assert!(known.contains("duckwake_build_info"));
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../deploy");
        for file in ["prometheus/duckwake-alerts.yml", "prometheus/duckwake-alerts.test.yml", "grafana/dashboards/duckwake.json"] {
            let text = std::fs::read_to_string(format!("{root}/{file}")).unwrap();
            // Every `duckwake_...` token but SQL functions (`duckwake_jobs()`) mentioned in the texts.
            let mut names: Vec<&str> = text
                .match_indices("duckwake_")
                .filter_map(|(i, _)| {
                    let rest = &text[i..];
                    let end = rest.find(|c: char| !(c.is_ascii_lowercase() || c == '_')).unwrap_or(rest.len());
                    (!rest[end..].starts_with('(')).then(|| &rest[..end])
                })
                .collect();
            names.sort();
            names.dedup();
            assert!(!names.is_empty(), "{file} names no series");
            for n in names {
                assert!(known.contains(n), "{file} uses {n}, which no worker exports");
            }
        }
    }
}
