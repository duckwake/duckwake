#!/usr/bin/env bash
# Summarizes a soak run (scripts/soak_test.sh), during or after it: per time bucket, commit latency from the
# worker's epoch log lines, plus lag, freshness, snapshots, catalog size, inlined rows, files and memory from
# the sampled metrics. BUCKET sets the bucket width (default 1 hour).
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${BIN:-target/release/duckwake}
OUT=${OUT:-.lake/soak}
BUCKET=${BUCKET:-1 hour}
export DUCKWAKE_CATALOG="postgres:dbname=soak host=localhost port=55432 user=lake password=lake"
export DUCKWAKE_DATA_PATH="$OUT/data/"
START=$(sed -n 's/^START=//p' "$OUT/params.env")
sed -n 2,3p "$OUT/params.env"
[[ -f $OUT/run.pid ]] && echo "run active, elapsed $(( $(date +%s) - START ))s" || echo "run finished"

"$BIN" sql "
CREATE TEMP TABLE ep AS
  WITH log AS (SELECT unnest(string_split(content, chr(10))) AS line FROM read_text('$OUT/worker.log'))
  SELECT regexp_extract(line, '^(\\S+)', 1)::TIMESTAMPTZ::TIMESTAMP AS ts,
         regexp_extract(line, 'rows_in=(\\d+)', 1)::BIGINT AS rows_in,
         regexp_extract(line, ' commit_ms=(\\d+)', 1)::INT AS commit_ms,
         regexp_extract(line, 'poll_stage_ms=(\\d+)', 1)::INT AS stage_ms,
         regexp_extract(line, 'attempts=(\\d+)', 1)::INT AS attempts
  FROM log WHERE line LIKE '%epoch committed%job=analytics.soak_windows %';
CREATE TEMP TABLE lg AS
  WITH log AS (SELECT unnest(string_split(content, chr(10))) AS line FROM read_text('$OUT/worker.log'))
  SELECT regexp_extract(line, '^(\\S+)', 1)::TIMESTAMPTZ::TIMESTAMP AS ts, line FROM log WHERE line LIKE '20%';
CREATE TEMP TABLE m AS SELECT * FROM read_csv('$OUT/metrics.csv', header = true, timestampformat = '%Y-%m-%d %H:%M:%S');
CREATE TEMP MACRO b(t) AS time_bucket(INTERVAL '$BUCKET', t, make_timestamp(${START}000000));
-- Per bucket: commit latency (ms), throughput, lag, freshness (s) and catalog / lake growth.
SELECT strftime(b(ts), '%m-%d %H:%M') AS bucket, count(*) AS epochs, round(sum(rows_in) / epoch(max(ts) - min(ts) + INTERVAL 10 SECOND)) AS ev_per_s,
       quantile_cont(commit_ms, 0.5)::INT AS commit_p50, quantile_cont(commit_ms, 0.95)::INT AS commit_p95, max(commit_ms) AS commit_max,
       quantile_cont(stage_ms, 0.95)::INT AS stage_p95, count(*) FILTER (attempts > 1) AS retried
FROM ep GROUP BY ALL ORDER BY 1;
SELECT strftime(b(ts), '%m-%d %H:%M') AS bucket, max(win_lag) AS lag_max, quantile_cont(win_freshness_s, 0.95) AS fresh_p95_s,
       last(snapshots ORDER BY ts) AS snapshots, last(checkpoint_rows ORDER BY ts) AS ckpts, last(catalog_mb ORDER BY ts) AS catalog_mb,
       max(inlined_rows) AS inlined_max, last(dead_tuples ORDER BY ts) AS pg_dead_tup, last(spend_files ORDER BY ts) AS spend_files,
       last(win_files ORDER BY ts) AS win_files, last(lake_data_mb ORDER BY ts) AS lake_mb, max(topic_mb) AS topic_mb,
       max(worker_rss_mb) AS rss_mb, last(worker_restarts ORDER BY ts) AS restarts
FROM m GROUP BY ALL ORDER BY 1;
-- Maintenance runs (ms) and warnings per bucket.
SELECT strftime(b(ts), '%m-%d %H:%M') AS bucket,
       count(*) FILTER (line LIKE '% WARN %' AND line NOT LIKE '%maintenance%') AS warns,
       count(*) FILTER (line LIKE '% ERROR %') AS errors,
       string_agg(DISTINCT CASE WHEN line LIKE '%maintenance:%done%' THEN regexp_extract(line, 'task=\"?([a-z ]+)', 1) END, ', ') AS maintenance_ran,
       max(CASE WHEN line LIKE '%maintenance:%done%' THEN regexp_extract(line, 'ms=(\\d+)', 1)::INT END) AS maint_max_ms,
       count(*) FILTER (line LIKE '%maintenance:%failed%') AS maint_failures
FROM lg GROUP BY ALL HAVING warns + errors > 0 OR maintenance_ran IS NOT NULL ORDER BY 1;
"
