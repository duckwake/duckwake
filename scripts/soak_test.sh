#!/usr/bin/env bash
# Soak test from the design's testing strategy: hours at 5k events/s of ~1 KB with maintenance enabled,
# tracking commit latency, freshness, lag, snapshot count, file count and catalog size.
#
# Runs in its own DuckLake catalog database (`soak`), data path and topic, so it does not touch the
# playground's lake. Every SAMPLE_EVERY seconds a row is appended to $OUT/metrics.csv; run
# scripts/soak_report.sh at any time for a summary. Stop early with: kill "$(cat .lake/soak/run.pid)".
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${BIN:-target/release/duckwake}
OUT=${OUT:-.lake/soak}
DURATION=${DURATION:-86400}          # seconds
RATE=${RATE:-5000}                   # events/s
PAD=${PAD:-1000}                     # bytes per event
EPOCH=${EPOCH:-10s}
SAMPLE_EVERY=${SAMPLE_EVERY:-60}
WORKER_FLAGS=${WORKER_FLAGS:-}       # e.g. "--stage-threads 0 --pipeline --shared-readers --lean-commit"
FLUSH_EVERY=${FLUSH_EVERY:-10m}; MERGE_EVERY=${MERGE_EVERY:-1h}; EXPIRE_EVERY=${EXPIRE_EVERY:-1h}
EXPIRE_OLDER_THAN=${EXPIRE_OLDER_THAN:-12h}; CLEANUP_EVERY=${CLEANUP_EVERY:-1h}; PRUNE_EVERY=${PRUNE_EVERY:-1h}
TOPIC=soak_clicks
# Docker's VM disk is small: cap the topic at 1 GiB per partition (~10 min of data at 5 MB/s).
RETENTION_BYTES=${RETENTION_BYTES:-1073741824}
export DUCKWAKE_CATALOG="postgres:dbname=soak host=localhost port=55432 user=lake password=lake"
export DUCKWAKE_DATA_PATH="$OUT/data/"

if [[ -f $OUT/run.pid ]] && kill -0 "$(cat "$OUT/run.pid")" 2>/dev/null; then
  echo "a soak run is already active (pid $(cat "$OUT/run.pid"))"; exit 1
fi
pg() { docker compose exec -T -e PGOPTIONS='-c client_min_messages=warning' postgres psql -q -U lake "$@"; }
rpk() { docker compose exec -T redpanda rpk "$@"; }

docker compose up -d --wait >/dev/null
rm -rf "$OUT"; mkdir -p "$OUT"
echo $$ >"$OUT/run.pid"
START=$(date +%s); END=$((START + DURATION))
cat >"$OUT/params.env" <<P
START=$START
DURATION=$DURATION RATE=$RATE PAD=$PAD EPOCH=$EPOCH WORKER_FLAGS="$WORKER_FLAGS"
FLUSH_EVERY=$FLUSH_EVERY MERGE_EVERY=$MERGE_EVERY EXPIRE_EVERY=$EXPIRE_EVERY EXPIRE_OLDER_THAN=$EXPIRE_OLDER_THAN CLEANUP_EVERY=$CLEANUP_EVERY PRUNE_EVERY=$PRUNE_EVERY
P
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$OUT/events.log"; }

log "fresh catalog database 'soak' and topic $TOPIC (retention ${RETENTION_BYTES} bytes/partition)"
pg -d lake -c "DROP DATABASE IF EXISTS soak" -c "CREATE DATABASE soak" >/dev/null
rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
rpk topic create "$TOPIC" -p 3 -c retention.bytes="$RETENTION_BYTES" -c segment.bytes=67108864 >/dev/null

"$BIN" init >/dev/null
"$BIN" create-source clicks --options "{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
  \"watermark_delay\":\"5 seconds\",\"start\":\"latest\",
  \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}" >/dev/null
"$BIN" create-view analytics.soak_windows --late-data analytics.soak_windows_late --epoch-interval "$EPOCH" \
  --sql "SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, max(amount) AS top
         FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL" >/dev/null
"$BIN" create-view analytics.soak_spend --epoch-interval "$EPOCH" \
  --sql "SELECT ts, game_id, user_id, amount FROM clicks WHERE amount >= 90" >/dev/null
"$BIN" start soak_windows --worker soak >/dev/null; "$BIN" start soak_spend --worker soak >/dev/null
log "views soak_windows (1-minute tumble) and soak_spend (10% filter) on worker 'soak', epoch $EPOCH"

caffeinate -i -w $$ &   # keep the Mac awake while the run is active (closing the lid still sleeps it)

# Supervised worker and producer: restart on exit, count restarts.
(
  while [[ $(date +%s) -lt $END && ! -f $OUT/stop ]]; do
    # shellcheck disable=SC2086
    "$BIN" run ${WORKER_FLAGS:-} --worker soak --flush-every "$FLUSH_EVERY" --merge-every "$MERGE_EVERY" --expire-every "$EXPIRE_EVERY" \
      --expire-older-than "$EXPIRE_OLDER_THAN" --cleanup-every "$CLEANUP_EVERY" --prune-every "$PRUNE_EVERY" >>"$OUT/worker.log" 2>&1 &
    echo $! >"$OUT/worker.pid"; wait $! && rc=0 || rc=$?
    [[ -f $OUT/stop || $(date +%s) -ge $END ]] && break
    log "worker exited with $rc; restarting"; echo x >>"$OUT/worker.restarts"; sleep 2
  done
) &
SUP_W=$!
(
  while [[ $(date +%s) -lt $END && ! -f $OUT/stop ]]; do
    left=$((END - $(date +%s)))
    "$BIN" produce --topic "$TOPIC" --partitions 3 --rate "$RATE" --count $((left * RATE)) --pad-bytes "$PAD" \
      --late-every 2000 --bad-every 5000 >>"$OUT/producer.log" 2>&1 &
    echo $! >"$OUT/producer.pid"; wait $! || true
    [[ -f $OUT/stop || $(date +%s) -ge $END ]] && break
    log "producer exited; restarting"; sleep 2
  done
) &
SUP_P=$!

echo "ts,elapsed_s,win_epoch,win_lag,win_freshness_s,spend_epoch,spend_lag,spend_freshness_s,snapshots,checkpoint_rows,spend_files,win_files,spend_rows,catalog_mb,inlined_rows,dead_tuples,snapshot_change_rows,data_file_rows,worker_rss_mb,lake_data_mb,topic_mb,worker_restarts" >"$OUT/metrics.csv"
LAKE_Q="WITH last AS (SELECT * FROM __duckwake.checkpoints QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY epoch DESC) = 1),
  w AS (SELECT * FROM last WHERE job_id = 'soak_windows'), s AS (SELECT * FROM last WHERE job_id = 'soak_spend')
  SELECT (SELECT epoch FROM w), (SELECT lag FROM w), (SELECT round(epoch(committed_at::TIMESTAMP - max_event_time), 1) FROM w),
         (SELECT epoch FROM s), (SELECT lag FROM s), (SELECT round(epoch(committed_at::TIMESTAMP - max_event_time), 1) FROM s),
         (SELECT count(*) FROM ducklake_snapshots('lake')), (SELECT count(*) FROM __duckwake.checkpoints),
         (SELECT count(*) FROM ducklake_list_files('lake', 'soak_spend', schema => 'analytics')),
         (SELECT count(*) FROM ducklake_list_files('lake', 'soak_windows', schema => 'analytics')),
         (SELECT count(*) FROM analytics.soak_spend)"
PG_Q="SELECT pg_database_size('soak') / 1048576,
  (SELECT coalesce(sum(n_live_tup), 0) FROM pg_stat_user_tables WHERE relname LIKE 'ducklake_inlined_data%'),
  (SELECT coalesce(sum(n_dead_tup), 0) FROM pg_stat_user_tables),
  (SELECT coalesce(sum(n_live_tup), 0) FROM pg_stat_user_tables WHERE relname = 'ducklake_snapshot_changes'),
  (SELECT coalesce(sum(n_live_tup), 0) FROM pg_stat_user_tables WHERE relname = 'ducklake_data_file')"
sample() {
  local lake pgm rss=0 data topic restarts
  lake=$("$BIN" sql --csv "$LAKE_Q" 2>/dev/null | tail -1) || true
  [[ $(tr -cd , <<<"$lake" | wc -c) -eq 10 ]] || lake=",,,,,,,,,,"
  pgm=$(pg -d soak -At -F, -c "$PG_Q" 2>/dev/null) || pgm=",,,,"
  [[ -f $OUT/worker.pid ]] && rss=$(ps -o rss= -p "$(cat "$OUT/worker.pid")" 2>/dev/null | awk '{print int($1/1024)}') || true
  data=$(du -sm "$OUT/data" 2>/dev/null | cut -f1)
  topic=$(docker compose exec -T redpanda du -sm "/var/lib/redpanda/data/kafka/$TOPIC" 2>/dev/null | cut -f1) || topic=
  restarts=0; [[ -f $OUT/worker.restarts ]] && restarts=$(wc -l <"$OUT/worker.restarts")
  echo "$(date -u '+%F %T'),$(($(date +%s) - START)),$lake,$pgm,${rss:-0},${data:-0},${topic},${restarts// /}" >>"$OUT/metrics.csv"
}

finish() {
  touch "$OUT/stop"
  log "stopping"
  kill -TERM "$(cat "$OUT/producer.pid" 2>/dev/null)" 2>/dev/null || true
  kill -TERM "$(cat "$OUT/worker.pid" 2>/dev/null)" 2>/dev/null || true
  wait "$SUP_W" "$SUP_P" 2>/dev/null || true
  sample
  log "done; report: scripts/soak_report.sh"
  rm -f "$OUT/run.pid"
  scripts/soak_report.sh || true
}
trap 'finish; exit 0' INT TERM

log "running for ${DURATION}s at ${RATE}/s; metrics every ${SAMPLE_EVERY}s in $OUT/metrics.csv"
while [[ $(date +%s) -lt $END ]]; do
  sleep "$SAMPLE_EVERY" & wait $!
  sample
done
finish
