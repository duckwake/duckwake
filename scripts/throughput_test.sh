#!/usr/bin/env bash
# Gate 1 throughput: ~1 KB events at 5k/s into both views with 10 s epochs and 1-minute windows.
# Reports epoch duration and freshness (now - newest committed event time) per epoch.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-5000}; DURATION=${DURATION:-120}

log "reset lake and topic"; reset_env
log "create source and views"; setup_views 10s "1 MINUTE" "5 seconds"; start_views w1
"$BIN" run $WORKER_FLAGS --worker w1 --flush-every 1m --merge-every 2m >"$OUT/worker.log" 2>&1 &
W=$!
log "produce $((RATE * DURATION)) events of ~1 KB at $RATE/s"
produce --rate "$RATE" --count $((RATE * DURATION)) --pad-bytes 1000 2>&1 | tail -1
sleep 12
"$BIN" jobs
kill -TERM "$W"; wait "$W" || true

log "per-epoch numbers (clicks_per_window)"
"$BIN" sql "SELECT count(*) AS epochs, sum(rows_in) AS rows_in,
              round(sum(rows_in) / epoch(max(committed_at) - min(committed_at)), 0) AS rows_per_s,
              round(quantile_cont(epoch(committed_at::TIMESTAMP - max_event_time), 0.5), 1) AS freshness_p50_s,
              round(quantile_cont(epoch(committed_at::TIMESTAMP - max_event_time), 0.95), 1) AS freshness_p95_s,
              max(lag) AS max_lag
            FROM lake.__duckwake.checkpoints WHERE job_id = 'analytics.clicks_per_window' AND rows_in > 0"
grep "epoch committed" "$OUT/worker.log" | grep clicks_per_window | sed -E 's/.*rows_in=([0-9]+).*stage_ms=([0-9]+) poll_stage_ms=([0-9]+) commit_ms=([0-9]+).*/rows_in=\1 stage_ms=\2 poll_stage_ms=\3 commit_ms=\4/' | tail -8
log "data files per sink table"
"$BIN" sql "SELECT 'clicks_per_window' AS t, count(*) AS files FROM ducklake_list_files('lake', 'clicks_per_window', schema => 'analytics')
            UNION ALL SELECT 'big_spend', count(*) FROM ducklake_list_files('lake', 'big_spend', schema => 'analytics')"
