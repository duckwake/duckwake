#!/usr/bin/env bash
# stream_reset: rewind both views to an earlier epoch, replay from its offsets and check the result is
# unchanged. Windowed state is restored from that epoch's DuckLake snapshot (time travel). Run after
# throughput_test.sh or crash_test.sh, on the lake they leave behind.
source "$(dirname "$0")/lib.sh"
TO=${TO:-6}
show() {
  "$BIN" sql "SELECT (SELECT count(*) FROM lake.analytics.clicks_per_window) AS window_rows,
                     (SELECT count(*) FROM lake.analytics.big_spend) AS big_spend_rows,
                     (SELECT count(*) FROM lake.analytics.game_totals) AS game_total_rows,
                     (SELECT count(*) FROM lake.__duckwake.\"state_analytics.clicks_per_window\") AS open_state_rows,
                     (SELECT max(epoch) FROM lake.__duckwake.checkpoints) AS last_epoch"
}
mkdir -p "$OUT"
log "before"; show
for v in "${VIEWS[@]}"; do "$BIN" pause "$v"; done
for v in "${VIEWS[@]}"; do "$BIN" reset "$v" --to-epoch "$TO"; done
log "after reset to epoch $TO"; show
start_views w1
log "replay"; drain w1 "$OUT/reset.log"
grep "job started" "$OUT/reset.log" | sed -E 's/.*(job=[a-z_]+ lease=[0-9]+ epoch=[0-9]+ offsets=.*)/\1/'
log "after replay"; show
log "verify"; verify_all
