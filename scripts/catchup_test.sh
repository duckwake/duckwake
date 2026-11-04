#!/usr/bin/env bash
# Catch-up throughput with the JSON decode in SQL and in Arrow: one backlog of ~1 KB events, then for each
# decode mode a fresh lake with the three test views and one worker, timed from start until every view has
# committed the whole topic (timed from the worker's start to its last epoch with events). Epochs are full
# (max_rows) throughout, so this is the worker's ceiling, not the producer's rate. Each mode's output is
# verified against the batch recomputation.
source "$(dirname "$0")/lib.sh"
EVENTS=${EVENTS:-600000}
MODES=${MODES:-sql arrow}

log "fresh topic: $EVENTS events of ~1 KB"; reset_env
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate 100000 --count "$EVENTS" --pad-bytes 1000 \
  --late-every 2000 --bad-every 5000 2>&1 | tail -1
end=$(topic_end)

for mode in $MODES; do
  log "decode $mode"
  reset_lake
  DECODE=$mode setup_views 10s "1 MINUTE" "5 seconds" >/dev/null
  start_views w1 >/dev/null
  logf="$OUT/worker_$mode.log"
  /usr/bin/time -l "$BIN" run $WORKER_FLAGS --worker w1 >"$logf" 2>&1 &
  T=$!
  for _ in $(seq 1 900); do
    done=1
    for v in "${VIEWS[@]}"; do [[ "$(committed "$v")" == "$end" ]] || done=0; done
    [[ $done == 1 ]] && break
    sleep 0.5
  done
  kill -TERM "$(pgrep -P "$T")"; wait "$T" || true
  [[ $done == 1 ]] || { echo "FAIL: $mode did not catch up"; exit 1; }
  python3 - "$logf" "$mode" "$end" <<'EOF'
import datetime, re, statistics, sys
logf, mode, end = sys.argv[1], sys.argv[2], int(sys.argv[3])
text = re.sub(r'\x1b\[[0-9;]*m', '', open(logf).read())
at = lambda line: datetime.datetime.fromisoformat(line.split()[0].replace('Z', '+00:00')).timestamp()
epochs, stage, t0, t1 = {}, {}, None, None
for line in text.splitlines():
    if 'worker started' in line:
        t0 = at(line)
    if 'epoch committed' not in line:
        continue
    f = dict(kv.split('=', 1) for kv in line.split() if '=' in kv)
    if int(f['rows_in']) == 0:
        continue
    t1 = at(line)
    epochs.setdefault(f['epoch'] + '/' + f['job'], f)
    stage.setdefault(f['job'], []).append(int(f['stage_ms']))
per_epoch = {}
for f in epochs.values():
    per_epoch.setdefault(f['epoch'], f)
ps = [int(f['poll_stage_ms']) for f in per_epoch.values()]
cm = [int(f['commit_ms']) for f in per_epoch.values()]
rss = re.search(r'(\d+)\s+maximum resident set size', text)
secs = t1 - t0
print(f"{mode}: {end} events in {secs:.1f} s = {end / secs:,.0f} events/s; {len(per_epoch)} epochs")
print(f"  per epoch: poll+stage (all views) median {statistics.median(ps)} ms, commit median {statistics.median(cm)} ms; "
      f"total poll+stage {sum(ps) / 1000:.1f} s, commit {sum(cm) / 1000:.1f} s")
for job, ms in sorted(stage.items()):
    print(f"  {job}: stage median {statistics.median(ms)} ms, max {max(ms)} ms")
if rss:
    print(f"  peak RSS {int(rss.group(1)) / 2**20:,.0f} MB")
EOF
  verify_all | sed 's/^/  /'
done
