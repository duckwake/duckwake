#!/usr/bin/env bash
# Scaling benchmark (step 0 of the parallel-processing plan): N views on one source, served by WORKERS workers,
# under a fixed event rate. For each N in COUNTS: a fresh lake and topic, the views (cycling through the
# playground's mix: tumble + lookup join, hop, filter, global aggregate, tumble), RATE events/s for DURATION s,
# then the time the workers need to commit the rest of the topic. Appends one row per run to $OUT/summary.csv.
#
#   COUNTS="1 2 4 8 16" RATE=20000 DURATION=60 EPOCH=2s WORKERS=1 FLAGS="--stage-threads 4" LABEL=x scripts/scale_bench.sh
#
# Columns: epoch period and freshness (commit time minus newest event time) are taken over the load phase
# after a 10 s warm-up; stage = poll + stage of all views per epoch, commit = the epoch transaction, split
# into its statements (writes) and the COMMIT call; drain_s = seconds from the end of the load until every
# view has committed the whole topic (about one epoch when the worker keeps up); cpu = user + sys over wall.
#
# With CATCHUP=1 the RATE * DURATION events are produced first, then the workers start on the backlog and
# the run measures how fast they get through it: the worker's ceiling with N views (appends to catchup.csv;
# events_per_s counts each event once, view_events_per_s once per view).
source "$(dirname "$0")/lib.sh"
COUNTS=${COUNTS:-1 2 4 8 16}
RATE=${RATE:-20000}; DURATION=${DURATION:-60}; EPOCH=${EPOCH:-2s}; WORKERS=${WORKERS:-1}
FLAGS=${FLAGS:-}; LABEL=${LABEL:-baseline}; PAD=${PAD:-0}; DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-300}
TOPIC=${BENCH_TOPIC:-bench_clicks}
OUT=${BENCH_OUT:-.lake/bench}
export DUCKWAKE_CATALOG="postgres:dbname=duckwake_bench host=localhost port=55432 user=lake password=lake"
export DUCKWAKE_DATA_PATH=".lake/bench/data/"
TEST_DB=duckwake_bench
mkdir -p "$OUT"

TEMPLATES=(
  "tumble_join|SELECT c.window_start, g.studio, count(*) AS clicks, sum(c.amount) AS revenue, max(c.amount) AS top_spend FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) c JOIN dim.games g ON g.game_id = c.game_id GROUP BY ALL|late"
  "hop|SELECT window_start, window_end, game_id, count(*) AS clicks, avg(amount) AS avg_amount FROM HOP(clicks, ts, INTERVAL 30 SECOND, INTERVAL 10 SECOND) GROUP BY ALL|late"
  "filter|SELECT ts, game_id, user_id, amount FROM clicks WHERE amount >= 95|"
  "global|SELECT game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS avg_amount FROM clicks GROUP BY ALL|"
  "tumble|SELECT window_start, game_id, count(*) AS clicks, sum(amount) AS revenue FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL|late"
)

# Views whose latest checkpoint covers the whole topic.
at_end() {
  "$BIN" sql --csv "WITH last AS (SELECT job_id, arg_max(source_offsets, epoch) AS o FROM lake.__duckwake.checkpoints GROUP BY job_id),
      n AS (SELECT job_id, sum(v::BIGINT) AS n FROM (SELECT job_id, unnest(json_extract(o, '\$.clicks.*')) AS v FROM last) GROUP BY job_id)
    SELECT count(*) FILTER (WHERE n = $1) FROM n" 2>/dev/null | tail -1
}

CATCHUP=${CATCHUP:-0}
[[ -f $OUT/catchup.csv ]] || echo "label,views,workers,events,seconds,events_per_s,view_events_per_s,epochs,stage_p50_ms,commit_p50_ms,cpu_pct,rss_mb" >"$OUT/catchup.csv"
[[ -f $OUT/summary.csv ]] || echo "label,views,workers,rate,epochs,rows_per_epoch,stage_p50_ms,commit_p50_ms,writes_p50_ms,commit_call_p50_ms,period_p50_s,period_p95_s,fresh_p50_s,fresh_p95_s,drain_s,cpu_pct,rss_mb" >"$OUT/summary.csv"

for n in $COUNTS; do
  run="$OUT/${LABEL}_${n}$([[ $CATCHUP == 1 ]] && echo _catchup)"
  rm -rf "$run"; mkdir -p "$run"
  log "$LABEL: $n views, $WORKERS worker(s), $RATE events/s for $DURATION s, epoch $EPOCH ${FLAGS:+($FLAGS)}"
  docker compose exec -T redpanda rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
  docker compose exec -T redpanda rpk topic create "$TOPIC" -p "$PARTITIONS" >/dev/null
  reset_lake
  "$BIN" create-source clicks --options "{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
    \"watermark_delay\":\"3 seconds\",\"idle_timeout\":\"30 seconds\",\"decode\":\"${DECODE:-auto}\",
    \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}" >/dev/null
  "$BIN" sql "CREATE SCHEMA dim; CREATE TABLE dim.games AS
      SELECT 'g' || i AS game_id, ['Aurora', 'Basalt', 'Cobalt'][i % 3 + 1] || ' Studio' AS studio FROM range(20) t(i)" >/dev/null
  for i in $(seq 1 "$n"); do
    IFS='|' read -r kind sql late <<<"${TEMPLATES[$(((i - 1) % ${#TEMPLATES[@]}))]}"
    name="v${i}_$kind"
    "$BIN" create-view "bench.$name" --epoch-interval "$EPOCH" --sql "$sql" ${late:+--late-data "bench.${name}_late"} >/dev/null
    "$BIN" start "$name" --worker "w$(((i - 1) % WORKERS + 1))" >/dev/null
  done

  if [[ $CATCHUP == 1 ]]; then
    "$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate 200000 --count $((RATE * DURATION)) \
      --pad-bytes "$PAD" --late-every 100 --bad-every 200 >"$run/producer.log" 2>&1
  fi
  pids=()
  for w in $(seq 1 "$WORKERS"); do
    # shellcheck disable=SC2086
    /usr/bin/time -l "$BIN" run --worker "w$w" --no-maintenance $FLAGS >"$run/worker_$w.log" 2>&1 &
    pids+=($!)
  done
  if [[ $CATCHUP == 1 ]]; then
    end=$(topic_end)
    t0=$(date +%s)
    until [[ "$(at_end "$end")" == "$n" ]] || (($(date +%s) - t0 > DRAIN_TIMEOUT)); do sleep 1; done
    for p in "${pids[@]}"; do kill -TERM "$(pgrep -P "$p")" 2>/dev/null || true; done
    for p in "${pids[@]}"; do wait "$p" || true; done
    python3 - "$run" "$LABEL" "$n" "$WORKERS" "$end" >>"$OUT/catchup.csv" <<'EOF'
import datetime, glob, re, statistics, sys
run, label, n, workers, end = sys.argv[1:]
at = lambda line: datetime.datetime.fromisoformat(line.split()[0].replace('Z', '+00:00')).timestamp()
t0, t1, epochs, stage, commit, cpu, rss = None, None, 0, [], [], 0.0, 0
for f in sorted(glob.glob(f"{run}/worker_*.log")):
    text = re.sub(r'\x1b\[[0-9;]*m', '', open(f).read())
    for line in text.splitlines():
        kv = dict(x.split('=', 1) for x in line.split() if '=' in x)
        if 'worker started' in line:
            t0 = min(t0 or at(line), at(line))
        elif 'epoch commit timing' in line:
            epochs += 1; commit.append(int(kv['commit_ms']))
        elif 'epoch committed' in line and int(kv['rows_in']) > 0:
            t1 = max(t1 or at(line), at(line)); stage.append(int(kv['poll_stage_ms']))
    m = re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys', text)
    if m:
        cpu += (float(m.group(2)) + float(m.group(3))) / float(m.group(1)) * 100
    m = re.search(r'(\d+)\s+maximum resident set size', text)
    if m:
        rss = max(rss, int(m.group(1)) // 2**20)
secs = t1 - t0
print(','.join(map(str, [label, n, workers, end, round(secs, 1), round(int(end) / secs), round(int(end) * int(n) / secs), epochs,
      statistics.median(stage), statistics.median(commit), round(cpu), rss])))
EOF
    tail -1 "$OUT/catchup.csv"
    continue
  fi
  sleep 2
  t_start=$(date +%s)
  "$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate "$RATE" --count $((RATE * DURATION)) \
    --pad-bytes "$PAD" --late-every 100 --bad-every 200 >"$run/producer.log" 2>&1
  t_end=$(date +%s)
  end=$(topic_end)
  drain_s=""
  while (($(date +%s) - t_end < DRAIN_TIMEOUT)); do
    if [[ "$(at_end "$end")" == "$n" ]]; then drain_s=$(($(date +%s) - t_end)); break; fi
    sleep 1
  done
  "$BIN" sql --csv "SELECT job_id, epoch(committed_at) AS at, epoch(committed_at::TIMESTAMP - max_event_time) AS fresh, lag
                    FROM lake.__duckwake.checkpoints WHERE rows_in > 0 ORDER BY committed_at" >"$run/checkpoints.csv" 2>/dev/null
  for p in "${pids[@]}"; do kill -TERM "$(pgrep -P "$p")" 2>/dev/null || true; done
  for p in "${pids[@]}"; do wait "$p" || true; done

  python3 - "$run" "$LABEL" "$n" "$WORKERS" "$RATE" "$t_start" "$t_end" "${drain_s:-timeout}" >>"$OUT/summary.csv" <<'EOF'
import csv, datetime, glob, re, statistics, sys
run, label, n, workers, rate, t0, t1, drain = sys.argv[1:]
t0, t1 = float(t0) + 10, float(t1)
q = lambda xs, p: (sorted(xs)[min(len(xs) - 1, int(p * len(xs)))] if xs else float('nan'))
at = lambda line: datetime.datetime.fromisoformat(line.split()[0].replace('Z', '+00:00')).timestamp()
stage, commit, writes, call, rows, cpu, rss = [], [], [], [], [], 0.0, 0
periods, epochs = [], 0
for f in sorted(glob.glob(f"{run}/worker_*.log")):
    text = re.sub(r'\x1b\[[0-9;]*m', '', open(f).read())
    last = None
    for line in text.splitlines():
        kv = dict(x.split('=', 1) for x in line.split() if '=' in x)
        if 'epoch commit timing' in line:
            t = at(line)
            if t0 <= t <= t1:
                epochs += 1
                commit.append(int(kv['commit_ms'])); writes.append(int(kv['writes_ms'])); call.append(int(kv['commit_call_ms']))
                if last is not None:
                    periods.append(t - last)
            last = t
        elif 'epoch committed' in line and t0 <= at(line) <= t1:
            rows.append(int(kv['rows_in']))
            stage.append(int(kv['poll_stage_ms']))
    m = re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys', text)
    if m:
        cpu += (float(m.group(2)) + float(m.group(3))) / float(m.group(1)) * 100
    m = re.search(r'(\d+)\s+maximum resident set size', text)
    if m:
        rss = max(rss, int(m.group(1)) // 2**20)
fresh = [float(r[2]) for r in csv.reader(open(f"{run}/checkpoints.csv")) if r and t0 <= float(r[1]) <= t1]
print(','.join(map(str, [label, n, workers, rate, epochs, int(q(rows, .5)) if rows else 0, q(stage, .5), q(commit, .5), q(writes, .5),
      q(call, .5), round(q(periods, .5), 2), round(q(periods, .95), 2), round(q(fresh, .5), 1), round(q(fresh, .95), 1), drain,
      round(cpu), rss])))
EOF
  tail -1 "$OUT/summary.csv"
done
if [[ $CATCHUP == 1 ]]; then column -s, -t "$OUT/catchup.csv"; else column -s, -t "$OUT/summary.csv"; fi
