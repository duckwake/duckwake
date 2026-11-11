//! Phase A of the Arrow ingest plan: is Arrow decode + `append_record_batch` at least 2x faster than the
//! engine's SQL decode at staging, on the soak's event shape, and does it give the same rows?
//!
//! For each corpus both paths build the tables the engine's `stage` builds (typed rows and dead letters) in
//! an in-memory DuckDB; then the outcomes are compared as multisets. The hostile corpus mixes clean events
//! with every form in the plan's semantics table and random byte mutations, to find disagreements.
//!
//!   cargo run --release -p duckwake_core --example phase_a_arrow_decode [-- EVENTS REPS]
//!
//! `-- produce BROKERS TOPIC MESSAGES SEED` instead writes the hostile corpus to a Kafka topic, for the
//! engine-level differential (`scripts/decode_test.sh`).
use anyhow::Result;
use duckdb::arrow::array::{new_null_array, ArrayRef, RecordBatch};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::{params, Connection};
use duckwake_core::decode::{ArrowDecoder, Decoded};
use duckwake_core::kafka::Msg;
use duckwake_core::model::{SourceDef, SourceOptions};
use duckwake_core::decode::{decode_sql, IS_JSON_OBJECT};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PARTITIONS: usize = 3;
/// Messages per decode call: about one 4 MB Kafka fetch of ~1 KB events.
const FETCH: usize = 4000;
const COLS: &str = "ts, game_id, user_id, amount";

fn source() -> SourceDef {
    let options = SourceOptions::from_json(&serde_json::json!({
        "brokers": "-", "topic": "-", "event_time": "ts",
        "columns": {"ts": "TIMESTAMP", "game_id": "VARCHAR", "user_id": "VARCHAR", "amount": "DECIMAL(18,2)"}
    }))
    .unwrap();
    SourceDef { name: "clicks".into(), kind: "kafka".into(), options }
}

fn ts(rng: &mut StdRng) -> String {
    let base = chrono::NaiveDate::from_ymd_opt(2026, 10, 30).unwrap().and_hms_opt(4, 0, 0).unwrap();
    (base + chrono::Duration::milliseconds(rng.random_range(0..600_000))).format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

/// The producer's event (`duckwake produce`): ~`pad` bytes, malformed every `bad_every`th message.
fn soak_event(rng: &mut StdRng, i: u64, bad_every: u64, pad: &str) -> String {
    let t = ts(rng);
    if bad_every > 0 && i.is_multiple_of(bad_every) {
        return if (i / bad_every).is_multiple_of(2) {
            format!("{{\"ts\": \"{t}\", \"game_id\": \"g1\", \"amount\": ")
        } else {
            format!("{{\"ts\": \"{t}\", \"game_id\": \"g1\", \"amount\": \"not-a-number\"}}")
        };
    }
    format!(
        "{{\"ts\": \"{t}\", \"game_id\": \"g{}\", \"user_id\": \"u{}\", \"amount\": {}.{:02}, \"seq\": {i}, \"pad\": \"{pad}\"}}",
        rng.random_range(0..20),
        rng.random_range(0..5000),
        rng.random_range(0..100),
        rng.random_range(0..100),
    )
}

/// Every row of the plan's semantics table, plus framing and encoding hazards.
fn hostile(rng: &mut StdRng) -> Vec<Option<Vec<u8>>> {
    let t = ts(rng);
    let ev = |extra: &str| format!("{{\"ts\": \"{t}\", \"game_id\": \"g1\", \"user_id\": \"u1\"{extra}}}");
    let mut v: Vec<String> = vec![
        "{\"ts\": \"2026-10-29 06:02:50+07:00\", \"amount\": 1.5}".into(),
        "{\"ts\": 1730000000, \"amount\": 1.5}".into(),
        "{\"ts\": \"2026-10-29T06:02:50\", \"amount\": 1.5}".into(),
        "{\"ts\": \"2026-10-29 06:02:50Z\"}".into(),
        "{\"ts\": \"2026-10-29\"}".into(),
        "{\"ts\": \"2026-10-29 06:02:50.1234567\"}".into(),
        "{\"ts\": \"2026-10-29 06:02:50.123456\"}".into(),
        "{\"ts\": \"2026-02-30 06:02:50\"}".into(),
        "{\"ts\": \"2026-10-29 25:02:50\"}".into(),
        "{\"ts\": null, \"game_id\": \"g1\"}".into(),
        "{\"game_id\": \"g1\"}".into(),
        "{}".into(),
        ev(", \"amount\": \"12.34\""),
        ev(", \"amount\": 12.345"),
        ev(", \"amount\": 12.355"),
        ev(", \"amount\": 1e2"),
        ev(", \"amount\": 1E-2"),
        ev(", \"amount\": -0.5"),
        ev(", \"amount\": -0"),
        ev(", \"amount\": 0.10"),
        ev(", \"amount\": 1e30"),
        ev(", \"amount\": 9999999999999999.99"),
        ev(", \"amount\": 12345678901234567.0"),
        ev(", \"amount\": true"),
        ev(", \"amount\": null"),
        ev(", \"amount\": [1]"),
        ev(", \"amount\": NaN"),
        ev(", \"amount\": 01"),
        ev(", \"amount\": 1.5, \"amount\": 2.5"),
        format!("{{\"ts\": \"{t}\", \"game_id\": 5}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": 5.10}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": true}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": {{\"a\": 1}}}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": [1, 2]}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"g\\u00e9\\n\\\"q\\\"\\t\"}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"\\ud83d\\ude00\"}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"\\ud800\"}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"\\x\"}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"g1\", \"seq\": 01}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"g1\", \"seq\": Infinity}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"g1\", \"seq\": 1.}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"g1\", \"seq\": -}}"),
        format!("{{\"ts\": \"{t}\",\n\t\"game_id\": \"g1\"\r\n}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"a\tb\"}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"\"}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"a\", \"game_id\": \"b\"}}"),
        format!("{{\"ts\": \"{t}\", \"ts\": \"2020-01-01 00:00:00\"}}"),
        format!("{{\"t\\u0073\": \"{t}\"}}"),
        format!("{{\"ts\": \"{t}\", \"nested\": {{\"ts\": \"x\", \"amount\": [{{}}]}}}}"),
        format!("{{\"TS\": \"{t}\"}}"),
        "[1, 2]".into(),
        "[{\"ts\": \"2026-10-29 06:02:50\"}]".into(),
        "5".into(),
        "\"str\"".into(),
        "null".into(),
        "true".into(),
        "".into(),
        "   ".into(),
        "{}{}".into(),
        format!("{{\"ts\": \"{t}\"}}{{\"ts\": \"{t}\"}}"),
        format!("{{\"ts\": \"{t}\"}} trailing"),
        format!("{{\"ts\": \"{t}\"}} "),
        format!("  \n{{\"ts\": \"{t}\"}}\n  "),
        format!("{{\"ts\": \"{t}\",}}"),
        format!("{{'ts': '{t}'}}"),
        format!("{{ts: \"{t}\"}}"),
        format!("{{\"ts\": \"{t}\" /* c */}}"),
        format!("\u{feff}{{\"ts\": \"{t}\"}}"),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"g1\", \"amount\": "),
        format!("{{\"ts\": \"{t}\", \"game_id\": \"g1"),
        "{".into(),
        "}".into(),
    ];
    v.push(ev(""));
    let mut out: Vec<Option<Vec<u8>>> = v.into_iter().map(|s| Some(s.into_bytes())).collect();
    out.push(None);
    out.push(Some(format!("{{\"ts\": \"{t}\", \"game_id\": \"\u{0}\"}}").into_bytes()));
    let mut bad_utf8 = format!("{{\"ts\": \"{t}\", \"game_id\": \"").into_bytes();
    bad_utf8.extend_from_slice(b"\xff\xfe\"}");
    out.push(Some(bad_utf8));
    out
}

fn mutate(rng: &mut StdRng, s: String) -> Vec<u8> {
    let mut b = s.into_bytes();
    match rng.random_range(0..4) {
        0 => b.truncate(rng.random_range(0..b.len())),
        1 => {
            let i = rng.random_range(0..b.len());
            b[i] = rng.random_range(0x20..0x7f);
        }
        2 => {
            if let Some(i) = b.iter().enumerate().filter(|(_, c)| **c == b'"').map(|(i, _)| i).nth(rng.random_range(0..6)) {
                b.remove(i);
            }
        }
        _ => {
            let i = rng.random_range(0..b.len());
            b.insert(i, b"{},:[\"\\"[rng.random_range(0..7)]);
        }
    }
    b
}

/// Messages per partition, offsets contiguous, as a reader would hand them over.
fn partitioned(payloads: Vec<Option<Vec<u8>>>) -> Vec<Vec<Msg>> {
    let mut parts: Vec<Vec<Msg>> = vec![Vec::new(); PARTITIONS];
    for (i, payload) in payloads.into_iter().enumerate() {
        let p = &mut parts[i % PARTITIONS];
        p.push(Msg { partition: (i % PARTITIONS) as i32, offset: p.len() as i64, payload });
    }
    parts
}

fn soak_corpus(n: u64, bad_every: u64, seed: u64) -> Vec<Vec<Msg>> {
    let mut rng = StdRng::seed_from_u64(seed);
    let pad = "x".repeat(1000 - 120);
    partitioned((1..=n).map(|i| Some(soak_event(&mut rng, i, bad_every, &pad).into_bytes())).collect())
}

fn hostile_corpus(n: usize, seed: u64) -> Vec<Vec<Msg>> {
    let mut rng = StdRng::seed_from_u64(seed);
    let pad = "x".repeat(40);
    let mut out = Vec::new();
    // Every hostile form once, each followed by a clean event, so framing hazards meet a neighbour.
    for h in hostile(&mut rng) {
        out.push(h);
        out.push(Some(soak_event(&mut rng, 1, 0, &pad).into_bytes()));
    }
    while out.len() < n {
        let r = rng.random_range(0..100);
        let clean = soak_event(&mut rng, 1, 0, &pad);
        out.push(Some(match r {
            0..70 => clean.into_bytes(),
            70..85 => {
                let h = hostile(&mut rng);
                let i = rng.random_range(0..h.len());
                match h.into_iter().nth(i).unwrap() {
                    Some(b) => b,
                    None => {
                        out.push(None);
                        continue;
                    }
                }
            }
            _ => mutate(&mut rng, clean),
        }));
    }
    partitioned(out)
}

/// Today's `engine::stage` decode, statement for statement.
fn sql_path(conn: &Connection, parts: &[Vec<Msg>]) -> Result<Duration> {
    let t = Instant::now();
    conn.execute_batch("CREATE OR REPLACE TABLE raw (_partition INTEGER, _offset BIGINT, payload VARCHAR)")?;
    {
        let mut app = conn.appender("raw")?;
        for m in parts.iter().flatten() {
            let payload = m.payload.as_deref().map(String::from_utf8_lossy);
            app.append_row(params![m.partition, m.offset, payload.as_deref()])?;
        }
        app.flush()?;
    }
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE ok AS SELECT * FROM raw WHERE {IS_JSON_OBJECT};
         CREATE OR REPLACE TABLE typed_sql AS {decode};
         CREATE OR REPLACE TABLE dead_sql AS
           SELECT _partition, _offset, payload, 'invalid json' AS _err FROM raw WHERE NOT ({IS_JSON_OBJECT})
           UNION ALL SELECT _partition, _offset, payload, _err FROM typed_sql WHERE _err IS NOT NULL;",
        decode = decode_sql(&source(), "ok")
    ))?;
    Ok(t.elapsed())
}

#[derive(Default, Debug, Clone, Copy)]
struct ArrowTimes {
    decode: Duration,
    append: Duration,
    fallback: Duration,
    total: Duration,
    fallback_rows: usize,
    dead_rows: usize,
    bisections: usize,
    bytes_per_row: f64,
}

/// Decodes each partition's fetches (one thread per partition when `parallel`, as the readers would), then
/// appends the batches and sends flagged messages through the SQL decode.
fn arrow_path(conn: &Connection, dec: &ArrowDecoder, parts: &[Vec<Msg>], parallel: bool) -> Result<ArrowTimes> {
    let t0 = Instant::now();
    fn decode_part<'a>(dec: &ArrowDecoder, msgs: &'a [Msg]) -> Result<Vec<(&'a [Msg], Decoded)>> {
        msgs.chunks(FETCH).map(|c| Ok((c, dec.decode(c)?))).collect()
    }
    let decoded: Vec<(&[Msg], Decoded)> = if parallel {
        std::thread::scope(|s| {
            let hs: Vec<_> = parts.iter().map(|p| s.spawn(move || decode_part(dec, p))).collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect::<Result<Vec<_>>>()
        })?
        .into_iter()
        .flatten()
        .collect()
    } else {
        parts.iter().map(|p| decode_part(dec, p)).collect::<Result<Vec<_>>>()?.into_iter().flatten().collect()
    };
    let decode = t0.elapsed();

    let t1 = Instant::now();
    let src = source();
    let cols: Vec<String> = src.options.columns.iter().map(|(c, t)| format!("{c} {t}")).collect();
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE typed_arw (_partition INTEGER, _offset BIGINT, payload VARCHAR, {}, _err VARCHAR)",
        cols.join(", ")
    ))?;
    let mut tm = ArrowTimes { decode, ..Default::default() };
    let (mut bytes, mut rows) = (0usize, 0usize);
    {
        let mut app = conn.appender("typed_arw")?;
        for (_, d) in &decoded {
            let b = &d.batch;
            bytes += b.get_array_memory_size();
            rows += b.num_rows();
            // The staging table also has `payload` and `_err`, both NULL for fast rows.
            let mut fields: Vec<Arc<Field>> = b.schema().fields().iter().cloned().collect();
            let mut arrays: Vec<ArrayRef> = b.columns().to_vec();
            let null = new_null_array(&DataType::Utf8, b.num_rows());
            fields.insert(2, Arc::new(Field::new("payload", DataType::Utf8, true)));
            arrays.insert(2, null.clone());
            fields.push(Arc::new(Field::new("_err", DataType::Utf8, true)));
            arrays.push(null);
            app.append_record_batch(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)?;
            tm.fallback_rows += d.fallback.len();
            tm.dead_rows += d.dead.len();
            tm.bisections += d.bisections;
        }
        app.flush()?;
    }
    tm.append = t1.elapsed();

    let t2 = Instant::now();
    conn.execute_batch("CREATE OR REPLACE TABLE fb (_partition INTEGER, _offset BIGINT, payload VARCHAR, dead BOOLEAN)")?;
    if tm.fallback_rows + tm.dead_rows > 0 {
        let mut app = conn.appender("fb")?;
        for (msgs, d) in &decoded {
            for (i, dead) in d.fallback.iter().map(|i| (*i, false)).chain(d.dead.iter().map(|i| (*i, true))) {
                let m = &msgs[i];
                let payload = m.payload.as_deref().map(String::from_utf8_lossy);
                app.append_row(params![m.partition, m.offset, payload.as_deref(), dead])?;
            }
        }
        app.flush()?;
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE fb_ok AS SELECT _partition, _offset, payload FROM fb WHERE NOT dead AND {IS_JSON_OBJECT};
             INSERT INTO typed_arw BY NAME {decode};",
            decode = decode_sql(&src, "fb_ok")
        ))?;
    }
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE dead_arw AS
           SELECT _partition, _offset, payload, 'invalid json' AS _err FROM fb WHERE dead OR NOT ({IS_JSON_OBJECT})
           UNION ALL SELECT _partition, _offset, payload, _err FROM typed_arw WHERE _err IS NOT NULL"
    ))?;
    tm.fallback = t2.elapsed();
    tm.total = t0.elapsed();
    tm.bytes_per_row = bytes as f64 / rows.max(1) as f64;
    Ok(tm)
}

/// Rows in one path's outcome but not the other's (typed rows and dead letters, as multisets).
fn differences(conn: &Connection) -> Result<(i64, i64)> {
    let outcome = |s: &str| {
        format!(
            "SELECT _partition, _offset, {COLS}, NULL AS _err, NULL AS payload FROM typed_{s} WHERE _err IS NULL
             UNION ALL SELECT _partition, _offset, NULL, NULL, NULL, NULL, _err, payload FROM dead_{s}"
        )
    };
    let q = |a: &str, b: &str| -> Result<i64> {
        let q = format!("SELECT count(*) FROM ((SELECT * FROM ({})) EXCEPT ALL (SELECT * FROM ({})))", outcome(a), outcome(b));
        Ok(conn.query_row(&q, [], |r| r.get(0))?)
    };
    Ok((q("sql", "arw")?, q("arw", "sql")?))
}

fn show_differences(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "WITH s AS (SELECT _partition, _offset, ts::VARCHAR AS ts, game_id, user_id, amount::VARCHAR AS amount, _err FROM typed_sql WHERE _err IS NULL
                    UNION ALL SELECT _partition, _offset, NULL, NULL, NULL, NULL, _err FROM dead_sql),
              a AS (SELECT _partition, _offset, ts::VARCHAR, game_id, user_id, amount::VARCHAR, _err FROM typed_arw WHERE _err IS NULL
                    UNION ALL SELECT _partition, _offset, NULL, NULL, NULL, NULL, _err FROM dead_arw),
              d AS ((SELECT 'sql' AS side, * FROM s EXCEPT ALL SELECT 'sql', * FROM a)
                    UNION ALL (SELECT 'arrow', * FROM a EXCEPT ALL SELECT 'arrow', * FROM s))
         SELECT d.side, d.ts, d.game_id, d.user_id, d.amount, d._err, r.payload FROM d
         JOIN raw r USING (_partition, _offset) ORDER BY _partition, _offset, side LIMIT 24",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(format!(
            "  {:5} ts={:?} game={:?} user={:?} amount={:?} err={:?}\n        payload={:?}",
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, Option<String>>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, Option<String>>(6)?.map(|p| p.chars().take(120).collect::<String>()),
        ))
    })?;
    for r in rows {
        println!("{}", r?);
    }
    Ok(())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn stats(mut v: Vec<f64>) -> String {
    v.sort_by(f64::total_cmp);
    format!("{:7.1} / {:7.1} / {:7.1}", v[0], v[v.len() / 2], v[v.len() - 1])
}

/// Writes `hostile_corpus(n, seed)` to `topic`, message i to partition i % PARTITIONS, tombstones included.
fn produce_hostile(brokers: &str, topic: &str, n: usize, seed: u64) -> Result<()> {
    use rskafka::client::partition::{Compression, UnknownTopicHandling};
    use rskafka::record::Record;
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let c = duckwake_core::kafka::plain_client(brokers).await?;
        duckwake_core::kafka::ensure_topic(&c, topic, PARTITIONS as i32).await?;
        for (p, msgs) in hostile_corpus(n, seed).into_iter().enumerate() {
            let pc = c.partition_client(topic, p as i32, UnknownTopicHandling::Retry).await?;
            for batch in msgs.chunks(500) {
                let now = chrono::Utc::now();
                let records =
                    batch.iter().map(|m| Record { key: None, value: m.payload.clone(), headers: Default::default(), timestamp: now }).collect();
                pc.produce(records, Compression::NoCompression).await?;
            }
            println!("partition {p}: {} messages", msgs.len());
        }
        anyhow::Ok(())
    })
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).is_some_and(|a| a == "produce") {
        let arg = |i: usize| args.get(i).cloned().unwrap_or_default();
        return produce_hostile(&arg(2), &arg(3), arg(4).parse()?, arg(5).parse()?);
    }
    let events: u64 = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(50_000);
    let reps: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(7);
    let conn = Connection::open_in_memory()?;
    let threads: i64 = conn.query_row("SELECT current_setting('threads')", [], |r| r.get(0))?;
    let dec = ArrowDecoder::new(&source().options)?.expect("soak schema is on the fast list");
    println!("DuckDB threads: {threads}; events per run: {events} (~1 KB); fetch = {FETCH} messages; {reps} runs after a warm-up\n");

    let mut ok = true;
    println!("== differential: hostile corpus (every semantics-table form, framing hazards, random mutations)");
    for seed in [1, 2, 3] {
        let corpus = hostile_corpus(20_000, seed);
        sql_path(&conn, &corpus)?;
        let tm = arrow_path(&conn, &dec, &corpus, true)?;
        let (missing, extra) = differences(&conn)?;
        let dead: i64 = conn.query_row("SELECT count(*) FROM dead_sql", [], |r| r.get(0))?;
        println!(
            "seed {seed}: 20000 messages, {dead} dead letters; arrow fast rows {}, fallback {}, bisections {}; differences: {missing} only in SQL, {extra} only in Arrow",
            20_000 - tm.fallback_rows - tm.dead_rows,
            tm.fallback_rows,
            tm.bisections
        );
        if missing + extra > 0 {
            ok = false;
            show_differences(&conn)?;
            if let Ok(path) = std::env::var("DUMP") {
                conn.execute_batch(&format!(
                    "ATTACH '{path}' AS dump; CREATE OR REPLACE TABLE dump.raw AS FROM raw;
                     CREATE OR REPLACE TABLE dump.typed_sql AS FROM typed_sql; CREATE OR REPLACE TABLE dump.dead_sql AS FROM dead_sql;
                     CREATE OR REPLACE TABLE dump.typed_arw AS FROM typed_arw; CREATE OR REPLACE TABLE dump.dead_arw AS FROM dead_arw; DETACH dump;"
                ))?;
            }
        }
    }

    println!("\n== staging time per epoch, ms (min / median / max)");
    println!("{:>9} | {:^27} | {:^27} | {:^27} | {:>7} | {:>6} | breakdown of parallel median (decode + append + fallback)", "bad rate", "SQL decode (today)", "Arrow, 1 thread", "Arrow, thread per partition", "speedup", "diffs");
    for (label, bad_every) in [("0%", 0u64), ("0.02%", 5000), ("1%", 100)] {
        let corpus = soak_corpus(events, bad_every, 7);
        let (mut s, mut a1, mut ap) = (Vec::new(), Vec::new(), Vec::new());
        let mut last = ArrowTimes::default();
        let mut parts: Vec<ArrowTimes> = Vec::new();
        for rep in 0..=reps {
            let ts = sql_path(&conn, &corpus)?;
            let t1 = arrow_path(&conn, &dec, &corpus, false)?;
            let tp = arrow_path(&conn, &dec, &corpus, true)?;
            if rep > 0 {
                s.push(ms(ts));
                a1.push(ms(t1.total));
                ap.push(ms(tp.total));
                parts.push(tp);
            }
            last = tp;
        }
        let (missing, extra) = differences(&conn)?;
        ok &= missing + extra == 0;
        parts.sort_by_key(|p| p.total);
        let mid = parts[parts.len() / 2];
        let med = |v: &Vec<f64>| {
            let mut v = v.clone();
            v.sort_by(f64::total_cmp);
            v[v.len() / 2]
        };
        println!(
            "{label:>9} | {} | {} | {} | {:>6.1}x | {:>6} | {:.0} + {:.0} + {:.0}; fallback {} dead {} bisections {}",
            stats(s.clone()),
            stats(a1.clone()),
            stats(ap.clone()),
            med(&s) / med(&ap),
            missing + extra,
            ms(mid.decode),
            ms(mid.append),
            ms(mid.fallback),
            last.fallback_rows,
            last.dead_rows,
            last.bisections
        );
        if missing + extra > 0 {
            show_differences(&conn)?;
        }
        if label == "0%" {
            let raw: i64 = conn.query_row("SELECT sum(strlen(payload)) / count(*) FROM raw", [], |r| r.get(0))?;
            println!("{:>9}   memory: {:.0} bytes per decoded row in Arrow vs {raw} bytes of raw payload", "", last.bytes_per_row);
        }
    }
    println!("\n{}", if ok { "decode outcomes identical in every run" } else { "DIFFERENCES FOUND (see above)" });
    Ok(())
}
