//! Spike F0 (Change feed sources): what views reading lake tables through DuckLake's change feed rely on,
//! on the Postgres catalog. Run with the docker-compose stack up and an empty catalog database:
//!
//!   docker compose exec -T postgres psql -U lake -d lake -c "DROP DATABASE IF EXISTS f0spike" -c "CREATE DATABASE f0spike"
//!   rm -rf .lake/f0 && cargo run --release --example phase_f0_change_feed
//!
//! 1. Under concurrent writers, a reader that follows the change feed by snapshot position sees every
//!    insert exactly once, and the snapshot ids it sees are gap-free at every poll.
//! 2. Paging a table at an old snapshot by `rowid` returns exactly that version, and the filter is pushed down.
//! 3. The cost of one recent snapshot's changes on a table with many files and deletes, against a full scan.
//! 4. How far `snapshot_time` runs out of order against snapshot ids.
use anyhow::Result;
use duckdb::Connection;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// What the reader thread reports: its final position, polls, gaps, the changes it saw, and duplicates.
type ReaderOut = (i64, usize, usize, HashSet<(i64, i64, i64)>, i64);

fn open() -> Result<Connection> {
    let c = Connection::open_in_memory()?;
    c.execute_batch("INSTALL ducklake; INSTALL postgres; INSTALL icu; LOAD ducklake; LOAD postgres; LOAD icu; SET TimeZone='UTC';")?;
    c.execute_batch("ATTACH 'ducklake:postgres:dbname=f0spike host=localhost port=55432 user=lake password=lake' AS lake (DATA_PATH '.lake/f0/')")?;
    Ok(c)
}

fn int(c: &Connection, sql: &str) -> Result<i64> {
    Ok(c.query_row(sql, [], |r| r.get::<_, Option<i64>>(0))?.unwrap_or(0))
}

fn is_conflict(e: &duckdb::Error) -> bool {
    e.to_string().to_lowercase().contains("conflict")
}

/// Experiment 1. Returns the snapshot ids the writers committed.
fn concurrent_writers() -> Result<()> {
    const WRITERS: i64 = 4;
    const COMMITS: i64 = 150;
    let setup = open()?;
    setup.execute_batch("CREATE SCHEMA lake.f0; CREATE TABLE lake.f0.t (writer BIGINT, seq BIGINT, k BIGINT);")?;
    let start = int(&setup, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")?;
    let done = Arc::new(AtomicBool::new(false));

    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            std::thread::spawn(move || -> Result<(i64, i64)> {
                let c = open()?;
                let (mut retries, mut rows) = (0, 0);
                for seq in 0..COMMITS {
                    // 1 to 40 rows: small inserts are inlined, larger ones write a file.
                    let n = (seq * 7 + w * 13) % 40 + 1;
                    let sql = format!("INSERT INTO lake.f0.t SELECT {w}, {seq}, range FROM range({n})");
                    loop {
                        match c.execute_batch(&sql) {
                            Ok(()) => break,
                            Err(e) if is_conflict(&e) => retries += 1,
                            Err(e) => return Err(e.into()),
                        }
                    }
                    rows += n;
                }
                Ok((rows, retries))
            })
        })
        .collect();

    // The reader: position P, each poll reads the changes in (P, M] for the newest visible snapshot M.
    let reader = {
        let done = done.clone();
        std::thread::spawn(move || -> Result<ReaderOut> {
            let c = open()?;
            let (mut p, mut seen, mut polls, mut gaps, mut dupes) = (start, HashSet::new(), 0usize, 0usize, 0i64);
            loop {
                let last = done.load(Ordering::SeqCst);
                // The cheap way to the newest snapshot; its freshness under concurrent writers is what this checks.
                let m = int(&c, "SELECT id::BIGINT FROM ducklake_current_snapshot('lake')")?;
                if m > p {
                    polls += 1;
                    let visible = int(&c, &format!("SELECT count(*) FROM ducklake_snapshots('lake') WHERE snapshot_id BETWEEN {} AND {m}", p + 1))?;
                    if visible != m - p {
                        gaps += 1;
                        println!("  gap: ids {}..={m} visible {visible} of {}", p + 1, m - p);
                    }
                    let mut stmt = c.prepare(&format!(
                        "SELECT writer, seq, k FROM ducklake_table_changes('lake', 'f0', 't', {}, {m}) WHERE change_type = 'insert'",
                        p + 1
                    ))?;
                    let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?)))?;
                    for row in rows {
                        if !seen.insert(row?) {
                            dupes += 1;
                        }
                    }
                    p = m;
                }
                if last {
                    return Ok((p, polls, gaps, seen, dupes));
                }
            }
        })
    };

    let t = Instant::now();
    let (mut rows, mut retries) = (0, 0);
    for w in writers {
        let (r, x) = w.join().unwrap()?;
        rows += r;
        retries += x;
    }
    let secs = t.elapsed().as_secs_f64();
    done.store(true, Ordering::SeqCst);
    let (p, polls, gaps, seen, dupes) = reader.join().unwrap()?;
    let in_table = int(&setup, "SELECT count(*) FROM lake.f0.t")?;
    println!(
        "1. {WRITERS} writers x {COMMITS} commits in {secs:.1}s ({retries} conflict retries), {rows} rows; reader: {polls} polls up to snapshot {p}"
    );
    println!(
        "   reader saw {} distinct inserts of {in_table} in the table, {dupes} twice; polls with a gap in visible ids: {gaps}",
        seen.len()
    );

    // Experiment 4 on these snapshots: snapshot_time against id order.
    let (inversions, worst_ms) = setup.query_row(
        &format!(
            "SELECT count(*) FILTER (WHERE d < 0), coalesce(-min(d) FILTER (WHERE d < 0), 0) FROM (
               SELECT epoch_us(snapshot_time) - epoch_us(lag(snapshot_time) OVER (ORDER BY snapshot_id)) AS d
               FROM ducklake_snapshots('lake') WHERE snapshot_id > {start})"
        ),
        [],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)? as f64 / 1000.0)),
    )?;
    let running_max_lag_ms = setup.query_row(
        &format!(
            "SELECT coalesce(max(m - t), 0) FROM (SELECT epoch_us(snapshot_time) AS t,
               max(epoch_us(snapshot_time)) OVER (ORDER BY snapshot_id ROWS UNBOUNDED PRECEDING) AS m
             FROM ducklake_snapshots('lake') WHERE snapshot_id > {start})"
        ),
        [],
        |r| Ok(r.get::<_, i64>(0)? as f64 / 1000.0),
    )?;
    println!(
        "4. snapshot_time out of id order: {inversions} of {} consecutive pairs, by up to {worst_ms:.1} ms; a snapshot's time is at most {running_max_lag_ms:.1} ms below the running max",
        p - start - 1
    );
    Ok(())
}

/// Experiment 2: a table changed after S0, read at S0 in pages by rowid.
fn paging_at_a_version() -> Result<()> {
    let c = open()?;
    c.execute_batch(
        "CREATE TABLE lake.f0.big (id BIGINT, v VARCHAR);
         INSERT INTO lake.f0.big SELECT range, 'a' || range FROM range(0, 1000000);
         INSERT INTO lake.f0.big SELECT range, 'b' || range FROM range(1000000, 2000000);
         DELETE FROM lake.f0.big WHERE id % 10 = 3;
         UPDATE lake.f0.big SET v = 'u' || id WHERE id % 10 = 7;",
    )?;
    let s0 = int(&c, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")?;
    c.execute_batch(
        "DELETE FROM lake.f0.big WHERE id % 10 = 1;
         UPDATE lake.f0.big SET v = 'w' || id WHERE id % 10 = 5;
         INSERT INTO lake.f0.big SELECT range, 'c' || range FROM range(2000000, 2500000);",
    )?;
    let at = format!("lake.f0.big AT (VERSION => {s0})");
    let t = Instant::now();
    let full = int(&c, &format!("SELECT count(*) FROM (SELECT rowid, * FROM {at})"))?;
    let full_ms = t.elapsed().as_millis();
    c.execute_batch("CREATE TABLE memory.main.pages (rid BIGINT, id BIGINT, v VARCHAR)")?;
    let (mut r, mut pages, mut page_ms) = (-1i64, 0, Vec::new());
    loop {
        let t = Instant::now();
        let n = c.execute(
            &format!("INSERT INTO memory.main.pages SELECT rowid, id, v FROM {at} WHERE rowid > {r} ORDER BY rowid LIMIT 200000"),
            [],
        )?;
        page_ms.push(t.elapsed().as_millis());
        if n == 0 {
            break;
        }
        pages += 1;
        r = int(&c, "SELECT max(rid) FROM memory.main.pages")?;
    }
    let diff = int(
        &c,
        &format!(
            "SELECT (SELECT count(*) FROM (SELECT rowid, id, v FROM {at} EXCEPT ALL SELECT * FROM memory.main.pages))
                  + (SELECT count(*) FROM (SELECT * FROM memory.main.pages EXCEPT ALL SELECT rowid, id, v FROM {at}))"
        ),
    )?;
    let mut stmt = c.prepare(&format!("EXPLAIN SELECT rowid, id FROM {at} WHERE rowid > 1500000 ORDER BY rowid LIMIT 10"))?;
    let plan: String = stmt.query_map([], |r| r.get::<_, String>(1))?.collect::<duckdb::Result<Vec<_>>>()?.concat();
    println!("{plan}");
    let pushed = plan.contains("Filters") || plan.contains("rowid>");
    println!(
        "2. at snapshot {s0}: {full} rows, full scan {full_ms} ms; {pages} pages of 200k, page times {page_ms:?} ms; pages vs version differ by {diff} rows; rowid filter in the scan: {pushed}"
    );
    Ok(())
}

/// Experiment 3: one recent snapshot's changes on a table with many files and deletes.
fn change_feed_cost() -> Result<()> {
    let c = open()?;
    c.execute_batch("CREATE TABLE lake.f0.many (id BIGINT, v DOUBLE)")?;
    for k in 0..300 {
        c.execute_batch(&format!("INSERT INTO lake.f0.many SELECT range, random() FROM range({}, {})", k * 10_000, (k + 1) * 10_000))?;
        if k % 10 == 9 {
            c.execute_batch(&format!("DELETE FROM lake.f0.many WHERE id % 97 = {}", k % 97))?;
        }
    }
    let files = int(
        &c,
        "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file f JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id)
          WHERE t.table_name = 'many' AND f.end_snapshot IS NULL AND t.end_snapshot IS NULL",
    )?;
    let first = int(&c, "SELECT min(snapshot_id) FROM ducklake_snapshots('lake') WHERE changes::VARCHAR LIKE '%many%'")?;
    c.execute_batch("INSERT INTO lake.f0.many SELECT range, random() FROM range(5000000, 5001000)")?;
    let ins = int(&c, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")?;
    c.execute_batch("DELETE FROM lake.f0.many WHERE id % 1000 = 17")?;
    let del = int(&c, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")?;
    let time = |sql: &str| -> Result<(i64, u128)> {
        let t = Instant::now();
        let n = int(&c, sql)?;
        Ok((n, t.elapsed().as_millis()))
    };
    let feed = |a: i64, b: i64| format!("SELECT count(*) FROM ducklake_table_changes('lake', 'f0', 'many', {a}, {b})");
    let (n_ins, ms_ins) = time(&feed(ins, ins))?;
    let (n_del, ms_del) = time(&feed(del, del))?;
    let (n_all, ms_all) = time(&feed(first, del))?;
    let (n_scan, ms_scan) = time("SELECT count(*) FROM (SELECT * FROM lake.f0.many)")?;
    println!("3. table with {files} live files and 30 deletes: one insert snapshot {n_ins} changes in {ms_ins} ms; one delete snapshot {n_del} changes in {ms_del} ms;");
    println!("   all history {n_all} changes in {ms_all} ms; full scan {n_scan} rows in {ms_scan} ms");
    Ok(())
}

/// Experiment 3b: how one snapshot's changes scale with the table's file count, warm, against a scan.
fn change_feed_scaling() -> Result<()> {
    let c = open()?;
    for files in [10, 100, 300, 1000] {
        let t = format!("lake.f0.files_{files}");
        c.execute_batch(&format!("CREATE TABLE {t} (id BIGINT, v DOUBLE)"))?;
        for k in 0..files {
            c.execute_batch(&format!("INSERT INTO {t} SELECT range, random() FROM range({}, {})", k * 1000, (k + 1) * 1000))?;
        }
        c.execute_batch(&format!("INSERT INTO {t} SELECT range, random() FROM range(0, 100)"))?;
        let s = int(&c, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")?;
        let ms = |sql: &str| -> Result<Vec<u128>> {
            (0..3).map(|_| {
                let t = Instant::now();
                int(&c, sql)?;
                Ok(t.elapsed().as_millis())
            }).collect()
        };
        let feed = ms(&format!("SELECT count(*) FROM ducklake_table_changes('lake', 'f0', 'files_{files}', {s}, {s})"))?;
        let scan = ms(&format!("SELECT count(*) FROM (SELECT * FROM {t})"))?;
        let snaps = ms("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")?;
        println!("3b. {files} files: one snapshot's changes {feed:?} ms; full scan {scan:?} ms; max snapshot id {snaps:?} ms");
    }
    Ok(())
}

fn main() -> Result<()> {
    if std::env::args().any(|a| a == "scaling") {
        return change_feed_scaling();
    }
    concurrent_writers()?;
    paging_at_a_version()?;
    change_feed_cost()?;
    change_feed_scaling()?;
    Ok(())
}
