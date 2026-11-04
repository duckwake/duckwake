//! Phase 0 spike: checks the DuckLake behaviours the design depends on.
//! Run with the docker-compose stack up: `cargo run --example phase0_commit_semantics`.
use duckdb::Connection;

const LAKE: &str = "ATTACH 'ducklake:postgres:dbname=lake host=localhost port=55432 user=lake password=lake' \
                    AS lake (DATA_PATH '.lake/spike/')";

fn open() -> anyhow::Result<Connection> {
    let c = Connection::open_in_memory()?;
    c.execute_batch("INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;")?;
    c.execute_batch(LAKE)?;
    Ok(c)
}

fn try_sql(c: &Connection, label: &str, sql: &str) {
    match c.execute_batch(sql) {
        Ok(()) => println!("[ok]   {label}"),
        Err(e) => println!("[fail] {label}: {e}"),
    }
}

fn scalar(c: &Connection, sql: &str) -> String {
    c.query_row(sql, [], |r| r.get::<_, String>(0)).unwrap_or_else(|e| format!("ERR {e}"))
}

fn main() -> anyhow::Result<()> {
    std::fs::create_dir_all(".lake/spike")?;
    let a = open()?;
    println!("version: {}", scalar(&a, "SELECT version()"));
    println!("ducklake ext: {}", scalar(&a, "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'"));

    a.execute_batch("DROP SCHEMA IF EXISTS lake.spike CASCADE; CREATE SCHEMA lake.spike;")?;
    try_sql(&a, "VARIANT column", "CREATE TABLE lake.spike.v (k VARCHAR, v VARIANT); INSERT INTO lake.spike.v VALUES ('a', '{\"x\":1}'::JSON::VARIANT);");
    println!("  variant readback: {}", scalar(&a, "SELECT v::VARCHAR FROM lake.spike.v"));
    a.execute_batch(
        "CREATE TABLE lake.spike.jobs (job_id VARCHAR, lease_epoch BIGINT, status VARCHAR);
         INSERT INTO lake.spike.jobs VALUES ('j1', 1, 'running'), ('j2', 1, 'running');
         CREATE TABLE lake.spike.sink (k INT, v INT);
         CREATE TABLE lake.spike.state (k INT, v INT);
         CREATE TABLE lake.spike.ckpt (job_id VARCHAR, epoch BIGINT);
         CREATE TABLE memory.main.delta AS SELECT range::INT AS k, 1 AS v FROM range(5);",
    )?;

    // 1. Writes to two attached databases in one transaction.
    try_sql(&a, "write memory + lake in one txn",
        "BEGIN; INSERT INTO memory.main.delta VALUES (99, 1); INSERT INTO lake.spike.sink VALUES (99, 1); COMMIT;");
    let _ = a.execute_batch("ROLLBACK");

    // 2. One transaction over several lake tables = one snapshot.
    let before = scalar(&a, "SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')");
    try_sql(&a, "multi-table commit",
        "BEGIN;
         UPDATE lake.spike.jobs SET status = 'running' WHERE job_id = 'j1' AND lease_epoch = 1;
         INSERT INTO lake.spike.sink SELECT * FROM memory.main.delta;
         INSERT INTO lake.spike.state SELECT * FROM memory.main.delta;
         INSERT INTO lake.spike.ckpt VALUES ('j1', 1);
         COMMIT;");
    let after = scalar(&a, "SELECT max(snapshot_id)::VARCHAR FROM ducklake_snapshots('lake')");
    println!("  snapshots: before={before} after={after}");
    println!("  changes: {}", scalar(&a, "SELECT changes::VARCHAR FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"));

    // 3. Rows affected by UPDATE inside a transaction (fencing check).
    a.execute_batch("BEGIN")?;
    let n = a.execute("UPDATE lake.spike.jobs SET status='running' WHERE job_id='j1' AND lease_epoch=1", [])?;
    let n0 = a.execute("UPDATE lake.spike.jobs SET status='running' WHERE job_id='j1' AND lease_epoch=7", [])?;
    a.execute_batch("ROLLBACK")?;
    println!("[info] UPDATE rows affected in txn: match={n} nomatch={n0}");

    // 4. Zombie: A starts a commit with lease 1, B takes over (lease 2), A tries to commit.
    let b = open()?;
    a.execute_batch("BEGIN")?;
    let n = a.execute("UPDATE lake.spike.jobs SET status='running' WHERE job_id='j1' AND lease_epoch=1", [])?;
    a.execute("INSERT INTO lake.spike.sink VALUES (1000, 1)", [])?;
    try_sql(&b, "takeover (lease 1 -> 2)", "UPDATE lake.spike.jobs SET lease_epoch = 2 WHERE job_id='j1'");
    match a.execute_batch("COMMIT") {
        Ok(()) => println!("[FAIL] zombie commit LANDED (fenced update matched {n} rows) -> fencing broken"),
        Err(e) => println!("[ok]   zombie commit rejected: {e}"),
    }
    let _ = a.execute_batch("ROLLBACK");
    println!("  sink rows with k=1000: {}", scalar(&a, "SELECT count(*)::VARCHAR FROM lake.spike.sink WHERE k=1000"));

    // 5. Same, but the two workers touch different job rows: should not conflict.
    a.execute_batch("BEGIN")?;
    a.execute("UPDATE lake.spike.jobs SET status='running' WHERE job_id='j1' AND lease_epoch=2", [])?;
    a.execute("INSERT INTO lake.spike.sink VALUES (2000, 1)", [])?;
    try_sql(&b, "other worker updates j2", "UPDATE lake.spike.jobs SET status='running' WHERE job_id='j2'");
    try_sql(&a, "commit after unrelated j2 update", "COMMIT");
    let _ = a.execute_batch("ROLLBACK");

    // 6. MERGE into a lake table.
    try_sql(&a, "MERGE INTO lake table",
        "MERGE INTO lake.spike.state s USING (SELECT range::INT AS k, 10 AS v FROM range(3, 8)) d ON s.k = d.k
         WHEN MATCHED THEN UPDATE SET v = s.v + d.v
         WHEN NOT MATCHED THEN INSERT VALUES (d.k, d.v);");
    println!("  state after merge: {}", scalar(&a, "SELECT string_agg(k || ':' || v, ',' ORDER BY k) FROM lake.spike.state"));

    // 7. Parser access for the view compiler.
    println!("json_serialize_sql: {}", scalar(&a,
        "SELECT json_serialize_sql('SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) WHERE amount > 0 GROUP BY ALL')::VARCHAR"));

    // 8. Options and maintenance calls.
    try_sql(&a, "set inlining limit per table", "CALL lake.set_option('data_inlining_row_limit', 1000, schema => 'spike', table_name => 'sink')");
    println!("  options: {}", scalar(&a, "SELECT string_agg(option_name || '=' || value || '@' || coalesce(scope,'') || ':' || coalesce(scope_entry,''), '; ') FROM lake.options()"));
    try_sql(&a, "flush inlined", "CALL ducklake_flush_inlined_data('lake')");
    try_sql(&a, "merge adjacent", "CALL ducklake_merge_adjacent_files('lake')");
    try_sql(&a, "expire snapshots", "CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL 1 HOUR)");
    try_sql(&a, "cleanup files", "CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL 1 HOUR)");
    println!("  data files: {}", scalar(&a, "SELECT count(*)::VARCHAR FROM ducklake_list_files('lake', 'sink', schema => 'spike')"));
    Ok(())
}
