//! Phase 0 spike, part 2: concurrency between job tables, commit messages, time travel.
//! Run with the docker-compose stack up: `cargo run --example phase0_concurrency`.
use duckdb::Connection;

fn open() -> anyhow::Result<Connection> {
    let c = Connection::open_in_memory()?;
    c.execute_batch("INSTALL ducklake; INSTALL postgres; INSTALL icu; LOAD ducklake; LOAD postgres; LOAD icu; SET TimeZone='UTC';")?;
    c.execute_batch("ATTACH 'ducklake:postgres:dbname=lake host=localhost port=55432 user=lake password=lake' AS lake (DATA_PATH '.lake/spike/')")?;
    Ok(c)
}
fn try_sql(c: &Connection, label: &str, sql: &str) {
    match c.execute_batch(sql) { Ok(()) => println!("[ok]   {label}"), Err(e) => println!("[fail] {label}: {e}") }
}
fn scalar(c: &Connection, sql: &str) -> String {
    c.query_row(sql, [], |r| r.get::<_, String>(0)).unwrap_or_else(|e| format!("ERR {e}"))
}

fn main() -> anyhow::Result<()> {
    let (a, b) = (open()?, open()?);
    a.execute_batch("DROP SCHEMA IF EXISTS lake.spike2 CASCADE; CREATE SCHEMA lake.spike2;
        CREATE TABLE lake.spike2.ckpt (job VARCHAR, epoch BIGINT, offsets JSON);
        CREATE TABLE lake.spike2.lease_a (owner VARCHAR, lease BIGINT); INSERT INTO lake.spike2.lease_a VALUES ('w1', 1);
        CREATE TABLE lake.spike2.lease_b (owner VARCHAR, lease BIGINT); INSERT INTO lake.spike2.lease_b VALUES ('w2', 1);
        CREATE TABLE lake.spike2.sink_a (v INT); CREATE TABLE lake.spike2.sink_b (v INT);")?;

    // Two workers, two jobs: each updates its own lease table and appends to the shared checkpoints table.
    a.execute_batch("BEGIN; UPDATE lake.spike2.lease_a SET lease = lease WHERE owner='w1';
        INSERT INTO lake.spike2.sink_a VALUES (1); INSERT INTO lake.spike2.ckpt VALUES ('a', 1, '{\"p0\": 5}');")?;
    try_sql(&b, "worker b epoch commit", "BEGIN; UPDATE lake.spike2.lease_b SET lease = lease WHERE owner='w2';
        INSERT INTO lake.spike2.sink_b VALUES (1); INSERT INTO lake.spike2.ckpt VALUES ('b', 1, '{\"p0\": 7}'); COMMIT;");
    try_sql(&a, "worker a epoch commit (concurrent append to shared ckpt table)", "COMMIT");
    let _ = a.execute_batch("ROLLBACK");
    println!("  ckpt rows: {}", scalar(&a, "SELECT string_agg(job || ':' || (offsets->>'p0'), ',' ORDER BY job) FROM lake.spike2.ckpt"));

    // Maintenance-style delete on ckpt racing an epoch insert.
    a.execute_batch("BEGIN; INSERT INTO lake.spike2.ckpt VALUES ('a', 2, '{}');")?;
    try_sql(&b, "prune ckpt rows", "DELETE FROM lake.spike2.ckpt WHERE epoch < 1");
    try_sql(&a, "epoch commit after prune (no rows deleted)", "COMMIT");
    let _ = a.execute_batch("ROLLBACK");
    a.execute_batch("BEGIN; INSERT INTO lake.spike2.ckpt VALUES ('a', 3, '{}');")?;
    try_sql(&b, "prune ckpt rows (deletes 1 row)", "DELETE FROM lake.spike2.ckpt WHERE job='b'");
    try_sql(&a, "epoch commit after real prune", "COMMIT");
    let _ = a.execute_batch("ROLLBACK");

    // Commit message on the epoch snapshot, then time travel to it.
    try_sql(&a, "commit with message", "BEGIN; CALL lake.set_commit_message('duckwake', 'epoch 7', extra_info => '{\"job\":\"a\",\"epoch\":7}');
        INSERT INTO lake.spike2.sink_a VALUES (7); COMMIT;");
    println!("  snapshot row: {}", scalar(&a, "SELECT snapshot_id || ' ' || coalesce(commit_message,'-') || ' ' || coalesce(commit_extra_info,'-') FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"));
    let snap = scalar(&a, "SELECT snapshot_id::VARCHAR FROM ducklake_snapshots('lake') WHERE commit_extra_info->>'epoch' = '7' ORDER BY snapshot_id DESC LIMIT 1");
    a.execute_batch("INSERT INTO lake.spike2.sink_a VALUES (8)")?;
    println!("  sink_a now: {} / at epoch 7 snapshot {snap}: {}", scalar(&a, "SELECT string_agg(v::VARCHAR, ',' ORDER BY v) FROM lake.spike2.sink_a"),
        scalar(&a, &format!("SELECT string_agg(v::VARCHAR, ',' ORDER BY v) FROM lake.spike2.sink_a AT (VERSION => {snap})")));

    // MERGE with NULL-safe keys and NULL partial sums.
    a.execute_batch("CREATE TABLE lake.spike2.st (ws TIMESTAMP, k VARCHAR, s DECIMAL(38,2), m INT);
        INSERT INTO lake.spike2.st VALUES ('2026-01-01 00:00', NULL, 1.5, 3), ('2026-01-01 00:00', 'x', NULL, 9);")?;
    try_sql(&a, "MERGE null-safe", "MERGE INTO lake.spike2.st s USING (SELECT TIMESTAMP '2026-01-01 00:00' AS ws, k, s, m FROM (VALUES (NULL, 2.0, 1), ('x', 1.0, 10), ('y', 4.0, 2)) t(k, s, m)) p
        ON s.ws = p.ws AND s.k IS NOT DISTINCT FROM p.k
        WHEN MATCHED THEN UPDATE SET s = coalesce(s.s + p.s, s.s, p.s), m = least(s.m, p.m)
        WHEN NOT MATCHED THEN INSERT (ws, k, s, m) VALUES (p.ws, p.k, p.s, p.m);");
    println!("  st: {}", scalar(&a, "SELECT string_agg(coalesce(k,'NULL') || '=' || coalesce(s::VARCHAR,'NULL') || '/' || m, ',' ORDER BY k NULLS FIRST) FROM lake.spike2.st"));
    println!("  least(NULL,1) = {}", scalar(&a, "SELECT coalesce(least(NULL::INT, 1)::VARCHAR, 'NULL')"));

    try_sql(&a, "expire snapshots (icu)", "CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL 1 HOUR)");
    try_sql(&a, "cleanup files (icu)", "CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL 1 HOUR)");
    try_sql(&a, "partitioned sink", "CREATE TABLE lake.spike2.p (window_start TIMESTAMP, v INT); ALTER TABLE lake.spike2.p SET PARTITIONED BY (year(window_start), month(window_start), day(window_start));");
    println!("  INTERVAL 1 MINUTE expr: {}", scalar(&a, "SELECT json_serialize_sql('SELECT 1 FROM TUMBLE(c, ts, INTERVAL 1 MINUTE) x')::JSON->'statements'->0->'node'->'from_table'"));
    println!("  base table: {}", scalar(&a, "SELECT json_serialize_sql('SELECT 1 FROM \"__t\" AS c')::JSON->'statements'->0->'node'->'from_table'"));
    println!("  explicit group: {}", scalar(&a, "SELECT json_serialize_sql('SELECT a, count(x) FROM t GROUP BY a HAVING count(*) > 1 ORDER BY 1 LIMIT 3')::JSON->'statements'->0->'node'"));
    Ok(())
}
