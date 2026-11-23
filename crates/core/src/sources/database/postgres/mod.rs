//! PostgreSQL sources (D1 of `docs/design/database-cdc-sources.md`): a source of kind `postgres` names tables
//! of one database, and an ingest job of the same name keeps a mirror table of each in the lake, read from a
//! logical replication slot (`sources::database`). This module holds the definition and what is done to the database:
//! checks, the publication and slot, the type mapping, and reading through DuckDB's postgres extension.
//!
//! Every value crosses as PostgreSQL's text form, from the snapshot copy and from `pgoutput` alike, and is cast
//! to the mirror column's DuckDB type in one place (`cast_sql`), so both paths agree.
pub mod pgoutput;

use crate::lake::{ident, lit, LAKE};
use crate::sources::database::mirror::{cast_sql, MirrorColumn, MirrorTable};
use crate::sources::database::{cast_error, stop, Batch, Change, Position, Read, Reader};
use anyhow::{anyhow, bail, Context, Result};
use duckdb::Connection;
use pgoutput::{format_lsn, parse_lsn, Message, Snapshot};
use serde::{Deserialize, Serialize};

pub const KIND: &str = "postgres";

/// Settings every session the source opens gets, so the text forms are the ones `cast_sql` reads: ISO dates,
/// UTC times, hex bytea and exact floats.
const SESSION_OPTIONS: &str =
    "-c DateStyle=ISO,MDY -c IntervalStyle=postgres -c TimeZone=UTC -c bytea_output=hex -c extra_float_digits=1";

/// A PostgreSQL source as stored in `__duckwake.sources` (kind `postgres`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgOptions {
    /// libpq connection string, without the password.
    pub dsn: String,
    /// `env:NAME` or `file:/path`; without it libpq's own `PGPASSWORD` or `.pgpass` apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// The lake schema of the mirror tables.
    pub schema: String,
    pub slot: String,
    pub publication: String,
    /// DuckWake created the publication, so dropping the source drops it.
    #[serde(default)]
    pub publication_created: bool,
    pub epoch_interval: String,
    /// Most changes one epoch reads from the slot (whole transactions: a larger one is read whole).
    pub max_changes: usize,
    pub tables: Vec<MirrorTable>,
}

impl PgOptions {
    /// The connection string DuckWake connects with: the user's, the password resolved, and the session settings.
    pub fn conninfo(&self) -> Result<String> {
        let mut s = self.dsn.trim().to_string();
        if let Some(p) = &self.password {
            s.push_str(&format!(" password={}", conn_value(&crate::secrets::resolve("password", p)?)));
        }
        if !s.contains("application_name=") {
            s.push_str(" application_name=duckwake");
        }
        s.push_str(&format!(" options={}", conn_value(SESSION_OPTIONS)));
        Ok(s)
    }

    pub fn interval(&self) -> Result<std::time::Duration> {
        crate::model::parse_duration(&self.epoch_interval)
    }
}

/// A value of a libpq `key=value` connection string.
fn conn_value(v: &str) -> String {
    format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The DuckDB name a process attaches source `name`'s database under.
pub fn alias(name: &str) -> String {
    ident(&format!("__duckwake_pg_{name}"))
}

/// Attaches the source's database (again, when `fresh`, so a broken connection is replaced).
pub fn attach(conn: &Connection, name: &str, opts: &PgOptions, fresh: bool) -> Result<String> {
    let a = alias(name);
    if fresh {
        conn.execute_batch(&format!("DETACH DATABASE IF EXISTS {a}"))?;
    }
    conn.execute_batch(&format!("ATTACH IF NOT EXISTS {} AS {a} (TYPE postgres)", lit(&opts.conninfo()?)))
        .with_context(|| format!("connecting to source {name}'s database ({})", opts.dsn))?;
    Ok(a)
}

/// Rows of a query run on the source's database, every column read as text.
pub fn rows(conn: &Connection, alias: &str, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let mut stmt = conn.prepare(&format!("SELECT * FROM postgres_query({}, {})", lit(alias.trim_matches('"')), lit(sql)))?;
    let n = stmt.column_count();
    let rows = stmt.query_map([], |r| (0..n).map(|i| r.get::<_, Option<String>>(i)).collect::<duckdb::Result<Vec<_>>>())?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

/// The one row of a query run on the source's database.
pub fn row(conn: &Connection, alias: &str, sql: &str) -> Result<Vec<Option<String>>> {
    rows(conn, alias, sql)?.pop().ok_or_else(|| anyhow!("no row from {sql}"))
}

pub fn execute(conn: &Connection, alias: &str, sql: &str) -> Result<()> {
    conn.execute_batch(&format!("CALL postgres_execute({}, {})", lit(alias.trim_matches('"')), lit(sql)))?;
    Ok(())
}

/// The DuckDB type of a PostgreSQL column. Types without a counterpart DuckLake stores exactly (arrays, enums,
/// ranges, network addresses, `interval`, whose negative values Parquet refuses, `timetz`, whose offset
/// Parquet drops, ...) keep their text form in a VARCHAR column.
pub fn duck_type(oid: u32, typmod: i32) -> String {
    match oid {
        16 => "BOOLEAN".into(),
        20 => "BIGINT".into(),
        21 => "SMALLINT".into(),
        23 => "INTEGER".into(),
        26 => "UINTEGER".into(),
        700 => "FLOAT".into(),
        701 => "DOUBLE".into(),
        1700 => {
            // typmod: ((precision << 16) | scale) + 4; -1 when unconstrained.
            if typmod >= 4 {
                let p = ((typmod - 4) >> 16) & 0xffff;
                let s = (typmod - 4) & 0x7ff;
                if (1..=38).contains(&p) && s <= p {
                    return format!("DECIMAL({p},{s})");
                }
            }
            "DOUBLE".into()
        }
        1082 => "DATE".into(),
        1083 => "TIME".into(),
        1114 => "TIMESTAMP".into(),
        1184 => "TIMESTAMPTZ".into(),
        2950 => "UUID".into(),
        114 | 3802 => "JSON".into(),
        17 => "BLOB".into(),
        _ => "VARCHAR".into(),
    }
}

/// The SQL that reads a table's rows under the transaction's snapshot, each column in its output function's
/// text form, as `pgoutput` sends it (`::text` would trim a `char(n)`'s padding).
pub fn copy_sql(t: &MirrorTable) -> String {
    let cols: Vec<String> = t
        .columns
        .iter()
        .map(|c| format!("CASE WHEN {q} IS NULL THEN NULL ELSE format('%s', {q}) END AS {q}", q = ident(&c.name)))
        .collect();
    format!("SELECT {} FROM {}.{}", cols.join(", "), ident(&t.schema), ident(&t.name))
}

/// The source definition from the options a user gives `create_source`, before its tables are read.
fn parse_input(name: &str, v: &serde_json::Value) -> Result<(PgOptions, Vec<(String, String)>)> {
    let obj = v.as_object().context("options must be a JSON object")?;
    const KNOWN: [&str; 8] = ["dsn", "password", "tables", "schema", "slot", "publication", "epoch_interval", "max_changes"];
    if let Some(k) = obj.keys().find(|k| !KNOWN.contains(&k.as_str())) {
        bail!("unknown option {k:?} for a postgres source (known: {})", KNOWN.join(", "));
    }
    let s = |k: &str| obj.get(k).map(|v| v.as_str().map(String::from).with_context(|| format!("option {k} must be a string"))).transpose();
    let dsn = s("dsn")?.context("option dsn is required: a libpq connection string, e.g. \"host=db dbname=shop user=duckwake\"")?;
    if dsn.contains("password=") {
        bail!("put the password in option password as env:NAME or file:/path, not in dsn, so it is never stored in the lake");
    }
    if dsn.contains("options=") {
        bail!("dsn must not set options: DuckWake sets the session's DateStyle, TimeZone and output formats itself");
    }
    let password = s("password")?;
    if let Some(p) = &password {
        crate::secrets::check_reference("password", p)?;
    }
    let tables = match obj.get("tables") {
        Some(serde_json::Value::Array(a)) if !a.is_empty() => a
            .iter()
            .map(|t| {
                let t = t.as_str().context("tables must be names like \"public.orders\"")?;
                Ok(match t.split_once('.') {
                    Some((s, n)) => (s.to_string(), n.to_string()),
                    None => ("public".to_string(), t.to_string()),
                })
            })
            .collect::<Result<Vec<_>>>()?,
        _ => bail!("option tables is required: the tables to mirror, e.g. [\"public.orders\"]"),
    };
    let schema = s("schema")?.unwrap_or_else(|| name.to_string());
    crate::lake::check_name("schema", &schema)?;
    let slot = s("slot")?.unwrap_or_else(|| format!("duckwake_{name}"));
    if slot.is_empty() || slot.len() > 63 || !slot.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        bail!("slot {slot:?}: PostgreSQL slot names are lower-case letters, digits and underscores, at most 63");
    }
    let publication = s("publication")?.unwrap_or_else(|| format!("duckwake_{name}"));
    let epoch_interval = s("epoch_interval")?.unwrap_or_else(|| "2s".into());
    crate::model::parse_duration(&epoch_interval)?;
    let max_changes = match obj.get("max_changes") {
        None => 100_000,
        Some(v) => v.as_u64().filter(|n| *n > 0).context("max_changes must be a positive integer")? as usize,
    };
    Ok((
        PgOptions { dsn, password, schema, slot, publication, publication_created: false, epoch_interval, max_changes, tables: vec![] },
        tables,
    ))
}

/// Reads a table's columns, key and replica identity, and refuses what the ingest job cannot mirror.
fn read_table(conn: &Connection, a: &str, schema: &str, table: &str, lake_schema: &str) -> Result<MirrorTable> {
    let what = format!("{schema}.{table}");
    crate::lake::check_name("mirrored table", table).with_context(|| format!("{what}: its mirror table takes its name"))?;
    let meta = rows(
        conn,
        a,
        &format!(
            "SELECT c.relkind::text, c.relreplident::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = {} AND c.relname = {}",
            lit(schema),
            lit(table)
        ),
    )?;
    let Some(meta) = meta.first() else { bail!("table {what} does not exist (or this role cannot see it)") };
    match meta[0].as_deref() {
        Some("r") => {}
        Some("p") => bail!("{what} is a partitioned table; mirror its partitions instead"),
        k => bail!("{what} is not a table (relkind {})", k.unwrap_or("?")),
    }
    match meta[1].as_deref() {
        Some("d") | Some("f") => {}
        Some("i") => bail!("{what} has REPLICA IDENTITY USING INDEX: set it to DEFAULT or FULL, so changes carry the primary key"),
        _ => bail!("{what} has REPLICA IDENTITY NOTHING: updates and deletes would not say which row; set it to DEFAULT"),
    }
    let cols = rows(
        conn,
        a,
        &format!(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), a.atttypid::bigint::text, a.atttypmod::text,
                    array_position(i.indkey::int2[], a.attnum)::text
             FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
             WHERE n.nspname = {} AND c.relname = {} AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
             ORDER BY a.attnum",
            lit(schema),
            lit(table)
        ),
    )?;
    let mut columns = Vec::new();
    let mut key: Vec<(i64, String)> = Vec::new();
    for r in cols {
        let name = r[0].clone().context("column name")?;
        let oid: u32 = r[2].as_deref().context("type oid")?.parse()?;
        let typmod: i32 = r[3].as_deref().context("typmod")?.parse()?;
        if let Some(pos) = r[4].as_deref() {
            key.push((pos.parse()?, name.clone()));
        }
        columns.push(MirrorColumn { duck_type: duck_type(oid, typmod), name, source_type: r[1].clone().unwrap_or_default(), oid, typmod });
    }
    if key.is_empty() {
        bail!("{what} has no primary key; a mirror needs one to apply updates and deletes");
    }
    key.sort();
    Ok(MirrorTable {
        schema: schema.into(),
        name: table.into(),
        mirror: format!("{LAKE}.{lake_schema}.{table}"),
        columns,
        key: key.into_iter().map(|(_, k)| k).collect(),
    })
}

/// Checks the server and the tables, creates the publication and the slot when they are missing, then the
/// mirror tables, the source and its ingest job (stopped) in one lake transaction.
pub fn create(conn: &Connection, name: &str, options: &serde_json::Value) -> Result<PgOptions> {
    crate::lake::check_name("source", name)?;
    let (mut opts, wanted) = parse_input(name, options)?;
    if crate::catalog::source_kind(conn, name)?.is_some() {
        bail!("source {name:?} already exists");
    }
    if crate::catalog::job_status(conn, name)?.is_some() {
        bail!("a view named {name:?} exists; a postgres source's ingest job takes the source's name");
    }
    let a = attach(conn, name, &opts, true)?;
    let server = row(
        conn,
        &a,
        "SELECT current_setting('wal_level'), current_setting('server_version_num'), current_setting('server_encoding'),
                (SELECT (rolreplication OR rolsuper)::text FROM pg_roles WHERE rolname = current_user), current_database()::text",
    )?;
    let get = |i: usize| server[i].clone().unwrap_or_default();
    if get(0) != "logical" {
        bail!("the server's wal_level is {}; logical decoding needs wal_level = logical (a restart)", get(0));
    }
    if get(1).parse::<i64>().unwrap_or(0) < 130000 {
        bail!("PostgreSQL {} is too old: 13 or later is needed", get(1));
    }
    if get(2) != "UTF8" {
        bail!("the database's encoding is {}; only UTF8 is supported", get(2));
    }
    if get(3) != "true" {
        bail!("the role needs the REPLICATION attribute (ALTER ROLE ... REPLICATION) to read a replication slot");
    }
    let database = get(4);
    for (s, t) in &wanted {
        let table = read_table(conn, &a, s, t, &opts.schema)?;
        if opts.tables.iter().any(|x| x.name == table.name) {
            bail!("two tables named {t}: their mirrors would collide in lake schema {}", opts.schema);
        }
        if crate::catalog::lake_table_exists(conn, &opts.schema, t)? {
            bail!("lake table {}.{t} exists already; mirror into another schema (option schema)", opts.schema);
        }
        opts.tables.push(table);
    }

    // The publication first: the slot decodes with the catalog as of each change, so a publication created after
    // the slot would be missing for the changes in between.
    let pub_lit = lit(&opts.publication);
    let pubs = rows(
        conn,
        &a,
        &format!("SELECT (pubinsert AND pubupdate AND pubdelete AND pubtruncate)::text FROM pg_publication WHERE pubname = {pub_lit}"),
    )?;
    match pubs.first() {
        Some(p) => {
            if p[0].as_deref() != Some("true") {
                bail!("publication {} must publish inserts, updates, deletes and truncates", opts.publication);
            }
            let published: Vec<(String, String)> = rows(conn, &a, &format!("SELECT schemaname::text, tablename::text FROM pg_publication_tables WHERE pubname = {pub_lit}"))?
                .into_iter()
                .map(|r| (r[0].clone().unwrap_or_default(), r[1].clone().unwrap_or_default()))
                .collect();
            if let Some(t) = opts.tables.iter().find(|t| !published.contains(&(t.schema.clone(), t.name.clone()))) {
                bail!("publication {} does not include {}: ALTER PUBLICATION ... ADD TABLE", opts.publication, t.source_name());
            }
        }
        None => {
            let list: Vec<String> = opts.tables.iter().map(|t| format!("{}.{}", ident(&t.schema), ident(&t.name))).collect();
            execute(conn, &a, &format!("CREATE PUBLICATION {} FOR TABLE {}", ident(&opts.publication), list.join(", ")))
                .with_context(|| format!("creating publication {} (the role must own the tables)", opts.publication))?;
            opts.publication_created = true;
        }
    }
    let slot_lit = lit(&opts.slot);
    let slots = rows(conn, &a, &format!("SELECT plugin::text, database::text, slot_type FROM pg_replication_slots WHERE slot_name = {slot_lit}"))?;
    let slot_created = match slots.first() {
        Some(s) => {
            if s[0].as_deref() != Some("pgoutput") || s[1].as_deref() != Some(database.as_str()) || s[2].as_deref() != Some("logical") {
                bail!("slot {} exists but is not a pgoutput slot of database {database}", opts.slot);
            }
            false
        }
        None => {
            create_slot(conn, &a, &opts.slot)?;
            true
        }
    };

    let res = crate::sources::database::catalog::create(conn, name, KIND, &serde_json::to_value(&opts)?, &opts.schema, &opts.tables);
    if let Err(e) = res {
        // Leave nothing behind on the database for a source that does not exist.
        if slot_created {
            let _ = execute(conn, &a, &format!("SELECT pg_drop_replication_slot({slot_lit})"));
        }
        if opts.publication_created {
            let _ = execute(conn, &a, &format!("DROP PUBLICATION IF EXISTS {}", ident(&opts.publication)));
        }
        return Err(e);
    }
    for t in &opts.tables {
        crate::catalog::set_table_inlining(conn, &opts.schema, &t.name, 10_000)?;
    }
    Ok(opts)
}

pub fn create_slot(conn: &Connection, a: &str, slot: &str) -> Result<()> {
    row(conn, a, &format!("SELECT lsn::text FROM pg_create_logical_replication_slot({}, 'pgoutput')", lit(slot)))
        .with_context(|| format!("creating replication slot {slot} (max_replication_slots may be reached)"))?;
    Ok(())
}

/// Drops a stopped source: its ingest job, its mirror tables, and on the database its slot and the publication
/// DuckWake created. Refused while views read a mirror table. When the database cannot be reached, the lake
/// side is dropped anyway and the message says what to drop there by hand: a slot left behind keeps WAL.
pub fn drop(conn: &Connection, name: &str, opts: &PgOptions) -> Result<String> {
    if crate::catalog::job_status(conn, name)? == Some(crate::model::JobStatus::Running) {
        bail!("stop {name:?} before dropping it");
    }
    let mirrors: Vec<String> = opts.tables.iter().map(|t| t.mirror.clone()).collect();
    let readers = crate::catalog::views_reading_tables(conn, &mirrors)?;
    if !readers.is_empty() {
        bail!("views {} read source {name}'s tables; drop them first", readers.join(", "));
    }
    let on_db = (|| -> Result<()> {
        let a = attach(conn, name, opts, true)?;
        let slot = lit(&opts.slot);
        execute(conn, &a, &format!("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = {slot}"))
            .with_context(|| format!("dropping slot {} (a worker still reading it lets go within an epoch: retry)", opts.slot))?;
        if opts.publication_created {
            execute(conn, &a, &format!("DROP PUBLICATION IF EXISTS {}", ident(&opts.publication)))?;
        }
        Ok(())
    })();
    let note = match on_db {
        Ok(()) => String::new(),
        Err(e) if format!("{e:#}").contains("is active for PID") => return Err(e),
        Err(e) => format!(
            "; could not reach the database ({e:#}): drop slot {} there by hand, SELECT pg_drop_replication_slot('{}'), \
             or it keeps WAL for ever",
            opts.slot, opts.slot
        ),
    };
    crate::sources::database::catalog::drop(conn, name, &opts.tables)?;
    let _ = conn.execute_batch(&format!("DETACH DATABASE IF EXISTS {}", alias(name)));
    Ok(format!("source {name}: dropped with its mirror tables, slot {}{note}", opts.slot))
}

/// An idle source commits its position once it is this far behind, so the slot lets go of the WAL.
const POSITION_BYTES: u64 = 64 << 20;

/// A PostgreSQL source's changes, read from its slot with `pg_logical_slot_peek_binary_changes`, so the database
/// keeps every change until DuckWake has committed it:
///
/// - The position is the end LSN of the last transaction read, or a flush LSN the slot had nothing before.
/// - The slot is advanced to the position only after the commit (`committed`). After a crash in between, the next
///   read advances it to the position first, so the transactions it would send again are gone however many there
///   are; any it still sends end at or before the position and are skipped.
/// - A slot whose confirmed position is past the lake's, gone or invalidated has lost changes: the job stops.
/// - The snapshot is taken after the slot exists, in one REPEATABLE READ transaction whose `pg_current_snapshot()`
///   is kept in the position: transactions it already saw are skipped by id, until every transaction running
///   when it was taken has ended and been read.
pub(crate) struct PgReader {
    job: String,
    alias: String,
    opts: PgOptions,
    /// Relations the slot described, as the source's tables (None: one the source does not mirror).
    relations: std::collections::HashMap<u32, Option<usize>>,
}

/// The transaction a reader is in: whether it applies, its commit time, and its changes.
type Txn = Option<(bool, i64, Vec<Change>)>;

impl PgReader {
    pub(crate) fn open(conn: &Connection, job: &str, opts: &PgOptions) -> Result<Self> {
        Ok(PgReader { job: job.into(), alias: attach(conn, job, opts, true)?, opts: opts.clone(), relations: Default::default() })
    }

    fn slot_lit(&self) -> String {
        lit(&self.opts.slot)
    }

    /// Moves the slot to `lsn`: the database may then drop the WAL before it.
    fn advance_sql(&self, lsn: u64) -> String {
        format!("SELECT end_lsn::text FROM pg_replication_slot_advance({}, {})", self.slot_lit(), lit(&format_lsn(lsn)))
    }

    /// The source table a change belongs to; None for a table the source does not mirror, or a transaction
    /// that does not apply.
    fn table(&self, rel: u32, txn: &Txn) -> Result<Option<usize>> {
        let r = self.relations.get(&rel).context("a change for a relation the slot did not describe")?;
        let Some((applies, _, _)) = txn.as_ref() else { bail!("a change outside a transaction") };
        Ok(r.filter(|_| *applies))
    }

    /// Copies every table in one REPEATABLE READ transaction (a DuckDB transaction is one PostgreSQL
    /// transaction: every postgres_query in it reads the same snapshot) into memory, so the lake transaction
    /// stays short. Returns the snapshot and the slot's confirmed position.
    fn copy_with(&self, conn: &Connection, staging: &dyn Fn(usize) -> String) -> Result<(String, u64)> {
        conn.execute_batch("BEGIN")?;
        let copied = (|| -> Result<(String, u64)> {
            let r = row(
                conn,
                &self.alias,
                &format!(
                    "SELECT pg_current_snapshot()::text, (SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = {})",
                    self.slot_lit()
                ),
            )?;
            let snap = r[0].clone().context("snapshot")?;
            let lsn = r[1].as_deref().map(parse_lsn).transpose()?.unwrap_or(0);
            for (i, t) in self.opts.tables.iter().enumerate() {
                let cols: Vec<String> = t.columns.iter().map(|c| format!("{} AS {}", cast_sql(c, &ident(&c.name)), ident(&c.name))).collect();
                conn.execute_batch(&format!(
                    "CREATE OR REPLACE TABLE {} AS SELECT {} FROM postgres_query({}, {})",
                    staging(i),
                    cols.join(", "),
                    lit(self.alias.trim_matches('"')),
                    lit(&copy_sql(t))
                ))
                .map_err(|e| cast_error(e, t))?;
            }
            Ok((snap, lsn))
        })();
        match copied {
            Ok(c) => {
                conn.execute_batch("COMMIT")?;
                Ok(c)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }
}

impl Reader for PgReader {
    fn backend(&self) -> &'static str {
        "pgoutput"
    }

    fn describe(&self, pos: &Position) -> String {
        format!("slot {} at LSN {}", self.opts.slot, format_lsn(pos.offset))
    }

    fn snapshot(&mut self, conn: &Connection, staging: &dyn Fn(usize) -> String, _batch: &mut Batch) -> Result<(Position, i64)> {
        let slot = rows(conn, &self.alias, &format!("SELECT wal_status FROM pg_replication_slots WHERE slot_name = {}", self.slot_lit()))?;
        match slot.first().map(|r| r[0].as_deref()) {
            Some(Some("lost")) => {
                tracing::warn!(job = %self.job, slot = %self.opts.slot, "slot was invalidated; creating it again for the snapshot");
                execute(conn, &self.alias, &format!("SELECT pg_drop_replication_slot({})", self.slot_lit()))?;
                create_slot(conn, &self.alias, &self.opts.slot)?;
            }
            Some(_) => {}
            None => create_slot(conn, &self.alias, &self.opts.slot)?,
        }
        let (snap, lsn) = self.copy_with(conn, staging)?;
        Ok((Position { offset: lsn, state: serde_json::json!({ "snapshot": snap }) }, 0))
    }

    fn copy(&mut self, conn: &Connection, staging: &dyn Fn(usize) -> String) -> Result<()> {
        self.copy_with(conn, staging).map(|_| ())
    }

    fn read(&mut self, conn: &Connection, pos: &Position, batch: &mut Batch) -> Result<Read> {
        let lsn = pos.offset;
        // Before the read: the flush position (everything before it the read below decodes), and with a
        // snapshot, the oldest transaction still running.
        let r = row(
            conn,
            &self.alias,
            &format!(
                "SELECT pg_current_wal_flush_lsn()::text, pg_current_wal_insert_lsn()::text,
                        pg_snapshot_xmin(pg_current_snapshot())::text, s.confirmed_flush_lsn::text, s.wal_status
                 FROM (SELECT 1) x LEFT JOIN pg_replication_slots s ON s.slot_name = {}",
                self.slot_lit()
            ),
        )?;
        let flush = parse_lsn(r[0].as_deref().context("flush lsn")?)?;
        let insert = parse_lsn(r[1].as_deref().context("insert lsn")?)?;
        let xmin_now: u64 = r[2].as_deref().context("xmin")?.parse()?;
        let reset = format!("reset it to take a new snapshot: duckwake reset {} --to-epoch 0", self.job);
        let Some(confirmed) = r[3].as_deref() else {
            return Err(stop(format!("replication slot {} is gone, so the changes since the last epoch are lost; {reset}", self.opts.slot)));
        };
        if r[4].as_deref() == Some("lost") {
            return Err(stop(format!(
                "replication slot {} was invalidated (max_slot_wal_keep_size), so the changes since the last epoch are lost; {reset}",
                self.opts.slot
            )));
        }
        let confirmed = parse_lsn(confirmed)?;
        if confirmed > lsn {
            return Err(stop(format!(
                "replication slot {} has confirmed {}, past the lake's position {}: something else advanced it, and those changes are lost; {reset}",
                self.opts.slot,
                format_lsn(confirmed),
                format_lsn(lsn)
            )));
        }

        if confirmed < lsn {
            // An epoch committed and the slot was not advanced after it (a crash in between, or a failed
            // advance): advance it now, to what the lake holds. Otherwise, with more than `max_changes` changes
            // between the slot and the position, every read would return only transactions the lake already
            // holds, skip them all, and never get past them.
            row(conn, &self.alias, &self.advance_sql(lsn))?;
        }

        let mut stmt = conn.prepare(&format!(
            "SELECT data FROM postgres_query({}, {})",
            lit(self.alias.trim_matches('"')),
            lit(&format!(
                "SELECT data FROM pg_logical_slot_peek_binary_changes({}, NULL, {}, 'proto_version', '1', 'publication_names', {})",
                self.slot_lit(),
                self.opts.max_changes,
                lit(&self.opts.publication)
            ))
        ))?;
        let messages = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
        let full = messages.len() >= self.opts.max_changes;
        let snapshot_text = pos.state.get("snapshot").and_then(|v| v.as_str()).map(String::from);
        let snapshot = snapshot_text.as_deref().map(Snapshot::parse).transpose()?;
        let mut clear_after = pos.state.get("clear_after").and_then(|v| v.as_str()).map(parse_lsn).transpose()?;
        let (mut end, mut txns, mut skipped, mut changes, mut newest_us) = (lsn, 0u64, 0u64, 0i64, None);
        let mut txn: Txn = None;
        for data in &messages {
            match pgoutput::decode(data)? {
                Message::Begin { final_lsn, commit_us, xid } => {
                    // Applied before a crash (its commit is before the position), or already in the copy.
                    let applies = final_lsn >= lsn && !snapshot.as_ref().is_some_and(|s| s.sees(xid));
                    txn = Some((applies, commit_us, Vec::new()));
                }
                Message::Commit { end_lsn, .. } => {
                    let (applies, commit_us, txn_changes) = txn.take().context("a Commit without its Begin")?;
                    txns += 1;
                    if applies {
                        for c in txn_changes {
                            batch.apply(c)?;
                        }
                        newest_us = Some(commit_us);
                    } else {
                        skipped += 1;
                    }
                    end = end.max(end_lsn);
                }
                Message::Relation { id, schema, name, columns, .. } => {
                    let table = self.opts.tables.iter().position(|t| t.schema == schema && t.name == name);
                    if let Some(i) = table {
                        check_columns(&self.opts.tables[i], &columns)?;
                    }
                    self.relations.insert(id, table);
                }
                Message::Insert { rel, new } => {
                    if let Some(t) = self.table(rel, &txn)? {
                        changes += 1;
                        txn.as_mut().unwrap().2.push(Change::Upsert { table: t, old_key: None, values: new });
                    }
                }
                Message::Update { rel, old, new } => {
                    if let Some(t) = self.table(rel, &txn)? {
                        changes += 1;
                        let old_key = old.map(|o| batch.key(t, &o)).transpose()?;
                        txn.as_mut().unwrap().2.push(Change::Upsert { table: t, old_key, values: new });
                    }
                }
                Message::Delete { rel, old } => {
                    if let Some(t) = self.table(rel, &txn)? {
                        changes += 1;
                        let key = batch.key(t, &old)?;
                        txn.as_mut().unwrap().2.push(Change::Delete { table: t, key });
                    }
                }
                Message::Truncate { rels } => {
                    let mut ts = Vec::new();
                    for rel in rels {
                        if let Some(t) = self.table(rel, &txn)? {
                            ts.push(t);
                        }
                    }
                    if let Some(x) = txn.as_mut() {
                        x.2.push(Change::Truncate { tables: ts });
                    }
                }
                Message::Other(_) => {}
            }
        }
        // Nothing before the flush position was left out: the slot can move there.
        if !full {
            end = end.max(flush);
        }
        // The snapshot is needed until every transaction that was running when it was taken has ended and been
        // read: once the oldest running one is newer than its xmax, every one ends before `insert`.
        if let Some(s) = &snapshot {
            if clear_after.is_none() && xmin_now >= s.xmax {
                clear_after = Some(insert);
            }
        }
        let forget = clear_after.is_some_and(|c| end >= c);
        let state = match (forget, &snapshot_text) {
            (false, Some(snap)) => serde_json::json!({ "snapshot": snap, "clear_after": clear_after.map(format_lsn) }),
            (true, Some(_)) => {
                tracing::info!(job = %self.job, "every transaction the snapshot saw has been read; forgetting it");
                serde_json::json!({})
            }
            _ => serde_json::json!({}),
        };
        Ok(Read {
            pos: Position { offset: end, state },
            full,
            txns,
            skipped,
            changes,
            newest_us,
            lag: flush.saturating_sub(end) as i64,
            due: end.saturating_sub(lsn) >= POSITION_BYTES,
        })
    }

    /// Lets the slot release what the lake now holds. A failure only delays it: the next epoch skips what
    /// the slot sends again, and advances it then.
    fn committed(&mut self, conn: &Connection, pos: &Position) {
        if let Err(e) = row(conn, &self.alias, &self.advance_sql(pos.offset)) {
            tracing::warn!(job = %self.job, error = %format!("{e:#}"), "advancing the slot failed; the next epoch retries");
        }
    }
}

/// The columns a `Relation` message describes must be the ones the mirror was created with.
fn check_columns(t: &MirrorTable, cols: &[pgoutput::Column]) -> Result<()> {
    let same = cols.len() == t.columns.len() && cols.iter().zip(&t.columns).all(|(a, b)| a.name == b.name && a.type_oid == b.oid);
    if !same {
        let now: Vec<String> = cols.iter().map(|c| format!("{} (type {})", c.name, c.type_oid)).collect();
        let was: Vec<String> = t.columns.iter().map(|c| format!("{} {}", c.name, c.source_type)).collect();
        return Err(stop(format!(
            "the columns of {} changed in PostgreSQL: now {}, the mirror has {}. Schema changes are not applied yet: \
             drop the source and create it again",
            t.source_name(),
            now.join(", "),
            was.join(", ")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_map() {
        assert_eq!(duck_type(23, -1), "INTEGER");
        assert_eq!(duck_type(1700, (18 << 16) + 2 + 4), "DECIMAL(18,2)");
        assert_eq!(duck_type(1700, -1), "DOUBLE", "unconstrained numeric");
        assert_eq!(duck_type(1700, (60 << 16) + 4), "DOUBLE", "wider than DuckDB's decimals");
        assert_eq!(duck_type(1184, -1), "TIMESTAMPTZ");
        assert_eq!(duck_type(1007, -1), "VARCHAR", "int4[] keeps its text form");
        assert_eq!(duck_type(1186, -1), "VARCHAR", "interval: Parquet refuses negative ones");
    }

    #[test]
    fn options() {
        let (o, t) = parse_input("shop", &serde_json::json!({"dsn": "host=db dbname=shop", "tables": ["orders", "inv.items"]})).unwrap();
        assert_eq!((o.schema.as_str(), o.slot.as_str(), o.publication.as_str()), ("shop", "duckwake_shop", "duckwake_shop"));
        assert_eq!(t, vec![("public".into(), "orders".into()), ("inv".into(), "items".into())]);
        assert_eq!(o.max_changes, 100_000);
        let bad = |v: serde_json::Value| parse_input("shop", &v).unwrap_err().to_string();
        assert!(bad(serde_json::json!({"dsn": "host=db password=x", "tables": ["t"]})).contains("password"));
        assert!(bad(serde_json::json!({"dsn": "host=db", "tables": []})).contains("tables"));
        assert!(bad(serde_json::json!({"dsn": "host=db", "tables": ["t"], "slot": "Bad-Slot"})).contains("slot"));
        assert!(bad(serde_json::json!({"dsn": "host=db", "tables": ["t"], "topic": "x"})).contains("unknown option"));
        assert!(bad(serde_json::json!({"dsn": "host=db", "tables": ["t"], "password": "hunter2"})).contains("reference"));
        let mut o = o;
        o.password = Some("env:DUCKWAKE_TEST_PG_PASSWORD".into());
        std::env::set_var("DUCKWAKE_TEST_PG_PASSWORD", "it's");
        let ci = o.conninfo().unwrap();
        assert!(ci.starts_with("host=db dbname=shop password='it\\'s' application_name=duckwake options='-c DateStyle=ISO,MDY"), "{ci}");
    }
}
