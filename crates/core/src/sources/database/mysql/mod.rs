//! MySQL sources (D3 of `docs/design/database-cdc-sources.md`): a source of kind `mysql` names tables of one
//! server, and its ingest job (`sources::database`) keeps a mirror table of each in the lake, read from the binlog by GTID.
//!
//! - **Position.** The GTID set of the transactions applied, kept in the checkpoint. Each epoch asks the server
//!   for the binlog after that set (`COM_BINLOG_DUMP_GTID`, non-blocking), so it sends no transaction twice and
//!   DuckWake acknowledges nothing. A server whose `gtid_purged` is not within the set has lost binlog the
//!   mirrors need: the job stops and asks for a reset.
//! - **Rows.** `binlog_row_image = FULL`: every change carries the whole row before and after, so a change is the
//!   row itself, and replaying one already applied changes nothing.
//! - **Snapshot.** The GTID set G1, then a `WITH CONSISTENT SNAPSHOT` transaction that copies every table, then
//!   the set G2. The copy holds every transaction of G1, some of those between G1 and G2, and nothing after.
//!   The first epoch applies the binlog after G1 up to G2 over the copy, in the same commit: full row images
//!   make the rows right whichever of them the copy held.
//! - **Values.** The binlog's values and the snapshot's (binary protocol, session time zone UTC) are turned into
//!   one text form per column kind (`Kind`), then cast like every mirror column (`mirror::cast_sql`).
mod gtid;
mod values;

pub use gtid::GtidSet;

use crate::lake::ident;
use crate::sources::database::mirror::{cast_sql, MirrorColumn, MirrorTable};
use crate::sources::database::Value as Text;
use crate::sources::database::{stop, Batch, Change, Position, Read, Reader};
use anyhow::{bail, Context, Result};
use duckdb::Connection;
use futures_util::StreamExt;
use gtid::uuid_text;
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn, Opts, OptsBuilder};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use values::{binlog_text, kind, kind_and_type, snapshot_text, Kind};

pub const KIND: &str = "mysql";

/// A MySQL source as stored in `__duckwake.sources` (kind `mysql`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MyOptions {
    /// `mysql://user@host:port`, without the password; URL parameters such as `require_ssl=true` pass through.
    pub url: String,
    /// `env:NAME` or `file:/path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// The lake schema of the mirror tables.
    pub schema: String,
    /// The replica id the ingest job reads the binlog as; unique among a server's replicas.
    pub server_id: u32,
    pub epoch_interval: String,
    /// Most row changes one epoch reads (whole transactions: a larger one is read whole).
    pub max_changes: usize,
    pub tables: Vec<MirrorTable>,
}

impl MyOptions {
    fn opts(&self) -> Result<Opts> {
        let base = Opts::from_url(&self.url).with_context(|| format!("url {:?}", self.url))?;
        let mut b = OptsBuilder::from_opts(base).prefer_socket(false);
        if let Some(p) = &self.password {
            b = b.pass(Some(crate::secrets::resolve("password", p)?));
        }
        Ok(b.into())
    }
}

/// A runtime of its own: the ingest thread, the CLI and the SQL functions all call in from outside tokio.
fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread().enable_all().build()?)
}

pub fn load(conn: &Connection, name: &str) -> Result<MyOptions> {
    Ok(serde_json::from_str(&crate::sources::database::catalog::load_options(conn, name, KIND)?)?)
}

/// The tables a DDL statement names (`ALTER TABLE`, `DROP TABLE`, `RENAME TABLE`, `TRUNCATE`), as
/// (database, table), and whether it is a TRUNCATE.
fn ddl_tables(query: &str, default_db: &str) -> (bool, Vec<(String, String)>) {
    let q: String = query.split("/*").map(|p| p.split_once("*/").map_or(p, |(_, r)| r)).collect::<Vec<_>>().join(" ");
    let words: Vec<&str> = q.split(|c: char| c.is_whitespace() || c == ',' || c == ';').filter(|w| !w.is_empty()).collect();
    let lower: Vec<String> = words.iter().map(|w| w.to_lowercase()).collect();
    let name = |w: &str| -> (String, String) {
        let w = w.trim_matches('`');
        match w.split_once("`.`").or_else(|| w.split_once('.')) {
            Some((d, t)) => (d.trim_matches('`').to_string(), t.trim_matches('`').to_string()),
            None => (default_db.to_string(), w.to_string()),
        }
    };
    let skip = ["table", "tables", "if", "exists", "temporary", "online", "offline", "ignore"];
    match lower.first().map(String::as_str) {
        Some("truncate") => (true, words.iter().zip(&lower).skip(1).filter(|(_, l)| !skip.contains(&l.as_str())).take(1).map(|(w, _)| name(w)).collect()),
        Some("alter") if lower.get(1).map(String::as_str) == Some("table") => (false, words.get(2).map(|w| vec![name(w)]).unwrap_or_default()),
        Some("drop") if lower.iter().take(3).any(|l| l == "table" || l == "tables") => {
            (false, words.iter().zip(&lower).skip(1).filter(|(_, l)| !skip.contains(&l.as_str()) && *l != "restrict" && *l != "cascade").map(|(w, _)| name(w)).collect())
        }
        Some("rename") => (false, words.iter().zip(&lower).skip(2).filter(|(_, l)| *l != "to").map(|(w, _)| name(w)).collect()),
        _ => (false, Vec::new()),
    }
}

/// A row of `information_schema.COLUMNS`: name, data type, column type, precision, scale, character set, extra.
type ColumnRow = (String, String, String, Option<u32>, Option<u32>, Option<String>, String);

/// The checks and table definitions of a new MySQL source, then the lake side (`catalog`).
pub fn create(conn: &Connection, name: &str, options: &serde_json::Value) -> Result<MyOptions> {
    crate::lake::check_name("source", name)?;
    let obj = options.as_object().context("options must be a JSON object")?;
    const KNOWN: [&str; 7] = ["url", "password", "tables", "schema", "server_id", "epoch_interval", "max_changes"];
    if let Some(k) = obj.keys().find(|k| !KNOWN.contains(&k.as_str())) {
        bail!("unknown option {k:?} for a mysql source (known: {})", KNOWN.join(", "));
    }
    let s = |k: &str| obj.get(k).map(|v| v.as_str().map(String::from).with_context(|| format!("option {k} must be a string"))).transpose();
    let url = s("url")?.context("option url is required, e.g. \"mysql://duckwake@db:3306\"")?;
    if !url.starts_with("mysql://") {
        bail!("url must start with mysql://");
    }
    if url.split_once("://").is_some_and(|(_, r)| r.split('@').next().is_some_and(|u| r.contains('@') && u.contains(':'))) {
        bail!("put the password in option password as env:NAME or file:/path, not in url, so it is never stored in the lake");
    }
    let password = s("password")?;
    if let Some(p) = &password {
        crate::secrets::check_reference("password", p)?;
    }
    let wanted: Vec<(String, String)> = match obj.get("tables") {
        Some(serde_json::Value::Array(a)) if !a.is_empty() => a
            .iter()
            .map(|t| {
                let t = t.as_str().context("tables must be names like \"shop.orders\"")?;
                t.split_once('.').map(|(d, n)| (d.to_string(), n.to_string())).context("tables must name their database: \"shop.orders\"")
            })
            .collect::<Result<_>>()?,
        _ => bail!("option tables is required: the tables to mirror, e.g. [\"shop.orders\"]"),
    };
    let schema = s("schema")?.unwrap_or_else(|| name.to_string());
    crate::lake::check_name("schema", &schema)?;
    let server_id = match obj.get("server_id") {
        None => 1_000_000 + (name.bytes().fold(2166136261u32, |h, b| (h ^ b as u32).wrapping_mul(16777619)) % 1_000_000_000),
        Some(v) => v.as_u64().filter(|n| *n > 0 && *n < u32::MAX as u64).context("server_id must be a positive integer")? as u32,
    };
    let epoch_interval = s("epoch_interval")?.unwrap_or_else(|| "2s".into());
    crate::model::parse_duration(&epoch_interval)?;
    let max_changes = match obj.get("max_changes") {
        None => 100_000,
        Some(v) => v.as_u64().filter(|n| *n > 0).context("max_changes must be a positive integer")? as usize,
    };
    if crate::catalog::source_kind(conn, name)?.is_some() {
        bail!("source {name:?} already exists");
    }
    if crate::catalog::job_status(conn, name)?.is_some() {
        bail!("a view named {name:?} exists; a mysql source's ingest job takes the source's name");
    }
    let mut opts = MyOptions { url, password, schema, server_id, epoch_interval, max_changes, tables: Vec::new() };
    let rt = runtime()?;
    opts.tables = rt.block_on(async {
        let mut c = Conn::new(opts.opts()?).await.with_context(|| format!("connecting to {}", opts.url))?;
        let vars: Vec<(String, String)> = c
            .query(
                "SELECT 'log_bin', @@GLOBAL.log_bin UNION ALL SELECT 'binlog_format', @@GLOBAL.binlog_format
                 UNION ALL SELECT 'binlog_row_image', @@GLOBAL.binlog_row_image UNION ALL SELECT 'gtid_mode', @@GLOBAL.gtid_mode
                 UNION ALL SELECT 'binlog_row_value_options', @@GLOBAL.binlog_row_value_options
                 UNION ALL SELECT 'binlog_transaction_compression', @@GLOBAL.binlog_transaction_compression",
            )
            .await?;
        let vars: BTreeMap<String, String> = vars.into_iter().collect();
        let want = [("log_bin", "1"), ("binlog_format", "ROW"), ("binlog_row_image", "FULL"), ("gtid_mode", "ON"), ("binlog_row_value_options", ""), ("binlog_transaction_compression", "0")];
        for (k, v) in want {
            if vars.get(k).map(|x| x.to_uppercase()) != Some(v.to_uppercase()) {
                bail!("the server's {k} is {:?}; reading its binlog needs {k} = {}", vars.get(k).cloned().unwrap_or_default(), if v.is_empty() { "''" } else { v });
            }
        }
        let mut tables = Vec::new();
        for (db, t) in &wanted {
            let what = format!("{db}.{t}");
            crate::lake::check_name("mirrored table", t).with_context(|| format!("{what}: its mirror table takes its name"))?;
            let engine: Option<String> =
                c.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'", (db, t)).await?;
            match engine.as_deref() {
                None => bail!("table {what} does not exist (or this user cannot see it)"),
                Some(e) if !e.eq_ignore_ascii_case("InnoDB") => bail!("{what} uses {e}; only InnoDB tables have a consistent snapshot"),
                _ => {}
            }
            let cols: Vec<ColumnRow> = c
                .exec(
                    "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_SET_NAME, EXTRA
                     FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                    (db, t),
                )
                .await?;
            let mut columns = Vec::new();
            for (cname, data_type, column_type, p, sc, charset, extra) in cols {
                if extra.to_uppercase().contains("GENERATED") {
                    bail!("{what}.{cname} is a generated column; tables with generated columns are not supported yet");
                }
                if let Some(cs) = charset.filter(|cs| !cs.starts_with("utf8") && cs != "ascii" && cs != "binary") {
                    bail!("{what}.{cname} uses character set {cs}; text columns must be utf8mb4 (or ascii)");
                }
                let (_, duck) = kind_and_type(&data_type.to_lowercase(), &column_type.to_lowercase(), p, sc);
                columns.push(MirrorColumn { name: cname, source_type: format!("{} {}", data_type.to_lowercase(), column_type.to_lowercase()), oid: 0, typmod: -1, duck_type: duck });
            }
            let key: Vec<String> = c
                .exec(
                    "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
                    (db, t),
                )
                .await?;
            if key.is_empty() {
                bail!("{what} has no primary key; a mirror needs one to apply updates and deletes");
            }
            if tables.iter().any(|x: &MirrorTable| &x.name == t) {
                bail!("two tables named {t}: their mirrors would collide in lake schema {}", opts.schema);
            }
            tables.push(MirrorTable { schema: db.clone(), name: t.clone(), mirror: format!("{}.{}.{t}", crate::lake::LAKE, opts.schema), columns, key });
        }
        c.disconnect().await?;
        Ok(tables)
    })?;
    for t in &opts.tables {
        if crate::catalog::lake_table_exists(conn, &opts.schema, &t.name)? {
            bail!("lake table {}.{} exists already; mirror into another schema (option schema)", opts.schema, t.name);
        }
    }
    crate::sources::database::catalog::create(conn, name, KIND, &serde_json::to_value(&opts)?, &opts.schema, &opts.tables)?;
    for t in &opts.tables {
        crate::catalog::set_table_inlining(conn, &opts.schema, &t.name, 10_000)?;
    }
    Ok(opts)
}

/// The transaction a reader is in: its GTID, commit time, whether it is a row transaction (`BEGIN` seen), and
/// its changes.
struct Txn {
    uuid: String,
    gno: u64,
    commit_us: i64,
    dml: bool,
    changes: Vec<Change>,
}

pub(crate) struct MyReader {
    job: String,
    opts: MyOptions,
    kinds: Vec<Vec<Kind>>,
    rt: tokio::runtime::Runtime,
}

impl MyReader {
    pub(crate) fn open(job: &str, opts: &MyOptions) -> Result<Self> {
        let kinds = opts.tables.iter().map(|t| t.columns.iter().map(kind).collect()).collect();
        Ok(MyReader { job: job.into(), opts: opts.clone(), kinds, rt: runtime()? })
    }

    fn reset_hint(&self) -> String {
        format!("reset it to take a new snapshot: duckwake reset {} --to-epoch 0", self.job)
    }

    /// Copies every table in one consistent-snapshot transaction into `staging(i)`, between the GTID sets
    /// executed before and after it starts.
    fn copy_between(&self, conn: &Connection, staging: &dyn Fn(usize) -> String) -> Result<(GtidSet, GtidSet)> {
        self.rt.block_on(async {
            let mut c = Conn::new(self.opts.opts()?).await?;
            let g1: String = c.query_first("SELECT @@GLOBAL.gtid_executed").await?.unwrap_or_default();
            c.query_drop("SET SESSION time_zone = '+00:00'").await?;
            c.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ").await?;
            c.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY").await?;
            let g2: String = c.query_first("SELECT @@GLOBAL.gtid_executed").await?.unwrap_or_default();
            for (i, t) in self.opts.tables.iter().enumerate() {
                let raw = format!("memory.main.{}", ident(&format!("ingest_{}_raw{i}", self.job)));
                let defs: Vec<String> = (0..t.columns.len()).map(|k| format!("c{k} VARCHAR")).collect();
                conn.execute_batch(&format!("CREATE OR REPLACE TABLE {raw} ({})", defs.join(", ")))?;
                {
                    let mut app = conn.appender_to_catalog_and_db(&format!("ingest_{}_raw{i}", self.job), "memory", "main")?;
                    let cols: Vec<String> = t.columns.iter().map(|c| format!("`{}`", c.name.replace('`', "``"))).collect();
                    let sql = format!("SELECT {} FROM `{}`.`{}`", cols.join(", "), t.schema.replace('`', "``"), t.name.replace('`', "``"));
                    let mut result = c.exec_iter(sql, ()).await?;
                    let mut stream = result.stream::<mysql_async::Row>().await?.context("the copy returned no rows")?;
                    while let Some(row) = stream.next().await {
                        let row = row?;
                        let vals: Vec<Option<String>> = row
                            .unwrap()
                            .into_iter()
                            .zip(&self.kinds[i])
                            .map(|(v, k)| snapshot_text(k, v))
                            .collect::<Result<_>>()
                            .map_err(|e| stop(format!("a value of {} cannot be read: {e:#}", t.source_name())))?;
                        app.append_row(duckdb::appender_params_from_iter(vals))?;
                    }
                }
                let casts: Vec<String> = t.columns.iter().enumerate().map(|(k, c)| format!("{} AS {}", cast_sql(c, &format!("c{k}")), ident(&c.name))).collect();
                conn.execute_batch(&format!("CREATE OR REPLACE TABLE {} AS SELECT {} FROM {raw}; DROP TABLE {raw}", staging(i), casts.join(", ")))
                    .map_err(|e| crate::sources::database::cast_error(e, t))?;
            }
            c.query_drop("COMMIT").await?;
            c.disconnect().await?;
            Ok((GtidSet::parse(&g1)?, GtidSet::parse(&g2)?))
        })
    }

    /// Reads the binlog after `set` into `batch`, whole transactions, until it ends, `max_changes` row changes
    /// were read, or `until` is within the set. Returns the set after, and the counts.
    async fn read_binlog(&self, mut set: GtidSet, batch: &mut Batch<'_>, until: Option<&GtidSet>) -> Result<(GtidSet, u64, i64, Option<i64>, bool)> {
        let c = Conn::new(self.opts.opts()?).await?;
        let req = BinlogStreamRequest::new(self.opts.server_id).with_gtid().with_gtid_set(set.sids()?).with_non_blocking();
        let mut stream = c.get_binlog_stream(req).await?;
        let (mut txns, mut changes, mut newest_us, mut full) = (0u64, 0i64, None, false);
        let mut txn: Option<Txn> = None;
        // Table ids of the binlog to the source's tables (None: one the source does not mirror).
        let mut tables: BTreeMap<u64, Option<usize>> = BTreeMap::new();
        while let Some(ev) = stream.next().await {
            let ev = ev?;
            let mut ended = false;
            match ev.read_data()? {
                Some(EventData::GtidEvent(g)) => {
                    txn = Some(Txn { uuid: uuid_text(g.sid()), gno: g.gno(), commit_us: g.original_commit_timestamp() as i64, dml: false, changes: Vec::new() });
                }
                Some(EventData::QueryEvent(q)) => {
                    let query = q.query();
                    let Some(t) = txn.as_mut() else { continue };
                    if query.eq_ignore_ascii_case("BEGIN") {
                        t.dml = true;
                    } else if query.eq_ignore_ascii_case("COMMIT") {
                        ended = true;
                    } else if !t.dml {
                        // A statement of its own (DDL): its own transaction.
                        let (truncate, named) = ddl_tables(&query, &q.schema());
                        let ours: Vec<usize> = named
                            .iter()
                            .filter_map(|(d, n)| self.opts.tables.iter().position(|x| x.schema.eq_ignore_ascii_case(d) && x.name.eq_ignore_ascii_case(n)))
                            .collect();
                        if truncate && !ours.is_empty() {
                            t.changes.push(Change::Truncate { tables: ours });
                        } else if let Some(&i) = ours.first() {
                            return Err(stop(format!(
                                "{} changed in MySQL ({}): schema changes are not applied yet; drop the source and create it again",
                                self.opts.tables[i].source_name(),
                                query.chars().take(200).collect::<String>()
                            )));
                        }
                        ended = true;
                    }
                }
                Some(EventData::TableMapEvent(tme)) => {
                    let i = self
                        .opts
                        .tables
                        .iter()
                        .position(|x| x.schema.eq_ignore_ascii_case(&tme.database_name()) && x.name.eq_ignore_ascii_case(&tme.table_name()));
                    if let Some(i) = i {
                        self.check_columns(i, &tme)?;
                    }
                    tables.insert(tme.table_id(), i);
                }
                Some(EventData::RowsEvent(r)) => {
                    let Some(&Some(i)) = tables.get(&r.table_id()) else { continue };
                    let tme = stream.get_tme(r.table_id()).context("a rows event without its table map")?;
                    let t = txn.as_mut().context("a rows event outside a transaction")?;
                    let (kinds, table) = (&self.kinds[i], &self.opts.tables[i]);
                    // A value that cannot be read now cannot be read on a retry either: the job stops.
                    let values = |row: &mysql_async::binlog::row::BinlogRow| -> Result<Vec<Text>> {
                        (0..kinds.len())
                            .map(|k| {
                                let v = binlog_text(&kinds[k], row.as_ref(k))
                                    .map_err(|e| stop(format!("a value of {}.{} cannot be read: {e:#}", table.source_name(), table.columns[k].name)))?;
                                Ok(v.map_or(Text::Null, Text::Text))
                            })
                            .collect()
                    };
                    for row in r.rows(tme) {
                        let (before, after) = row?;
                        changes += 1;
                        match (&r, before, after) {
                            (RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_), _, Some(a)) => {
                                t.changes.push(Change::Upsert { table: i, old_key: None, values: values(&a)? })
                            }
                            (RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_), Some(b), Some(a)) => {
                                let old_key = Some(batch.key(i, &values(&b)?)?);
                                t.changes.push(Change::Upsert { table: i, old_key, values: values(&a)? })
                            }
                            (RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_), Some(b), _) => {
                                t.changes.push(Change::Delete { table: i, key: batch.key(i, &values(&b)?)? })
                            }
                            (RowsEventData::PartialUpdateRowsEvent(_), _, _) => {
                                bail!("a partial row update: the server's binlog_row_value_options must be empty")
                            }
                            _ => bail!("a rows event without the row images it needs: binlog_row_image must be FULL"),
                        }
                    }
                }
                Some(EventData::XidEvent(_)) => ended = true,
                Some(EventData::TransactionPayloadEvent(_)) => bail!("a compressed transaction: the server's binlog_transaction_compression must be OFF"),
                _ => {}
            }
            if ended {
                let t = txn.take().context("a transaction end without its GTID")?;
                for c in t.changes {
                    batch.apply(c)?;
                }
                set.add(&t.uuid, t.gno);
                txns += 1;
                newest_us = Some(t.commit_us);
                if until.is_some_and(|u| u.is_subset(&set)) {
                    break;
                }
                if changes >= self.opts.max_changes as i64 && until.is_none() {
                    full = true;
                    break;
                }
            }
        }
        // Ending the dump closes its connection.
        std::mem::drop(stream);
        Ok((set, txns, changes, newest_us, full))
    }

    /// The columns a table map describes must be the ones the mirror was created with.
    fn check_columns(&self, i: usize, tme: &mysql_async::binlog::events::TableMapEvent<'_>) -> Result<()> {
        let t = &self.opts.tables[i];
        let mut names: Vec<String> = Vec::new();
        for f in tme.iter_optional_meta() {
            if let mysql_async::binlog::events::OptionalMetadataField::ColumnName(n) = f? {
                for name in n.iter_names() {
                    names.push(name?.name().to_string());
                }
            }
        }
        let same_count = tme.columns_count() as usize == t.columns.len();
        let same_names = names.is_empty() || names.iter().zip(&t.columns).all(|(a, b)| *a == b.name);
        if !same_count || !same_names {
            let was: Vec<&str> = t.columns.iter().map(|c| c.name.as_str()).collect();
            return Err(stop(format!(
                "the columns of {} changed in MySQL: now {} columns{}, the mirror has {}. Schema changes are not applied yet: \
                 drop the source and create it again",
                t.source_name(),
                tme.columns_count(),
                if names.is_empty() { String::new() } else { format!(" ({})", names.join(", ")) },
                was.join(", ")
            )));
        }
        Ok(())
    }

    /// The server's executed and purged GTID sets.
    async fn server_sets(&self) -> Result<(GtidSet, GtidSet)> {
        let mut c = Conn::new(self.opts.opts()?).await?;
        let (executed, purged): (String, String) =
            c.query_first("SELECT @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged").await?.context("gtid_executed")?;
        c.disconnect().await?;
        Ok((GtidSet::parse(&executed)?, GtidSet::parse(&purged)?))
    }
}

fn position(set: &GtidSet) -> Position {
    Position { offset: set.count(), state: serde_json::json!({ "gtid": set.to_string() }) }
}

impl Reader for MyReader {
    fn backend(&self) -> &'static str {
        "binlog"
    }

    fn describe(&self, pos: &Position) -> String {
        let s = pos.state.get("gtid").and_then(|v| v.as_str()).unwrap_or("");
        format!("GTID set {}", if s.len() > 120 { format!("{}...", &s[..120]) } else { s.to_string() })
    }

    fn snapshot(&mut self, conn: &Connection, staging: &dyn Fn(usize) -> String, batch: &mut Batch) -> Result<(Position, i64)> {
        let (g1, g2) = self.copy_between(conn, staging)?;
        // The copy holds G1 and some of what came after it, up to G2: the binlog from G1 up to G2, applied over
        // it, makes it the tables as of G2 (full row images: a change already in the copy changes nothing).
        let (set, _, changes, _, _) = self.rt.block_on(self.read_binlog(g1, batch, Some(&g2)))?;
        Ok((position(&set), changes))
    }

    fn copy(&mut self, conn: &Connection, staging: &dyn Fn(usize) -> String) -> Result<()> {
        self.copy_between(conn, staging).map(|_| ())
    }

    fn read(&mut self, _conn: &Connection, pos: &Position, batch: &mut Batch) -> Result<Read> {
        let set = GtidSet::parse(pos.state.get("gtid").and_then(|v| v.as_str()).unwrap_or(""))?;
        let (executed, purged) = self.rt.block_on(self.server_sets())?;
        if !purged.is_subset(&set) {
            return Err(stop(format!(
                "the server purged binlog the mirrors had not read (gtid_purged {purged} is not within the lake's position {set}), \
                 so those changes are lost; {}",
                self.reset_hint()
            )));
        }
        let (after, txns, changes, newest_us, full) = self.rt.block_on(self.read_binlog(set, batch, None))?;
        let lag = executed.count().saturating_sub(after.count()) as i64;
        Ok(Read { pos: position(&after), full, txns, skipped: 0, changes, newest_us, lag, due: false })
    }

    fn committed(&mut self, _conn: &Connection, _pos: &Position) {}
}

/// Drops a stopped MySQL source: its ingest job and mirror tables. Nothing is left on the server: a binlog reader
/// holds nothing there.
pub fn drop(conn: &Connection, name: &str, opts: &MyOptions) -> Result<String> {
    if crate::catalog::job_status(conn, name)? == Some(crate::model::JobStatus::Running) {
        bail!("stop {name:?} before dropping it");
    }
    let mirrors: Vec<String> = opts.tables.iter().map(|t| t.mirror.clone()).collect();
    let readers = crate::catalog::views_reading_tables(conn, &mirrors)?;
    if !readers.is_empty() {
        bail!("views {} read source {name}'s tables; drop them first", readers.join(", "));
    }
    crate::sources::database::catalog::drop(conn, name, &opts.tables)?;
    Ok(format!("source {name}: dropped with its mirror tables"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl() {
        assert_eq!(ddl_tables("TRUNCATE TABLE `shop`.`orders`", "x"), (true, vec![("shop".into(), "orders".into())]));
        assert_eq!(ddl_tables("truncate orders", "shop"), (true, vec![("shop".into(), "orders".into())]));
        assert_eq!(ddl_tables("ALTER TABLE orders ADD COLUMN x int", "shop"), (false, vec![("shop".into(), "orders".into())]));
        assert_eq!(
            ddl_tables("DROP TABLE IF EXISTS `a`,`b` /* generated by server */", "shop"),
            (false, vec![("shop".into(), "a".into()), ("shop".into(), "b".into())])
        );
        assert_eq!(ddl_tables("RENAME TABLE a TO b", "shop").1.len(), 2);
        assert_eq!(ddl_tables("CREATE TABLE t (id int)", "shop"), (false, vec![]));
    }
}
