//! Tests of the compiler, the circuits and their SQL.
mod circuits;
mod harness;
mod views;

use crate::frontend::SourceSchema;
use crate::plan::{self, Plan};
use crate::View;
use duckdb::Connection;
use std::collections::HashMap;

const SINK: &str = "lake.analytics.v";

/// A source of the tests.
pub(crate) struct TestSource {
    pub columns: Vec<(String, String)>,
    pub append_only: bool,
    /// A lake table or view read through its change feed, with the columns a window over it may use.
    pub lake: Option<Vec<String>>,
}

impl SourceSchema for TestSource {
    fn event_time(&self) -> &str {
        "ts"
    }
    fn columns(&self) -> &[(String, String)] {
        &self.columns
    }
    fn append_only(&self) -> bool {
        self.append_only
    }
    fn is_lake(&self) -> bool {
        self.lake.is_some()
    }
    fn time_columns(&self) -> Vec<String> {
        self.lake.clone().unwrap_or_else(|| vec!["ts".into()])
    }
}

/// A connection with the lookup table the tests join, and three sources: `clicks`, `purchases` with a
/// `price` where `clicks` has an `amount`, and `changes`, shaped like `clicks` but deleting and updating rows.
/// Lake inputs too: the lake table `shop.orders`, `shop.accounts` shaped like it but read with its updates and
/// deletes, and `per_min`, a windowed view's table.
pub(crate) fn setup() -> (Connection, HashMap<String, TestSource>) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE games (game_id VARCHAR, studio VARCHAR);
         CREATE SCHEMA shop; CREATE TABLE shop.orders (id INTEGER, game_id VARCHAR, amount DOUBLE);
         CREATE TABLE shop.accounts AS FROM shop.orders;",
    )
    .unwrap();
    let cols = |cs: &[(&str, &str)]| cs.iter().map(|(c, t)| (c.to_string(), t.to_string())).collect::<Vec<_>>();
    let source = |value: &str, append_only: bool| TestSource {
        columns: cols(&[("ts", "TIMESTAMP"), ("game_id", "VARCHAR"), ("user_id", "VARCHAR"), (value, "DECIMAL(18,2)")]),
        append_only,
        lake: None,
    };
    let orders = TestSource {
        columns: cols(&[("id", "INTEGER"), ("game_id", "VARCHAR"), ("amount", "DOUBLE"), ("_commit_ts", "TIMESTAMP")]),
        append_only: true,
        lake: Some(vec!["_commit_ts".into()]),
    };
    // The same table as read by a view that takes its updates and deletes.
    let accounts = TestSource { columns: orders.columns.clone(), append_only: false, lake: orders.lake.clone() };
    let per_min = TestSource {
        columns: cols(&[
            ("window_start", "TIMESTAMP"),
            ("window_end", "TIMESTAMP"),
            ("game_id", "VARCHAR"),
            ("n", "BIGINT"),
            ("_commit_ts", "TIMESTAMP"),
        ]),
        append_only: true,
        lake: Some(vec!["_commit_ts".into(), "window_start".into(), "window_end".into()]),
    };
    (
        conn,
        HashMap::from([
            ("clicks".to_string(), source("amount", true)),
            ("purchases".to_string(), source("price", true)),
            ("changes".to_string(), source("amount", false)),
            ("shop.orders".to_string(), orders),
            ("shop.accounts".to_string(), accounts),
            ("per_min".to_string(), per_min),
        ]),
    )
}

struct Def {
    plan: Plan,
    late: bool,
}

impl Def {
    fn view(&self) -> View<'_> {
        View { id: "v", sink: SINK, late_data: self.late.then_some("lake.analytics.v_late"), plan: &self.plan }
    }
}

fn def_for(sql: &str, late: bool) -> Def {
    let (conn, sources) = setup();
    Def { plan: plan::compile(&conn, "v", sql, &sources).unwrap(), late }
}
