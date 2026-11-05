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
}

/// A connection with the lookup table the tests join, and three sources: `clicks`, `purchases` with a
/// `price` where `clicks` has an `amount`, and `changes`, shaped like `clicks` but deleting and updating rows.
pub(crate) fn setup() -> (Connection, HashMap<String, TestSource>) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE games (game_id VARCHAR, studio VARCHAR)").unwrap();
    let source = |value: &str, append_only: bool| {
        let columns = [("ts", "TIMESTAMP"), ("game_id", "VARCHAR"), ("user_id", "VARCHAR"), (value, "DECIMAL(18,2)")];
        TestSource { columns: columns.iter().map(|(c, t)| (c.to_string(), t.to_string())).collect(), append_only }
    };
    (
        conn,
        HashMap::from([
            ("clicks".to_string(), source("amount", true)),
            ("purchases".to_string(), source("price", true)),
            ("changes".to_string(), source("amount", false)),
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
