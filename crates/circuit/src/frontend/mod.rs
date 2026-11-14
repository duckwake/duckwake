//! SQL to Q: DuckDB parses the view, `plan` compiles it, `lower` turns the plan into an operator graph.
mod lower;
pub mod plan;

/// What the compiler needs to know about a source.
pub trait SourceSchema {
    /// Its event-time column, the one windows must use.
    fn event_time(&self) -> &str;
    /// Its columns and their DuckDB types, in order.
    fn columns(&self) -> &[(String, String)];
    /// Whether its rows are only ever inserted. A source that can retract stages a weight `__w` with each row.
    fn append_only(&self) -> bool {
        true
    }
    /// Whether it is a DuckLake table (a lake table, or another view's table) read through its change feed,
    /// rather than a declared Kafka source. Joined to a Kafka source, such a table is a lookup instead.
    fn is_lake(&self) -> bool {
        false
    }
    /// The columns a window may use as its time: the event-time column, and for a windowed view's table
    /// also its window columns.
    fn time_columns(&self) -> Vec<String> {
        vec![self.event_time().to_string()]
    }
}
