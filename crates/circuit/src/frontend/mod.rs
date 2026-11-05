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
}
