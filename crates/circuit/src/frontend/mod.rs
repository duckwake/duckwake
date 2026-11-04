//! SQL to Q: DuckDB parses the view, `plan` compiles it, `lower` turns the plan into an operator graph.
mod lower;
pub mod plan;

/// What the compiler needs to know about a source.
pub trait SourceSchema {
    /// Its event-time column, the one windows must use.
    fn event_time(&self) -> &str;
    /// Its columns and their DuckDB types, in order.
    fn columns(&self) -> &[(String, String)];
}
