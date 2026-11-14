//! DuckWake engine: continuous SQL over Kafka in micro-batches, with sink rows, operator state and
//! source offsets committed to DuckLake in one transaction per epoch.
pub mod adhoc;
pub mod api;
pub mod catalog;
pub mod decode;
pub mod engine;
pub mod failover;
pub mod kafka;
pub mod lake;
pub mod maintenance;
pub mod metrics;
pub mod model;
pub mod secrets;
pub mod sql_functions;
pub mod verify;

/// Views as DBSP circuits, and the compiler from SQL (`plan`).
pub use duckwake_circuit as circuit;
pub use duckwake_circuit::plan;
