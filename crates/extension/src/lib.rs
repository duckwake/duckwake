//! The `duckwake` loadable extension, for development: `LOAD 'duckwake.duckdb_extension'` in DuckDB 2.0
//! registers the `duckwake_*` functions. Extensions get no handle to the host database, so the functions run
//! on a private DuckDB in the same process that attaches the lake (`DUCKWAKE_CATALOG` / `DUCKWAKE_DATA_PATH`,
//! or `CALL duckwake_connect(...)`), and `CALL duckwake_worker_start()` runs a worker next to it. Attach the
//! same lake in the host to read the views' tables.
use duckdb::{duckdb_entrypoint_c_api, Connection};
use duckwake_core::lake::LakeConfig;
use duckwake_core::sql_functions::{self, Api};
use std::error::Error;
use std::sync::OnceLock;

/// Kafka reads run on this runtime for as long as the host process lives.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("duckwake-io")
            .enable_all()
            .build()
            .expect("starting the duckwake runtime")
    })
}

#[duckdb_entrypoint_c_api(ext_name = "duckwake", min_duckdb_version = "v1.5.6")]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    let env = |k: &str, default: &str| std::env::var(k).unwrap_or_else(|_| default.to_string());
    let lake = LakeConfig {
        catalog: env("DUCKWAKE_CATALOG", "postgres:dbname=lake host=localhost port=55432 user=lake password=lake"),
        data_path: env("DUCKWAKE_DATA_PATH", ".lake/data/"),
        memory_limit: std::env::var("DUCKWAKE_MEMORY_LIMIT").ok(),
        temp_dir: std::env::var("DUCKWAKE_TEMP_DIR").ok(),
        threads: std::env::var("DUCKWAKE_THREADS").ok().and_then(|n| n.parse().ok()),
        max_spill: std::env::var("DUCKWAKE_MAX_SPILL").ok(),
    };
    sql_functions::register(&con, &Api::local(lake, runtime().handle().clone())).map_err(|e| format!("{e:#}"))?;
    Ok(())
}
