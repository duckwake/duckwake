//! `duckwaked [FLAGS]` is `duckwake serve [FLAGS]`: a worker plus the SQL functions over Quack and metrics.
fn main() -> anyhow::Result<()> {
    let mut args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    args.insert(1.min(args.len()), "serve".into());
    duckwake::cli::main_with(args)
}
