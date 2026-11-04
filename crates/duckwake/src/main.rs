fn main() -> anyhow::Result<()> {
    duckwake::cli::main_with(std::env::args_os().collect())
}
