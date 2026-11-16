//! Ordinary, bounded Rust tool for the user-facing SQLite instrument index.
use anyhow::Result;
use clap::{Parser, Subcommand};
use ly_cme_data::catalog::{self, Build, Query};

#[derive(Parser)]
#[command(about = "LSEG SQLite instrument index: query product/RIC without selecting dates")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Build(Build),
    List(Query),
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    ly_cme_data::resource_limits::enforce()?;
    match args.command {
        Command::Build(args) => catalog::build(args).await,
        Command::List(args) => catalog::list(args).await,
    }
}
