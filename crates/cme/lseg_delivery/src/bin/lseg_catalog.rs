//! Ordinary, bounded Rust tool for the user-facing SQLite instrument index.
use anyhow::Result;
use clap::{Parser, Subcommand};
use lseg_delivery::catalog::{self, Build, IbSync, Query};

#[derive(Parser)]
#[command(about = "LSEG SQLite instrument index: query product/RIC without selecting dates")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Build(Build),
    /// Refresh only ib_contracts / ib_directories from the IB manifest.
    IbSync(IbSync),
    List(Query),
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Build(args) => catalog::build(args).await,
        Command::IbSync(args) => catalog::ib_sync(args).await,
        Command::List(args) => catalog::list(args).await,
    }
}
