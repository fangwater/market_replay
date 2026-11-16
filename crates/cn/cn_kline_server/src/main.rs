//! `cn-kline-server`: standalone CN futures gRPC + dominant HTTP service
//! (the same service is also mounted by `market-replay-server`).
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "cn-kline-server")]
struct Cli {
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cn_kline_server::serve(cn_kline_server::Config::load(&cli.config)?).await
}
