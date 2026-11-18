use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    check: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.check {
        cn_kline_server::minute_cache::check(&args.config)
    } else {
        cn_kline_server::minute_cache::run(&args.config)
    }
}
