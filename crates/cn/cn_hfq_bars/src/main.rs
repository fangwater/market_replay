use anyhow::Result;
use clap::Parser;
use cn_hfq_bars::run::{run, Options};
use std::path::PathBuf;

/// Daily incremental CN HFQ coarse bars: baseline_data_{5min,15min,1h,1day}_hfq from baseline_data_1min_hfq.
#[derive(Parser)]
#[command(name = "cn-hfq-bars")]
struct Args {
    /// cn-daily config (only `baseline_root` is read)
    #[arg(long)]
    config: PathBuf,
    #[arg(long, default_value = "config/cn_hfq_universe_75.json")]
    universe: PathBuf,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    /// Rebuild every selected product from its first day
    #[arg(long)]
    rebuild: bool,
    /// Comma list of products (subset)
    #[arg(long, value_delimiter = ',')]
    product: Option<Vec<String>>,
    /// Write below this root instead of the live datasets (verification); state is neither used nor changed
    #[arg(long)]
    out_root: Option<PathBuf>,
    /// host:port of the server: wait while it has running or queued jobs (manual runs)
    #[arg(long)]
    gate: Option<String>,
}

fn main() -> Result<()> {
    let a = Args::parse();
    anyhow::ensure!(a.workers >= 1 && a.workers <= 8, "--workers must be 1..=8");
    run(&Options {
        config: a.config,
        universe: a.universe,
        workers: a.workers,
        rebuild: a.rebuild,
        product: a
            .product
            .map(|v| v.into_iter().map(|p| p.to_ascii_uppercase()).collect()),
        out_root: a.out_root,
        gate: a.gate,
    })
}
