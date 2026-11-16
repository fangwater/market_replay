//! `python3 -m cn_futures.roll_replay --eod-input ...` in Rust.

use anyhow::{bail, Result};
use clap::Parser;
use cn_futures_roll::dominant::{Rule, RULE_RQDATA_0, SUPPORTED_RULES};
use cn_futures_roll::replay::{
    parse_py_date, run_roll_replay, RollReplayArgs, DEFAULT_OVERLAP_CUT, EXCHANGES,
};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    about = "Write all-products EOD, dominant, prev_close_spread, continuous-price and continuity tables from a pre-folded EOD CSV (cn_futures_roll output)"
)]
struct Args {
    /// Pre-folded all-product EOD CSV, for example cn_futures_roll's output.
    #[arg(long)]
    eod_input: PathBuf,
    #[arg(long)]
    exchange: String,
    /// Inclusive TradDay YYYY-MM-DD or YYYYMMDD.
    #[arg(long)]
    start: String,
    /// Inclusive TradDay YYYY-MM-DD or YYYYMMDD.
    #[arg(long)]
    end: String,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value = RULE_RQDATA_0)]
    rule: String,
    /// Only recorded in the summary JSON on this path (matches Python).
    #[arg(long, default_value = DEFAULT_OVERLAP_CUT)]
    overlap_cut: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let Some(rule) = Rule::parse(&args.rule) else {
        bail!(
            "unsupported dominant rule: {} (choose from {SUPPORTED_RULES:?})",
            args.rule
        );
    };
    if !EXCHANGES.contains(&args.exchange.as_str()) {
        bail!(
            "unsupported exchange: {} (choose from {EXCHANGES:?})",
            args.exchange
        );
    }
    let result = run_roll_replay(&RollReplayArgs {
        eod_input: &args.eod_input,
        output_dir: &args.output_dir,
        exchange: &args.exchange,
        start: parse_py_date(&args.start)?,
        end: parse_py_date(&args.end)?,
        rule,
        overlap_cut: Some(parse_py_date(&args.overlap_cut)?),
    })?;
    println!(
        "roll replay ok exchange={} files={} products={} rolls={} continuous={}/{}",
        result.exchange,
        result.n_files,
        result.n_products,
        result.n_rolls,
        result.n_continuous_products,
        result.n_products
    );
    println!("summary={}", result.summary_path.display());
    Ok(())
}
