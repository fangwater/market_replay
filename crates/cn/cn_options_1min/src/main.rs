use anyhow::{ensure, Context, Result};
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use cn_options_1min::config::Config;
use cn_options_1min::pipeline::{backfill_daily, backfill_hist, run_daily, verify_overlap};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "cn-options-1min",
    about = "Clean domestic commodity option 1-min data into baseline_data_1min parquet layout"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Backfill historical zip days (TradDay <= cutoff_day)
    BackfillHist {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long)]
        workers: Option<usize>,
        #[arg(long)]
        overwrite: bool,
    },
    /// Backfill daily MDL days (TradDay > cutoff_day)
    BackfillDaily {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long)]
        workers: Option<usize>,
        #[arg(long)]
        overwrite: bool,
    },
    /// Incremental daily job (for market-replay-server schedule)
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        workers: Option<usize>,
    },
    /// Compare historical zip vs daily MDL on an overlap TradDay
    VerifyOverlap {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        day: String,
    },
}

fn parse_opt_date(raw: Option<String>) -> Result<Option<NaiveDate>> {
    raw.map(|s| {
        NaiveDate::parse_from_str(&s, "%Y-%m-%d")
            .with_context(|| format!("invalid date {s:?}, expected YYYY-MM-DD"))
    })
    .transpose()
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Command::BackfillHist {
            config,
            start,
            end,
            workers,
            overwrite,
        } => {
            let cfg = Config::load(&config)?;
            let w = workers.unwrap_or(cfg.default_workers);
            ensure!((1..=8).contains(&w), "--workers must be 1..=8");
            backfill_hist(
                &cfg,
                parse_opt_date(start)?,
                parse_opt_date(end)?,
                w,
                overwrite,
            )
        }
        Command::BackfillDaily {
            config,
            start,
            end,
            workers,
            overwrite,
        } => {
            let cfg = Config::load(&config)?;
            let w = workers.unwrap_or(cfg.default_workers);
            ensure!((1..=8).contains(&w), "--workers must be 1..=8");
            let _ = backfill_daily(
                &cfg,
                parse_opt_date(start)?,
                parse_opt_date(end)?,
                w,
                overwrite,
            )?;
            Ok(())
        }
        Command::Run { config, workers } => {
            let cfg = Config::load(&config)?;
            let w = workers.unwrap_or(cfg.default_workers);
            ensure!((1..=8).contains(&w), "--workers must be 1..=8");
            run_daily(&cfg, w)
        }
        Command::VerifyOverlap { config, day } => {
            let cfg = Config::load(&config)?;
            let d = NaiveDate::parse_from_str(&day, "%Y-%m-%d")
                .with_context(|| format!("invalid --day {day:?}, expected YYYY-MM-DD"))?;
            verify_overlap(&cfg, d)
        }
    }
}
