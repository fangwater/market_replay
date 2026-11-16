//! Standalone Rust direct RocksDB -> daily Parquet tool.
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use lseg_delivery::native_kline::engine::{self, Config};
use std::path::PathBuf;
#[derive(Parser)]
#[command(
    about = "Direct LSEG RocksDB to session status and continuous 1min Parquet; no Python or event JSON"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Compare existing Parquet data. RIC directories sample first/middle/last UTC days.
    Compare {
        #[arg(long)]
        left: PathBuf,
        #[arg(long)]
        right: PathBuf,
    },
    Run(RunArgs),
    /// Deliver status messages for RICs with no trade minute; requires completed minute groups.
    StatusOnly(RunArgs),
    /// Verify that the source plan includes every live product CF in its source DBs.
    CheckPlan {
        #[arg(
            long,
            default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/lseg_full_rocksdb_20261112/plan.json"
        )]
        plan: PathBuf,
    },
    Check {
        #[arg(
            long,
            default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/lseg_full_rocksdb_20261112/plan.json"
        )]
        plan: PathBuf,
        #[arg(long)]
        job: String,
        #[arg(long)]
        ric: String,
        #[arg(long, default_value_t = 100000)]
        messages: u64,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        cme: bool,
    },
}
#[derive(ClapArgs)]
struct RunArgs {
    #[arg(long, default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data")]
    project: PathBuf,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/lseg_full_rocksdb_20261112/plan.json"
    )]
    plan: PathBuf,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_full_rocksdb_20261112/extract"
    )]
    cache: PathBuf,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/lseg_native_klines_20261112"
    )]
    work: PathBuf,
    #[arg(long, default_value = "/mnt/nfs/30.100_cta_data/lseg")]
    output: PathBuf,
    #[arg(long)]
    year: Option<i32>,
    #[arg(long)]
    product: Option<String>,
    #[arg(long)]
    ric: Option<String>,
    #[arg(long, default_value_t = 2)]
    workers: usize,
    /// Compare existing kline/session_status day files and replace differing
    /// ones; the old file is first copied here (local NVMe, not the delivery disk).
    #[arg(long)]
    backup_root: Option<PathBuf>,
    /// With --backup-root: existing files of earlier UTC days (YYYY-MM-DD) are
    /// only footer-checked. Default: compare every regenerated day.
    #[arg(long, requires = "backup_root")]
    compare_from: Option<String>,
    /// With --backup-root: JSON lines, one per replaced file.
    #[arg(long, requires = "backup_root")]
    replaced_log: Option<PathBuf>,
}
impl TryFrom<RunArgs> for Config {
    type Error = anyhow::Error;
    fn try_from(args: RunArgs) -> Result<Self> {
        let RunArgs {
            project,
            plan,
            cache,
            work,
            output,
            year,
            product,
            ric,
            workers,
            backup_root,
            compare_from,
            replaced_log,
        } = args;
        let replace = match backup_root {
            None => None,
            Some(backup) => {
                let from = match compare_from {
                    Some(d) => lseg_delivery::native_kline::io::parse_ns(&d)
                        .with_context(|| format!("--compare-from {d}"))?,
                    None => 0,
                };
                let log = replaced_log.unwrap_or_else(|| work.join("replaced.jsonl"));
                Some(lseg_delivery::native_kline::io::Replace::new(
                    output.clone(),
                    backup,
                    from,
                    &log,
                )?)
            }
        };
        Ok(Config {
            project,
            plan,
            cache,
            work,
            output,
            year,
            product,
            ric,
            workers,
            publisher: None,
            replace,
        })
    }
}
fn main() -> Result<()> {
    let args = Args::parse();
    let status_only = matches!(&args.command, Command::StatusOnly(_));
    match args.command {
        Command::Compare { left, right } => lseg_delivery::native_kline::io::compare(&left, &right),
        Command::CheckPlan { plan } => engine::check_plan(&plan),
        Command::Run(args) | Command::StatusOnly(args) => {
            let config = Config::try_from(args)?;
            let state = config.run_dir().join(if status_only {
                "status_only_state.json"
            } else {
                "state.json"
            });
            let result = if status_only {
                engine::supplement_status(config)
            } else {
                engine::run(config)
            };
            if let Err(error) = &result {
                let mut value = lseg_delivery::native_kline::io::read_json(&state)
                    .unwrap_or_else(|_| serde_json::json!({}));
                value["stage"] = serde_json::json!("failed");
                value["fatal_error"] = serde_json::json!(format!("{error:#}"));
                let _ = lseg_delivery::native_kline::io::save(&state, &value);
            }
            result
        }
        Command::Check {
            plan,
            job,
            ric,
            messages,
            output,
            cme,
        } => engine::check(&plan, &job, &ric, messages, &output, cme),
    }
}
