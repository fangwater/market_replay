//! Standalone Rust direct RocksDB -> daily Parquet tool.
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Parser, Subcommand};
use lseg_delivery::native_kline::engine::{self, Config};
use std::path::PathBuf;

const DEFAULT_WITNESSES: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/tas_trade_witnesses_20261110";
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
    /// Import the raw source witnesses of completed identity recovery
    /// (scripts/lseg/extract_revision_identity.py) into a per-RIC store
    /// for trade-correction netting. Never overwrites an existing store.
    WitnessImport {
        /// identity_jobs_*.json lists (all jobs must be done).
        #[arg(long, value_delimiter = ',', required = true)]
        jobs: Vec<PathBuf>,
        #[arg(
            long,
            default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/trade_correction_work/identities"
        )]
        identities: PathBuf,
        /// JSON whose top-level keys (object) or items (array) are the RICs
        /// whose ledgers this recovery evidences.
        #[arg(long)]
        rics: PathBuf,
        #[arg(long, default_value_t = 2026)]
        year: i32,
        #[arg(long, default_value = DEFAULT_WITNESSES)]
        output: PathBuf,
    },
    /// Append the raw-CSV correction evidence of one daily batch (all
    /// Correction rows, plus same-RIC trades with a corrected payload) as an
    /// immutable store segment. Idempotent by segment name.
    WitnessAppend {
        #[arg(long, default_value = DEFAULT_WITNESSES)]
        store: PathBuf,
        #[arg(long)]
        segment: String,
        #[arg(long)]
        year: i32,
        /// Downloaded daily CSV files (.csv.gz / .csv.zst / .csv).
        #[arg(long, value_delimiter = ',', required = true)]
        files: Vec<PathBuf>,
        /// Files decoded in parallel (fixed pool, 1..8).
        #[arg(long, default_value_t = 1)]
        workers: usize,
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
    /// Raw source witness store for trade-correction netting
    /// (correction_policy_version=1); see `witness-import`.
    #[arg(long, default_value = DEFAULT_WITNESSES)]
    witnesses: PathBuf,
    /// Aggregate raw prints only (no witness store). Refused for the RICs
    /// revised by the original ledger.
    #[arg(long)]
    no_witnesses: bool,
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
            witnesses,
            no_witnesses,
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
        let witnesses = if no_witnesses {
            None
        } else {
            Some(std::sync::Arc::new(
                lseg_delivery::native_kline::netting::WitnessStore::open(&witnesses)
                    .with_context(|| format!("witness store {}", witnesses.display()))?,
            ))
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
            witnesses,
        })
    }
}
fn main() -> Result<()> {
    let args = Args::parse();
    let status_only = matches!(&args.command, Command::StatusOnly(_));
    match args.command {
        Command::Compare { left, right } => lseg_delivery::native_kline::io::compare(&left, &right),
        Command::CheckPlan { plan } => engine::check_plan(&plan),
        Command::WitnessAppend {
            store,
            segment,
            year,
            files,
            workers,
        } => {
            let (added, corrections, rows) =
                lseg_delivery::native_kline::netting::append_daily_witnesses(
                    &store, &segment, year, &files, workers,
                )?;
            println!(
                "witness segment {segment}: {corrections} corrections, {rows} rows, {}",
                if added {
                    "added"
                } else if rows == 0 {
                    "nothing to add"
                } else {
                    "already present"
                }
            );
            Ok(())
        }
        Command::WitnessImport {
            jobs,
            identities,
            rics,
            year,
            output,
        } => {
            let list = lseg_delivery::native_kline::io::read_json(&rics)?;
            let rics: std::collections::BTreeSet<String> = match &list {
                serde_json::Value::Object(m) => m.keys().cloned().collect(),
                serde_json::Value::Array(v) => v
                    .iter()
                    .map(|r| r.as_str().map(str::to_owned).context("RIC string"))
                    .collect::<Result<_>>()?,
                _ => anyhow::bail!("--rics must be a JSON object or array"),
            };
            lseg_delivery::native_kline::netting::import_witnesses(
                &jobs,
                &identities,
                &rics,
                year,
                &output,
            )?;
            println!(
                "imported witnesses for {} RICs into {}",
                rics.len(),
                output.display()
            );
            Ok(())
        }
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
