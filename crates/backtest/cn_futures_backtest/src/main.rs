use anyhow::Result;
use clap::{Parser, Subcommand};
use cn_futures_backtest::{config::Task, store::Store, Config};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "cn-backtest",
    about = "CN futures target-lot conversion, execution and maker-maker roll research"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}
#[derive(Subcommand)]
enum Cmd {
    Inspect {
        input: PathBuf,
    },
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    Convert {
        #[arg(long)]
        config: PathBuf,
    },
    Submit {
        #[arg(long)]
        config: PathBuf,
    },
    Worker {
        #[arg(long)]
        root: PathBuf,
    },
    List {
        #[arg(long)]
        root: PathBuf,
    },
    Cancel {
        #[arg(long)]
        root: PathBuf,
        id: String,
    },
    /// Run a serial grid of spread thresholds, preserving incomplete outcomes.
    CompareRoll {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, value_delimiter = ',', default_value = "0,1,2")]
        spread_ticks: Vec<f64>,
    },
}
fn print(v: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}
fn main() -> Result<()> {
    match Cli::parse().command {
        Cmd::Inspect { input } => print(&cn_futures_backtest::input::inspect(&input)?)?,
        Cmd::Run { config } | Cmd::Convert { config } => {
            let mut cfg = Config::load(&config)?;
            // `convert` is selected below by clap's command name as a separate path.
            if std::env::args().nth(1).as_deref() == Some("convert") {
                cfg.task = Task::Convert;
            }
            let store = Store::new(cfg.output_root.clone())?;
            let s = store.submit(&cfg)?;
            eprintln!("run={} outputs={}", s.id, store.path(&s.id)?.display());
            while let Some(done) = store.worker()? {
                let ours = done.id == s.id;
                print(&done)?;
                if ours {
                    anyhow::ensure!(done.state == "completed", "run failed: {:?}", done.error);
                    break;
                }
            }
        }
        Cmd::Submit { config } => {
            let cfg = Config::load(&config)?;
            print(&Store::new(cfg.output_root.clone())?.submit(&cfg)?)?;
        }
        Cmd::Worker { root } => print(&Store::new(root)?.worker()?)?,
        Cmd::List { root } => print(&Store::new(root)?.list(100)?)?,
        Cmd::Cancel { root, id } => print(&Store::new(root)?.cancel(&id)?)?,
        Cmd::CompareRoll {
            config,
            spread_ticks,
        } => {
            let cfg = Config::load(&config)?;
            anyhow::ensure!(
                cfg.task == Task::RollCost,
                "compare-roll requires task=roll_cost"
            );
            let store = Store::new(cfg.output_root.clone())?;
            let mut ids = vec![];
            for threshold in spread_ticks {
                let mut c = cfg.clone();
                c.roll.spread_improvement_ticks = threshold;
                ids.push(store.submit(&c)?.id);
            }
            while let Some(s) = store.worker()? {
                print(&s)?;
            }
            let comparison = ids
                .iter()
                .map(|id| store.status(id))
                .collect::<Result<Vec<_>>>()?;
            cn_futures_backtest::store::atomic_json(
                &cfg.output_root.join("roll_comparison.json"),
                &comparison,
            )?;
            print(&comparison)?;
        }
    }
    Ok(())
}
