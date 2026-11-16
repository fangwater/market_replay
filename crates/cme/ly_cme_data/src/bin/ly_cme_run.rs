//! Ordinary foreground program supervisor; no cgroup or root prerequisite.
use anyhow::{bail, Result};
use clap::Parser;
use std::ffi::OsString;

#[derive(Parser)]
struct Args {
    /// Report the program budget without starting a data task.
    #[arg(long)]
    check: bool,
    #[arg(last = true)]
    command: Vec<OsString>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.check && args.command.is_empty() {
        bail!("use --check or -- <bin> [arguments]");
    }
    let code = ly_cme_data::resource_limits::launch(&args.command, args.check)?;
    std::process::exit(code)
}
