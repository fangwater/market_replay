//! Inspect TAS CF names and watermarks without opening a writer.
use anyhow::Result;
use clap::Parser;
use lseg_delivery::tasdb::{self, Tuning};
use rocksdb::IteratorMode;
use std::{fs::File, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    output: PathBuf,
}
fn main() -> Result<()> {
    let a = Args::parse();
    let names = tasdb::list_cfs(&a.db)?;
    let db = tasdb::open_read_only(&a.db, ["default", "replay_meta"], Tuning::new(8, 256))?;
    let meta = db.cf_handle("replay_meta").unwrap();
    let mut watermarks = std::collections::BTreeMap::new();
    for pair in db.iterator_cf(meta, IteratorMode::Start) {
        let (k, v) = pair?;
        watermarks.insert(
            String::from_utf8(k.to_vec())?,
            String::from_utf8(v.to_vec())?,
        );
    }
    serde_json::to_writer_pretty(
        File::create(a.output)?,
        &serde_json::json!({"db":a.db,"column_families":names,"watermarks":watermarks}),
    )?;
    Ok(())
}
