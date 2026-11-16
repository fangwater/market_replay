//! Inspect TAS CF names and watermarks without opening a writer.
use anyhow::Result;
use clap::Parser;
use cme_tas_replay::product::quote_last_merge;
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};
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
    let names = DB::list_cf(&Options::default(), &a.db)?;
    let mut options = Options::default();
    options.set_max_open_files(256);
    options.set_skip_stats_update_on_db_open(true);
    options.set_merge_operator_associative("quote_last", quote_last_merge);
    let db = DB::open_cf_descriptors_read_only(
        &options,
        &a.db,
        ["default", "replay_meta"]
            .into_iter()
            .map(|n| ColumnFamilyDescriptor::new(n, Options::default())),
        false,
    )?;
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
