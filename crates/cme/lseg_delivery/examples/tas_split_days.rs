//! STAGING ONLY: split a multi-day TAS RocksDB into one DB per UTC
//! publication day from `--split-from` on (earlier days stay in `rest`), to
//! simulate a month of lseg-daily daily DBs for merge measurements. The
//! source is opened read-only; outputs must not exist yet.
//!
//! cargo run --release -p lseg_delivery --example tas_split_days -- \
//!   --db SRC --out DIR --split-from 2026-10-01 --rest-period 2026-06-01_2026-10-01
use anyhow::{ensure, Context, Result};
use cme_tas_replay::product::{is_product_cf_name, quote_last_merge};
use lseg_delivery::tasdb::{self, Tuning, DAY_NS};
use rocksdb::{DBCompressionType, Options, ReadOptions, WriteBatch, WriteOptions, DB};
use std::collections::BTreeMap;
use std::path::PathBuf;

fn arg(name: &str) -> Result<String> {
    let a: Vec<String> = std::env::args().collect();
    let i = a
        .iter()
        .position(|x| x == name)
        .with_context(|| format!("missing {name}"))?;
    a.get(i + 1).cloned().context("value")
}

fn ns(d: &str) -> Result<u64> {
    let d = chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")?;
    Ok(d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as u64 * 1_000_000_000)
}

fn day(ts: u64) -> String {
    chrono::DateTime::from_timestamp((ts / 1_000_000_000) as i64, 0)
        .unwrap()
        .format("%Y-%m-%d")
        .to_string()
}

struct Out {
    db: DB,
    batch: WriteBatch,
}

fn main() -> Result<()> {
    let src = PathBuf::from(arg("--db")?);
    let out = PathBuf::from(arg("--out")?);
    let from = ns(&arg("--split-from")?)?;
    let rest_period = arg("--rest-period")?;
    ensure!(!out.exists(), "{} exists", out.display());
    let names = tasdb::list_cfs(&src)?;
    let products: Vec<String> = names
        .iter()
        .filter(|n| is_product_cf_name(n))
        .cloned()
        .collect();
    let cfs: Vec<String> = ["default".to_string(), "replay_meta".to_string()]
        .into_iter()
        .chain(products.iter().cloned())
        .collect();
    let db = tasdb::open_read_only(&src, &cfs, Tuning::new(64, 256))?;
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_write_buffer_size(16 << 20);
    opts.set_max_background_jobs(2);
    opts.set_merge_operator_associative("quote_last", quote_last_merge);
    let mut outs: BTreeMap<String, Out> = BTreeMap::new();
    let mut wo = WriteOptions::default();
    wo.disable_wal(true);
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for cf in &products {
        let mut ro = ReadOptions::default();
        ro.fill_cache(false);
        ro.set_readahead_size(2 << 20);
        let mut it = db.raw_iterator_cf_opt(db.cf_handle(cf).unwrap(), ro);
        it.seek_to_first();
        while let (Some(k), Some(v)) = (it.key(), it.value()) {
            let ts = u64::from_le_bytes(v[20..28].try_into()?);
            let name = if ts < from {
                "rest".to_string()
            } else {
                format!("day_{}", day(ts / DAY_NS * DAY_NS))
            };
            if !outs.contains_key(&name) {
                let o = DB::open_cf(&opts, out.join(&name), &cfs)?;
                outs.insert(
                    name.clone(),
                    Out {
                        db: o,
                        batch: WriteBatch::default(),
                    },
                );
            }
            let o = outs.get_mut(&name).unwrap();
            o.batch.put_cf(o.db.cf_handle(cf).unwrap(), k, v);
            if o.batch.size_in_bytes() > 4 << 20 {
                o.db.write_opt(std::mem::take(&mut o.batch), &wo)?;
            }
            *counts.entry(name).or_default() += 1;
            it.next();
        }
        it.status()?;
    }
    for (name, o) in outs.iter_mut() {
        o.db.write_opt(std::mem::take(&mut o.batch), &wo)?;
        let period = if name == "rest" {
            rest_period.clone()
        } else {
            let d = &name[4..];
            format!("{d}_{}", day(ns(d)? + DAY_NS))
        };
        o.db.put_cf(
            o.db.cf_handle("replay_meta").unwrap(),
            format!("period:{period}"),
            "done",
        )?;
        for cf in &cfs {
            o.db.flush_cf(o.db.cf_handle(cf).unwrap())?;
        }
        println!("{name}\t{period}\t{}", counts[name]);
    }
    Ok(())
}
