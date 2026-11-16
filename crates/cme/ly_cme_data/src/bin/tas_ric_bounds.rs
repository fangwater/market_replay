//! Indexed first/last Trade and sampled Quote probes; never scans all ticks.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use cme_tas_replay::product::{encode_all_key, parse_product, product_cf_name, quote_last_merge};
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, ReadOptions, DB};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    path::PathBuf,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    plan: PathBuf,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products"
    )]
    db: PathBuf,
    #[arg(long)]
    output: PathBuf,
}
fn main() -> Result<()> {
    let a = Args::parse();
    let plan: Value = serde_json::from_reader(File::open(a.plan)?)?;
    let jobs = plan["jobs"].as_array().context("jobs")?;
    let names: BTreeSet<String> = DB::list_cf(&Options::default(), &a.db)?
        .into_iter()
        .collect();
    let mut selected = BTreeSet::from(["default".to_string(), "replay_meta".to_string()]);
    for j in jobs {
        for r in j["tas_aliases"].as_array().unwrap() {
            let product = parse_product(r.as_str().unwrap()).context("RIC product")?;
            for year in 2010..=2026 {
                let cf = product_cf_name(year, &product)?;
                if names.contains(&cf) {
                    selected.insert(cf);
                }
            }
        }
    }
    let cache = Cache::new_lru_cache(256 * 1024 * 1024);
    let options = || {
        let mut o = Options::default();
        o.set_max_open_files(512);
        o.set_skip_stats_update_on_db_open(true);
        o.set_merge_operator_associative("quote_last", quote_last_merge);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        table.set_cache_index_and_filter_blocks(true);
        table.set_pin_l0_filter_and_index_blocks_in_cache(false);
        o.set_block_based_table_factory(&table);
        o
    };
    eprintln!("opening {} CFs read-only", selected.len());
    let db = DB::open_cf_descriptors_read_only(
        &options(),
        &a.db,
        selected
            .iter()
            .map(|n| ColumnFamilyDescriptor::new(n, options())),
        false,
    )?;
    let mut results = serde_json::Map::new();
    for (index, j) in jobs.iter().enumerate() {
        let year = j["delivery_year"].as_i64().unwrap() as i32;
        let lower = chrono::NaiveDate::from_ymd_opt(year - 9, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap() as u64;
        let upper = chrono::NaiveDate::from_ymd_opt(year + 2, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap() as u64;
        let mut trade_first = None;
        let mut trade_last = None;
        let mut quote_first = None;
        let mut quote_last = None;
        let mut evidence = Vec::new();
        for alias in j["tas_aliases"].as_array().unwrap() {
            let ric = alias.as_str().unwrap();
            let product = parse_product(ric).unwrap();
            for source_year in 2010..=2026 {
                let cf_name = product_cf_name(source_year, &product)?;
                let Some(cf) = db.cf_handle(&cf_name) else {
                    continue;
                };
                for kind in [1u8, 3] {
                    let low = encode_all_key(kind, ric, lower, 0, 0)?;
                    let high = encode_all_key(kind, ric, upper, 0, 0)?;
                    let mut ro = ReadOptions::default();
                    ro.fill_cache(false);
                    ro.set_verify_checksums(true);
                    let mut it = db.raw_iterator_cf_opt(cf, ro);
                    it.seek(low);
                    let first = if let Some(key) = it.key() {
                        if key.len() == 31
                            && key[..17] == low[..17]
                            && key.as_ref() < high.as_slice()
                        {
                            Some((
                                u64::from_be_bytes(key[17..25].try_into()?),
                                u64::from_le_bytes(it.value().unwrap()[20..28].try_into()?),
                            ))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    it.status()?;
                    let Some(first) = first else {
                        continue;
                    };
                    it.seek_for_prev(high);
                    if it.key().is_some_and(|k| k >= high.as_slice()) {
                        it.prev();
                    }
                    let key = it.key().context("last key")?;
                    ensure!(
                        key.len() == 31 && key[..17] == low[..17] && key.as_ref() >= low.as_slice(),
                        "last key outside RIC"
                    );
                    let last = (
                        u64::from_be_bytes(key[17..25].try_into()?),
                        u64::from_le_bytes(it.value().unwrap()[20..28].try_into()?),
                    );
                    it.status()?;
                    let (min, max) = if kind == 1 {
                        (&mut trade_first, &mut trade_last)
                    } else {
                        (&mut quote_first, &mut quote_last)
                    };
                    *min = Some(min.map_or(first.0, |x: u64| x.min(first.0)));
                    *max = Some(max.map_or(last.0, |x: u64| x.max(last.0)));
                    evidence.push(json!({"cf":cf_name,"ric":ric,"kind":kind,"first_event_ns":first.0,"last_event_ns":last.0,"first_source_ns":first.1,"last_source_ns":last.1}));
                }
            }
        }
        results.insert(j["ric"].as_str().unwrap().into(),json!({"trade_first_ns":trade_first,"trade_last_ns":trade_last,"quote_first_bucket_ns":quote_first,"quote_last_bucket_ns":quote_last,"evidence":evidence}));
        if (index + 1) % 100 == 0 {
            eprintln!("probed {} / {} RICs", index + 1, jobs.len());
        }
    }
    if let Some(parent) = a.output.parent() {
        fs::create_dir_all(parent)?;
    }
    serde_json::to_writer_pretty(
        File::create(a.output)?,
        &json!({"status":"passed","db":a.db,"bounds":results,"quote_basis":"exchange-time second bucket; source timestamp endpoints retained in evidence","cycle_guard":"source event >= Jan1(delivery_year-9), < Jan1(delivery_year+2); HRD expiry-approved aliases only"}),
    )?;
    Ok(())
}
