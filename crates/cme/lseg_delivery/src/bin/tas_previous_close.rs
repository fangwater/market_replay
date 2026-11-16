//! Bounded read-only reverse seeks for each RIC's own pre-window trade close.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use cme_tas_replay::{
    decode_cme_trade,
    product::{encode_all_key, product_cf_name},
    trade_timing::{bar_time, TimeBasis},
};
use lseg_delivery::tasdb::{self, Tuning};
use rocksdb::{Options, ReadOptions, DB};
use serde_json::{json, Value};
use std::{collections::BTreeSet, fs, path::PathBuf};

const HALF_DAY: u64 = 43_200_000_000_000;

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
    let args = Args::parse();
    let plan: Value = serde_json::from_slice(&fs::read(&args.plan)?)?;
    let jobs = plan
        .as_object()
        .context("contract plan must be keyed by RIC")?;
    let names: BTreeSet<_> = DB::list_cf(&Options::default(), &args.db)?
        .into_iter()
        .collect();
    let mut selected = BTreeSet::from(["default".to_owned()]);
    for job in jobs.values() {
        let product = job["product"].as_str().context("product")?;
        for year in 2010..=2026 {
            let name = product_cf_name(year, product)?;
            if names.contains(&name) {
                selected.insert(name);
            }
        }
    }
    let before = fs::read(args.db.join("CURRENT"))?;
    eprintln!("opening {} column families read-only", selected.len());
    let db = tasdb::open_read_only(&args.db, &selected, Tuning::new(64, 256))?;
    let mut seeds = serde_json::Map::new();
    let mut examined = 0u64;
    for (ric, job) in jobs {
        // Query canonical identities only, never guess one-digit year aliases.
        let product = job["product"].as_str().unwrap();
        let start = job["start_ns"].as_u64().context("start_ns")?;
        let upper = encode_all_key(1, ric, start + HALF_DAY, 0, 0)?;
        let mut best: Option<(u64, u64, Vec<u8>, i64, String)> = None;
        for year in (2010u16..=2026).rev() {
            let name = product_cf_name(year, product)?;
            let Some(cf) = db.cf_handle(&name) else {
                continue;
            };
            let year_end = chrono::NaiveDate::from_ymd_opt(i32::from(year) + 1, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap() as u64;
            if best
                .as_ref()
                .is_some_and(|b| year_end.saturating_add(HALF_DAY) < b.0)
            {
                continue;
            }
            let mut ro = ReadOptions::default();
            ro.fill_cache(false);
            ro.set_verify_checksums(true);
            let mut it = db.raw_iterator_cf_opt(cf, ro);
            it.seek_for_prev(&upper);
            if it.key().is_some_and(|key| key >= upper.as_slice()) {
                it.prev();
            }
            while let Some(key) = it.key() {
                if key.len() != 31 || key[..17] != upper[..17] {
                    break;
                }
                let published = u64::from_be_bytes(key[17..25].try_into()?);
                if best
                    .as_ref()
                    .is_some_and(|b| published.saturating_add(HALF_DAY) < b.0)
                {
                    break;
                }
                let trade = decode_cme_trade(it.value().context("trade value")?)?;
                ensure!(
                    trade.ric == *ric && trade.ts_utc_ns == published,
                    "trade/key mismatch: {ric}"
                );
                let event = bar_time(published, trade.exch_hms_ns, TimeBasis::Exchange)?;
                examined += 1;
                if event < start && trade.price != i64::MIN {
                    let order = (event, published, key);
                    if best
                        .as_ref()
                        .is_none_or(|b| order > (b.0, b.1, b.2.as_slice()))
                    {
                        best = Some((event, published, key.to_vec(), trade.price, name.clone()));
                    }
                }
                it.prev();
            }
            it.status()?;
        }
        seeds.insert(ric.clone(),best.map_or(Value::Null,|b| json!({
            "event_ns":b.0,"published_ns":b.1,"price_e9":b.3,"column_family":b.4,"source_ric":ric
        })));
    }
    drop(db);
    ensure!(
        before == fs::read(args.db.join("CURRENT"))?,
        "production CURRENT changed during read-only query"
    );
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = args.output.with_extension("json.partial");
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(&json!({"seeds":seeds,"examined_trades":examined,
        "basis":"own canonical RIC, latest exchange event before start; nearest UTC date within 12h of publication"}))?,
    )?;
    fs::rename(temporary, args.output)?;
    eprintln!(
        "{} RICs, {} candidate trades inspected",
        jobs.len(),
        examined
    );
    Ok(())
}
