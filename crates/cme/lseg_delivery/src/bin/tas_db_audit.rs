//! Read-only persisted-record audit for an isolated TAS replay.
use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use cme_tas_replay::{product::*, *};
use lseg_delivery::tasdb::{self, Tuning};
use rocksdb::{IteratorMode, ReadOptions};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    dump: Option<PathBuf>,
    #[arg(long)]
    require_done: Option<String>,
    /// Publication-time bounds; defaults to --require-done or the old supplement period.
    #[arg(long)]
    source_period: Option<String>,
}
#[derive(Default, Serialize)]
struct Inventory {
    rows: u64,
    kinds: BTreeMap<u8, u64>,
    first_ts_ns: Option<u64>,
    last_ts_ns: Option<u64>,
}
impl Inventory {
    fn add(&mut self, kind: u8, ts: u64) {
        self.rows += 1;
        *self.kinds.entry(kind).or_default() += 1;
        self.first_ts_ns = Some(self.first_ts_ns.map_or(ts, |t| t.min(ts)));
        self.last_ts_ns = Some(self.last_ts_ns.map_or(ts, |t| t.max(ts)));
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn validate(kind: u8, value: &[u8]) -> Result<()> {
    macro_rules! check {
        ($decode:ident) => {{
            $decode(value)?;
        }};
    }
    match kind {
        1 => check!(decode_cme_trade),
        2 => check!(decode_cme_special),
        3 => check!(decode_cme_quote),
        4 => check!(decode_symbology_change),
        5 => check!(decode_cme_price_limit),
        6 => check!(decode_cme_settlement),
        7 => check!(decode_cme_implied_vol),
        8 => check!(decode_cme_tot_volume),
        9 => check!(decode_cme_price_print),
        10 => check!(decode_cme_imbalance),
        11 => check!(decode_cme_auction),
        12 => check!(decode_cme_correction),
        13 => check!(decode_cme_status),
        14 => check!(decode_reference_change),
        _ => bail!("unknown stored kind {kind}"),
    }
    Ok(())
}
fn main() -> Result<()> {
    let a = Args::parse();
    let source_period = a
        .source_period
        .clone()
        .or_else(|| a.require_done.clone())
        .unwrap_or_else(|| "2026-01-01_2026-05-30".to_string());
    let (start, end) = source_period
        .split_once('_')
        .context("source period requires start_end")?;
    let start = chrono::NaiveDate::parse_from_str(start, "%Y-%m-%d")?;
    let end = chrono::NaiveDate::parse_from_str(end, "%Y-%m-%d")?;
    ensure!(start < end, "invalid source period bounds");
    let year = source_period[..4].parse::<u16>()?;
    let to_ns = |d: chrono::NaiveDate| -> Result<u64> {
        let seconds = d
            .and_hms_opt(0, 0, 0)
            .context("date midnight")?
            .and_utc()
            .timestamp();
        u64::try_from(seconds)?
            .checked_mul(1_000_000_000)
            .context("date overflow")
    };
    let bounds = to_ns(start)?..to_ns(end)?;
    let names = tasdb::list_cfs(&a.db)?;
    let db = tasdb::open_read_only(&a.db, &names, Tuning::new(32, 1024))?;
    let meta = db.cf_handle(CF_REPLAY_META).context("replay_meta")?;
    let mut watermarks = BTreeMap::new();
    for pair in db.iterator_cf(meta, IteratorMode::Start) {
        let (k, v) = pair?;
        watermarks.insert(
            String::from_utf8(k.to_vec())?,
            String::from_utf8(v.to_vec())?,
        );
    }
    if let Some(period) = &a.require_done {
        ensure!(
            watermarks
                .get(&format!("period:{period}"))
                .map(String::as_str)
                == Some("done"),
            "period is not done"
        );
    }
    let mut dump = a
        .dump
        .as_ref()
        .map(File::create)
        .transpose()?
        .map(BufWriter::new);
    let mut all = Inventory::default();
    let mut products = BTreeMap::new();
    let mut rics = BTreeMap::<String, Inventory>::new();
    let mut digests = BTreeMap::new();
    let mut samples = BTreeMap::new();
    for name in &names {
        if !is_product_cf_name(name) {
            continue;
        }
        let cf = db.cf_handle(name).context("CF missing")?;
        let mut ro = ReadOptions::default();
        ro.fill_cache(false);
        ro.set_verify_checksums(true);
        let mut counts = Inventory::default();
        let mut digest = Sha256::new();
        for pair in db.iterator_cf_opt(cf, ro, IteratorMode::Start) {
            let (key, value) = pair?;
            ensure!(
                key.len() == ALL_KEY_LEN && value.len() >= 28,
                "malformed key/value"
            );
            let kind = key[0];
            validate(kind, &value)?;
            ensure!(
                value[3] == kind && key[1..17] == value[4..20],
                "key/value identity mismatch"
            );
            let ric = decode_ric(&key[1..17])?;
            let product = parse_product(&ric).context("unroutable stored RIC")?;
            ensure!(
                product_cf_name(year, &product)? == *name,
                "misrouted RIC {ric}"
            );
            let ts = u64::from_le_bytes(value[20..28].try_into()?);
            let key_ts = u64::from_be_bytes(key[17..25].try_into()?);
            if kind == KIND_CME_QUOTE {
                let quote = decode_cme_quote(&value)?;
                ensure!(
                    quote_second_bucket_ns(ts, quote.exch_hms_ns)?.0 == key_ts,
                    "wrong quote bucket"
                );
                ensure!(key[25..].iter().all(|b| *b == 0), "nonzero quote part/seq");
            } else {
                ensure!(ts == key_ts, "timestamp mismatch");
            }
            ensure!(bounds.contains(&ts), "record outside source period");
            all.add(kind, ts);
            counts.add(kind, ts);
            rics.entry(ric.clone()).or_default().add(kind, ts);
            digest.update(&key);
            digest.update(&value);
            let sample_key = format!("{name}/{ric}/{kind}");
            samples
                .entry(sample_key)
                .or_insert_with(|| serde_json::json!({"key":hex(&key),"value":hex(&value)}));
            if let Some(out) = dump.as_mut() {
                serde_json::to_writer(&mut *out, &(name, hex(&key), hex(&value)))?;
                out.write_all(b"\n")?;
            }
        }
        eprintln!("audited {name}: {} rows", counts.rows);
        products.insert(name.clone(), counts);
        digests.insert(name.clone(), format!("{:x}", digest.finalize()));
    }
    if let Some(out) = dump.as_mut() {
        out.flush()?;
    }
    let result = serde_json::json!({"status":"passed", "db":a.db,"watermarks":watermarks,"source_period":source_period,
        "all":all,"products":products,"rics":rics,"key_value_sha256":digests,"samples":samples});
    serde_json::to_writer_pretty(BufWriter::new(File::create(a.output)?), &result)?;
    Ok(())
}
