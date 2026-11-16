//! Append audited TAS data to an existing DB, with semantic deduplication.
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use cme_tas_replay::product::{is_product_cf_name, quote_last_merge, ALL_KEY_LEN};
use rocksdb::{
    checkpoint::Checkpoint, BlockBasedOptions, Cache, ColumnFamilyDescriptor,
    IngestExternalFileOptions, IteratorMode, Options, ReadOptions, SstFileWriter, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

const IMPORT_PART: u16 = u16::MAX;
const META_KEY: &str = "supplement:futures_11:2026-01-01_2026-05-30";

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Plan {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        target: PathBuf,
        #[arg(long)]
        source_audit: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Apply {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        checkpoint: PathBuf,
    },
}
#[derive(Default, Serialize, Deserialize)]
struct CfPlan {
    name: String,
    new_cf: bool,
    source_rows: u64,
    insert_rows: u64,
    duplicate_rows: u64,
    quote_target_preferred: u64,
    rekeyed_rows: u64,
    source_sha256: String,
    insert_sha256: String,
    sst: Option<PathBuf>,
    sst_sha256: Option<String>,
    kinds_inserted: BTreeMap<u8, u64>,
    difference_examples: Vec<serde_json::Value>,
}
#[derive(Serialize, Deserialize)]
struct Plan {
    status: String,
    source: PathBuf,
    target: PathBuf,
    target_identity: String,
    target_sequence: u64,
    original_cfs: Vec<String>,
    original_meta: BTreeMap<String, String>,
    source_audit: Option<PathBuf>,
    source_audit_sha256: Option<String>,
    products: Vec<CfPlan>,
    policy: String,
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let partial = path.with_extension("json.partial");
    let mut f = File::create(&partial)?;
    serde_json::to_writer_pretty(&mut f, value)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    fs::rename(partial, path)?;
    Ok(())
}
fn file_hash(path: &Path) -> Result<String> {
    let mut f = File::open(path)?;
    let mut hash = Sha256::new();
    std::io::copy(&mut f, &mut hash)?;
    Ok(format!("{:x}", hash.finalize()))
}
fn options(cache: &Cache) -> Options {
    let mut o = Options::default();
    o.set_max_open_files(512);
    o.set_max_file_opening_threads(4);
    o.set_skip_stats_update_on_db_open(true);
    o.set_disable_auto_compactions(true);
    o.set_write_buffer_size(16 * 1024 * 1024);
    o.set_max_write_buffer_number(2);
    o.set_db_write_buffer_size(256 * 1024 * 1024);
    o.set_max_background_jobs(4);
    o.increase_parallelism(4);
    o.set_compression_type(rocksdb::DBCompressionType::Lz4);
    o.set_merge_operator_associative("quote_last", quote_last_merge);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(cache);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(false);
    o.set_block_based_table_factory(&table);
    o
}
fn read_options() -> ReadOptions {
    let mut r = ReadOptions::default();
    r.fill_cache(false);
    r.set_verify_checksums(true);
    r
}
fn readonly(path: &Path, names: &[String], cache: &Cache) -> Result<DB> {
    Ok(DB::open_cf_descriptors_read_only(
        &options(cache),
        path,
        names
            .iter()
            .map(|n| ColumnFamilyDescriptor::new(n, options(cache))),
        false,
    )?)
}
fn meta(db: &DB) -> Result<BTreeMap<String, String>> {
    let cf = db.cf_handle("replay_meta").context("missing replay_meta")?;
    db.iterator_cf(cf, IteratorMode::Start)
        .map(|r| {
            let (k, v) = r?;
            Ok((
                String::from_utf8(k.to_vec())?,
                String::from_utf8(v.to_vec())?,
            ))
        })
        .collect()
}
fn make_plan(source: &Path, target: &Path, source_audit: Option<&Path>, out: &Path) -> Result<()> {
    ensure!(
        !out.exists(),
        "plan output already exists; preserve earlier evidence"
    );
    fs::create_dir_all(out)?;
    let source = source.canonicalize()?;
    let target = target.canonicalize()?;
    ensure!(source != target, "source is target");
    let all = DB::list_cf(&Options::default(), &target)?;
    let names: BTreeSet<_> = all.iter().cloned().collect();
    let mut products: Vec<_> = DB::list_cf(&Options::default(), &source)?
        .into_iter()
        .filter(|n| is_product_cf_name(n))
        .collect();
    products.sort();
    ensure!(!products.is_empty(), "no source products");
    let mut sn = products.clone();
    sn.extend(["default".into(), "replay_meta".into()]);
    let mut tn: Vec<_> = products
        .iter()
        .filter(|n| names.contains(*n))
        .cloned()
        .collect();
    tn.extend(["default".into(), "replay_meta".into()]);
    let cache = Cache::new_lru_cache(128 * 1024 * 1024);
    let source_db = readonly(&source, &sn, &cache)?;
    let target_db = readonly(&target, &tn, &cache)?;
    let original_meta = meta(&target_db)?;
    ensure!(
        !original_meta.contains_key(META_KEY),
        "supplement already claimed"
    );
    for (k, v) in &original_meta {
        if k.starts_with("period:") {
            ensure!(v == "done", "unfinished target period {k}");
        }
    }
    let sm = meta(&source_db)?;
    ensure!(
        sm.get("period:2026-01-01_2026-05-30").map(String::as_str) == Some("done"),
        "source not complete"
    );
    let expected: Option<serde_json::Value> = source_audit
        .map(|p| Ok::<_, anyhow::Error>(serde_json::from_reader(File::open(p)?)?))
        .transpose()?;
    if let Some(e) = &expected {
        ensure!(e["status"] == "passed", "source audit did not pass");
    }
    let mut reports = Vec::new();
    let sst_options = options(&cache);
    for name in &products {
        let new_cf = !names.contains(name);
        eprintln!("planning {name}, new_cf={new_cf}");
        let scf = source_db.cf_handle(name).unwrap();
        let tcf = target_db.cf_handle(name);
        let mut tr = tcf.map(|c| target_db.raw_iterator_cf_opt(c, read_options()));
        if let Some(r) = tr.as_mut() {
            r.seek_to_first();
        }
        let sst = out.join(format!("{name}.sst"));
        let mut writer = SstFileWriter::create(&sst_options);
        writer.open(&sst)?;
        let mut r = CfPlan {
            name: name.clone(),
            new_cf,
            ..Default::default()
        };
        let mut sh = Sha256::new();
        let mut ih = Sha256::new();
        let mut it = source_db
            .iterator_cf_opt(scf, read_options(), IteratorMode::Start)
            .peekable();
        if new_cf {
            for row in it {
                let (key, value) = row?;
                ensure!(key.len() == ALL_KEY_LEN, "bad source key");
                writer.put(&key, &value)?;
                sh.update(&key);
                sh.update(&value);
                ih.update(&key);
                ih.update(&value);
                r.source_rows += 1;
                r.insert_rows += 1;
                *r.kinds_inserted.entry(key[0]).or_default() += 1;
            }
        } else {
            while let Some(row) = it.next() {
                let (first_key, first_value) = row?;
                ensure!(first_key.len() == ALL_KEY_LEN, "bad source key");
                let prefix = first_key[..25].to_vec();
                let mut group = vec![(first_key.to_vec(), first_value.to_vec())];
                while let Some(Ok((key, _))) = it.peek() {
                    if key.len() != ALL_KEY_LEN || key[..25] != prefix {
                        break;
                    }
                    let (key, value) = it.next().unwrap()?;
                    group.push((key.to_vec(), value.to_vec()));
                }
                let mut original = HashMap::<Vec<u8>, u64>::new();
                let mut original_n = 0u64;
                let mut next_seq = 0u64;
                if let Some(t) = tr.as_mut() {
                    if t.key().is_some_and(|k| k[..25] < prefix[..]) {
                        t.seek(&prefix);
                    }
                    while t.key().is_some_and(|k| k[..25] == prefix[..]) {
                        let k = t.key().unwrap();
                        let v = t.value().unwrap();
                        *original.entry(v.to_vec()).or_default() += 1;
                        original_n += 1;
                        if u16::from_be_bytes(k[25..27].try_into()?) == IMPORT_PART {
                            next_seq =
                                next_seq.max(u32::from_be_bytes(k[27..31].try_into()?) as u64 + 1);
                        }
                        t.next();
                    }
                    t.status()?;
                }
                let mut seen = HashMap::<Vec<u8>, u64>::new();
                for (mut key, value) in group {
                    sh.update(&key);
                    sh.update(&value);
                    r.source_rows += 1;
                    if key[0] == 3 && original_n > 0 {
                        if original.contains_key(&value) {
                            r.duplicate_rows += 1;
                        } else {
                            r.quote_target_preferred += 1;
                            if r.difference_examples.len() < 20 {
                                r.difference_examples.push(serde_json::json!({"ric":String::from_utf8_lossy(&key[1..17]).trim_end_matches('\0'),"bucket_ns":u64::from_be_bytes(key[17..25].try_into()?),"policy":"existing quote retained"}));
                            }
                        }
                        continue;
                    }
                    let count = seen.entry(value.clone()).or_default();
                    *count += 1;
                    if *count <= original.get(&value).copied().unwrap_or(0) {
                        r.duplicate_rows += 1;
                        continue;
                    }
                    if original_n > 0 {
                        ensure!(next_seq <= u32::MAX as u64, "import seq overflow");
                        key[25..27].copy_from_slice(&IMPORT_PART.to_be_bytes());
                        key[27..31].copy_from_slice(&(next_seq as u32).to_be_bytes());
                        next_seq += 1;
                        r.rekeyed_rows += 1;
                    }
                    writer.put(&key, &value)?;
                    ih.update(&key);
                    ih.update(&value);
                    r.insert_rows += 1;
                    *r.kinds_inserted.entry(key[0]).or_default() += 1;
                }
            }
        }
        r.source_sha256 = format!("{:x}", sh.finalize());
        r.insert_sha256 = format!("{:x}", ih.finalize());
        ensure!(
            r.source_rows == r.insert_rows + r.duplicate_rows + r.quote_target_preferred,
            "accounting error"
        );
        if let Some(e) = &expected {
            ensure!(
                e["key_value_sha256"][name].as_str() == Some(&r.source_sha256),
                "audited source changed: {name}"
            );
            ensure!(
                e["products"][name]["rows"].as_u64() == Some(r.source_rows),
                "source count mismatch"
            );
        }
        if r.insert_rows > 0 {
            writer.finish()?;
            r.sst = Some(sst.clone());
        }
        drop(writer);
        if r.insert_rows > 0 {
            r.sst_sha256 = Some(file_hash(&sst)?);
        } else {
            fs::remove_file(&sst)?;
        }
        eprintln!(
            "planned {name}: source={} insert={} duplicate={} quote_preferred={} rekeyed={}",
            r.source_rows,
            r.insert_rows,
            r.duplicate_rows,
            r.quote_target_preferred,
            r.rekeyed_rows
        );
        write_json(&out.join(format!("{name}.json")), &r)?;
        reports.push(r);
    }
    let plan=Plan { status:"ready".into(),source,target:target.clone(),
        target_identity:fs::read_to_string(target.join("IDENTITY"))?,target_sequence:target_db.latest_sequence_number(),
        original_cfs:all,original_meta,source_audit:source_audit.map(Path::to_path_buf),
        source_audit_sha256:source_audit.map(file_hash).transpose()?,products:reports,
        policy:"append only; compare non-quote value multiplicities within kind/RIC/timestamp ignoring source part; existing quote bucket wins; colliding new non-quotes use reserved part 65535".into() };
    write_json(&out.join("plan.json"), &plan)?;
    Ok(())
}
fn apply_plan(plan_path: &Path, checkpoint: &Path) -> Result<()> {
    let plan: Plan = serde_json::from_reader(File::open(plan_path)?)?;
    ensure!(plan.status == "ready", "plan not ready");
    let out = plan_path.parent().context("plan directory")?;
    ensure!(!out.join("completion.json").exists(), "already completed");
    ensure!(
        !checkpoint.exists(),
        "checkpoint already exists; inspect previous attempt"
    );
    ensure!(
        fs::read_to_string(plan.target.join("IDENTITY"))? == plan.target_identity,
        "target DB identity changed"
    );
    if let Some(path) = &plan.source_audit {
        ensure!(
            Some(file_hash(path)?) == plan.source_audit_sha256,
            "source audit changed"
        );
    }
    let names = DB::list_cf(&Options::default(), &plan.target)?;
    ensure!(
        names.iter().collect::<BTreeSet<_>>() == plan.original_cfs.iter().collect::<BTreeSet<_>>(),
        "target CF catalog changed; re-plan"
    );
    for p in &plan.products {
        if let Some(path) = &p.sst {
            ensure!(Some(file_hash(path)?) == p.sst_sha256, "staged SST changed");
        }
    }
    let cache = Cache::new_lru_cache(256 * 1024 * 1024);
    let mut db = DB::open_cf_descriptors(
        &options(&cache),
        &plan.target,
        names
            .iter()
            .map(|n| ColumnFamilyDescriptor::new(n, options(&cache))),
    )?;
    ensure!(
        db.latest_sequence_number() == plan.target_sequence,
        "target sequence changed; re-plan"
    );
    ensure!(meta(&db)? == plan.original_meta, "target metadata changed");
    eprintln!("creating rollback checkpoint {}", checkpoint.display());
    Checkpoint::new(&db)?.create_checkpoint(checkpoint)?;
    write_json(
        &out.join("checkpoint.json"),
        &serde_json::json!({"path":checkpoint,"identity":plan.target_identity,"sequence":plan.target_sequence}),
    )?;
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    db.put_cf_opt(
        db.cf_handle("replay_meta").unwrap(),
        META_KEY,
        b"writing",
        &wo,
    )?;
    let copy_dir = out.join("ingest_copies");
    fs::create_dir(&copy_dir)?;
    for p in &plan.products {
        if p.insert_rows == 0 {
            continue;
        }
        if p.new_cf {
            db.create_cf(&p.name, &options(&cache))?;
        }
        let staged = p.sst.as_ref().context("missing SST")?;
        let copy = copy_dir.join(staged.file_name().unwrap());
        fs::copy(staged, &copy)?;
        let mut ingest = IngestExternalFileOptions::default();
        ingest.set_move_files(true);
        ingest.set_snapshot_consistency(true);
        ingest.set_allow_global_seqno(true);
        ingest.set_allow_blocking_flush(false);
        db.ingest_external_file_cf_opts(db.cf_handle(&p.name).unwrap(), &ingest, vec![copy])?;
        eprintln!("inserted {}: {} rows", p.name, p.insert_rows);
    }
    let mut backup_names = vec!["default".to_string(), "replay_meta".to_string()];
    backup_names.extend(
        plan.products
            .iter()
            .filter(|p| !p.new_cf)
            .map(|p| p.name.clone()),
    );
    let backup = readonly(checkpoint, &backup_names, &cache)?;
    let mut verified = BTreeMap::new();
    for p in &plan.products {
        if p.new_cf && p.insert_rows == 0 {
            continue;
        }
        let cf = db.cf_handle(&p.name).context("target CF missing")?;
        let mut before = if p.new_cf {
            None
        } else {
            Some(backup.raw_iterator_cf_opt(backup.cf_handle(&p.name).unwrap(), read_options()))
        };
        if let Some(b) = before.as_mut() {
            b.seek_to_first();
        }
        let mut added = 0u64;
        let mut retained = 0u64;
        let mut hash = Sha256::new();
        for row in db.iterator_cf_opt(cf, read_options(), IteratorMode::Start) {
            let (key, value) = row?;
            let is_old = if let Some(b) = before.as_mut() {
                if let Some(k) = b.key() {
                    ensure!(key.as_ref() <= k, "original key removed");
                    if key.as_ref() == k {
                        ensure!(
                            b.value() == Some(value.as_ref()),
                            "original value overwritten"
                        );
                        b.next();
                        retained += 1;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };
            if !is_old {
                added += 1;
                hash.update(&key);
                hash.update(&value);
            }
        }
        if let Some(b) = before.as_ref() {
            b.status()?;
            ensure!(!b.valid(), "original suffix removed");
        }
        ensure!(
            added == p.insert_rows,
            "inserted row count differs for {}",
            p.name
        );
        ensure!(
            format!("{:x}", hash.finalize()) == p.insert_sha256,
            "inserted bytes differ for {}",
            p.name
        );
        eprintln!(
            "verified {}: added={added} original_retained={retained}",
            p.name
        );
        verified.insert(p.name.clone(),serde_json::json!({"inserted":added,"original_retained_unchanged":retained,"insert_sha256":p.insert_sha256}));
    }
    let current_meta = meta(&db)?;
    for (k, v) in &plan.original_meta {
        ensure!(current_meta.get(k) == Some(v), "original metadata changed");
    }
    let result = serde_json::json!({"status":"complete_verified","completed_at":chrono::Utc::now().to_rfc3339(),
        "target":plan.target,"source":plan.source,"checkpoint":checkpoint,"plan_sha256":file_hash(plan_path)?,
        "inserted_rows":plan.products.iter().map(|p|p.insert_rows).sum::<u64>(),
        "duplicate_rows":plan.products.iter().map(|p|p.duplicate_rows).sum::<u64>(),
        "quote_target_preferred":plan.products.iter().map(|p|p.quote_target_preferred).sum::<u64>(),
        "new_product_cfs":plan.products.iter().filter(|p|p.new_cf&&p.insert_rows>0).map(|p|&p.name).collect::<Vec<_>>(),
        "verified":verified,"original_period_watermarks_unchanged":true,"policy":plan.policy});
    db.put_cf_opt(
        db.cf_handle("replay_meta").unwrap(),
        META_KEY,
        serde_json::to_vec(&result)?,
        &wo,
    )?;
    db.flush_cf(db.cf_handle("replay_meta").unwrap())?;
    db.flush_wal(true)?;
    drop(backup);
    drop(db);
    write_json(&out.join("completion.json"), &result)?;
    write_json(
        &plan.target.join("_SUPPLEMENT_MERGE_SUCCESS_20261103.json"),
        &result,
    )?;
    Ok(())
}
fn main() -> Result<()> {
    match Args::parse().command {
        Command::Plan {
            source,
            target,
            source_audit,
            output,
        } => make_plan(&source, &target, Some(&source_audit), &output),
        Command::Apply { plan, checkpoint } => apply_plan(&plan, &checkpoint),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::product::encode_all_key;
    use cme_tas_replay::{
        encode_cme_quote, encode_cme_trade, SlimQuote, SlimTrade, MISSING_PRICE, MISSING_VOLUME,
    };
    fn fixture(path: &Path, names: &[&str]) -> DB {
        let cache = Cache::new_lru_cache(1024 * 1024);
        let mut o = options(&cache);
        o.create_if_missing(true);
        o.create_missing_column_families(true);
        DB::open_cf_descriptors(
            &o,
            path,
            names
                .iter()
                .map(|n| ColumnFamilyDescriptor::new(*n, options(&cache))),
        )
        .unwrap()
    }
    fn trade(ric: &str, ts: u64) -> Vec<u8> {
        encode_cme_trade(&SlimTrade {
            ric: ric.into(),
            ts_utc_ns: ts,
            exch_hms_ns: ts,
            price: 12,
            volume: 1,
            bid: MISSING_PRICE,
            bid_size: MISSING_VOLUME,
            ask: MISSING_PRICE,
            ask_size: MISSING_VOLUME,
            aggressor: 0,
        })
        .unwrap()
        .to_vec()
    }
    #[test]
    fn semantic_dedup_preserves_original_and_duplicate_multiplicity() -> Result<()> {
        let base =
            PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/merge-test")
                .join(format!(
                    "{}-{}",
                    std::process::id(),
                    chrono::Utc::now().timestamp_nanos_opt().unwrap()
                ));
        fs::create_dir_all(&base)?;
        let target = base.join("target");
        let source = base.join("source");
        let t = fixture(&target, &["default", "replay_meta", "p:2026:TEST"]);
        let s = fixture(
            &source,
            &["default", "replay_meta", "p:2026:TEST", "p:2026:NEW"],
        );
        for db in [&t, &s] {
            db.put_cf(
                db.cf_handle("replay_meta").unwrap(),
                "period:2026-01-01_2026-05-30",
                b"done",
            )?;
        }
        let ric = "TESTH26";
        let tc = t.cf_handle("p:2026:TEST").unwrap();
        let sc = s.cf_handle("p:2026:TEST").unwrap();
        t.put_cf(tc, encode_all_key(1, ric, 100, 7, 5)?, trade(ric, 100))?;
        s.put_cf(sc, encode_all_key(1, ric, 100, 0, 0)?, trade(ric, 100))?;
        t.put_cf(tc, encode_all_key(1, ric, 200, 1, 0)?, trade(ric, 200))?;
        for seq in 0..2 {
            s.put_cf(sc, encode_all_key(1, ric, 200, 0, seq)?, trade(ric, 200))?;
        }
        s.put_cf(sc, encode_all_key(1, ric, 300, 0, 0)?, trade(ric, 300))?;
        s.put_cf(
            s.cf_handle("p:2026:NEW").unwrap(),
            encode_all_key(1, "NEWH26", 400, 0, 0)?,
            trade("NEWH26", 400),
        )?;
        let q = SlimQuote {
            ric: ric.into(),
            ts_utc_ns: 6_000_000_001,
            exch_hms_ns: 6_000_000_001,
            bid: 10,
            bid_size: 1,
            ask: 20,
            ask_size: 1,
        };
        let qk = encode_all_key(3, ric, 6_000_000_000, 0, 0)?;
        t.put_cf(tc, qk, encode_cme_quote(&q)?)?;
        let mut q2 = q.clone();
        q2.bid = 11;
        s.put_cf(sc, qk, encode_cme_quote(&q2)?)?;
        for db in [&t, &s] {
            for n in DB::list_cf(&Options::default(), db.path())? {
                db.flush_cf(db.cf_handle(&n).unwrap())?;
            }
        }
        drop(t);
        drop(s);
        let out = base.join("plan");
        make_plan(&source, &target, None, &out)?;
        let plan: Plan = serde_json::from_reader(File::open(out.join("plan.json"))?)?;
        assert_eq!(plan.products.iter().map(|p| p.insert_rows).sum::<u64>(), 3);
        assert_eq!(
            plan.products.iter().map(|p| p.duplicate_rows).sum::<u64>(),
            2
        );
        assert_eq!(
            plan.products
                .iter()
                .map(|p| p.quote_target_preferred)
                .sum::<u64>(),
            1
        );
        assert_eq!(plan.products.iter().map(|p| p.rekeyed_rows).sum::<u64>(), 1);
        let cp = base.join("checkpoint");
        apply_plan(&out.join("plan.json"), &cp)?;
        assert!(apply_plan(&out.join("plan.json"), &base.join("second-checkpoint")).is_err());
        let cache = Cache::new_lru_cache(1024 * 1024);
        let names = DB::list_cf(&Options::default(), &target)?;
        let final_db = readonly(&target, &names, &cache)?;
        let cf = final_db.cf_handle("p:2026:TEST").unwrap();
        assert_eq!(final_db.get_cf(cf, qk)?.unwrap(), encode_cme_quote(&q)?);
        assert_eq!(
            final_db
                .get_cf(cf, encode_all_key(1, ric, 200, IMPORT_PART, 0)?)?
                .unwrap(),
            trade(ric, 200)
        );
        drop(final_db);
        fs::remove_dir_all(base)?;
        Ok(())
    }
}
