//! Read only the kind=12 key ranges, never replay or mutate production data.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use cme_tas_replay::{decode_cme_correction, product::{product_cf_name, quote_last_merge}};
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, ReadOptions, DB};
use serde_json::{json, Value};
use std::{collections::BTreeSet, fs, path::PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)] plan: PathBuf,
    #[arg(long)] output: PathBuf,
}

fn main() -> Result<()> {
    let args=Args::parse();
    let plan:Value=serde_json::from_slice(&fs::read(args.plan)?)?;
    let rics:BTreeSet<_>=plan["rics"].as_array().context("rics")?.iter().map(|v|v.as_str().unwrap().to_owned()).collect();
    let cache=Cache::new_lru_cache(64*1024*1024);
    let options=|| {
        let mut o=Options::default();o.set_max_open_files(128);o.set_skip_stats_update_on_db_open(true);
        o.set_merge_operator_associative("quote_last",quote_last_merge);
        let mut b=BlockBasedOptions::default();b.set_block_cache(&cache);o.set_block_based_table_factory(&b);o
    };
    let mut rows=Vec::new();
    for job in plan["databases"].as_array().context("databases")? {
        let path=PathBuf::from(job["path"].as_str().context("path")?);
        let before=fs::read(path.join("CURRENT"))?;
        let names:BTreeSet<_>=DB::list_cf(&options(),&path)?.into_iter().collect();
        let mut selected=BTreeSet::from(["default".to_owned()]);
        for product in job["products"].as_array().context("products")? {
            let name=product_cf_name(2026,product.as_str().unwrap())?;
            if names.contains(&name) {selected.insert(name);}
        }
        eprintln!("read-only {} ({} CFs)",path.display(),selected.len());
        let db=DB::open_cf_descriptors_read_only(&options(),&path,
            selected.iter().map(|name|ColumnFamilyDescriptor::new(name,options())),false)?;
        for name in selected.iter().filter(|name|name.starts_with("p:")) {
            let cf=db.cf_handle(name).unwrap();let mut ro=ReadOptions::default();ro.fill_cache(false);ro.set_verify_checksums(true);
            let mut it=db.raw_iterator_cf_opt(cf,ro);it.seek([12u8]);
            while let Some(key)=it.key() {
                if key[0]!=12 {break;}
                ensure!(key.len()==31,"unexpected correction key");
                let row=decode_cme_correction(it.value().context("value")?)?;
                if rics.contains(&row.ric) {
                    let price=|v:i64|if v==i64::MIN {None}else{Some(v)};
                    let qty=|v:u32|if v==u32::MAX {None}else{Some(v)};
                    rows.push(json!({"db":path,"source":job["source"],"cf":name,"ric":row.ric,
                        "published_ns":row.ts_utc_ns,"exch_hms_ns":if row.exch_hms_ns==u64::MAX {None}else{Some(row.exch_hms_ns)},
                        "price":price(row.price),"volume":qty(row.volume),"acc_volume":qty(row.acc_volume),
                        "original_price":price(row.original_price),"original_volume":qty(row.original_volume),
                        "original_seq":if row.original_seq==u64::MAX {None}else{Some(row.original_seq)},"qualifiers":row.qualifiers,
                        "part":u16::from_be_bytes(key[25..27].try_into()?),"ordinal":u32::from_be_bytes(key[27..31].try_into()?)}));
                }
                it.next();
            }
            it.status()?;
        }
        drop(db);ensure!(before==fs::read(path.join("CURRENT"))?,"production CURRENT changed");
    }
    if let Some(parent)=args.output.parent() {fs::create_dir_all(parent)?;}
    let temporary=args.output.with_extension("json.partial");
    fs::write(&temporary,serde_json::to_vec_pretty(&json!({"corrections":rows}))?)?;
    fs::rename(temporary,args.output)?;
    eprintln!("{} correction events",rows.len());Ok(())
}
