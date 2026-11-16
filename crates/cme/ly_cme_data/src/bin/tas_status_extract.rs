//! Bounded read-only kind=13 extraction into a local working Parquet.
use anyhow::{ensure, Context, Result};
use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use cme_tas_replay::{decode_cme_status, product::quote_last_merge};
use parquet::{arrow::ArrowWriter, basic::{Compression,ZstdLevel}, file::properties::WriterProperties};
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, ReadOptions, DB};
use serde_json::Value;
use std::{collections::BTreeSet, fs::{self, File}, path::PathBuf, sync::Arc};

#[derive(Parser)]
struct Args {
    #[arg(long)] plan: PathBuf,
    #[arg(long)] output: PathBuf,
}

fn main() -> Result<()> {
    let args=Args::parse();
    let plan:Value=serde_json::from_slice(&fs::read(args.plan)?)?;
    fs::create_dir_all(&args.output)?;
    let schema=Arc::new(Schema::new(vec![
        Field::new("product",DataType::Utf8,false),Field::new("ric",DataType::Utf8,false),
        Field::new("ts_utc_ns",DataType::UInt64,false),Field::new("exch_hms_ns",DataType::UInt64,false),
        Field::new("qualifiers",DataType::Utf8,false),Field::new("source_index",DataType::UInt32,false),
        Field::new("source_order",DataType::UInt64,false)]));
    let year=plan["year"].as_i64().context("year")? as i32;
    let bound=|y|chrono::NaiveDate::from_ymd_opt(y,1,1).unwrap().and_hms_opt(0,0,0).unwrap().and_utc().timestamp() as u64*1_000_000_000;
    let cache=Cache::new_lru_cache(32*1024*1024);
    let options=|| {
        let mut o=Options::default();o.set_max_open_files(128);o.set_skip_stats_update_on_db_open(true);
        o.set_merge_operator_associative("quote_last",quote_last_merge);
        let mut b=BlockBasedOptions::default();b.set_block_cache(&cache);o.set_block_based_table_factory(&b);o
    };
    for (source_index,job) in plan["jobs"].as_array().context("jobs")?.iter().enumerate() {
        let id=job["id"].as_str().context("id")?;
        let path=PathBuf::from(job["db"].as_str().context("db")?);
        let current=fs::read(path.join("CURRENT"))?;
        let target=args.output.join(format!("{id}.parquet"));
        let state=args.output.join(format!("{id}.json"));
        let identity=serde_json::json!({"job":job,"current":String::from_utf8(current.clone())?,"source_index":source_index});
        if target.exists() && state.exists() {
            let prior:Value=serde_json::from_slice(&fs::read(&state)?)?;
            ensure!(prior["identity"]==identity,"source changed for {id}");continue;
        }
        let cfs=job["cfs"].as_object().context("cfs")?;
        let selected:BTreeSet<_>=cfs.keys().cloned().chain(["default".to_owned(),"replay_meta".to_owned()]).collect();
        let db=DB::open_cf_descriptors_read_only(&options(),&path,
            selected.iter().map(|n|ColumnFamilyDescriptor::new(n,options())),false)?;
        let temporary=target.with_extension("parquet.partial");
        let props=WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?)).build();
        let mut writer=ArrowWriter::try_new(File::create(&temporary)?,schema.clone(),Some(props))?;
        let mut rows=Vec::new();let mut products=Vec::new();let mut orders=Vec::new();let mut total=0u64;
        let flush=|writer:&mut ArrowWriter<File>,rows:&mut Vec<cme_tas_replay::SlimStatus>,products:&mut Vec<String>,orders:&mut Vec<u64>|->Result<()> {
            if rows.is_empty() {return Ok(());}
            let columns:Vec<ArrayRef>=vec![Arc::new(StringArray::from(products.clone())),
                Arc::new(StringArray::from(rows.iter().map(|r|r.ric.as_str()).collect::<Vec<_>>())),
                Arc::new(UInt64Array::from(rows.iter().map(|r|r.ts_utc_ns).collect::<Vec<_>>())),
                Arc::new(UInt64Array::from(rows.iter().map(|r|r.exch_hms_ns).collect::<Vec<_>>())),
                Arc::new(StringArray::from(rows.iter().map(|r|r.qualifiers.as_str()).collect::<Vec<_>>())),
                Arc::new(UInt32Array::from(vec![source_index as u32;rows.len()])),Arc::new(UInt64Array::from(orders.clone()))];
            writer.write(&RecordBatch::try_new(schema.clone(),columns)?)?;
            rows.clear();products.clear();orders.clear();Ok(())
        };
        for (name,expected) in cfs {
            let cf=db.cf_handle(name).context("CF")?;
            let mut ro=ReadOptions::default();ro.fill_cache(false);ro.set_iterate_upper_bound(vec![14]);
            let mut it=db.raw_iterator_cf_opt(cf,ro);it.seek([13]);let mut count=0u64;
            while let Some(key)=it.key() {
                if key[0]!=13 {break;}
                ensure!(key.len()==31,"status key length");
                let row=decode_cme_status(it.value().context("status value")?)?;
                ensure!(bound(year)<=row.ts_utc_ns && row.ts_utc_ns<bound(year+1),"year range");
                let order=(u16::from_be_bytes(key[25..27].try_into()?) as u64)<<32 | u32::from_be_bytes(key[27..31].try_into()?) as u64;
                rows.push(row);products.push(name.split(':').nth(2).context("product")?.to_owned());orders.push(order);
                count+=1;if rows.len()>=65536 {flush(&mut writer,&mut rows,&mut products,&mut orders)?;}
                it.next();
            }
            it.status()?;
            if let Some(expected)=expected.as_u64() {ensure!(count==expected,"status census mismatch for {name}");}
            total+=count;
        }
        flush(&mut writer,&mut rows,&mut products,&mut orders)?;writer.close()?;File::open(&temporary)?.sync_all()?;
        drop(db);ensure!(current==fs::read(path.join("CURRENT"))?,"production CURRENT changed");
        fs::rename(temporary,&target)?;
        fs::write(&state,serde_json::to_vec_pretty(&serde_json::json!({"identity":identity,"rows":total}))?)?;
        eprintln!("{id}: {total} status rows");
    }
    Ok(())
}
