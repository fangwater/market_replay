//! Export persisted TAS market-condition events, without inferring session intervals.
use anyhow::{ensure, Context, Result};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use cme_tas_replay::{decode_cme_status, product::quote_last_merge, SlimStatus, KIND_CME_STATUS};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use rayon::prelude::*;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, Direction, IteratorMode, Options,
    ReadOptions, DB,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    plan: PathBuf,
    #[arg(long)]
    output: PathBuf,
    /// Local restart state only; never published as a success report.
    #[arg(long)]
    checkpoint: PathBuf,
    #[arg(long, default_value_t = 3)]
    workers: usize,
}
#[derive(Deserialize)]
struct Plan {
    year: i32,
    exchanges: HashMap<String, String>,
    jobs: Vec<Job>,
}
#[derive(Deserialize)]
struct Job {
    id: String,
    db: PathBuf,
    period: String,
    cfs: BTreeMap<String, Option<u64>>,
}

fn component(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b"_-.^".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    if out == "." || out == ".." {
        out = out.replace('.', "%2E");
    }
    out
}

/// Extract only this FID. A missing, unknown or contradictory code stays unknown.
fn trading_status(qualifiers: &str) -> Option<String> {
    let mut found: Option<String> = None;
    for part in qualifiers.split(';') {
        if let Some(value) = part.trim().strip_suffix("[TRD_STATUS]") {
            let value = value.trim().trim_matches('"').trim();
            if value.is_empty() {
                continue;
            }
            if found.as_deref().is_some_and(|old| old != value) {
                return None;
            }
            found = Some(value.to_string());
        }
    }
    found
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(vec![
        Field::new("product", DataType::Utf8, false),
        Field::new("ric", DataType::Utf8, false),
        Field::new("ts_utc_ns", DataType::UInt64, false),
        Field::new("exch_hms_ns", DataType::UInt64, false),
        Field::new("qualifiers", DataType::Utf8, false),
        Field::new("trading_status", DataType::Utf8, true),
        Field::new("is_explicit_halt", DataType::Boolean, true),
    ], HashMap::from([
        ("source_kind".into(), "lseg_market_condition".into()),
        ("time_basis".into(), "ts_utc_ns is source publication UTC nanoseconds; no inferred exchange event time".into()),
        ("missing_exch_hms_ns".into(), u64::MAX.to_string()),
        ("status_semantics".into(), "TRD_STATUS H=halted, N=normal; is_explicit_halt is null for missing/other/contradictory codes; no forward fill; not a complete session calendar".into()),
        ("row_policy".into(), "all persisted kind=13 events; original Qualifiers and duplicate records preserved; source RocksDB key order".into()),
    ])))
}

fn batch(product: &str, rows: &[SlimStatus], schema: Arc<Schema>) -> Result<RecordBatch> {
    let codes: Vec<_> = rows.iter().map(|r| trading_status(&r.qualifiers)).collect();
    let halt = BooleanArray::from(
        codes
            .iter()
            .map(|s| match s.as_deref() {
                Some("H") => Some(true),
                Some("N") => Some(false),
                _ => None,
            })
            .collect::<Vec<_>>(),
    );
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![product; rows.len()])),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.ric.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.ts_utc_ns).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.exch_hms_ns).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.qualifiers.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(codes)),
        Arc::new(halt),
    ];
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn write_day(
    path: &Path,
    rows: &[SlimStatus],
    product: &str,
    origin: &str,
    sampled: bool,
) -> Result<()> {
    let base = schema();
    let mut metadata = base.metadata().clone();
    metadata.insert("source_origin".into(), origin.into());
    let schema = Arc::new(base.as_ref().clone().with_metadata(metadata));
    let expected = batch(product, rows, schema.clone())?;
    if !path.exists() {
        fs::create_dir_all(path.parent().context("parent")?)?;
        let temp = path.with_extension("parquet.partial");
        // A previous interrupted attempt may have left only its temporary file.
        if temp.exists() {
            fs::remove_file(&temp)?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_size(rows.len().max(1))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        writer.write(&expected)?;
        writer.close()?;
        File::open(&temp)?.sync_all()?;
        // No overwrite of an unrelated delivery, even if another process appears.
        fs::hard_link(&temp, path)?;
        fs::remove_file(temp)?;
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    ensure!(
        builder.metadata().file_metadata().num_rows() == rows.len() as i64,
        "row count: {}",
        path.display()
    );
    ensure!(
        builder.schema().as_ref() == schema.as_ref(),
        "schema/origin mismatch: {}",
        path.display()
    );
    if sampled {
        let mut reader = builder.with_batch_size(rows.len().max(1)).build()?;
        let actual = reader.next().context("missing sample batch")??;
        // Arrow's batch reader may omit file-level schema metadata from batches;
        // the builder schema above already checks it. Compare every column here.
        ensure!(
            actual.num_rows() == expected.num_rows()
                && actual.columns() == expected.columns()
                && reader.next().is_none(),
            "sample contents: {}",
            path.display()
        );
    }
    Ok(())
}

fn export_job(job: &Job, plan: &Plan, args: &Args) -> Result<()> {
    let current = fs::read_to_string(job.db.join("CURRENT"))?;
    let cache = Cache::new_lru_cache(32 * 1024 * 1024);
    let options = || {
        let mut o = Options::default();
        o.set_max_open_files(128);
        o.set_skip_stats_update_on_db_open(true);
        o.set_merge_operator_associative("quote_last", quote_last_merge);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        o.set_block_based_table_factory(&table);
        o
    };
    let names = ["default".to_string(), "replay_meta".to_string()]
        .into_iter()
        .chain(job.cfs.keys().cloned());
    let db = DB::open_cf_descriptors_read_only(
        &options(),
        &job.db,
        names.map(|n| ColumnFamilyDescriptor::new(n, options())),
        false,
    )?;
    let meta = db.cf_handle("replay_meta").context("replay_meta")?;
    ensure!(
        db.get_cf(meta, format!("period:{}", job.period))?
            .as_deref()
            == Some(b"done"),
        "period not done: {}",
        job.id
    );
    let start = chrono::NaiveDate::from_ymd_opt(plan.year, 1, 1)
        .context("year")?
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp() as u64
        * 1_000_000_000;
    let end = chrono::NaiveDate::from_ymd_opt(plan.year + 1, 1, 1)
        .context("year")?
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp() as u64
        * 1_000_000_000;
    for (name, expected_rows) in &job.cfs {
        let origin = format!(
            "{}|{}|{}|{}",
            job.db.display(),
            current.trim(),
            name,
            job.period
        );
        let checkpoint =
            args.checkpoint
                .join(format!("{}--{}.txt", component(&job.id), component(name)));
        if checkpoint.exists() {
            ensure!(
                fs::read_to_string(&checkpoint)?.starts_with(&(origin.clone() + "\n")),
                "checkpoint source changed"
            );
            continue;
        }
        let cf = db.cf_handle(name).context("product CF")?;
        let mut ro = ReadOptions::default();
        ro.fill_cache(false);
        ro.set_iterate_upper_bound(vec![KIND_CME_STATUS + 1]);
        let mut groups: BTreeMap<(String, String), Vec<SlimStatus>> = BTreeMap::new();
        let mut count = 0u64;
        for entry in db.iterator_cf_opt(
            cf,
            ro,
            IteratorMode::From(&[KIND_CME_STATUS], Direction::Forward),
        ) {
            let (key, value) = entry?;
            ensure!(key.first() == Some(&KIND_CME_STATUS), "status key range");
            let row = decode_cme_status(&value)?;
            ensure!(
                row.ts_utc_ns >= start && row.ts_utc_ns < end,
                "status outside requested year"
            );
            let date = chrono::DateTime::from_timestamp((row.ts_utc_ns / 1_000_000_000) as i64, 0)
                .context("timestamp")?
                .format("%Y%m%d")
                .to_string();
            groups.entry((row.ric.clone(), date)).or_default().push(row);
            count += 1;
        }
        if let Some(expected) = expected_rows {
            ensure!(count == *expected, "status census mismatch: {name}");
        }
        let mut ordered: Vec<_> = groups.into_iter().collect();
        ordered.sort_by(|a, b| (&a.0 .1, &a.0 .0).cmp(&(&b.0 .1, &b.0 .0)));
        let size = ordered.len();
        let product = name.splitn(3, ':').nth(2).context("CF product")?;
        let mut explicit_halts = 0usize;
        let mut review = std::collections::BTreeSet::new();
        for ((ric, _day), rows) in &ordered {
            let exchange = plan
                .exchanges
                .get(ric)
                .map(String::as_str)
                .unwrap_or("review");
            if exchange == "review" {
                review.insert(ric.clone());
            }
            explicit_halts += rows
                .iter()
                .filter(|r| trading_status(&r.qualifiers).as_deref() == Some("H"))
                .count();
        }
        ordered.par_iter().enumerate().try_for_each(
            |(index, ((ric, day), rows))| -> Result<()> {
                let exchange = plan
                    .exchanges
                    .get(ric)
                    .map(String::as_str)
                    .unwrap_or("review");
                let path = args
                    .output
                    .join(exchange)
                    .join(plan.year.to_string())
                    .join(component(ric))
                    .join(day)
                    .join("status.parquet");
                write_day(
                    &path,
                    rows,
                    product,
                    &origin,
                    index == 0 || index == size / 2 || index + 1 == size,
                )?;
                Ok(())
            },
        )?;
        ensure!(
            fs::read_to_string(job.db.join("CURRENT"))? == current,
            "source manifest changed"
        );
        fs::create_dir_all(&args.checkpoint)?;
        let state = format!("{origin}\nrows={count}\nfiles={size}\nexplicit_halts={explicit_halts}\nreview_rics={}\n", serde_json::to_string(&review)?);
        let temp = checkpoint.with_extension("partial");
        fs::write(&temp, state)?;
        fs::rename(temp, checkpoint)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let plan: Plan = serde_json::from_reader(File::open(&args.plan)?)?;
    for exchange in plan.exchanges.values() {
        ensure!(
            !exchange.is_empty()
                && exchange
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "invalid exchange path"
        );
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers.max(1))
        .build()?
        .install(|| {
            plan.jobs.par_iter().try_for_each(|j| {
                export_job(j, &plan, &args).with_context(|| format!("status source {}", j.id))
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_reason_and_unknown_codes_do_not_imply_open_or_halted() {
        for q in [
            "1[HALT_REASN];SS[HALT_RSN]",
            "   [MKT_ST_IND]",
            "X[TRD_STATUS]",
            "H[TRD_STATUS];N[TRD_STATUS]",
        ] {
            let row = SlimStatus {
                ric: "ESH26".into(),
                ts_utc_ns: 1,
                exch_hms_ns: u64::MAX,
                qualifiers: q.into(),
            };
            let b = batch("ES", &[row], schema()).unwrap();
            assert!(b.column(6).is_null(0));
        }
        assert_eq!(
            trading_status("1[HALT_REASN];\" H \"[TRD_STATUS]"),
            Some("H".into())
        );
    }
    #[test]
    fn original_qualifiers_duplicates_and_missing_exchange_time_round_trip() {
        let dir =
            PathBuf::from("/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/validation")
                .join(format!("status_parquet_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("status.parquet");
        let row = SlimStatus {
            ric: "ESH26".into(),
            ts_utc_ns: 1767225600123456789,
            exch_hms_ns: u64::MAX,
            qualifiers: "1[HALT_REASN];H[TRD_STATUS];\"  \"[HALT_RSN]".into(),
        };
        write_day(
            &path,
            &[row.clone(), row.clone()],
            "ES",
            "test source",
            true,
        )
        .unwrap();
        write_day(
            &path,
            &[row.clone(), row.clone()],
            "ES",
            "test source",
            true,
        )
        .unwrap();
        assert!(write_day(&path, &[row], "ES", "other source", false).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}
