//! Contract-level (non-HFQ) CN futures coarse bars (`baseline_data_{5min,15min,1h,1day}`)
//! aggregated from contract-level `baseline_data_1min` day files.
//!
//! Uses the exact same bar-cutting (`group_bounds`) and arithmetic (`aggregate`, NumPy pairwise sum,
//! active-minute OHLC, volume-weighted VWAP, mean TWAP, last-minute book) as `cn_hfq_bars`,
//! applied independently per `contract_id` inside each day file.

use anyhow::{bail, ensure, Context, Result};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::NaiveDate;
use clap::Parser;
use cn_hfq_bars::{aggregate, group_bounds, Kind, SourceDay};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const DEFAULT_DATA_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data";

#[derive(Parser)]
#[command(
    name = "cn-baseline-bars",
    about = "Build contract-level non-HFQ baseline_data_{5min,15min,1h,1day} from baseline_data_1min"
)]
struct Args {
    /// Data root containing `baseline_data_1min/` and target `baseline_data_{5min,15min,1h,1day}/`.
    #[arg(long, default_value = DEFAULT_DATA_ROOT)]
    data_root: PathBuf,
    /// First TradDay inclusive (YYYY-MM-DD or YYYYMMDD).
    #[arg(long)]
    start: String,
    /// Last TradDay inclusive (YYYY-MM-DD or YYYYMMDD).
    #[arg(long)]
    end: String,
    /// Worker threads (1..=8).
    #[arg(long, default_value_t = 8)]
    workers: usize,
    /// Optional comma-separated exchange filter (e.g. `ccfx,xdce,xzce`).
    #[arg(long, value_delimiter = ',')]
    exchange: Option<Vec<String>>,
    /// Optional comma-separated product filter (e.g. `M,RB`).
    #[arg(long, value_delimiter = ',')]
    product: Option<Vec<String>>,
    /// Overwrite existing output files instead of skipping.
    #[arg(long)]
    overwrite: bool,
}

fn dataset_name(kind: Kind) -> &'static str {
    match kind {
        Kind::M5 => "baseline_data_5min",
        Kind::M15 => "baseline_data_15min",
        Kind::H1 => "baseline_data_1h",
        Kind::Day => "baseline_data_1day",
    }
}

fn parse_day_ymd(raw: &str) -> Result<String> {
    let s = raw.trim();
    let d = if s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()) {
        NaiveDate::parse_from_str(s, "%Y%m%d")
    } else {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
    }
    .with_context(|| format!("invalid date {raw:?}"))?;
    Ok(d.format("%Y%m%d").to_string())
}

/// Read one multi-contract `baseline_data_1min` day parquet and split into one `SourceDay` per
/// `contract_id`, ordered by `contract_id`.
fn read_multi_contract_day(path: &Path) -> Result<(Arc<Schema>, Vec<SourceDay>)> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    let mut names = Vec::new();
    for field in schema.fields() {
        if field.name() != "contract_id" && field.name() != "ts" {
            ensure!(
                *field.data_type() == DataType::Float64,
                "column {} is not double in {}",
                field.name(),
                path.display()
            );
            names.push(field.name().clone());
        }
    }
    let num_cols = names.len();

    // Map contract_id -> (ts_vec, cols_vec)
    let mut by_contract: BTreeMap<String, (Vec<i64>, Vec<Vec<f64>>)> = BTreeMap::new();

    for batch in reader {
        let batch = batch?;
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }
        let cid_idx = schema
            .index_of("contract_id")
            .context("missing contract_id")?;
        let ts_idx = schema.index_of("ts").context("missing ts")?;
        let cid_arr = batch
            .column(cid_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("contract_id is not Utf8")?;
        let ts_arr = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("ts is not Int64")?;
        ensure!(cid_arr.null_count() == 0, "null contract_id");
        ensure!(ts_arr.null_count() == 0, "null ts");

        let mut float_cols: Vec<&Float64Array> = Vec::with_capacity(num_cols);
        for name in &names {
            let idx = schema.index_of(name)?;
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .with_context(|| format!("{name} is not Float64"))?;
            float_cols.push(arr);
        }

        for r in 0..n {
            let cid = cid_arr.value(r);
            let ts = ts_arr.value(r);
            let entry = by_contract
                .entry(cid.to_string())
                .or_insert_with(|| (Vec::new(), (0..num_cols).map(|_| Vec::new()).collect()));
            entry.0.push(ts);
            for (c, arr) in float_cols.iter().enumerate() {
                entry.1[c].push(if arr.is_null(r) {
                    f64::NAN
                } else {
                    arr.value(r)
                });
            }
        }
    }

    ensure!(!by_contract.is_empty(), "empty day file {}", path.display());
    let mut out = Vec::with_capacity(by_contract.len());
    for (cid, (ts, numeric)) in by_contract {
        out.push(SourceDay {
            schema: schema.clone(),
            contract_id: cid,
            ts,
            numeric,
            numeric_names: names.clone(),
        });
    }
    Ok((schema, out))
}

fn build_multi_contract_kind(
    src_schema: &Arc<Schema>,
    contracts: &[SourceDay],
    kind: Kind,
) -> Result<(Vec<u8>, usize)> {
    let names = &contracts[0].numeric_names;
    let num_cols = names.len();
    let mut all_cids: Vec<&str> = Vec::new();
    let mut all_ts: Vec<i64> = Vec::new();
    let mut all_cols: Vec<Vec<f64>> = (0..num_cols).map(|_| Vec::new()).collect();

    let bid_idx = names.iter().position(|n| n == "bid0p");
    let ask_idx = names.iter().position(|n| n == "ask0p");

    for day in contracts {
        let (starts, ends, _) = group_bounds(&day.ts, kind);
        let bars = aggregate(day, &starts, &ends)
            .with_context(|| format!("aggregate {} {:?}", day.contract_id, kind))?;

        // Relaxed book sanity for contract-level data: illiquid far-month contracts may have null
        // bid0p/ask0p; whenever both are finite, require ask0p >= bid0p.
        if let (Some(bi), Some(ai)) = (bid_idx, ask_idx) {
            for (&b, &a) in bars.numeric[bi].iter().zip(&bars.numeric[ai]) {
                if b.is_finite() && a.is_finite() {
                    ensure!(
                        a >= b,
                        "crossed book in {} {:?}: bid0p={b} > ask0p={a}",
                        day.contract_id,
                        kind
                    );
                }
            }
        }

        let m = bars.ts.len();
        all_cids.extend(std::iter::repeat_n(day.contract_id.as_str(), m));
        all_ts.extend_from_slice(&bars.ts);
        for (c, col) in bars.numeric.into_iter().enumerate() {
            all_cols[c].extend(col);
        }
    }

    let total_rows = all_ts.len();
    ensure!(total_rows > 0, "no output rows for {:?}", kind);

    let out_schema = Arc::new(Schema::new(
        src_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<Field>>(),
    ));

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
    for field in out_schema.fields() {
        match field.name().as_str() {
            "contract_id" => arrays.push(Arc::new(StringArray::from(all_cids.clone()))),
            "ts" => arrays.push(Arc::new(Int64Array::from(all_ts.clone()))),
            name => {
                let c = names
                    .iter()
                    .position(|n| n == name)
                    .context("numeric column")?;
                let values: Float64Array = all_cols[c]
                    .iter()
                    .map(|v| if v.is_nan() { None } else { Some(*v) })
                    .collect();
                arrays.push(Arc::new(values));
            }
        }
    }

    let batch = RecordBatch::try_new(out_schema.clone(), arrays)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(total_rows.max(1))
        .set_created_by("cn-baseline-bars".to_string())
        .build();
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, out_schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    // Verify readable parquet footer & row count before returning.
    let check = SerializedFileReader::new(bytes::Bytes::from(buf.clone()))?;
    ensure!(
        check.metadata().file_metadata().num_rows() as usize == total_rows,
        "footer row count mismatch"
    );
    Ok((buf, total_rows))
}

fn publish_atomic(target: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let tid = format!("{:?}", std::thread::current().id())
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();
    let tmp = target.with_extension(format!("parquet.tmp.{}.{}", std::process::id(), tid));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, target)?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        (1..=8).contains(&args.workers),
        "--workers must be in 1..=8"
    );
    let start_ymd = parse_day_ymd(&args.start)?;
    let end_ymd = parse_day_ymd(&args.end)?;
    if start_ymd > end_ymd {
        bail!("--start ({start_ymd}) must not be after --end ({end_ymd})");
    }

    let ex_filter: Option<Vec<String>> = args
        .exchange
        .map(|v| v.into_iter().map(|s| s.to_ascii_lowercase()).collect());
    let prod_filter: Option<Vec<String>> = args
        .product
        .map(|v| v.into_iter().map(|s| s.to_ascii_uppercase()).collect());

    let in_root = args.data_root.join("baseline_data_1min");
    ensure!(
        in_root.is_dir(),
        "input directory does not exist: {}",
        in_root.display()
    );

    // Discover (exchange, product, day_stem, src_path)
    let mut tasks: Vec<(String, String, String, PathBuf)> = Vec::new();
    for ex_ent in fs::read_dir(&in_root)? {
        let ex_path = ex_ent?.path();
        if !ex_path.is_dir() {
            continue;
        }
        let ex_name = ex_path.file_name().unwrap().to_string_lossy().into_owned();
        if ex_name.starts_with('_') || ex_name.starts_with('.') {
            continue;
        }
        if let Some(ref flt) = ex_filter {
            if !flt.contains(&ex_name) {
                continue;
            }
        }
        for prod_ent in fs::read_dir(&ex_path)? {
            let prod_path = prod_ent?.path();
            if !prod_path.is_dir() {
                continue;
            }
            let prod_name = prod_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned();
            if let Some(ref flt) = prod_filter {
                if !flt.contains(&prod_name) {
                    continue;
                }
            }
            for f_ent in fs::read_dir(&prod_path)? {
                let f_path = f_ent?.path();
                let Some(stem) = f_path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                if f_path.extension().and_then(|s| s.to_str()) != Some("parquet")
                    || stem.len() != 8
                    || !stem.bytes().all(|b| b.is_ascii_digit())
                {
                    continue;
                }
                if stem < start_ymd.as_str() || stem > end_ymd.as_str() {
                    continue;
                }
                tasks.push((ex_name.clone(), prod_name.clone(), stem.to_string(), f_path));
            }
        }
    }
    tasks.sort();
    let total = tasks.len();
    eprintln!(
        "[cn-baseline-bars] {} source 1min files in {}..{} (workers={}, overwrite={})",
        total, start_ymd, end_ymd, args.workers, args.overwrite
    );
    if total == 0 {
        return Ok(());
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build()?;
    let done = AtomicUsize::new(0);
    let written = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);

    pool.install(|| -> Result<()> {
        tasks
            .par_iter()
            .try_for_each(|(ex, prod, day, src)| -> Result<()> {
                let targets: Vec<(Kind, PathBuf)> = Kind::ALL
                    .iter()
                    .map(|&k| {
                        let p = args
                            .data_root
                            .join(dataset_name(k))
                            .join(ex)
                            .join(prod)
                            .join(format!("{day}.parquet"));
                        (k, p)
                    })
                    .collect();

                if !args.overwrite && targets.iter().all(|(_, p)| p.exists()) {
                    skipped.fetch_add(4, Ordering::Relaxed);
                    done.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }

                let (schema, contracts) = read_multi_contract_day(src)?;
                for (kind, target) in targets {
                    if !args.overwrite && target.exists() {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let (bytes, _rows) = build_multi_contract_kind(&schema, &contracts, kind)?;
                    publish_atomic(&target, &bytes)?;
                    written.fetch_add(1, Ordering::Relaxed);
                }

                let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                if n.is_multiple_of(2000) || n == total {
                    eprintln!(
                        "[cn-baseline-bars] ({}/{}) written={} skipped={}",
                        n,
                        total,
                        written.load(Ordering::Relaxed),
                        skipped.load(Ordering::Relaxed)
                    );
                }
                Ok(())
            })
    })?;

    eprintln!(
        "[cn-baseline-bars] ok source_files={} written_files={} skipped_files={}",
        total,
        written.load(Ordering::Relaxed),
        skipped.load(Ordering::Relaxed)
    );
    Ok(())
}
