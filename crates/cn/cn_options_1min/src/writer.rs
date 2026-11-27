use crate::parse::OptionBarRow;
use anyhow::{bail, Context, Result};
use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

/// Exact 9-column schema matching `baseline_data_1min` naming and types.
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("contract_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("open_interest", DataType::Float64, false),
    ]))
}

/// Atomically write one `{exchange}/{PRODUCT}/{YYYYMMDD}.parquet` file:
/// 1. Write single-row-group ZSTD parquet to a sibling `.tmp` file.
/// 2. Sync to disk and verify parquet footer row count.
/// 3. Rename over `target`.
pub fn write_day_parquet(target: &Path, rows: &[OptionBarRow]) -> Result<()> {
    if rows.is_empty() {
        bail!("refusing to write empty parquet {}", target.display());
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create directory {}", parent.display()))?;
    }

    let n = rows.len();
    let mut cids = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    let mut opens = Vec::with_capacity(n);
    let mut highs = Vec::with_capacity(n);
    let mut lows = Vec::with_capacity(n);
    let mut closes = Vec::with_capacity(n);
    let mut vols = Vec::with_capacity(n);
    let mut amts = Vec::with_capacity(n);
    let mut ois = Vec::with_capacity(n);

    for r in rows {
        cids.push(r.contract_id.as_str());
        tss.push(r.ts);
        opens.push(r.open);
        highs.push(r.high);
        lows.push(r.low);
        closes.push(r.close);
        vols.push(r.volume);
        amts.push(r.amount);
        ois.push(r.open_interest);
    }

    let sch = schema();
    let batch = RecordBatch::try_new(
        sch.clone(),
        vec![
            Arc::new(StringArray::from(cids)),
            Arc::new(Int64Array::from(tss)),
            Arc::new(Float64Array::from(opens)),
            Arc::new(Float64Array::from(highs)),
            Arc::new(Float64Array::from(lows)),
            Arc::new(Float64Array::from(closes)),
            Arc::new(Float64Array::from(vols)),
            Arc::new(Float64Array::from(amts)),
            Arc::new(Float64Array::from(ois)),
        ],
    )?;

    let tid = format!("{:?}", std::thread::current().id())
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>();
    let tmp = target.with_extension(format!("parquet.tmp.{}.{}", std::process::id(), tid));

    let write_res = (|| -> Result<()> {
        let file =
            File::create(&tmp).with_context(|| format!("create temp parquet {}", tmp.display()))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_size(n.max(1))
            .build();
        let mut writer = ArrowWriter::try_new(file, sch, Some(props))?;
        writer.write(&batch)?;
        let file = writer.into_inner()?;
        file.sync_all()
            .with_context(|| format!("fsync {}", tmp.display()))?;

        // Verify footer before atomic publish.
        let check = SerializedFileReader::new(File::open(&tmp)?)
            .with_context(|| format!("verify footer {}", tmp.display()))?;
        let meta = check.metadata().file_metadata();
        if meta.num_rows() as usize != n {
            bail!(
                "parquet footer row count mismatch for {}: expected {}, got {}",
                tmp.display(),
                n,
                meta.num_rows()
            );
        }
        Ok(())
    })();

    if let Err(err) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(err);
    }

    fs::rename(&tmp, target)
        .with_context(|| format!("rename {} -> {}", tmp.display(), target.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_and_verify_parquet() {
        let dir = tempdir().unwrap();
        let out = dir.path().join("xdce/M/20260803.parquet");
        let rows = vec![OptionBarRow {
            contract_id: "m2609-C-3000".to_string(),
            ts: 1785502800,
            open: 120.0,
            high: 125.0,
            low: 118.0,
            close: 122.0,
            volume: 10.0,
            amount: 12100.0,
            open_interest: 350.0,
            seq: 0,
        }];
        write_day_parquet(&out, &rows).unwrap();
        let reader = SerializedFileReader::new(File::open(&out).unwrap()).unwrap();
        let meta = reader.metadata().file_metadata();
        assert_eq!(meta.num_rows(), 1);
        assert_eq!(meta.schema_descr().num_columns(), 9);
    }
}
