//! COMEX metals L2 under the Globex RIC family (`1GC…`, `1SI…`, `1HG…`, `1ALI…`).
//!
//! LSEG published COMEX market depth in two families: `comex_futures` (standard RICs such as
//! `GCZ2`) and `comex_globex_futures` (the Globex RICs with a leading `1`, such as `1GCZ2`).
//! 2016–2023 the standard family has no depth (Report counts 0); the Globex family has it.
//! From 2024-07-10 the standard RICs carry depth and the `1` family stops after 2024-09-27.
//! TAS only ever uses the standard RIC, so the `1` files were never linked to their contract.
//!
//! A `1` RIC-year is treated as the same book as the standard RIC only after a check
//! (docs/lseg/l2_alias.md): on sample days its L1 bid/ask must match a TAS quote of the
//! standard RIC within one second (positive control GCZ24 vs its own TAS: 0.727; the alias
//! 1GCZ24: 0.727; a different month: 0.000). Then the alias day files are written again with
//! `ric` set to the standard RIC and `l2_alias_of` in the footer, only for days the standard
//! RIC has no L2 file; nothing existing is replaced.
use anyhow::{bail, ensure, Context, Result};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::DataType;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties},
    format::KeyValue,
};
use std::{fs::File, path::Path, sync::Arc};

/// Products of the Globex family.
pub const ROOTS: [&str; 4] = ["GC", "SI", "HG", "ALI"];
/// A grid point matches when a TAS quote within this window has the same L1 bid and ask.
pub const WINDOW_NS: i64 = 1_000_000_000;
/// Verdict thresholds (median over sample days / maximum over the negative controls).
pub const MIN_MATCH: f64 = 0.5;
pub const MAX_NEGATIVE: f64 = 0.05;
const ROW_GROUP_ROWS: usize = 65_536;

/// `1GCZ2` -> `GCZ2` for the Globex family; `None` for anything else.
pub fn standard_of(alias: &str) -> Option<String> {
    let rest = alias.strip_prefix('1')?;
    let root = ROOTS.iter().find(|r| rest.starts_with(**r))?;
    let tail = &rest[root.len()..];
    let mut c = tail.chars();
    let month = c.next()?;
    let digits = c.as_str();
    ("FGHJKMNQUVXZ".contains(month)
        && (1..=2).contains(&digits.len())
        && digits.bytes().all(|b| b.is_ascii_digit()))
    .then(|| rest.to_string())
}

fn u64s(a: &ArrayRef) -> Result<Vec<i64>> {
    Ok(match a.data_type() {
        DataType::UInt64 => a
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .iter()
            .map(|v| *v as i64)
            .collect(),
        DataType::Int64 => a
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec(),
        t => bail!("unexpected timestamp type {t}"),
    })
}

fn column(b: &RecordBatch, name: &str) -> Result<ArrayRef> {
    Ok(b.column(
        b.schema()
            .index_of(name)
            .with_context(|| format!("no column {name}"))?,
    )
    .clone())
}

/// (ts, scaled bid, scaled ask) of the L1 of an L2 file; rows with an empty side are skipped.
pub fn l2_l1(path: &Path) -> Result<Vec<(i64, i64, i64)>> {
    let r = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;
    let mut out = Vec::new();
    for b in r {
        let b = b?;
        let ts = u64s(&column(&b, "ts_utc_ns")?)?;
        let bid = column(&b, "l1_bid")?;
        let ask = column(&b, "l1_ask")?;
        let bid = bid
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("l1_bid type")?;
        let ask = ask
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("l1_ask type")?;
        for (i, &t) in ts.iter().enumerate() {
            if bid.is_valid(i) && ask.is_valid(i) {
                out.push((
                    t,
                    (bid.value(i) * 1e9).round() as i64,
                    (ask.value(i) * 1e9).round() as i64,
                ));
            }
        }
    }
    Ok(out)
}

/// (ts, bid, ask) of a TAS `quote.parquet` (prices scaled by 1e9), sorted by time.
pub fn tas_quotes(path: &Path) -> Result<Vec<(i64, i64, i64)>> {
    let r = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;
    let mut out = Vec::new();
    for b in r {
        let b = b?;
        let ts = u64s(&column(&b, "ts_utc_ns")?)?;
        let bid = column(&b, "bid")?;
        let ask = column(&b, "ask")?;
        let bid = bid
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("bid type")?;
        let ask = ask
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("ask type")?;
        for (i, &t) in ts.iter().enumerate() {
            if bid.is_valid(i) && ask.is_valid(i) {
                out.push((t, bid.value(i), ask.value(i)));
            }
        }
    }
    out.sort_by_key(|q| q.0);
    Ok(out)
}

/// Share of L2 grid points whose L1 (bid, ask) equals a quote in force within `WINDOW_NS`
/// (the quote in force at `ts - window` and every quote up to `ts + window`).
pub fn match_rate(l2: &[(i64, i64, i64)], quotes: &[(i64, i64, i64)]) -> Option<f64> {
    if l2.is_empty() || quotes.is_empty() {
        return None;
    }
    let mut hits = 0usize;
    for &(ts, b, a) in l2 {
        let lo = quotes
            .partition_point(|q| q.0 < ts - WINDOW_NS)
            .saturating_sub(1);
        let hi = quotes.partition_point(|q| q.0 <= ts + WINDOW_NS);
        if quotes[lo..hi].iter().any(|q| q.1 == b && q.2 == a) {
            hits += 1;
        }
    }
    Some(hits as f64 / l2.len() as f64)
}

/// Writes `src` again as `dst` with every `ric` value set to `standard`; the footer keeps the
/// source key/values and adds `l2_alias_of`. Returns the row count.
pub fn rewrite(src: &Path, dst: &Path, alias: &str, standard: &str) -> Result<u64> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(src)?)?;
    let schema = builder.schema().clone();
    let mut kv: Vec<KeyValue> = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|k| k.key != "ARROW:schema")
        .cloned()
        .collect();
    ensure!(
        !kv.iter().any(|k| k.key == "l2_alias_of"),
        "{} is already an alias copy",
        src.display()
    );
    kv.push(KeyValue::new("l2_alias_of".to_string(), alias.to_string()));
    kv.push(KeyValue::new(
        "l2_alias_rule".to_string(),
        "COMEX Globex RIC (leading 1) of the same contract, checked against the TAS quotes of the standard RIC; see docs/lseg/l2_alias.md".to_string(),
    ));
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(kv))
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(ROW_GROUP_ROWS)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_created_by("lseg_l2_alias".to_string())
        .build();
    let ric_i = schema.index_of("ric")?;
    let mut w = ArrowWriter::try_new(File::create(dst)?, schema.clone(), Some(props))?;
    let mut rows = 0u64;
    for b in builder.build()? {
        let b = b?;
        let old = b
            .column(ric_i)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("ric type")?;
        ensure!(
            (0..old.len()).all(|i| old.value(i) == alias),
            "{}: a row is not {alias}",
            src.display()
        );
        let mut cols = b.columns().to_vec();
        cols[ric_i] = Arc::new(StringArray::from(vec![standard; b.num_rows()]));
        rows += b.num_rows() as u64;
        w.write(&RecordBatch::try_new(schema.clone(), cols)?)?;
    }
    w.close()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_rics() {
        assert_eq!(standard_of("1GCZ2").as_deref(), Some("GCZ2"));
        assert_eq!(standard_of("1ALIZ24").as_deref(), Some("ALIZ24"));
        assert_eq!(standard_of("1HGH26").as_deref(), Some("HGH26"));
        assert_eq!(standard_of("GCZ2"), None);
        assert_eq!(standard_of("1CLZ2"), None);
        assert_eq!(standard_of("1GCZ"), None);
        assert_eq!(standard_of("1GCc1"), None);
    }

    #[test]
    fn match_rate_uses_a_one_second_window() {
        let q = vec![(1_000_000_000, 10, 11), (2_500_000_000, 12, 13)];
        // At 2.0 s the quote in force is (10,11); (12,13) arrives 0.5 s later: both count.
        let l2 = vec![
            (2_000_000_000, 10, 11),
            (2_000_000_000, 12, 13),
            (2_000_000_000, 99, 100),
        ];
        assert_eq!(match_rate(&l2, &q), Some(2.0 / 3.0));
        // Outside the window.
        assert_eq!(match_rate(&[(9_000_000_000, 10, 11)], &q), Some(0.0));
        assert_eq!(match_rate(&[], &q), None);
    }

    #[test]
    fn rewrite_changes_only_the_ric() {
        use arrow_array::{BooleanArray, UInt32Array};
        use arrow_schema::{Field, Schema};
        let t = tempfile::tempdir().unwrap();
        let src = t.path().join("a.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("ric", DataType::Utf8, false),
            Field::new("ts_utc_ns", DataType::UInt64, false),
            Field::new("anchor", DataType::Boolean, false),
            Field::new("l1_bid", DataType::Float64, true),
            Field::new("l1_bid_size", DataType::UInt32, true),
        ]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["1GCZ2", "1GCZ2"])),
                Arc::new(UInt64Array::from(vec![1u64, 2])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
                Arc::new(UInt32Array::from(vec![Some(3u32), None])),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "freq_ms".to_string(),
                "250".to_string(),
            )]))
            .build();
        let mut w = ArrowWriter::try_new(File::create(&src).unwrap(), schema, Some(props)).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
        let dst = t.path().join("b.parquet");
        assert_eq!(rewrite(&src, &dst, "1GCZ2", "GCZ2").unwrap(), 2);
        let r = ParquetRecordBatchReaderBuilder::try_new(File::open(&dst).unwrap()).unwrap();
        let kv: Vec<(String, Option<String>)> = r
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .filter(|k| k.key != "ARROW:schema")
            .map(|k| (k.key.clone(), k.value.clone()))
            .collect();
        assert_eq!(kv[0], ("freq_ms".into(), Some("250".into())));
        assert_eq!(kv[1], ("l2_alias_of".into(), Some("1GCZ2".into())));
        let out = r.build().unwrap().next().unwrap().unwrap();
        let old = b;
        for i in 1..out.num_columns() {
            assert_eq!(out.column(i).as_ref(), old.column(i).as_ref());
        }
        let ric = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!((0..2).all(|i| ric.value(i) == "GCZ2"));
        // A second alias pass is refused.
        assert!(rewrite(&dst, &t.path().join("c.parquet"), "GCZ2", "X").is_err());
    }
}
