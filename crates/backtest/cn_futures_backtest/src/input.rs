//! Batch reader for the user's minute × product capital-allocation matrix.
use anyhow::{ensure, Context, Result};
use arrow_array::{
    Array, Float64Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    ProjectionMask,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputRow {
    pub ts: i64,
    pub values: BTreeMap<String, f64>,
}

pub struct Input {
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    at: usize,
    previous: Option<i64>,
    time: String,
}
impl Input {
    pub fn open(path: &Path, products: &[String]) -> Result<Self> {
        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
        let selected: BTreeSet<_> = products.iter().map(|x| x.to_ascii_uppercase()).collect();
        let names: Vec<_> = b
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        ensure!(
            names.iter().any(|x| x == "TimeStamp"),
            "input requires TimeStamp index"
        );
        for p in &selected {
            ensure!(
                names.iter().any(|n| n.eq_ignore_ascii_case(p)),
                "input has no product {p}"
            );
        }
        let indices: Vec<_> = names
            .iter()
            .enumerate()
            .filter(|(_, n)| {
                n.as_str() == "TimeStamp"
                    || selected.is_empty()
                    || selected.contains(&n.to_ascii_uppercase())
            })
            .map(|(i, _)| i)
            .collect();
        let projection = ProjectionMask::roots(b.parquet_schema(), indices);
        Ok(Self {
            reader: b
                .with_projection(projection)
                .with_batch_size(4096)
                .build()?,
            batch: None,
            at: 0,
            previous: None,
            time: "TimeStamp".into(),
        })
    }
    pub fn read(&mut self) -> Result<Option<InputRow>> {
        if self.batch.as_ref().is_none_or(|b| self.at >= b.num_rows()) {
            self.batch = self.reader.next().transpose()?;
            self.at = 0;
        }
        let Some(b) = &self.batch else {
            return Ok(None);
        };
        let a = b.column_by_name(&self.time).context("TimeStamp missing")?;
        ensure!(!a.is_null(self.at), "null TimeStamp");
        let (raw, div) = if let Some(x) = a.as_any().downcast_ref::<TimestampNanosecondArray>() {
            (x.value(self.at), 1000)
        } else if let Some(x) = a.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            (x.value(self.at), 1)
        } else if let Some(x) = a.as_any().downcast_ref::<TimestampMillisecondArray>() {
            (x.value(self.at) * 1000, 1)
        } else {
            anyhow::bail!("unsupported TimeStamp type {}", a.data_type())
        };
        let has_tz = matches!(a.data_type(), arrow_schema::DataType::Timestamp(_, Some(_)));
        let ts = raw / div - if has_tz { 0 } else { 8 * 3600 * 1_000_000 };
        ensure!(
            self.previous.is_none_or(|p| p < ts),
            "input timestamps must be strictly increasing, ts={ts}"
        );
        let mut values = BTreeMap::new();
        for (f, a) in b.schema().fields().iter().zip(b.columns()) {
            if f.name() == &self.time {
                continue;
            }
            let x = a
                .as_any()
                .downcast_ref::<Float64Array>()
                .with_context(|| format!("{} must be float64", f.name()))?;
            ensure!(
                !x.is_null(self.at) && x.value(self.at).is_finite(),
                "invalid value {} at {ts}",
                f.name()
            );
            values.insert(f.name().to_ascii_uppercase(), x.value(self.at));
        }
        self.previous = Some(ts);
        self.at += 1;
        Ok(Some(InputRow { ts, values }))
    }
}

pub fn inspect(path: &Path) -> Result<serde_json::Value> {
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let names: Vec<_> = b
        .schema()
        .fields()
        .iter()
        .filter(|x| x.name() != "TimeStamp")
        .map(|x| x.name().clone())
        .collect();
    let mut reader = Input::open(path, &[])?;
    let mut count = 0u64;
    let mut first = None;
    let mut last = None;
    let mut min_net = f64::INFINITY;
    let mut max_net = f64::NEG_INFINITY;
    let mut max_gross = 0.0f64;
    let mut gross_sum = 0.0;
    let mut examples = vec![];
    while let Some(r) = reader.read()? {
        first = first.or(Some(r.ts));
        last = Some(r.ts);
        count += 1;
        let net = r.values.values().sum::<f64>();
        let gross = r.values.values().map(|x| x.abs()).sum::<f64>();
        min_net = min_net.min(net);
        max_net = max_net.max(net);
        max_gross = max_gross.max(gross);
        gross_sum += gross;
        if examples.len() < 3 && gross > 0.0 {
            examples.push(r);
        }
    }
    ensure!(count > 0, "empty input");
    Ok(
        serde_json::json!({"path":path,"rows":count,"products":names,"first_timestamp_us":first,"last_timestamp_us":last,"timezone":"Asia/Shanghai","effective_at_label":true,"signed_sum_min":min_net,"signed_sum_max":max_net,"absolute_sum_max":max_gross,"absolute_sum_mean":gross_sum/count as f64,"examples":examples}),
    )
}
