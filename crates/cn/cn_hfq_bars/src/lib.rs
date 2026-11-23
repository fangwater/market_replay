//! CN HFQ coarse bars (5min / 15min / 1h / 1day) from `baseline_data_1min_hfq` day files.
//!
//! Port of the Python builders of 2026-10-14 (5min / 15min) and 2026-10-28 (1h / 1day)
//! (`scripts/cn_hfq/build_cn_hfq_5min.py`, `cn_hfq_coarse_reference.py`). The arithmetic is the same, including
//! NumPy's summation order (`np.add.reduceat` = first element + pairwise sum of the rest), so the values are
//! bit-identical to the existing files. Only the parquet bytes and the omitted pandas metadata differ.
use anyhow::{bail, ensure, Context, Result};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

pub mod contracts;
pub mod run;

/// Columns summed over a bar.
pub const SUM_COLUMNS: [&str; 21] = [
    "volume",
    "amount",
    "count",
    "buy_count",
    "sell_count",
    "buy_amount",
    "sell_amount",
    "buy_volume",
    "sell_volume",
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
];

/// The four maintained bar kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    M5,
    M15,
    H1,
    Day,
}

impl Kind {
    pub const ALL: [Kind; 4] = [Kind::M5, Kind::M15, Kind::H1, Kind::Day];
    pub fn minutes(self) -> usize {
        match self {
            Kind::M5 => 5,
            Kind::M15 => 15,
            Kind::H1 => 60,
            Kind::Day => 1440,
        }
    }
    pub fn dataset(self) -> &'static str {
        match self {
            Kind::M5 => "baseline_data_5min_hfq",
            Kind::M15 => "baseline_data_15min_hfq",
            Kind::H1 => "baseline_data_1h_hfq",
            Kind::Day => "baseline_data_1day_hfq",
        }
    }
    fn contract_key(self) -> &'static str {
        match self {
            Kind::M5 | Kind::M15 => "cn_hfq_bars_contract",
            _ => "cn_hfq_coarse_contract",
        }
    }
    fn contract(self) -> &'static str {
        match self {
            Kind::M5 => contracts::BAR_CONTRACT_5,
            Kind::M15 => contracts::BAR_CONTRACT_15,
            _ => contracts::COARSE_CONTRACT,
        }
    }
}

/// One 1min HFQ trading-day file: key columns plus the numeric columns in file order.
pub struct SourceDay {
    pub schema: Arc<Schema>,
    pub contract_id: String,
    pub ts: Vec<i64>,
    /// Numeric columns (NaN = null), index-aligned with `numeric_names`.
    pub numeric: Vec<Vec<f64>>,
    pub numeric_names: Vec<String>,
}

pub fn read_day(bytes: Vec<u8>) -> Result<SourceDay> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let mut contract = Vec::new();
    let mut ts = Vec::new();
    let mut names = Vec::new();
    let mut cols: Vec<Vec<f64>> = Vec::new();
    for field in schema.fields() {
        if field.name() != "contract_id" && field.name() != "ts" {
            ensure!(
                *field.data_type() == DataType::Float64,
                "column {} is not double",
                field.name()
            );
            names.push(field.name().clone());
            cols.push(Vec::new());
        }
    }
    for batch in reader {
        let batch = batch?;
        for (i, field) in schema.fields().iter().enumerate() {
            let array = batch.column(i);
            match field.name().as_str() {
                "contract_id" => {
                    let a = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .context("contract_id is not a string column")?;
                    for k in 0..a.len() {
                        ensure!(!a.is_null(k), "null contract_id");
                        contract.push(a.value(k).to_string());
                    }
                }
                "ts" => {
                    let a = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .context("ts is not int64")?;
                    ensure!(a.null_count() == 0, "null ts");
                    ts.extend(a.values().iter().copied());
                }
                name => {
                    let a = array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .with_context(|| format!("{name} is not a double column"))?;
                    let c = names
                        .iter()
                        .position(|n| n == name)
                        .expect("numeric column");
                    cols[c].extend((0..a.len()).map(|k| {
                        if a.is_null(k) {
                            f64::NAN
                        } else {
                            a.value(k)
                        }
                    }));
                }
            }
        }
    }
    ensure!(!ts.is_empty(), "empty trading day");
    ensure!(
        contract.iter().all(|c| *c == contract[0]),
        "more than one contract_id in a day file"
    );
    Ok(SourceDay {
        schema,
        contract_id: contract.swap_remove(0),
        ts,
        numeric: cols,
        numeric_names: names,
    })
}

/// NumPy's pairwise summation of `a[lo..lo+n]` (the float64 reduction kernel, block size 128, unrolled by 8).
fn pairwise(a: &[f64], lo: usize, n: usize) -> f64 {
    if n < 8 {
        let mut res = 0.0;
        for x in &a[lo..lo + n] {
            res += x;
        }
        res
    } else if n <= 128 {
        let mut r = [0.0; 8];
        r.copy_from_slice(&a[lo..lo + 8]);
        let mut i = 8;
        while i < n - (n % 8) {
            for (j, rj) in r.iter_mut().enumerate() {
                *rj += a[lo + i + j];
            }
            i += 8;
        }
        let mut res = ((r[0] + r[1]) + (r[2] + r[3])) + ((r[4] + r[5]) + (r[6] + r[7]));
        while i < n {
            res += a[lo + i];
            i += 1;
        }
        res
    } else {
        let mut n2 = n / 2;
        n2 -= n2 % 8;
        pairwise(a, lo, n2) + pairwise(a, lo + n2, n - n2)
    }
}

/// `np.add.reduceat(a, starts)` for the groups `[start, end)`: the first element plus the pairwise sum of the rest.
fn reduceat_add(a: &[f64], starts: &[usize], ends: &[usize]) -> Vec<f64> {
    starts
        .iter()
        .zip(ends)
        .map(|(&s, &e)| {
            if e - s == 1 {
                a[s]
            } else {
                a[s] + pairwise(a, s + 1, e - s - 1)
            }
        })
        .collect()
}

/// `np.maximum.reduceat` / `np.minimum.reduceat` (NaN propagates).
fn reduceat_extreme(a: &[f64], starts: &[usize], ends: &[usize], max: bool) -> Vec<f64> {
    starts
        .iter()
        .zip(ends)
        .map(|(&s, &e)| {
            let mut out = a[s];
            for &b in &a[s + 1..e] {
                let keep = if max { out >= b } else { out <= b } || out.is_nan();
                if !keep {
                    out = b;
                }
            }
            out
        })
        .collect()
}

/// Bars of one day: `ts` (first contributing minute) and the numeric columns, in the order of the source.
pub struct Bars {
    pub ts: Vec<i64>,
    pub numeric: Vec<Vec<f64>>,
    pub source_minutes: Vec<i64>,
    pub end_ts: Vec<i64>,
}

fn column<'a>(day: &'a SourceDay, name: &str) -> Result<&'a Vec<f64>> {
    let i = day
        .numeric_names
        .iter()
        .position(|n| n == name)
        .with_context(|| format!("source column {name} missing"))?;
    Ok(&day.numeric[i])
}

/// Group boundaries of a day: bucket starts (every `minutes` rows inside a continuous source segment, plus every
/// segment start) and the segment start timestamps. `minutes == 1440` is one bar for the whole day.
pub fn group_bounds(ts: &[i64], kind: Kind) -> (Vec<usize>, Vec<usize>, Vec<i64>) {
    let n = ts.len();
    let seg_start: Vec<bool> = (0..n).map(|i| i == 0 || ts[i] - ts[i - 1] != 60).collect();
    let mut starts = Vec::new();
    if kind == Kind::Day {
        starts.push(0);
    } else {
        let mut anchor = 0usize;
        for (i, &segment) in seg_start.iter().enumerate() {
            if segment {
                anchor = i;
            }
            if segment || (i - anchor).is_multiple_of(kind.minutes()) {
                starts.push(i);
            }
        }
    }
    let ends: Vec<usize> = starts.iter().skip(1).copied().chain([n]).collect();
    let seg_ts: Vec<i64> = if kind == Kind::Day {
        vec![ts[0]]
    } else {
        (0..n).filter(|&i| seg_start[i]).map(|i| ts[i]).collect()
    };
    (starts, ends, seg_ts)
}

pub fn aggregate(day: &SourceDay, starts: &[usize], ends: &[usize]) -> Result<Bars> {
    let n = day.ts.len();
    ensure!(
        day.ts.windows(2).all(|w| w[1] > w[0]) && day.ts.iter().all(|t| t % 60 == 0),
        "timestamps must be strictly increasing UTC minute endpoints"
    );
    ensure!(
        !starts.is_empty()
            && starts[0] == 0
            && *ends.last().unwrap() == n
            && starts.iter().zip(ends).all(|(s, e)| e > s)
            && starts[1..] == ends[..ends.len() - 1],
        "groups must partition the source without overlap or omission"
    );
    let groups = starts.len();
    let last: Vec<usize> = ends.iter().map(|e| e - 1).collect();
    let sizes: Vec<f64> = starts
        .iter()
        .zip(ends)
        .map(|(s, e)| (e - s) as f64)
        .collect();
    let mut out: Vec<Vec<f64>> = day
        .numeric
        .iter()
        .map(|c| last.iter().map(|&i| c[i]).collect())
        .collect();
    let index = |name: &str| -> Result<usize> {
        day.numeric_names
            .iter()
            .position(|n| n == name)
            .with_context(|| format!("source column {name} missing"))
    };
    for name in SUM_COLUMNS {
        let i = index(name)?;
        out[i] = reduceat_add(&day.numeric[i], starts, ends);
    }
    let (count, amount) = (out[index("count")?].clone(), out[index("amount")?].clone());
    let avg: Vec<f64> = (0..groups)
        .map(|g| {
            if count[g] > 0.0 {
                amount[g] / count[g]
            } else {
                0.0
            }
        })
        .collect();
    let (buy_amount, sell_amount) = (
        out[index("buy_amount")?].clone(),
        out[index("sell_amount")?].clone(),
    );
    let (buy_volume, sell_volume) = (
        out[index("buy_volume")?].clone(),
        out[index("sell_volume")?].clone(),
    );
    let net_amount: Vec<f64> = (0..groups)
        .map(|g| buy_amount[g] - sell_amount[g])
        .collect();
    let net_volume: Vec<f64> = (0..groups)
        .map(|g| buy_volume[g] - sell_volume[g])
        .collect();
    let net_pct: Vec<f64> = (0..groups)
        .map(|g| {
            let directed = buy_amount[g] + sell_amount[g];
            if directed > 0.0 {
                net_amount[g] / directed
            } else {
                0.0
            }
        })
        .collect();
    let (ia, inb, inv, inp) = (
        index("avg_amount")?,
        index("net_buy_amount")?,
        index("net_buy_volume")?,
        index("net_buy_pct")?,
    );
    out[ia] = avg;
    out[inb] = net_amount;
    out[inv] = net_volume;
    out[inp] = net_pct;
    // Carried empty-minute OHLC must not become the first print / high / low of a bar that contains trades.
    let src_count = column(day, "count")?;
    let active: Vec<bool> = src_count.iter().map(|c| *c > 0.0).collect();
    let (src_open, src_close) = (column(day, "open")?, column(day, "close")?);
    let (mut has_trade, mut first_trade, mut last_trade) =
        (vec![false; groups], vec![n; groups], vec![0usize; groups]);
    for g in 0..groups {
        for (i, &is_trade) in active.iter().enumerate().take(ends[g]).skip(starts[g]) {
            if is_trade {
                if !has_trade[g] {
                    first_trade[g] = i;
                }
                has_trade[g] = true;
                last_trade[g] = i;
            }
        }
    }
    let carried: Vec<f64> = last.iter().map(|&i| src_close[i]).collect();
    let (io, ic) = (index("open")?, index("close")?);
    out[io] = (0..groups)
        .map(|g| {
            if has_trade[g] {
                src_open[first_trade[g].min(n - 1)]
            } else {
                carried[g]
            }
        })
        .collect();
    out[ic] = (0..groups)
        .map(|g| {
            if has_trade[g] {
                src_close[last_trade[g]]
            } else {
                carried[g]
            }
        })
        .collect();
    for (name, max, sentinel) in [
        ("high", true, f64::NEG_INFINITY),
        ("low", false, f64::INFINITY),
    ] {
        let src = column(day, name)?;
        let masked: Vec<f64> = (0..n)
            .map(|i| if active[i] { src[i] } else { sentinel })
            .collect();
        let value = reduceat_extreme(&masked, starts, ends, max);
        let i = index(name)?;
        out[i] = (0..groups)
            .map(|g| if has_trade[g] { value[g] } else { carried[g] })
            .collect();
    }
    for (name, weight_name) in [
        ("vwap", "volume"),
        ("buy_vwap", "buy_volume"),
        ("sell_vwap", "sell_volume"),
    ] {
        let (price, weight) = (column(day, name)?, column(day, weight_name)?);
        let contribution: Vec<f64> = (0..n)
            .map(|i| {
                if weight[i] != 0.0 {
                    price[i] * weight[i]
                } else {
                    0.0
                }
            })
            .collect();
        let numerator = reduceat_add(&contribution, starts, ends);
        let denominator = out[index(weight_name)?].clone();
        out[index(name)?] = (0..groups)
            .map(|g| {
                if denominator[g] > 0.0 {
                    numerator[g] / denominator[g]
                } else {
                    price[last[g]]
                }
            })
            .collect();
    }
    let twap_sum = reduceat_add(column(day, "twap")?, starts, ends);
    let it = index("twap")?;
    out[it] = (0..groups).map(|g| twap_sum[g] / sizes[g]).collect();
    ensure!(
        out.iter().all(|c| c.iter().all(|v| !v.is_infinite())),
        "nonfinite aggregate result"
    );
    Ok(Bars {
        ts: starts.iter().map(|&s| day.ts[s]).collect(),
        numeric: out,
        source_minutes: starts
            .iter()
            .zip(ends)
            .map(|(s, e)| (e - s) as i64)
            .collect(),
        end_ts: last.iter().map(|&i| day.ts[i] + 60).collect(),
    })
}

/// Segment JSON next to the bar file (`_segments/<exchange>/<product>/<day>.json`), key order as the Python tools.
#[derive(Serialize)]
struct MetaBars<'a> {
    bar_minutes: usize,
    ts: &'a [i64],
    segment_start_ts: &'a [i64],
    source_minutes: &'a [i64],
    end_ts: &'a [i64],
    trading_day: i64,
}

#[derive(Serialize)]
struct MetaCoarse<'a> {
    bar_minutes: usize,
    trading_day: i64,
    ts: &'a [i64],
    segment_start_ts: &'a [i64],
    continuous_bars: bool,
    source_minutes: &'a [i64],
    end_ts: &'a [i64],
}

pub struct DayOutput {
    pub parquet: Vec<u8>,
    pub segments_json: Vec<u8>,
    pub bars: usize,
}

/// Builds the bar file and segment JSON of one day for one kind from the parsed 1min HFQ day file.
pub fn build_day(day: &SourceDay, kind: Kind, day_number: i64) -> Result<DayOutput> {
    let (starts, ends, seg_ts) = group_bounds(&day.ts, kind);
    let bars = aggregate(day, &starts, &ends)?;
    // Book sanity as in the Python builders: every bar has a finite bilateral top of book.
    let (bid, ask) = (
        column_of(day, &bars, "bid0p")?,
        column_of(day, &bars, "ask0p")?,
    );
    ensure!(
        bid.iter()
            .zip(ask)
            .all(|(b, a)| b.is_finite() && a.is_finite() && a >= b),
        "invalid bilateral book"
    );
    // Output schema = source schema with the bar contract as the only metadata.
    let metadata: HashMap<String, String> =
        HashMap::from([(kind.contract_key().to_string(), kind.contract().to_string())]);
    let schema = Arc::new(Schema::new_with_metadata(
        day.schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<Field>>(),
        metadata,
    ));
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        match field.name().as_str() {
            "contract_id" => arrays.push(Arc::new(StringArray::from(vec![
                day.contract_id.as_str();
                bars.ts.len()
            ]))),
            "ts" => arrays.push(Arc::new(Int64Array::from(bars.ts.clone()))),
            name => {
                let c = day
                    .numeric_names
                    .iter()
                    .position(|n| n == name)
                    .context("numeric column")?;
                let values: Float64Array = bars.numeric[c]
                    .iter()
                    .map(|v| if v.is_nan() { None } else { Some(*v) })
                    .collect();
                arrays.push(Arc::new(values));
            }
        }
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_created_by("cn_hfq_bars".to_string())
        .build();
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    verify_roundtrip(&buffer, &batch)?;
    let segments_json = match kind {
        Kind::M5 | Kind::M15 => serde_json::to_vec_pretty(&MetaBars {
            bar_minutes: kind.minutes(),
            ts: &bars.ts,
            segment_start_ts: &seg_ts,
            source_minutes: &bars.source_minutes,
            end_ts: &bars.end_ts,
            trading_day: day_number,
        })?,
        _ => serde_json::to_vec_pretty(&MetaCoarse {
            bar_minutes: kind.minutes(),
            trading_day: day_number,
            ts: &bars.ts,
            segment_start_ts: &seg_ts,
            continuous_bars: true,
            source_minutes: &bars.source_minutes,
            end_ts: &bars.end_ts,
        })?,
    };
    let mut json = segments_json;
    json.push(b'\n');
    Ok(DayOutput {
        parquet: buffer,
        segments_json: json,
        bars: bars.ts.len(),
    })
}

fn column_of<'a>(day: &SourceDay, bars: &'a Bars, name: &str) -> Result<&'a Vec<f64>> {
    let i = day
        .numeric_names
        .iter()
        .position(|n| n == name)
        .with_context(|| format!("source column {name} missing"))?;
    Ok(&bars.numeric[i])
}

/// Reads the written bytes back and checks every column equals what was written.
fn verify_roundtrip(bytes: &[u8], expected: &RecordBatch) -> Result<()> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.to_vec()))?;
    let stored = builder.schema().clone();
    let reader = builder.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    ensure!(!batches.is_empty(), "readback has no batch");
    let actual = arrow_select_concat(&batches)?;
    ensure!(
        actual.num_rows() == expected.num_rows() && actual.num_columns() == expected.num_columns(),
        "parquet readback has a different shape"
    );
    for (i, field) in expected.schema().fields().iter().enumerate() {
        ensure!(
            actual.schema().field(i).name() == field.name()
                && actual.schema().field(i).data_type() == field.data_type(),
            "parquet readback column {} differs in name or type",
            field.name()
        );
        ensure!(
            actual.column(i).to_data() == expected.column(i).to_data(),
            "parquet readback column {} differs in values",
            field.name()
        );
    }
    ensure!(
        stored.metadata().get(expected_key(expected))
            == expected.schema().metadata().get(expected_key(expected)),
        "parquet readback lost the bar contract metadata"
    );
    Ok(())
}

fn expected_key(batch: &RecordBatch) -> &String {
    batch
        .schema_ref()
        .metadata()
        .keys()
        .next()
        .expect("contract metadata")
}

fn arrow_select_concat(batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    bail!(
        "unexpected number of row groups in readback: {}",
        batches.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairwise_matches_numpy_on_known_sums() {
        // numpy: np.add.reduceat(np.arange(1, 301, dtype=float) * 0.1, [0]) -> 4515.000000000001
        let a: Vec<f64> = (1..=300).map(|i| i as f64 * 0.1).collect();
        let r = reduceat_add(&a, &[0], &[300]);
        assert_eq!(r[0].to_bits(), 4515.000000000001f64.to_bits());
        // short groups are plain left-to-right sums
        let b = [0.1, 0.2, 0.3, 0.4];
        let r = reduceat_add(&b, &[0, 2], &[2, 4]);
        assert_eq!(r, vec![0.1 + 0.2, 0.3 + 0.4]);
    }

    #[test]
    fn bounds_start_at_every_segment_and_every_n_minutes() {
        // 12 contiguous minutes, a 16 minute gap, then 7 contiguous minutes
        let mut ts: Vec<i64> = (0..12).map(|i| 1_000_000 + i * 60).collect();
        ts.extend((0..7).map(|i| 1_000_000 + 12 * 60 + 15 * 60 + i * 60));
        let (starts, ends, seg) = group_bounds(&ts, Kind::M5);
        assert_eq!(starts, vec![0, 5, 10, 12, 17]);
        assert_eq!(ends, vec![5, 10, 12, 17, 19]);
        assert_eq!(seg, vec![ts[0], ts[12]]);
        let (s, e, seg) = group_bounds(&ts, Kind::Day);
        assert_eq!((s, e), (vec![0], vec![19]));
        assert_eq!(seg, vec![ts[0]]);
    }

    #[test]
    fn extremes_propagate_nan() {
        let a = [1.0, f64::NAN, 3.0, 2.0, 5.0];
        assert!(reduceat_extreme(&a, &[0], &[3], true)[0].is_nan());
        assert_eq!(reduceat_extreme(&a, &[3], &[5], true), vec![5.0]);
        assert_eq!(reduceat_extreme(&a, &[3], &[5], false), vec![2.0]);
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;

    pub const DEPTH: [&str; 20] = [
        "bid0p", "bid0v", "bid1p", "bid1v", "bid2p", "bid2v", "bid3p", "bid3v", "bid4p", "bid4v",
        "ask0p", "ask0v", "ask1p", "ask1v", "ask2p", "ask2v", "ask3p", "ask3v", "ask4p", "ask4v",
    ];
    pub const PRICES: [&str; 9] = [
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "buy_vwap",
        "sell_vwap",
        "twap",
        "mid_price",
    ];

    /// A 1min HFQ day file with the 56 expected columns: `minutes[i]` is the endpoint timestamp of minute i,
    /// every trade minute has count 2 / volume 3 / amount 30 and price 100 + i.
    pub fn day_parquet(product: &str, minutes: &[i64], trade_every: usize) -> Vec<u8> {
        let mut names: Vec<&str> = vec!["contract_id", "ts"];
        names.extend(SUM_COLUMNS);
        names.extend(DEPTH);
        names.extend(PRICES);
        names.extend([
            "avg_amount",
            "net_buy_amount",
            "net_buy_volume",
            "net_buy_pct",
        ]);
        assert_eq!(names.len(), 56);
        let n = minutes.len();
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for name in &names {
            match *name {
                "contract_id" => {
                    fields.push(Field::new(*name, DataType::Utf8, true));
                    arrays.push(Arc::new(StringArray::from(vec![product; n])));
                }
                "ts" => {
                    fields.push(Field::new(*name, DataType::Int64, true));
                    arrays.push(Arc::new(Int64Array::from(minutes.to_vec())));
                }
                other => {
                    fields.push(Field::new(*name, DataType::Float64, true));
                    let trade = |i: usize| trade_every != 0 && i.is_multiple_of(trade_every);
                    let values: Vec<f64> = (0..n)
                        .map(|i| match other {
                            "count" | "buy_count" => f64::from(u8::from(trade(i))) * 2.0,
                            "volume" | "buy_volume" => f64::from(u8::from(trade(i))) * 3.0,
                            "amount" | "buy_amount" => f64::from(u8::from(trade(i))) * 30.0,
                            "bid0p" | "mid_price" | "open" | "close" | "vwap" | "buy_vwap"
                            | "twap" | "high" | "low" => 100.0 + i as f64,
                            "ask0p" => 100.5 + i as f64,
                            _ => 0.0,
                        })
                        .collect();
                    arrays.push(Arc::new(Float64Array::from(values)));
                }
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buffer
    }
}

#[cfg(test)]
mod build_tests {
    use super::fixtures::*;
    use super::*;

    /// 12 contiguous minutes, a 16 minute break (tea break), 7 more minutes.
    fn minutes() -> Vec<i64> {
        let base = 1_787_000_000 - 1_787_000_000 % 60;
        let mut ts: Vec<i64> = (0..12).map(|i| base + i * 60).collect();
        ts.extend((0..7).map(|i| base + (12 + 15 + i) * 60));
        ts
    }

    fn read_back(bytes: &[u8]) -> (RecordBatch, HashMap<String, String>) {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.to_vec())).unwrap();
        let metadata = builder.schema().metadata().clone();
        let batch = builder.build().unwrap().next().unwrap().unwrap();
        (batch, metadata)
    }

    #[test]
    fn five_minute_bars_restart_at_the_break_and_keep_the_short_tail() {
        let day = read_day(day_parquet("RB", &minutes(), 1)).unwrap();
        let out = build_day(&day, Kind::M5, 20260826).unwrap();
        assert_eq!(out.bars, 5); // 5 + 5 + 2 | 5 + 2
        let (batch, metadata) = read_back(&out.parquet);
        assert!(metadata.contains_key("cn_hfq_bars_contract"));
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let minutes = minutes();
        assert_eq!(
            ts.values().to_vec(),
            vec![
                minutes[0],
                minutes[5],
                minutes[10],
                minutes[12],
                minutes[17]
            ]
        );
        let get = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone()
        };
        assert_eq!(
            get("count").values().to_vec(),
            vec![10.0, 10.0, 4.0, 10.0, 4.0]
        );
        assert_eq!(
            get("volume").values().to_vec(),
            vec![15.0, 15.0, 6.0, 15.0, 6.0]
        );
        assert_eq!(get("open").values().to_vec()[..2], [100.0, 105.0]);
        assert_eq!(get("close").values().to_vec()[..2], [104.0, 109.0]);
        assert_eq!(get("high").values().to_vec()[..2], [104.0, 109.0]);
        assert_eq!(get("low").values().to_vec()[..2], [100.0, 105.0]);
        // volume weighted: all minutes trade 3, so the bar vwap is the mean of the minute quotations
        assert_eq!(get("vwap").values().to_vec()[0], 102.0);
        // the book comes from the last minute of the bar
        assert_eq!(get("ask0p").values().to_vec()[0], 104.5);
        let json: serde_json::Value = serde_json::from_slice(&out.segments_json).unwrap();
        assert_eq!(json["source_minutes"], serde_json::json!([5, 5, 2, 5, 2]));
        assert_eq!(
            json["segment_start_ts"],
            serde_json::json!([minutes[0], minutes[12]])
        );
        assert_eq!(json["end_ts"][0], minutes[4] + 60);
        assert_eq!(json["trading_day"], 20260826);
        assert!(out.segments_json.ends_with(b"}\n"));
    }

    #[test]
    fn hour_and_day_bars_and_a_carried_close_in_a_no_trade_bar() {
        // a trade only in the first minute of every 5: the second 5-minute bar trades, so does every bar
        let day = read_day(day_parquet("RB", &minutes(), 5)).unwrap();
        let day_bar = build_day(&day, Kind::Day, 20260826).unwrap();
        assert_eq!(day_bar.bars, 1);
        let (batch, metadata) = read_back(&day_bar.parquet);
        assert!(metadata.contains_key("cn_hfq_coarse_contract"));
        let get = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone()
        };
        assert_eq!(get("count").value(0), 8.0); // minutes 0, 5, 10, 15
        let json: serde_json::Value = serde_json::from_slice(&day_bar.segments_json).unwrap();
        assert_eq!(json["continuous_bars"], true);
        assert_eq!(json["source_minutes"], serde_json::json!([19]));
        let hour = build_day(&day, Kind::H1, 20260826).unwrap();
        assert_eq!(hour.bars, 2); // one bar per source segment (12 and 7 minutes)
                                  // a bar without any trade keeps the source close of its last minute and has volume 0
        let no_trade = read_day(day_parquet("RB", &minutes()[1..4], 0)).unwrap();
        let out = build_day(&no_trade, Kind::M5, 20260826).unwrap();
        let (batch, _) = read_back(&out.parquet);
        let get = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone()
        };
        assert_eq!(get("volume").value(0), 0.0);
        assert_eq!(get("close").value(0), 102.0);
    }

    #[test]
    fn invalid_input_is_refused() {
        let mut ts = minutes();
        ts[3] = ts[2]; // not strictly increasing
        let day = read_day(day_parquet("RB", &ts, 1)).unwrap();
        assert!(build_day(&day, Kind::M5, 1).is_err());
        let mut odd = minutes();
        odd[0] += 7; // not a minute endpoint
        let day = read_day(day_parquet("RB", &odd, 1)).unwrap();
        assert!(build_day(&day, Kind::M5, 1).is_err());
    }
}
