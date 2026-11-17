//! Bounded, streaming Parquet audit tables. All timestamps are UTC microseconds.
use anyhow::Result;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};

pub const TABLES: &[&str] = &[
    "target_lots",
    "orders",
    "fills",
    "positions",
    "equity",
    "execution",
    "cost_estimate",
    "roll_events",
    "roll_exposure",
    "roll_batches",
];

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Row {
    pub timestamp_us: i64,
    pub kind: String,
    pub product: String,
    pub contract: String,
    pub order_id: Option<u64>,
    pub side: Option<i64>,
    pub lots: Option<i64>,
    pub target_lots: Option<i64>,
    pub actual_lots: Option<i64>,
    pub price: Option<f64>,
    pub reference_price: Option<f64>,
    pub multiplier: Option<f64>,
    pub fee_cny: Option<f64>,
    pub pnl_cny: Option<f64>,
    pub shortfall_cny: Option<f64>,
    pub exposure_cny: Option<f64>,
    pub target_cny: Option<f64>,
    pub value: Option<f64>,
    pub input_ts_us: Option<i64>,
    pub reference_ts_us: Option<i64>,
    pub source_cursor: Option<u64>,
    pub reason: String,
}
pub trait Sink {
    fn emit(&mut self, table: &str, row: Row) -> Result<()>;
}

fn schema() -> Arc<Schema> {
    let mut fields = vec![Field::new("timestamp_us", DataType::Int64, false)];
    for n in ["kind", "product", "contract"] {
        fields.push(Field::new(n, DataType::Utf8, false));
    }
    fields.push(Field::new("order_id", DataType::UInt64, true));
    for n in ["side", "lots", "target_lots", "actual_lots"] {
        fields.push(Field::new(n, DataType::Int64, true));
    }
    for n in [
        "price",
        "reference_price",
        "multiplier",
        "fee_cny",
        "pnl_cny",
        "shortfall_cny",
        "exposure_cny",
        "target_cny",
        "value",
    ] {
        fields.push(Field::new(n, DataType::Float64, true));
    }
    for n in ["input_ts_us", "reference_ts_us"] {
        fields.push(Field::new(n, DataType::Int64, true));
    }
    fields.push(Field::new("source_cursor", DataType::UInt64, true));
    fields.push(Field::new("reason", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}
struct Table {
    writer: ArrowWriter<File>,
    rows: Vec<Row>,
    count: u64,
}
pub struct Outputs {
    tables: BTreeMap<String, Table>,
    schema: Arc<Schema>,
}
impl Outputs {
    pub fn new(dir: &Path) -> Result<Self> {
        let schema = schema();
        let mut tables = BTreeMap::new();
        for n in TABLES {
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .set_max_row_group_size(8192)
                .build();
            let writer = ArrowWriter::try_new(
                File::create(dir.join(format!("{n}.parquet")))?,
                schema.clone(),
                Some(props),
            )?;
            tables.insert(
                n.to_string(),
                Table {
                    writer,
                    rows: Vec::with_capacity(1024),
                    count: 0,
                },
            );
        }
        Ok(Self { tables, schema })
    }
    fn flush(table: &mut Table, schema: Arc<Schema>) -> Result<()> {
        if table.rows.is_empty() {
            return Ok(());
        }
        let r = &table.rows;
        macro_rules! int {
            ($f:ident) => {
                Arc::new(Int64Array::from(r.iter().map(|x| x.$f).collect::<Vec<_>>())) as ArrayRef
            };
        }
        macro_rules! float {
            ($f:ident) => {
                Arc::new(Float64Array::from(
                    r.iter().map(|x| x.$f).collect::<Vec<_>>(),
                )) as ArrayRef
            };
        }
        macro_rules! string {
            ($f:ident) => {
                Arc::new(StringArray::from(
                    r.iter().map(|x| x.$f.as_str()).collect::<Vec<_>>(),
                )) as ArrayRef
            };
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(
                r.iter().map(|x| x.timestamp_us).collect::<Vec<_>>(),
            )),
            string!(kind),
            string!(product),
            string!(contract),
            Arc::new(UInt64Array::from(
                r.iter().map(|x| x.order_id).collect::<Vec<_>>(),
            )),
            int!(side),
            int!(lots),
            int!(target_lots),
            int!(actual_lots),
            float!(price),
            float!(reference_price),
            float!(multiplier),
            float!(fee_cny),
            float!(pnl_cny),
            float!(shortfall_cny),
            float!(exposure_cny),
            float!(target_cny),
            float!(value),
            int!(input_ts_us),
            int!(reference_ts_us),
            Arc::new(UInt64Array::from(
                r.iter().map(|x| x.source_cursor).collect::<Vec<_>>(),
            )),
            string!(reason),
        ];
        table.writer.write(&RecordBatch::try_new(schema, arrays)?)?;
        table.rows.clear();
        Ok(())
    }
    pub fn finish(mut self) -> Result<BTreeMap<String, u64>> {
        let mut counts = BTreeMap::new();
        for (name, mut t) in std::mem::take(&mut self.tables) {
            Self::flush(&mut t, self.schema.clone())?;
            t.writer.close()?;
            counts.insert(name, t.count);
        }
        Ok(counts)
    }
}
impl Sink for Outputs {
    fn emit(&mut self, table: &str, row: Row) -> Result<()> {
        let t = self
            .tables
            .get_mut(table)
            .ok_or_else(|| anyhow::anyhow!("unknown table {table}"))?;
        t.rows.push(row);
        t.count += 1;
        if t.rows.len() >= 1024 {
            Self::flush(t, self.schema.clone())?;
        }
        Ok(())
    }
}
#[derive(Default)]
pub struct MemorySink(pub Vec<(String, Row)>);
impl Sink for MemorySink {
    fn emit(&mut self, table: &str, row: Row) -> Result<()> {
        self.0.push((table.into(), row));
        Ok(())
    }
}
