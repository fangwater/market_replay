//! Port of `cn_futures.roll_replay.run_roll_replay(..., eod_input=...)` and the
//! `cn_futures/io.py` helpers it uses.

use crate::adjustment::{
    audit_continuity, build_continuous_prices, compute_adjustment_factors, AdjustmentFactor,
    ContinuityStatus, ContinuousPrice,
};
use crate::dominant::{select_dominants, ContractDay, DominantDay, Rule};
use crate::instrument::{canonicalize_instrument_id, is_maintained_product};
use crate::pyfmt::{py_json_str, py_opt_float, PyCsvWriter};
use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub const EXCHANGES: &[&str] = &["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"];
/// `cn_futures.stitch.DEFAULT_OVERLAP_CUT`.
pub const DEFAULT_OVERLAP_CUT: &str = "2025-11-03";

const REQUIRED_COLUMNS: &[&str] = &["trad_day", "instrument_id", "open_int", "volume", "close"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollReplaySummary {
    pub exchange: String,
    pub n_files: usize,
    pub n_products: usize,
    pub n_rolls: i64,
    pub n_continuous_products: usize,
    pub summary_path: PathBuf,
}

fn digits_between(text: &str, min: usize, max: usize) -> Option<u32> {
    (text.len() >= min && text.len() <= max && text.bytes().all(|b| b.is_ascii_digit()))
        .then(|| text.parse().ok())
        .flatten()
}

/// `datetime.strptime(text.strip(), fmt)` for `%Y-%m-%d` then `%Y%m%d`.
pub fn parse_py_date(value: &str) -> Result<NaiveDate> {
    let text = value.trim();
    let dashed = || -> Option<NaiveDate> {
        let mut parts = text.split('-');
        let (y, m, d) = (parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() {
            return None;
        }
        let year = digits_between(y, 4, 4)?;
        NaiveDate::from_ymd_opt(
            year as i32,
            digits_between(m, 1, 2)?,
            digits_between(d, 1, 2)?,
        )
    };
    let compact = || -> Option<NaiveDate> {
        if text.len() != 8 {
            return None;
        }
        NaiveDate::from_ymd_opt(
            digits_between(&text[..4], 4, 4)? as i32,
            digits_between(&text[4..6], 2, 2)?,
            digits_between(&text[6..], 2, 2)?,
        )
    };
    dashed()
        .or_else(compact)
        .with_context(|| format!("unrecognized date: {value:?}"))
}

/// `io._parse_float`: blank / nan / none / null are `None`, else `float(text)`.
pub fn parse_py_float(value: &str) -> Result<Option<f64>> {
    let text = value.trim();
    if text.is_empty() || matches!(text.to_lowercase().as_str(), "nan" | "none" | "null") {
        return Ok(None);
    }
    match text.parse::<f64>() {
        Ok(number) => Ok(Some(number)),
        // Python also accepts PEP 515 digit separators.
        Err(_) if text.contains('_') => text
            .replace('_', "")
            .parse::<f64>()
            .map(Some)
            .with_context(|| format!("could not convert string to float: {text:?}")),
        Err(_) => bail!("could not convert string to float: {text:?}"),
    }
}

fn parse_py_bool(value: &str) -> bool {
    matches!(
        value.trim().to_lowercase().as_str(),
        "1" | "true" | "yes" | "y"
    )
}

/// `io.load_daily_cross_section` (then `canonicalize_contract_days`).
pub fn load_daily_cross_section(path: &Path) -> Result<Vec<ContractDay>> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("open {}", path.display()))?;
    let headers = reader.headers()?.clone();
    // DictReader: a duplicated header name maps to its last column.
    let column = |name: &str| {
        headers
            .iter()
            .enumerate()
            .filter(|(_, header)| *header == name)
            .map(|(index, _)| index)
            .last()
    };
    let missing: Vec<&str> = REQUIRED_COLUMNS
        .iter()
        .copied()
        .filter(|name| column(name).is_none())
        .collect();
    if !missing.is_empty() {
        bail!("{} missing columns: {missing:?}", path.display());
    }
    let trad_day_col = column("trad_day").unwrap();
    let instrument_col = column("instrument_id").unwrap();
    let open_int_col = column("open_int").unwrap();
    let volume_col = column("volume").unwrap();
    let close_col = column("close").unwrap();
    let product_col = column("product_id");
    let expired_col = column("expired");

    // canonicalize_contract_days: the last row per (TradDay, canonical id) wins.
    let mut latest: BTreeMap<(NaiveDate, String), ContractDay> = BTreeMap::new();
    for (index, record) in reader.records().enumerate() {
        let record =
            record.with_context(|| format!("read {} row {}", path.display(), index + 2))?;
        let get = |col: Option<usize>| col.and_then(|col| record.get(col));
        let context = || format!("{} row {}", path.display(), index + 2);
        let trad_day =
            parse_py_date(get(Some(trad_day_col)).with_context(context)?).with_context(context)?;
        let instrument = get(Some(instrument_col)).with_context(context)?.trim();
        let instrument_id = canonicalize_instrument_id(instrument, trad_day);
        let row = ContractDay {
            trad_day,
            instrument_id: instrument_id.clone(),
            open_int: parse_py_float(get(Some(open_int_col)).unwrap_or(""))
                .with_context(context)?,
            volume: parse_py_float(get(Some(volume_col)).unwrap_or("")).with_context(context)?,
            close: parse_py_float(get(Some(close_col)).unwrap_or("")).with_context(context)?,
            product_id: get(product_col).unwrap_or("").trim().to_owned(),
            expired: parse_py_bool(get(expired_col).unwrap_or("")),
        };
        latest.insert((trad_day, instrument_id), row);
    }
    Ok(latest.into_values().collect())
}

fn iso(day: NaiveDate) -> String {
    day.format("%Y-%m-%d").to_string()
}

fn iso_opt(day: Option<NaiveDate>) -> String {
    day.map(iso).unwrap_or_default()
}

fn bool_int(value: bool) -> &'static str {
    if value {
        "1"
    } else {
        "0"
    }
}

fn write_table<F>(path: &Path, header: &[&str], mut body: F) -> Result<()>
where
    F: FnMut(&mut PyCsvWriter<BufWriter<File>>) -> std::io::Result<()>,
{
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = PyCsvWriter::new(BufWriter::new(file));
    writer.write_row(header)?;
    body(&mut writer)?;
    writer.into_inner().flush()?;
    Ok(())
}

fn write_eod(path: &Path, rows: &[ContractDay]) -> Result<()> {
    write_table(
        path,
        &[
            "trad_day",
            "product_id",
            "instrument_id",
            "open_int",
            "volume",
            "close",
            "expired",
        ],
        |writer| {
            for row in rows {
                writer.write_row(&[
                    iso(row.trad_day),
                    row.product_id.clone(),
                    row.instrument_id.clone(),
                    py_opt_float(row.open_int),
                    py_opt_float(row.volume),
                    py_opt_float(row.close),
                    bool_int(row.expired).to_owned(),
                ])?;
            }
            Ok(())
        },
    )
}

fn write_dominants(path: &Path, days: &[DominantDay]) -> Result<()> {
    write_table(
        path,
        &[
            "trad_day",
            "product_id",
            "instrument_id",
            "roll_flag",
            "decision_open_int",
            "scheduled_next",
            "rule",
        ],
        |writer| {
            for day in days {
                writer.write_row(&[
                    iso(day.trad_day),
                    day.product_id.clone(),
                    day.instrument_id.clone().unwrap_or_default(),
                    day.roll_flag.to_string(),
                    py_opt_float(day.decision_open_int),
                    day.scheduled_next.clone().unwrap_or_default(),
                    day.rule.name().to_owned(),
                ])?;
            }
            Ok(())
        },
    )
}

fn write_factors(path: &Path, factors: &[AdjustmentFactor]) -> Result<()> {
    write_table(
        path,
        &[
            "product_id",
            "effective_trading_day",
            "from_instrument",
            "to_instrument",
            "reference_trading_day",
            "old_reference_price",
            "new_reference_price",
            "adjustment_method",
            "adjustment_value",
            "cumulative_factor",
            "skipped",
            "skip_reason",
        ],
        |writer| {
            for factor in factors {
                writer.write_row(&[
                    factor.product_id.clone(),
                    iso(factor.effective_trading_day),
                    factor.from_instrument.clone(),
                    factor.to_instrument.clone(),
                    iso(factor.reference_trading_day),
                    py_opt_float(factor.old_reference_price),
                    py_opt_float(factor.new_reference_price),
                    factor.adjustment_method.to_owned(),
                    py_opt_float(factor.adjustment_value),
                    py_opt_float(factor.cumulative_factor),
                    bool_int(factor.skipped).to_owned(),
                    factor.skip_reason.to_owned(),
                ])?;
            }
            Ok(())
        },
    )
}

fn write_prices(path: &Path, prices: &[ContinuousPrice]) -> Result<()> {
    write_table(
        path,
        &[
            "trading_day",
            "product_id",
            "instrument_id",
            "raw_close",
            "price_adjustment",
            "adjusted_close",
            "adjustment_complete",
        ],
        |writer| {
            for price in prices {
                writer.write_row(&[
                    iso(price.trading_day),
                    price.product_id.clone(),
                    price.instrument_id.clone(),
                    py_opt_float(price.raw_close),
                    py_opt_float(price.price_adjustment),
                    py_opt_float(price.adjusted_close),
                    bool_int(price.adjustment_complete).to_owned(),
                ])?;
            }
            Ok(())
        },
    )
}

fn write_continuity(path: &Path, statuses: &[ContinuityStatus]) -> Result<()> {
    write_table(
        path,
        &[
            "product_id",
            "first_trading_day",
            "last_trading_day",
            "first_assigned_trading_day",
            "observed_days",
            "assigned_days",
            "unassigned_prefix_days",
            "unassigned_after_start_days",
            "rolls",
            "adjustment_rows",
            "skipped_adjustments",
            "missing_dominant_closes",
            "mapping_continuous",
            "price_continuous",
            "continuous",
            "first_failure_day",
            "failure_reason",
        ],
        |writer| {
            for row in statuses {
                writer.write_row(&[
                    row.product_id.clone(),
                    iso_opt(row.first_trading_day),
                    iso_opt(row.last_trading_day),
                    iso_opt(row.first_assigned_trading_day),
                    row.observed_days.to_string(),
                    row.assigned_days.to_string(),
                    row.unassigned_prefix_days.to_string(),
                    row.unassigned_after_start_days.to_string(),
                    row.rolls.to_string(),
                    row.adjustment_rows.to_string(),
                    row.skipped_adjustments.to_string(),
                    row.missing_dominant_closes.to_string(),
                    bool_int(row.mapping_continuous).to_owned(),
                    bool_int(row.price_continuous).to_owned(),
                    bool_int(row.continuous).to_owned(),
                    iso_opt(row.first_failure_day),
                    row.failure_reason.to_owned(),
                ])?;
            }
            Ok(())
        },
    )
}

/// JSON scalar used in the summary.
enum Json {
    Str(String),
    Int(i64),
    Null,
    Failed(Vec<[(&'static str, Json); 3]>),
}

impl Json {
    fn render(&self, out: &mut String) {
        match self {
            Json::Str(value) => out.push_str(&py_json_str(value)),
            Json::Int(value) => out.push_str(&value.to_string()),
            Json::Null => out.push_str("null"),
            Json::Failed(items) if items.is_empty() => out.push_str("[]"),
            Json::Failed(items) => {
                out.push_str("[\n");
                for (index, item) in items.iter().enumerate() {
                    out.push_str("    {\n");
                    for (key_index, (key, value)) in item.iter().enumerate() {
                        out.push_str("      ");
                        out.push_str(&py_json_str(key));
                        out.push_str(": ");
                        value.render(out);
                        out.push_str(if key_index + 1 < item.len() {
                            ",\n"
                        } else {
                            "\n"
                        });
                    }
                    out.push_str("    }");
                    out.push_str(if index + 1 < items.len() { ",\n" } else { "\n" });
                }
                out.push_str("  ]");
            }
        }
    }
}

/// `json.dumps(summary, indent=2, sort_keys=True) + "\n"`.
fn render_summary(mut fields: Vec<(&'static str, Json)>) -> String {
    fields.sort_by(|a, b| a.0.cmp(b.0));
    let mut out = String::from("{\n");
    for (index, (key, value)) in fields.iter().enumerate() {
        out.push_str("  ");
        out.push_str(&py_json_str(key));
        out.push_str(": ");
        value.render(&mut out);
        out.push_str(if index + 1 < fields.len() {
            ",\n"
        } else {
            "\n"
        });
    }
    out.push_str("}\n");
    out
}

pub struct RollReplayArgs<'a> {
    pub eod_input: &'a Path,
    pub output_dir: &'a Path,
    pub exchange: &'a str,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub rule: Rule,
    pub overlap_cut: Option<NaiveDate>,
}

/// Create EOD, dominant, factor, adjusted-price and audit tables for one exchange.
pub fn run_roll_replay(args: &RollReplayArgs<'_>) -> Result<RollReplaySummary> {
    if args.end < args.start {
        bail!("end date precedes start date");
    }
    let exchange = args.exchange.trim().to_lowercase();
    if !EXCHANGES.contains(&exchange.as_str()) {
        bail!("unsupported exchange: {exchange}");
    }
    let rows: Vec<ContractDay> = load_daily_cross_section(args.eod_input)?
        .into_iter()
        .filter(|row| args.start <= row.trad_day && row.trad_day <= args.end)
        .collect();
    let n_files = 1usize;
    let n_source_days = rows
        .iter()
        .map(|row| row.trad_day)
        .collect::<BTreeSet<_>>()
        .len();
    let rows: Vec<ContractDay> = rows
        .into_iter()
        .filter(|row| is_maintained_product(&row.product_id))
        .collect();
    if rows.is_empty() {
        bail!(
            "no future main-quote rows for {exchange} in {}..{}",
            args.start,
            args.end
        );
    }

    // Rows are globally sorted by (trad_day, instrument_id), so each product
    // bucket stays sorted the same way.
    let mut by_product: BTreeMap<&str, Vec<ContractDay>> = BTreeMap::new();
    for row in &rows {
        by_product
            .entry(row.product_id.as_str())
            .or_default()
            .push(row.clone());
    }
    let mut dominants: Vec<DominantDay> = Vec::new();
    let mut factors: Vec<AdjustmentFactor> = Vec::new();
    let mut prices: Vec<ContinuousPrice> = Vec::new();
    let mut statuses: Vec<ContinuityStatus> = Vec::new();
    for (product_id, product_rows) in &by_product {
        let product_dominants = select_dominants(product_rows, product_id, args.rule);
        let product_factors =
            compute_adjustment_factors(product_rows, &product_dominants, product_id);
        let product_prices = build_continuous_prices(
            product_rows,
            &product_dominants,
            &product_factors,
            product_id,
        );
        statuses.push(audit_continuity(
            &product_dominants,
            &product_factors,
            &product_prices,
            product_id,
        ));
        dominants.extend(product_dominants);
        factors.extend(product_factors);
        prices.extend(product_prices);
    }

    fs::create_dir_all(args.output_dir)
        .with_context(|| format!("create {}", args.output_dir.display()))?;
    let output_dir = args.output_dir.canonicalize()?;
    write_eod(&output_dir.join(format!("{exchange}_eod.csv")), &rows)?;
    write_dominants(
        &output_dir.join(format!("{exchange}_dominant.csv")),
        &dominants,
    )?;
    write_factors(
        &output_dir.join(format!("{exchange}_adjustment_factor.csv")),
        &factors,
    )?;
    write_prices(
        &output_dir.join(format!("{exchange}_continuous_price.csv")),
        &prices,
    )?;
    write_continuity(
        &output_dir.join(format!("{exchange}_continuity.csv")),
        &statuses,
    )?;

    let failed: Vec<&ContinuityStatus> = statuses.iter().filter(|s| !s.continuous).collect();
    let n_rolls: i64 = dominants.iter().map(|day| day.roll_flag).sum();
    let n_continuous_products = statuses.len() - failed.len();
    let failed_json = failed
        .iter()
        .map(|status| {
            [
                (
                    "failure_reason",
                    Json::Str(status.failure_reason.to_owned()),
                ),
                (
                    "first_failure_day",
                    status
                        .first_failure_day
                        .map_or(Json::Null, |day| Json::Str(iso(day))),
                ),
                ("product_id", Json::Str(status.product_id.clone())),
            ]
        })
        .collect();
    let count = |value: usize| Json::Int(value as i64);
    let summary = render_summary(vec![
        ("exchange", Json::Str(exchange.clone())),
        ("start", Json::Str(iso(args.start))),
        ("end", Json::Str(iso(args.end))),
        ("rule", Json::Str(args.rule.name().to_owned())),
        ("n_files", count(n_files)),
        ("n_source_days", count(n_source_days)),
        ("n_eod_rows", count(rows.len())),
        ("n_products", count(statuses.len())),
        ("n_dominant_rows", count(dominants.len())),
        ("n_rolls", Json::Int(n_rolls)),
        ("n_adjustment_rows", count(factors.len())),
        (
            "n_skipped_adjustments",
            count(factors.iter().filter(|factor| factor.skipped).count()),
        ),
        ("n_continuous_rows", count(prices.len())),
        ("n_continuous_products", count(n_continuous_products)),
        ("failed_products", Json::Failed(failed_json)),
        (
            "overlap_cut",
            args.overlap_cut
                .map_or(Json::Null, |day| Json::Str(iso(day))),
        ),
    ]);
    let summary_path = output_dir.join(format!("{exchange}_roll_replay_summary.json"));
    fs::write(&summary_path, summary)
        .with_context(|| format!("write {}", summary_path.display()))?;
    Ok(RollReplaySummary {
        exchange,
        n_files,
        n_products: statuses.len(),
        n_rolls,
        n_continuous_products,
        summary_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_parse_like_strptime() {
        let day = NaiveDate::from_ymd_opt(2019, 1, 2).unwrap();
        assert_eq!(parse_py_date("2019-01-02").unwrap(), day);
        assert_eq!(parse_py_date(" 2019-1-2 ").unwrap(), day);
        assert_eq!(parse_py_date("20190102").unwrap(), day);
        assert!(parse_py_date("2019-02-30").is_err());
        assert!(parse_py_date("19-01-02").is_err());
        assert!(parse_py_date("2019/01/02").is_err());
    }

    #[test]
    fn floats_parse_like_python() {
        assert_eq!(parse_py_float("11245.000").unwrap(), Some(11245.0));
        assert_eq!(parse_py_float(" 282 ").unwrap(), Some(282.0));
        assert_eq!(parse_py_float("").unwrap(), None);
        assert_eq!(parse_py_float("NaN").unwrap(), None);
        assert_eq!(parse_py_float("None").unwrap(), None);
        assert_eq!(parse_py_float("1_000").unwrap(), Some(1000.0));
        assert!(parse_py_float("abc").is_err());
    }

    #[test]
    fn summary_json_matches_python_layout() {
        let text = render_summary(vec![
            ("start", Json::Str("2019-01-01".into())),
            ("n_files", Json::Int(1)),
            (
                "failed_products",
                Json::Failed(vec![[
                    ("failure_reason", Json::Str("missing_dominant_close".into())),
                    ("first_failure_day", Json::Null),
                    ("product_id", Json::Str("AP".into())),
                ]]),
            ),
            ("overlap_cut", Json::Null),
        ]);
        assert_eq!(
            text,
            "{\n  \"failed_products\": [\n    {\n      \"failure_reason\": \"missing_dominant_close\",\n      \"first_failure_day\": null,\n      \"product_id\": \"AP\"\n    }\n  ],\n  \"n_files\": 1,\n  \"overlap_cut\": null,\n  \"start\": \"2019-01-01\"\n}\n"
        );
        assert_eq!(
            render_summary(vec![("failed_products", Json::Failed(vec![]))]),
            "{\n  \"failed_products\": []\n}\n"
        );
    }

    #[test]
    fn eod_load_canonicalizes_dedups_and_writes_python_floats() {
        let dir = std::env::temp_dir().join(format!("cn_roll_replay_test_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let input = dir.join("in.csv");
        fs::write(
            &input,
            "trad_day,product_id,instrument_id,open_int,volume,close,expired\n\
             2025-11-04,AP,AP2601,1,1,1.000,0\n\
             2025-11-04,AP,AP601,125052,90,8952.000,0\n\
             2025-11-03,AP,AP2601,132129,100,9100.5,0\n\
             2025-11-03,WR,wr2601,10,1,3000,0\n",
        )
        .unwrap();
        let rows = load_daily_cross_section(&input).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].instrument_id, "AP2601");
        assert_eq!(rows[1].instrument_id, "wr2601");
        assert_eq!(rows[2].instrument_id, "AP2601");
        assert_eq!(rows[2].open_int, Some(125052.0));

        let out = dir.join("out");
        let summary = run_roll_replay(&RollReplayArgs {
            eod_input: &input,
            output_dir: &out,
            exchange: "xzce",
            start: NaiveDate::from_ymd_opt(2025, 11, 1).unwrap(),
            end: NaiveDate::from_ymd_opt(2025, 11, 30).unwrap(),
            rule: Rule::Rqdata0,
            overlap_cut: Some(NaiveDate::from_ymd_opt(2025, 11, 3).unwrap()),
        })
        .unwrap();
        assert_eq!(summary.n_products, 1);
        let eod = fs::read_to_string(out.join("xzce_eod.csv")).unwrap();
        assert_eq!(
            eod,
            "trad_day,product_id,instrument_id,open_int,volume,close,expired\r\n\
             2025-11-03,AP,AP2601,132129.0,100.0,9100.5,0\r\n\
             2025-11-04,AP,AP2601,125052.0,90.0,8952.0,0\r\n"
        );
        let dominant = fs::read_to_string(out.join("xzce_dominant.csv")).unwrap();
        assert!(dominant.ends_with("2025-11-04,AP,AP2601,0,125052.0,AP2601,local_rqdata_rule0\r\n"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
