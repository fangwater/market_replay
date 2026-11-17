//! Shared constants and normalization rules (port of `cn_futures_ref/models.py`).
use crate::dec::canonical_decimal;
use crate::json::J;
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::OnceLock;
use unicode_normalization::UnicodeNormalization;

pub const VERSION: &str = "0.5.0";
pub const SCHEMA_VERSION: i64 = 4;
pub const SOURCE_TYPE: &str = "OPENCTP_BROKER_REFERENCE";
pub const DEFAULT_SOURCE_URL: &str = "http://www.openctp.cn/fees.html";
pub const SESSION_SOURCE_TYPE: &str = "OPENCTP_SESSION_REFERENCE";
pub const DEFAULT_SESSION_SOURCE_URL: &str = "http://dict.openctp.cn/times?types=futures";

pub const VENUE_NAMES: &[(&str, &str)] = &[
    ("CFFEX", "中国金融期货交易所"),
    ("SHFE", "上海期货交易所"),
    ("INE", "上海国际能源交易中心"),
    ("DCE", "大连商品交易所"),
    ("CZCE", "郑州商品交易所"),
    ("GFEX", "广州期货交易所"),
];

pub fn venue_name(code: &str) -> &str {
    VENUE_NAMES
        .iter()
        .find(|(c, _)| *c == code)
        .map_or(code, |(_, n)| n)
}

/// A user-visible data problem: HTTP 400 (everything else is 500).
#[derive(Debug)]
pub struct DataError(pub String);
impl fmt::Display for DataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for DataError {}

pub fn data_error<T>(msg: impl Into<String>) -> anyhow::Result<T> {
    Err(DataError(msg.into()).into())
}

const VENUE_ALIASES: &[(&str, &str)] = &[
    ("CFFEX", "CFFEX"),
    ("中金所", "CFFEX"),
    ("中国金融期货交易所", "CFFEX"),
    ("SHFE", "SHFE"),
    ("上期所", "SHFE"),
    ("上海期货交易所", "SHFE"),
    ("INE", "INE"),
    ("上期能源", "INE"),
    ("上海国际能源交易中心", "INE"),
    ("DCE", "DCE"),
    ("大商所", "DCE"),
    ("大连商品交易所", "DCE"),
    ("CZCE", "CZCE"),
    ("郑商所", "CZCE"),
    ("郑州商品交易所", "CZCE"),
    ("GFEX", "GFEX"),
    ("广期所", "GFEX"),
    ("广州期货交易所", "GFEX"),
];

pub const HEADER_ALIAS_LIST: &[(&str, &str)] = &[
    ("交易所", "venue"),
    ("合约代码", "instrument_id"),
    ("合约名称", "instrument_name"),
    ("品种代码", "product_id"),
    ("品种名称", "product_name"),
    ("合约乘数", "contract_multiplier"),
    ("最小跳动", "price_tick"),
    ("最小变动价位", "price_tick"),
    ("开仓费率（按金额）", "open_rate_by_money"),
    ("开仓费率", "open_rate_by_money"),
    ("开仓费用（按手）", "open_fee_per_lot"),
    ("开仓费用/手", "open_fee_per_lot"),
    ("平仓费率（按金额）", "close_rate_by_money"),
    ("平仓费率", "close_rate_by_money"),
    ("平仓费用（按手）", "close_fee_per_lot"),
    ("平仓费用/手", "close_fee_per_lot"),
    ("平今费率（按金额）", "close_today_rate_by_money"),
    ("平今费率", "close_today_rate_by_money"),
    ("平今费用（按手）", "close_today_fee_per_lot"),
    ("平今费用/手", "close_today_fee_per_lot"),
    ("做多保证金率（按金额）", "long_margin_rate_by_money"),
    ("做多保证金率", "long_margin_rate_by_money"),
    ("做多保证金（按手）", "long_margin_per_lot"),
    ("做多保证金/手", "long_margin_per_lot"),
    ("做空保证金率（按金额）", "short_margin_rate_by_money"),
    ("做空保证金率", "short_margin_rate_by_money"),
    ("做空保证金（按手）", "short_margin_per_lot"),
    ("做空保证金/手", "short_margin_per_lot"),
    ("上日结算价", "previous_settlement_price"),
    ("上日收盘价", "previous_close_price"),
    ("最新价", "latest_price"),
    ("成交量", "volume"),
    ("持仓量", "open_interest"),
    ("1手开仓费用", "calculated_open_fee_one_lot"),
    ("1手平仓费用", "calculated_close_fee_one_lot"),
    ("1手平今费用", "calculated_close_today_fee_one_lot"),
    ("做多1手保证金", "calculated_long_margin_one_lot"),
    ("做空1手保证金", "calculated_short_margin_one_lot"),
    ("更新时间", "source_updated_at"),
];

const NUMERIC_FIELDS: &[&str] = &[
    "contract_multiplier",
    "price_tick",
    "open_rate_by_money",
    "open_fee_per_lot",
    "close_rate_by_money",
    "close_fee_per_lot",
    "close_today_rate_by_money",
    "close_today_fee_per_lot",
    "long_margin_rate_by_money",
    "long_margin_per_lot",
    "short_margin_rate_by_money",
    "short_margin_per_lot",
    "calculated_open_fee_one_lot",
    "calculated_close_fee_one_lot",
    "calculated_close_today_fee_one_lot",
    "calculated_long_margin_one_lot",
    "calculated_short_margin_one_lot",
    "previous_settlement_price",
    "previous_close_price",
    "latest_price",
    "volume",
    "open_interest",
];

pub const REQUIRED_FIELDS: &[&str] = &[
    "venue",
    "instrument_id",
    "product_id",
    "contract_multiplier",
    "price_tick",
    "open_rate_by_money",
    "open_fee_per_lot",
    "close_rate_by_money",
    "close_fee_per_lot",
    "close_today_rate_by_money",
    "close_today_fee_per_lot",
];

pub const DB_COLUMNS: &[&str] = &[
    "venue",
    "instrument_id",
    "instrument_name",
    "product_id",
    "product_name",
    "instrument_type",
    "contract_multiplier",
    "price_tick",
    "open_rate_by_money",
    "open_fee_per_lot",
    "close_rate_by_money",
    "close_fee_per_lot",
    "close_today_rate_by_money",
    "close_today_fee_per_lot",
    "long_margin_rate_by_money",
    "long_margin_per_lot",
    "short_margin_rate_by_money",
    "short_margin_per_lot",
    "calculated_open_fee_one_lot",
    "calculated_close_fee_one_lot",
    "calculated_close_today_fee_one_lot",
    "calculated_long_margin_one_lot",
    "calculated_short_margin_one_lot",
    "previous_settlement_price",
    "previous_close_price",
    "latest_price",
    "volume",
    "open_interest",
    "source_updated_at",
    "raw_json",
];

/// `models.header_key`: NFKC, drop BOM, drop all whitespace.
pub fn header_key(value: &str) -> String {
    value
        .nfkc()
        .collect::<String>()
        .replace('\u{feff}', "")
        .split_whitespace()
        .collect()
}

pub fn header_aliases() -> &'static HashMap<String, &'static str> {
    static MAP: OnceLock<HashMap<String, &'static str>> = OnceLock::new();
    MAP.get_or_init(|| {
        HEADER_ALIAS_LIST
            .iter()
            .filter(|(n, _)| !n.is_empty())
            .map(|(n, f)| (header_key(n), *f))
            .collect()
    })
}

/// `models.normalize_venue`.
pub fn normalize_venue(value: &str) -> String {
    let v = value.trim();
    VENUE_ALIASES
        .iter()
        .find(|(a, _)| *a == v)
        .map_or_else(|| v.to_uppercase(), |(_, c)| (*c).to_string())
}

pub fn infer_instrument_type(instrument_id: &str) -> &'static str {
    static RE: OnceLock<(Regex, Regex)> = OnceLock::new();
    let (a, b) = RE.get_or_init(|| {
        (
            Regex::new(r"-[CP]-").unwrap(),
            Regex::new(r"\d[CP]\d").unwrap(),
        )
    });
    let v = instrument_id.to_uppercase();
    if a.is_match(&v) || b.is_match(&v) {
        "OPTION"
    } else {
        "FUTURE"
    }
}

/// One normalized instrument row: DB column name -> value (`None` = NULL).
#[derive(Clone, Debug, Default)]
pub struct Record {
    pub f: HashMap<&'static str, Option<String>>,
}

impl Record {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.f.get(name).and_then(|v| v.as_deref())
    }
    fn set(&mut self, name: &'static str, value: Option<String>) {
        self.f.insert(name, value);
    }
    /// The `DB_COLUMNS` values as a JSON array (the record-hash payload).
    pub fn hash_payload(&self) -> String {
        J::Arr(
            DB_COLUMNS
                .iter()
                .map(|c| J::opt(self.get(c).map(String::from)))
                .collect(),
        )
        .dumps(false)
    }
}

/// `models.normalize_row`. `raw` is the header -> cell map, later duplicate
/// headers replacing earlier ones, like a Python dict.
pub fn normalize_row(raw: &[(String, String)]) -> Record {
    let aliases = header_aliases();
    let mut rec = Record::default();
    let mut sorted: BTreeMap<&str, &str> = BTreeMap::new();
    for (header, value) in raw {
        sorted.insert(header, value);
        if let Some(field) = aliases.get(&header_key(header)) {
            let v = if NUMERIC_FIELDS.contains(field) {
                canonical_decimal(Some(value))
            } else {
                let t = value.trim();
                (!t.is_empty()).then(|| t.to_string())
            };
            rec.set(field, v);
        }
    }
    if let Some(v) = rec.get("venue").map(normalize_venue) {
        rec.set("venue", Some(v));
    }
    for field in ["instrument_id", "product_id"] {
        if let Some(v) = rec.get(field).map(|s| s.trim().to_string()) {
            rec.set(field, Some(v));
        }
    }
    let ty = infer_instrument_type(rec.get("instrument_id").unwrap_or(""));
    rec.set("instrument_type", Some(ty.to_string()));
    let raw_json = J::Obj(
        sorted
            .into_iter()
            .map(|(k, v)| (k.to_string(), J::s(v)))
            .collect(),
    )
    .dumps(false);
    rec.set("raw_json", Some(raw_json));
    rec
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_width_headers_match_after_nfkc() {
        assert_eq!(
            header_aliases()
                .get(&header_key("开仓费率 (按金额)"))
                .copied(),
            Some("open_rate_by_money")
        );
        assert_eq!(header_key("\u{feff} 交 易 所"), "交易所");
    }

    #[test]
    fn option_detection() {
        assert_eq!(infer_instrument_type("m2701-C-3000"), "OPTION");
        assert_eq!(infer_instrument_type("sn2701"), "FUTURE");
    }
}
