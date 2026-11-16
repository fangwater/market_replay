//! Chinese / English name sources. Nothing here is invented:
//! - IB symbol -> Chinese name + category: the `PRODUCTS` table of
//!   `scripts/lseg/make_futures_checklist_excel.py` (the
//!   "IB期货与原始TAS中文清单" builder), compiled in.
//! - IB exchange code -> Chinese: the `EXCHANGES` table of the same script.
//! - LSEG exchange directory -> Chinese: `exchange_summary.csv`.
//! - LSEG product -> IB symbol: `all_ib_futures_tas_matches.csv` (tas_root)
//!   and catalog `ib_contracts` rows with TAS data.
//! - LSEG English: most common per-RIC `reference_description` in
//!   `ric_catalog.csv`, with the contract-month suffix stripped.
//! - CN exchanges: the six statutory exchange names.
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

pub const CHECKLIST_SCRIPT: &str =
    include_str!("../../../../scripts/lseg/make_futures_checklist_excel.py");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChineseName {
    pub name: String,
    pub category: String,
}

/// Parses `"KEY": ("中文", "类别"),` entries of the `PRODUCTS = {` block.
pub fn parse_products(script: &str) -> BTreeMap<String, ChineseName> {
    let mut out = BTreeMap::new();
    for line in dict_block(script, "PRODUCTS = {") {
        let strings = quoted(line);
        if strings.len() >= 3 && line.contains('(') {
            out.insert(
                strings[0].clone(),
                ChineseName {
                    name: strings[1].clone(),
                    category: strings[2].clone(),
                },
            );
        }
    }
    out
}

/// Parses `"KEY": "中文",` pairs of the `EXCHANGES = {` block (several per line).
pub fn parse_exchanges(script: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in dict_block(script, "EXCHANGES = {") {
        let strings = quoted(line);
        for pair in strings.chunks(2) {
            if let [k, v] = pair {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    out
}

fn dict_block<'a>(script: &'a str, start: &str) -> Vec<&'a str> {
    let mut lines = Vec::new();
    let mut inside = false;
    for line in script.lines() {
        if !inside {
            if line.trim_start().starts_with(start) {
                inside = true;
                let rest = &line[line.find('{').unwrap_or(0) + 1..];
                if !rest.trim().is_empty() {
                    lines.push(rest);
                }
            }
            continue;
        }
        if line.trim_start().starts_with('}') {
            break;
        }
        lines.push(line);
    }
    lines
}

/// Double-quoted literals of one Python line (no escapes used in these tables).
fn quoted(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur: Option<String> = None;
    for ch in line.chars() {
        match (&mut cur, ch) {
            (None, '"') => cur = Some(String::new()),
            (None, '#') => break,
            (Some(s), '"') => {
                out.push(std::mem::take(s));
                cur = None;
            }
            (Some(s), c) => s.push(c),
            (None, _) => {}
        }
    }
    out
}

pub fn cn_exchange_name(code: &str) -> &'static str {
    match code {
        "ccfx" => "中国金融期货交易所",
        "xdce" => "大连商品交易所",
        "xgfe" => "广州期货交易所",
        "xsge" => "上海期货交易所",
        "xsie" => "上海国际能源交易中心",
        "xzce" => "郑州商品交易所",
        _ => "",
    }
}

/// `exchange_summary.csv`: exchange (upper) -> exchange_name_cn; keyed lower-case.
pub fn load_exchange_summary(path: &Path) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Ok(mut rdr) = csv_reader(path) else {
        return out;
    };
    let Ok(headers) = rdr.headers().cloned() else {
        return out;
    };
    let (Some(ie), Some(ic)) = (col(&headers, "exchange"), col(&headers, "exchange_name_cn"))
    else {
        return out;
    };
    for rec in rdr.records().flatten() {
        let ex = rec.get(ie).unwrap_or("").to_ascii_lowercase();
        let cn = rec.get(ic).unwrap_or("").to_string();
        if !ex.is_empty() && !cn.is_empty() {
            out.insert(ex, cn);
        }
    }
    out
}

pub struct IbMatch {
    pub ib_symbol: String,
    pub ib_names: String,
    pub tas_root: String,
    pub status: String,
}

pub fn load_ib_matches(path: &Path) -> Vec<IbMatch> {
    let mut out = Vec::new();
    let Ok(mut rdr) = csv_reader(path) else {
        return out;
    };
    let Ok(headers) = rdr.headers().cloned() else {
        return out;
    };
    let get = |rec: &csv::StringRecord, name: &str| {
        col(&headers, name)
            .and_then(|i| rec.get(i))
            .unwrap_or("")
            .trim()
            .to_string()
    };
    for rec in rdr.records().flatten() {
        let names = get(&rec, "ib_names");
        out.push(IbMatch {
            ib_symbol: get(&rec, "ib_symbol"),
            ib_names: names
                .split('|')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(" / "),
            tas_root: get(&rec, "tas_root"),
            status: get(&rec, "status"),
        });
    }
    out
}

/// IB symbol -> (multiplier, currency, company_name), most common per symbol.
pub fn load_ib_snapshot(path: &Path) -> HashMap<String, (String, String, String)> {
    let mut out = HashMap::new();
    let Ok(bytes) = std::fs::read(path) else {
        return out;
    };
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return out;
    };
    let mut counts: HashMap<String, HashMap<(String, String, String), usize>> = HashMap::new();
    for c in v["contracts"].as_array().into_iter().flatten() {
        let s = |k: &str| c[k].as_str().unwrap_or("").trim().to_string();
        *counts
            .entry(s("symbol"))
            .or_default()
            .entry((s("multiplier"), s("currency"), s("company_name")))
            .or_default() += 1;
    }
    for (sym, m) in counts {
        if let Some((k, _)) = m
            .into_iter()
            .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
        {
            out.insert(sym, k);
        }
    }
    out
}

/// LSEG routed_product -> English description derived from per-RIC
/// `reference_description` (futures only).
pub fn load_lseg_descriptions(path: &Path) -> HashMap<String, String> {
    let mut counts: HashMap<String, HashMap<String, usize>> = HashMap::new();
    if let Ok(mut rdr) = csv_reader(path) {
        if let Ok(headers) = rdr.headers().cloned() {
            if let (Some(ia), Some(ip), Some(id)) = (
                col(&headers, "asset_class"),
                col(&headers, "routed_product"),
                col(&headers, "reference_description"),
            ) {
                for rec in rdr.records().flatten() {
                    if rec.get(ia) != Some("futures") {
                        continue;
                    }
                    let product = rec.get(ip).unwrap_or("");
                    for part in rec.get(id).unwrap_or("").split('|') {
                        let d = strip_contract_suffix(part);
                        if d.chars().filter(|c| c.is_ascii_alphabetic()).count() >= 3 {
                            *counts
                                .entry(product.to_string())
                                .or_default()
                                .entry(d)
                                .or_default() += 1;
                        }
                    }
                }
            }
        }
    }
    counts
        .into_iter()
        .filter_map(|(p, m)| {
            m.into_iter()
                .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
                .map(|(d, _)| (p, d))
        })
        .collect()
}

const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const SHORT: [&str; 16] = [
    "JA", "FE", "MR", "AP", "MY", "JN", "JL", "AU", "SE", "OC", "NO", "DE", "SU", "WI", "YR", "Q",
];

/// "LIGHT CRUDE JAN0" -> "LIGHT CRUDE"; "RCHRDS BAYSU19" -> "RCHRDS BAY";
/// "US 10YR NTE SEP5" -> "US 10YR NTE"; "RCHRDS BAY Q110" -> "RCHRDS BAY".
pub fn strip_contract_suffix(desc: &str) -> String {
    let s = desc.trim();
    let mut tokens: Vec<&str> = s.split_whitespace().collect();
    let Some(last) = tokens.last().copied() else {
        return String::new();
    };
    let upper = last.to_ascii_uppercase();
    let digits = upper.len() - upper.trim_end_matches(|c: char| c.is_ascii_digit()).len();
    let alpha = &upper[..upper.len() - digits];
    if digits == 0 {
        return s.to_string();
    }
    // "YR 20"
    if alpha.is_empty() && digits <= 2 && tokens.len() >= 3 {
        if tokens[tokens.len() - 2].eq_ignore_ascii_case("YR") {
            tokens.truncate(tokens.len() - 2);
            return tokens.join(" ");
        }
        return s.to_string();
    }
    // "Q110" / "Q20": quarter number + year digits.
    if alpha == "Q" && (2..=3).contains(&digits) && tokens.len() >= 2 {
        tokens.pop();
        return tokens.join(" ");
    }
    if digits > 2 {
        return s.to_string();
    }
    for m in MONTHS.iter().chain(SHORT.iter()) {
        if *m != "Q" && alpha.ends_with(m) {
            let keep = &last[..alpha.len() - m.len()];
            tokens.pop();
            if !keep.is_empty() {
                tokens.push(keep);
            }
            if tokens.is_empty() {
                return s.to_string();
            }
            return tokens.join(" ");
        }
    }
    s.to_string()
}

fn csv_reader(path: &Path) -> anyhow::Result<csv::Reader<std::fs::File>> {
    Ok(csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(std::fs::File::open(path)?))
}

fn col(headers: &csv::StringRecord, name: &str) -> Option<usize> {
    headers
        .iter()
        .position(|h| h.trim_start_matches('\u{feff}') == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_checklist_tables() {
        let p = parse_products(CHECKLIST_SCRIPT);
        assert!(p.len() >= 60, "{}", p.len());
        assert_eq!(p["ES"].name, "E-mini标普500指数");
        assert_eq!(p["CL"].name, "WTI轻质原油");
        assert_eq!(p["GC"].category, "贵金属");
        let e = parse_exchanges(CHECKLIST_SCRIPT);
        assert_eq!(e["CME"], "芝加哥商业交易所");
        assert_eq!(e["ICEEUSOFT"], "ICE欧洲软商品市场");
    }

    #[test]
    fn strips_month_suffixes() {
        assert_eq!(strip_contract_suffix("LIGHT CRUDE JAN0"), "LIGHT CRUDE");
        assert_eq!(strip_contract_suffix("RCHRDS BAYSU19"), "RCHRDS BAY");
        assert_eq!(strip_contract_suffix("US 10YR NTE SEP5"), "US 10YR NTE");
        assert_eq!(strip_contract_suffix("BITCOIN JA20"), "BITCOIN");
        assert_eq!(strip_contract_suffix("RCHRDS BAY Q110"), "RCHRDS BAY");
        assert_eq!(strip_contract_suffix("ECX EUA"), "ECX EUA");
    }
}
