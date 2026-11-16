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
//! - Maintained table `config/product_names.toml` ([`ProductNames`]): takes
//!   priority over all of the above; each entry records its evidence kind.
use crate::search::{Market, Product};
use serde::Deserialize;
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

/// Evidence kinds allowed in `product_names.toml` (`source` field).
pub const NAME_SOURCES: [&str; 5] = [
    "exchange_cn_page",
    "industry_standard",
    "derived_from_description",
    "derived_from_data",
    "local_record",
];

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NameEntry {
    pub market: String,
    pub code: String,
    pub name_cn: String,
    #[serde(default)]
    pub name_en: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    pub source: String,
    #[serde(default)]
    pub evidence: Option<String>,
}

/// `prefix` + 1–3 ASCII digits; `{n}` in the names is the number without
/// leading zeros.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NamePattern {
    pub market: String,
    pub prefix: String,
    pub name_cn: String,
    #[serde(default)]
    pub name_en: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    pub source: String,
    #[serde(default)]
    pub evidence: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamesFile {
    #[serde(default)]
    product: Vec<NameEntry>,
    #[serde(default)]
    pattern: Vec<NamePattern>,
}

/// The maintained name table (`config/product_names.toml`).
#[derive(Debug, Default)]
pub struct ProductNames {
    entries: HashMap<(Market, String), NameEntry>,
    patterns: Vec<(Market, NamePattern)>,
}

impl ProductNames {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
        Self::parse(&text).map_err(|e| anyhow::anyhow!("{}: {e:#}", path.display()))
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let file: NamesFile = toml::from_str(text)?;
        let check = |market: &str, key: &str, name: &str, source: &str| {
            let m = Market::parse(market)
                .ok_or_else(|| anyhow::anyhow!("{key}: unknown market {market:?}"))?;
            if name.trim().is_empty() {
                anyhow::bail!("{key}: empty name_cn");
            }
            if !NAME_SOURCES.contains(&source) {
                anyhow::bail!("{key}: source {source:?} not one of {NAME_SOURCES:?}");
            }
            Ok(m)
        };
        let mut out = Self::default();
        for e in file.product {
            let key = format!("{}/{}", e.market, e.code);
            let m = check(&e.market, &key, &e.name_cn, &e.source)?;
            if e.code.is_empty() {
                anyhow::bail!("{key}: empty code");
            }
            if out.entries.insert((m, e.code.clone()), e).is_some() {
                anyhow::bail!("{key}: duplicate entry");
            }
        }
        for p in file.pattern {
            let key = format!("{}/{}*", p.market, p.prefix);
            let m = check(&p.market, &key, &p.name_cn, &p.source)?;
            if p.prefix.is_empty() {
                anyhow::bail!("{key}: empty prefix");
            }
            out.patterns.push((m, p));
        }
        Ok(out)
    }

    /// (explicit entries, patterns)
    pub fn counts(&self) -> (usize, usize) {
        (self.entries.len(), self.patterns.len())
    }

    /// Explicit entry first, then the first matching pattern.
    pub fn lookup(&self, market: Market, code: &str) -> Option<NameEntry> {
        if let Some(e) = self.entries.get(&(market, code.to_string())) {
            return Some(e.clone());
        }
        self.patterns.iter().find_map(|(m, p)| {
            if *m != market {
                return None;
            }
            let rest = code.strip_prefix(p.prefix.as_str())?;
            if !(1..=3).contains(&rest.len()) || !rest.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let n = rest.trim_start_matches('0');
            let n = if n.is_empty() { "0" } else { n };
            let fill = |s: &str| s.replace("{n}", n);
            Some(NameEntry {
                market: p.market.clone(),
                code: code.to_string(),
                name_cn: fill(&p.name_cn),
                name_en: p.name_en.as_deref().map(fill),
                exchange: p.exchange.clone(),
                category: p.category.clone(),
                source: p.source.clone(),
                evidence: p.evidence.clone(),
            })
        })
    }

    /// Overrides Chinese name / category / English name of matching products.
    pub fn apply(&self, products: &mut [Product]) {
        for p in products.iter_mut() {
            let Some(e) = self.lookup(p.market, &p.code) else {
                continue;
            };
            p.name_cn_source = Some(match &e.evidence {
                Some(ev) => format!("{PRODUCT_NAMES_LABEL}（{}；{ev}）", e.source),
                None => format!("{PRODUCT_NAMES_LABEL}（{}）", e.source),
            });
            p.name_cn = Some(e.name_cn);
            if let Some(c) = e.category {
                p.category = Some(c);
            }
            if let Some(en) = e.name_en {
                p.name_en = Some(en);
                p.name_en_source = Some(PRODUCT_NAMES_LABEL.to_string());
            }
        }
    }
}

pub const PRODUCT_NAMES_LABEL: &str = "品种中文名表 config/product_names.toml";

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

    const REPO_NAMES: &str = include_str!("../../../../config/product_names.toml");

    fn product(market: Market, code: &str, cn: Option<&str>) -> Product {
        Product {
            market,
            code: code.into(),
            exchanges: vec![],
            exchange_cn: String::new(),
            name_cn: cn.map(Into::into),
            name_cn_source: cn.map(|_| "compiled".into()),
            category: None,
            name_en: Some("LSEG DESC".into()),
            name_en_source: Some("ric_catalog".into()),
            multiplier: None,
            tick: None,
            summary: serde_json::Value::Null,
        }
    }

    #[test]
    fn product_names_priority_and_patterns() {
        let t = ProductNames::parse(
            r#"
[[pattern]]
market = "lseg"
prefix = "VX"
name_cn = "VIX第{n}周"
source = "industry_standard"

[[product]]
market = "lseg"
code = "VX09"
name_cn = "明确条目"
name_en = "Explicit"
category = "波动率"
source = "derived_from_description"
evidence = "e"

[[product]]
market = "cn"
code = "LR"
name_cn = "晚籼稻"
source = "local_record"
"#,
        )
        .unwrap();
        assert_eq!(t.counts(), (2, 1));
        assert_eq!(t.lookup(Market::Lseg, "VX05").unwrap().name_cn, "VIX第5周");
        assert_eq!(t.lookup(Market::Lseg, "VX10").unwrap().name_cn, "VIX第10周");
        // explicit beats pattern; pattern needs 1-3 digits; market matters
        assert_eq!(t.lookup(Market::Lseg, "VX09").unwrap().name_cn, "明确条目");
        assert!(t.lookup(Market::Lseg, "VX").is_none());
        assert!(t.lookup(Market::Lseg, "VXU2:VE").is_none());
        assert!(t.lookup(Market::Ib, "VX05").is_none());
        assert!(t.lookup(Market::Lseg, "LR").is_none());

        let mut ps = vec![
            product(Market::Lseg, "VX09", Some("VIX波动率指数")),
            product(Market::Cn, "LR", None),
            product(Market::Cn, "RB", Some("螺纹钢")),
        ];
        t.apply(&mut ps);
        // file wins over the compiled-in name
        assert_eq!(ps[0].name_cn.as_deref(), Some("明确条目"));
        assert_eq!(ps[0].category.as_deref(), Some("波动率"));
        assert_eq!(ps[0].name_en.as_deref(), Some("Explicit"));
        assert!(ps[0]
            .name_cn_source
            .as_deref()
            .unwrap()
            .contains("derived_from_description"));
        assert_eq!(ps[1].name_cn.as_deref(), Some("晚籼稻"));
        assert_eq!(ps[1].name_en.as_deref(), Some("LSEG DESC"));
        // not in the file: compiled-in name kept
        assert_eq!(ps[2].name_cn.as_deref(), Some("螺纹钢"));
        assert_eq!(ps[2].name_cn_source.as_deref(), Some("compiled"));
    }

    #[test]
    fn product_names_rejects_bad_rows() {
        let row = |m: &str, src: &str| {
            format!("[[product]]\nmarket = \"{m}\"\ncode = \"X\"\nname_cn = \"x\"\nsource = \"{src}\"\n")
        };
        assert!(ProductNames::parse(&row("lseg", "industry_standard")).is_ok());
        assert!(ProductNames::parse(&row("lseg", "guess")).is_err());
        assert!(ProductNames::parse(&row("cme", "industry_standard")).is_err());
        let dup = row("lseg", "industry_standard").repeat(2);
        assert!(ProductNames::parse(&dup).is_err());
        assert!(ProductNames::parse("[[product]]\nmarket = \"cn\"\ncode = \"LR\"\nname_cn = \"x\"\nsource = \"local_record\"\ntypo = 1\n").is_err());
        assert!(ProductNames::load(Path::new("/nonexistent/product_names.toml")).is_err());
    }

    /// Every LSEG product without a Chinese name in the 2026-11-16 catalog
    /// (and CN LR) is covered by `config/product_names.toml`.
    #[test]
    fn repo_table_covers_known_gaps() {
        let t = ProductNames::parse(REPO_NAMES).unwrap();
        let missing = [
            "AD",
            "AFCM",
            "AFCQ",
            "AFCS",
            "AFCY",
            "ALI",
            "ATWM",
            "ATWQ",
            "ATWS",
            "ATWY",
            "AUL",
            "BP",
            "BR",
            "CD",
            "CGB",
            "CGF",
            "CGZ",
            "CNC",
            "CNH",
            "Cc1",
            "Cc10",
            "Cc11",
            "Cc12",
            "Cc13",
            "Cc14",
            "Cc15",
            "Cc16",
            "Cc17",
            "Cc2",
            "Cc3",
            "Cc4",
            "Cc5",
            "Cc6",
            "Cc7",
            "Cc8",
            "Cc9",
            "DM",
            "ETH",
            "FBTP",
            "FCE",
            "FCPO",
            "FCPO-TOT",
            "FEI",
            "FF",
            "FFI",
            "FGBX",
            "FLG",
            "FOAT",
            "FSMI",
            "FSR3",
            "FST3",
            "FTXIN9",
            "FVS",
            "FXXP",
            "G",
            "GIF",
            "GIFV4_TMP",
            "GSD",
            "H",
            "HCEI",
            "HHTI",
            "HRC",
            "HSI",
            "HTE",
            "IBV",
            "IPC",
            "JKM",
            "JNI",
            "JY",
            "KRW",
            "KS",
            "KW",
            "KWR",
            "LSUSPOT",
            "LXR",
            "MEM",
            "MFS",
            "MFST",
            "MP",
            "NCFM",
            "NCFQ",
            "NCFY",
            "NE",
            "NGLND",
            "NGLNDBOM",
            "NGLNDBOW",
            "NGLNDDA",
            "NGLNDMT",
            "NGLNDSAT",
            "NGLNDSUN",
            "NGLNDWDNW",
            "NGLNDWE",
            "NOKA",
            "NSEI",
            "OJ",
            "PHC",
            "PLZ",
            "RA",
            "RTY",
            "S1R",
            "SEK",
            "SEKA",
            "SF",
            "SINR",
            "SON3",
            "SRA",
            "SRI",
            "SSG",
            "STWN",
            "STXE",
            "SXF",
            "TFUBM",
            "TN",
            "TPY",
            "U",
            "URO",
            "US",
            "VX01",
            "VX02",
            "VX04",
            "VX05",
            "VX06",
            "VX08",
            "VX10",
            "VX11",
            "VX12",
            "VX14",
            "VX15",
            "VX16",
            "VX17",
            "VX18",
            "VX19",
            "VX20",
            "VX21",
            "VX22",
            "VX23",
            "VX24",
            "VX25",
            "VX27",
            "VX28",
            "VX29",
            "VX30",
            "VX31",
            "VX33",
            "VX34",
            "VX35",
            "VX37",
            "VX38",
            "VX39",
            "VX41",
            "VX43",
            "VX44",
            "VX48",
            "VX49",
            "VX50",
            "VX51",
            "VX52",
            "VX53",
            "VXU2:VE",
            "WTCL",
            "YAA",
            "YAP",
            "YR",
            "YTC",
            "YTT",
            "ZP",
        ];
        assert_eq!(missing.len(), 166);
        for code in missing {
            assert!(t.lookup(Market::Lseg, code).is_some(), "{code}");
        }
        assert_eq!(t.lookup(Market::Cn, "LR").unwrap().name_cn, "晚籼稻");
        assert_eq!(
            t.lookup(Market::Lseg, "Cc12").unwrap().name_cn,
            "CBOT玉米第12连续合约"
        );
    }

    #[test]
    fn search_finds_new_chinese_names() {
        let t = ProductNames::parse(REPO_NAMES).unwrap();
        let mut ps: Vec<Product> = ["HSI", "HCEI", "JY", "FCPO", "VX05", "AD"]
            .iter()
            .map(|c| product(Market::Lseg, c, None))
            .collect();
        // SGX iron ore keeps its compiled-in (checklist via IB SCI) name
        ps.push(product(Market::Lseg, "SZZF", Some("SGX铁矿石")));
        ps.push(product(Market::Cn, "I", Some("铁矿石")));
        ps.push(product(Market::Cn, "LR", None));
        t.apply(&mut ps);
        let ix = crate::search::Index::new(ps);
        let first = |q: &str| {
            ix.search(q, 10)
                .into_iter()
                .map(|h| h.product.code)
                .collect::<Vec<_>>()
        };
        assert_eq!(first("恒生指数"), vec!["HSI"]);
        assert_eq!(first("恒生"), vec!["HCEI", "HSI"]);
        assert_eq!(first("日元"), vec!["JY"]);
        assert_eq!(first("棕榈油"), vec!["FCPO"]);
        assert_eq!(first("铁矿石"), vec!["I", "SZZF"]);
        assert_eq!(first("晚籼稻"), vec!["LR"]);
        assert_eq!(first("VIX"), vec!["VX05"]);
        assert_eq!(first("澳元"), vec!["AD"]);
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
