//! IB contract -> actual LSEG RIC. Faithful Rust port of `delivery()` and the
//! RIC selection in `scripts/prepare_ib_ric_export.py`, with the CURRENT /
//! SUPPLEMENT root tables of `scripts/match_ib_all_futures_tas.py`.
//!
//! The snapshot plan (`contract_plan.json`) stays authoritative for every
//! con_id it resolved; this port is only used for contracts listed later.
//! "Observed" RICs come from the read-only `lseg/meta/catalog.sqlite`
//! (`contracts.ric`, `source_year`) instead of the 2026-11-02 inventory CSV;
//! the expiry evidence is the same actual HRD files.
use anyhow::{Context, Result};
use chrono::{Datelike, Duration, NaiveDate};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
};

pub const MONTHS: &str = "FGHJKMNQUVXZ";
/// (venue, currency, multiplier, IB local-symbol prefix)
pub type Spec = (&'static str, &'static str, &'static str, &'static str);
/// IB symbol -> Spec, as in scripts/prepare_ib_ric_export.py.
pub const SPECS: &[(&str, Spec)] = &[
    ("BRR", ("CME", "USD", "5", "BTC")),
    ("CC", ("NYBOT", "USD", "10", "CC")),
    ("CL", ("NYMEX", "USD", "1000", "CL")),
    ("COIL", ("IPE", "USD", "1000", "COIL")),
    ("CT", ("NYBOT", "USD", "50000", "CT")),
    ("DAX", ("EUREX", "EUR", "25", "FDAX")),
    ("DX", ("NYBOT", "USD", "1000", "DX")),
    ("ECF", ("ENDEX", "EUR", "1000", "ECF")),
    ("ES", ("CME", "USD", "50", "ES")),
    ("GBL", ("EUREX", "EUR", "1000", "FGBL")),
    ("GBM", ("EUREX", "EUR", "1000", "FGBM")),
    ("GBS", ("EUREX", "EUR", "1000", "FGBS")),
    ("GC", ("COMEX", "USD", "100", "GC")),
    ("GF", ("CME", "USD", "50000", "GF")),
    ("GOIL", ("IPE", "USD", "100", "GOIL")),
    ("HE", ("CME", "USD", "40000", "HE")),
    ("HG", ("COMEX", "USD", "25000", "HG")),
    ("HO", ("NYMEX", "USD", "42000", "HO")),
    ("JGB", ("OSE.JPN", "JPY", "1000000", "")),
    ("KC", ("NYBOT", "USD", "37500", "KC")),
    ("LE", ("CME", "USD", "40000", "LE")),
    ("NG", ("NYMEX", "USD", "10000", "NG")),
    ("NKD", ("CME", "USD", "5", "NKD")),
    ("NQ", ("CME", "USD", "20", "NQ")),
    ("PA", ("NYMEX", "USD", "100", "PA")),
    ("PL", ("NYMEX", "USD", "50", "PL")),
    ("RB", ("NYMEX", "USD", "42000", "RB")),
    ("RS", ("NYBOT", "CAD", "20", "RS")),
    ("SB", ("NYBOT", "USD", "112000", "SB")),
    ("SCI", ("SGX", "USD", "100", "FEF")),
    ("SI", ("COMEX", "USD", "5000", "SI")),
    ("TSR20", ("SGX", "USD", "50", "TF")),
    ("VIX", ("CFE", "USD", "1000", "VX")),
    ("W", ("ICEEUSOFT", "USD", "50", "W")),
    ("XINA50", ("SGX", "USD", "1", "CN")),
    ("YM", ("CBOT", "USD", "5", "YM")),
    ("ZC", ("CBOT", "USD", "5000", "ZC")),
    ("ZF", ("CBOT", "USD", "1000", "ZF")),
    ("ZL", ("CBOT", "USD", "60000", "ZL")),
    ("ZM", ("CBOT", "USD", "100", "ZM")),
    ("ZN", ("CBOT", "USD", "1000", "ZN")),
    ("ZS", ("CBOT", "USD", "5000", "ZS")),
    ("ZT", ("CBOT", "USD", "2000", "ZT")),
    ("ZW", ("CBOT", "USD", "5000", "ZW")),
];

/// Source roots checked against actual HRD venue, description, currency/units, ticker.
pub const CURRENT: &[(&str, &str)] = &[
    ("BRR", "BTC"),
    ("CC", "CC"),
    ("CL", "CL"),
    ("COIL", "LCO"),
    ("CT", "CT"),
    ("DAX", "FDX"),
    ("DX", "DX"),
    ("ECF", "CFI2"),
    ("ES", "ES"),
    ("GBL", "FGBL"),
    ("GBM", "FGBM"),
    ("GBS", "FGBS"),
    ("GC", "GC"),
    ("GF", "FC"),
    ("HE", "LH"),
    ("HG", "HG"),
    ("HO", "HO"),
    ("JGB", "JGB"),
    ("KC", "KC"),
    ("LE", "LC"),
    ("NG", "NG"),
    ("NQ", "NQ"),
    ("PA", "PA"),
    ("PL", "PL"),
    ("RB", "RB"),
    ("RS", "RS"),
    ("SB", "SB"),
    ("SCI", "SZZF"),
    ("SI", "SI"),
    ("VIX", "VX"),
    ("W", "LSU"),
    ("XINA50", "SFC"),
    ("YM", "YM"),
    ("ZC", "C"),
    ("ZF", "FV"),
    ("ZL", "BO"),
    ("ZM", "SM"),
    ("ZN", "TY"),
    ("ZS", "S"),
    ("ZT", "TU"),
    ("ZW", "W"),
];
pub const SUPPLEMENT: &[(&str, &str)] = &[("GOIL", "LGO"), ("NKD", "NK"), ("TSR20", "STF")];

fn lookup<T: Copy>(table: &[(&str, T)], key: &str) -> Option<T> {
    table.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
}

/// One `contract_info` row (instrument_type='FUT').
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Contract {
    pub con_id: String,
    pub symbol: String,
    pub exchange: Option<String>,
    pub multiplier: Option<String>,
    pub maturity_date: Option<String>,
    pub instrument_type: Option<String>,
    pub local_symbol: Option<String>,
    pub currency: Option<String>,
    pub company_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub root: String,
    pub month: char,
    pub year: i32,
}

/// Python `Decimal(a) == Decimal(b)` for plain decimal literals.
fn decimal_key(text: &str) -> Option<(bool, String, String)> {
    let t = text.trim();
    let (negative, t) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (int, frac) = t.split_once('.').unwrap_or((t, ""));
    if (int.is_empty() && frac.is_empty())
        || !int.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let int = int.trim_start_matches('0').to_string();
    let frac = frac.trim_end_matches('0').to_string();
    let zero = int.is_empty() && frac.is_empty();
    Some((negative && !zero, int, frac))
}

fn parse_expiry(text: Option<&str>) -> Result<NaiveDate, String> {
    let t = text.unwrap_or("");
    if t.len() != 8 || !t.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("invalid_maturity_date:{t}"));
    }
    NaiveDate::parse_from_str(t, "%Y%m%d").map_err(|_| format!("invalid_maturity_date:{t}"))
}

fn month_code(month: u32) -> char {
    MONTHS.as_bytes()[month as usize - 1] as char
}

/// Port of `delivery(c)`: (TAS root, delivery month code, delivery year).
pub fn delivery(c: &Contract) -> Result<Delivery, String> {
    let (venue, currency, multiplier, prefix) =
        lookup(SPECS, &c.symbol).ok_or_else(|| "symbol_not_in_covered_specs".to_string())?;
    let same_size = match c.multiplier.as_deref().map(decimal_key) {
        None => return Err("invalid_multiplier".into()),
        Some(None) => return Err("invalid_multiplier".into()),
        Some(Some(k)) => Some(k) == decimal_key(multiplier),
    };
    if c.exchange.as_deref() != Some(venue) || c.currency.as_deref() != Some(currency) || !same_size
    {
        return Err("different_venue_currency_or_size".into());
    }
    let expiry = parse_expiry(c.maturity_date.as_deref())?;
    let local = c.local_symbol.as_deref().unwrap_or("");
    let current = |s: &str| lookup(CURRENT, s).map(str::to_string);
    if c.symbol == "JGB" {
        let ok = local.len() == 9
            && local.starts_with("16")
            && local.bytes().all(|b| b.is_ascii_digit());
        if !ok {
            return Err("unrecognized_JGB_local_symbol".into());
        }
        return Ok(Delivery {
            root: current("JGB").unwrap(),
            month: month_code(expiry.month()),
            year: expiry.year(),
        });
    }
    let p = regex::escape(prefix);
    if venue == "EUREX" {
        let dated = Regex::new(&format!(r"^{p} (\d{{8}}) M$")).unwrap();
        let by_date = dated
            .captures(local)
            .is_some_and(|m| Some(&m[1]) == c.maturity_date.as_deref());
        if !by_date {
            let words = Regex::new(&format!(
                r"^{p} (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC) (\d{{2}})$"
            ))
            .unwrap();
            let ok = words.captures(local).is_some_and(|w| {
                w[1] == expiry.format("%b").to_string().to_uppercase()
                    && w[2] == expiry.format("%y").to_string()
            });
            if !ok {
                return Err("unrecognized_Eurex_local_symbol".into());
            }
        }
        return Ok(Delivery {
            root: current(&c.symbol)
                .context("Eurex root")
                .map_err(|e| e.to_string())?,
            month: month_code(expiry.month()),
            year: expiry.year(),
        });
    }
    let weekly = if c.symbol == "VIX" { r"(\d{2})?" } else { "" };
    let pattern = Regex::new(&format!(r"^{p}{weekly}([{MONTHS}])(\d{{1,2}})$")).unwrap();
    let Some(m) = pattern.captures(local) else {
        return Err("unrecognized_local_symbol".into());
    };
    let n = m.len();
    let month = m[n - 2].chars().next().unwrap();
    let token = &m[n - 1];
    let years: Vec<i32> = (expiry.year() - 1..expiry.year() + 2)
        .filter(|y| y.to_string().ends_with(token))
        .collect();
    if years.len() != 1 {
        return Err("ambiguous_delivery_year".into());
    }
    let mut root = lookup(CURRENT, &c.symbol)
        .or_else(|| lookup(SUPPLEMENT, &c.symbol))
        .ok_or_else(|| "symbol_without_tas_root".to_string())?
        .to_string();
    if c.symbol == "VIX" {
        if let Some(w) = m.get(1) {
            root.push_str(w.as_str());
        }
    }
    Ok(Delivery {
        root,
        month,
        year: years[0],
    })
}

/// HRD fields retained as expiry evidence (same as the Python preparation).
pub const HRD_FIELDS: [&str; 7] = [
    "RIC",
    "Expiration Date",
    "Market MIC",
    "Currency Code",
    "Lot Size",
    "Lot Units",
    "Ticker",
];
pub type HrdRecord = BTreeMap<String, String>;

/// Read only the wanted RICs' HRD records from each CSV (streamed, bounded by `wanted`).
pub fn load_hrd(
    files: &[impl AsRef<Path>],
    wanted: &BTreeSet<String>,
) -> Result<HashMap<String, Vec<HrdRecord>>> {
    let mut out: HashMap<String, Vec<HrdRecord>> = HashMap::new();
    for file in files {
        let file = file.as_ref();
        let mut reader =
            csv::Reader::from_path(file).with_context(|| format!("HRD {}", file.display()))?;
        let headers = reader.headers()?.clone();
        let index: Vec<usize> = HRD_FIELDS
            .iter()
            .map(|f| {
                headers
                    .iter()
                    .position(|h| h == *f)
                    .with_context(|| format!("HRD column {f} in {}", file.display()))
            })
            .collect::<Result<_>>()?;
        for record in reader.records() {
            let record = record?;
            let ric = record.get(index[0]).unwrap_or("");
            if !wanted.contains(ric) {
                continue;
            }
            let v: HrdRecord = HRD_FIELDS
                .iter()
                .zip(&index)
                .map(|(k, i)| (k.to_string(), record.get(*i).unwrap_or("").to_string()))
                .collect();
            let list = out.entry(ric.to_string()).or_default();
            if !list.contains(&v) {
                list.push(v);
            }
        }
    }
    Ok(out)
}

/// RIC -> source years in which LSEG TAS observed it.
pub type Observed = HashMap<String, BTreeSet<i32>>;

pub struct Candidate {
    pub contract: Contract,
    pub delivery: Delivery,
    pub possible: Vec<String>,
}

fn in_window(periods: &BTreeSet<i32>, year: i32) -> bool {
    periods.iter().any(|p| year - 9 <= *p && *p <= year + 1)
}

/// First half of the Python main loop: delivery + candidate RIC names.
pub fn candidate(c: &Contract, observed: &Observed) -> Result<Candidate, String> {
    let d = delivery(c)?;
    let stems = [
        format!("{}{}{:02}", d.root, d.month, d.year.rem_euclid(100)),
        format!("{}{}{}", d.root, d.month, d.year.rem_euclid(10)),
    ];
    let mut possible: Vec<String> = if c.symbol == "VIX" && d.root != "VX" {
        // Weekly RIC month codes can refer to the following monthly cycle;
        // the actual expiry date, not an IB month substitution, resolves it.
        let weekly = Regex::new(&format!(
            r"^{}[{MONTHS}]\d{{1,2}}(?:\^.*)?$",
            regex::escape(&d.root)
        ))
        .unwrap();
        observed
            .iter()
            .filter(|(ric, p)| weekly.is_match(ric) && in_window(p, d.year))
            .map(|(r, _)| r.clone())
            .collect()
    } else {
        observed
            .iter()
            .filter(|(ric, p)| {
                stems.contains(&ric.split('^').next().unwrap().to_string()) && in_window(p, d.year)
            })
            .map(|(r, _)| r.clone())
            .collect()
    };
    // HRD may contain listed contracts without source events yet.
    possible.extend(stems);
    possible.sort();
    possible.dedup();
    Ok(Candidate {
        contract: c.clone(),
        delivery: d,
        possible,
    })
}

fn hrd_date(text: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(text, "%m/%d/%Y").ok()
}

pub struct Resolved {
    pub ric: String,
    pub aliases: Vec<String>,
    pub evidence: BTreeMap<String, Vec<HrdRecord>>,
}

/// Second half of the Python main loop: HRD expiry evidence and canonical RIC.
pub fn resolve(
    c: &Candidate,
    hrd: &HashMap<String, Vec<HrdRecord>>,
    observed: &Observed,
) -> Result<Resolved, String> {
    let expiry = parse_expiry(c.contract.maturity_date.as_deref())?;
    let (dy, dm) = (
        c.delivery.year,
        MONTHS.find(c.delivery.month).unwrap() as u32 + 1,
    );
    let mut aliases = Vec::new();
    let mut evidence = BTreeMap::new();
    for ric in &c.possible {
        let mut versions = Vec::new();
        for v in hrd.get(ric).into_iter().flatten() {
            let Some(d) = hrd_date(&v["Expiration Date"]) else {
                continue;
            };
            let mut same = (d - expiry).num_days().abs() <= 7;
            if c.contract.symbol == "TSR20" {
                // IB records the previous month's last trading day; HRD records a
                // delivery-month date. Match the delivery month and SICOM identity.
                let previous = NaiveDate::from_ymd_opt(dy, dm, 1).unwrap() - Duration::days(1);
                same = d.year() == dy
                    && d.month() == dm
                    && expiry.year() == previous.year()
                    && expiry.month() == previous.month()
                    && v["Market MIC"] == "XSIM"
                    && v["Currency Code"] == "USc"
                    && v["Lot Size"] == "5,000"
                    && v["Lot Units"] == "KG"
                    && v["Ticker"] == "TF";
            }
            if same {
                versions.push(v.clone());
            }
        }
        if !versions.is_empty() {
            aliases.push(ric.clone());
            evidence.insert(ric.clone(), versions);
        }
    }
    if aliases.is_empty() {
        return Err("no_matching_expiry_in_actual_HRD".into());
    }
    let digits = Regex::new(r"\d+$").unwrap();
    let rank = |ric: &String| {
        let valid = observed
            .get(ric)
            .into_iter()
            .flatten()
            .filter(|p| dy - 9 <= **p && **p <= dy + 1)
            .max()
            .copied()
            .unwrap_or(0);
        let stem = ric.split('^').next().unwrap();
        let n = digits.find(stem).map(|m| m.as_str().len()).unwrap_or(0);
        (valid, n, !ric.contains('^'), ric.clone())
    };
    let ric = aliases.iter().max_by_key(|r| rank(r)).unwrap().clone();
    aliases.sort();
    Ok(Resolved {
        ric,
        aliases,
        evidence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn c(
        symbol: &str,
        exchange: &str,
        currency: &str,
        mult: &str,
        maturity: &str,
        local: &str,
    ) -> Contract {
        Contract {
            con_id: "1".into(),
            symbol: symbol.into(),
            exchange: Some(exchange.into()),
            multiplier: Some(mult.into()),
            maturity_date: Some(maturity.into()),
            instrument_type: Some("FUT".into()),
            local_symbol: Some(local.into()),
            currency: Some(currency.into()),
            company_name: None,
        }
    }
    fn d(root: &str, month: char, year: i32) -> Delivery {
        Delivery {
            root: root.into(),
            month,
            year,
        }
    }
    #[test]
    fn delivery_month_comes_from_local_symbol_not_expiry() {
        // CLM6 expires in May 2026 but delivers June.
        assert_eq!(
            delivery(&c("CL", "NYMEX", "USD", "1000", "20260519", "CLM6")),
            Ok(d("CL", 'M', 2026))
        );
        assert_eq!(
            delivery(&c("ES", "CME", "USD", "50", "20261218", "ESZ6")),
            Ok(d("ES", 'Z', 2026))
        );
        // Decimal equality like Python: "50.0" == "50".
        assert_eq!(
            delivery(&c("ES", "CME", "USD", "50.0", "20261218", "ESZ6")),
            Ok(d("ES", 'Z', 2026))
        );
        assert_eq!(
            delivery(&c("SI", "COMEX", "USD", "1000", "20261229", "SILZ6")),
            Err("different_venue_currency_or_size".into())
        );
        assert_eq!(
            delivery(&c("GOIL", "IPE", "USD", "100", "20261210", "GOILZ6")),
            Ok(d("LGO", 'Z', 2026))
        );
        assert_eq!(
            delivery(&c("TSR20", "SGX", "USD", "50", "20260529", "TFM6")),
            Ok(d("STF", 'M', 2026))
        );
        assert_eq!(
            delivery(&c("FOO", "CME", "USD", "1", "20261218", "FOOZ6")),
            Err("symbol_not_in_covered_specs".into())
        );
        assert_eq!(
            delivery(&c("ES", "CME", "USD", "50", "2026121", "ESZ6")),
            Err("invalid_maturity_date:2026121".into())
        );
    }
    #[test]
    fn vix_monthly_and_weekly_roots() {
        assert_eq!(
            delivery(&c("VIX", "CFE", "USD", "1000", "20261118", "VXX6")),
            Ok(d("VX", 'X', 2026))
        );
        // Weekly VXnn keeps its own root; never folded into VX.
        assert_eq!(
            delivery(&c("VIX", "CFE", "USD", "1000", "20261125", "VX48X6")),
            Ok(d("VX48", 'X', 2026))
        );
        assert_eq!(
            delivery(&c("VIX", "CFE", "USD", "1000", "20261125", "VX4X6")),
            Err("unrecognized_local_symbol".into())
        );
    }
    #[test]
    fn eurex_dated_and_word_local_symbols() {
        assert_eq!(
            delivery(&c(
                "DAX",
                "EUREX",
                "EUR",
                "25",
                "20261218",
                "FDAX 20261218 M"
            )),
            Ok(d("FDX", 'Z', 2026))
        );
        assert_eq!(
            delivery(&c("GBL", "EUREX", "EUR", "1000", "20261208", "FGBL DEC 26")),
            Ok(d("FGBL", 'Z', 2026))
        );
        // Date token must equal maturity, else the month-word form must match.
        assert_eq!(
            delivery(&c(
                "GBL",
                "EUREX",
                "EUR",
                "1000",
                "20261208",
                "FGBL 20261209 M"
            )),
            Err("unrecognized_Eurex_local_symbol".into())
        );
        assert_eq!(
            delivery(&c("GBL", "EUREX", "EUR", "1000", "20261208", "FGBL NOV 26")),
            Err("unrecognized_Eurex_local_symbol".into())
        );
        // Mini DAX has another multiplier.
        assert_eq!(
            delivery(&c(
                "DAX",
                "EUREX",
                "EUR",
                "5",
                "20261218",
                "FDXM 20261218 M"
            )),
            Err("different_venue_currency_or_size".into())
        );
    }
    #[test]
    fn jgb_numeric_local_symbol_uses_expiry_month() {
        assert_eq!(
            delivery(&c(
                "JGB",
                "OSE.JPN",
                "JPY",
                "1000000",
                "20261211",
                "164120018"
            )),
            Ok(d("JGB", 'Z', 2026))
        );
        assert_eq!(
            delivery(&c("JGB", "OSE.JPN", "JPY", "1000000", "20261211", "JBZ6")),
            Err("unrecognized_JGB_local_symbol".into())
        );
    }
    #[test]
    fn delivery_year_window_and_two_digit_years() {
        assert_eq!(
            delivery(&c("ES", "CME", "USD", "50", "20271217", "ESZ27")),
            Ok(d("ES", 'Z', 2027))
        );
        // One-digit token resolves within expiry-1..expiry+1 only.
        assert_eq!(
            delivery(&c("ZC", "CBOT", "USD", "5000", "20271214", "ZCZ6")),
            Ok(d("C", 'Z', 2026))
        );
        assert_eq!(
            delivery(&c("ZC", "CBOT", "USD", "5000", "20271214", "ZCZ4")),
            Err("ambiguous_delivery_year".into())
        );
    }
    fn hrd(ric: &str, exp: &str) -> HrdRecord {
        HRD_FIELDS
            .iter()
            .map(|k| (k.to_string(), String::new()))
            .chain([
                ("RIC".to_string(), ric.to_string()),
                ("Expiration Date".to_string(), exp.to_string()),
            ])
            .collect()
    }
    #[test]
    fn canonical_ric_prefers_latest_observed_cycle_and_two_digit_year() {
        let ct = c("ES", "CME", "USD", "50", "20261218", "ESZ6");
        let mut observed = Observed::new();
        observed.insert("ESZ6".into(), [2016, 2026].into());
        observed.insert("ESZ26".into(), [2026].into());
        let cand = candidate(&ct, &observed).unwrap();
        assert_eq!(cand.possible, vec!["ESZ26".to_string(), "ESZ6".to_string()]);
        let mut h = HashMap::new();
        h.insert("ESZ26".to_string(), vec![hrd("ESZ26", "12/18/2026")]);
        h.insert(
            "ESZ6".to_string(),
            vec![hrd("ESZ6", "12/16/2016"), hrd("ESZ6", "12/18/2026")],
        );
        let r = resolve(&cand, &h, &observed).unwrap();
        assert_eq!(r.ric, "ESZ26");
        assert_eq!(r.aliases, vec!["ESZ26".to_string(), "ESZ6".to_string()]);
        assert_eq!(r.evidence["ESZ6"].len(), 1);
        // No HRD expiry within 7 days -> unresolved (a problem, never guessed).
        let mut far = HashMap::new();
        far.insert("ESZ26".to_string(), vec![hrd("ESZ26", "12/30/2026")]);
        assert_eq!(
            resolve(&cand, &far, &observed).err(),
            Some("no_matching_expiry_in_actual_HRD".to_string())
        );
    }
    #[test]
    fn weekly_vix_candidates_come_from_observed_and_expiry_selects() {
        let ct = c("VIX", "CFE", "USD", "1000", "20261125", "VX48X6");
        let mut observed = Observed::new();
        for (r, y) in [
            ("VX48X6", 2026),
            ("VX48Z6", 2026),
            ("VX48Z5", 2015),
            ("VXX6", 2026),
        ] {
            observed.insert(r.into(), [y].into());
        }
        let cand = candidate(&ct, &observed).unwrap();
        assert_eq!(
            cand.possible,
            vec!["VX48X6", "VX48X26", "VX48Z6"]
                .into_iter()
                .map(String::from)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        );
        let mut h = HashMap::new();
        h.insert("VX48Z6".to_string(), vec![hrd("VX48Z6", "11/25/2026")]);
        h.insert("VX48X6".to_string(), vec![hrd("VX48X6", "11/04/2026")]);
        assert_eq!(resolve(&cand, &h, &observed).unwrap().ric, "VX48Z6");
    }
    #[test]
    fn tsr20_matches_delivery_month_identity() {
        let ct = c("TSR20", "SGX", "USD", "50", "20260529", "TFM6");
        let mut observed = Observed::new();
        observed.insert("STFM6".into(), [2026].into());
        let cand = candidate(&ct, &observed).unwrap();
        let mut rec = hrd("STFM6", "06/08/2026");
        for (k, v) in [
            ("Market MIC", "XSIM"),
            ("Currency Code", "USc"),
            ("Lot Size", "5,000"),
            ("Lot Units", "KG"),
            ("Ticker", "TF"),
        ] {
            rec.insert(k.into(), v.into());
        }
        let mut h = HashMap::new();
        h.insert("STFM6".to_string(), vec![rec.clone()]);
        assert_eq!(resolve(&cand, &h, &observed).unwrap().ric, "STFM6");
        rec.insert("Lot Units".into(), "MT".into());
        h.insert("STFM6".to_string(), vec![rec]);
        assert!(resolve(&cand, &h, &observed).is_err());
    }
}
