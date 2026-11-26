//! OpenCTP continuous-session normalization (port of `trading_sessions.py`).
use crate::json::J;
use crate::models::{data_error, normalize_venue, DataError, VERSION};
use crate::source;
use anyhow::Result;
use chrono::NaiveTime;
use std::collections::{BTreeMap, BTreeSet, HashSet};

const REQUIRED: [&str; 7] = [
    "ExchangeID",
    "ProductID",
    "SegmentNo",
    "TimeBegin",
    "TimeEnd",
    "ProductClass",
    "Area",
];
const EXPECTED_VENUES: [&str; 6] = ["CFFEX", "CZCE", "DCE", "GFEX", "INE", "SHFE"];

const DAY_STOCK: [(&str, &str); 2] = [("09:30:00", "11:30:00"), ("13:00:00", "15:00:00")];
const DAY_BOND: [(&str, &str); 2] = [("09:30:00", "11:30:00"), ("13:00:00", "15:15:00")];

fn cffex_sessions(product: &str) -> Option<&'static [(&'static str, &'static str); 2]> {
    match product {
        "IF" | "IH" | "IC" | "IM" => Some(&DAY_STOCK),
        "T" | "TF" | "TL" | "TS" => Some(&DAY_BOND),
        _ => None,
    }
}
const CFFEX_PRODUCTS: [&str; 8] = ["IF", "IH", "IC", "IM", "T", "TF", "TL", "TS"];

/// One normalized session row; field order is the DB / hash order.
#[derive(Clone, Debug)]
pub struct SessionRecord {
    pub venue: String,
    pub product_id: String,
    pub segment_no: i64,
    pub time_begin: String,
    pub time_end: String,
    pub crosses_midnight: bool,
    pub session_group: String,
    pub product_class: String,
    pub area: String,
    pub is_active: bool,
    /// `json.dumps(raw, sort_keys=True, compact)` text, or the parsed value
    /// for tombstones copied from a stored row.
    pub raw_json: J,
}

fn clock(value: &J, field: &str) -> Result<NaiveTime> {
    let bad = || {
        DataError(format!(
            "OpenCTP session {field} must use HH:MM:SS: {}",
            py_value_repr(value)
        ))
    };
    match value {
        J::Str(s) => NaiveTime::parse_from_str(s, "%H:%M:%S").map_err(|_| bad().into()),
        _ => Err(bad().into()),
    }
}

/// `repr()` for the JSON scalars that appear in error messages.
fn py_value_repr(v: &J) -> String {
    match v {
        J::Null => "None".into(),
        J::Bool(true) => "True".into(),
        J::Bool(false) => "False".into(),
        J::Int(i) => i.to_string(),
        J::Num(n) => n.clone(),
        J::Str(s) => crate::json::py_repr(s),
        _ => "<value>".into(),
    }
}

/// Python `str(value)` for scalars.
fn py_str(v: &J) -> String {
    match v {
        J::Str(s) => s.clone(),
        other => py_value_repr(other),
    }
}

/// `trading_sessions.parse_payload`.
pub fn parse_payload(content: &[u8], content_type: &str) -> Result<Vec<SessionRecord>> {
    let text = source::decode(content, content_type)?;
    let payload: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => return data_error(format!("OpenCTP session response is not valid JSON: {e}")),
    };
    let obj = payload.as_object();
    if obj.and_then(|o| o.get("rsp_code")).and_then(|c| c.as_i64()) != Some(0) {
        let msg = obj
            .and_then(|o| o.get("rsp_message"))
            .map(|m| m.as_str().map_or(m.to_string(), String::from))
            .unwrap_or_else(|| "invalid response".into());
        return data_error(format!("OpenCTP session request failed: {msg}"));
    }
    let data = match obj.and_then(|o| o.get("data")).and_then(|d| d.as_array()) {
        Some(d) if !d.is_empty() => d,
        _ => return data_error("OpenCTP session response contained no rows"),
    };
    let mut records: Vec<SessionRecord> = Vec::new();
    let mut seen: HashSet<(String, String, i64)> = HashSet::new();
    for (index, raw) in data.iter().enumerate() {
        let Some(raw) = raw.as_object() else {
            return data_error(format!("OpenCTP session row {index} is not an object"));
        };
        let mut missing: Vec<&str> = REQUIRED
            .iter()
            .copied()
            .filter(|f| !raw.contains_key(*f))
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return data_error(format!(
                "OpenCTP session row {index} is missing fields: {}",
                missing.join(", ")
            ));
        }
        let jv = |k: &str| -> J { serde_json::from_value(raw[k].clone()).unwrap_or(J::Null) };
        let venue = normalize_venue(&truthy_str(&jv("ExchangeID")));
        let product_id = truthy_str(&jv("ProductID")).trim().to_string();
        let segment_no = match jv("SegmentNo") {
            J::Int(i) => i,
            J::Str(s) => s.trim().parse::<i64>().map_err(|_| {
                DataError(format!(
                    "OpenCTP session SegmentNo is invalid: {}",
                    crate::json::py_repr(&s)
                ))
            })?,
            other => {
                return data_error(format!(
                    "OpenCTP session SegmentNo is invalid: {}",
                    py_value_repr(&other)
                ))
            }
        };
        if venue.is_empty() || product_id.is_empty() || segment_no < 1 {
            return data_error(format!(
                "OpenCTP session row {index} has an invalid identity"
            ));
        }
        let key = (venue.to_uppercase(), product_id.to_uppercase(), segment_no);
        if !seen.insert(key.clone()) {
            return data_error(format!(
                "duplicate OpenCTP session row: {}.{}.{}",
                key.0, key.1, key.2
            ));
        }
        let begin = clock(&jv("TimeBegin"), "TimeBegin")?;
        let end = clock(&jv("TimeEnd"), "TimeEnd")?;
        if begin == end {
            return data_error(format!(
                "OpenCTP session has equal begin and end times: {}.{}.{}",
                key.0, key.1, key.2
            ));
        }
        let sorted: BTreeMap<&String, &serde_json::Value> = raw.iter().collect();
        let raw_text = J::Obj(
            sorted
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        serde_json::from_value(v.clone()).unwrap_or(J::Null),
                    )
                })
                .collect(),
        )
        .dumps(false);
        records.push(SessionRecord {
            venue,
            product_id,
            segment_no,
            time_begin: begin.format("%H:%M:%S").to_string(),
            time_end: end.format("%H:%M:%S").to_string(),
            crosses_midnight: end < begin,
            session_group: if begin >= NaiveTime::from_hms_opt(18, 0, 0).unwrap() {
                "NIGHT".into()
            } else {
                "DAY".into()
            },
            product_class: py_str(&jv("ProductClass")),
            area: py_str(&jv("Area")),
            is_active: true,
            raw_json: J::Str(raw_text),
        });
    }
    let mut grouped: BTreeMap<String, Vec<&SessionRecord>> = BTreeMap::new();
    for r in &records {
        if r.venue == "CFFEX" && cffex_sessions(&r.product_id.to_uppercase()).is_some() {
            grouped
                .entry(r.product_id.to_uppercase())
                .or_default()
                .push(r);
        }
    }
    for (product, mut rows) in grouped {
        rows.sort_by_key(|r| r.segment_no);
        let actual: Vec<(&str, &str)> = rows
            .iter()
            .map(|r| (r.time_begin.as_str(), r.time_end.as_str()))
            .collect();
        let expected = cffex_sessions(&product).unwrap();
        if actual != expected {
            return data_error(format!(
                "OpenCTP CFFEX {product} sessions changed; expected {}, got {}; verify the exchange schedule before importing",
                py_pairs(expected),
                py_pairs(&actual)
            ));
        }
    }
    Ok(records)
}

fn py_pairs(p: &[(&str, &str)]) -> String {
    let items: Vec<String> = p.iter().map(|(a, b)| format!("('{a}', '{b}')")).collect();
    match items.len() {
        1 => format!("({},)", items[0]),
        _ => format!("({})", items.join(", ")),
    }
}

/// New products that OpenCTP's session list does not carry yet: `(venue, new product, model product, why)`.
/// Until OpenCTP lists the product its rows are copies of the model product's rows (same venue), marked in
/// `raw_json` (`AliasOf`, `AliasReason`); the moment OpenCTP lists it, its own rows win (the next sync records
/// them as a new version). An entry is added only with evidence that the hours are the same.
const LISTING_ALIASES: [(&str, &str, &str, &str); 1] = [(
    "GFEX",
    "LI",
    "LC",
    "GFEX lithium hydroxide futures listed 2026-11-26; day-only hours like lithium carbonate (the same trading windows in the Tonglian 2026-11-26 data)",
)];

fn alias_raw(model_raw: &J, new_product: &str, model: &str, why: &str) -> J {
    let mut map: BTreeMap<String, J> = match model_raw {
        J::Str(text) => serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .map(|o| {
                o.into_iter()
                    .map(|(k, v)| (k, serde_json::from_value(v).unwrap_or(J::Null)))
                    .collect()
            })
            .unwrap_or_default(),
        _ => BTreeMap::new(),
    };
    map.insert("ProductID".into(), J::Str(new_product.to_string()));
    map.insert("AliasOf".into(), J::Str(model.to_string()));
    map.insert("AliasReason".into(), J::Str(why.to_string()));
    J::Str(J::Obj(map.into_iter().collect()).dumps(false))
}

/// Adds the alias rows for products OpenCTP does not list; returns what was added (`VENUE.PRODUCT`).
pub fn apply_listing_aliases(records: &mut Vec<SessionRecord>) -> Vec<String> {
    let mut applied = Vec::new();
    for (venue, product, model, why) in LISTING_ALIASES {
        let is =
            |r: &SessionRecord, p: &str| r.venue == venue && r.product_id.eq_ignore_ascii_case(p);
        if records.iter().any(|r| is(r, product)) {
            continue;
        }
        let rows: Vec<SessionRecord> = records.iter().filter(|r| is(r, model)).cloned().collect();
        if rows.is_empty() {
            continue;
        }
        let lower = rows[0].product_id.chars().all(|c| !c.is_uppercase());
        let id = if lower {
            product.to_lowercase()
        } else {
            product.to_string()
        };
        for mut row in rows {
            row.raw_json = alias_raw(&row.raw_json, &id, model, why);
            row.product_id = id.clone();
            records.push(row);
        }
        applied.push(format!("{venue}.{product}"));
    }
    applied
}

/// Python truthiness then `str()`: empty/zero/null -> "".
fn truthy_str(v: &J) -> String {
    match v {
        J::Null | J::Bool(false) | J::Int(0) => String::new(),
        J::Str(s) => s.clone(),
        other => py_str(other),
    }
}

pub fn validate_full_snapshot(records: &[SessionRecord]) -> Result<()> {
    let venues: BTreeSet<&str> = records.iter().map(|r| r.venue.as_str()).collect();
    let missing: Vec<&str> = EXPECTED_VENUES
        .iter()
        .copied()
        .filter(|v| !venues.contains(v))
        .collect();
    if !missing.is_empty() {
        return data_error(format!(
            "OpenCTP full session response is missing venues: {}",
            missing.join(", ")
        ));
    }
    let products: HashSet<(&str, String)> = records
        .iter()
        .map(|r| (r.venue.as_str(), r.product_id.to_uppercase()))
        .collect();
    let mut missing_cffex: Vec<&str> = CFFEX_PRODUCTS
        .iter()
        .copied()
        .filter(|p| !products.contains(&("CFFEX", (*p).to_string())))
        .collect();
    missing_cffex.sort_unstable();
    if !missing_cffex.is_empty() {
        return data_error(format!(
            "OpenCTP full session response is missing CFFEX products: {}",
            missing_cffex.join(", ")
        ));
    }
    Ok(())
}

pub async fn fetch(url: &str, timeout: u64, retries: u32) -> Result<(Vec<u8>, String, String)> {
    source::fetch(
        url,
        timeout,
        retries,
        &format!("cn-futures-ref/{VERSION}"),
        Some("application/json,*/*;q=0.8"),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(rows: &str) -> Vec<u8> {
        format!("{{\"rsp_code\": 0, \"rsp_message\": \"succeed\", \"data\": [{rows}]}}")
            .into_bytes()
    }

    #[test]
    fn parses_night_and_cross_midnight() {
        let row = r#"{"ExchangeID":"SHFE","ProductID":"sn","SegmentNo":1,"TimeBegin":"21:00:00","TimeEnd":"01:00:00","ProductClass":"1","Area":"cn"}"#;
        let r = parse_payload(&payload(row), "").unwrap();
        assert_eq!(r.len(), 1);
        assert!(r[0].crosses_midnight);
        assert_eq!(r[0].session_group, "NIGHT");
        assert_eq!(
            r[0].raw_json,
            J::s(
                r#"{"Area":"cn","ExchangeID":"SHFE","ProductClass":"1","ProductID":"sn","SegmentNo":1,"TimeBegin":"21:00:00","TimeEnd":"01:00:00"}"#
            )
        );
    }

    #[test]
    fn rejects_changed_cffex_schedule_and_duplicates() {
        let row = |t: &str| {
            format!(
                r#"{{"ExchangeID":"CFFEX","ProductID":"IF","SegmentNo":1,"TimeBegin":"{t}","TimeEnd":"11:30:00","ProductClass":"1","Area":"cn"}}"#
            )
        };
        assert!(parse_payload(&payload(&row("09:31:00")), "").is_err());
        let dup = format!("{},{}", row("09:30:00"), row("09:30:00"));
        let e = parse_payload(&payload(&dup), "").unwrap_err().to_string();
        assert!(e.contains("duplicate OpenCTP session row"), "{e}");
    }

    #[test]
    fn listing_alias_copies_model_only_when_product_missing() {
        let row = |p: &str, b: &str, e: &str, n: u8| {
            format!(
                r#"{{"ExchangeID":"GFEX","ProductID":"{p}","SegmentNo":{n},"TimeBegin":"{b}","TimeEnd":"{e}","ProductClass":"1","Area":"cn"}}"#
            )
        };
        let rows = format!(
            "{},{}",
            row("lc", "09:00:00", "10:15:00", 1),
            row("lc", "10:30:00", "11:30:00", 2)
        );
        let mut r = parse_payload(&payload(&rows), "").unwrap();
        assert_eq!(apply_listing_aliases(&mut r), vec!["GFEX.LI".to_string()]);
        let li: Vec<_> = r.iter().filter(|x| x.product_id == "li").collect();
        assert_eq!(li.len(), 2);
        assert_eq!(li[1].time_begin, "10:30:00");
        let J::Str(raw) = &li[0].raw_json else {
            panic!()
        };
        assert!(
            raw.contains(r#""AliasOf":"LC""#) && raw.contains(r#""ProductID":"li""#),
            "{raw}"
        );
        // already listed by OpenCTP: nothing added
        assert!(apply_listing_aliases(&mut r).is_empty());
    }
}
