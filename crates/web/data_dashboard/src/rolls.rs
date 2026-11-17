//! 换月: CN dominant-contract rolls, from the dominant API the dashboard
//! already reads (`/api/v1/dominant`, plus `/api/v1/dominant/{product}` for the
//! few products that rolled on the newest trading day). Read-only, nothing new is accessed.
//!
//! Semantics (docs/cn_daily_and_kline.md): `dominant` is the contract in
//! effect on `trading_day` (the newest processed trading day); `last_roll_day`
//! is the trading day it took effect; `scheduled_next` is what the close of
//! `trading_day` assigns to the next trading day (`rolls_next_day`).
use crate::search::Market;
use crate::AppState;
use chrono::NaiveDate;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

/// Calendar days of history requested for the previous contract of a product
/// that rolled on the newest trading day (covers weekends and holidays).
const HISTORY_CALENDAR_DAYS: i64 = 20;

/// `(trad_day, instrument_id)` ascending.
pub type History = Vec<(String, String)>;

fn nonempty(v: &Value) -> Option<&str> {
    v.as_str().filter(|s| !s.is_empty())
}

fn parse_history(v: &Value) -> History {
    v["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| {
            Some((
                nonempty(&r["trad_day"])?.to_string(),
                nonempty(&r["instrument_id"])?.to_string(),
            ))
        })
        .collect()
}

/// The contract in effect on the trading day before `day`.
fn previous_contract(h: &History, day: &str) -> Option<String> {
    h.iter()
        .rev()
        .find(|(d, _)| d.as_str() < day)
        .map(|(_, c)| c.clone())
}

/// Pure assembly of the section from the dominant list and the fetched
/// histories. `name_cn` resolves a product code to its Chinese name.
pub fn build(
    dominant: &Value,
    histories: &BTreeMap<String, History>,
    today: NaiveDate,
    name_cn: &dyn Fn(&str) -> Option<String>,
) -> Value {
    let trading_day = dominant["latest_trading_day"].as_str().unwrap_or("");
    let mut rows: Vec<Value> = Vec::new();
    for p in dominant["products"].as_array().into_iter().flatten() {
        let Some(product) = nonempty(&p["product"]) else {
            continue;
        };
        let current = nonempty(&p["dominant"]).unwrap_or("");
        let last_roll = nonempty(&p["last_roll_day"]);
        let rolled_today = (last_roll == Some(trading_day) && !trading_day.is_empty()).then(|| {
            let from = histories
                .get(product)
                .and_then(|h| previous_contract(h, trading_day));
            json!({"from": from, "to": current})
        });
        let rolling_next = (p["rolls_next_day"] == true)
            .then(|| json!({"from": current, "to": nonempty(&p["scheduled_next"])}));
        let days_since = last_roll
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
            .map(|d| (today - d).num_days());
        let exchange = nonempty(&p["exchange"]).unwrap_or("");
        rows.push(json!({
            "product": product,
            "name_cn": name_cn(product),
            "exchange": exchange,
            "exchange_cn": crate::names::cn_exchange_name(exchange),
            "dominant": current,
            "rolled_today": rolled_today,
            "rolling_next": rolling_next,
            "last_roll_day": last_roll,
            "days_since": days_since,
        }));
    }
    // Today's and tomorrow's rolls first, then the most recent rolls.
    rows.sort_by(|a, b| {
        let key = |r: &Value| {
            (
                r["rolled_today"].is_null(),
                r["rolling_next"].is_null(),
                std::cmp::Reverse(r["last_roll_day"].as_str().unwrap_or("").to_string()),
                r["product"].as_str().unwrap_or("").to_string(),
            )
        };
        key(a).cmp(&key(b))
    });
    json!({
        "trading_day": trading_day,
        "today": today.to_string(),
        "rows": rows,
    })
}

/// Fetches the histories it needs and publishes the `rolls` section.
pub async fn refresh(st: &Arc<AppState>) {
    let started = Instant::now();
    let Some(dominant) = st.dominant.read().await.clone() else {
        st.set("rolls", started, Err("主力合约接口不可用".into()))
            .await;
        return;
    };
    let products = dominant["products"].as_array().cloned().unwrap_or_default();
    let trading_day = dominant["latest_trading_day"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let today = chrono::Utc::now()
        .with_timezone(&chrono_tz::Asia::Shanghai)
        .date_naive();
    let base = st.cfg.cn.dominant_api.trim_end_matches('/').to_string();
    let end = NaiveDate::parse_from_str(&trading_day, "%Y-%m-%d").ok();
    // Only products that rolled on the newest trading day need their history
    // (for the previous contract); the rest of the table needs no extra request.
    let mut histories: BTreeMap<String, History> = BTreeMap::new();
    if let Some(end) = end {
        let start = end - chrono::Duration::days(HISTORY_CALENDAR_DAYS);
        for p in &products {
            let Some(code) = nonempty(&p["product"]) else {
                continue;
            };
            if nonempty(&p["last_roll_day"]) != Some(trading_day.as_str()) {
                continue;
            }
            let url = format!("{base}/api/v1/dominant/{code}?start={start}&end={end}");
            // A failed fetch only leaves the old contract of that row unknown.
            if let Ok(v) = st.get_json(&url).await {
                histories.insert(code.to_string(), parse_history(&v));
            }
        }
    }
    let index = st.index.read().await.clone();
    let name_cn = move |code: &str| {
        index
            .as_ref()
            .and_then(|ix| ix.get(Market::Cn, code))
            .and_then(|p| p.name_cn.clone())
    };
    let v = build(&dominant, &histories, today, &name_cn);
    st.set("rolls", started, Ok(v)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hist(rows: &[(&str, &str)]) -> History {
        rows.iter()
            .map(|(d, c)| (d.to_string(), c.to_string()))
            .collect()
    }

    fn dominant() -> Value {
        json!({
            "latest_trading_day": "2026-11-16",
            "products": [
                {"product": "A", "exchange": "xdce", "dominant": "a2701", "last_roll_day": "2026-10-14",
                 "rolls_next_day": false, "scheduled_next": "a2701"},
                {"product": "PG", "exchange": "xdce", "dominant": "pg2701", "last_roll_day": "2026-11-16",
                 "rolls_next_day": false, "scheduled_next": "pg2701"},
                {"product": "AU", "exchange": "xsge", "dominant": "au2612", "last_roll_day": "2026-10-20",
                 "rolls_next_day": true, "scheduled_next": "au2702"},
                {"product": "NEW", "exchange": "xzce", "dominant": "new2701", "last_roll_day": null,
                 "rolls_next_day": false, "scheduled_next": "new2701"},
            ]
        })
    }

    fn today() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 11, 17).unwrap()
    }

    #[test]
    fn today_and_tomorrow_rolls_sort_first_with_old_to_new() {
        let mut hs = BTreeMap::new();
        hs.insert(
            "PG".to_string(),
            hist(&[("2026-11-13", "pg2612"), ("2026-11-16", "pg2701")]),
        );
        let v = build(&dominant(), &hs, today(), &|c| {
            (c == "PG").then(|| "液化石油气".to_string())
        });
        let rows = v["rows"].as_array().unwrap();
        assert_eq!(rows[0]["product"], "PG", "today's roll first");
        assert_eq!(
            rows[0]["rolled_today"],
            json!({"from": "pg2612", "to": "pg2701"})
        );
        assert_eq!(rows[0]["name_cn"], "液化石油气");
        assert_eq!(rows[1]["product"], "AU", "tomorrow's scheduled roll second");
        assert_eq!(
            rows[1]["rolling_next"],
            json!({"from": "au2612", "to": "au2702"})
        );
        assert_eq!(
            rows[2]["product"], "A",
            "then the most recent last_roll_day"
        );
        assert_eq!(rows[2]["days_since"], 34);
        assert_eq!(rows[3]["product"], "NEW");
        assert!(rows[3]["last_roll_day"].is_null() && rows[3]["days_since"].is_null());
    }

    #[test]
    fn missing_history_keeps_the_table_with_the_old_contract_unknown() {
        let v = build(&dominant(), &BTreeMap::new(), today(), &|_| None);
        assert_eq!(v["rows"].as_array().unwrap().len(), 4);
        assert!(
            v["rows"][0]["rolled_today"]["from"].is_null(),
            "old contract unknown, new is known"
        );
        assert_eq!(v["rows"][0]["rolled_today"]["to"], "pg2701");
    }
}
