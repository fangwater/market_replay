//! Investor-specific CTP observations. Public OpenCTP records stay independent.
use crate::{
    dec::canonical_decimal,
    json::{parse_ordered, J},
    models::DataError,
    storage::Db,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, Row};
use std::collections::BTreeSet;

pub const SOURCE: &str = "CTP_ACCOUNT_QUERY";
const MAX_ROWS: usize = 10_000;
const DDL: &str = "
CREATE TABLE IF NOT EXISTS {S}.account_rate_accounts (
 account_key TEXT PRIMARY KEY, broker TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS {S}.account_rate_observations (
 account_key TEXT NOT NULL REFERENCES {S}.account_rate_accounts(account_key),
 venue TEXT NOT NULL, instrument_id TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL,
 trading_day DATE NOT NULL, payload JSONB NOT NULL, record_hash TEXT NOT NULL,
 receipt_sha256 TEXT NOT NULL, imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(account_key, venue, instrument_id, observed_at)
);";

fn invalid(message: impl Into<String>) -> anyhow::Error {
    DataError(message.into()).into()
}
fn label(value: &str, name: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')),
        "invalid {name}"
    );
    Ok(())
}
fn decimal(value: &Value, positive: bool) -> Result<String> {
    let text = match value {
        Value::String(v) => v.clone(),
        Value::Number(n) => n.to_string(),
        _ => return Err(invalid("rate field must be a finite decimal")),
    };
    ensure!(text.len() <= 64, "oversized decimal");
    let n: f64 = text.parse().context("invalid account-rate decimal")?;
    ensure!(
        n.is_finite() && if positive { n > 0.0 } else { n >= 0.0 },
        "invalid account-rate decimal"
    );
    canonical_decimal(Some(&text)).context("invalid decimal encoding")
}
fn venue_symbol(venue: Option<&str>, instrument: Option<&str>) -> Result<()> {
    if let Some(v) = venue {
        ensure!(
            ["SHFE", "INE", "DCE", "CZCE", "CFFEX", "GFEX"].contains(&v),
            "invalid venue"
        );
    }
    if let Some(s) = instrument {
        ensure!(
            !s.is_empty() && s.len() <= 63 && s.bytes().all(|c| c.is_ascii_alphanumeric()),
            "invalid instrument"
        );
    }
    Ok(())
}
fn timestamp(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|v| v.with_timezone(&Utc))
        .map_err(|_| invalid("asOf/start/end require an RFC3339 timestamp with timezone"))
}

pub struct Observation {
    pub venue: String,
    pub instrument: String,
    pub observed_at: DateTime<Utc>,
    pub trading_day: NaiveDate,
    pub payload: Value,
}
/// Imports the query receipts already produced by the external CTP observer.
/// Funds, credentials and local risk budgets are deliberately outside this dataset.
pub fn parse_receipt(bytes: &[u8], account: &str, broker: &str) -> Result<Vec<Observation>> {
    label(account, "account key")?;
    label(broker, "broker")?;
    ensure!(bytes.len() <= 16 * 1024 * 1024, "account receipt too large");
    let document: Value = serde_json::from_slice(bytes).context("decode CTP query receipt")?;
    ensure!(
        document["success"] == true
            && document["read_only"] == true
            && document["process_exit"] == 0,
        "receipt must be a successfully completed read-only query"
    );
    ensure!(
        document["source"]
            .as_str()
            .is_some_and(|s| s.starts_with("live investor-specific CTP query")),
        "receipt source is not an investor-specific CTP query"
    );
    let observed = timestamp(
        document["queried_utc"]
            .as_str()
            .context("receipt omitted query timestamp")?,
    )?;
    ensure!(
        observed <= Utc::now() + chrono::Duration::seconds(60),
        "query timestamp is in the future"
    );
    let day = NaiveDate::parse_from_str(
        document["trading_day"]
            .as_str()
            .context("receipt omitted trading day")?,
        "%Y%m%d",
    )?;
    let rules: Vec<&Value> = match (document.get("rules"), document.get("rule")) {
        (Some(Value::Array(rows)), None) => rows.iter().collect(),
        (None, Some(row)) if row.is_object() => vec![row],
        _ => {
            return Err(invalid(
                "receipt must contain one unambiguous rule or rules array",
            ))
        }
    };
    ensure!(
        !rules.is_empty() && rules.len() <= MAX_ROWS,
        "invalid receipt rule count"
    );
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for r in rules {
        let venue = r["venue"].as_str().context("rule omitted venue")?;
        let instrument = r["symbol"].as_str().context("rule omitted symbol")?;
        venue_symbol(Some(venue), Some(instrument))?;
        ensure!(
            seen.insert((venue, instrument)),
            "duplicate contract in account receipt"
        );
        let product: String = instrument
            .chars()
            .take_while(char::is_ascii_alphabetic)
            .collect();
        ensure!(
            !product.is_empty()
                && product.len() < instrument.len()
                && instrument[product.len()..]
                    .bytes()
                    .all(|b| b.is_ascii_digit()),
            "rule is not a native futures contract"
        );
        let multiple = r["contract_multiple"]
            .as_i64()
            .filter(|m| *m > 0)
            .context("invalid contract multiplier")?;
        let tick = decimal(&r["price_tick"], true)?;
        let commission = &r["commission"];
        let margin = &r["margin"];
        let pair = |offset: &str| -> Result<Value> {
            Ok(
                json!({"rate":decimal(&commission[format!("{offset}_by_money")],false)?,"perLot":decimal(&commission[format!("{offset}_by_volume")],false)?}),
            )
        };
        let fees =
            json!({"open":pair("open")?,"close":pair("close")?,"closeToday":pair("close_today")?});
        let margins = json!({"longRate":decimal(&margin["long_by_money"],false)?,"longPerLot":decimal(&margin["long_by_volume"],false)?,"shortRate":decimal(&margin["short_by_money"],false)?,"shortPerLot":decimal(&margin["short_by_volume"],false)?});
        let active = r["active"]
            .as_bool()
            .context("rule omitted active status")?;
        let payload = json!({"sourceType":SOURCE,"accountKey":account,"broker":broker,"venue":venue,"instrumentId":instrument,"productId":product.to_ascii_uppercase(),"observedAt":observed.to_rfc3339(),"tradingDay":day.format("%Y%m%d").to_string(),"contractMultiplier":multiple.to_string(),"priceTick":tick,"active":active,"fees":fees,"margin":margins});
        output.push(Observation {
            venue: venue.into(),
            instrument: instrument.into(),
            observed_at: observed,
            trading_day: day,
            payload,
        });
    }
    Ok(output)
}

fn sql(db: &Db, text: &str) -> String {
    text.replace("{S}", &db.schema)
}
pub async fn import(db: &Db, bytes: &[u8], account: &str, broker: &str) -> Result<J> {
    let rows = parse_receipt(bytes, account, broker)?;
    let ddl = sql(db, DDL);
    sqlx::raw_sql(AssertSqlSafe(ddl.as_str()))
        .execute(&db.pool)
        .await?;
    let mut tx = db.pool.begin().await?;
    let query=sql(db,"INSERT INTO {S}.account_rate_accounts(account_key,broker) VALUES($1,$2) ON CONFLICT DO NOTHING");
    sqlx::query(AssertSqlSafe(query.as_str()))
        .bind(account)
        .bind(broker)
        .execute(&mut *tx)
        .await?;
    let query = sql(
        db,
        "SELECT broker FROM {S}.account_rate_accounts WHERE account_key=$1 FOR UPDATE",
    );
    let existing: String = sqlx::query_scalar(AssertSqlSafe(query.as_str()))
        .bind(account)
        .fetch_one(&mut *tx)
        .await?;
    ensure!(
        existing == broker,
        "account alias is already bound to another broker"
    );
    let receipt_hash = format!("{:x}", Sha256::digest(bytes));
    let mut inserted = 0;
    for row in &rows {
        let payload = serde_json::to_string(&row.payload)?;
        let hash = format!("{:x}", Sha256::digest(payload.as_bytes()));
        let query=sql(db,"INSERT INTO {S}.account_rate_observations(account_key,venue,instrument_id,observed_at,trading_day,payload,record_hash,receipt_sha256) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8) ON CONFLICT DO NOTHING");
        let n = sqlx::query(AssertSqlSafe(query.as_str()))
            .bind(account)
            .bind(&row.venue)
            .bind(&row.instrument)
            .bind(row.observed_at)
            .bind(row.trading_day)
            .bind(payload)
            .bind(&hash)
            .bind(&receipt_hash)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n == 0 {
            let query=sql(db,"SELECT record_hash FROM {S}.account_rate_observations WHERE account_key=$1 AND venue=$2 AND instrument_id=$3 AND observed_at=$4");
            let stored: String = sqlx::query_scalar(AssertSqlSafe(query.as_str()))
                .bind(account)
                .bind(&row.venue)
                .bind(&row.instrument)
                .bind(row.observed_at)
                .fetch_one(&mut *tx)
                .await?;
            ensure!(
                stored == hash,
                "conflicting account rates at the same observation timestamp; original retained"
            );
        }
        inserted += n;
    }
    tx.commit().await?;
    Ok(J::obj(vec![
        ("sourceType", J::s(SOURCE)),
        ("accountKey", J::s(account)),
        ("inserted", J::Int(inserted as i64)),
        ("confirmed", J::Int(rows.len() as i64 - inserted as i64)),
        ("receiptSha256", J::s(receipt_hash)),
    ]))
}

pub struct Query<'a> {
    pub account: &'a str,
    pub venue: Option<&'a str>,
    pub instrument: Option<&'a str>,
    pub as_of: Option<&'a str>,
    pub start: Option<&'a str>,
    pub end: Option<&'a str>,
}
pub async fn query(db: &Db, request: Query<'_>) -> Result<J> {
    label(request.account, "account key").map_err(|e| invalid(e.to_string()))?;
    venue_symbol(request.venue, request.instrument).map_err(|e| invalid(e.to_string()))?;
    let as_of = request
        .as_of
        .map(timestamp)
        .transpose()?
        .unwrap_or_else(Utc::now);
    let history = request.start.is_some() || request.end.is_some();
    if history && request.as_of.is_some() {
        return Err(invalid("asOf cannot be combined with history start/end"));
    }
    let (start, end) = if history {
        let start = timestamp(
            request
                .start
                .ok_or_else(|| invalid("history requires start and end"))?,
        )?;
        let end = timestamp(
            request
                .end
                .ok_or_else(|| invalid("history requires start and end"))?,
        )?;
        if start > end || end - start > chrono::Duration::days(366) {
            return Err(invalid(
                "history range must be ordered and at most 366 days",
            ));
        }
        (Some(start), Some(end))
    } else {
        (None, None)
    };
    let present: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(format!("{}.account_rate_observations", db.schema))
        .fetch_one(&db.pool)
        .await?;
    let rows = if present.is_none() {
        Vec::new()
    } else {
        let query = sql(
            db,
            if history {
                "SELECT payload::text AS payload,receipt_sha256,record_hash FROM {S}.account_rate_observations WHERE account_key=$1 AND ($2::text IS NULL OR venue=$2) AND ($3::text IS NULL OR instrument_id=$3) AND observed_at>=$4 AND observed_at<=$5 ORDER BY observed_at,venue,instrument_id LIMIT 10001"
            } else {
                "SELECT DISTINCT ON(venue,instrument_id) payload::text AS payload,receipt_sha256,record_hash FROM {S}.account_rate_observations WHERE account_key=$1 AND ($2::text IS NULL OR venue=$2) AND ($3::text IS NULL OR instrument_id=$3) AND observed_at<=$4 ORDER BY venue,instrument_id,observed_at DESC LIMIT 10001"
            },
        );
        let query = sqlx::query(AssertSqlSafe(query.as_str()))
            .bind(request.account)
            .bind(request.venue)
            .bind(request.instrument);
        if history {
            query.bind(start).bind(end).fetch_all(&db.pool).await?
        } else {
            query.bind(as_of).fetch_all(&db.pool).await?
        }
    };
    ensure!(
        rows.len() <= MAX_ROWS,
        "account-rate result exceeds 10000 records; narrow the query"
    );
    let mut rates = Vec::new();
    for row in rows {
        let mut payload = parse_ordered(row.try_get::<&str, _>("payload")?)?;
        payload.set(
            "receiptSha256",
            J::s(row.try_get::<String, _>("receipt_sha256")?),
        );
        payload.set("recordHash", J::s(row.try_get::<String, _>("record_hash")?));
        rates.push(payload);
    }
    Ok(J::obj(vec![
        ("sourceType", J::s(SOURCE)),
        ("accountKey", J::s(request.account)),
        (
            "asOf",
            if history {
                J::Null
            } else {
                J::s(as_of.to_rfc3339())
            },
        ),
        ("mode", J::s(if history { "observations" } else { "as_of" })),
        ("count", J::Int(rates.len() as i64)),
        ("rates", J::Arr(rates)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn receipt(at: &str) -> Value {
        json!({"success":true,"read_only":true,"process_exit":0,"source":"live investor-specific CTP query on sh_dev","queried_utc":at,"trading_day":"20261117","account":{"balance":500000,"user":"must_not_store"},"rule":{"venue":"SHFE","symbol":"sn2612","active":true,"contract_multiple":1,"price_tick":10,"cash_budget_per_open_lot":250000,"commission":{"open_by_money":0.00000006,"open_by_volume":3.03,"close_by_money":0.00000006,"close_by_volume":3.03,"close_today_by_money":0.00000006,"close_today_by_volume":3.03},"margin":{"long_by_money":0.2,"long_by_volume":0,"short_by_money":0.2,"short_by_volume":0}}})
    }
    #[test]
    fn original_units_and_observation_time_are_preserved_without_funds() {
        let r = receipt("2026-11-17T02:35:56.861457Z");
        let rows = parse_receipt(
            &serde_json::to_vec(&r).unwrap(),
            "example_account",
            "example_broker",
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let p = &rows[0].payload;
        assert_eq!(p["fees"]["open"]["rate"], "0.00000006");
        assert_eq!(p["fees"]["open"]["perLot"], "3.03");
        assert_eq!(p["margin"]["longRate"], "0.2");
        assert_eq!(p["observedAt"], "2026-11-17T02:35:56.861457+00:00");
        assert_eq!(p["tradingDay"], "20261117");
        assert!(p.get("account").is_none());
        assert!(p.get("cash_budget_per_open_lot").is_none());
    }
    #[test]
    fn incomplete_failed_ambiguous_and_negative_receipts_are_rejected() {
        let r = receipt("2026-11-17T02:35:56Z");
        let check = |r: &Value| {
            parse_receipt(
                &serde_json::to_vec(r).unwrap(),
                "example_account",
                "example_broker",
            )
            .is_err()
        };
        let mut v = r.clone();
        v["success"] = json!(false);
        assert!(check(&v));
        let mut v = r.clone();
        v["rule"]["margin"] = Value::Null;
        assert!(check(&v));
        let mut v = r.clone();
        v["rule"]["commission"]["open_by_money"] = json!(-0.1);
        assert!(check(&v));
        let mut v = r.clone();
        v["rules"] = json!([v["rule"].clone()]);
        assert!(check(&v));
        let mut v = r.clone();
        v["rules"] = json!([v["rule"].clone(), v["rule"].clone()]);
        v.as_object_mut().unwrap().remove("rule");
        assert!(check(&v));
        let mut v = r.clone();
        v["queried_utc"] = json!("2026-11-17");
        assert!(check(&v));
        assert!(parse_receipt(&serde_json::to_vec(&r).unwrap(), "bad/account", "broker").is_err());
    }
    #[tokio::test]
    #[ignore = "requires explicit CN_ACCOUNT_RATES_TEST_DATABASE_URL pointing at local metadata PostgreSQL"]
    async fn postgres_asof_is_account_scoped_idempotent_atomic_and_has_no_future_fallback() {
        let url = std::env::var("CN_ACCOUNT_RATES_TEST_DATABASE_URL").unwrap();
        let schema = format!(
            "cn_acct_rates_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_micros()
        );
        let db = Db::connect(&url, &schema).await.unwrap();
        let first = receipt("2026-11-17T00:00:00Z");
        let mut second = receipt("2026-11-17T02:00:00Z");
        second["rule"]["commission"]["open_by_volume"] = json!(4.03);
        let bytes = serde_json::to_vec(&first).unwrap();
        let imported = import(&db, &bytes, "test_account", "test_broker")
            .await
            .unwrap();
        assert_eq!(imported.get("inserted"), Some(&J::Int(1)));
        let duplicate = import(&db, &bytes, "test_account", "test_broker")
            .await
            .unwrap();
        assert_eq!(duplicate.get("confirmed"), Some(&J::Int(1)));
        import(
            &db,
            &serde_json::to_vec(&second).unwrap(),
            "test_account",
            "test_broker",
        )
        .await
        .unwrap();
        for (account, at, count, fee) in [
            ("test_account", "2026-11-16T23:59:59Z", 0, ""),
            ("other_account", "2026-11-17T03:00:00Z", 0, ""),
            ("test_account", "2026-11-17T01:00:00Z", 1, "3.03"),
            ("test_account", "2026-11-17T03:00:00Z", 1, "4.03"),
        ] {
            let out = query(
                &db,
                Query {
                    account,
                    venue: Some("SHFE"),
                    instrument: Some("sn2612"),
                    as_of: Some(at),
                    start: None,
                    end: None,
                },
            )
            .await
            .unwrap();
            let out: Value = serde_json::from_str(&out.dumps(false)).unwrap();
            assert_eq!(out["count"], count);
            if count == 1 {
                assert_eq!(out["rates"][0]["fees"]["open"]["perLot"], fee);
            }
        }
        let mut changed = first;
        changed["rule"]["commission"]["open_by_volume"] = json!(999);
        assert!(import(
            &db,
            &serde_json::to_vec(&changed).unwrap(),
            "test_account",
            "test_broker"
        )
        .await
        .is_err());
        assert!(import(&db, &bytes, "test_account", "different_broker")
            .await
            .is_err());
        let out = query(
            &db,
            Query {
                account: "test_account",
                venue: None,
                instrument: None,
                as_of: None,
                start: Some("2026-11-17T00:00:00Z"),
                end: Some("2026-11-17T03:00:00Z"),
            },
        )
        .await
        .unwrap();
        assert_eq!(out.get("count"), Some(&J::Int(2)));
        let drop = format!("DROP SCHEMA {schema} CASCADE");
        sqlx::raw_sql(AssertSqlSafe(drop.as_str()))
            .execute(&db.pool)
            .await
            .unwrap();
        db.pool.close().await;
    }
}
