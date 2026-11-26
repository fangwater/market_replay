//! Application operations (port of the parts of `services.py` and `api.py`
//! that build responses and run syncs).
use crate::dec::{canonical_decimal, Dec};
use crate::json::J;
use crate::models::{
    data_error, normalize_venue, venue_name, DB_COLUMNS, DEFAULT_SESSION_SOURCE_URL,
    DEFAULT_SOURCE_URL, SESSION_SOURCE_TYPE, SOURCE_TYPE, VERSION,
};
use crate::source::{self, parse_date};
use crate::storage::Db;
use crate::{sessions, storage};
use anyhow::Result;
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

fn non_empty(v: Option<&str>) -> Option<&str> {
    v.filter(|s| !s.is_empty())
}

/// `services.records`: data state plus current rows with overrides applied.
pub async fn records(
    db: &Db,
    as_of: Option<&str>,
    venue: Option<&str>,
    product: Option<&str>,
    instrument: Option<&str>,
    include_options: bool,
) -> Result<(J, Vec<J>)> {
    let mut conn = db.pool.acquire().await?;
    let state = db.data_state_on(&mut conn, as_of).await?;
    let effective: NaiveDate = match non_empty(as_of) {
        Some(a) => parse_date(a, "as-of")?,
        None => parse_date(state.str_of("as_of").unwrap_or_default(), "as-of")?,
    };
    let venue = non_empty(venue).map(normalize_venue);
    let rows = db
        .current_records(
            &mut conn,
            as_of,
            venue.as_deref(),
            non_empty(product),
            non_empty(instrument),
            include_options,
        )
        .await?;
    let overrides = db.overrides_effective(&mut conn, effective).await?;
    let out = rows
        .into_iter()
        .map(|row| apply_overrides(row, &overrides))
        .collect();
    Ok((state, out))
}

fn apply_overrides(mut record: J, overrides: &[J]) -> J {
    let eq = |a: Option<&str>, b: Option<&str>| {
        a.unwrap_or_default().to_uppercase() == b.unwrap_or_default().to_uppercase()
    };
    let mut applied = Vec::new();
    let matching: Vec<&J> = overrides
        .iter()
        .filter(|o| {
            eq(o.str_of("venue"), record.str_of("venue"))
                && eq(o.str_of("product_id"), record.str_of("product_id"))
                && (o.str_of("instrument_id").unwrap_or_default().is_empty()
                    || eq(o.str_of("instrument_id"), record.str_of("instrument_id")))
        })
        .collect();
    for o in matching {
        let field = o.str_of("field_name").unwrap_or_default().to_string();
        let value = o.get("field_value").cloned().unwrap_or(J::Null);
        record.set(&field, value.clone());
        applied.push(J::obj(vec![
            ("field", J::s(field)),
            ("value", value),
            (
                "effectiveFrom",
                o.get("effective_from").cloned().unwrap_or(J::Null),
            ),
            (
                "effectiveTo",
                o.get("effective_to").cloned().unwrap_or(J::Null),
            ),
            ("sourceUrl", o.get("source_url").cloned().unwrap_or(J::Null)),
            ("note", o.get("note").cloned().unwrap_or(J::Null)),
        ]));
    }
    record.set("applied_overrides", J::Arr(applied));
    record
}

pub async fn one_record(
    db: &Db,
    as_of: Option<&str>,
    venue: &str,
    instrument: &str,
) -> Result<(J, J)> {
    let (state, mut rows) = records(db, as_of, Some(venue), None, Some(instrument), true).await?;
    if rows.is_empty() {
        return data_error(format!("instrument not found: {venue}.{instrument}"));
    }
    if rows.len() > 1 {
        return data_error(format!(
            "instrument lookup was not unique: {venue}.{instrument}"
        ));
    }
    Ok((state, rows.remove(0)))
}

fn g(record: &J, key: &str) -> J {
    record.get(key).cloned().unwrap_or(J::Null)
}

/// `api._instrument`.
pub fn instrument_json(r: &J) -> J {
    J::obj(vec![
        ("venue", g(r, "venue")),
        ("instrumentId", g(r, "instrument_id")),
        ("instrumentName", g(r, "instrument_name")),
        ("productId", g(r, "product_id")),
        ("productName", g(r, "product_name")),
        ("instrumentType", g(r, "instrument_type")),
        ("contractMultiplier", g(r, "contract_multiplier")),
        ("priceTick", g(r, "price_tick")),
        ("latestPrice", g(r, "latest_price")),
        ("previousSettlementPrice", g(r, "previous_settlement_price")),
        ("volume", g(r, "volume")),
        ("openInterest", g(r, "open_interest")),
        ("sourceDate", g(r, "source_date")),
        ("startTs", g(r, "start_ts")),
        ("updateTs", g(r, "update_ts")),
        (
            "fees",
            J::obj(vec![
                (
                    "open",
                    J::obj(vec![
                        ("rate", g(r, "open_rate_by_money")),
                        ("perLot", g(r, "open_fee_per_lot")),
                    ]),
                ),
                (
                    "close",
                    J::obj(vec![
                        ("rate", g(r, "close_rate_by_money")),
                        ("perLot", g(r, "close_fee_per_lot")),
                    ]),
                ),
                (
                    "closeToday",
                    J::obj(vec![
                        ("rate", g(r, "close_today_rate_by_money")),
                        ("perLot", g(r, "close_today_fee_per_lot")),
                    ]),
                ),
            ]),
        ),
        (
            "margin",
            J::obj(vec![
                ("longRate", g(r, "long_margin_rate_by_money")),
                ("shortRate", g(r, "short_margin_rate_by_money")),
            ]),
        ),
        (
            "appliedOverrides",
            r.get("applied_overrides")
                .cloned()
                .unwrap_or(J::Arr(vec![])),
        ),
    ])
}

/// `api.overview` (also serves `/exchanges/{venue}`).
pub async fn overview(
    db: &Db,
    as_of: Option<&str>,
    include_options: bool,
    venue: Option<&str>,
    product: Option<&str>,
    search: Option<&str>,
) -> Result<J> {
    let (state, mut rows) = records(db, as_of, venue, product, None, include_options).await?;
    let needle = search.unwrap_or("").trim().to_uppercase();
    if !needle.is_empty() {
        rows.retain(|r| {
            r.str_of("instrument_id")
                .unwrap_or_default()
                .to_uppercase()
                .contains(&needle)
                || r.str_of("product_id")
                    .unwrap_or_default()
                    .to_uppercase()
                    .contains(&needle)
                || r.str_of("instrument_name")
                    .unwrap_or_default()
                    .to_uppercase()
                    .contains(&needle)
        });
    }
    struct Group {
        instruments: Vec<J>,
        products: BTreeSet<String>,
        futures: i64,
        options: i64,
    }
    let mut grouped: BTreeMap<String, Group> = BTreeMap::new();
    let total = rows.len() as i64;
    for r in &rows {
        let code = r.str_of("venue").unwrap_or_default().to_string();
        let g = grouped.entry(code).or_insert(Group {
            instruments: vec![],
            products: BTreeSet::new(),
            futures: 0,
            options: 0,
        });
        g.instruments.push(instrument_json(r));
        g.products
            .insert(r.str_of("product_id").unwrap_or_default().to_string());
        if r.str_of("instrument_type") == Some("OPTION") {
            g.options += 1;
        } else {
            g.futures += 1;
        }
    }
    let futures: i64 = grouped.values().map(|g| g.futures).sum();
    let options: i64 = grouped.values().map(|g| g.options).sum();
    let exchanges: Vec<J> = grouped
        .into_iter()
        .map(|(code, g)| {
            let count = g.instruments.len() as i64;
            J::obj(vec![
                ("code", J::s(code.clone())),
                ("name", J::s(venue_name(&code))),
                ("instruments", J::Arr(g.instruments)),
                (
                    "products",
                    J::Arr(g.products.into_iter().map(J::Str).collect()),
                ),
                ("futures", J::Int(g.futures)),
                ("options", J::Int(g.options)),
                ("count", J::Int(count)),
            ])
        })
        .collect();
    let version_count = state.get("version_count").cloned().unwrap_or(J::Null);
    let exchange_count = exchanges.len() as i64;
    Ok(J::obj(vec![
        ("dataState", state.clone()),
        ("asOf", g(&state, "as_of")),
        ("historicalExact", J::Bool(false)),
        ("feeScope", J::s("BROKER_REFERENCE")),
        ("sourceType", J::s(SOURCE_TYPE)),
        (
            "summary",
            J::obj(vec![
                ("instruments", J::Int(total)),
                ("exchanges", J::Int(exchange_count)),
                ("futures", J::Int(futures)),
                ("options", J::Int(options)),
                ("instrumentVersions", version_count),
            ]),
        ),
        ("exchanges", J::Arr(exchanges)),
    ]))
}

fn dec_product(left: Option<&str>, right: Option<&str>) -> J {
    match (canonical_decimal(left), canonical_decimal(right)) {
        (Some(l), Some(r)) => match (Dec::parse(&l), Dec::parse(&r)) {
            (Some(a), Some(b)) => J::Str(a.mul(&b).plain()),
            _ => J::Null,
        },
        _ => J::Null,
    }
}

/// `services.exchange_info`.
pub fn exchange_info(r: &J) -> J {
    let rate = |effect: &str, rate_field: &str, fixed: &str, reference: &str| {
        J::obj(vec![
            ("positionEffect", J::s(effect)),
            ("liquidityRole", J::s("ANY")),
            ("adValoremRate", g(r, rate_field)),
            ("fixedPerLot", g(r, fixed)),
            ("sourceReferenceOneLotFee", g(r, reference)),
        ])
    };
    J::obj(vec![
        ("venue", g(r, "venue")),
        ("symbol", g(r, "instrument_id")),
        ("productId", g(r, "product_id")),
        ("name", g(r, "instrument_name")),
        ("instrumentType", g(r, "instrument_type")),
        ("contractMultiplier", g(r, "contract_multiplier")),
        (
            "tickValue",
            dec_product(r.str_of("price_tick"), r.str_of("contract_multiplier")),
        ),
        (
            "filters",
            J::Arr(vec![
                J::obj(vec![
                    ("filterType", J::s("PRICE_FILTER")),
                    ("tickSize", g(r, "price_tick")),
                ]),
                J::obj(vec![
                    ("filterType", J::s("LOT_SIZE")),
                    ("stepSize", J::s("1")),
                    ("minQty", J::s("1")),
                    ("maxQty", J::Null),
                    ("status", J::s("OPENCTP_FEE_TABLE_DOES_NOT_SUPPLY_LIMITS")),
                ]),
            ]),
        ),
        (
            "fees",
            J::Arr(vec![
                rate(
                    "OPEN",
                    "open_rate_by_money",
                    "open_fee_per_lot",
                    "calculated_open_fee_one_lot",
                ),
                rate(
                    "CLOSE_YESTERDAY",
                    "close_rate_by_money",
                    "close_fee_per_lot",
                    "calculated_close_fee_one_lot",
                ),
                rate(
                    "CLOSE_TODAY",
                    "close_today_rate_by_money",
                    "close_today_fee_per_lot",
                    "calculated_close_today_fee_one_lot",
                ),
            ]),
        ),
        (
            "margin",
            J::obj(vec![
                ("longRate", g(r, "long_margin_rate_by_money")),
                ("longPerLot", g(r, "long_margin_per_lot")),
                ("shortRate", g(r, "short_margin_rate_by_money")),
                ("shortPerLot", g(r, "short_margin_per_lot")),
            ]),
        ),
        (
            "marketReference",
            J::obj(vec![
                ("previousSettlementPrice", g(r, "previous_settlement_price")),
                ("previousClosePrice", g(r, "previous_close_price")),
                ("latestPrice", g(r, "latest_price")),
            ]),
        ),
        ("sourceUpdatedAt", g(r, "source_updated_at")),
        ("startTs", g(r, "start_ts")),
        ("updateTs", g(r, "update_ts")),
        (
            "appliedOverrides",
            r.get("applied_overrides")
                .cloned()
                .unwrap_or(J::Arr(vec![])),
        ),
    ])
}

pub async fn export_document(
    db: &Db,
    as_of: Option<&str>,
    venue: Option<&str>,
    product: Option<&str>,
    include_options: bool,
) -> Result<J> {
    let (state, rows) = records(db, as_of, venue, product, None, include_options).await?;
    Ok(J::obj(vec![
        ("schema", J::s("cn-futures-exchange-info/v1")),
        ("asOf", g(&state, "as_of")),
        ("dataState", state.clone()),
        ("feeScope", J::s("BROKER_REFERENCE")),
        ("sourceType", J::s(SOURCE_TYPE)),
        ("historicalExact", J::Bool(false)),
        ("makerTakerApplicable", J::Bool(false)),
        ("messageFeeSupported", J::Bool(false)),
        (
            "feeFormula",
            J::s("quantity * fixedPerLot + abs(price * contractMultiplier * quantity) * adValoremRate"),
        ),
        ("symbols", J::Arr(rows.iter().map(exchange_info).collect())),
    ]))
}

fn csv_field(v: &str) -> String {
    if v.is_empty() {
        return String::new();
    }
    if v.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", v.replace('"', "\"\""))
    } else {
        v.to_string()
    }
}

/// `services.records_csv` (without the BOM, which the response adds).
pub fn records_csv(rows: &[J]) -> String {
    let fields: Vec<&str> = DB_COLUMNS
        .iter()
        .copied()
        .filter(|f| *f != "raw_json")
        .chain(["source_date", "start_ts", "update_ts", "applied_overrides"])
        .collect();
    let mut out = String::new();
    out.push_str(
        &fields
            .iter()
            .map(|f| csv_field(f))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push_str("\r\n");
    for r in rows {
        let cells: Vec<String> = fields
            .iter()
            .map(|f| {
                if *f == "applied_overrides" {
                    let v = r
                        .get("applied_overrides")
                        .cloned()
                        .unwrap_or(J::Arr(vec![]));
                    csv_field(&v.dumps(true))
                } else {
                    csv_field(r.str_of(f).unwrap_or(""))
                }
            })
            .collect();
        out.push_str(&cells.join(","));
        out.push_str("\r\n");
    }
    out
}

/// `services.continuous_sessions`.
pub async fn continuous_sessions(
    db: &Db,
    as_of: Option<&str>,
    venue: Option<&str>,
    product: Option<&str>,
) -> Result<(J, Vec<J>)> {
    let mut conn = db.pool.acquire().await?;
    let state = db.continuous_session_state(&mut conn, as_of).await?;
    let venue = non_empty(venue).map(normalize_venue);
    let rows = db
        .current_continuous_sessions(
            &mut conn,
            as_of,
            venue.as_deref(),
            non_empty(product),
            false,
        )
        .await?;
    Ok((state, rows))
}

// ----------------------------------------------------------------- sync

/// `services._observed_at`.
pub fn observed_at(records: &[crate::models::Record], source_date: &str) -> Result<DateTime<Utc>> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"(20\d{2})[-/](\d{2})[-/](\d{2})\s+(\d{2}):(\d{2}):(\d{2})").unwrap()
    });
    for r in records {
        if let Some(m) = re.captures(r.get("source_updated_at").unwrap_or("")) {
            let n = |i: usize| m[i].parse::<u32>().unwrap_or(0);
            if let Some(local) = NaiveDate::from_ymd_opt(m[1].parse()?, n(2), n(3))
                .and_then(|d| d.and_hms_opt(n(4), n(5), n(6)))
            {
                return Ok(Utc.from_utc_datetime(&(local - Duration::hours(8))));
            }
        }
    }
    let d = NaiveDate::parse_from_str(source_date, "%Y-%m-%d")?;
    Ok(Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap()))
}

/// Result of a sync: JSON body and whether new versions were inserted.
pub struct SyncOutcome {
    pub body: J,
    pub inserted: bool,
}

pub async fn sync(
    db: &Db,
    source_url: Option<&str>,
    source_date: Option<&str>,
    timeout: u64,
    retries: u32,
) -> Result<SyncOutcome> {
    let url = non_empty(source_url).unwrap_or(DEFAULT_SOURCE_URL);
    let (content, content_type, resolved) = source::fetch(
        url,
        timeout,
        retries,
        &format!("cn-futures-ref/{VERSION}"),
        None,
    )
    .await?;
    import_html(db, &content, &resolved, source_date, &content_type).await
}

pub async fn import_html(
    db: &Db,
    content: &[u8],
    source_url: &str,
    source_date: Option<&str>,
    content_type: &str,
) -> Result<SyncOutcome> {
    let text = source::decode(content, content_type)?;
    let records = source::parse_html(&text)?;
    let date = source::source_date(&text, &records, source_date)?;
    let observed = observed_at(&records, &date)?;
    let counts = db
        .merge_records(
            &records,
            parse_date(&date, "source date")?,
            observed,
            source_url,
        )
        .await?;
    let state = db.data_state(None).await?;
    Ok(SyncOutcome {
        inserted: counts.inserted > 0,
        body: J::obj(vec![
            ("inserted", J::Int(counts.inserted)),
            ("confirmed", J::Int(counts.confirmed)),
            ("total", J::Int(counts.total)),
            ("status", J::s("synced")),
            ("sourceDate", J::s(date)),
            ("dataState", state),
            ("sourceType", J::s(SOURCE_TYPE)),
        ]),
    })
}

pub async fn sync_continuous_sessions(
    db: &Db,
    source_url: Option<&str>,
    source_date: Option<&str>,
    timeout: u64,
    retries: u32,
) -> Result<SyncOutcome> {
    let url = non_empty(source_url).unwrap_or(DEFAULT_SESSION_SOURCE_URL);
    let (content, content_type, resolved) = sessions::fetch(url, timeout, retries).await?;
    import_continuous_sessions(db, &content, &resolved, source_date, &content_type).await
}

fn query_has(url: &str, keys: &[&str]) -> bool {
    reqwest::Url::parse(url)
        .map(|u| {
            u.query_pairs()
                .any(|(k, v)| keys.contains(&k.as_ref()) && !v.is_empty())
        })
        .unwrap_or(false)
}

pub async fn import_continuous_sessions(
    db: &Db,
    content: &[u8],
    source_url: &str,
    source_date: Option<&str>,
    content_type: &str,
) -> Result<SyncOutcome> {
    let mut rows = sessions::parse_payload(content, content_type)?;
    let filtered = query_has(source_url, &["markets", "products"]);
    if !filtered {
        sessions::validate_full_snapshot(&rows)?;
    }
    sessions::apply_listing_aliases(&mut rows);
    let date = match non_empty(source_date) {
        Some(s) => parse_date(s, "source date")?,
        None => (Utc::now() + Duration::hours(8)).date_naive(),
    };
    let observed = Utc::now().with_nanosecond_zero();
    let counts = db
        .merge_continuous_sessions(&rows, date, observed, source_url, !filtered)
        .await?;
    let mut conn = db.pool.acquire().await?;
    let state = db.continuous_session_state(&mut conn, None).await?;
    Ok(SyncOutcome {
        inserted: counts.inserted > 0,
        body: J::obj(vec![
            ("inserted", J::Int(counts.inserted)),
            ("confirmed", J::Int(counts.confirmed)),
            ("deactivated", J::Int(counts.deactivated)),
            ("total", J::Int(counts.total)),
            ("status", J::s("synced")),
            ("sourceDate", J::s(date.to_string())),
            ("dataState", state),
            ("sourceType", J::s(SESSION_SOURCE_TYPE)),
            ("reconciled", J::Bool(!filtered)),
        ]),
    })
}

trait NanoZero {
    fn with_nanosecond_zero(self) -> Self;
}
impl NanoZero for DateTime<Utc> {
    fn with_nanosecond_zero(self) -> Self {
        use chrono::Timelike;
        self.with_nanosecond(0).unwrap_or(self)
    }
}

/// Alias used by the daily command.
pub use storage::PRODUCTION_SCHEMA;
