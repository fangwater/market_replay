//! Contract choices are computed at the service boundary from published EOD
//! observations. Effective choices never use the effective day's OI or volume.
use crate::dominant::{parse_file, DominantRow, EXCHANGES};
use anyhow::{ensure, Context, Result};
use chrono::NaiveDate;
use cn_futures_roll::instrument::delivery_yyyymm;
use cn_market_proto::{
    ContractSelection, ContractSelectionMode as Mode, Exchange, GetContractSelectionsRequest,
    GetContractSelectionsResponse, SelectedContract,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::SystemTime,
};

pub const RULE_VERSION: &str = "previous_close_contract_selection/v1";

pub fn exchange(code: &str) -> Result<Exchange> {
    Ok(match code {
        "ccfx" => Exchange::Cffex,
        "xsge" => Exchange::Shfe,
        "xsie" => Exchange::Ine,
        "xzce" => Exchange::Czce,
        "xdce" => Exchange::Dce,
        "xgfe" => Exchange::Gfex,
        _ => anyhow::bail!("unknown exchange {code}"),
    })
}

fn date(day: u32) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(&day.to_string(), "%Y%m%d").context("invalid trading day")
}
fn day(text: &str) -> Result<u32> {
    let d = NaiveDate::parse_from_str(text, "%Y-%m-%d")?;
    Ok(d.format("%Y%m%d").to_string().parse()?)
}

pub fn validate(req: &GetContractSelectionsRequest) -> Result<Mode> {
    Exchange::try_from(req.exchange).context("unknown exchange")?;
    let mode = Mode::try_from(req.mode).context("unknown selection mode")?;
    ensure!(mode != Mode::Unspecified, "selection mode is required");
    ensure!(req.products.len() <= 200, "at most 200 products");
    for p in &req.products {
        ensure!(
            !p.is_empty() && p.bytes().all(|b| b.is_ascii_alphabetic()),
            "invalid product {p:?}"
        );
    }
    if req.start_trading_day != 0 || req.end_trading_day != 0 {
        let a = date(req.start_trading_day)?;
        let b = date(req.end_trading_day)?;
        ensure!(
            (0..=31).contains(&(b - a).num_days()),
            "range must be ordered and span at most 31 days"
        );
    }
    Ok(mode)
}

#[derive(Clone, Debug)]
struct Candidate {
    contract: String,
    oi: Option<f64>,
    volume: Option<f64>,
}
fn metric(x: Option<f64>) -> f64 {
    x.unwrap_or(-1.0)
}
fn cmp(a: &Candidate, b: &Candidate, volume: bool) -> Ordering {
    let (a1, a2, b1, b2) = if volume {
        (a.volume, a.oi, b.volume, b.oi)
    } else {
        (a.oi, a.volume, b.oi, b.volume)
    };
    metric(a1)
        .total_cmp(&metric(b1))
        .then_with(|| metric(a2).total_cmp(&metric(b2)))
        .then_with(|| a.contract.cmp(&b.contract))
}
fn choose(mode: Mode, anchor: &str, source_day: u32, candidates: &[Candidate]) -> SelectedContract {
    let anchor = anchor.to_ascii_lowercase();
    let missing = |reason: &str| SelectedContract {
        ranking_source_day: source_day,
        unavailable_reason: reason.into(),
        ..Default::default()
    };
    if mode == Mode::Dominant {
        if anchor.is_empty() {
            return missing("dominant_not_assigned");
        }
        let c = candidates.iter().find(|c| c.contract == anchor);
        return SelectedContract {
            contract: anchor,
            ranking_source_day: source_day,
            open_interest: c.and_then(|c| c.oi),
            volume_lots: c.and_then(|c| c.volume),
            unavailable_reason: String::new(),
        };
    }
    if source_day == 0 {
        return missing("no_previous_close");
    }
    if mode != Mode::VolumeSecond && anchor.is_empty() {
        return missing("dominant_not_assigned");
    }
    let anchor_month = delivery_yyyymm(&anchor);
    let mut ranked: Vec<_> = candidates
        .iter()
        .filter(|c| {
            if mode == Mode::VolumeSecond {
                return c.volume.is_some();
            }
            if c.contract == anchor || c.oi.is_none() {
                return false;
            }
            match mode {
                Mode::FarSecondaryOi => delivery_yyyymm(&c.contract)
                    .zip(anchor_month)
                    .is_some_and(|(a, b)| a > b),
                Mode::NearSecondaryOi => delivery_yyyymm(&c.contract)
                    .zip(anchor_month)
                    .is_some_and(|(a, b)| a < b),
                _ => true,
            }
        })
        .collect();
    ranked.sort_by(|a, b| cmp(b, a, mode == Mode::VolumeSecond));
    let Some(c) = ranked.get(usize::from(mode == Mode::VolumeSecond)) else {
        return missing("no_eligible_contract");
    };
    SelectedContract {
        contract: c.contract.clone(),
        ranking_source_day: source_day,
        open_interest: c.oi,
        volume_lots: c.volume,
        unavailable_reason: String::new(),
    }
}

type Stamp = Option<(u64, SystemTime)>;
fn stamp(path: &Path) -> Result<Stamp> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(Some((m.len(), m.modified()?))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Bounded by one exchange day's contracts and the requested response range.
/// No mutation or data-dependent choice based on the current day's prices.
pub fn query(
    root: &Path,
    req: GetContractSelectionsRequest,
) -> Result<GetContractSelectionsResponse> {
    let mode = validate(&req)?;
    let wanted: BTreeSet<_> = req
        .products
        .iter()
        .map(|p| p.to_ascii_uppercase())
        .collect();
    let mut paths: Vec<(String, PathBuf, PathBuf)> = Vec::new();
    for ex in EXCHANGES {
        if req.exchange != 0 && exchange(ex)? as i32 != req.exchange {
            continue;
        }
        paths.push((
            ex.into(),
            root.join(format!("{ex}_dominant.csv")),
            root.join(format!("{ex}_eod.csv")),
        ));
    }
    let stamps = paths
        .iter()
        .map(|(_, a, b)| Ok((stamp(a)?, stamp(b)?)))
        .collect::<Result<Vec<_>>>()?;
    let mut rows = Vec::new();
    let mut found = BTreeSet::new();
    for (ex, dominant_path, eod_path) in &paths {
        if !dominant_path.exists() {
            continue;
        }
        let all = parse_file(dominant_path, ex)?;
        let latest: BTreeMap<_, _> = all
            .iter()
            .map(|r| (r.product.as_str(), r.trad_day.as_str()))
            .collect();
        let mut requested: BTreeMap<u32, Vec<&DominantRow>> = BTreeMap::new();
        for r in &all {
            if !wanted.is_empty() && !wanted.contains(&r.product) {
                continue;
            }
            found.insert(r.product.clone());
            let d = day(&r.trad_day)?;
            let include = if req.start_trading_day == 0 {
                latest[&r.product.as_str()] == r.trad_day
            } else {
                req.start_trading_day <= d && d <= req.end_trading_day
            };
            if include {
                requested.entry(d).or_default().push(r);
            }
        }
        if requested.is_empty() {
            continue;
        }
        let mut reader = csv::Reader::from_path(eod_path)
            .with_context(|| format!("open published EOD {}", eod_path.display()))?;
        let headers = reader.headers()?.clone();
        let col = |n| {
            headers
                .iter()
                .position(|h| h == n)
                .with_context(|| format!("missing EOD {n}"))
        };
        let (td, p, c, oi, vol, expired) = (
            col("trad_day")?,
            col("product_id")?,
            col("instrument_id")?,
            col("open_int")?,
            col("volume")?,
            col("expired")?,
        );
        let mut current_day = 0;
        let mut previous_day = 0;
        let mut current: BTreeMap<String, Vec<Candidate>> = BTreeMap::new();
        let mut previous = BTreeMap::new();
        let mut completed = BTreeSet::new();
        let max_day = *requested.last_key_value().unwrap().0;
        let mut flush = |d: u32,
                         before: u32,
                         prior: &BTreeMap<String, Vec<Candidate>>,
                         today: &BTreeMap<String, Vec<Candidate>>|
         -> Result<()> {
            if let Some(items) = requested.get(&d) {
                for r in items {
                    rows.push(ContractSelection {
                        exchange: exchange(ex)? as i32,
                        product: r.product.clone(),
                        trading_day: d,
                        dominant: r.instrument_id.to_ascii_lowercase(),
                        selected: Some(choose(
                            mode,
                            &r.instrument_id,
                            before,
                            prior.get(&r.product).map(Vec::as_slice).unwrap_or(&[]),
                        )),
                        scheduled_next: Some(choose(
                            mode,
                            &r.scheduled_next,
                            d,
                            today.get(&r.product).map(Vec::as_slice).unwrap_or(&[]),
                        )),
                        dominant_rule: r.rule.clone(),
                    });
                }
                completed.insert(d);
            }
            Ok(())
        };
        for record in reader.records() {
            let record = record?;
            let d = day(&record[td])?;
            ensure!(d >= current_day, "EOD rows are not sorted by trading day");
            if d != current_day {
                if current_day != 0 {
                    flush(current_day, previous_day, &previous, &current)?;
                }
                previous_day = current_day;
                previous = std::mem::take(&mut current);
                current_day = d;
                if d > max_day {
                    break;
                }
            }
            let product = record[p].to_ascii_uppercase();
            if (!wanted.is_empty() && !wanted.contains(&product)) || record[expired] == *"1" {
                continue;
            }
            let contract = record[c].to_ascii_lowercase();
            if delivery_yyyymm(&contract).is_none() {
                continue;
            }
            let number = |i: usize| {
                record[i]
                    .parse::<f64>()
                    .ok()
                    .filter(|v| v.is_finite() && *v >= 0.0)
            };
            current.entry(product).or_default().push(Candidate {
                contract,
                oi: number(oi),
                volume: number(vol),
            });
        }
        if current_day <= max_day {
            flush(current_day, previous_day, &previous, &current)?;
        }
        ensure!(
            requested.keys().all(|d| completed.contains(d)),
            "published dominant/EOD trading days disagree for {ex}"
        );
    }
    ensure!(
        wanted.is_subset(&found),
        "unknown requested products: {:?}",
        wanted.difference(&found).collect::<Vec<_>>()
    );
    for ((_, a, b), before) in paths.iter().zip(stamps) {
        ensure!(
            (stamp(a)?, stamp(b)?) == before,
            "published selection sources changed during request; retry"
        );
    }
    rows.sort_by(|a, b| (a.trading_day, &a.product).cmp(&(b.trading_day, &b.product)));
    let mut out = GetContractSelectionsResponse {
        mode: mode as i32,
        rows,
        rule_version: RULE_VERSION.into(),
        snapshot_id: String::new(),
    };
    out.snapshot_id = format!("{:x}", Sha256::digest(out.encode_to_vec()));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn c(contract: &str, oi: f64, volume: f64) -> Candidate {
        Candidate {
            contract: contract.into(),
            oi: Some(oi),
            volume: Some(volume),
        }
    }
    #[test]
    fn ranks_by_previous_oi_with_strict_delivery_filters_and_independent_volume_rank() {
        let rows = vec![
            c("rb2612", 900., 40.),
            c("rb2701", 800., 60.),
            c("rb2705", 700., 100.),
            c("rb2710", 700., 80.),
        ];
        let pick = |mode| choose(mode, "rb2701", 20261113, &rows).contract;
        assert_eq!(pick(Mode::Dominant), "rb2701");
        assert_eq!(pick(Mode::SecondaryOi), "rb2612");
        assert_eq!(pick(Mode::FarSecondaryOi), "rb2705");
        assert_eq!(pick(Mode::NearSecondaryOi), "rb2612");
        assert_eq!(pick(Mode::VolumeSecond), "rb2710");
        assert!(choose(Mode::NearSecondaryOi, "rb2612", 20261113, &rows)
            .contract
            .is_empty());
        assert_eq!(
            choose(Mode::VolumeSecond, "rb2701", 20261113, &rows[..2]).contract,
            "rb2612"
        );
    }

    #[test]
    fn ties_missing_metrics_and_volume_rank_do_not_inherit_main_hysteresis() {
        let rows = vec![
            c("rb2701", 100., 90.),
            c("rb2705", 100., 100.),
            c("rb2710", 100., 100.),
        ];
        assert_eq!(
            choose(Mode::SecondaryOi, "rb2701", 20261112, &rows).contract,
            "rb2710"
        );
        assert_eq!(
            choose(Mode::VolumeSecond, "rb2701", 20261112, &rows[..2]).contract,
            "rb2701"
        );
        let missing = vec![Candidate {
            contract: "rb2705".into(),
            oi: None,
            volume: Some(20.),
        }];
        assert!(choose(Mode::SecondaryOi, "rb2701", 20261112, &missing)
            .contract
            .is_empty());
        assert!(choose(Mode::VolumeSecond, "rb2701", 20261112, &missing)
            .contract
            .is_empty());
        assert!(choose(Mode::FarSecondaryOi, "rb2710", 20261112, &rows)
            .contract
            .is_empty());
    }
    #[test]
    fn effective_selection_ignores_same_day_volume_and_weekend_and_next_are_explicit() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("xsge_dominant.csv"),
            "trad_day,product_id,instrument_id,roll_flag,decision_open_int,scheduled_next,rule\n2026-11-13,RB,rb2701,0,100,rb2701,local_rqdata_rule0\n2026-11-16,RB,rb2701,0,100,rb2705,local_rqdata_rule0\n").unwrap();
        std::fs::write(dir.path().join("xsge_eod.csv"),
            "trad_day,product_id,instrument_id,open_int,volume,close,expired\n2026-11-13,RB,rb2612,500,20,3000,0\n2026-11-13,RB,rb2701,800,50,3000,0\n2026-11-13,RB,rb2705,400,30,3000,0\n2026-11-16,RB,rb2612,10,1,3000,0\n2026-11-16,RB,rb2701,100,2,3000,0\n2026-11-16,RB,rb2705,900,9999,3000,0\n").unwrap();
        let out = query(
            dir.path(),
            GetContractSelectionsRequest {
                exchange: Exchange::Shfe as i32,
                products: vec!["rb".into()],
                mode: Mode::SecondaryOi as i32,
                start_trading_day: 20261116,
                end_trading_day: 20261116,
            },
        )
        .unwrap();
        assert_eq!(out.rows.len(), 1);
        let r = &out.rows[0];
        assert_eq!(r.selected.as_ref().unwrap().contract, "rb2612");
        assert_eq!(r.selected.as_ref().unwrap().ranking_source_day, 20261113);
        assert_eq!(r.scheduled_next.as_ref().unwrap().contract, "rb2701");
        assert_eq!(
            r.scheduled_next.as_ref().unwrap().ranking_source_day,
            20261116
        );
        assert_eq!(out.snapshot_id.len(), 64);
    }
}
