use crate::config::Config;
use anyhow::{ensure, Context, Result};
use chrono::{NaiveTime, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
};

pub const SCALE: i128 = 1_000_000;
pub fn money(value: f64) -> i128 {
    (value * SCALE as f64).round() as i128
}
pub fn cny(value: i128) -> f64 {
    value as f64 / SCALE as f64
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Fee {
    pub rate: f64,
    pub per_lot: f64,
}
impl Fee {
    pub fn calculate(&self, notional: f64, lots: i64) -> i128 {
        money(notional.abs() * self.rate + lots.abs() as f64 * self.per_lot)
    }
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Fees {
    pub open: Fee,
    pub close: Fee,
    pub close_today: Fee,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Window {
    pub begin: u32,
    pub end: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Spec {
    pub product: String,
    pub exchange: String,
    pub venue: String,
    pub multiplier: f64,
    pub tick: f64,
    pub fees: Option<Fees>,
    pub fee_source: String,
    pub windows: Vec<Window>,
}
impl Spec {
    pub fn ticks(&self, p: f64) -> Option<i64> {
        if !p.is_finite() || p <= 0.0 || p >= 1e20 {
            return None;
        }
        let n = (p / self.tick).round();
        if (p - n * self.tick).abs() > self.tick * 1e-5 {
            return None;
        }
        Some(n as i64)
    }
    pub fn price(&self, ticks: i64) -> f64 {
        ticks as f64 * self.tick
    }
    /// Continuous-session end, exclusive; based on the actual local calendar date.
    pub fn segment_end(&self, ts: i64) -> Option<i64> {
        let local = ts + 8 * 3600 * 1_000_000;
        let day = local.div_euclid(86_400_000_000);
        let sec = local.rem_euclid(86_400_000_000) / 1_000_000;
        self.windows.iter().find_map(|w| {
            let (b, e) = (i64::from(w.begin), i64::from(w.end));
            let end_day = if b < e {
                if !(b <= sec && sec < e) {
                    return None;
                }
                day
            } else if sec >= b {
                day + 1
            } else if sec < e {
                day
            } else {
                return None;
            };
            Some((end_day * 86400 + e - 8 * 3600) * 1_000_000)
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub reference_as_of: String,
    pub historical_exact: bool,
    pub products: BTreeMap<String, Spec>,
    pub contracts: BTreeMap<String, Spec>,
    pub provenance: Value,
}
fn number(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str()?.parse().ok())
        .filter(|x| x.is_finite())
}
fn fee(v: &Value) -> Option<Fee> {
    Some(Fee {
        rate: number(v.get("rate")?)?,
        per_lot: number(v.get("perLot")?)?,
    })
}
fn fees(v: &Value) -> Option<Fees> {
    Some(Fees {
        open: fee(v.get("open")?)?,
        close: fee(v.get("close")?)?,
        close_today: fee(v.get("closeToday")?)?,
    })
}
fn clock(s: &str) -> Result<u32> {
    Ok(NaiveTime::parse_from_str(s, "%H:%M:%S")?.num_seconds_from_midnight())
}

impl Metadata {
    pub fn load(cfg: &Config) -> Result<Self> {
        if let Some(path) = &cfg.metadata_file {
            let m: Self = serde_json::from_reader(std::fs::File::open(path)?)?;
            m.validate()?;
            if cfg.fee_policy == "account_snapshot" {
                ensure!(
                    m.provenance["fee_policy"] == "account_snapshot"
                        && m.provenance["account_rates"]["accountKey"].as_str()
                            == cfg.fee_account.as_deref(),
                    "frozen metadata does not contain the requested account fee source"
                );
                let actual = chrono::DateTime::parse_from_rfc3339(
                    m.provenance["account_rates"]["asOf"]
                        .as_str()
                        .context("frozen account metadata omitted asOf")?,
                )?;
                let requested = chrono::DateTime::parse_from_rfc3339(
                    cfg.fee_as_of
                        .as_deref()
                        .context("account_snapshot requires fee_as_of")?,
                )?;
                ensure!(
                    actual == requested,
                    "frozen account metadata has another fee_as_of"
                );
            }
            return Ok(m);
        }
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let overview: Value = client
            .get(format!("{}/overview", cfg.reference_api))
            .query(&[("asOf", &cfg.reference_as_of)])
            .send()?
            .error_for_status()?
            .json()?;
        let sessions: Value = client
            .get(format!("{}/trading-sessions", cfg.reference_api))
            .query(&[("asOf", &cfg.reference_as_of)])
            .send()?
            .error_for_status()?
            .json()?;
        let account_snapshot: Option<Value> = if cfg.fee_policy == "account_snapshot" {
            Some(
                client
                    .get(format!("{}/account-rates", cfg.reference_api))
                    .query(&[
                        (
                            "account",
                            cfg.fee_account
                                .as_deref()
                                .context("account_snapshot requires fee_account")?,
                        ),
                        (
                            "asOf",
                            cfg.fee_as_of
                                .as_deref()
                                .context("account_snapshot requires fee_as_of")?,
                        ),
                    ])
                    .send()?
                    .error_for_status()?
                    .json()?,
            )
        } else {
            None
        };
        let mut account_rates = BTreeMap::new();
        if let Some(snapshot) = &account_snapshot {
            ensure!(
                snapshot["sourceType"] == "CTP_ACCOUNT_QUERY"
                    && snapshot["accountKey"].as_str() == cfg.fee_account.as_deref(),
                "account rate response has the wrong source or account"
            );
            for row in snapshot["rates"]
                .as_array()
                .context("missing account rate rows")?
            {
                let venue = row["venue"].as_str().context("account rate venue")?;
                let instrument = row["instrumentId"]
                    .as_str()
                    .context("account rate instrument")?;
                ensure!(
                    account_rates
                        .insert((venue.to_string(), instrument.to_string()), row)
                        .is_none(),
                    "duplicate account rate contract"
                );
            }
        }
        let mut windows: BTreeMap<String, Vec<Window>> = BTreeMap::new();
        for r in sessions["sessions"]
            .as_array()
            .context("reference sessions missing")?
        {
            let p = r["product_id"]
                .as_str()
                .context("session product")?
                .to_ascii_uppercase();
            windows.entry(p).or_default().push(Window {
                begin: clock(r["time_begin"].as_str().context("session begin")?)?,
                end: clock(r["time_end"].as_str().context("session end")?)?,
            });
        }
        let output=Command::new("/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql")
            .args(["-X","-h","/mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/run","-p","5433","-U","u171","-d","market_metadata","-A","-t","-F",",","-c","SELECT product,exchange,volume_multiple,verified FROM public.domestic_future_product_multipliers"])
            .env("PGOPTIONS","-c default_transaction_read_only=on").output().context("read verified multiplier catalog")?;
        ensure!(
            output.status.success(),
            "verified multiplier query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut multipliers = BTreeMap::new();
        for line in std::str::from_utf8(&output.stdout)?.lines() {
            let p: Vec<_> = line.split(',').collect();
            ensure!(p.len() == 4, "invalid multiplier row");
            if p[3] == "t" {
                multipliers.insert(
                    p[0].to_ascii_uppercase(),
                    (p[1].to_string(), p[2].parse::<f64>()?),
                );
            }
        }
        let mut contracts = BTreeMap::new();
        let mut variants: BTreeMap<String, Vec<Spec>> = BTreeMap::new();
        for ex in overview["exchanges"]
            .as_array()
            .context("reference overview missing exchanges")?
        {
            let venue = ex["code"]
                .as_str()
                .or_else(|| ex["venue"].as_str())
                .unwrap_or("");
            for r in ex["instruments"]
                .as_array()
                .context("reference instruments missing")?
            {
                if r["instrumentType"]
                    .as_str()
                    .is_some_and(|x| x != "FUTURE" && x != "FUTURES")
                {
                    continue;
                }
                let contract = r["instrumentId"].as_str().context("reference contract")?;
                if !contract.bytes().all(|x| x.is_ascii_alphanumeric()) {
                    continue;
                }
                let product = crate::product(contract);
                let Some((exchange, multiplier)) = multipliers.get(&product) else {
                    continue;
                };
                let Some(tick) = number(&r["priceTick"]).filter(|x| *x > 0.0) else {
                    continue;
                };
                let Some(w) = windows.get(&product) else {
                    continue;
                };
                let spec = Spec {
                    product: product.clone(),
                    exchange: exchange.clone(),
                    venue: venue.into(),
                    multiplier: *multiplier,
                    tick,
                    fees: match cfg.fee_policy.as_str() {
                        "zero" => Some(Fees::default()),
                        "unknown" => None,
                        "account_snapshot" => account_rates
                            .get(&(venue.to_string(), contract.to_string()))
                            .and_then(|row| fees(&row["fees"])),
                        _ => fees(&r["fees"]),
                    },
                    fee_source: if cfg.fee_policy == "account_snapshot" {
                        let row = account_rates.get(&(venue.to_string(), contract.to_string()));
                        format!(
                            "account_snapshot:{}:{}",
                            cfg.fee_account.as_deref().unwrap_or_default(),
                            row.and_then(|r| r["observedAt"].as_str())
                                .unwrap_or("missing")
                        )
                    } else {
                        format!(
                            "{}:{}",
                            cfg.fee_policy,
                            r["sourceDate"].as_str().unwrap_or("unknown")
                        )
                    },
                    windows: w.clone(),
                };
                let canonical = mdl_replay::line::canonical_contract(
                    contract.as_bytes(),
                    crate::day_number(&cfg.reference_as_of)?,
                )?;
                contracts.insert(canonical, spec.clone());
                variants.entry(product).or_default().push(spec);
            }
        }
        let mut products = BTreeMap::new();
        for (p, rows) in variants {
            let mut s = rows[0].clone();
            if !rows.iter().all(|x| x.tick == s.tick) {
                continue;
            }
            if !rows.iter().all(|x| x.fees == s.fees) {
                s.fees = None;
                s.fee_source = "ambiguous_product_fees".into();
            }
            if cfg.fee_policy == "account_snapshot" {
                s.fees = None;
                s.fee_source = format!(
                    "account_snapshot:{}:contract_required",
                    cfg.fee_account.as_deref().unwrap_or_default()
                );
            }
            products.insert(p, s);
        }
        let m = Self {
            reference_as_of: cfg.reference_as_of.clone(),
            historical_exact: false,
            products,
            contracts,
            provenance: serde_json::json!({"overview":overview,"sessions":sessions,"multiplier_source":"market_metadata.public.domestic_future_product_multipliers","fee_policy":cfg.fee_policy,"account_rates":account_snapshot}),
        };
        m.validate()?;
        Ok(m)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.products.is_empty(), "metadata has no products");
        for s in self.products.values().chain(self.contracts.values()) {
            ensure!(
                s.tick.is_finite()
                    && s.tick > 0.0
                    && s.multiplier.is_finite()
                    && s.multiplier > 0.0
                    && !s.windows.is_empty(),
                "invalid metadata {}",
                s.product
            );
            if let Some(f) = &s.fees {
                for f in [&f.open, &f.close, &f.close_today] {
                    ensure!(
                        f.rate.is_finite()
                            && f.rate >= 0.0
                            && f.per_lot.is_finite()
                            && f.per_lot >= 0.0,
                        "invalid fee"
                    );
                }
            }
        }
        Ok(())
    }
    pub fn spec(&self, c: &str) -> Result<Spec> {
        let spec = self
            .contracts
            .get(&c.to_ascii_lowercase())
            .or_else(|| self.products.get(&crate::product(c)))
            .cloned()
            .with_context(|| format!("missing verified multiplier/tick/session for {c}"))?;
        ensure!(!spec.fee_source.starts_with("account_snapshot:") || spec.fees.is_some(), "missing account-specific fee observation for {c}; choose an observed contract or another explicit fee policy");
        Ok(spec)
    }
}

#[derive(Clone, Debug, Default)]
pub struct Dominants {
    pub days: BTreeMap<u32, BTreeMap<String, String>>,
    pub hashes: BTreeMap<String, String>,
}
impl Dominants {
    pub fn load(dir: &Path, products: &[String]) -> Result<Self> {
        let wanted: BTreeSet<_> = products.iter().map(|x| x.to_ascii_uppercase()).collect();
        let mut out = Self::default();
        for ex in ["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"] {
            let path = dir.join(format!("{ex}_dominant.csv"));
            if !path.exists() {
                continue;
            }
            out.hashes
                .insert(path.display().to_string(), crate::sha256(&path)?);
            let mut reader = csv::Reader::from_path(&path)?;
            let h = reader.headers()?.clone();
            let col = |n: &str| {
                h.iter()
                    .position(|x| x == n)
                    .with_context(|| format!("missing {n}"))
            };
            let (d, p, c) = (col("trad_day")?, col("product_id")?, col("instrument_id")?);
            for r in reader.records() {
                let r = r?;
                let product = r[p].to_ascii_uppercase();
                if (!wanted.is_empty() && !wanted.contains(&product)) || r[c].is_empty() {
                    continue;
                }
                out.days
                    .entry(crate::day_number(&r[d])?)
                    .or_default()
                    .insert(product, r[c].to_ascii_lowercase());
            }
        }
        ensure!(
            !out.days.is_empty(),
            "no dominant history in {}",
            dir.display()
        );
        Ok(out)
    }
}

#[cfg(test)]
mod account_tests {
    use super::*;
    fn metadata() -> Metadata {
        let mut s = Spec {
            product: "SN".into(),
            exchange: "xsge".into(),
            venue: "SHFE".into(),
            multiplier: 1.,
            tick: 10.,
            fees: Some(Fees::default()),
            fee_source: "account_snapshot:test_account:2026-11-17T02:35:56Z".into(),
            windows: vec![Window {
                begin: 9 * 3600,
                end: 15 * 3600,
            }],
        };
        let contracts = BTreeMap::from([("sn2612".into(), s.clone())]);
        s.fees = None;
        s.fee_source = "account_snapshot:test_account:contract_required".into();
        Metadata {
            reference_as_of: "2026-11-17".into(),
            historical_exact: false,
            products: BTreeMap::from([("SN".into(), s)]),
            contracts,
            provenance: serde_json::json!({"fee_policy":"reference_snapshot"}),
        }
    }
    #[test]
    fn account_fees_cannot_fall_back_to_another_delivery_month() {
        let m = metadata();
        assert!(m.spec("sn2612").unwrap().fees.is_some());
        assert!(m.spec("sn2701").is_err());
    }
    #[test]
    fn selecting_account_fees_rejects_frozen_openctp_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("metadata.json");
        serde_json::to_writer(std::fs::File::create(&path).unwrap(), &metadata()).unwrap();
        let cfg = Config {
            fee_policy: "account_snapshot".into(),
            fee_account: Some("test_account".into()),
            fee_as_of: Some("2026-11-17T04:00:00Z".into()),
            metadata_file: Some(path),
            ..Config::default()
        };
        assert!(Metadata::load(&cfg)
            .unwrap_err()
            .to_string()
            .contains("requested account fee source"));
    }
}
