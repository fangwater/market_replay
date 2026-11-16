//! Per-RIC IB contract manifest `<state-dir>/ib_contracts.json`.
//!
//! The source of the catalog's `ib_contracts` table (`lseg_catalog ib-sync`
//! or `build` import it). It records where each RIC's `kline_ib.parquet`
//! lives and how the exchange directory was chosen; `tas_data = false`
//! marks IB-only contracts (no TAS for the RIC or its aliases, exchange taken
//! from the product) which carry only `kline_ib.parquet`.
use crate::catalog::Basis;
use crate::ib_minute::Job;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const FILE: &str = "ib_contracts.json";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Entry {
    pub ric: String,
    pub con_id: String,
    pub ib_symbol: String,
    pub ib_local_symbol: String,
    pub ib_exchange: String,
    pub maturity_date: String,
    pub product: String,
    /// Delivery exchange directory (`lseg/<exchange>/...`).
    pub exchange: String,
    /// `ric`, `alias:<RIC>` or `product`.
    pub exchange_basis: String,
    /// Whether TAS data exists for this RIC (or an alias); false = IB only.
    pub tas_data: bool,
    /// First / last UTC day with a published `kline_ib.parquet`
    /// (`YYYYMMDD`, the directory name); empty when unknown.
    pub first_day: String,
    pub last_day: String,
}

pub fn basis_label(b: &Basis) -> String {
    match b {
        Basis::Ric => "ric".into(),
        Basis::Alias(a) => format!("alias:{a}"),
        Basis::Product => "product".into(),
    }
}

impl Entry {
    pub fn new(job: &Job, exchange: &str, basis: &Basis) -> Self {
        let product = job
            .extra
            .get("tas_product")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Self {
            ric: job.ric.clone(),
            con_id: job.con_id.clone(),
            ib_symbol: job.symbol.clone(),
            ib_local_symbol: job.local_symbol.clone(),
            ib_exchange: job.exchange.clone(),
            maturity_date: job.maturity_date.clone(),
            product,
            exchange: exchange.into(),
            exchange_basis: basis_label(basis),
            tas_data: *basis != Basis::Product,
            first_day: compact_day(&job.ib_first_utc),
            last_day: compact_day(&job.ib_last_utc),
        }
    }
}

/// `YYYY-MM-DD ...` -> `YYYYMMDD` (empty stays empty).
fn compact_day(utc: &str) -> String {
    utc.get(..10)
        .map(|d| d.replace('-', ""))
        .unwrap_or_default()
}

fn path(state_dir: &Path) -> PathBuf {
    state_dir.join(FILE)
}

pub fn load(state_dir: &Path) -> Result<BTreeMap<String, Entry>> {
    let p = path(state_dir);
    if !p.exists() {
        return Ok(BTreeMap::new());
    }
    let list: Vec<Entry> =
        serde_json::from_slice(&fs::read(&p)?).with_context(|| format!("parse {}", p.display()))?;
    Ok(list.into_iter().map(|e| (e.ric.clone(), e)).collect())
}

fn min_nonempty(a: &str, b: &str) -> String {
    match (a.is_empty(), b.is_empty()) {
        (true, _) => b.into(),
        (_, true) => a.into(),
        _ => a.min(b).into(),
    }
}

/// Merges `entries` into the manifest: identity fields are replaced, the
/// observed UTC range only widens. Written via temp + fsync + rename.
pub fn merge(state_dir: &Path, entries: impl IntoIterator<Item = Entry>) -> Result<usize> {
    let mut all = load(state_dir)?;
    for mut e in entries {
        if let Some(old) = all.get(&e.ric) {
            e.first_day = min_nonempty(&old.first_day, &e.first_day);
            e.last_day = old.last_day.clone().max(e.last_day);
        }
        all.insert(e.ric.clone(), e);
    }
    fs::create_dir_all(state_dir)?;
    let p = path(state_dir);
    let tmp = p.with_extension("json.partial");
    let mut f = fs::File::create(&tmp)?;
    serde_json::to_writer_pretty(&mut f, &all.values().collect::<Vec<_>>())?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    fs::rename(&tmp, &p)?;
    Ok(all.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(first: &str, last: &str, basis: &str) -> Entry {
        Entry {
            ric: "ESZ26".into(),
            con_id: "1".into(),
            ib_symbol: "ES".into(),
            ib_local_symbol: "ESZ6".into(),
            ib_exchange: "CME".into(),
            maturity_date: "20261218".into(),
            product: "ES".into(),
            exchange: "cme".into(),
            exchange_basis: basis.into(),
            tas_data: basis != "product",
            first_day: first.into(),
            last_day: last.into(),
        }
    }

    #[test]
    fn merge_widens_range_and_replaces_identity() {
        let t = tempfile::tempdir().unwrap();
        merge(t.path(), [entry("20251201", "20261103", "ric")]).unwrap();
        merge(t.path(), [entry("", "20261116", "product")]).unwrap();
        let m = load(t.path()).unwrap();
        let e = &m["ESZ26"];
        assert_eq!(e.first_day, "20251201");
        assert_eq!(e.last_day, "20261116");
        assert_eq!(compact_day("2026-07-14 16:44:00"), "20260714");
        assert_eq!(e.exchange_basis, "product");
        assert!(!e.tas_data);
    }
}
