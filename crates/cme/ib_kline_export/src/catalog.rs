//! Read-only view of `lseg/meta/catalog.sqlite`: where each RIC's TAS data is
//! filed (exchange directory) and in which source years LSEG observed it.
use crate::{ib_minute::Job, mapping::Observed};
use anyhow::{Context, Result};
use sqlx::{sqlite::SqliteConnectOptions, Connection, Row, SqliteConnection};
use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
};

/// Not a market: RICs pending identity review. Used only when the RIC itself
/// is filed there, never as a product-level fallback.
pub const REVIEW: &str = "review";

#[derive(Default)]
pub struct Catalog {
    pub by_ric: HashMap<String, BTreeSet<String>>,
    pub by_product: HashMap<String, BTreeSet<String>>,
    pub observed: Observed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Basis {
    Ric,
    Alias(String),
    Product,
}

impl Catalog {
    pub fn load(path: &Path) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let options = SqliteConnectOptions::new()
                .filename(path)
                .read_only(true)
                .immutable(true);
            let mut conn = SqliteConnection::connect_with(&options)
                .await
                .with_context(|| format!("open catalog {}", path.display()))?;
            let rows = sqlx::query("SELECT source_year, product, ric, exchange FROM contracts")
                .fetch_all(&mut conn)
                .await?;
            let mut c = Catalog::default();
            for r in rows {
                let year: i64 = r.try_get(0)?;
                let product: String = r.try_get(1)?;
                let ric: String = r.try_get(2)?;
                let exchange: String = r.try_get(3)?;
                c.by_ric
                    .entry(ric.clone())
                    .or_default()
                    .insert(exchange.clone());
                c.by_product.entry(product).or_default().insert(exchange);
                c.observed.entry(ric).or_default().insert(year as i32);
            }
            conn.close().await?;
            anyhow::ensure!(
                !c.by_ric.is_empty(),
                "catalog has no contracts: {}",
                path.display()
            );
            Ok(c)
        })
    }

    /// RIC itself, then its HRD-verified TAS aliases, then the TAS product
    /// (excluding `review`). Must be unique at the first level that has data.
    pub fn exchange(&self, job: &Job) -> Result<(String, Basis), String> {
        let unique = |set: &BTreeSet<String>| -> Result<String, String> {
            match set.len() {
                1 => Ok(set.iter().next().unwrap().clone()),
                _ => Err(format!(
                    "ambiguous_exchange:{}",
                    set.iter().cloned().collect::<Vec<_>>().join("|")
                )),
            }
        };
        if let Some(set) = self.by_ric.get(&job.ric) {
            return unique(set).map(|e| (e, Basis::Ric));
        }
        let aliases: Vec<String> = job
            .extra
            .get("tas_aliases")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let mut set = BTreeSet::new();
        let mut first = None;
        for a in &aliases {
            if let Some(s) = self.by_ric.get(a) {
                set.extend(s.iter().cloned());
                first.get_or_insert(a.clone());
            }
        }
        if let Some(a) = first {
            return unique(&set).map(|e| (e, Basis::Alias(a)));
        }
        let product = job.extra.get("tas_product").and_then(|v| v.as_str());
        if let Some(set) = product.and_then(|p| self.by_product.get(p)) {
            let markets: BTreeSet<String> = set.iter().filter(|e| *e != REVIEW).cloned().collect();
            if !markets.is_empty() {
                return unique(&markets).map(|e| (e, Basis::Product));
            }
        }
        Err(format!(
            "no_tas_directory_for_ric_aliases_or_product:{}",
            product.unwrap_or("")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn job(ric: &str, aliases: &[&str], product: &str) -> Job {
        let mut extra = std::collections::BTreeMap::new();
        extra.insert("tas_aliases".into(), serde_json::json!(aliases));
        extra.insert("tas_product".into(), serde_json::json!(product));
        Job {
            ric: ric.into(),
            con_id: "1".into(),
            symbol: String::new(),
            exchange: String::new(),
            maturity_date: String::new(),
            local_symbol: String::new(),
            currency: String::new(),
            multiplier: String::new(),
            ib_first_utc: String::new(),
            ib_last_utc: String::new(),
            cutoff_id: 0,
            extra,
        }
    }
    #[test]
    fn exchange_resolution_order() {
        let mut c = Catalog::default();
        let mut add = |ric: &str, product: &str, ex: &str| {
            c.by_ric.entry(ric.into()).or_default().insert(ex.into());
            c.by_product
                .entry(product.into())
                .or_default()
                .insert(ex.into());
        };
        add("ESZ26", "ES", "cme");
        add("ESX99", "ES", REVIEW);
        add("NKZ6", "NK", "cme");
        add("CFI2Z6", "CFI2", "ice_endex");
        add("CFI2H7", "CFI2", "ice_europe");
        add("RSX26", "RS", REVIEW);
        assert_eq!(
            c.exchange(&job("ESZ26", &["ESZ26"], "ES")),
            Ok(("cme".into(), Basis::Ric))
        );
        assert_eq!(
            c.exchange(&job("RSX26", &["RSX26"], "RS")),
            Ok((REVIEW.into(), Basis::Ric))
        );
        assert_eq!(
            c.exchange(&job("NKZ26", &["NKZ26", "NKZ6"], "NK")),
            Ok(("cme".into(), Basis::Alias("NKZ6".into())))
        );
        // Product fallback ignores review.
        assert_eq!(
            c.exchange(&job("ESH30", &["ESH30"], "ES")),
            Ok(("cme".into(), Basis::Product))
        );
        assert!(c
            .exchange(&job("CFI2M7", &["CFI2M7"], "CFI2"))
            .unwrap_err()
            .starts_with("ambiguous_exchange"));
        assert!(c
            .exchange(&job("RSF27", &["RSF27"], "RS"))
            .unwrap_err()
            .starts_with("no_tas_directory"));
    }
}
