//! In-memory snapshot of `lseg/meta/catalog.sqlite` (≈40k contract rows).
//!
//! The file is replaced by rename when the delivery job refreshes it, so each
//! load opens the current inode with `read_only + immutable` (no locks, no
//! journal, no writes on NFS) and closes it right after the queries. Reloaded
//! only when the file's mtime changes.
use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::{sqlite::SqliteConnectOptions, Connection, Row, SqliteConnection};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize)]
pub struct Contract {
    pub source_year: i64,
    pub product: String,
    pub ric: String,
    pub exchange: String,
    pub first_trade_utc: Option<String>,
    pub last_trade_utc: Option<String>,
    pub last_quote_utc: Option<String>,
    pub last_market_date: Option<String>,
    pub last_session_state: String,
    pub expired_at_utc: Option<String>,
    pub data_directory: Option<String>,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct IbContract {
    pub ric: String,
    pub con_id: String,
    pub ib_symbol: String,
    pub ib_local_symbol: String,
    pub ib_exchange: String,
    pub maturity_date: String,
    pub product: String,
    pub exchange: String,
    pub exchange_basis: String,
    pub tas_data: bool,
    pub first_day: Option<String>,
    pub last_day: Option<String>,
}

#[derive(Default)]
pub struct Catalog {
    pub mtime: Option<SystemTime>,
    pub loaded_at: String,
    pub contracts: Vec<Contract>,
    /// product -> contract indices
    pub by_product: HashMap<String, Vec<usize>>,
    /// upper-case RIC -> contract indices (several source years)
    pub by_ric: HashMap<String, Vec<usize>>,
    /// (ric, source_year) -> [(utc_year, directory)]
    pub dirs: HashMap<(String, i64), Vec<(i64, String)>>,
    pub ib: Vec<IbContract>,
    /// upper-case RIC and IB local symbol -> ib index
    pub ib_by_code: HashMap<String, usize>,
    pub ib_dirs: HashMap<String, Vec<(i64, String)>>,
}

impl Catalog {
    pub async fn load(path: &Path, mtime: Option<SystemTime>) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .immutable(true);
        let mut conn = SqliteConnection::connect_with(&options)
            .await
            .with_context(|| format!("open catalog {}", path.display()))?;
        let mut c = Catalog {
            mtime,
            loaded_at: crate::util::now_utc(),
            ..Default::default()
        };
        let active: HashSet<(i64, String, String)> =
            sqlx::query("SELECT source_year, product, ric FROM active_contracts")
                .fetch_all(&mut conn)
                .await?
                .into_iter()
                .filter_map(|r| Some((r.try_get(0).ok()?, r.try_get(1).ok()?, r.try_get(2).ok()?)))
                .collect();
        let rows = sqlx::query(
            "SELECT source_year, product, ric, exchange, first_trade_utc, last_trade_utc, \
             last_quote_utc, last_market_date, last_session_state, expired_at_utc, data_directory \
             FROM contracts ORDER BY product, source_year, ric",
        )
        .fetch_all(&mut conn)
        .await?;
        for r in rows {
            let k = Contract {
                source_year: r.try_get(0)?,
                product: r.try_get(1)?,
                ric: r.try_get(2)?,
                exchange: r.try_get(3)?,
                first_trade_utc: r.try_get(4)?,
                last_trade_utc: r.try_get(5)?,
                last_quote_utc: r.try_get(6)?,
                last_market_date: r.try_get(7)?,
                last_session_state: r.try_get(8)?,
                expired_at_utc: r.try_get(9)?,
                data_directory: r.try_get(10)?,
                active: false,
            };
            let mut k = k;
            k.active = active.contains(&(k.source_year, k.product.clone(), k.ric.clone()));
            let i = c.contracts.len();
            c.by_product.entry(k.product.clone()).or_default().push(i);
            c.by_ric
                .entry(k.ric.to_ascii_uppercase())
                .or_default()
                .push(i);
            c.contracts.push(k);
        }
        for r in sqlx::query("SELECT source_year, ric, utc_year, directory FROM directories")
            .fetch_all(&mut conn)
            .await?
        {
            let year: i64 = r.try_get(0)?;
            let ric: String = r.try_get(1)?;
            c.dirs
                .entry((ric, year))
                .or_default()
                .push((r.try_get(2)?, r.try_get(3)?));
        }
        for v in c.dirs.values_mut() {
            v.sort();
        }
        for r in sqlx::query(
            "SELECT ric, con_id, ib_symbol, ib_local_symbol, ib_exchange, maturity_date, product, \
             exchange, exchange_basis, tas_data, first_day, last_day FROM ib_contracts \
             ORDER BY ib_symbol, maturity_date",
        )
        .fetch_all(&mut conn)
        .await?
        {
            let tas: i64 = r.try_get(9)?;
            let k = IbContract {
                ric: r.try_get(0)?,
                con_id: r.try_get(1)?,
                ib_symbol: r.try_get(2)?,
                ib_local_symbol: r.try_get(3)?,
                ib_exchange: r.try_get(4)?,
                maturity_date: r.try_get(5)?,
                product: r.try_get(6)?,
                exchange: r.try_get(7)?,
                exchange_basis: r.try_get(8)?,
                tas_data: tas != 0,
                first_day: r.try_get(10)?,
                last_day: r.try_get(11)?,
            };
            let i = c.ib.len();
            c.ib_by_code.insert(k.ric.to_ascii_uppercase(), i);
            c.ib_by_code
                .entry(k.ib_local_symbol.to_ascii_uppercase())
                .or_insert(i);
            c.ib.push(k);
        }
        for r in sqlx::query("SELECT ric, utc_year, directory FROM ib_directories")
            .fetch_all(&mut conn)
            .await?
        {
            let ric: String = r.try_get(0)?;
            c.ib_dirs
                .entry(ric)
                .or_default()
                .push((r.try_get(1)?, r.try_get(2)?));
        }
        for v in c.ib_dirs.values_mut() {
            v.sort();
        }
        conn.close().await?;
        anyhow::ensure!(!c.contracts.is_empty(), "catalog has no contracts");
        Ok(c)
    }

    pub fn product_contracts(&self, product: &str) -> impl Iterator<Item = &Contract> {
        self.by_product
            .get(product)
            .into_iter()
            .flatten()
            .map(|&i| &self.contracts[i])
    }
}
