//! Persistent driver state (local NVMe only; never on the delivery disk).
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::remote::Fingerprint;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Stage {
    Planned,
    Downloaded,
    Staged,
    Replayed,
    Audited,
    Exported,
    Published,
    /// trade/quote/status delivered (kline tracked separately).
    Done,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedFile {
    pub date: String,
    pub name: String,
    pub url: String,
    pub remote: Fingerprint,
    /// Local source path once downloaded or reused.
    #[serde(default)]
    pub source_file: Option<PathBuf>,
    #[serde(default)]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductRun {
    pub files: Vec<PlannedFile>,
    pub stage: Stage,
    /// `<start>_<end>`, end exclusive (day after the last source date).
    pub period: String,
    pub year: i32,
    #[serde(default)]
    pub replay_attempt: u32,
    #[serde(default)]
    pub export_attempt: u32,
    #[serde(default)]
    pub db: Option<PathBuf>,
    #[serde(default)]
    pub audit: Option<PathBuf>,
    /// Product CF -> persisted status (kind 13) rows.
    #[serde(default)]
    pub cfs: BTreeMap<String, u64>,
    #[serde(default)]
    pub rics: usize,
    #[serde(default)]
    pub source_rows: u64,
    #[serde(default)]
    pub trade_rows: u64,
    #[serde(default)]
    pub quote_rows: u64,
    #[serde(default)]
    pub files_new: u64,
    #[serde(default)]
    pub files_existing: u64,
    /// First source_index of this product's kline plan jobs.
    #[serde(default)]
    pub plan_index: Option<u32>,
    #[serde(default)]
    pub kline_done: bool,
    /// Last date delivered for this product before this batch (late-day
    /// classification and the kline compare window).
    #[serde(default)]
    pub prev_last: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    /// Monthly merged DB (`Merged::id`) that replaced `db` in the plan; the
    /// daily DB is retired from then on.
    #[serde(default)]
    pub merged_into: Option<String>,
    /// The retired daily DB was deleted (`merge --delete-sources`).
    #[serde(default)]
    pub db_deleted: bool,
}

impl ProductRun {
    pub fn dates(&self) -> Vec<String> {
        self.files.iter().map(|f| f.date.clone()).collect()
    }
    pub fn finished(&self) -> bool {
        self.stage == Stage::Done && self.kline_done
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub id: String,
    pub created_utc: String,
    pub products: BTreeMap<String, ProductRun>,
}

impl Batch {
    pub fn finished(&self) -> bool {
        self.products.values().all(ProductRun::finished)
    }
}

/// One source of a monthly merged DB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergedFrom {
    /// Daily: `daily<batch>_<product>`; an earlier merge: its `Merged::id`.
    pub id: String,
    pub db: PathBuf,
    /// `CURRENT` of the source when it was merged.
    pub current: String,
    pub coverage_days: Vec<String>,
    #[serde(default)]
    pub batch: Option<String>,
}

/// A monthly merged TAS DB of one product directory (`lseg-daily merge`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Merged {
    /// `<YYYY-MM>/<product>/attempt_<n>`.
    pub id: String,
    pub month: String,
    pub product: String,
    pub db: PathBuf,
    /// Product CF -> persisted status (kind 13) rows (as `ProductRun::cfs`).
    pub cfs: BTreeMap<String, u64>,
    pub plan_index: u32,
    pub period: String,
    /// Union of the sources' delivered UTC days (unchanged by the merge).
    pub coverage_days: Vec<String>,
    pub exchange_map: PathBuf,
    /// Merge + verification report.
    pub report: PathBuf,
    pub merged_from: Vec<MergedFrom>,
    pub created_utc: String,
    #[serde(default)]
    pub trade_rows: u64,
    #[serde(default)]
    pub quote_rows: u64,
    /// A later merge of the same month folded this DB in.
    #[serde(default)]
    pub retired_by: Option<String>,
    #[serde(default)]
    pub db_deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run: String,
    pub finished_utc: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    #[serde(default)]
    pub bootstrapped: bool,
    /// Product directory -> source dates delivered (incl. header-only days).
    #[serde(default)]
    pub delivered: BTreeMap<String, BTreeSet<String>>,
    /// Raw RIC -> source product directory that published it.
    #[serde(default)]
    pub owners: BTreeMap<String, String>,
    /// Raw RIC -> exchange directory assigned by this driver (new RICs only).
    #[serde(default)]
    pub venues: BTreeMap<String, String>,
    /// "product date" -> reason, for zero-byte placeholders / changed remotes.
    #[serde(default)]
    pub gaps: BTreeMap<String, String>,
    #[serde(default)]
    pub batches: Vec<Batch>,
    #[serde(default)]
    pub next_source_index: Option<u32>,
    #[serde(default)]
    pub catalog_dirty: bool,
    /// "year:product" kline groups that had one full compare of every day.
    #[serde(default)]
    pub kline_compared: BTreeSet<String>,
    #[serde(default)]
    pub history: Vec<RunRecord>,
    /// Monthly merged DBs (active ones replace their sources in the plan).
    #[serde(default)]
    pub merged: Vec<Merged>,
}

impl State {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        serde_json::from_slice(&fs::read(path)?).with_context(|| format!("parse {path:?}"))
    }
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(p) = path.parent() {
            fs::create_dir_all(p)?;
        }
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        fs::File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, path)?;
        Ok(())
    }
    pub fn in_open_batch(&self, product: &str, date: &str) -> bool {
        self.batches.iter().any(|b| {
            b.products
                .get(product)
                .is_some_and(|p| !p.finished() && p.files.iter().any(|f| f.date == date))
        })
    }
}
