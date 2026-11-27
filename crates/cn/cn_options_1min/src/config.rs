use anyhow::{Context, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Root output directory: `<out_root>/{exchange}/{PRODUCT}/{YYYYMMDD}.parquet`
    pub out_root: PathBuf,
    /// Historical zip HTTP root (ends with `/`), e.g. `.../future_opt_pricemin/`
    pub hist_base_url: String,
    /// Daily MDL HTTP root (ends with `/`), e.g. `http://10.61.20.11/users/el01/public/tonglian/data/`
    pub daily_base_url: String,
    /// Hard layer cutoff: `TradDay <= cutoff_day` uses historical zip; `> cutoff_day` uses daily MDL.
    pub cutoff_day: String,
    /// State JSON path tracking completed TradDays.
    pub state_file: PathBuf,
    /// Only process today's TradDay after this Asia/Shanghai HH:MM clock (default `16:00`).
    #[serde(default = "default_ready_after")]
    pub ready_after: String,
    /// Host tag included in DingTalk alerts.
    #[serde(default = "default_host_tag")]
    pub host_tag: String,
    #[serde(default)]
    pub dingtalk_webhook_env: Option<String>,
    #[serde(default)]
    pub dingtalk_secret_env: Option<String>,
    /// Default worker count for batch runs (capped at 8).
    #[serde(default = "default_workers")]
    pub default_workers: usize,
}

fn default_ready_after() -> String {
    "16:00".to_string()
}

fn default_host_tag() -> String {
    "market_replay@30.38".to_string()
}

fn default_workers() -> usize {
    4
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
        let cfg: Self =
            toml::from_str(&raw).with_context(|| format!("parse config {}", path.display()))?;
        let _ = cfg.cutoff_date()?;
        Ok(cfg)
    }

    pub fn cutoff_date(&self) -> Result<NaiveDate> {
        NaiveDate::parse_from_str(&self.cutoff_day, "%Y-%m-%d")
            .with_context(|| format!("invalid cutoff_day {:?}", self.cutoff_day))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DaySummary {
    pub source: String,
    pub files: usize,
    pub rows: usize,
    pub dedup_dropped: usize,
    pub completed_at_utc: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct State {
    pub last_trad_day: Option<String>,
    pub completed_days: BTreeMap<String, DaySummary>,
}

impl State {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw =
            fs::read_to_string(path).with_context(|| format!("read state {}", path.display()))?;
        serde_json::from_str(&raw).with_context(|| format!("parse state {}", path.display()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create state dir {}", parent.display()))?;
        }
        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
        let json = serde_json::to_string_pretty(self)?;
        fs::write(&tmp, json).with_context(|| format!("write temp state {}", tmp.display()))?;
        fs::rename(&tmp, path)
            .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }

    pub fn mark_done(&mut self, day: NaiveDate, summary: DaySummary) {
        let key = day.format("%Y-%m-%d").to_string();
        self.completed_days.insert(key, summary);
        self.last_trad_day = self.completed_days.keys().next_back().cloned();
    }

    pub fn is_done(&self, day: NaiveDate) -> bool {
        let key = day.format("%Y-%m-%d").to_string();
        self.completed_days.contains_key(&key)
    }
}
