//! `config/data_dashboard.toml`. Every path is read-only for the dashboard.
use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "d_listen")]
    pub listen: String,
    /// Standalone binary only (inside market-replay-server the server's HTTP
    /// port and runtime are used).
    #[serde(default = "d_workers")]
    pub workers: usize,
    /// Max concurrent filesystem / subprocess jobs started by requests.
    #[serde(default = "d_io_permits")]
    pub io_permits: usize,

    pub cn: CnConfig,
    pub lseg: LsegConfig,
    pub ib: IbConfig,
    pub services: ServicesConfig,
    pub names: NamesConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CnConfig {
    /// The cn-daily config; state_file / log_dir / baseline_root / roll_root /
    /// msg_root / ready_after are taken from it.
    pub cn_daily_config: PathBuf,
    /// HTTP dominant-contract API of cn-kline-server.
    pub dominant_api: String,
    /// psql binary + socket for the read-only metadata queries.
    pub psql: PathBuf,
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_db: String,
    pub pg_user: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LsegConfig {
    pub catalog: PathBuf,
    pub problems_dir: PathBuf,
    /// Future lseg-daily driver state (may not exist yet).
    pub lseg_daily_state: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IbConfig {
    pub state: PathBuf,
    pub contracts_json: PathBuf,
    pub log_dir: PathBuf,
    pub problems_csv: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServicesConfig {
    pub mdl_replay_config: PathBuf,
    pub reference_api: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamesConfig {
    /// `lseg_exchange_groups_*/exchange_summary.csv` (Chinese exchange names).
    pub exchange_summary_csv: PathBuf,
    /// `lseg_exchange_groups_*/ric_catalog.csv` (LSEG per-RIC descriptions).
    pub ric_catalog_csv: PathBuf,
    /// `ib_tas_all_futures_*/all_ib_futures_tas_matches.csv` (IB symbol -> TAS root).
    pub ib_matches_csv: PathBuf,
    /// `ib_tas_all_futures_*/ib_futures_snapshot.json` (IB multipliers, names).
    pub ib_snapshot_json: PathBuf,
}

fn d_listen() -> String {
    "0.0.0.0:18320".into()
}
fn d_workers() -> usize {
    2
}
fn d_io_permits() -> usize {
    4
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("read {path:?}"))?;
        let cfg: Config = toml::from_str(&text).with_context(|| format!("parse {path:?}"))?;
        Ok(cfg)
    }
}

/// The subset of `config/cn_daily.toml` the dashboard reads.
#[derive(Debug, Clone, Deserialize)]
pub struct CnDaily {
    pub msg_root: PathBuf,
    pub baseline_root: PathBuf,
    pub log_dir: PathBuf,
    pub state_file: PathBuf,
    #[serde(default = "d_ready")]
    pub ready_after: String,
    pub roll_root: Option<PathBuf>,
    pub session_as_of: Option<String>,
}

fn d_ready() -> String {
    "16:00".into()
}

impl CnDaily {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("read {path:?}"))?;
        toml::from_str(&text).with_context(|| format!("parse {path:?}"))
    }
}

/// The subset of `config/mdl_replay.toml` the dashboard reads.
#[derive(Debug, Clone, Deserialize)]
pub struct MdlReplay {
    pub cache_root: PathBuf,
    #[serde(default = "d_cache_gb")]
    pub cache_max_gb: u64,
}

fn d_cache_gb() -> u64 {
    48
}

impl MdlReplay {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("read {path:?}"))?;
        toml::from_str(&text).with_context(|| format!("parse {path:?}"))
    }
}
