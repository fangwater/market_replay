use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Read-only NFS root with `YYYYMMDD/mdl_*.csv`.
    #[serde(default = "d_msg_root")]
    pub msg_root: PathBuf,
    /// Local NVMe directory for re-sharded frames.
    pub cache_root: PathBuf,
    /// ROUTER socket for REQ clients.
    #[serde(default = "d_control")]
    pub control_endpoint: String,
    /// Interface for per-session XPUB sockets (ephemeral ports).
    #[serde(default = "d_local")]
    pub bind_host: String,
    /// Host put into returned `tcp://` endpoints when binding a wildcard.
    #[serde(default = "d_local")]
    pub advertise_host: String,
    /// Threads per shard build (NFS reads, parsing, writing).
    #[serde(default = "d_threads")]
    pub build_threads: usize,
    #[serde(default = "d_sessions")]
    pub max_sessions: usize,
    /// Concurrent heavy requests (replay/build/symbols).
    #[serde(default = "d_jobs")]
    pub max_jobs: usize,
    /// LRU budget for the replay shard cache (whole natural days, ~8 GB each); 0 = unlimited.
    #[serde(default = "d_cache_max_gb")]
    pub cache_max_gb: u64,
    /// Finished sessions are dropped after this long.
    #[serde(default = "d_ttl")]
    pub session_ttl_s: u64,
}

fn d_msg_root() -> PathBuf {
    "/mnt/nfs/30.3_tonglian_msg".into()
}
fn d_control() -> String {
    "tcp://127.0.0.1:13100".into()
}
fn d_local() -> String {
    "127.0.0.1".into()
}
fn d_threads() -> usize {
    16
}
fn d_sessions() -> usize {
    32
}
fn d_jobs() -> usize {
    4
}
fn d_cache_max_gb() -> u64 {
    48
}
fn d_ttl() -> u64 {
    1800
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read config {path:?}"))?;
        let cfg: Self = toml::from_str(&text).with_context(|| format!("parse config {path:?}"))?;
        anyhow::ensure!(
            cfg.cache_root != cfg.msg_root && !cfg.cache_root.starts_with(&cfg.msg_root),
            "cache_root must not be inside the read-only msg_root"
        );
        Ok(cfg)
    }

    pub fn cache(&self) -> crate::cache::Cache {
        crate::cache::Cache::new(
            self.msg_root.clone(),
            self.cache_root.clone(),
            self.build_threads,
        )
        .with_max_bytes(self.cache_max_gb << 30)
    }

    pub fn session_endpoint(&self) -> String {
        format!("tcp://{}:*", self.bind_host)
    }
}
