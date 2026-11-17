use anyhow::{bail, ensure, Context, Result};
use chrono::{NaiveTime, Weekday};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Run history and per-process logs.
    pub state_dir: PathBuf,
    /// Private KEY=VALUE file (mode 600) loaded into the server environment
    /// and therefore inherited by every child (DingTalk, admin token).
    #[serde(default)]
    pub env_file: Option<PathBuf>,
    pub http: Http,
    /// `cn-kline-server` config (gRPC `listen`, baseline root, roll dir).
    pub cn_market: CnMarket,
    /// Reference data (OpenCTP fees, ticks, sessions) API, served in-process.
    /// Omit to leave :8765 to the Python service.
    #[serde(default)]
    pub reference: Option<Reference>,
    /// Read-only dashboard 数据处理进度与品种查询 (`data_dashboard`): UI at
    /// `/`, JSON at `/api/dashboard/*`. Omit to serve without it.
    #[serde(default)]
    pub dashboard: Option<Dashboard>,
    #[serde(default, rename = "service")]
    pub services: Vec<Service>,
    #[serde(default, rename = "job")]
    pub jobs: Vec<Job>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Http {
    pub listen: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CnMarket {
    pub config: PathBuf,
}

/// `cn_reference` API: Rust owns the listen port; paths it does not handle
/// (web UI, announcements, message fees, fee estimates, imports) are proxied
/// to the Python process at `fallback`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reference {
    pub listen: String,
    pub fallback: String,
    /// KEY=VALUE file with CN_FUTURES_REF_DATABASE_URL (the Python `.env`),
    /// used when the variable is not in the environment.
    #[serde(default)]
    pub env_file: Option<PathBuf>,
    #[serde(default = "d_ref_schema")]
    pub schema: String,
}

fn d_ref_schema() -> String {
    "cn_futures_reference".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dashboard {
    /// `config/data_dashboard.toml` (its `listen` is ignored here).
    pub config: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub cwd: PathBuf,
    pub command: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// Long-running child, restarted with backoff when it exits.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub name: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    pub description: String,
    #[serde(flatten)]
    pub run: Command,
    /// SIGTERM -> SIGKILL grace period on stop.
    #[serde(default = "d_stop")]
    pub stop_timeout_s: u64,
}

/// One-shot task started on a daily schedule (or from the page). Jobs run
/// one at a time through a single queue.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub name: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    pub description: String,
    #[serde(flatten)]
    pub run: Command,
    pub schedule: Schedule,
    #[serde(default = "d_timeout")]
    pub timeout_minutes: u64,
    #[serde(default = "d_stop")]
    pub stop_timeout_s: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    /// IANA zone, e.g. "Asia/Shanghai" or "UTC".
    pub tz: String,
    /// Local start times "HH:MM".
    pub at: Vec<String>,
    /// Optional weekday filter ("Mon".."Sun"); empty = every day.
    #[serde(default)]
    pub weekdays: Vec<String>,
    /// Optional day-of-month filter (1..=28); empty = every day.
    #[serde(default)]
    pub month_days: Vec<u32>,
}

fn yes() -> bool {
    true
}
fn d_stop() -> u64 {
    30
}
fn d_timeout() -> u64 {
    12 * 60
}

impl Schedule {
    pub fn zone(&self) -> Result<chrono_tz::Tz> {
        self.tz
            .parse()
            .map_err(|_| anyhow::anyhow!("unknown time zone {:?}", self.tz))
    }
    pub fn times(&self) -> Result<Vec<NaiveTime>> {
        self.at
            .iter()
            .map(|t| {
                NaiveTime::parse_from_str(t, "%H:%M").with_context(|| format!("bad time {t:?}"))
            })
            .collect()
    }
    pub fn days(&self) -> Result<Vec<Weekday>> {
        self.weekdays
            .iter()
            .map(|d| {
                d.parse::<Weekday>()
                    .map_err(|_| anyhow::anyhow!("bad weekday {d:?}"))
            })
            .collect()
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let cfg: Config = toml::from_str(&std::fs::read_to_string(path)?)
            .with_context(|| format!("parse {path:?}"))?;
        let mut names = BTreeSet::new();
        for n in cfg
            .services
            .iter()
            .map(|s| &s.name)
            .chain(cfg.jobs.iter().map(|j| &j.name))
        {
            ensure!(
                !n.is_empty()
                    && n.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "bad name {n:?}"
            );
            if !names.insert(n.clone()) {
                bail!("duplicate service/job name {n}");
            }
        }
        for j in &cfg.jobs {
            j.schedule.zone()?;
            ensure!(
                !j.schedule.times()?.is_empty(),
                "job {} has no start time",
                j.name
            );
            j.schedule.days()?;
            ensure!(
                j.schedule.month_days.iter().all(|d| (1..=28).contains(d)),
                "job {}: month_days must be 1..28",
                j.name
            );
        }
        Ok(cfg)
    }
}

/// Loads KEY=VALUE lines (no shell expansion) from a private file into the
/// process environment. Refuses group/world-readable files.
pub fn load_env_file(path: &Path) -> Result<usize> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)
        .with_context(|| format!("stat {path:?}"))?
        .permissions()
        .mode();
    ensure!(
        mode & 0o077 == 0,
        "{path:?} must not be group/world accessible (chmod 600)"
    );
    let mut n = 0;
    for line in std::fs::read_to_string(path)?.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let v = v.trim().trim_matches('"').trim_matches('\'');
        // SAFETY: called once at startup before any thread is spawned.
        unsafe { std::env::set_var(k.trim(), v) };
        n += 1;
    }
    Ok(n)
}
