//! Snapshot plan (`_audit/contract_plan.json`), local NVMe state and run guards.
use crate::ib_minute::Job;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
pub struct Plan {
    pub cutoff_id: i64,
    pub jobs: Vec<Job>,
    #[serde(default)]
    pub excluded: Vec<serde_json::Value>,
}

impl Plan {
    pub fn load(path: &Path) -> Result<Self> {
        let p: Plan = serde_json::from_reader(std::io::BufReader::new(
            File::open(path).with_context(|| format!("plan {}", path.display()))?,
        ))?;
        let mut seen = BTreeMap::new();
        for j in &p.jobs {
            ensure!(
                seen.insert(j.ric.clone(), j.con_id.clone()).is_none(),
                "duplicate RIC in plan: {}",
                j.ric
            );
            ensure!(
                j.cutoff_id == p.cutoff_id,
                "job cutoff differs from plan: {}",
                j.ric
            );
        }
        Ok(p)
    }

    /// con_id -> HRD-verified identity: exported jobs plus resolved contracts
    /// that had no valid IB minutes at the snapshot cutoff (they may have now).
    pub fn mapping(&self) -> Result<BTreeMap<String, Job>> {
        let mut out = BTreeMap::new();
        for j in &self.jobs {
            ensure!(
                out.insert(j.con_id.clone(), j.clone()).is_none(),
                "duplicate con_id in plan: {}",
                j.con_id
            );
        }
        for e in &self.excluded {
            if e.get("status").and_then(|v| v.as_str()) != Some("no_valid_IB_minutes") {
                continue;
            }
            let mut v = e.clone();
            let o = v.as_object_mut().context("excluded entry")?;
            o.remove("status");
            for k in ["ib_first_utc", "ib_last_utc"] {
                if o.get(k).is_none_or(|x| x.is_null()) {
                    o.insert(k.into(), "".into());
                }
            }
            o.entry("cutoff_id").or_insert(self.cutoff_id.into());
            let j: Job = serde_json::from_value(v).context("excluded no-minute contract")?;
            ensure!(
                out.insert(j.con_id.clone(), j).is_none(),
                "duplicate con_id in plan excluded"
            );
        }
        let mut rics = BTreeMap::new();
        for j in out.values() {
            ensure!(
                rics.insert(j.ric.clone(), ()).is_none(),
                "RIC mapped to two con_ids in plan: {}",
                j.ric
            );
        }
        Ok(out)
    }
}

/// Atomic local JSON write (NVMe control state, never the delivery disk).
pub fn save_json(path: &Path, value: &impl Serialize) -> Result<()> {
    fs::create_dir_all(path.parent().context("state parent")?)?;
    let tmp = path.with_extension(format!("json.{}.partial", std::process::id()));
    let mut f = File::create(&tmp)?;
    serde_json::to_writer_pretty(&mut f, value)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

pub fn load_json<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<Option<T>> {
    match File::open(path) {
        Ok(f) => Ok(Some(
            serde_json::from_reader(std::io::BufReader::new(f))
                .with_context(|| format!("{}", path.display()))?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Incremental (MySQL) state on NVMe.
#[derive(Serialize, Deserialize, Clone)]
pub struct State {
    pub delivery_root: PathBuf,
    /// Every row with id <= cutoff_id has been published.
    pub cutoff_id: i64,
    pub updated_at_utc: String,
    /// Contracts mapped by the delivery() port after the snapshot; frozen at
    /// first mapping so their RIC and schema metadata stay stable.
    #[serde(default)]
    pub new_contracts: BTreeMap<String, Job>,
}

/// In-process budget check (no external launcher): fixed pools are the
/// primary control; this stops the run if the process ever exceeds them.
pub struct Budget {
    pub max_rss_bytes: u64,
    pub max_threads: u64,
}
impl Budget {
    pub fn check(&self) -> Result<()> {
        let status = fs::read_to_string("/proc/self/status")?;
        let field = |name: &str| -> Option<u64> {
            status
                .lines()
                .find(|l| l.starts_with(name))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        };
        let rss = field("VmRSS:").unwrap_or(0) * 1024 + field("VmSwap:").unwrap_or(0) * 1024;
        let threads = field("Threads:").unwrap_or(0);
        ensure!(
            rss <= self.max_rss_bytes,
            "resource budget exceeded: rss+swap={rss} > {}",
            self.max_rss_bytes
        );
        ensure!(
            threads <= self.max_threads,
            "resource budget exceeded: threads={threads} > {}",
            self.max_threads
        );
        Ok(())
    }
    pub fn peak_rss_bytes() -> u64 {
        fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmHWM:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse::<u64>().ok())
            })
            .unwrap_or(0)
            * 1024
    }
}

pub fn now_utc() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
