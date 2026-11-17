//! Persistent run queue, cancellation markers, and one process-wide worker lease.
use crate::{engine::Summary, Config};
use anyhow::{ensure, Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Status {
    pub id: String,
    pub state: String,
    pub created_at: String,
    pub updated_at: String,
    pub error: Option<String>,
    pub summary: Option<Summary>,
}
pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut f = File::create(&tmp)?;
    serde_json::to_writer_pretty(&mut f, value)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}
#[derive(Clone)]
pub struct Store {
    pub root: PathBuf,
}
impl Store {
    pub fn new(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(root.join("runs"))?;
        Ok(Self { root })
    }
    pub fn path(&self, id: &str) -> Result<PathBuf> {
        ensure!(
            !id.is_empty() && id.bytes().all(|x| x.is_ascii_alphanumeric() || x == b'-'),
            "invalid run id"
        );
        Ok(self.root.join("runs").join(id))
    }
    pub fn submit(&self, cfg: &Config) -> Result<Status> {
        cfg.validate()?;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = format!(
            "{}-{}-{}",
            chrono::Utc::now().format("%Y%m%dT%H%M%S%f"),
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let dir = self.path(&id)?;
        std::fs::create_dir(&dir)?;
        atomic_json(&dir.join("config.json"), cfg)?;
        let now = chrono::Utc::now().to_rfc3339();
        let status = Status {
            id,
            state: "queued".into(),
            created_at: now.clone(),
            updated_at: now,
            error: None,
            summary: None,
        };
        atomic_json(&dir.join("status.json"), &status)?;
        Ok(status)
    }
    pub fn status(&self, id: &str) -> Result<Status> {
        Ok(serde_json::from_reader(File::open(
            self.path(id)?.join("status.json"),
        )?)?)
    }
    pub fn list(&self, limit: usize) -> Result<Vec<Status>> {
        let mut paths: Vec<_> = std::fs::read_dir(self.root.join("runs"))?
            .filter_map(|x| x.ok().map(|x| x.path()))
            .filter(|p| p.is_dir())
            .collect();
        paths.sort();
        paths.reverse();
        let mut out = Vec::new();
        for p in paths.into_iter().take(limit.min(1000)) {
            if let Ok(f) = File::open(p.join("status.json")) {
                if let Ok(s) = serde_json::from_reader(f) {
                    out.push(s);
                }
            }
        }
        Ok(out)
    }
    pub fn pending(&self) -> Result<bool> {
        Ok(self.list(1000)?.iter().any(|s| s.state == "queued"))
    }
    pub fn cancel(&self, id: &str) -> Result<Status> {
        let dir = self.path(id)?;
        let mut s = self.status(id)?;
        if ["completed", "failed", "cancelled", "interrupted"].contains(&s.state.as_str()) {
            return Ok(s);
        }
        File::create(dir.join("cancel"))?.sync_all()?;
        // The worker owns running status; avoid racing its final publication.
        if s.state == "queued" {
            s.state = "cancelled".into();
            s.updated_at = chrono::Utc::now().to_rfc3339();
            atomic_json(&dir.join("status.json"), &s)?;
        }
        Ok(s)
    }
    pub fn lease(&self) -> Result<File> {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join("worker.lock"))?;
        f.try_lock_exclusive()
            .context("another backtest worker is running")?;
        Ok(f)
    }
    pub fn worker(&self) -> Result<Option<Status>> {
        let _lease = self.lease()?;
        let mut runs = self.list(1000)?;
        runs.reverse();
        // Holding the exclusive lease proves no previous worker is still executing.
        for s in &mut runs {
            if ["preparing", "running"].contains(&s.state.as_str()) {
                s.state = "interrupted".into();
                s.error = Some("previous worker exited before publishing a result".into());
                atomic_json(&self.path(&s.id)?.join("status.json"), s)?;
            }
        }
        let Some(mut s) = runs.into_iter().find(|s| s.state == "queued") else {
            return Ok(None);
        };
        let dir = self.path(&s.id)?;
        if dir.join("cancel").exists() {
            s.state = "cancelled".into();
            atomic_json(&dir.join("status.json"), &s)?;
            return Ok(Some(s));
        }
        let cfg: Config = serde_json::from_reader(File::open(dir.join("config.json"))?)?;
        s.state = "running".into();
        s.updated_at = chrono::Utc::now().to_rfc3339();
        atomic_json(&dir.join("status.json"), &s)?;
        let result = crate::runner::run(&cfg, &dir, || dir.join("cancel").exists());
        s.updated_at = chrono::Utc::now().to_rfc3339();
        match result {
            Ok(summary) => {
                s.state = "completed".into();
                s.summary = Some(summary);
            }
            Err(e) => {
                s.state = if dir.join("cancel").exists() {
                    "cancelled"
                } else {
                    "failed"
                }
                .into();
                s.error = Some(format!("{e:#}"));
            }
        }
        atomic_json(&dir.join("status.json"), &s)?;
        Ok(Some(s))
    }
}
