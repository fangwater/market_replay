//! Supervised services (restart with backoff) and scheduled jobs (one queue,
//! one job at a time). All state is visible through `Manager::snapshot`.

use crate::config::{Config, Job, Service};
use crate::process;
use anyhow::Result;
use chrono::{DateTime, Datelike, Duration as CDuration, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{watch, Notify};

/// Missed slots older than this are not caught up after a restart.
const CATCH_UP_HOURS: i64 = 6;

#[derive(Clone, Debug, Serialize)]
pub struct ServiceView {
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub status: String,
    pub pid: Option<u32>,
    pub started_at: Option<DateTime<Utc>>,
    pub restarts: u32,
    pub last_exit: Option<String>,
    pub last_exit_at: Option<DateTime<Utc>>,
    pub log: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunRecord {
    pub job: String,
    pub trigger: String,
    pub slot: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub result: Option<String>,
    pub ok: Option<bool>,
    pub log: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct JobView {
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub schedule: String,
    pub next_run: Option<DateTime<Utc>>,
    pub queued: bool,
    pub running: Option<RunRecord>,
    pub last: Option<RunRecord>,
    pub log: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub now: DateTime<Utc>,
    pub services: Vec<ServiceView>,
    pub jobs: Vec<JobView>,
    pub queue: Vec<String>,
}

struct Shared {
    services: BTreeMap<String, ServiceView>,
    jobs: BTreeMap<String, JobView>,
    queue: VecDeque<(String, String, Option<DateTime<Utc>>)>,
}

pub struct Manager {
    cfg: Config,
    shared: Mutex<Shared>,
    restart: BTreeMap<String, Arc<Notify>>,
    work: Notify,
    shutdown: watch::Receiver<bool>,
}

fn runs_path(cfg: &Config) -> PathBuf {
    cfg.state_dir.join("runs.jsonl")
}
fn slots_path(cfg: &Config) -> PathBuf {
    cfg.state_dir.join("job_slots.json")
}

fn schedule_text(j: &Job) -> String {
    let mut days = if j.schedule.weekdays.is_empty() {
        "每天".to_string()
    } else {
        j.schedule.weekdays.join(",")
    };
    if !j.schedule.month_days.is_empty() {
        let d: Vec<String> = j
            .schedule
            .month_days
            .iter()
            .map(|d| d.to_string())
            .collect();
        days = format!("每月 {} 日", d.join(","));
    }
    format!("{days} {} ({})", j.schedule.at.join(", "), j.schedule.tz)
}

/// Slots (UTC instants) of `job` in (from, to], in order.
pub fn slots_between(job: &Job, from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<DateTime<Utc>> {
    let (Ok(tz), Ok(times), Ok(days)) = (
        job.schedule.zone(),
        job.schedule.times(),
        job.schedule.days(),
    ) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut d = from.with_timezone(&tz).date_naive() - CDuration::days(1);
    let last = to.with_timezone(&tz).date_naive();
    while d <= last {
        let month_day =
            job.schedule.month_days.is_empty() || job.schedule.month_days.contains(&d.day());
        if month_day && (days.is_empty() || days.contains(&d.weekday())) {
            for t in &times {
                if let Some(local) = tz.from_local_datetime(&d.and_time(*t)).earliest() {
                    let u = local.with_timezone(&Utc);
                    if u > from && u <= to {
                        out.push(u);
                    }
                }
            }
        }
        d += CDuration::days(1);
    }
    out.sort();
    out
}

fn next_slot(job: &Job, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let horizon = if job.schedule.month_days.is_empty() {
        8
    } else {
        32
    };
    slots_between(job, now, now + CDuration::days(horizon))
        .into_iter()
        .next()
}

fn append_run(cfg: &Config, r: &RunRecord) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(runs_path(cfg))
    {
        if let Ok(line) = serde_json::to_string(r) {
            let _ = writeln!(f, "{line}");
        }
    }
}

/// Most recent `limit` run records, newest first (optionally one job).
pub fn recent_runs(cfg: &Config, job: Option<&str>, limit: usize) -> Vec<RunRecord> {
    let text = process::tail(&runs_path(cfg), 4 << 20);
    let mut out: Vec<RunRecord> = text
        .lines()
        .filter_map(|l| serde_json::from_str::<RunRecord>(l).ok())
        .filter(|r| job.is_none_or(|j| r.job == j))
        .collect();
    out.reverse();
    out.truncate(limit);
    out
}

impl Manager {
    pub fn new(cfg: Config, shutdown: watch::Receiver<bool>) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&cfg.state_dir)?;
        let mut services = BTreeMap::new();
        let mut restart = BTreeMap::new();
        for s in &cfg.services {
            services.insert(
                s.name.clone(),
                ServiceView {
                    name: s.name.clone(),
                    description: s.description.clone(),
                    enabled: s.enabled,
                    status: if s.enabled { "starting" } else { "disabled" }.into(),
                    pid: None,
                    started_at: None,
                    restarts: 0,
                    last_exit: None,
                    last_exit_at: None,
                    log: process::log_path(&cfg.state_dir, "service", &s.name),
                },
            );
            restart.insert(s.name.clone(), Arc::new(Notify::new()));
        }
        let now = Utc::now();
        let mut jobs = BTreeMap::new();
        for j in &cfg.jobs {
            jobs.insert(
                j.name.clone(),
                JobView {
                    name: j.name.clone(),
                    description: j.description.clone(),
                    enabled: j.enabled,
                    schedule: schedule_text(j),
                    next_run: j.enabled.then(|| next_slot(j, now)).flatten(),
                    queued: false,
                    running: None,
                    last: recent_runs(&cfg, Some(&j.name), 1).into_iter().next(),
                    log: process::log_path(&cfg.state_dir, "job", &j.name),
                },
            );
        }
        Ok(Arc::new(Self {
            cfg,
            shared: Mutex::new(Shared {
                services,
                jobs,
                queue: VecDeque::new(),
            }),
            restart,
            work: Notify::new(),
            shutdown,
        }))
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn snapshot(&self) -> Snapshot {
        let s = self.shared.lock().unwrap();
        Snapshot {
            now: Utc::now(),
            services: s.services.values().cloned().collect(),
            jobs: s.jobs.values().cloned().collect(),
            queue: s.queue.iter().map(|(n, _, _)| n.clone()).collect(),
        }
    }

    /// Queues `name` unless it is already queued or running.
    pub fn enqueue(
        &self,
        name: &str,
        trigger: &str,
        slot: Option<DateTime<Utc>>,
    ) -> Result<bool, String> {
        let mut s = self.shared.lock().unwrap();
        let Some(j) = s.jobs.get_mut(name) else {
            return Err(format!("unknown job {name}"));
        };
        if j.queued || j.running.is_some() {
            return Ok(false);
        }
        j.queued = true;
        s.queue
            .push_back((name.to_string(), trigger.to_string(), slot));
        drop(s);
        self.work.notify_one();
        Ok(true)
    }

    pub fn restart_service(&self, name: &str) -> Result<(), String> {
        let n = self
            .restart
            .get(name)
            .ok_or_else(|| format!("unknown service {name}"))?;
        n.notify_one();
        Ok(())
    }

    fn update_service(&self, name: &str, f: impl FnOnce(&mut ServiceView)) {
        if let Some(v) = self.shared.lock().unwrap().services.get_mut(name) {
            f(v);
        }
    }

    pub fn start(self: &Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();
        for s in self.cfg.services.iter().filter(|s| s.enabled) {
            let (me, s) = (self.clone(), s.clone());
            handles.push(tokio::spawn(async move { me.supervise(s).await }));
        }
        let me = self.clone();
        handles.push(tokio::spawn(async move { me.schedule_loop().await }));
        let me = self.clone();
        handles.push(tokio::spawn(async move { me.run_loop().await }));
        handles
    }

    async fn supervise(self: Arc<Self>, svc: Service) {
        let log = process::log_path(&self.cfg.state_dir, "service", &svc.name);
        let restart = self.restart[&svc.name].clone();
        let mut shutdown = self.shutdown.clone();
        let mut delay = Duration::from_secs(2);
        let grace = Duration::from_secs(svc.stop_timeout_s);
        loop {
            if *shutdown.borrow() {
                break;
            }
            process::mark(
                &log,
                &format!("start {:?} {:?}", svc.run.command, svc.run.args),
            );
            let mut child = match process::spawn(&svc.run, &log) {
                Ok(c) => c,
                Err(e) => {
                    process::mark(&log, &format!("spawn failed: {e:#}"));
                    self.update_service(&svc.name, |v| {
                        v.status = "backoff".into();
                        v.last_exit = Some(format!("spawn failed: {e:#}"));
                        v.last_exit_at = Some(Utc::now());
                    });
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = shutdown.changed() => {}
                    }
                    delay = (delay * 2).min(Duration::from_secs(60));
                    continue;
                }
            };
            let started = Instant::now();
            self.update_service(&svc.name, |v| {
                v.status = "running".into();
                v.pid = child.id();
                v.started_at = Some(Utc::now());
            });
            let (status, manual) = tokio::select! {
                s = child.wait() => (s.ok(), false),
                _ = restart.notified() => (process::stop(&mut child, grace).await, true),
                _ = shutdown.changed() => (process::stop(&mut child, grace).await, true),
            };
            let text = process::describe(status);
            process::mark(&log, &format!("stopped: {text}"));
            let stopping = *shutdown.borrow();
            self.update_service(&svc.name, |v| {
                v.status = if stopping { "stopped" } else { "backoff" }.into();
                v.pid = None;
                v.restarts += 1;
                v.last_exit = Some(text);
                v.last_exit_at = Some(Utc::now());
            });
            if stopping {
                break;
            }
            if manual || started.elapsed() > Duration::from_secs(60) {
                delay = Duration::from_secs(2);
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.changed() => {}
                }
                delay = (delay * 2).min(Duration::from_secs(60));
            }
        }
    }

    fn load_slots(&self) -> BTreeMap<String, DateTime<Utc>> {
        std::fs::read(slots_path(&self.cfg))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default()
    }

    fn save_slots(&self, slots: &BTreeMap<String, DateTime<Utc>>) {
        let p = slots_path(&self.cfg);
        let tmp = p.with_extension("json.tmp");
        if let Ok(b) = serde_json::to_vec_pretty(slots) {
            if std::fs::write(&tmp, b).is_ok() {
                let _ = std::fs::rename(&tmp, &p);
            }
        }
    }

    async fn schedule_loop(self: Arc<Self>) {
        let mut slots = self.load_slots();
        let mut shutdown = self.shutdown.clone();
        let now = Utc::now();
        // First sight of a job: start from now (no surprise run at deploy).
        for j in &self.cfg.jobs {
            slots.entry(j.name.clone()).or_insert(now);
        }
        self.save_slots(&slots);
        loop {
            let now = Utc::now();
            for j in self.cfg.jobs.iter().filter(|j| j.enabled) {
                let last = slots[&j.name];
                let due = slots_between(j, last, now);
                if let Some(&slot) = due.last() {
                    slots.insert(j.name.clone(), slot);
                    self.save_slots(&slots);
                    if now - slot <= CDuration::hours(CATCH_UP_HOURS) {
                        let trigger = if now - slot > CDuration::minutes(2) {
                            "catch-up"
                        } else {
                            "schedule"
                        };
                        let _ = self.enqueue(&j.name, trigger, Some(slot));
                    }
                }
                let next = next_slot(j, now);
                if let Some(v) = self.shared.lock().unwrap().jobs.get_mut(&j.name) {
                    v.next_run = next;
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                _ = shutdown.changed() => break,
            }
        }
    }

    async fn run_loop(self: Arc<Self>) {
        let mut shutdown = self.shutdown.clone();
        loop {
            let next = self.shared.lock().unwrap().queue.pop_front();
            let Some((name, trigger, slot)) = next else {
                tokio::select! {
                    _ = self.work.notified() => continue,
                    _ = shutdown.changed() => break,
                }
            };
            let Some(job) = self.cfg.jobs.iter().find(|j| j.name == name).cloned() else {
                continue;
            };
            let log = process::log_path(&self.cfg.state_dir, "job", &name);
            let mut rec = RunRecord {
                job: name.clone(),
                trigger,
                slot,
                started_at: Utc::now(),
                finished_at: None,
                result: None,
                ok: None,
                log: log.clone(),
            };
            {
                let mut s = self.shared.lock().unwrap();
                let v = s.jobs.get_mut(&name).unwrap();
                v.queued = false;
                v.running = Some(rec.clone());
            }
            process::mark(
                &log,
                &format!(
                    "run ({}) {:?} {:?}",
                    rec.trigger, job.run.command, job.run.args
                ),
            );
            let grace = Duration::from_secs(job.stop_timeout_s);
            let (text, ok) = match process::spawn(&job.run, &log) {
                Err(e) => (format!("spawn failed: {e:#}"), false),
                Ok(mut child) => {
                    tokio::select! {
                        s = child.wait() => {
                            let s = s.ok();
                            (process::describe(s), s.is_some_and(|s| s.success()))
                        }
                        _ = tokio::time::sleep(Duration::from_secs(job.timeout_minutes * 60)) => {
                            let s = process::stop(&mut child, grace).await;
                            (format!("timeout after {} min ({})", job.timeout_minutes, process::describe(s)), false)
                        }
                        _ = shutdown.changed() => {
                            let s = process::stop(&mut child, grace).await;
                            (format!("interrupted by server shutdown ({})", process::describe(s)), false)
                        }
                    }
                }
            };
            process::mark(&log, &format!("finished: {text}"));
            rec.finished_at = Some(Utc::now());
            rec.result = Some(text);
            rec.ok = Some(ok);
            append_run(&self.cfg, &rec);
            {
                let mut s = self.shared.lock().unwrap();
                let v = s.jobs.get_mut(&name).unwrap();
                v.running = None;
                v.last = Some(rec);
            }
            if *shutdown.borrow() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Command, Schedule};

    fn job(tz: &str, at: &[&str], weekdays: &[&str]) -> Job {
        Job {
            name: "j".into(),
            enabled: true,
            description: String::new(),
            run: Command {
                cwd: "/".into(),
                command: "/bin/true".into(),
                args: vec![],
                env: Default::default(),
            },
            schedule: Schedule {
                tz: tz.into(),
                at: at.iter().map(|s| s.to_string()).collect(),
                weekdays: weekdays.iter().map(|s| s.to_string()).collect(),
                month_days: vec![],
            },
            timeout_minutes: 1,
            stop_timeout_s: 1,
        }
    }

    #[test]
    fn slots_follow_zone_and_weekdays() {
        let j = job("Asia/Shanghai", &["16:30"], &[]);
        let from = Utc.with_ymd_and_hms(2026, 11, 16, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2026, 11, 18, 0, 0, 0).unwrap();
        let s = slots_between(&j, from, to);
        assert_eq!(
            s,
            vec![
                Utc.with_ymd_and_hms(2026, 11, 16, 8, 30, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 11, 17, 8, 30, 0).unwrap()
            ]
        );
        // 2026-11-16 is a Monday.
        let j = job("UTC", &["01:30"], &["Tue"]);
        assert_eq!(
            slots_between(&j, from, to),
            vec![Utc.with_ymd_and_hms(2026, 11, 17, 1, 30, 0).unwrap()]
        );
        // Exclusive start, inclusive end.
        let j = job("UTC", &["08:30"], &[]);
        let at = Utc.with_ymd_and_hms(2026, 11, 16, 8, 30, 0).unwrap();
        assert!(slots_between(&j, at, at).is_empty());
        assert_eq!(slots_between(&j, at - CDuration::seconds(1), at), vec![at]);
        assert_eq!(next_slot(&j, at), Some(at + CDuration::days(1)));
    }

    #[test]
    fn monthly_slots() {
        let mut j = job("Asia/Shanghai", &["13:00"], &[]);
        j.schedule.month_days = vec![2];
        let from = Utc.with_ymd_and_hms(2026, 11, 16, 0, 0, 0).unwrap();
        // 2026-12-02 13:00 Shanghai = 05:00 UTC; next one is 2027-01-02.
        let first = Utc.with_ymd_and_hms(2026, 12, 2, 5, 0, 0).unwrap();
        assert_eq!(next_slot(&j, from), Some(first));
        assert_eq!(
            slots_between(&j, from, first + CDuration::days(40)),
            vec![first, Utc.with_ymd_and_hms(2027, 1, 2, 5, 0, 0).unwrap()]
        );
        assert!(schedule_text(&j).starts_with("每月 2 日"));
    }
}
