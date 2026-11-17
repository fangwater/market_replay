//! Driver pieces of the one-off L2 backfill (`lseg_l2_slice backfill`): the
//! units (period packages and daily files), the gate that keeps the run out of
//! the way of the server's scheduled jobs, and the progress file.
use crate::l2_stream::{Progress, Unit, UnitKind};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as Days, NaiveDate, Utc};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    fs,
    io::{Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

/// A file younger than this may still be downloading.
const TOO_NEW: Duration = Duration::from_secs(600);

/// The period packages of the backfill, in the order of the stream.
pub const PACKAGES: [(&str, &str, &str); 3] = [
    (
        "p2025",
        "shanghai_evolution_futures_market_depth_ric_list_0_ll2_2025-01-01_2026-01-01",
        "merged-Data-part-",
    ),
    (
        "p2026h1",
        "shanghai_evolution_futures_market_depth_ric_list_0_ll2_2026-01-01_2026-06-01",
        "merged-Data-part-",
    ),
    (
        "s2025",
        "shanghai_evolution_market_depth_ric_list_0_ll2_2025-01-01_2026-01-01",
        "merged-Data.csv.gz",
    ),
];

pub fn package_units(package_root: &Path, which: &[String]) -> Result<Vec<Unit>> {
    let mut out = Vec::new();
    for (id, dir, stem) in PACKAGES {
        if !which.iter().any(|w| w == id) {
            continue;
        }
        let d = package_root.join(dir);
        let mut parts: Vec<PathBuf> = fs::read_dir(&d)
            .with_context(|| format!("read {}", d.display()))?
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(stem) && n.ends_with(".csv.gz"))
            })
            .collect();
        parts.sort();
        if parts.is_empty() {
            bail!("no data part in {}", d.display());
        }
        let gz_bytes = parts
            .iter()
            .map(|p| fs::metadata(p).map_or(0, |m| m.len()))
            .sum();
        out.push(Unit {
            kind: UnitKind::Stream {
                id: id.to_string(),
                parts,
            },
            gz_bytes,
        });
    }
    Ok(out)
}

#[derive(Default, Debug)]
pub struct DailyScan {
    pub units: Vec<Unit>,
    /// (product, day) with a product directory but no file for that day.
    pub missing: Vec<(String, String)>,
    pub empty: Vec<(String, String)>,
    pub too_new: Vec<(String, String)>,
    /// Products that were asked for but have no directory yet.
    pub no_dir: Vec<String>,
}

pub fn days_between(start: &str, end: &str) -> Result<Vec<String>> {
    let (a, b) = (
        NaiveDate::parse_from_str(start, "%Y-%m-%d")?,
        NaiveDate::parse_from_str(end, "%Y-%m-%d")?,
    );
    let mut v = Vec::new();
    let mut d = a;
    while d <= b {
        v.push(d.format("%Y-%m-%d").to_string());
        d += Days::days(1);
    }
    Ok(v)
}

pub fn daily_units(
    raw_root: &Path,
    products: Option<&[String]>,
    days: &[String],
) -> Result<DailyScan> {
    let mut names: Vec<String> = match products {
        Some(p) => p.to_vec(),
        None => fs::read_dir(raw_root)?
            .flatten()
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect(),
    };
    names.sort();
    let mut scan = DailyScan::default();
    let now = SystemTime::now();
    for p in names {
        let dir = raw_root.join(&p);
        if !dir.is_dir() {
            scan.no_dir.push(p);
            continue;
        }
        for d in days {
            let f = dir.join(format!("{p}_{d}_normll2.csv.gz"));
            let Ok(m) = fs::metadata(&f) else {
                scan.missing.push((p.clone(), d.clone()));
                continue;
            };
            if m.len() == 0 {
                scan.empty.push((p.clone(), d.clone()));
            } else if m
                .modified()
                .ok()
                .and_then(|t| now.duration_since(t).ok())
                .is_some_and(|age| age < TOO_NEW)
            {
                scan.too_new.push((p.clone(), d.clone()));
            } else {
                scan.units.push(Unit {
                    kind: UnitKind::Daily {
                        product: p.clone(),
                        day: d.clone(),
                    },
                    gz_bytes: m.len(),
                });
            }
        }
    }
    Ok(scan)
}

// ---------------------------------------------------------------- gate

/// Pauses the producers while one of the server's jobs runs, is queued, or is
/// due within the next ten minutes.
pub struct Gate {
    paused: AtomicBool,
    reason: Mutex<String>,
    pub stop: AtomicBool,
}

impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            paused: AtomicBool::new(false),
            reason: Mutex::new(String::new()),
            stop: AtomicBool::new(false),
        })
    }
    pub fn wait(&self) {
        while self.paused.load(Ordering::Relaxed) && !self.stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(5));
        }
    }
    pub fn state(&self) -> (bool, String) {
        (
            self.paused.load(Ordering::Relaxed),
            self.reason.lock().expect("reason lock").clone(),
        )
    }
}

fn http_get(hostport: &str, path: &str) -> Result<String> {
    let mut s = TcpStream::connect_timeout(&hostport.parse()?, Duration::from_secs(5))?;
    s.set_read_timeout(Some(Duration::from_secs(10)))?;
    write!(
        s,
        "GET {path} HTTP/1.0\r\nHost: {hostport}\r\nConnection: close\r\n\r\n"
    )?;
    let mut buf = String::new();
    s.read_to_string(&mut buf)?;
    let body = buf.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    Ok(body.to_string())
}

/// `Some(reason)` when the run must pause.
pub fn pause_reason(status: &Value, now: DateTime<Utc>) -> Option<String> {
    let jobs: Vec<&Value> = match &status["jobs"] {
        Value::Array(a) => a.iter().collect(),
        Value::Object(o) => o.values().collect(),
        _ => return None,
    };
    for j in jobs {
        let name = j["name"].as_str().unwrap_or("?");
        if j["enabled"].as_bool() == Some(false) {
            continue;
        }
        if j["running"].is_object()
            || j["running"].as_bool() == Some(true)
            || j["running"].is_string()
        {
            return Some(format!("job {name} is running"));
        }
        if j["queued"].as_bool() == Some(true) {
            return Some(format!("job {name} is queued"));
        }
        if let Some(t) = j["next_run"]
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        {
            if (t.with_timezone(&Utc) - now).num_seconds() < 600 {
                return Some(format!("job {name} is due at {t}"));
            }
        }
    }
    None
}

/// Polls `http://<hostport>/api/admin/status` every 15 s. An unreachable
/// server keeps the previous state (a first failure does not pause).
pub fn spawn_gate(gate: Arc<Gate>, hostport: String) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !gate.stop.load(Ordering::Relaxed) {
            if let Ok(body) = http_get(&hostport, "/api/admin/status") {
                if let Ok(v) = serde_json::from_str::<Value>(&body) {
                    let r = pause_reason(&v, Utc::now());
                    gate.paused.store(r.is_some(), Ordering::Relaxed);
                    *gate.reason.lock().expect("reason lock") = r.unwrap_or_default();
                }
            }
            for _ in 0..15 {
                if gate.stop.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    })
}

// ---------------------------------------------------------------- progress

pub fn write_json_atomic(path: &Path, v: &Value) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(v)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Writes `path` every 10 s until `gate.stop`.
pub fn spawn_monitor(
    progress: Arc<Progress>,
    gate: Arc<Gate>,
    path: PathBuf,
    meta: Value,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let started = Instant::now();
        let mut samples: VecDeque<(f64, u64)> = VecDeque::new();
        loop {
            let t = started.elapsed().as_secs_f64();
            let read = progress.gz_bytes_read.load(Ordering::Relaxed);
            samples.push_back((t, read));
            while samples.front().is_some_and(|s| t - s.0 > 900.0) {
                samples.pop_front();
            }
            let rate = match (samples.front(), samples.back()) {
                (Some(a), Some(b)) if b.0 > a.0 => (b.1 - a.1) as f64 / (b.0 - a.0),
                _ => 0.0,
            };
            let total = progress.gz_bytes_total.load(Ordering::Relaxed);
            let remaining = total.saturating_sub(read);
            let eta = (rate > 1.0).then(|| (remaining as f64 / rate) as u64);
            let (paused, reason) = gate.state();
            let gb = |b: u64| (b as f64 / 1e9 * 100.0).round() / 100.0;
            let v = json!({
                "updated_utc": Utc::now().to_rfc3339(),
                "state": if gate.stop.load(Ordering::Relaxed) { "finished" } else if paused { "paused" } else { "running" },
                "pause_reason": reason,
                "elapsed_s": t as u64,
                "units_total": progress.units_total.load(Ordering::Relaxed),
                "units_done": progress.units_done.load(Ordering::Relaxed),
                "gz_gb_total": gb(total),
                "gz_gb_read": gb(read),
                "gz_gb_in_finished_units": gb(progress.gz_bytes_done_units.load(Ordering::Relaxed)),
                "pct": if total > 0 { (read as f64 / total as f64 * 1000.0).round() / 10.0 } else { 0.0 },
                "gz_mb_per_s_15min": (rate / 1e6 * 10.0).round() / 10.0,
                "eta_s": eta,
                "rows": progress.rows.load(Ordering::Relaxed),
                "current": *progress.current.lock().expect("current lock"),
                "errors": progress.errors.lock().expect("errors lock").iter().take(20).cloned().collect::<Vec<_>>(),
                "run": meta,
            });
            let _ = write_json_atomic(&path, &v);
            if gate.stop.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(Duration::from_secs(10));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn pauses_for_running_queued_and_imminent_jobs() {
        let now = at("2026-11-17T08:00:00Z");
        let idle = json!({"jobs": [
            {"name": "cn-daily", "running": null, "queued": false, "next_run": "2026-11-17T08:30:00Z"},
            {"name": "lseg-daily", "running": null, "queued": false, "next_run": "2026-11-18T03:00:00Z"}]});
        assert_eq!(pause_reason(&idle, now), None);
        // due within ten minutes: pause; 11 minutes away: run
        assert!(pause_reason(&idle, at("2026-11-17T08:21:00Z"))
            .unwrap()
            .contains("cn-daily"));
        assert_eq!(pause_reason(&idle, at("2026-11-17T08:19:00Z")), None);
        // overdue and not yet running also pauses
        assert!(pause_reason(&idle, at("2026-11-17T08:31:00Z")).is_some());
        let running = json!({"jobs": [{"name": "ib", "running": {"since": "x"}, "queued": false, "next_run": "2026-11-18T01:30:00Z"}]});
        assert!(pause_reason(&running, now).unwrap().contains("running"));
        let queued = json!({"jobs": {"a": {"name": "ib", "running": null, "queued": true, "next_run": "2026-11-18T01:30:00Z"}}});
        assert!(pause_reason(&queued, now).unwrap().contains("queued"));
    }

    #[test]
    fn day_ranges_are_inclusive() {
        let d = days_between("2026-05-30", "2026-06-02").unwrap();
        assert_eq!(d, ["2026-05-30", "2026-05-31", "2026-06-01", "2026-06-02"]);
    }
}
