//! The daily run: which products / days to build, state, change detection, publication.
use crate::{build_day, read_day, Kind};
use anyhow::{bail, ensure, Context, Result};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, UNIX_EPOCH};

/// First trading day of the research data (user decision 2026-10-14).
pub const START: &str = "20210101";

pub struct Options {
    pub config: PathBuf,
    pub universe: PathBuf,
    pub workers: usize,
    pub rebuild: bool,
    pub product: Option<Vec<String>>,
    /// Write below this root instead of the live datasets: no state is read for the decision or updated.
    pub out_root: Option<PathBuf>,
    /// host:port of the server whose job state gates manual runs.
    pub gate: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct ProductState {
    through: String,
    /// day -> [mtime_ns, size] of every 1min HFQ source file seen at the last run.
    files: BTreeMap<String, [i64; 2]>,
    updated_utc: String,
}

#[derive(Serialize, Deserialize, Default)]
struct State {
    products: BTreeMap<String, ProductState>,
}

struct Job {
    exchange: String,
    product: String,
    out_days: Vec<String>,
    rebuild: bool,
    signatures: BTreeMap<String, [i64; 2]>,
}

fn signature(path: &Path) -> Result<[i64; 2]> {
    let meta = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let mtime = meta.modified()?.duration_since(UNIX_EPOCH)?.as_nanos() as i64;
    Ok([mtime, meta.len() as i64])
}

fn day_stems(dir: &Path, through: &str) -> Result<Vec<String>> {
    let mut days: Vec<String> = fs::read_dir(dir)
        .with_context(|| format!("read {}", dir.display()))?
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let stem = name.strip_suffix(".parquet")?;
            (stem.len() == 8
                && stem.bytes().all(|b| b.is_ascii_digit())
                && stem >= START
                && stem <= through)
                .then(|| stem.to_string())
        })
        .collect();
    days.sort();
    Ok(days)
}

/// Writes `bytes` to `path` through a temporary file in the same directory, unless the file already holds them.
fn publish_bytes(path: &Path, bytes: &[u8]) -> Result<bool> {
    if let Ok(existing) = fs::read(path) {
        if existing == bytes {
            return Ok(false);
        }
    }
    let dir = path.parent().context("no parent directory")?;
    fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(true)
}

fn process(job: &Job, data: &Path, out: &Path) -> Result<(usize, usize)> {
    let source = data
        .join("baseline_data_1min_hfq")
        .join(&job.exchange)
        .join(&job.product);
    let (mut files, mut written) = (0usize, 0usize);
    for day in &job.out_days {
        let bytes = fs::read(source.join(format!("{day}.parquet")))?;
        let parsed =
            read_day(bytes).with_context(|| format!("{}/{} {day}", job.exchange, job.product))?;
        ensure!(
            parsed.contract_id == job.product,
            "unexpected product in {day}"
        );
        let number: i64 = day.parse()?;
        for kind in Kind::ALL {
            let built = build_day(&parsed, kind, number).with_context(|| {
                format!("{}/{} {day} {}", job.exchange, job.product, kind.dataset())
            })?;
            let base = out.join(kind.dataset());
            let bar = base
                .join(&job.exchange)
                .join(&job.product)
                .join(format!("{day}.parquet"));
            let seg = base
                .join("_segments")
                .join(&job.exchange)
                .join(&job.product)
                .join(format!("{day}.json"));
            written += usize::from(publish_bytes(&bar, &built.parquet)?);
            written += usize::from(publish_bytes(&seg, &built.segments_json)?);
            files += 2;
        }
    }
    Ok((files, written))
}

fn server_busy(address: &str) -> bool {
    let fetch = || -> Option<serde_json::Value> {
        let mut stream = TcpStream::connect(address).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
        write!(
            stream,
            "GET /api/admin/status HTTP/1.0\r\nHost: {address}\r\n\r\n"
        )
        .ok()?;
        let mut text = String::new();
        stream.read_to_string(&mut text).ok()?;
        serde_json::from_str(text.split("\r\n\r\n").nth(1)?).ok()
    };
    let Some(status) = fetch() else { return false };
    let jobs: Vec<&serde_json::Value> = match &status["jobs"] {
        serde_json::Value::Array(a) => a.iter().collect(),
        serde_json::Value::Object(o) => o.values().collect(),
        _ => Vec::new(),
    };
    jobs.iter().any(|j| {
        j["running"].as_bool() == Some(true)
            || j["queued"].as_bool() == Some(true)
            || j["queued"].as_i64().is_some_and(|n| n > 0)
    })
}

pub fn run(o: &Options) -> Result<()> {
    let cfg: toml::Value = toml::from_str(&fs::read_to_string(&o.config)?)?;
    let baseline = PathBuf::from(cfg["baseline_root"].as_str().context("baseline_root")?);
    let data = baseline
        .parent()
        .context("baseline_root has no parent")?
        .to_path_buf();
    let manifest: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        data.join("baseline_data_1min_hfq/_provenance/hfq/generation.json"),
    )?)?;
    let through = manifest["through"]
        .as_str()
        .context("through")?
        .replace('-', "");
    let universe: serde_json::Value = serde_json::from_str(&fs::read_to_string(&o.universe)?)?;
    let mut products: Vec<(String, String)> = Vec::new();
    for (exchange, group) in universe["included_products_by_exchange"]
        .as_object()
        .context("universe")?
    {
        for p in group.as_array().context("universe products")? {
            products.push((exchange.clone(), p.as_str().context("product")?.to_string()));
        }
    }
    if let Some(wanted) = &o.product {
        products.retain(|(_, p)| wanted.contains(p));
    }
    // Verification runs (--out-root) only read the live data and write elsewhere: no lock needed.
    let _lock = if o.out_root.is_none() {
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(data.join(".cn-hfq-multi.lock"))?;
        if file.try_lock().is_err() {
            bail!("another cn-hfq-bars run holds the lock");
        }
        Some(file)
    } else {
        None
    };
    let state_path = data.join("_provenance/hfq_multi/state.json");
    let state: State = match fs::read_to_string(&state_path) {
        Ok(t) => serde_json::from_str(&t)?,
        Err(_) => State::default(),
    };
    let mut jobs = Vec::new();
    for (exchange, product) in &products {
        let dir = data
            .join("baseline_data_1min_hfq")
            .join(exchange)
            .join(product);
        let days = day_stems(&dir, &through)?;
        if days.is_empty() {
            continue;
        }
        let mut signatures = BTreeMap::new();
        for d in &days {
            signatures.insert(d.clone(), signature(&dir.join(format!("{d}.parquet")))?);
        }
        let (rebuild, out_days) = match state.products.get(&format!("{exchange}/{product}")) {
            Some(old) if !o.rebuild && o.out_root.is_none() => {
                let changed = days
                    .iter()
                    .any(|d| *d <= old.through && old.files.get(d) != signatures.get(d))
                    || old.files.keys().any(|d| !signatures.contains_key(d));
                if changed {
                    (true, days.clone())
                } else {
                    (
                        false,
                        days.iter().filter(|d| **d > old.through).cloned().collect(),
                    )
                }
            }
            _ => (true, days.clone()),
        };
        if !out_days.is_empty() {
            jobs.push(Job {
                exchange: exchange.clone(),
                product: product.clone(),
                out_days,
                rebuild,
                signatures,
            });
        }
    }
    println!(
        "{}",
        json!({"through": through, "products": products.len(), "to_process": jobs.len(),
               "rebuild": jobs.iter().filter(|j| j.rebuild).count()})
    );
    let out = o.out_root.clone().unwrap_or_else(|| data.clone());
    let started = Instant::now();
    let shared = Mutex::new((state, 0usize, Vec::<String>::new()));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(o.workers)
        .build()?;
    let total = jobs.len();
    pool.install(|| {
        jobs.par_iter().for_each(|job| {
            while o.gate.as_deref().is_some_and(server_busy) {
                std::thread::sleep(Duration::from_secs(30));
            }
            let t0 = Instant::now();
            let result = process(job, &data, &out);
            let mut guard = shared.lock().expect("state lock");
            match result {
                Ok((files, written)) => {
                    guard.1 += 1;
                    println!(
                        "{}",
                        json!({"done": guard.1, "of": total, "exchange": job.exchange, "product": job.product,
                               "rebuild": job.rebuild, "days": job.out_days.len(), "files": files, "written": written,
                               "seconds": t0.elapsed().as_secs_f64().round()})
                    );
                    if o.out_root.is_none() {
                        guard.0.products.insert(
                            format!("{}/{}", job.exchange, job.product),
                            ProductState {
                                through: through.clone(),
                                files: job.signatures.clone(),
                                updated_utc: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            },
                        );
                        let save = || -> Result<()> {
                            fs::create_dir_all(state_path.parent().unwrap())?;
                            let tmp = state_path.with_extension("json.tmp");
                            fs::write(&tmp, serde_json::to_vec(&guard.0)?)?;
                            fs::rename(&tmp, &state_path)?;
                            Ok(())
                        };
                        if let Err(e) = save() {
                            guard.2.push(format!("state: {e:#}"));
                        }
                    }
                }
                Err(e) => {
                    let message = format!("{}/{}: {e:#}", job.exchange, job.product);
                    eprintln!("FAILED {message}");
                    guard.2.push(message);
                }
            }
        });
    });
    let (_, done, errors) = shared.into_inner().expect("state lock");
    println!(
        "{}",
        json!({"finished": true, "products": done, "failed": errors.len(), "seconds": started.elapsed().as_secs()})
    );
    if !errors.is_empty() {
        bail!("{} product(s) failed: {}", errors.len(), errors.join("; "));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::day_parquet;

    struct Env {
        _dir: tempfile::TempDir,
        data: PathBuf,
        options: Options,
    }

    fn write_day(data: &Path, day: &str, seed: i64) {
        let dir = data.join("baseline_data_1min_hfq/xsge/RB");
        fs::create_dir_all(&dir).unwrap();
        let base = 1_780_000_000 - 1_780_000_000 % 60 + seed * 86_400;
        let minutes: Vec<i64> = (0..30).map(|i| base + i * 60).collect();
        fs::write(
            dir.join(format!("{day}.parquet")),
            day_parquet("RB", &minutes, 1),
        )
        .unwrap();
    }

    fn env(days: &[&str]) -> Env {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        fs::create_dir_all(data.join("baseline_data_1min_hfq/_provenance/hfq")).unwrap();
        fs::write(
            data.join("baseline_data_1min_hfq/_provenance/hfq/generation.json"),
            json!({"through": "2026-08-28"}).to_string(),
        )
        .unwrap();
        for (i, d) in days.iter().enumerate() {
            write_day(&data, d, i as i64);
        }
        let config = dir.path().join("cn_daily.toml");
        fs::write(
            &config,
            format!(
                "baseline_root = \"{}\"\n",
                data.join("baseline_data_1min").display()
            ),
        )
        .unwrap();
        let universe = dir.path().join("universe.json");
        fs::write(
            &universe,
            json!({"included_products_by_exchange": {"xsge": ["RB"]}}).to_string(),
        )
        .unwrap();
        let options = Options {
            config,
            universe,
            workers: 1,
            rebuild: false,
            product: None,
            out_root: None,
            gate: None,
        };
        Env {
            _dir: dir,
            data,
            options,
        }
    }

    fn bar_files(data: &Path, dataset: &str) -> Vec<String> {
        let mut v: Vec<String> = fs::read_dir(data.join(dataset).join("xsge/RB"))
            .map(|d| {
                d.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    fn mtime(path: &Path) -> std::time::SystemTime {
        fs::metadata(path).unwrap().modified().unwrap()
    }

    #[test]
    fn first_run_builds_everything_then_only_new_days_then_a_rebuild_when_an_old_day_changes() {
        let e = env(&["20260825", "20260826"]);
        run(&e.options).unwrap();
        for kind in Kind::ALL {
            assert_eq!(
                bar_files(&e.data, kind.dataset()),
                vec!["20260825.parquet", "20260826.parquet"]
            );
            assert!(e
                .data
                .join(kind.dataset())
                .join("_segments/xsge/RB/20260826.json")
                .is_file());
        }
        let first = e
            .data
            .join("baseline_data_5min_hfq/xsge/RB/20260825.parquet");
        let stamp = mtime(&first);
        std::thread::sleep(Duration::from_millis(30));

        // nothing new: no file is touched
        run(&e.options).unwrap();
        assert_eq!(mtime(&first), stamp);

        // a new day: only that day is built, the old files stay as they are
        write_day(&e.data, "20260827", 2);
        run(&e.options).unwrap();
        assert_eq!(mtime(&first), stamp);
        assert_eq!(bar_files(&e.data, "baseline_data_1day_hfq").len(), 3);

        // an old 1min file changes (a roll): the product is rebuilt, files with new bytes are replaced
        std::thread::sleep(Duration::from_millis(30));
        let dir = e.data.join("baseline_data_1min_hfq/xsge/RB");
        let base = 1_780_000_000 - 1_780_000_000 % 60;
        let minutes: Vec<i64> = (0..31).map(|i| base + i * 60).collect();
        fs::write(dir.join("20260825.parquet"), day_parquet("RB", &minutes, 1)).unwrap();
        run(&e.options).unwrap();
        assert_ne!(mtime(&first), stamp);
        let state: State = serde_json::from_str(
            &fs::read_to_string(e.data.join("_provenance/hfq_multi/state.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(state.products["xsge/RB"].through, "20260828");
        assert_eq!(state.products["xsge/RB"].files.len(), 3);
    }

    #[test]
    fn an_out_root_run_writes_elsewhere_and_keeps_no_state() {
        let mut e = env(&["20260825"]);
        let out = e.data.join("check");
        e.options.out_root = Some(out.clone());
        run(&e.options).unwrap();
        assert!(out
            .join("baseline_data_15min_hfq/xsge/RB/20260825.parquet")
            .is_file());
        assert!(!e.data.join("baseline_data_15min_hfq").exists());
        assert!(!e.data.join("_provenance/hfq_multi/state.json").exists());
    }
}
