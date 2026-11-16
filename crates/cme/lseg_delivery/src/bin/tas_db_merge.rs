//! Monthly merge of lseg-daily TAS RocksDBs and its staging checks
//! (driven by `lseg-daily merge`; see docs/lseg/lseg_daily.md).
//!
//! - `merge`: k-way merge of the spec's sources into a fresh DB, full
//!   bottommost compaction, read-only count/digest verification, report JSON.
//! - `seed`: copy the delivered kline/session_status files of one (year,
//!   product) group from `--from` on into a staging root (read-only source).
//! - `check-staging`: after `lseg_kline run --backup-root` on the staging root,
//!   every seeded file must be byte-identical, nothing replaced or added.
use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use cme_tas_replay::product::parse_product;
use lseg_delivery::{limits, merge, tasdb};
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Merge {
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        report: PathBuf,
    },
    Seed {
        /// Delivered root (read only).
        #[arg(long)]
        from_root: PathBuf,
        #[arg(long)]
        staging: PathBuf,
        #[arg(long)]
        year: i32,
        /// CF product (p:<year>:<product>).
        #[arg(long)]
        product: String,
        /// First UTC day YYYY-MM-DD.
        #[arg(long)]
        from: String,
        #[arg(long)]
        list: PathBuf,
    },
    CheckStaging {
        #[arg(long)]
        staging: PathBuf,
        #[arg(long)]
        year: i32,
        #[arg(long)]
        product: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        list: PathBuf,
        #[arg(long)]
        replaced_log: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

const FILES: [&str; 2] = ["kline.parquet", "session_status.parquet"];

fn unescape(c: &str) -> Option<String> {
    let b = c.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let h = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(h, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Day files `<venue>/<year>/<ric>/<YYYYMMDD>/{kline,session_status}.parquet`
/// of RICs routed to `product`, from `from` (YYYYMMDD) on; relative paths.
fn group_files(root: &Path, year: i32, product: &str, from: &str) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for venue in fs::read_dir(root).with_context(|| format!("list {}", root.display()))? {
        let venue = venue?;
        let name = venue.file_name().to_string_lossy().into_owned();
        if name == "meta" || !venue.file_type()?.is_dir() {
            continue;
        }
        let ydir = venue.path().join(year.to_string());
        if !ydir.is_dir() {
            continue;
        }
        for ric in fs::read_dir(&ydir)? {
            let ric = ric?;
            let rname = ric.file_name().to_string_lossy().into_owned();
            let Some(raw) = unescape(&rname) else {
                continue;
            };
            if parse_product(&raw).as_deref() != Some(product) {
                continue;
            }
            for day in fs::read_dir(ric.path())? {
                let day = day?;
                let d = day.file_name().to_string_lossy().into_owned();
                if d.len() != 8 || !d.bytes().all(|b| b.is_ascii_digit()) || d.as_str() < from {
                    continue;
                }
                for f in FILES {
                    if day.path().join(f).is_file() {
                        out.push(
                            PathBuf::from(&name)
                                .join(year.to_string())
                                .join(&rname)
                                .join(&d)
                                .join(f),
                        );
                    }
                }
            }
        }
    }
    out.sort();
    Ok(out)
}

fn ymd(d: &str) -> Result<String> {
    Ok(chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")
        .with_context(|| format!("date {d}"))?
        .format("%Y%m%d")
        .to_string())
}

fn hashes(root: &Path, files: &[PathBuf]) -> Result<BTreeMap<String, String>> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(limits::IO_THREADS)
        .build()?;
    pool.install(|| {
        files
            .par_iter()
            .map(|f| Ok((f.display().to_string(), tasdb::file_sha256(&root.join(f))?)))
            .collect()
    })
}

fn main() -> Result<()> {
    match Args::parse().cmd {
        Cmd::Merge { spec, report } => {
            let s: merge::Spec = serde_json::from_slice(&fs::read(&spec)?)
                .with_context(|| format!("parse {}", spec.display()))?;
            match merge::merge(&s) {
                Ok(r) => {
                    tasdb::save_json(&report, &r)?;
                    println!(
                        "merged {} sources into {}: {} SST files, {} bytes",
                        s.sources.len(),
                        s.target.display(),
                        r.target_dir.sst_files,
                        r.target_dir.bytes
                    );
                    Ok(())
                }
                Err(e) => {
                    let _ = tasdb::save_json(
                        &report,
                        &serde_json::json!({"status": "failed", "target": s.target, "error": format!("{e:#}")}),
                    );
                    Err(e)
                }
            }
        }
        Cmd::Seed {
            from_root,
            staging,
            year,
            product,
            from,
            list,
        } => {
            ensure!(
                !staging.starts_with(&from_root) && !from_root.starts_with(&staging),
                "staging overlaps the delivered root"
            );
            let files = group_files(&from_root, year, &product, &ymd(&from)?)?;
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(limits::IO_THREADS)
                .build()?;
            pool.install(|| {
                files.par_iter().try_for_each(|f| -> Result<()> {
                    let to = staging.join(f);
                    ensure!(!to.exists(), "staging file exists: {}", to.display());
                    fs::create_dir_all(to.parent().unwrap())?;
                    fs::copy(from_root.join(f), &to)?;
                    Ok(())
                })
            })?;
            let h = hashes(&staging, &files)?;
            tasdb::save_json(
                &list,
                &serde_json::json!({"from_root": from_root, "year": year, "product": product, "from": from, "files": h}),
            )?;
            println!("seeded {} files", h.len());
            Ok(())
        }
        Cmd::CheckStaging {
            staging,
            year,
            product,
            from,
            list,
            replaced_log,
            output,
        } => {
            let seeded: serde_json::Value = serde_json::from_slice(&fs::read(&list)?)?;
            let want: BTreeMap<String, String> = serde_json::from_value(seeded["files"].clone())?;
            let files = group_files(&staging, year, &product, &ymd(&from)?)?;
            let got = hashes(&staging, &files)?;
            let replaced = match fs::read_to_string(&replaced_log) {
                Ok(t) => t.lines().filter(|l| !l.trim().is_empty()).count(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                Err(e) => return Err(e.into()),
            };
            let missing: Vec<_> = want
                .keys()
                .filter(|k| !got.contains_key(*k))
                .cloned()
                .collect();
            let extra: Vec<_> = got
                .keys()
                .filter(|k| !want.contains_key(*k))
                .cloned()
                .collect();
            let changed: Vec<_> = want
                .iter()
                .filter(|(k, v)| got.get(*k).is_some_and(|g| g != *v))
                .map(|(k, _)| k.clone())
                .collect();
            let ok = replaced == 0 && missing.is_empty() && extra.is_empty() && changed.is_empty();
            let head = |v: &[String]| v.iter().take(20).cloned().collect::<Vec<_>>();
            tasdb::save_json(
                &output,
                &serde_json::json!({"status": if ok {"identical"} else {"differs"}, "files": want.len(),
                    "replaced": replaced, "missing": missing.len(), "extra": extra.len(), "changed": changed.len(),
                    "missing_head": head(&missing), "extra_head": head(&extra), "changed_head": head(&changed)}),
            )?;
            ensure!(
                ok,
                "staging differs from the delivered files: replaced={replaced} missing={} extra={} changed={} (see {})",
                missing.len(),
                extra.len(),
                changed.len(),
                output.display()
            );
            println!("identical: {} files", want.len());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_files_filter_product_year_and_day() {
        let d = tempfile::tempdir().unwrap();
        let mk = |p: &str| {
            let p = d.path().join(p);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, b"x").unwrap();
        };
        mk("cme/2026/ESZ6/20261101/kline.parquet");
        mk("cme/2026/ESZ6/20261101/session_status.parquet");
        mk("cme/2026/ESZ6/20261101/trade.parquet");
        mk("cme/2026/ESZ6/20261031/kline.parquet");
        mk("cme/2026/NQZ6/20261101/kline.parquet");
        mk("cme/2025/ESZ5/20261101/kline.parquet");
        mk("meta/2026/ESZ6/20261101/kline.parquet");
        let f = group_files(d.path(), 2026, "ES", "20261101").unwrap();
        assert_eq!(
            f,
            vec![
                PathBuf::from("cme/2026/ESZ6/20261101/kline.parquet"),
                PathBuf::from("cme/2026/ESZ6/20261101/session_status.parquet")
            ]
        );
        assert_eq!(unescape("A%2FB").as_deref(), Some("A/B"));
    }
}
