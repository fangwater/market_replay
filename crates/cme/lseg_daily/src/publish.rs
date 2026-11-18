//! Check a finished `lseg_tas_parquet --layout ric-day` export and publish it.
//!
//! Port of `scripts/lseg/summarize_export.py`, `parquet_delivery_checks.py`
//! and `copy_verified()` from `process_tas_daily.py`:
//! * every file: size, footer row count, schema, per-row-group RIC and UTC day
//!   statistics; exact file set; per-CF and dataset totals;
//! * publish = temp file + fsync + footer check + no-overwrite hard link;
//! * full SHA256 only for first/middle/last file per CF and kind.
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use lseg_delivery::publish as shared;
pub use lseg_delivery::publish::{component, Footer, Identity, Part};

/// Footer checks for TAS files: the UTC day end is exclusive.
pub fn check_footer(path: &Path, part: &Part) -> Result<Footer> {
    shared::check_footer(path, part, false)
}

fn copy_one(source: &Path, target: &Path, part: &Part, full: bool) -> Result<bool> {
    shared::copy_one(source, target, part, full, false)
}

#[derive(Debug, Deserialize)]
struct Report {
    cf: String,
    trade_rows: u64,
    quote_rows: u64,
    verified: bool,
    parts: Vec<Part>,
}

#[derive(Debug)]
pub struct Export {
    pub root: PathBuf,
    /// (CF, parts) in report order.
    pub reports: Vec<(String, Vec<Part>)>,
    pub trade_rows: u64,
    pub quote_rows: u64,
    pub files: u64,
    pub bytes: u64,
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path).with_context(|| format!("read {path:?}"))?)
        .with_context(|| format!("parse {path:?}"))
}

fn walk(dir: &Path, root: &Path, out: &mut BTreeSet<String>, partial: &mut usize) -> Result<()> {
    for e in fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        if e.file_type()?.is_dir() {
            walk(&p, root, out, partial)?;
        } else {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".partial") {
                *partial += 1;
            } else if name.ends_with(".parquet") {
                out.insert(p.strip_prefix(root)?.to_string_lossy().into_owned());
            }
        }
    }
    Ok(())
}

/// Full independent check of a finished local export (`summarize()`).
pub fn check_export(root: &Path) -> Result<Export> {
    let success = read_json(&root.join("_SUCCESS.json"))?;
    let run = read_json(&root.join("_audit/run.json"))?;
    ensure!(success["status"] == "complete", "export not complete");
    ensure!(run["sample"] == false, "sample export");
    ensure!(
        success["layout"] == "ric_utc_day_v1",
        "not a ric-day export"
    );
    let mut reports = Vec::new();
    for e in fs::read_dir(root.join("_audit"))? {
        let p = e?.path();
        let n = p.file_name().unwrap().to_string_lossy();
        if n.starts_with("p%3A") && n.ends_with(".json") {
            let r: Report =
                serde_json::from_slice(&fs::read(&p)?).with_context(|| format!("parse {p:?}"))?;
            reports.push(r);
        }
    }
    reports.sort_by(|a, b| a.cf.cmp(&b.cf));
    let mut cfs: Vec<String> = serde_json::from_value(run["products"].clone())?;
    cfs.sort();
    ensure!(
        reports.iter().map(|r| r.cf.clone()).collect::<Vec<_>>() == cfs,
        "source CF / completed CF sets differ"
    );
    let mut expected = BTreeSet::new();
    let mut schemas: BTreeMap<u8, Identity> = BTreeMap::new();
    let mut owner: BTreeMap<String, String> = BTreeMap::new();
    let (mut trade, mut quote, mut bytes) = (0u64, 0u64, 0u64);
    for r in &reports {
        ensure!(r.verified, "unverified CF {}", r.cf);
        let (mut t, mut q) = (0u64, 0u64);
        for part in &r.parts {
            ensure!(
                expected.insert(part.path.clone()),
                "duplicate file {}",
                part.path
            );
            let kind = match part.kind {
                1 => "trade",
                3 => "quote",
                k => bail!("unexpected kind {k} in {}", part.path),
            };
            let rel = Path::new(&part.path);
            let comps: Vec<String> = rel
                .iter()
                .map(|c| c.to_string_lossy().into_owned())
                .collect();
            ensure!(
                comps.len() == 6
                    && comps[0] == "lseg"
                    && comps[3] == component(&part.ric)
                    && comps[4] == part.utc_date
                    && comps[5] == format!("{kind}.parquet"),
                "invalid RIC/day output path {}",
                part.path
            );
            let path = root.join(rel);
            let f = check_footer(&path, part)?;
            let fields: Vec<String> =
                serde_json::from_value(run[format!("schema_{kind}")].clone())?;
            ensure!(f.names == fields, "footer schema mismatch {}", part.path);
            ensure!(
                fs::metadata(&path)?.len() == part.bytes,
                "size mismatch {}",
                part.path
            );
            match schemas.get(&part.kind) {
                Some(s) => ensure!(*s == f.identity, "inconsistent schema {}", part.path),
                None => {
                    schemas.insert(part.kind, f.identity);
                }
            }
            let o = owner
                .entry(part.ric.clone())
                .or_insert_with(|| r.cf.clone());
            ensure!(*o == r.cf, "RIC {} routed to two CFs", part.ric);
            if part.kind == 1 {
                t += part.rows;
            } else {
                q += part.rows;
            }
            bytes += part.bytes;
        }
        ensure!(
            (t, q) == (r.trade_rows, r.quote_rows),
            "CF counts mismatch {}",
            r.cf
        );
        trade += t;
        quote += q;
    }
    let mut actual = BTreeSet::new();
    let mut partial = 0;
    if root.join("lseg").exists() {
        walk(&root.join("lseg"), root, &mut actual, &mut partial)?;
    }
    ensure!(
        actual == expected && partial == 0,
        "unlisted, missing or unfinished files in {root:?}"
    );
    ensure!(
        Some(trade) == success["trade_rows"].as_u64()
            && Some(quote) == success["quote_rows"].as_u64()
            && Some(expected.len() as u64) == success["files"].as_u64(),
        "dataset totals mismatch"
    );
    ensure!(
        success["source_current"] == success["source_current_after"],
        "source manifest changed during export"
    );
    Ok(Export {
        root: root.to_path_buf(),
        files: expected.len() as u64,
        reports: reports.into_iter().map(|r| (r.cf, r.parts)).collect(),
        trade_rows: trade,
        quote_rows: quote,
        bytes,
    })
}

/// First/middle/last by (UTC day, RIC, path) per CF and kind.
pub fn sampled(export: &Export) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (_, parts) in &export.reports {
        for kind in [1u8, 3] {
            let mut v: Vec<&Part> = parts.iter().filter(|p| p.kind == kind).collect();
            v.sort_by(|a, b| (&a.utc_date, &a.ric, &a.path).cmp(&(&b.utc_date, &b.ric, &b.path)));
            if !v.is_empty() {
                for i in [0, v.len() / 2, v.len() - 1] {
                    out.insert(v[i].path.clone());
                }
            }
        }
    }
    out
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Published {
    pub new_files: u64,
    pub existing_identical: u64,
    pub sha256_checked: u64,
    pub bytes: u64,
}

/// Publishes every part under `delivery_root` (the `lseg/` prefix of the
/// export path is the delivery root itself). `venue` must match the path.
pub fn publish(
    export: &Export,
    delivery_root: &Path,
    venues: &BTreeMap<String, String>,
) -> Result<Published> {
    let samples = sampled(export);
    let mut p = Published::default();
    for (_, parts) in &export.reports {
        for part in parts {
            let rel = Path::new(&part.path).strip_prefix("lseg")?;
            let venue = rel.iter().next().context("venue")?.to_string_lossy();
            ensure!(
                venues.get(&part.ric).map(String::as_str) == Some(venue.as_ref()),
                "{}: exchange directory differs from the batch map",
                part.path
            );
            let full = samples.contains(&part.path);
            let created = copy_one(
                &export.root.join(&part.path),
                &delivery_root.join(rel),
                part,
                full,
            )?;
            if created {
                p.new_files += 1;
            } else {
                p.existing_identical += 1;
            }
            p.sha256_checked += u64::from(full);
            p.bytes += part.bytes;
        }
    }
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_escapes_like_tasdb() {
        assert_eq!(component("CLF0^1"), "CLF0^1");
        assert_eq!(component("A/B C"), "A%2FB%20C");
        assert_eq!(component(".."), "%2E%2E");
    }

    #[test]
    fn sample_picks_first_middle_last_per_kind() {
        let part = |d: &str, r: &str, k: u8| Part {
            path: format!(
                "lseg/x/2026/{r}/{d}/{}.parquet",
                if k == 1 { "trade" } else { "quote" }
            ),
            ric: r.into(),
            kind: k,
            rows: 1,
            bytes: 1,
            utc_date: d.into(),
        };
        let e = Export {
            root: PathBuf::new(),
            reports: vec![(
                "p:2026:X".into(),
                vec![
                    part("20261103", "A", 1),
                    part("20261101", "A", 1),
                    part("20261102", "A", 1),
                    part("20261104", "B", 1),
                    part("20261101", "A", 3),
                ],
            )],
            trade_rows: 0,
            quote_rows: 0,
            files: 5,
            bytes: 5,
        };
        let s = sampled(&e);
        assert_eq!(s.len(), 4);
        assert!(s.contains("lseg/x/2026/A/20261101/trade.parquet"));
        assert!(s.contains("lseg/x/2026/A/20261103/trade.parquet"));
        assert!(s.contains("lseg/x/2026/B/20261104/trade.parquet"));
        assert!(!s.contains("lseg/x/2026/A/20261102/trade.parquet"));
    }
}
