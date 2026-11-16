//! Check a finished `lseg_tas_parquet --layout ric-day` export and publish it.
//!
//! Port of `scripts/lseg/summarize_export.py`, `parquet_delivery_checks.py`
//! and `copy_verified()` from `process_tas_daily.py`:
//! * every file: size, footer row count, schema, per-row-group RIC and UTC day
//!   statistics; exact file set; per-CF and dataset totals;
//! * publish = temp file + fsync + footer check + no-overwrite hard link;
//! * full SHA256 only for first/middle/last file per CF and kind.
use anyhow::{bail, ensure, Context, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const DAY_NS: u64 = 86_400_000_000_000;

#[derive(Debug, Clone, Deserialize)]
pub struct Part {
    pub path: String,
    pub ric: String,
    pub kind: u8,
    pub rows: u64,
    pub bytes: u64,
    pub utc_date: String,
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

/// Lossless escaping of one RIC path component (same as `tasdb::component`).
pub fn component(value: &str) -> String {
    if value == "." || value == ".." {
        return value.bytes().map(|_| "%2E").collect();
    }
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"_-.^".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path).with_context(|| format!("read {path:?}"))?)
        .with_context(|| format!("parse {path:?}"))
}

fn day_start(day: &str) -> Result<u64> {
    let d = chrono::NaiveDate::parse_from_str(day, "%Y%m%d")
        .with_context(|| format!("UTC day {day:?}"))?;
    Ok(u64::try_from(
        d.and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .context("day ns")?,
    )?)
}

/// Column names, row count and a comparable schema/metadata fingerprint.
pub struct Footer {
    pub names: Vec<String>,
    pub rows: u64,
    /// Arrow schema incl. its metadata (order-insensitive) + other footer keys.
    pub identity: Identity,
}

pub type Identity = (arrow_schema::Schema, BTreeMap<String, Option<String>>);

/// Footer checks of `check_footer()`: rows, one RIC per row group, all
/// timestamps inside the part's UTC day.
pub fn check_footer(path: &Path, part: &Part) -> Result<Footer> {
    let reader =
        SerializedFileReader::new(File::open(path).with_context(|| format!("open {path:?}"))?)
            .with_context(|| format!("footer {path:?}"))?;
    let meta = reader.metadata();
    let fm = meta.file_metadata();
    let schema = fm.schema_descr();
    let names: Vec<String> = schema
        .root_schema()
        .get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let ric_i = names
        .iter()
        .position(|n| n == "ric")
        .context("no ric column")?;
    let ts_i = names
        .iter()
        .position(|n| n == "ts_utc_ns")
        .context("no ts_utc_ns column")?;
    ensure!(
        fm.num_rows() as u64 == part.rows,
        "{path:?}: footer rows {} != {}",
        fm.num_rows(),
        part.rows
    );
    let start = day_start(&part.utc_date)?;
    for rg in meta.row_groups() {
        match rg.column(ric_i).statistics() {
            Some(Statistics::ByteArray(s)) => {
                let (lo, hi) = (s.min_opt(), s.max_opt());
                ensure!(
                    lo.zip(hi)
                        .is_some_and(|(a, b)| a.data() == part.ric.as_bytes()
                            && b.data() == part.ric.as_bytes()),
                    "{path:?}: row-group RIC differs from {}",
                    part.ric
                );
            }
            _ => bail!("{path:?}: missing RIC statistics"),
        }
        match rg.column(ts_i).statistics() {
            Some(Statistics::Int64(s)) => {
                let (lo, hi) = s
                    .min_opt()
                    .zip(s.max_opt())
                    .context("missing timestamp min/max")?;
                let (lo, hi) = (*lo as u64, *hi as u64);
                ensure!(
                    start <= lo && lo <= hi && hi < start + DAY_NS,
                    "{path:?}: timestamps outside UTC day {}",
                    part.utc_date
                );
            }
            _ => bail!("{path:?}: missing timestamp statistics"),
        }
    }
    let kv = fm.key_value_metadata();
    let arrow = parquet::arrow::parquet_to_arrow_schema(schema, kv)
        .with_context(|| format!("{path:?}: arrow schema"))?;
    let other: BTreeMap<String, Option<String>> = kv
        .into_iter()
        .flatten()
        .filter(|k| k.key != "ARROW:schema")
        .map(|k| (k.key.clone(), k.value.clone()))
        .collect();
    Ok(Footer {
        names,
        rows: fm.num_rows() as u64,
        identity: (arrow, other),
    })
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

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 8 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Published {
    pub new_files: u64,
    pub existing_identical: u64,
    pub sha256_checked: u64,
    pub bytes: u64,
}

/// Same as an already-published target? Size, footer identity, and the full
/// hash for sampled files. A differing target is never replaced.
fn same(target: &Path, source: &Path, part: &Part, full: bool, src: &Footer) -> Result<()> {
    ensure!(
        fs::metadata(target)?.len() == part.bytes,
        "existing delivery file differs in size (not replaced): {target:?}"
    );
    let f = check_footer(target, part)?;
    ensure!(
        f.identity == src.identity && f.rows == src.rows,
        "existing delivery file has another footer (not replaced): {target:?}"
    );
    if full {
        ensure!(
            sha256_file(target)? == sha256_file(source)?,
            "existing delivery file differs (not replaced): {target:?}"
        );
    }
    Ok(())
}

fn copy_one(source: &Path, target: &Path, part: &Part, full: bool) -> Result<bool> {
    let src = check_footer(source, part)?;
    if target.exists() {
        same(target, source, part, full, &src)?;
        return Ok(false);
    }
    let dir = target.parent().context("target parent")?;
    fs::create_dir_all(dir)?;
    let temp = dir.join(format!(
        ".{}.{}.partial",
        target.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    let result = (|| -> Result<bool> {
        let mut digest = full.then(Sha256::new);
        {
            let mut input = File::open(source)?;
            let mut out = File::options().write(true).create_new(true).open(&temp)?;
            let mut buf = vec![0u8; 8 << 20];
            loop {
                let n = input.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                out.write_all(&buf[..n])?;
                if let Some(d) = digest.as_mut() {
                    d.update(&buf[..n]);
                }
            }
            out.sync_all()?;
        }
        ensure!(
            fs::metadata(&temp)?.len() == part.bytes,
            "copy size {target:?}"
        );
        let f = check_footer(&temp, part)?;
        ensure!(
            f.identity == src.identity,
            "copied footer differs {target:?}"
        );
        if let Some(d) = digest {
            let want = format!("{:x}", d.finalize());
            ensure!(
                sha256_file(&temp)? == want,
                "copied SHA256 differs {target:?}"
            );
            ensure!(
                sha256_file(source)? == want,
                "source changed while copying {source:?}"
            );
        }
        match fs::hard_link(&temp, target) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                same(target, source, part, full, &src)?;
                Ok(false)
            }
            Err(e) => Err(e).with_context(|| format!("publish {target:?}")),
        }
    })();
    let _ = fs::remove_file(&temp);
    result
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
