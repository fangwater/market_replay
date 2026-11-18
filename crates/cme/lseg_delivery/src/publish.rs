//! Publishing one verified local parquet file into the delivery tree.
//!
//! Shared by `lseg-daily` (TAS trade/quote) and the L2 deliver tool, so there is
//! exactly one implementation of the delivery rules: temp file + fsync + footer
//! check (rows, one RIC per row group, timestamps inside the part's UTC day) +
//! no-overwrite hard link (NFS has no RENAME_NOREPLACE). An existing identical
//! file is skipped, an existing different file is never replaced (an error).
use anyhow::{bail, ensure, Context, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};

pub const DAY_NS: u64 = 86_400_000_000_000;

#[derive(Debug, Clone, Deserialize)]
pub struct Part {
    pub path: String,
    pub ric: String,
    pub kind: u8,
    pub rows: u64,
    pub bytes: u64,
    pub utc_date: String,
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

pub fn day_start(day: &str) -> Result<u64> {
    let d = chrono::NaiveDate::parse_from_str(day, "%Y%m%d")
        .with_context(|| format!("UTC day {day:?}"))?;
    Ok(u64::try_from(
        d.and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .context("day out of range")?,
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
/// timestamps inside the part's UTC day. `day_end_inclusive` admits a timestamp
/// exactly at the next midnight (L2 grid points of the last update of a day are
/// rounded up to the grid and may land there); TAS files use `false`.
pub fn check_footer(path: &Path, part: &Part, day_end_inclusive: bool) -> Result<Footer> {
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
    let end = start + DAY_NS - u64::from(!day_end_inclusive);
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
                    start <= lo && lo <= hi && hi <= end,
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

/// Same as an already-published target? Size, footer identity, and the full
/// hash for sampled files. A differing target is never replaced.
pub fn same(
    target: &Path,
    source: &Path,
    part: &Part,
    full: bool,
    src: &Footer,
    day_end_inclusive: bool,
) -> Result<()> {
    ensure!(
        fs::metadata(target)?.len() == part.bytes,
        "existing delivery file differs in size (not replaced): {target:?}"
    );
    let f = check_footer(target, part, day_end_inclusive)?;
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

/// Where the time of one publish went (for the NFS throughput measurement).
#[derive(Debug, Default, Clone, Copy)]
pub struct CopyTiming {
    pub source_footer: Duration,
    pub exists_check: Duration,
    pub mkdir: Duration,
    pub write: Duration,
    pub fsync: Duration,
    pub footer_check: Duration,
    pub link: Duration,
    pub cleanup: Duration,
}

impl CopyTiming {
    pub fn add(&mut self, o: &CopyTiming) {
        self.source_footer += o.source_footer;
        self.exists_check += o.exists_check;
        self.mkdir += o.mkdir;
        self.write += o.write;
        self.fsync += o.fsync;
        self.footer_check += o.footer_check;
        self.link += o.link;
        self.cleanup += o.cleanup;
    }
}

/// Publishes `source` as `target`; `Ok(true)` = newly created, `Ok(false)` =
/// an identical file already existed.
pub fn copy_one(
    source: &Path,
    target: &Path,
    part: &Part,
    full: bool,
    day_end_inclusive: bool,
) -> Result<bool> {
    copy_one_timed(source, target, part, full, day_end_inclusive).map(|(created, _)| created)
}

pub fn copy_one_timed(
    source: &Path,
    target: &Path,
    part: &Part,
    full: bool,
    day_end_inclusive: bool,
) -> Result<(bool, CopyTiming)> {
    let mut t = CopyTiming::default();
    let clock = Instant::now();
    let src = check_footer(source, part, day_end_inclusive)?;
    t.source_footer = clock.elapsed();
    let clock = Instant::now();
    let exists = target.exists();
    t.exists_check = clock.elapsed();
    if exists {
        same(target, source, part, full, &src, day_end_inclusive)?;
        return Ok((false, t));
    }
    let dir = target.parent().context("target parent")?;
    let clock = Instant::now();
    fs::create_dir_all(dir)?;
    t.mkdir = clock.elapsed();
    let temp = dir.join(format!(
        ".{}.{}.partial",
        target.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    let result = (|| -> Result<bool> {
        let mut digest = full.then(Sha256::new);
        {
            let clock = Instant::now();
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
            t.write = clock.elapsed();
            let clock = Instant::now();
            out.sync_all()?;
            t.fsync = clock.elapsed();
        }
        let clock = Instant::now();
        ensure!(
            fs::metadata(&temp)?.len() == part.bytes,
            "copy size {target:?}"
        );
        let f = check_footer(&temp, part, day_end_inclusive)?;
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
        t.footer_check = clock.elapsed();
        let clock = Instant::now();
        let linked = match fs::hard_link(&temp, target) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                same(target, source, part, full, &src, day_end_inclusive)?;
                Ok(false)
            }
            Err(e) => Err(e).with_context(|| format!("publish {target:?}")),
        };
        t.link = clock.elapsed();
        linked
    })();
    let clock = Instant::now();
    let _ = fs::remove_file(&temp);
    t.cleanup = clock.elapsed();
    result.map(|created| (created, t))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    fn write(path: &Path, ric: &str, ts: &[i64]) -> Part {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ric", DataType::Utf8, false),
            Field::new("ts_utc_ns", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![ric; ts.len()])),
                Arc::new(Int64Array::from(ts.to_vec())),
            ],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        Part {
            path: "x".into(),
            ric: ric.into(),
            kind: 0,
            rows: ts.len() as u64,
            bytes: fs::metadata(path).unwrap().len(),
            utc_date: "20261110".into(),
        }
    }

    #[test]
    fn component_escapes_like_tasdb() {
        assert_eq!(component("CLF0^1"), "CLF0^1");
        assert_eq!(component("A/B C"), "A%2FB%20C");
        assert_eq!(component(".."), "%2E%2E");
    }

    #[test]
    fn footer_day_end_is_exclusive_unless_asked() {
        let t = tempfile::tempdir().unwrap();
        let start = day_start("20261110").unwrap() as i64;
        let f = t.path().join("a.parquet");
        let at_midnight = write(&f, "ESZ26", &[start + 1, start + DAY_NS as i64]);
        assert!(check_footer(&f, &at_midnight, false).is_err());
        assert!(check_footer(&f, &at_midnight, true).is_ok());
        let wrong_ric = Part {
            ric: "ESH27".into(),
            ..at_midnight.clone()
        };
        assert!(check_footer(&f, &wrong_ric, true).is_err());
    }

    #[test]
    fn publish_never_overwrites_and_skips_identical() {
        let t = tempfile::tempdir().unwrap();
        let start = day_start("20261110").unwrap() as i64;
        let src = t.path().join("src.parquet");
        let part = write(&src, "ESZ26", &[start + 5, start + 10]);
        let target = t
            .path()
            .join("lseg/cme/2026/ESZ26/20261110/l2_250ms.parquet");
        assert!(copy_one(&src, &target, &part, true, true).unwrap());
        assert!(
            !copy_one(&src, &target, &part, true, true).unwrap(),
            "identical: skipped"
        );
        assert!(fs::read_dir(target.parent().unwrap()).unwrap().all(|e| !e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".partial")));
        // A different file at the same target is an error and stays untouched.
        let other = t.path().join("other.parquet");
        let other_part = write(&other, "ESZ26", &[start + 7]);
        let before = fs::read(&target).unwrap();
        assert!(copy_one(&other, &target, &other_part, false, true).is_err());
        assert_eq!(fs::read(&target).unwrap(), before);
    }
}
