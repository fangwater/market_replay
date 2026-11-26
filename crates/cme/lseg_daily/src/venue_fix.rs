//! Resolving the `review` exchange directory (user approval 2026-11-26).
//!
//! The delivery puts a RIC-year under `review/` when its venue is unresolved. Two causes:
//! - **one-digit RIC collisions:** a bare RIC such as `TNH2` has HRD versions of two products
//!   ten years apart (`TNH2^1` NYMEX tanker freight 2012, `TNH2^2` CBOT Ultra 10-year 2022),
//!   and the exchange grouping merged all versions without looking at the data date;
//! - **new listings:** contracts listed after the static HRD extraction (2026-06-06) have no
//!   reference at all.
//!
//! Rules, first match wins, each recorded with its evidence:
//! 1. `hrd_by_date`: of the HRD versions of the bare RIC, those whose life (first trading date
//!    .. last trading day, 31 days slack) contains the first data day; one venue -> that venue.
//! 2. `root_follow`: no HRD version covers it; every outright contract of the same root
//!    (`<root><month code><year digits>`) delivered within three years of the data year sits
//!    in one exchange directory -> that directory. Two directories -> stays `review`.
//!
//! Nothing else is inferred; everything unresolved stays `review` with its reason.
use anyhow::{Context, Result};
use chrono::{Datelike, Duration, NaiveDate};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

use crate::venue;

/// One HRD row (a version of a bare RIC) with a known venue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HrdVersion {
    pub ric: String,
    pub venue: String,
    pub first: Option<NaiveDate>,
    pub last: Option<NaiveDate>,
}

/// `TNH2^1` -> `TNH2`.
pub fn bare(ric: &str) -> &str {
    ric.split('^').next().unwrap_or(ric)
}

/// Splits an outright futures RIC into (root, month code, year digits).
pub fn outright(ric: &str) -> Option<(&str, char, &str)> {
    let b = ric.as_bytes();
    let mut end = b.len();
    while end > 0 && b[end - 1].is_ascii_digit() {
        end -= 1;
    }
    let digits = &ric[end..];
    if digits.is_empty() || digits.len() > 2 || end < 2 {
        return None;
    }
    let month = b[end - 1] as char;
    if !"FGHJKMNQUVXZ".contains(month) {
        return None;
    }
    let root = &ric[..end - 1];
    root.bytes()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        .then_some((root, month, digits))
}

fn date(v: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(v, "%m/%d/%Y").ok()
}

/// HRD versions of the requested bare RICs (streams the CSVs).
pub fn hrd_versions(
    paths: &[impl AsRef<Path>],
    bares: &HashSet<String>,
) -> Result<HashMap<String, Vec<HrdVersion>>> {
    let mut out: HashMap<String, Vec<HrdVersion>> = HashMap::new();
    for path in paths {
        let path = path.as_ref();
        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .from_path(path)
            .with_context(|| format!("open {path:?}"))?;
        let headers = rdr.headers()?.clone();
        let pos = |name: &str| {
            headers
                .iter()
                .position(|h| h.trim_start_matches('\u{feff}') == name)
                .with_context(|| format!("{path:?} lacks column {name}"))
        };
        let (ric_i, mic_i, code_i) = (pos("RIC")?, pos("Market MIC")?, pos("Exchange Code")?);
        let (first_i, last_i, exp_i) = (
            pos("First Trading Date")?,
            pos("Last Trading Day")?,
            pos("Expiration Date")?,
        );
        for row in rdr.records() {
            let row = row?;
            let Some(ric) = row.get(ric_i) else { continue };
            if !bares.contains(bare(ric)) {
                continue;
            }
            let mut r = venue::Reference::default();
            if let Some(v) = row.get(mic_i).filter(|v| !v.is_empty()) {
                r.mics.insert(v.to_string());
            }
            if let Some(v) = row.get(code_i).filter(|v| !v.is_empty()) {
                r.codes.insert(v.to_string());
            }
            let v = venue::classify(ric, Some(&r));
            if v == "REVIEW" {
                continue;
            }
            let last = row
                .get(last_i)
                .and_then(date)
                .or_else(|| row.get(exp_i).and_then(date));
            let version = HrdVersion {
                ric: ric.to_string(),
                venue: v.to_lowercase(),
                first: row.get(first_i).and_then(date),
                last,
            };
            let list = out.entry(bare(ric).to_string()).or_default();
            if !list.contains(&version) {
                list.push(version);
            }
        }
    }
    Ok(out)
}

/// Rule 1: venues of the versions whose life contains `day`.
pub fn covering_venues(versions: &[HrdVersion], day: NaiveDate) -> BTreeSet<String> {
    let slack = Duration::days(31);
    versions
        .iter()
        .filter(|v| {
            let Some(last) = v.last else { return false };
            last + slack >= day
                && v.first.is_none_or(|f| f - slack <= day)
                && last - day <= Duration::days(366 * 11)
        })
        .map(|v| v.venue.clone())
        .collect()
}

#[derive(Clone, Debug, Serialize)]
pub struct Resolution {
    pub ric: String,
    pub utc_year: i32,
    pub first_day: String,
    pub last_day: String,
    /// Resolved lower-case directory, or `review`.
    pub venue: String,
    pub rule: String,
    pub evidence: String,
}

/// Venue directories of outright contracts per (root, UTC year) on the delivery disk, from
/// directory names only (no file is opened). `review` is not counted.
pub fn root_venues(
    delivery: &Path,
    years: &BTreeSet<i32>,
) -> Result<HashMap<(String, i32), BTreeMap<String, usize>>> {
    let mut out: HashMap<(String, i32), BTreeMap<String, usize>> = HashMap::new();
    for e in std::fs::read_dir(delivery)? {
        let e = e?;
        let venue = e.file_name().to_string_lossy().into_owned();
        if venue == "review" || venue == "meta" || !e.file_type()?.is_dir() {
            continue;
        }
        for &y in years {
            let dir = e.path().join(y.to_string());
            let Ok(rd) = std::fs::read_dir(&dir) else {
                continue;
            };
            for r in rd {
                let name =
                    lseg_delivery::tasdb::unescape_component(&r?.file_name().to_string_lossy());
                if let Some((root, _, _)) = outright(&name) {
                    *out.entry((root.to_string(), y))
                        .or_default()
                        .entry(venue.clone())
                        .or_default() += 1;
                }
            }
        }
    }
    Ok(out)
}

/// Resolves one review RIC-year.
pub fn resolve(
    ric: &str,
    utc_year: i32,
    first_day: NaiveDate,
    versions: Option<&Vec<HrdVersion>>,
    roots: &HashMap<(String, i32), BTreeMap<String, usize>>,
) -> (String, String, String) {
    let empty = Vec::new();
    let versions = versions.unwrap_or(&empty);
    let covering = covering_venues(versions, first_day);
    let describe = |vs: &[HrdVersion]| {
        vs.iter()
            .map(|v| {
                format!(
                    "{} {} {}..{}",
                    v.ric,
                    v.venue,
                    v.first.map_or("?".into(), |d| d.to_string()),
                    v.last.map_or("?".into(), |d| d.to_string())
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    };
    match covering.len() {
        1 => {
            return (
                covering.into_iter().next().unwrap(),
                "hrd_by_date".into(),
                format!(
                    "first data day {first_day}; HRD versions: {}",
                    describe(versions)
                ),
            )
        }
        n if n > 1 => {
            return (
                "review".into(),
                "unresolved".into(),
                format!(
                    "several HRD venues cover {first_day}: {}",
                    describe(versions)
                ),
            )
        }
        _ => {}
    }
    let Some((root, _, _)) = outright(ric) else {
        return (
            "review".into(),
            "unresolved".into(),
            "no HRD version covers the data and the RIC is not an outright futures code".into(),
        );
    };
    let mut venues: BTreeMap<String, usize> = BTreeMap::new();
    for y in utc_year - 3..=utc_year + 3 {
        if let Some(m) = roots.get(&(root.to_string(), y)) {
            for (v, n) in m {
                *venues.entry(v.clone()).or_default() += n;
            }
        }
    }
    let seen = format!(
        "{} contracts of root {root} in {}..{}: {venues:?}",
        venues.values().sum::<usize>(),
        utc_year - 3,
        utc_year + 3
    );
    let hrd = if versions.is_empty() {
        "no HRD version".to_string()
    } else {
        format!("no HRD version covers {first_day} ({})", describe(versions))
    };
    if venues.len() == 1 {
        (
            venues.into_keys().next().unwrap(),
            "root_follow".into(),
            format!("{hrd}; {seen}"),
        )
    } else {
        (
            "review".into(),
            "unresolved".into(),
            format!("{hrd}; {seen}"),
        )
    }
}

/// Every RIC-year under `<delivery>/review/`, with its first and last data day.
pub fn review_dirs(delivery: &Path) -> Result<Vec<(String, i32, NaiveDate, NaiveDate)>> {
    let mut out = Vec::new();
    let review = delivery.join("review");
    for y in std::fs::read_dir(&review)? {
        let y = y?;
        let Ok(year) = y.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        for r in std::fs::read_dir(y.path())? {
            let r = r?;
            if !r.file_type()?.is_dir() {
                continue;
            }
            let ric = lseg_delivery::tasdb::unescape_component(&r.file_name().to_string_lossy());
            let mut days: Vec<NaiveDate> = std::fs::read_dir(r.path())?
                .filter_map(|d| d.ok())
                .filter_map(|d| {
                    NaiveDate::parse_from_str(&d.file_name().to_string_lossy(), "%Y%m%d").ok()
                })
                .collect();
            days.sort();
            if let (Some(a), Some(b)) = (days.first(), days.last()) {
                ensure_year(*a, year)?;
                out.push((ric, year, *a, *b));
            }
        }
    }
    out.sort();
    Ok(out)
}

fn ensure_year(d: NaiveDate, y: i32) -> Result<()> {
    anyhow::ensure!(d.year() == y, "day {d} under year {y}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn outright_codes() {
        assert_eq!(outright("TNH2"), Some(("TN", 'H', "2")));
        assert_eq!(outright("HGH26"), Some(("HG", 'H', "26")));
        assert_eq!(outright("CFI2F2"), Some(("CFI2", 'F', "2")));
        assert_eq!(outright("ESc1"), None);
        assert_eq!(outright("ESZ6-H7"), None);
        assert_eq!(outright("Z6"), None);
        assert_eq!(outright("ESZ123"), None);
    }

    #[test]
    fn collision_is_resolved_by_the_data_date() {
        let v = vec![
            HrdVersion {
                ric: "TNH2^1".into(),
                venue: "nymex".into(),
                first: None,
                last: Some(d("2012-03-30")),
            },
            HrdVersion {
                ric: "TNH2^2".into(),
                venue: "cbot".into(),
                first: Some(d("2021-03-23")),
                last: Some(d("2022-03-22")),
            },
        ];
        let roots = HashMap::new();
        let (venue, rule, _) = resolve("TNH2", 2022, d("2022-01-01"), Some(&v), &roots);
        assert_eq!((venue.as_str(), rule.as_str()), ("cbot", "hrd_by_date"));
        let (venue, _, _) = resolve("TNH2", 2012, d("2012-01-03"), Some(&v), &roots);
        assert_eq!(venue, "nymex");
        // Both cover: stays review.
        let both = vec![
            HrdVersion {
                ric: "X^1".into(),
                venue: "nymex".into(),
                first: None,
                last: Some(d("2022-06-30")),
            },
            HrdVersion {
                ric: "X^2".into(),
                venue: "cbot".into(),
                first: None,
                last: Some(d("2022-03-22")),
            },
        ];
        assert_eq!(
            resolve("XH2", 2022, d("2022-01-01"), Some(&both), &roots).0,
            "review"
        );
    }

    #[test]
    fn new_listing_follows_a_single_venue_root() {
        let mut roots = HashMap::new();
        roots.insert(
            ("TY".to_string(), 2025),
            BTreeMap::from([("cbot".to_string(), 4)]),
        );
        roots.insert(
            ("TY".to_string(), 2026),
            BTreeMap::from([("cbot".to_string(), 3)]),
        );
        let (venue, rule, _) = resolve("TYH27", 2026, d("2026-11-20"), None, &roots);
        assert_eq!((venue.as_str(), rule.as_str()), ("cbot", "root_follow"));
        roots.insert(
            ("TY".to_string(), 2024),
            BTreeMap::from([("nymex".to_string(), 1)]),
        );
        assert_eq!(
            resolve("TYH27", 2026, d("2026-11-20"), None, &roots).0,
            "review"
        );
        assert_eq!(
            resolve("QQH27", 2026, d("2026-11-20"), None, &roots).0,
            "review"
        );
    }
}

/// Processes that write the delivery disk; `venues-migrate --apply` refuses to run while any
/// of them is alive.
pub const DELIVERY_WRITERS: &[&str] = &[
    "lseg-daily",
    "lseg_tas_parquet",
    "lseg_tas_publish",
    "tas_status_parquet",
    "lseg_kline",
    "lseg_catalog",
    "lseg_l2_slice",
    "ib_kline_export",
];

/// Live processes (pid, command line) whose executable name contains one of `names`,
/// excluding this process.
pub fn running_writers(names: &[&str]) -> Vec<(u32, String)> {
    let me = std::process::id();
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir("/proc") else {
        return out;
    };
    for e in rd.flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if pid == me {
            continue;
        }
        let Ok(raw) = std::fs::read(e.path().join("cmdline")) else {
            continue;
        };
        let args: Vec<String> = raw
            .split(|b| *b == 0)
            .filter(|a| !a.is_empty())
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .collect();
        let Some(exe) = args.first() else { continue };
        let exe = exe.rsplit('/').next().unwrap_or(exe);
        if names.iter().any(|n| exe.starts_with(n))
            && !args
                .iter()
                .any(|a| a == "venues-migrate" || a == "venues-plan")
        {
            out.push((pid, args.join(" ")));
        }
    }
    out
}

/// Every file under `dir`, relative paths, sorted.
pub fn files_under(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).with_context(|| format!("list {}", d.display()))? {
            let e = e?;
            let t = e.file_type()?;
            if t.is_dir() {
                stack.push(e.path());
            } else if t.is_file() {
                out.push(e.path().strip_prefix(dir)?.to_path_buf());
            } else {
                anyhow::bail!("{} is neither a file nor a directory", e.path().display());
            }
        }
    }
    out.sort();
    Ok(out)
}

/// One planned move of a RIC-year directory.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct PlanRow {
    pub ric: String,
    pub utc_year: i32,
    pub venue: String,
    pub rule: String,
    pub evidence: String,
}

/// Derived day files that the kline run rewrites; a differing copy at the target wins.
pub fn derived(rel: &Path) -> bool {
    matches!(
        rel.file_name().and_then(|n| n.to_str()),
        Some("kline.parquet") | Some("session_status.parquet")
    )
}

/// What happens to one file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileStep {
    /// Target missing: hard-link it.
    Link,
    /// Target is already the same inode (an earlier interrupted run).
    AlreadyLinked,
    /// Target is a different file with identical bytes.
    SameBytes,
    /// Target differs: the RIC-year is not moved.
    Conflict,
}

pub fn file_step(src: &Path, dst: &Path) -> Result<FileStep> {
    use std::os::unix::fs::MetadataExt;
    let Ok(d) = std::fs::metadata(dst) else {
        return Ok(FileStep::Link);
    };
    let s = std::fs::metadata(src)?;
    if s.dev() == d.dev() && s.ino() == d.ino() {
        return Ok(FileStep::AlreadyLinked);
    }
    if s.len() == d.len()
        && lseg_delivery::tasdb::file_sha256(src)? == lseg_delivery::tasdb::file_sha256(dst)?
    {
        return Ok(FileStep::SameBytes);
    }
    Ok(FileStep::Conflict)
}

#[cfg(test)]
mod migrate_tests {
    use super::*;

    #[test]
    fn file_steps() {
        let t = tempfile::tempdir().unwrap();
        let (a, b, c, e) = (
            t.path().join("a"),
            t.path().join("b"),
            t.path().join("c"),
            t.path().join("e"),
        );
        std::fs::write(&a, b"x").unwrap();
        assert_eq!(file_step(&a, &b).unwrap(), FileStep::Link);
        std::fs::hard_link(&a, &b).unwrap();
        assert_eq!(file_step(&a, &b).unwrap(), FileStep::AlreadyLinked);
        std::fs::write(&c, b"x").unwrap();
        assert_eq!(file_step(&a, &c).unwrap(), FileStep::SameBytes);
        std::fs::write(&e, b"y").unwrap();
        assert_eq!(file_step(&a, &e).unwrap(), FileStep::Conflict);
        // hard_link never replaces an existing file.
        assert!(std::fs::hard_link(&a, &e).is_err());
        let sub = t.path().join("d/x");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("f"), b"1").unwrap();
        let files = files_under(&t.path().join("d")).unwrap();
        assert_eq!(files, vec![std::path::PathBuf::from("x/f")]);
    }
}

/// A row of the kline venue overrides (`lseg_delivery::native_kline::engine::VENUE_OVERRIDES`).
#[derive(Clone, Debug, serde::Deserialize, Serialize)]
pub struct OverrideRow {
    pub ric: String,
    pub utc_year: i32,
    pub venue: String,
    pub rule: String,
    pub evidence: String,
}
