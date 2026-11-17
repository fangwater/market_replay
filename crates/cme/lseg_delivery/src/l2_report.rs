//! Verification and report of the L2 backfill (`lseg_l2_slice report`).
//!
//! For every database (the period packages' stream databases and the per-day
//! databases of the daily files) it
//! * reads the completion markers (what the pipeline counted while washing: rows,
//!   RICs, first/last time, per-day row counts and crc sums),
//! * scans every RIC's update records (the first varint of each stored second)
//!   and compares them with the markers, per RIC and UTC month,
//! * looks for RICs with data but without a marker,
//!
//! Then it compares the (RIC, UTC day) digests of the different inputs with each other
//! (identical rows = duplicate, overlapping time with other content = conflict).
use crate::l2_backfill::{days_between, write_json_atomic};
use crate::l2_db::{self, month_of_day};
use crate::l2_layout::get_varint;
use crate::tasdb::{SharedCache, Tuning};
use anyhow::{Context, Result};
use rocksdb::{Direction, IteratorMode, ReadOptions, DB};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

pub struct ReportConfig {
    pub db_root: PathBuf,
    pub raw_root: PathBuf,
    pub daily_start: String,
    pub daily_end: String,
    pub gaps: Option<PathBuf>,
    pub spot_checks: Option<PathBuf>,
    /// The backfill's stdout log (failed units are counted from it).
    pub log: Option<PathBuf>,
    pub out_json: PathBuf,
    pub out_md: PathBuf,
    pub workers: usize,
    pub cache_mb: usize,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
struct Digest {
    count: u64,
    first: i64,
    last: i64,
    crc: u64,
}

#[derive(Default)]
struct RicExp {
    updates: u64,
    first: i64,
    last: i64,
    months: BTreeMap<u32, u64>,
    days: Vec<(i64, Digest)>,
}

struct Source {
    /// "p2026h1", "p2025", "s2025", or "daily".
    input: String,
    /// Unit label for the overlap analysis.
    dir: PathBuf,
    rics: BTreeMap<String, RicExp>,
    /// Stream databases: the unit marker (None = the stream is not finished).
    unit_marker: Option<Value>,
    daily: bool,
    gz_bytes: u64,
}

fn month_of_sec(sec: u32) -> u32 {
    month_of_day((sec / 86_400) as i64)
}

fn read_stream(dir: &Path, id: &str, cache: &SharedCache) -> Result<Source> {
    let db = l2_db::open_read(dir, cache)?;
    let unit_marker = db
        .get(l2_db::marker_key(id))?
        .map(|v| serde_json::from_slice::<Value>(&v))
        .transpose()?;
    let mut rics = BTreeMap::new();
    for item in db.iterator(IteratorMode::From(b"R|", Direction::Forward)) {
        let (k, v) = item?;
        if !k.starts_with(b"R|") {
            break;
        }
        let ric = String::from_utf8_lossy(&k[2..]).into_owned();
        let v: Value = serde_json::from_slice(&v)?;
        let mut e = RicExp {
            updates: v["updates"].as_u64().unwrap_or(0),
            first: v["first_ts"].as_i64().unwrap_or(0),
            last: v["last_ts"].as_i64().unwrap_or(0),
            ..Default::default()
        };
        if let Some(m) = v["months"].as_object() {
            for (k, n) in m {
                e.months.insert(k.parse()?, n.as_u64().unwrap_or(0));
            }
        }
        if let Some(d) = v["days"].as_array() {
            for x in d {
                e.days.push((
                    x[0].as_i64().unwrap_or(0),
                    Digest {
                        count: x[1].as_u64().unwrap_or(0),
                        first: x[2].as_i64().unwrap_or(0),
                        last: x[3].as_i64().unwrap_or(0),
                        crc: x[4].as_u64().unwrap_or(0),
                    },
                ));
            }
        }
        rics.insert(ric, e);
    }
    let gz_bytes = unit_marker
        .as_ref()
        .and_then(|m| m["input_gz_bytes"].as_u64())
        .unwrap_or(0);
    Ok(Source {
        input: id.to_string(),
        dir: dir.to_path_buf(),
        rics,
        unit_marker,
        daily: false,
        gz_bytes,
    })
}

/// A day database: one marker (`M<product>`) per product-day.
fn read_day(dir: &Path, cache: &SharedCache) -> Result<(Source, Vec<(String, String)>)> {
    let db = l2_db::open_read(dir, cache)?;
    let mut rics = BTreeMap::new();
    let mut washed = Vec::new();
    for item in db.iterator(IteratorMode::From(b"M", Direction::Forward)) {
        let (k, v) = item?;
        if !k.starts_with(b"M") {
            break;
        }
        let v: Value = serde_json::from_slice(&v)?;
        let day = v["day"].as_str().unwrap_or("").to_string();
        let product = v["product"].as_str().unwrap_or("").to_string();
        let dn = crate::l2_slice::day_number(&day)?;
        for r in v["rics"].as_array().into_iter().flatten() {
            let ric = r[0].as_str().unwrap_or("").to_string();
            let d = Digest {
                count: r[1].as_u64().unwrap_or(0),
                first: r[2].as_i64().unwrap_or(0),
                last: r[3].as_i64().unwrap_or(0),
                crc: r[4].as_u64().unwrap_or(0),
            };
            let e = rics.entry(ric).or_insert_with(RicExp::default);
            e.updates += d.count;
            e.first = d.first;
            e.last = d.last;
            *e.months.entry(month_of_day(dn)).or_default() += d.count;
            e.days.push((dn, d));
        }
        washed.push((product, day));
    }
    Ok((
        Source {
            input: "daily".into(),
            dir: dir.to_path_buf(),
            rics,
            unit_marker: None,
            daily: true,
            gz_bytes: 0,
        },
        washed,
    ))
}

#[derive(Default)]
struct Scan {
    /// month -> (update records, stored value bytes)
    months: BTreeMap<u32, (u64, u64)>,
    keys: u64,
}

fn scan_ric(db: &DB, ric: &str) -> Result<Scan> {
    let prefix = l2_db::update_prefix(ric);
    let mut upper = prefix.clone();
    *upper.last_mut().expect("prefix") += 1;
    let mut ro = ReadOptions::default();
    ro.set_iterate_upper_bound(upper);
    ro.fill_cache(false);
    ro.set_readahead_size(4 << 20);
    let mut s = Scan::default();
    let (mut day, mut month) = (u32::MAX, 0u32);
    for item in db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro) {
        let (k, v) = item?;
        let sec = u32::from_be_bytes(k[k.len() - 4..].try_into().context("short key")?);
        if sec / 86_400 != day {
            day = sec / 86_400;
            month = month_of_sec(sec);
        }
        let mut pos = 0usize;
        let n = get_varint(&v, &mut pos)?;
        let e = s.months.entry(month).or_default();
        e.0 += n;
        e.1 += v.len() as u64;
        s.keys += 1;
    }
    Ok(s)
}

/// RICs that have update records (one seek per RIC).
fn rics_with_data(db: &DB) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut next = vec![b'U'];
    loop {
        let mut it = db.iterator(IteratorMode::From(&next, Direction::Forward));
        let Some(item) = it.next() else { break };
        let (k, _) = item?;
        if k.first() != Some(&b'U') {
            break;
        }
        let end = k
            .iter()
            .position(|b| *b == 0)
            .context("key without RIC end")?;
        out.push(String::from_utf8_lossy(&k[1..end]).into_owned());
        next = k[..=end].to_vec();
        *next.last_mut().expect("next") += 1;
    }
    Ok(out)
}

#[derive(Debug, PartialEq, Eq)]
enum Overlap {
    /// Same rows (count and crc sum): a duplicate.
    Identical,
    /// Different rows, but the time ranges do not intersect: the day is split over inputs.
    Complementary,
    /// Different rows with intersecting time ranges.
    Conflict,
}

fn classify(v: &[Digest]) -> Overlap {
    let first = v[0];
    if v.iter()
        .all(|x| x.count == first.count && x.crc == first.crc)
    {
        return Overlap::Identical;
    }
    let mut s = v.to_vec();
    s.sort_by_key(|x| x.first);
    if s.windows(2).all(|w| w[0].last < w[1].first) {
        Overlap::Complementary
    } else {
        Overlap::Conflict
    }
}

fn gb(b: u64) -> f64 {
    (b as f64 / 1e9 * 100.0).round() / 100.0
}

fn ts_iso(ns: i64) -> String {
    chrono::DateTime::from_timestamp(ns.div_euclid(1_000_000_000), 0)
        .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default()
}

fn day_iso(d: i64) -> String {
    let (y, m, dd) = l2_db::civil_from_days(d);
    format!("{y}-{m:02}-{dd:02}")
}

#[derive(Default)]
struct MonthAgg {
    rows_src: u64,
    rows_db: u64,
    value_bytes: u64,
    rics: u64,
    gz_bytes: u64,
}

pub fn run_report(cfg: &ReportConfig) -> Result<Value> {
    let cache = SharedCache::new(Tuning::new(cfg.cache_mb, 64));
    // ---- discover the databases
    let mut sources: Vec<Source> = Vec::new();
    let mut washed_daily: std::collections::HashSet<(String, String)> = Default::default();
    let stream_root = cfg.db_root.join("_stream");
    if stream_root.is_dir() {
        let mut ids: Vec<_> = fs::read_dir(&stream_root)?
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        ids.sort();
        for id in ids {
            sources.push(read_stream(&stream_root.join(&id), &id, &cache)?);
        }
    }
    let mut day_dirs: Vec<PathBuf> = fs::read_dir(&cfg.db_root)?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.len() == 8 && n.bytes().all(|b| b.is_ascii_digit()))
        })
        .collect();
    day_dirs.sort();
    for d in &day_dirs {
        let (s, w) = read_day(d, &cache)?;
        washed_daily.extend(w);
        sources.push(s);
    }
    // ---- count verification: scan every RIC of every database
    let mut mismatches: Vec<Value> = Vec::new();
    let mut stray: Vec<Value> = Vec::new();
    let scans: Mutex<Vec<(usize, String, Scan)>> = Mutex::new(Vec::new());
    for (si, s) in sources.iter().enumerate() {
        let db = l2_db::open_read(&s.dir, &cache)?;
        let have = rics_with_data(&db)?;
        for r in &have {
            if !s.rics.contains_key(r) {
                stray.push(json!({"db": s.dir, "ric": r}));
            }
        }
        let names: Vec<&String> = s.rics.keys().collect();
        let next = AtomicUsize::new(0);
        let err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
        std::thread::scope(|sc| {
            for _ in 0..cfg.workers.max(1) {
                sc.spawn(|| loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(ric) = names.get(i) else { break };
                    match scan_ric(&db, ric) {
                        Ok(x) => scans.lock().expect("scans").push((si, (*ric).clone(), x)),
                        Err(e) => {
                            *err.lock().expect("err") = Some(e.context(format!("scan {ric}")));
                            break;
                        }
                    }
                });
            }
        });
        if let Some(e) = err.into_inner().expect("err") {
            return Err(e);
        }
    }
    let mut agg: BTreeMap<(String, u32), MonthAgg> = BTreeMap::new();
    let mut input_rows: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new(); // src, db, units
    let mut ric_sets: BTreeMap<String, std::collections::BTreeSet<String>> = BTreeMap::new();
    let mut first_ts: BTreeMap<String, i64> = BTreeMap::new();
    let mut last_ts: BTreeMap<String, i64> = BTreeMap::new();
    let mut month_rics: BTreeMap<(String, u32), std::collections::HashSet<String>> =
        BTreeMap::new();
    let mut input_keys: BTreeMap<String, u64> = BTreeMap::new();
    for (si, ric, sc) in scans.into_inner().expect("scans") {
        let s = &sources[si];
        let exp = &s.rics[&ric];
        let db_total: u64 = sc.months.values().map(|m| m.0).sum();
        let db_months: BTreeMap<u32, u64> = sc.months.iter().map(|(k, v)| (*k, v.0)).collect();
        if db_total != exp.updates || db_months != exp.months {
            mismatches.push(
                json!({"db": s.dir, "ric": ric, "marker": exp.updates, "db": db_total,
                                   "marker_months": exp.months, "db_months": db_months}),
            );
        }
        *input_keys.entry(s.input.clone()).or_default() += sc.keys;
        let r = input_rows.entry(s.input.clone()).or_default();
        r.0 += exp.updates;
        r.1 += db_total;
        ric_sets
            .entry(s.input.clone())
            .or_default()
            .insert(ric.clone());
        let f = first_ts.entry(s.input.clone()).or_insert(i64::MAX);
        *f = (*f).min(exp.first);
        let l = last_ts.entry(s.input.clone()).or_insert(i64::MIN);
        *l = (*l).max(exp.last);
        for (m, (n, vb)) in &sc.months {
            let a = agg.entry((s.input.clone(), *m)).or_default();
            a.rows_db += n;
            a.value_bytes += vb;
            month_rics
                .entry((s.input.clone(), *m))
                .or_default()
                .insert(ric.clone());
        }
        for (m, n) in &exp.months {
            agg.entry((s.input.clone(), *m)).or_default().rows_src += n;
        }
    }
    for ((inp, m), set) in &month_rics {
        agg.get_mut(&(inp.clone(), *m)).expect("agg").rics = set.len() as u64;
    }
    // ---- sizes
    let mut db_sst: BTreeMap<String, u64> = BTreeMap::new();
    let mut input_units: BTreeMap<String, u64> = BTreeMap::new();
    let mut input_gz: BTreeMap<String, u64> = BTreeMap::new();
    let mut unfinished: Vec<String> = Vec::new();
    for s in &sources {
        let db = l2_db::open_read(&s.dir, &cache)?;
        let b = db
            .property_int_value("rocksdb.total-sst-files-size")?
            .unwrap_or(0);
        *db_sst.entry(s.input.clone()).or_default() += b;
        if s.daily {
            *input_units.entry("daily".into()).or_default() = washed_daily.len() as u64;
        } else {
            *input_units.entry(s.input.clone()).or_default() += 1;
            *input_gz.entry(s.input.clone()).or_default() += s.gz_bytes;
            if s.unit_marker.is_none() {
                unfinished.push(s.input.clone());
            }
        }
    }
    // daily files: gz sizes and the product-days that should have been washed
    let days = days_between(&cfg.daily_start, &cfg.daily_end)?;
    let mut not_washed: Vec<Value> = Vec::new();
    let (mut expected_daily, mut daily_gz) = (0u64, 0u64);
    let mut daily_gz_month: BTreeMap<u32, u64> = BTreeMap::new();
    for e in fs::read_dir(&cfg.raw_root)?.flatten() {
        if !e.path().is_dir() {
            continue;
        }
        let p = e.file_name().to_string_lossy().into_owned();
        for d in &days {
            let f = e.path().join(format!("{p}_{d}_normll2.csv.gz"));
            let Ok(m) = fs::metadata(&f) else { continue };
            if m.len() == 0 {
                continue;
            }
            expected_daily += 1;
            daily_gz += m.len();
            let dn = crate::l2_slice::day_number(d)?;
            *daily_gz_month.entry(month_of_day(dn)).or_default() += m.len();
            if !washed_daily.contains(&(p.clone(), d.clone())) {
                not_washed.push(json!([p, d]));
            }
        }
    }
    input_gz.insert("daily".into(), daily_gz);
    for (m, b) in &daily_gz_month {
        agg.entry(("daily".into(), *m)).or_default().gz_bytes = *b;
    }
    // ---- overlaps between inputs
    let mut groups: HashMap<(&str, i64), Vec<(&str, Digest)>> = HashMap::new();
    for s in &sources {
        for (ric, e) in &s.rics {
            for (d, dg) in &e.days {
                groups
                    .entry((ric.as_str(), *d))
                    .or_default()
                    .push((s.input.as_str(), *dg));
            }
        }
    }
    let (mut dup_ident, mut dup_rows, mut complementary) = (0u64, 0u64, 0u64);
    let mut conflicts: Vec<Value> = Vec::new();
    let mut conflict_total = 0u64;
    let mut dup_examples: Vec<Value> = Vec::new();
    for ((ric, d), v) in &groups {
        if v.len() < 2 {
            continue;
        }
        let digests: Vec<Digest> = v.iter().map(|x| x.1).collect();
        match classify(&digests) {
            Overlap::Identical => {
                dup_ident += 1;
                dup_rows += digests[0].count * (v.len() as u64 - 1);
                if dup_examples.len() < 10 {
                    dup_examples.push(json!({"ric": ric, "day": day_iso(*d), "inputs": v.iter().map(|x| x.0).collect::<Vec<_>>(), "rows": digests[0].count}));
                }
            }
            Overlap::Complementary => complementary += 1,
            Overlap::Conflict => {
                conflict_total += 1;
                if conflicts.len() < 50 {
                    conflicts.push(json!({"ric": ric, "day": day_iso(*d),
                        "inputs": v.iter().map(|x| json!([x.0, x.1.count, ts_iso(x.1.first), ts_iso(x.1.last)])).collect::<Vec<_>>()}));
                }
            }
        }
    }
    // ---- seams: RICs per input around the two joins
    let mut seams: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    for (lo, hi) in [("2025-12-30", "2026-01-02"), ("2026-05-30", "2026-06-02")] {
        let (a, b) = (
            crate::l2_slice::day_number(lo)?,
            crate::l2_slice::day_number(hi)?,
        );
        for d in a..=b {
            let mut per: BTreeMap<&str, u64> = BTreeMap::new();
            for ((_, dd), v) in &groups {
                if *dd == d {
                    for x in v {
                        *per.entry(x.0).or_default() += 1;
                    }
                }
            }
            seams.insert(
                day_iso(d),
                per.into_iter().map(|(k, n)| (k.to_string(), n)).collect(),
            );
        }
    }
    // ---- assemble
    let mut inputs = json!({});
    for (inp, (src, dbn, _)) in &input_rows {
        let sst = db_sst.get(inp).copied().unwrap_or(0);
        let gz = input_gz.get(inp).copied().unwrap_or(0);
        inputs[inp] = json!({
            "units": input_units.get(inp), "rics": ric_sets[inp].len(),
            "rows_source_pipeline": src, "rows_db_scan": dbn, "rows_equal": src == dbn,
            "update_keys": input_keys.get(inp),
            "first_ts": ts_iso(first_ts[inp]), "last_ts": ts_iso(last_ts[inp]),
            "gz_gb": gb(gz), "db_sst_gb": gb(sst),
            "db_vs_gz": if gz > 0 { Some((sst as f64 / gz as f64 * 1000.0).round() / 1000.0) } else { None },
        });
    }
    let mut months = json!({});
    for ((inp, m), a) in &agg {
        let total_vb: u64 = agg
            .iter()
            .filter(|((i, _), _)| i == inp)
            .map(|(_, x)| x.value_bytes)
            .sum();
        let est = if total_vb > 0 {
            db_sst.get(inp).copied().unwrap_or(0) as f64 * a.value_bytes as f64 / total_vb as f64
        } else {
            0.0
        };
        months[m.to_string()][inp] = json!({
            "rows_source_pipeline": a.rows_src, "rows_db_scan": a.rows_db, "equal": a.rows_src == a.rows_db,
            "rics": a.rics, "db_gb_estimated": gb(est as u64),
            "gz_gb": if a.gz_bytes > 0 { Some(gb(a.gz_bytes)) } else { None },
        });
    }
    let spot = cfg
        .spot_checks
        .as_ref()
        .and_then(|p| fs::read_to_string(p).ok())
        .map(|t| {
            t.lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let gaps: Value = cfg
        .gaps
        .as_ref()
        .and_then(|p| fs::read_to_string(p).ok())
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Value::Null);
    let failed_in_log = cfg
        .log
        .as_ref()
        .and_then(|p| fs::read_to_string(p).ok())
        .map(|t| {
            t.lines()
                .filter(|l| l.contains("\"status\":\"failed\""))
                .count()
        })
        .unwrap_or(0);
    let ok = failed_in_log == 0
        && mismatches.is_empty()
        && stray.is_empty()
        && conflict_total == 0
        && not_washed.is_empty()
        && unfinished.is_empty();
    let report = json!({
        "generated_utc": chrono::Utc::now().to_rfc3339(),
        "ok": ok,
        "inputs": inputs, "months": months,
        "failed_or_unfinished_units": {"failed_units_in_log": failed_in_log, "streams_without_marker": unfinished,
            "daily_expected": expected_daily, "daily_not_washed": not_washed.len(), "daily_not_washed_list": not_washed.iter().take(100).collect::<Vec<_>>()},
        "count_mismatches": mismatches.len(), "count_mismatch_list": mismatches.iter().take(50).collect::<Vec<_>>(),
        "rics_with_data_but_no_marker": stray.len(), "stray_list": stray.iter().take(50).collect::<Vec<_>>(),
        "overlaps": {"duplicate_identical_ric_days": dup_ident, "duplicate_rows": dup_rows,
                     "complementary_ric_days": complementary, "conflicting_ric_days": conflict_total,
                     "duplicate_examples": dup_examples, "conflicts": conflicts},
        "seams_rics_per_input_per_day": seams,
        "source_gaps": gaps,
        "spot_checks": spot,
    });
    write_json_atomic(&cfg.out_json, &report)?;
    let mut md = String::new();
    md.push_str(&format!(
        "# L2 backfill report\n\n{} (UTC). **ok = {}**\n\n",
        chrono::Utc::now().format("%Y-%m-%d %H:%M"),
        ok
    ));
    md.push_str("| input | units | RICs | rows (pipeline) | rows (DB scan) | first | last | gz GB | DB GB | DB/gz |\n|---|---|---|---|---|---|---|---|---|---|\n");
    for (inp, v) in inputs.as_object().into_iter().flatten() {
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            inp,
            v["units"],
            v["rics"],
            v["rows_source_pipeline"],
            v["rows_db_scan"],
            v["first_ts"].as_str().unwrap_or(""),
            v["last_ts"].as_str().unwrap_or(""),
            v["gz_gb"],
            v["db_sst_gb"],
            v["db_vs_gz"]
        ));
    }
    md.push_str(&format!(
        "\n- count mismatches: {}; RICs with data but no marker: {}; streams without marker: {}; daily files not washed: {} of {}\n",
        mismatches.len(), stray.len(), report["failed_or_unfinished_units"]["streams_without_marker"], not_washed.len(), expected_daily
    ));
    md.push_str(&format!(
        "- overlaps between inputs: {} identical RIC-days ({} duplicate rows), {} complementary, {} **conflicting**\n",
        dup_ident, dup_rows, complementary, conflict_total
    ));
    md.push_str("- per-month numbers, seams, source gaps and spot checks: see report.json\n");
    fs::write(&cfg.out_md, md)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(count: u64, first: i64, last: i64, crc: u64) -> Digest {
        Digest {
            count,
            first,
            last,
            crc,
        }
    }

    #[test]
    fn overlaps_are_classified() {
        // same rows in two inputs
        assert_eq!(
            classify(&[d(10, 0, 9, 77), d(10, 0, 9, 77)]),
            Overlap::Identical
        );
        // same count, other content
        assert_eq!(
            classify(&[d(10, 0, 9, 77), d(10, 0, 9, 78)]),
            Overlap::Conflict
        );
        // the day is split at a time: no (RIC, time) in two inputs
        assert_eq!(
            classify(&[d(5, 0, 4, 1), d(5, 5, 9, 2)]),
            Overlap::Complementary
        );
        // intersecting ranges with other content
        assert_eq!(classify(&[d(6, 0, 5, 1), d(5, 5, 9, 2)]), Overlap::Conflict);
    }
}
