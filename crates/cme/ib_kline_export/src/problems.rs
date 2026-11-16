//! Problems only, on the delivery disk: `<root>/meta/problems/ib_kline/problems.csv`.
//! Nothing is written when a run has no problems and none were open before.
//! Entries are keyed by (kind, ric, con_id, utc_day); later runs upsert them and
//! a successful publication of the same RIC/day clears an earlier
//! `publication_failure` / `existing_newer_than_source` entry.
use anyhow::{ensure, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

const MAX_PROBLEMS: usize = 200_000;
type Key = (String, String, String, String);

#[derive(Default)]
pub struct Problems {
    items: Mutex<BTreeMap<Key, String>>,
    cleared: Mutex<BTreeSet<(String, String)>>,
}

pub fn path(root: &Path) -> PathBuf {
    root.join("meta/problems/ib_kline/problems.csv")
}

/// Kinds that a later successful publication of the same RIC/day resolves.
const DAY_KINDS: [&str; 2] = ["publication_failure", "existing_newer_than_source"];

impl Problems {
    pub fn record(&self, kind: &str, ric: &str, con_id: &str, utc_day: &str, detail: &str) {
        let mut m = self.items.lock().unwrap();
        if m.len() < MAX_PROBLEMS {
            m.insert(
                (kind.into(), ric.into(), con_id.into(), utc_day.into()),
                detail.replace(['\n', '\r'], " "),
            );
        }
    }
    pub fn clear_day(&self, ric: &str, utc_day: &str) {
        self.cleared
            .lock()
            .unwrap()
            .insert((ric.into(), utc_day.into()));
    }
    pub fn kinds(&self) -> BTreeMap<String, usize> {
        let mut out = BTreeMap::new();
        for k in self.items.lock().unwrap().keys() {
            *out.entry(k.0.clone()).or_default() += 1;
        }
        out
    }

    /// Merge with the existing file; rewrite atomically only if it changes.
    pub fn flush(&self, root: &Path, now_utc: &str) -> Result<()> {
        let target = path(root);
        let items = self.items.lock().unwrap();
        let cleared = self.cleared.lock().unwrap();
        ensure!(
            items.len() < MAX_PROBLEMS,
            "more than {MAX_PROBLEMS} problems; stopping instead of truncating"
        );
        // key -> (detail, first_seen, last_seen)
        let mut merged: BTreeMap<Key, (String, String, String)> = BTreeMap::new();
        let before = match fs::read(&target) {
            Ok(bytes) => {
                let mut r = csv::Reader::from_reader(bytes.as_slice());
                for rec in r.records() {
                    let rec = rec?;
                    let g = |i: usize| rec.get(i).unwrap_or("").to_string();
                    merged.insert((g(0), g(1), g(2), g(3)), (g(4), g(5), g(6)));
                }
                Some(bytes)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        merged.retain(|k, _| {
            !(DAY_KINDS.contains(&k.0.as_str()) && cleared.contains(&(k.1.clone(), k.3.clone())))
        });
        for (k, detail) in items.iter() {
            match merged.get_mut(k) {
                Some(v) => {
                    if v.0 != *detail {
                        v.0 = detail.clone();
                    }
                    v.2 = now_utc.into();
                }
                None => {
                    merged.insert(k.clone(), (detail.clone(), now_utc.into(), now_utc.into()));
                }
            }
        }
        if merged.is_empty() && before.is_none() {
            return Ok(());
        }
        let mut w = csv::Writer::from_writer(Vec::new());
        w.write_record([
            "kind",
            "ric",
            "con_id",
            "utc_day",
            "detail",
            "first_seen_utc",
            "last_seen_utc",
        ])?;
        for (k, v) in &merged {
            w.write_record([&k.0, &k.1, &k.2, &k.3, &v.0, &v.1, &v.2])?;
        }
        let bytes = w.into_inner().map_err(|e| anyhow::anyhow!("csv: {e}"))?;
        // Re-seen problems only bump last_seen; avoid churn when content is otherwise equal.
        if before
            .as_deref()
            .is_some_and(|b| strip_last_seen(b) == strip_last_seen(&bytes))
        {
            return Ok(());
        }
        fs::create_dir_all(target.parent().unwrap())?;
        let tmp = target.with_extension(format!("csv.{}.partial", std::process::id()));
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        fs::rename(&tmp, &target)?;
        Ok(())
    }
}

fn strip_last_seen(bytes: &[u8]) -> Vec<Vec<String>> {
    let mut r = csv::Reader::from_reader(bytes);
    r.records()
        .filter_map(|x| x.ok())
        .map(|rec| rec.iter().take(6).map(String::from).collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nothing_written_without_problems_and_merge_clears_resolved_days() {
        let root = std::env::temp_dir().join(format!("ib_kline_problems_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        Problems::default().flush(&root, "t0").unwrap();
        assert!(!path(&root).exists());
        let p = Problems::default();
        p.record("publication_failure", "ESZ26", "1", "20261103", "boom");
        p.record(
            "unmapped_contract",
            "",
            "9",
            "",
            "symbol_not_in_covered_specs",
        );
        p.flush(&root, "t1").unwrap();
        let text = fs::read_to_string(path(&root)).unwrap();
        assert!(text.contains("boom") && text.contains("unmapped_contract"));
        let q = Problems::default();
        q.clear_day("ESZ26", "20261103");
        q.flush(&root, "t2").unwrap();
        let text = fs::read_to_string(path(&root)).unwrap();
        assert!(
            !text.contains("boom")
                && text.contains("unmapped_contract,,9,,symbol_not_in_covered_specs,t1,t1")
        );
        fs::remove_dir_all(&root).unwrap();
    }
}
