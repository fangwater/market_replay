//! Daily dominant-contract (主力) maintenance on top of the roll replay.
//!
//! Layout under `roll_root`:
//!   eod/<ex>_eod.csv     raw extractor cross-sections since `roll_start` (cumulative)
//!   result/              published replay outputs (served by cn-kline-server)
//!   result.prev/         previous publication
//!   staging/<D>/         per-day extractor output
//!
//! Dominant selection is path dependent (the first dominant comes from
//! `roll_start`), so every run replays the whole history and refuses to
//! publish if any already-published dominant row changed.

use anyhow::{bail, ensure, Context, Result};
use chrono::NaiveDate;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub const EXCHANGES: [&str; 6] = ["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"];

pub fn staging_file(root: &Path, day: NaiveDate, exchange: &str) -> PathBuf {
    root.join("staging")
        .join(day.format("%Y%m%d").to_string())
        .join(format!("{exchange}_eod.csv"))
}

fn trad_day(line: &str) -> &str {
    line.split(',').next().unwrap_or("")
}

/// Replaces `day`'s rows in the cumulative EOD file with the staged rows,
/// keeping rows in TradDay order. Written via a temp file + rename.
pub fn merge_day(root: &Path, day: NaiveDate) -> Result<usize> {
    let d = day.format("%Y-%m-%d").to_string();
    let mut added = 0;
    for ex in EXCHANGES {
        let master = root.join("eod").join(format!("{ex}_eod.csv"));
        let staged = staging_file(root, day, ex);
        let staged_text =
            fs::read_to_string(&staged).with_context(|| format!("read {staged:?}"))?;
        let mut staged_lines = staged_text.lines();
        let header = staged_lines
            .next()
            .context("staged EOD has no header")?
            .to_string();
        let new_rows: Vec<&str> = staged_lines.filter(|l| !l.is_empty()).collect();
        ensure!(
            new_rows.iter().all(|l| trad_day(l) == d),
            "{staged:?} has rows outside {d}"
        );
        let master_text =
            fs::read_to_string(&master).with_context(|| format!("read {master:?}"))?;
        let mut lines = master_text.lines();
        let master_header = lines.next().context("master EOD has no header")?;
        ensure!(
            master_header == header,
            "{master:?} header differs from extractor output"
        );
        let rows: Vec<&str> = lines
            .filter(|l| !l.is_empty() && trad_day(l) != d)
            .collect();
        let at = rows
            .iter()
            .position(|l| trad_day(l) > d.as_str())
            .unwrap_or(rows.len());
        let mut out = String::with_capacity(master_text.len() + staged_text.len());
        out.push_str(&header);
        out.push('\n');
        for l in rows[..at]
            .iter()
            .chain(new_rows.iter())
            .chain(rows[at..].iter())
        {
            out.push_str(l);
            out.push('\n');
        }
        let tmp = master.with_extension("csv.tmp");
        fs::write(&tmp, out)?;
        fs::rename(&tmp, &master)?;
        added += new_rows.len();
    }
    Ok(added)
}

/// Dominant rows (without header) with TradDay <= `through`, in file order.
fn dominant_rows(path: &Path, through: Option<&str>) -> Result<Vec<String>> {
    let text = fs::read_to_string(path).with_context(|| format!("read {path:?}"))?;
    Ok(text
        .lines()
        .skip(1)
        .filter(|l| !l.trim().is_empty())
        .filter(|l| through.is_none_or(|t| trad_day(l) <= t))
        .map(|l| l.trim_end_matches('\r').to_string())
        .collect())
}

fn latest_day(path: &Path) -> Result<Option<String>> {
    Ok(dominant_rows(path, None)?
        .iter()
        .map(|l| trad_day(l).to_string())
        .max())
}

#[derive(Debug, Default, Serialize)]
pub struct RollReport {
    pub through: String,
    /// Products whose dominant changed on `through` (roll_flag = 1).
    pub rolled_today: Vec<String>,
    /// Products whose next-day contract differs from today's.
    pub rolling_next: Vec<String>,
    pub products: usize,
}

/// Checks that every row already published is unchanged in `new_dir`.
pub fn check_history(published: &Path, new_dir: &Path) -> Result<()> {
    for ex in EXCHANGES {
        let old = published.join(format!("{ex}_dominant.csv"));
        if !old.exists() {
            continue;
        }
        let Some(through) = latest_day(&old)? else {
            continue;
        };
        let before = dominant_rows(&old, Some(&through))?;
        let after = dominant_rows(&new_dir.join(format!("{ex}_dominant.csv")), Some(&through))?;
        if before != after {
            let first = before
                .iter()
                .zip(&after)
                .find(|(a, b)| a != b)
                .map(|(a, b)| format!("published `{a}` vs new `{b}`"))
                .unwrap_or_else(|| format!("{} vs {} rows", before.len(), after.len()));
            bail!("{ex}: already published dominant history changed through {through}: {first}");
        }
    }
    Ok(())
}

pub fn report(dir: &Path) -> Result<RollReport> {
    let mut last: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut through = String::new();
    for ex in EXCHANGES {
        let path = dir.join(format!("{ex}_dominant.csv"));
        if !path.exists() {
            continue;
        }
        for l in dominant_rows(&path, None)? {
            let f: Vec<String> = l.split(',').map(str::to_string).collect();
            if f.len() < 7 {
                continue;
            }
            through = through.max(f[0].clone());
            let e = last.entry(f[1].clone()).or_default();
            if e.is_empty() || f[0] >= e[0] {
                *e = f;
            }
        }
    }
    let mut r = RollReport {
        through: through.clone(),
        products: last.len(),
        ..RollReport::default()
    };
    for (product, f) in &last {
        if f[0] != through {
            continue;
        }
        if f[3] == "1" {
            r.rolled_today.push(format!("{product} {}", f[2]));
        }
        if !f[5].is_empty() && f[5] != f[2] {
            r.rolling_next.push(format!(
                "{product} {}→{}",
                if f[2].is_empty() { "-" } else { &f[2] },
                f[5]
            ));
        }
    }
    Ok(r)
}

/// Replaces `result` with `new_dir`, keeping the previous one as `result.prev`.
pub fn publish(root: &Path, new_dir: &Path) -> Result<()> {
    let result = root.join("result");
    let prev = root.join("result.prev");
    if prev.exists() {
        fs::remove_dir_all(&prev)?;
    }
    if result.exists() {
        fs::rename(&result, &prev)?;
    }
    fs::rename(new_dir, &result)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: &str = "trad_day,product_id,instrument_id,open_int,volume,close,expired";

    #[test]
    fn merge_replaces_day_and_keeps_order() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        fs::create_dir_all(root.join("eod")).unwrap();
        for ex in EXCHANGES {
            fs::write(
                root.join("eod").join(format!("{ex}_eod.csv")),
                format!("{H}\n2026-11-11,RB,rb2701,1.000,1,3000.000,0\n2026-11-13,RB,rb2701,3.000,3,3002.000,0\n"),
            )
            .unwrap();
            let s = staging_file(root, NaiveDate::from_ymd_opt(2026, 11, 12).unwrap(), ex);
            fs::create_dir_all(s.parent().unwrap()).unwrap();
            fs::write(
                &s,
                format!("{H}\n2026-11-12,RB,rb2701,2.000,2,3001.000,0\n"),
            )
            .unwrap();
        }
        let n = merge_day(root, NaiveDate::from_ymd_opt(2026, 11, 12).unwrap()).unwrap();
        assert_eq!(n, 6);
        let text = fs::read_to_string(root.join("eod/xsge_eod.csv")).unwrap();
        let days: Vec<&str> = text.lines().skip(1).map(trad_day).collect();
        assert_eq!(days, vec!["2026-11-11", "2026-11-12", "2026-11-13"]);
        // Re-merging the same day is idempotent.
        merge_day(root, NaiveDate::from_ymd_opt(2026, 11, 12).unwrap()).unwrap();
        assert_eq!(
            fs::read_to_string(root.join("eod/xsge_eod.csv")).unwrap(),
            text
        );
    }

    #[test]
    fn history_check_rejects_changed_published_rows() {
        let t = tempfile::tempdir().unwrap();
        let (old, new) = (t.path().join("old"), t.path().join("new"));
        fs::create_dir_all(&old).unwrap();
        fs::create_dir_all(&new).unwrap();
        let hd =
            "trad_day,product_id,instrument_id,roll_flag,decision_open_int,scheduled_next,rule\r\n";
        fs::write(
            old.join("xsge_dominant.csv"),
            format!("{hd}2026-11-12,RB,rb2701,0,1.0,rb2701,r\r\n"),
        )
        .unwrap();
        fs::write(
            new.join("xsge_dominant.csv"),
            format!("{hd}2026-11-12,RB,rb2701,0,1.0,rb2701,r\r\n2026-11-13,RB,rb2701,0,2.0,rb2705,r\r\n"),
        )
        .unwrap();
        check_history(&old, &new).unwrap();
        let r = report(&new).unwrap();
        assert_eq!(r.through, "2026-11-13");
        assert_eq!(r.rolling_next, vec!["RB rb2701→rb2705".to_string()]);
        fs::write(
            new.join("xsge_dominant.csv"),
            format!("{hd}2026-11-12,RB,rb2705,1,1.0,rb2705,r\r\n"),
        )
        .unwrap();
        assert!(check_history(&old, &new).is_err());
    }
}
