//! Exchange directory for a raw futures RIC.
//!
//! Port of `classify()` / `references()` from
//! `scripts/lseg/group_lseg_by_exchange.py`, futures branch only: the
//! exchange comes from the HRD `Market MIC` of the same literal RIC; with no
//! MIC, from unambiguous LSEG exchange codes; otherwise `REVIEW`. Indices
//! (`.xxx`) go to `INDICES`. Nothing is inferred from similar prefixes.
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

const MIC: &[(&str, &[&str])] = &[
    ("CME", &["XCME"]),
    ("CBOT", &["XCBT", "XKBT"]),
    ("COMEX", &["XCEC"]),
    ("NYMEX", &["XNYM"]),
    (
        "ICE_EUROPE",
        &["IFEU", "ICEU", "IFLL", "IFLO", "IFLX", "XLIF"],
    ),
    ("ICE_US", &["IFUS", "ICUS", "XNLI"]),
    ("ICE_CANADA", &["IFCA", "XWCE"]),
    ("ICE_ENDEX", &["NDEX"]),
    ("EUREX", &["XEUR"]),
    ("EURONEXT_PARIS", &["XMON"]),
    ("CFE", &["XCBF"]),
    ("CBOE", &["XCBO"]),
    ("SGX", &["XSIM", "XSCE"]),
    ("HKEX_FUTURES", &["XHKF"]),
    ("LME", &["XLME"]),
    ("OSE", &["XOSE"]),
    ("TSE", &["XTKS"]),
    ("ASX24", &["XSFE"]),
    ("KRX", &["XKRX", "XKFE"]),
    ("MONTREAL", &["XMOD"]),
    ("B3", &["BVMF", "XBMF"]),
    ("BURSA_MALAYSIA", &["XKLS"]),
    ("MEXDER", &["XEMD"]),
    ("BMV", &["XMEX"]),
    ("NSE_IFSC", &["INSE"]),
    ("INE", &["XINE"]),
    ("NASDAQ", &["XNAS"]),
    ("NYSE", &["XNYS"]),
    ("NYSE_ARCA", &["ARCX"]),
    ("CBOE_BZX", &["BATS"]),
];

const CODE: &[(&str, &str)] = &[
    ("NYM", "NYMEX"),
    ("CMX", "COMEX"),
    ("CBT", "CBOT"),
    ("KBT", "CBOT"),
    ("CME", "CME"),
    ("IMM", "CME"),
    ("IOM", "CME"),
    ("GEM", "CME"),
    ("IEU", "ICE_EUROPE"),
    ("LIF", "ICE_EUROPE"),
    ("IUS", "ICE_US"),
    ("ICA", "ICE_CANADA"),
    ("EDX", "ICE_ENDEX"),
    ("EUX", "EUREX"),
    ("SFE", "ASX24"),
    ("MON", "EURONEXT_PARIS"),
    ("MNP", "EURONEXT_PARIS"),
    ("SIM", "SGX"),
    ("SIC", "SGX"),
    ("CBF", "CFE"),
    ("KFE", "KRX"),
    ("KSC", "KRX"),
    ("HFE", "HKEX_FUTURES"),
    ("BMF", "B3"),
    ("MDX", "BURSA_MALAYSIA"),
    ("OSA", "OSE"),
    ("TYO", "TSE"),
    ("MDE", "MEXDER"),
    ("LME", "LME"),
];

#[derive(Debug, Default, Clone)]
pub struct Reference {
    pub mics: BTreeSet<String>,
    pub codes: BTreeSet<String>,
}

fn venue_of_mic(mic: &str) -> String {
    MIC.iter()
        .find(|(_, mics)| mics.contains(&mic))
        .map(|(v, _)| v.to_string())
        .unwrap_or_else(|| format!("MIC_{mic}"))
}

/// Upper-case venue as in the Python classifier (`REVIEW` when unresolved).
pub fn classify(ric: &str, r: Option<&Reference>) -> String {
    if ric.starts_with('.') {
        return "INDICES".into();
    }
    let empty = Reference::default();
    let r = r.unwrap_or(&empty);
    let candidates: BTreeSet<String> = r.mics.iter().map(|m| venue_of_mic(m)).collect();
    match candidates.len() {
        1 => candidates.into_iter().next().unwrap(),
        n if n > 1 => "REVIEW".into(),
        _ => {
            let mapped: BTreeSet<&str> = r
                .codes
                .iter()
                .filter_map(|c| CODE.iter().find(|(k, _)| k == c).map(|(_, v)| *v))
                .collect();
            let all_known = r.codes.iter().all(|c| CODE.iter().any(|(k, _)| k == c));
            if mapped.len() == 1 && all_known {
                mapped.into_iter().next().unwrap().to_string()
            } else {
                "REVIEW".into()
            }
        }
    }
}

/// Streams the HRD CSVs, keeping only the requested RICs.
pub fn references(
    paths: &[impl AsRef<Path>],
    rics: &HashSet<String>,
) -> Result<HashMap<String, Reference>> {
    let mut out: HashMap<String, Reference> = HashMap::new();
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
        let (ric, mic, code) = (pos("RIC")?, pos("Market MIC")?, pos("Exchange Code")?);
        for row in rdr.records() {
            let row = row?;
            let Some(r) = row.get(ric) else { continue };
            if !rics.contains(r) {
                continue;
            }
            let e = out.entry(r.to_string()).or_default();
            if let Some(v) = row.get(mic).filter(|v| !v.is_empty()) {
                e.mics.insert(v.to_string());
            }
            if let Some(v) = row.get(code).filter(|v| !v.is_empty()) {
                e.codes.insert(v.to_string());
            }
        }
    }
    Ok(out)
}

/// Lower-case directory per RIC: a previously assigned venue (driver state
/// or the approved 2026 status map) wins, so a RIC never changes directory.
pub fn assign(
    rics: &[String],
    known: &BTreeMap<String, String>,
    approved: &HashMap<String, String>,
    hrd: &[impl AsRef<Path>],
) -> Result<BTreeMap<String, String>> {
    let missing: HashSet<String> = rics
        .iter()
        .filter(|r| !known.contains_key(*r) && !approved.contains_key(*r))
        .cloned()
        .collect();
    let refs = if missing.is_empty() {
        HashMap::new()
    } else {
        references(hrd, &missing)?
    };
    Ok(rics
        .iter()
        .map(|r| {
            let v = known
                .get(r)
                .or_else(|| approved.get(r))
                .cloned()
                .unwrap_or_else(|| classify(r, refs.get(r)).to_lowercase());
            (r.clone(), v)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(mics: &[&str], codes: &[&str]) -> Reference {
        Reference {
            mics: mics.iter().map(|s| s.to_string()).collect(),
            codes: codes.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn classify_matches_python_rules() {
        assert_eq!(classify("ESZ26", Some(&r(&["XCME"], &["CME"]))), "CME");
        assert_eq!(classify("X", Some(&r(&["XCME", "XCBT"], &[]))), "REVIEW");
        assert_eq!(classify("X", Some(&r(&["ZZZZ"], &[]))), "MIC_ZZZZ");
        assert_eq!(classify("X", Some(&r(&[], &["IEU", "LIF"]))), "ICE_EUROPE");
        assert_eq!(classify("X", Some(&r(&[], &["IEU", "???"]))), "REVIEW");
        assert_eq!(classify("X", None), "REVIEW");
        assert_eq!(classify(".SPX", None), "INDICES");
    }

    #[test]
    fn assign_prefers_known_then_approved() {
        let t = tempfile::tempdir().unwrap();
        let hrd = t.path().join("hrd.csv");
        std::fs::write(
            &hrd,
            "\u{feff}RIC,Market MIC,Exchange Code\nNEW1,XNYM,NYM\nOLD,XCME,CME\n",
        )
        .unwrap();
        let known = BTreeMap::from([("OLD".to_string(), "review".to_string())]);
        let approved = HashMap::from([("APP".to_string(), "cbot".to_string())]);
        let m = assign(
            &["NEW1".into(), "OLD".into(), "APP".into(), "NONE".into()],
            &known,
            &approved,
            &[hrd],
        )
        .unwrap();
        assert_eq!(m["NEW1"], "nymex");
        assert_eq!(m["OLD"], "review");
        assert_eq!(m["APP"], "cbot");
        assert_eq!(m["NONE"], "review");
    }
}
