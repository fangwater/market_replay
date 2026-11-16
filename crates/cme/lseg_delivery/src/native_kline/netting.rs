//! Identity-aware historical netting of LSEG TAS cancels, corrections and
//! inserts (`correction_policy_version=1`).
//!
//! Port of `scripts/lseg/trade_revision_ledger.py`. Raw prints stay
//! immutable; the ledger only says which physical trades are cancelled or
//! replaced and which trades are inserted. Every event that cannot be linked
//! reliably stays unresolved (and the original print is kept).
//!
//! Inputs per RIC:
//! * candidates: physical source trades whose (price, volume) equals the
//!   original or new payload of a kind=12 row of the same RIC, published no
//!   later than the last such row (RocksDB; the slim codec has no IDs);
//! * witnesses: the raw TAS CSV rows (Trade and Correction) recovered for
//!   these candidates/corrections, with their identifiers.
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

pub const VERSION: &str = "1";
const DAY: i128 = 86_400_000_000_000;
pub const MISSING_CLOCK: u64 = u64::MAX;

/// Physical identity of a source trade: (source order index, publication
/// ns, RocksDB key order). Inserted trades use `u32::MAX` as source index.
pub type TradeId = (u32, u64, u64);

pub(crate) fn price_e9(text: &str) -> Result<i64> {
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    ensure!(
        !(int.is_empty() && frac.is_empty())
            && int.bytes().all(|b| b.is_ascii_digit())
            && frac.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal {text:?}"
    );
    let (kept, rest) = frac.split_at(frac.len().min(9));
    ensure!(rest.bytes().all(|b| b == b'0'), "price precision {text:?}");
    let mut value: i128 = if int.is_empty() { 0 } else { int.parse()? };
    value = value * 1_000_000_000 + format!("{kept:0<9}").parse::<i128>()?;
    if negative {
        value = -value
    }
    Ok(i64::try_from(value)?)
}

pub(crate) fn quantity(text: &str) -> Result<i64> {
    let (int, frac) = text.split_once('.').unwrap_or((text, ""));
    ensure!(
        frac.bytes().all(|b| b == b'0'),
        "fractional quantity {text:?}"
    );
    Ok(int.parse()?)
}

/// `HH:MM:SS[.fffffffff]` as nanoseconds of day (fraction truncated at ns).
fn clock(text: &str) -> Result<i64> {
    let mut parts = text.split(':');
    let (Some(h), Some(m), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        bail!("invalid clock {text:?}")
    };
    let (sec, frac) = s.split_once('.').unwrap_or((s, ""));
    ensure!(
        frac.bytes().all(|b| b.is_ascii_digit()),
        "invalid clock {text:?}"
    );
    let ns: i64 = format!("{:0<9}", &frac[..frac.len().min(9)]).parse()?;
    Ok(
        (h.parse::<i64>()? * 3600 + m.parse::<i64>()? * 60 + sec.parse::<i64>()?) * 1_000_000_000
            + ns,
    )
}

fn timestamp(text: &str) -> Result<u64> {
    let t = chrono::DateTime::parse_from_rfc3339(text)
        .with_context(|| format!("Date-Time {text:?}"))?;
    Ok(u64::try_from(
        t.timestamp_nanos_opt().context("Date-Time range")?,
    )?)
}

/// Calendar date as `YYYY-MM-DD`; unknown formats compare verbatim.
fn date_key(value: &Option<String>) -> Option<String> {
    let v = value.as_deref()?;
    for f in ["%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%d-%b-%Y"] {
        if let Ok(d) = chrono::NaiveDate::parse_from_str(v, f) {
            return Some(d.format("%Y-%m-%d").to_string());
        }
    }
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(v) {
        return Some(t.date_naive().format("%Y-%m-%d").to_string());
    }
    Some(v.into())
}

/// Python `event_time`: exchange clock on the UTC date nearest publication.
pub fn event_time(published: u64, hms: Option<i64>) -> u64 {
    let Some(hms) = hms.filter(|v| *v as u64 != MISSING_CLOCK) else {
        return published;
    };
    let p = i128::from(published);
    let mut t = p.div_euclid(DAY) * DAY + i128::from(hms);
    if t > p + DAY / 2 {
        t -= DAY
    }
    if t < p - DAY / 2 {
        t += DAY
    }
    t as u64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Kind {
    Trade,
    Correction,
}

/// One raw TAS CSV row (Python `source_record`). Empty cells are `None`.
#[derive(Clone, Debug, Serialize)]
pub struct Witness {
    pub ric: String,
    pub published_ns: u64,
    pub kind: Kind,
    pub price: Option<i64>,
    pub volume: Option<i64>,
    pub original_price: Option<i64>,
    pub original_volume: Option<i64>,
    pub uid: Option<String>,
    pub original_uid: Option<String>,
    pub seq: Option<String>,
    pub original_seq: Option<String>,
    pub trade_seq: Option<String>,
    pub original_trade_seq: Option<String>,
    pub hms: Option<i64>,
    pub original_hms: Option<i64>,
    pub date: Option<String>,
    pub original_date: Option<String>,
    pub qualifiers: String,
    pub source: String,
    pub line: u64,
}

impl Witness {
    /// `{"source":..,"line":..,"fields":{CSV header: value}}`.
    pub fn from_json(record: &Value) -> Result<Self> {
        let f = record["fields"].as_object().context("witness fields")?;
        let text = |name: &str| -> Option<String> {
            f.get(name)
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
        };
        let num = |name: &str, parse: fn(&str) -> Result<i64>| -> Result<Option<i64>> {
            text(name)
                .map(|v| parse(&v).with_context(|| format!("witness {name}")))
                .transpose()
        };
        let kind = match text("Type").as_deref() {
            Some("Trade") => Kind::Trade,
            Some("Correction") => Kind::Correction,
            other => bail!("unexpected witness Type {other:?}"),
        };
        Ok(Self {
            ric: text("#RIC").context("witness #RIC")?,
            published_ns: timestamp(&text("Date-Time").context("witness Date-Time")?)?,
            kind,
            price: num("Price", price_e9)?,
            volume: num("Volume", quantity)?,
            original_price: num("Original Price", price_e9)?,
            original_volume: num("Original Volume", quantity)?,
            uid: text("Unique Trade Identification"),
            original_uid: text("Original Unique Trade Identification"),
            seq: text("Seq. No."),
            original_seq: text("Original Seq. No."),
            trade_seq: text("Trade Sequence Number"),
            original_trade_seq: text("Original Trade Sequence Number"),
            hms: num("Exch Time", clock)?,
            original_hms: num("Original Exch Time", clock)?,
            date: text("Date"),
            original_date: text("Original Date"),
            qualifiers: text("Qualifiers").unwrap_or_default(),
            source: record["source"].as_str().unwrap_or_default().into(),
            line: record["line"].as_u64().unwrap_or(0),
        })
    }
}

/// A physical source trade that may be the target of a revision.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub id: TradeId,
    pub published_ns: u64,
    pub event_ns: u64,
    /// Raw exchange clock; `MISSING_CLOCK` when absent.
    pub exch_hms_ns: u64,
    pub price: i64,
    pub volume: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Cancel,
    Insert,
    Amend,
    UnresolvedConflictingFragments,
    UnresolvedAction,
}
impl Action {
    fn unresolved(self) -> Option<&'static str> {
        match self {
            Self::UnresolvedConflictingFragments => Some("unresolved_conflicting_fragments"),
            Self::UnresolvedAction => Some("unresolved_action"),
            _ => None,
        }
    }
}

/// Complementary Correction fragments of one event (Python
/// `combine_corrections`), with all source witnesses.
#[derive(Clone, Debug, Serialize)]
pub struct Correction {
    pub event_id: String,
    pub published_ns: u64,
    pub action: Action,
    pub price: Option<i64>,
    pub volume: Option<i64>,
    pub original_price: Option<i64>,
    pub original_volume: Option<i64>,
    pub uid: Option<String>,
    pub original_uid: Option<String>,
    pub seq: Option<String>,
    pub original_seq: Option<String>,
    pub trade_seq: Option<String>,
    pub original_trade_seq: Option<String>,
    pub hms: Option<i64>,
    pub original_hms: Option<i64>,
    pub date: Option<String>,
    pub original_date: Option<String>,
    pub qualifiers: String,
    pub conflicts: Vec<&'static str>,
    pub witnesses: Vec<(String, u64)>,
}

fn tags(qualifiers: &str) -> HashMap<String, String> {
    // Python: re.findall(r'([^;]*)\[([^\]]+)\]', q) with value.strip(' "').
    // The greedy value cannot cross ';', so each ';' segment yields at most
    // one match: the rightmost '[' followed by a non-empty run up to ']'.
    let mut out = HashMap::new();
    for segment in qualifiers.split(';') {
        for (p, _) in segment
            .match_indices('[')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            if let Some(q) = segment[p + 1..].find(']') {
                if q > 0 {
                    let value = segment[..p].trim_matches(|c| c == ' ' || c == '"');
                    out.insert(segment[p + 1..p + 1 + q].to_owned(), value.to_owned());
                    break;
                }
            }
        }
    }
    out
}

pub fn combine_corrections(records: &[Witness]) -> Vec<Correction> {
    type Key = (
        u64,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<i64>,
    );
    let mut order: Vec<Key> = Vec::new();
    let mut groups: HashMap<Key, Vec<&Witness>> = HashMap::new();
    for r in records.iter().filter(|r| r.kind == Kind::Correction) {
        let key = (
            r.published_ns,
            r.original_uid.clone(),
            r.original_seq.clone(),
            r.original_trade_seq.clone(),
            r.original_price,
            r.original_volume,
            r.original_date.clone(),
            r.original_hms,
        );
        groups
            .entry(key.clone())
            .or_insert_with(|| {
                order.push(key.clone());
                Vec::new()
            })
            .push(r);
    }
    let mut out = Vec::with_capacity(order.len());
    for key in order {
        let rows = &groups[&key];
        let first = rows[0];
        let mut conflicts = Vec::new();
        fn one<T: Clone + Eq + std::hash::Hash>(
            values: impl Iterator<Item = Option<T>>,
            name: &'static str,
            conflicts: &mut Vec<&'static str>,
        ) -> Option<T> {
            let mut set: Vec<T> = Vec::new();
            for v in values.flatten() {
                if !set.contains(&v) {
                    set.push(v)
                }
            }
            if set.len() > 1 {
                conflicts.push(name)
            }
            (set.len() == 1).then(|| set.remove(0))
        }
        let price = one(rows.iter().map(|r| r.price), "price", &mut conflicts);
        let volume = one(rows.iter().map(|r| r.volume), "volume", &mut conflicts);
        let uid = one(rows.iter().map(|r| r.uid.clone()), "uid", &mut conflicts);
        let seq = one(rows.iter().map(|r| r.seq.clone()), "seq", &mut conflicts);
        let trade_seq = one(
            rows.iter().map(|r| r.trade_seq.clone()),
            "trade_seq",
            &mut conflicts,
        );
        let hms = one(rows.iter().map(|r| r.hms), "hms", &mut conflicts);
        let date = one(rows.iter().map(|r| r.date.clone()), "date", &mut conflicts);
        let mut tokens: Vec<&str> = Vec::new();
        for r in rows {
            for t in r.qualifiers.split(';').filter(|t| !t.is_empty()) {
                if !tokens.contains(&t) {
                    tokens.push(t)
                }
            }
        }
        let qualifiers = tokens.join(";");
        let t = tags(&qualifiers);
        let action = if !conflicts.is_empty() {
            Action::UnresolvedConflictingFragments
        } else if t.get("CAN_COND").map(String::as_str) == Some("611")
            || t.get("CAN_COND_N").map(String::as_str) == Some("2")
        {
            Action::Cancel
        } else if t.get("INSSALCOND").map(String::as_str) == Some("2") {
            Action::Insert
        } else if (first.original_price.is_some() || first.original_volume.is_some())
            && (price.is_some() || volume.is_some())
        {
            Action::Amend
        } else {
            Action::UnresolvedAction
        };
        out.push(Correction {
            event_id: String::new(),
            published_ns: first.published_ns,
            action,
            price,
            volume,
            original_price: first.original_price,
            original_volume: first.original_volume,
            uid,
            original_uid: first.original_uid.clone(),
            seq,
            original_seq: first.original_seq.clone(),
            trade_seq,
            original_trade_seq: first.original_trade_seq.clone(),
            hms,
            original_hms: first.original_hms,
            date,
            original_date: first.original_date.clone(),
            qualifiers,
            conflicts,
            witnesses: rows.iter().map(|r| (r.source.clone(), r.line)).collect(),
        });
    }
    // Stable sort, as Python's `sorted`.
    out.sort_by(|a, b| {
        (
            a.published_ns,
            a.seq.as_deref().unwrap_or(""),
            a.original_uid.as_deref().unwrap_or(""),
        )
            .cmp(&(
                b.published_ns,
                b.seq.as_deref().unwrap_or(""),
                b.original_uid.as_deref().unwrap_or(""),
            ))
    });
    out
}

#[derive(Clone, Copy, Debug)]
struct Version {
    price: i64,
    volume: i64,
}

#[derive(Clone, Debug)]
struct Original {
    price: i64,
    volume: i64,
    /// Raw clock; `MISSING_CLOCK` stays as is (Python keeps the sentinel).
    exch_hms_ns: u64,
}

#[derive(Clone, Debug)]
struct St {
    id: TradeId,
    insert_event: Option<String>,
    published_ns: u64,
    event_ns: u64,
    price: i64,
    volume: i64,
    active: bool,
    uid: Option<String>,
    seq: Option<String>,
    trade_seq: Option<String>,
    date: Option<String>,
    recovered: bool,
    original: Original,
    history: Vec<Version>,
    revisions: Vec<String>,
}
impl St {
    fn id_field(&self, name: &str) -> Option<&str> {
        match name {
            "uid" => self.uid.as_deref(),
            "trade_seq" => self.trade_seq.as_deref(),
            "seq" => self.seq.as_deref(),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PatchAction {
    Cancel,
    Replace,
}

#[derive(Clone, Debug, Serialize)]
pub struct Patch {
    pub id: TradeId,
    pub published_ns: u64,
    pub event_ns: u64,
    pub original_price: i64,
    pub original_volume: i64,
    pub action: PatchAction,
    pub price: i64,
    pub volume: i64,
    pub revisions: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Insertion {
    pub event_id: String,
    pub published_ns: u64,
    pub event_ns: u64,
    pub exch_hms_ns: u64,
    pub price: i64,
    pub volume: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Applied {
    pub event_id: String,
    pub published_ns: u64,
    pub result: &'static str,
    pub basis: Option<&'static str>,
    pub matched: usize,
    pub targets: Vec<TradeId>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Unresolved {
    pub event_id: Option<String>,
    pub published_ns: u64,
    pub action: &'static str,
    pub reason: &'static str,
    pub candidates: Option<usize>,
    pub original_uid: Option<String>,
    pub original_seq: Option<String>,
    pub original_price: Option<i64>,
    pub original_volume: Option<i64>,
    pub hms: Option<i64>,
    pub qualifiers: String,
    pub witnesses: Vec<(String, u64)>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Ledger {
    pub patches: Vec<Patch>,
    pub insertions: Vec<Insertion>,
    pub applied: Vec<Applied>,
    pub unresolved: Vec<Unresolved>,
}
impl Ledger {
    /// Effective (count, volume) change against gross prints, restricted to
    /// target event times inside `[lo, hi)`.
    pub fn delta(&self, lo: u64, hi: u64) -> (i64, i64) {
        let mut count = 0i64;
        let mut volume = 0i64;
        for p in self
            .patches
            .iter()
            .filter(|p| lo <= p.event_ns && p.event_ns < hi)
        {
            match p.action {
                PatchAction::Cancel => {
                    count -= 1;
                    volume -= p.original_volume
                }
                PatchAction::Replace => volume += p.volume - p.original_volume,
            }
        }
        for i in self
            .insertions
            .iter()
            .filter(|i| lo <= i.event_ns && i.event_ns < hi)
        {
            count += 1;
            volume += i.volume
        }
        (count, volume)
    }
}

fn unresolved(c: &Correction, reason: &'static str, candidates: Option<usize>) -> Unresolved {
    Unresolved {
        event_id: Some(c.event_id.clone()),
        published_ns: c.published_ns,
        action: match c.action {
            Action::Cancel => "cancel",
            Action::Insert => "insert",
            Action::Amend => "amend",
            Action::UnresolvedConflictingFragments => "unresolved_conflicting_fragments",
            Action::UnresolvedAction => "unresolved_action",
        },
        reason,
        candidates,
        original_uid: c.original_uid.clone(),
        original_seq: c.original_seq.clone(),
        original_price: c.original_price,
        original_volume: c.original_volume,
        hms: c.hms,
        qualifiers: c.qualifiers.clone(),
        witnesses: c.witnesses.clone(),
    }
}

/// Python `attach_identities`: a raw witness is attached to a physical
/// trade only when the (published, price, volume, clock) group has exactly
/// as many witnesses as physical prints.
fn attach(candidates: &[Candidate], witnesses: &[Witness]) -> Vec<St> {
    type K = (u64, i64, i64, Option<i64>);
    let mut raw: HashMap<K, Vec<&Witness>> = HashMap::new();
    for w in witnesses.iter().filter(|w| w.kind == Kind::Trade) {
        if let (Some(p), Some(v)) = (w.price, w.volume) {
            raw.entry((w.published_ns, p, v, w.hms))
                .or_default()
                .push(w)
        }
    }
    let mut order: Vec<K> = Vec::new();
    let mut physical: HashMap<K, Vec<&Candidate>> = HashMap::new();
    for c in candidates {
        let hms = (c.exch_hms_ns != MISSING_CLOCK).then_some(c.exch_hms_ns as i64);
        let key = (c.published_ns, c.price, c.volume, hms);
        physical
            .entry(key)
            .or_insert_with(|| {
                order.push(key);
                Vec::new()
            })
            .push(c);
    }
    let mut states = Vec::with_capacity(candidates.len());
    for key in order {
        let mut w: Vec<&Witness> = raw.get(&key).cloned().unwrap_or_default();
        w.sort_by(|a, b| (a.source.as_str(), a.line).cmp(&(b.source.as_str(), b.line)));
        let mut rows = physical[&key].clone();
        rows.sort_by_key(|r| r.id);
        let same = w.len() == rows.len();
        for (i, r) in rows.into_iter().enumerate() {
            let a = same.then(|| w[i]);
            states.push(St {
                id: r.id,
                insert_event: None,
                published_ns: r.published_ns,
                event_ns: r.event_ns,
                price: r.price,
                volume: r.volume,
                active: true,
                uid: a.and_then(|a| a.uid.clone()),
                seq: a.and_then(|a| a.seq.clone()),
                trade_seq: a.and_then(|a| a.trade_seq.clone()),
                date: a.and_then(|a| a.date.clone()),
                recovered: a.is_some(),
                original: Original {
                    price: r.price,
                    volume: r.volume,
                    exch_hms_ns: r.exch_hms_ns,
                },
                history: Vec::new(),
                revisions: Vec::new(),
            })
        }
    }
    states
}

const IDS: [(&str, &str); 3] = [
    ("original_uid", "uid"),
    ("original_trade_seq", "trade_seq"),
    ("original_seq", "seq"),
];
fn reference<'a>(c: &'a Correction, name: &str) -> Option<&'a str> {
    match name {
        "original_uid" => c.original_uid.as_deref(),
        "original_trade_seq" => c.original_trade_seq.as_deref(),
        "original_seq" => c.original_seq.as_deref(),
        _ => unreachable!(),
    }
}

/// Python `resolve`. `candidates` must be in source order (publication,
/// source index, key order).
pub fn resolve(candidates: &[Candidate], witnesses: &[Witness]) -> Ledger {
    let mut states = attach(candidates, witnesses);
    let mut corrections = combine_corrections(witnesses);
    let ric = witnesses.first().map(|w| w.ric.clone()).unwrap_or_default();
    let mut out = Ledger::default();
    for (index, c) in corrections.iter_mut().enumerate() {
        c.event_id = format!("{ric}:{}:{index}", c.published_ns);
    }
    for c in &corrections {
        if let Some(reason) = c.action.unresolved() {
            out.unresolved.push(unresolved(c, reason, None));
            continue;
        }
        let prior: Vec<usize> = (0..states.len())
            .filter(|&i| states[i].published_ns <= c.published_ns)
            .collect();
        if c.action == Action::Insert {
            let (Some(price), Some(volume)) = (c.price, c.volume) else {
                out.unresolved
                    .push(unresolved(c, "insert_missing_price_or_volume", None));
                continue;
            };
            let mut already: Vec<usize> = prior
                .iter()
                .copied()
                .filter(|&i| {
                    let s = &states[i];
                    s.published_ns == c.published_ns && s.price == price && s.volume == volume
                })
                .collect();
            if already.is_empty() {
                if let Some(uid) = &c.uid {
                    already = prior
                        .iter()
                        .copied()
                        .filter(|&i| {
                            let s = &states[i];
                            s.uid.as_ref() == Some(uid) && s.price == price && s.volume == volume
                        })
                        .collect();
                }
            }
            if !already.is_empty() {
                out.applied.push(Applied {
                    event_id: c.event_id.clone(),
                    published_ns: c.published_ns,
                    result: "insert_already_in_trade_stream",
                    basis: None,
                    matched: already.len(),
                    targets: vec![],
                });
                continue;
            }
            let at = event_time(c.published_ns, c.hms);
            let hms = c.hms.map_or(MISSING_CLOCK, |v| v as u64);
            states.push(St {
                id: (u32::MAX, c.published_ns, states.len() as u64),
                insert_event: Some(c.event_id.clone()),
                published_ns: c.published_ns,
                event_ns: at,
                price,
                volume,
                active: true,
                uid: c.uid.clone(),
                seq: c.seq.clone(),
                trade_seq: c.trade_seq.clone(),
                date: c.date.clone(),
                recovered: true,
                original: Original {
                    price,
                    volume,
                    exch_hms_ns: hms,
                },
                history: Vec::new(),
                revisions: vec![c.event_id.clone()],
            });
            out.applied.push(Applied {
                event_id: c.event_id.clone(),
                published_ns: c.published_ns,
                result: "insert",
                basis: None,
                matched: 0,
                targets: vec![],
            });
            continue;
        }
        let mut evidence = None;
        let mut matches: Vec<usize> = Vec::new();
        for (source, target) in IDS {
            if let Some(r) = reference(c, source) {
                matches = prior
                    .iter()
                    .copied()
                    .filter(|&i| {
                        let s = &states[i];
                        s.id_field(target) == Some(r)
                            && (source == "original_uid"
                                || c.original_date.is_some()
                                || i128::from(c.published_ns) - i128::from(s.published_ns) <= DAY)
                    })
                    .collect();
                if !matches.is_empty() {
                    evidence = Some(source);
                    break;
                }
            }
        }
        let compatible = |s: &St| -> bool {
            let ok = |price: i64, volume: i64| {
                c.original_price.is_none_or(|v| v == price)
                    && c.original_volume.is_none_or(|v| v == volume)
            };
            ok(s.price, s.volume)
                || ok(s.original.price, s.original.volume)
                || s.history.iter().any(|h| ok(h.price, h.volume))
        };
        if !matches.is_empty() {
            matches.retain(|&i| compatible(&states[i]));
        } else {
            if c.original_price.is_none() || c.original_volume.is_none() {
                out.unresolved.push(unresolved(
                    c,
                    "no_usable_identity_or_original_payload",
                    None,
                ));
                continue;
            }
            matches = prior
                .iter()
                .copied()
                .filter(|&i| compatible(&states[i]))
                .collect();
            for (source, target) in IDS {
                if let Some(r) = reference(c, source) {
                    matches.retain(|&i| states[i].id_field(target).is_none_or(|v| v == r));
                }
            }
            if let Some(h) = c.original_hms {
                matches.retain(|&i| states[i].original.exch_hms_ns == h as u64);
            }
            evidence = Some("unique_original_payload");
        }
        // Explicit references cannot be contradicted by a weaker matched key.
        for (source, target) in IDS {
            if let Some(r) = reference(c, source) {
                matches.retain(|&i| states[i].id_field(target).is_none_or(|v| v == r));
            }
        }
        if c.original_date.is_some() {
            let want = date_key(&c.original_date);
            matches.retain(|&i| date_key(&states[i].date) == want);
        }
        if let Some(h) = c.original_hms {
            matches.retain(|&i| states[i].original.exch_hms_ns == h as u64);
        }
        if matches.is_empty() {
            out.unresolved
                .push(unresolved(c, "original_trade_not_found", None));
            continue;
        }
        if matches.iter().any(|&i| !states[i].recovered) {
            out.unresolved
                .push(unresolved(c, "original_source_identity_unrecovered", None));
            continue;
        }
        let mut fingerprint: Vec<(u64, i64, i64)> = matches
            .iter()
            .map(|&i| (states[i].event_ns, states[i].price, states[i].volume))
            .collect();
        fingerprint.sort();
        fingerprint.dedup();
        let payload = evidence == Some("unique_original_payload");
        if matches.len() > 1 && (payload || fingerprint.len() != 1) {
            out.unresolved.push(unresolved(
                c,
                "multiple_original_candidates",
                Some(matches.len()),
            ));
            continue;
        }
        if payload && c.original_hms.is_none() {
            // A clock inherited from the current last sale is not an
            // original clock; the ID-less path needs exact confirmation.
            if c.hms
                .is_none_or(|h| states[matches[0]].original.exch_hms_ns != h as u64)
            {
                out.unresolved
                    .push(unresolved(c, "original_clock_not_confirmed", None));
                continue;
            }
        }
        if c.action == Action::Amend {
            if let Some(h) = c.hms {
                if matches
                    .iter()
                    .any(|&i| states[i].original.exch_hms_ns != h as u64)
                {
                    out.unresolved.push(unresolved(
                        c,
                        "amendment_clock_change_requires_review",
                        None,
                    ));
                    continue;
                }
            }
        }
        let mut broke = false;
        for &i in &matches {
            let s = &mut states[i];
            if c.action == Action::Cancel {
                s.active = false
            } else {
                if !s.active {
                    out.unresolved
                        .push(unresolved(c, "amendment_of_inactive_trade", None));
                    broke = true;
                    break;
                }
                s.history.push(Version {
                    price: s.price,
                    volume: s.volume,
                });
                if let Some(p) = c.price {
                    s.price = p
                }
                if let Some(v) = c.volume {
                    s.volume = v
                }
            }
            s.revisions.push(c.event_id.clone());
        }
        if !broke {
            out.applied.push(Applied {
                event_id: c.event_id.clone(),
                published_ns: c.published_ns,
                result: if c.action == Action::Cancel {
                    "cancel"
                } else {
                    "amend"
                },
                basis: evidence,
                matched: matches.len(),
                targets: matches.iter().map(|&i| states[i].id).collect(),
            });
        }
    }
    for s in &states {
        if let Some(event_id) = &s.insert_event {
            if s.active {
                out.insertions.push(Insertion {
                    event_id: event_id.clone(),
                    published_ns: s.published_ns,
                    event_ns: s.event_ns,
                    exch_hms_ns: s.original.exch_hms_ns,
                    price: s.price,
                    volume: s.volume,
                })
            }
            continue;
        }
        if !s.active || s.price != s.original.price || s.volume != s.original.volume {
            out.patches.push(Patch {
                id: s.id,
                published_ns: s.published_ns,
                event_ns: s.event_ns,
                original_price: s.original.price,
                original_volume: s.original.volume,
                action: if s.active {
                    PatchAction::Replace
                } else {
                    PatchAction::Cancel
                },
                price: s.price,
                volume: s.volume,
                revisions: s.revisions.clone(),
            })
        }
    }
    out
}

/// One archived kind=12 row (RocksDB slim codec) of the RIC.
#[derive(Clone, Debug)]
pub struct ControlRow {
    pub published_ns: u64,
    pub price: Option<i64>,
    pub volume: Option<i64>,
    pub original_price: Option<i64>,
    pub original_volume: Option<i64>,
    pub qualifiers: String,
    pub origin: String,
}

/// Archived kind=12 rows without a raw source witness (Python compile:
/// multiset difference on (published, original price, original volume)).
pub fn pending(controls: &[ControlRow], witnesses: &[Witness]) -> Vec<Unresolved> {
    let mut seen: BTreeMap<(u64, Option<i64>, Option<i64>), usize> = BTreeMap::new();
    for w in witnesses.iter().filter(|w| w.kind == Kind::Correction) {
        *seen
            .entry((w.published_ns, w.original_price, w.original_volume))
            .or_default() += 1;
    }
    let mut out = Vec::new();
    for r in controls {
        let key = (r.published_ns, r.original_price, r.original_volume);
        match seen.get_mut(&key) {
            Some(n) if *n > 0 => *n -= 1,
            _ => out.push(Unresolved {
                event_id: None,
                published_ns: r.published_ns,
                action: "pending",
                reason: "source_witness_pending",
                candidates: None,
                original_uid: None,
                original_seq: None,
                original_price: r.original_price,
                original_volume: r.original_volume,
                hms: None,
                qualifiers: r.qualifiers.clone(),
                witnesses: vec![(r.origin.clone(), 0)],
            }),
        }
    }
    out
}

/// Per-RIC raw source witnesses (local NVMe), imported once from the
/// identity recovery of `extract_revision_identity.py`.
///
/// Layout: `manifest.json` plus `<component(RIC)>.jsonl`, each line the
/// original `{"source","line","fields"}` record in Python compile order
/// (identity jobs sorted by ID, file line order, duplicate (source, line)
/// dropped). A RIC listed with zero rows has an evidenced empty ledger.
pub struct WitnessStore {
    root: std::path::PathBuf,
    /// (year, RIC) -> segment files in append order.
    entries: BTreeMap<(i32, String), Vec<SegmentFile>>,
    hashes: BTreeMap<(i32, String), String>,
}

#[derive(Clone, Debug)]
struct SegmentFile {
    path: std::path::PathBuf,
    rows: u64,
    sha256: String,
}

pub const STORE_LIMIT_BYTES: u64 = 1 << 30;
const STORE_VERSION: u64 = 2;

fn read_manifest(root: &std::path::Path) -> Result<Value> {
    let m = super::io::read_json(&root.join("manifest.json"))?;
    ensure!(m["version"] == STORE_VERSION, "witness store version");
    Ok(m)
}

/// Layout (version 2): `manifest.json` lists immutable segments in append
/// order; each segment has a year and one jsonl per RIC
/// (`<segment>/<component(RIC)>.jsonl`, the original
/// `{"source","line","fields"}` records). The `base` segment is the
/// one-time import of the 2026-11-10 identity recovery (RICs listed with
/// zero rows have an evidenced empty ledger); later segments come from
/// daily batches (`witness-append`).
impl WitnessStore {
    pub fn open(root: &std::path::Path) -> Result<Self> {
        let m = read_manifest(root)?;
        let mut entries: BTreeMap<(i32, String), Vec<SegmentFile>> = BTreeMap::new();
        for seg in m["segments"].as_array().context("witness segments")? {
            let name = seg["name"].as_str().context("segment name")?;
            let year = i32::try_from(seg["year"].as_i64().context("segment year")?)?;
            for (ric, v) in seg["rics"].as_object().context("segment RICs")? {
                entries
                    .entry((year, ric.clone()))
                    .or_default()
                    .push(SegmentFile {
                        path: root
                            .join(name)
                            .join(format!("{}.jsonl", super::component(ric))),
                        rows: v["rows"].as_u64().context("witness rows")?,
                        sha256: v["sha256"].as_str().context("witness sha256")?.into(),
                    });
            }
        }
        let hashes = entries
            .iter()
            .map(|(k, files)| {
                use sha2::{Digest, Sha256};
                let mut d = Sha256::new();
                for f in files {
                    d.update(f.sha256.as_bytes());
                    d.update(b"\n");
                }
                (k.clone(), format!("{:x}", d.finalize()))
            })
            .collect();
        Ok(Self {
            root: root.into(),
            entries,
            hashes,
        })
    }
    /// Identity of the RIC-year's witnesses, without reading them.
    pub fn hash(&self, year: i32, ric: &str) -> Option<&str> {
        self.hashes.get(&(year, ric.to_owned())).map(String::as_str)
    }
    /// Witness rows (all segments, append order) and their identity; `None`
    /// when the store has no ledger for this RIC-year.
    pub fn load(&self, year: i32, ric: &str) -> Result<Option<(Vec<Witness>, String)>> {
        use sha2::{Digest, Sha256};
        use std::io::BufRead;
        let key = (year, ric.to_owned());
        let Some(files) = self.entries.get(&key) else {
            return Ok(None);
        };
        let mut out = Vec::new();
        let mut bytes = 0u64;
        for f in files {
            let file =
                std::fs::File::open(&f.path).with_context(|| format!("{}", f.path.display()))?;
            bytes += file.metadata()?.len();
            ensure!(bytes <= STORE_LIMIT_BYTES, "witness files too large: {ric}");
            let mut digest = Sha256::new();
            let mut rows = 0;
            for line in std::io::BufReader::with_capacity(1 << 16, file).lines() {
                let line = line?;
                digest.update(line.as_bytes());
                digest.update(b"\n");
                let w = Witness::from_json(&serde_json::from_str(&line)?)
                    .with_context(|| format!("witness row in {}", f.path.display()))?;
                ensure!(w.ric == ric, "witness RIC mismatch in {}", f.path.display());
                out.push(w);
                rows += 1;
            }
            ensure!(
                format!("{:x}", digest.finalize()) == f.sha256 && rows == f.rows,
                "witness file changed: {}",
                f.path.display()
            );
        }
        let _ = &self.root;
        Ok(Some((out, self.hashes[&key].clone())))
    }
}

/// Exclusive lock on the store for writers.
fn lock_store(root: &std::path::Path) -> Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    std::fs::create_dir_all(root)?;
    let lock = std::fs::File::create(root.join(".lock"))?;
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } == 0,
        "cannot lock witness store"
    );
    Ok(lock)
}

/// Write one immutable segment (`rows` per RIC, in order) and register it in
/// the manifest atomically. A segment already registered is left alone.
fn add_segment(
    root: &std::path::Path,
    name: &str,
    year: i32,
    rows: &BTreeMap<String, Vec<String>>,
    extra: Value,
) -> Result<bool> {
    use sha2::{Digest, Sha256};
    use std::io::Write;
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b)),
        "invalid segment name {name:?}"
    );
    let _lock = lock_store(root)?;
    let manifest = root.join("manifest.json");
    let mut m = if manifest.exists() {
        read_manifest(root)?
    } else {
        serde_json::json!({"version": STORE_VERSION, "segments": []})
    };
    let segments = m["segments"].as_array_mut().context("witness segments")?;
    if segments.iter().any(|s| s["name"] == name) {
        return Ok(false);
    }
    let dir = root.join(name);
    if dir.exists() {
        // Left over by an interrupted writer: never registered.
        std::fs::remove_dir_all(&dir)?;
    }
    let building = root.join(format!(".{name}.building"));
    if building.exists() {
        std::fs::remove_dir_all(&building)?;
    }
    std::fs::create_dir_all(&building)?;
    let mut listed = serde_json::Map::new();
    for (ric, lines) in rows {
        let path = building.join(format!("{}.jsonl", super::component(ric)));
        let mut f = std::fs::File::options()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut digest = Sha256::new();
        for line in lines {
            f.write_all(line.as_bytes())?;
            f.write_all(b"\n")?;
            digest.update(line.as_bytes());
            digest.update(b"\n");
        }
        f.sync_all()?;
        listed.insert(
            ric.clone(),
            serde_json::json!({"rows": lines.len(), "sha256": format!("{:x}", digest.finalize())}),
        );
    }
    std::fs::rename(&building, &dir)?;
    let mut seg = serde_json::json!({"name": name, "year": year, "rics": listed});
    if let Value::Object(e) = extra {
        for (k, v) in e {
            seg[k] = v;
        }
    }
    segments.push(seg);
    super::io::save(&manifest, &m)?;
    Ok(true)
}

/// Import the base segment from completed identity jobs. `rics` lists the
/// RICs whose (possibly empty) ledgers are evidenced by this recovery.
pub fn import_witnesses(
    jobs: &[std::path::PathBuf],
    identities: &std::path::Path,
    rics: &std::collections::BTreeSet<String>,
    year: i32,
    output: &std::path::Path,
) -> Result<()> {
    use std::io::BufRead;
    ensure!(
        !output.join("manifest.json").exists(),
        "witness store already exists: {}",
        output.display()
    );
    let mut ids = Vec::new();
    for path in jobs {
        let list = super::io::read_json(path)?;
        for job in list.as_array().context("identity job list")? {
            ids.push(job["id"].as_str().context("identity job id")?.to_owned());
        }
    }
    ids.sort();
    ids.dedup();
    let missing = ids
        .iter()
        .filter(|id| !identities.join(format!("{id}.done.json")).exists())
        .count();
    ensure!(
        missing == 0,
        "identity recovery incomplete: {missing} source jobs remain"
    );
    let mut seen = std::collections::HashSet::new();
    let mut per: BTreeMap<String, Vec<String>> = rics.iter().map(|r| (r.clone(), vec![])).collect();
    let mut bytes = 0u64;
    for id in &ids {
        let path = identities.join(format!("{id}.jsonl"));
        if !path.exists() {
            continue;
        }
        let file = std::fs::File::open(&path)?;
        for line in std::io::BufReader::with_capacity(1 << 16, file).lines() {
            let line = line?;
            let v: Value = serde_json::from_str(&line)?;
            let key = (
                v["source"].as_str().context("witness source")?.to_owned(),
                v["line"].as_u64().context("witness line")?,
            );
            if !seen.insert(key) {
                continue;
            }
            let w = Witness::from_json(&v).with_context(|| format!("{}", path.display()))?;
            if let Some(rows) = per.get_mut(&w.ric) {
                bytes += line.len() as u64 + 1;
                ensure!(bytes <= STORE_LIMIT_BYTES, "witness import over budget");
                rows.push(line);
            }
        }
    }
    add_segment(
        output,
        "base",
        year,
        &per,
        serde_json::json!({"source_jobs": ids.len(), "identities": identities, "job_lists": jobs}),
    )?;
    Ok(())
}

/// Fields kept from a raw CSV row (everything `Witness::from_json` reads).
const WITNESS_FIELDS: [&str; 19] = [
    "#RIC",
    "Date-Time",
    "Type",
    "Price",
    "Volume",
    "Original Price",
    "Original Volume",
    "Unique Trade Identification",
    "Original Unique Trade Identification",
    "Seq. No.",
    "Original Seq. No.",
    "Trade Sequence Number",
    "Original Trade Sequence Number",
    "Exch Time",
    "Original Exch Time",
    "Date",
    "Original Date",
    "Qualifiers",
    "Domain",
];
/// Most Correction rows / selected trades one daily segment may hold.
const SEGMENT_LIMIT: usize = 1_000_000;

fn open_csv(path: &std::path::Path) -> Result<csv::Reader<Box<dyn std::io::Read>>> {
    let file = std::fs::File::open(path).with_context(|| format!("{}", path.display()))?;
    let raw = std::io::BufReader::with_capacity(1 << 20, file);
    let name = path.to_string_lossy();
    let stream: Box<dyn std::io::Read> = if name.ends_with(".gz") {
        Box::new(flate2::read::MultiGzDecoder::new(raw))
    } else if name.ends_with(".zst") {
        Box::new(zstd::stream::read::Decoder::new(raw)?)
    } else {
        Box::new(raw)
    };
    Ok(csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .buffer_capacity(1 << 20)
        .from_reader(stream))
}

/// Raw-CSV evidence of one daily batch: every Correction row, plus every
/// Trade row of the same RIC whose (price, volume) equals a Correction's
/// original or new payload. Two streaming passes per file; the second only
/// runs when the file has corrections. Returns (rows per RIC, corrections).
pub fn extract_daily_witnesses(
    files: &[std::path::PathBuf],
    workers: usize,
) -> Result<(BTreeMap<String, Vec<String>>, usize)> {
    use rayon::prelude::*;
    ensure!((1..=8).contains(&workers), "witness workers must be 1..8");
    // One file per worker from a fixed pool; results merged in file order.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()?;
    let parts = pool.install(|| {
        files
            .par_iter()
            .map(|f| extract_file(f))
            .collect::<Result<Vec<_>>>()
    })?;
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut corrections = 0usize;
    let mut total = 0usize;
    for (rows, found) in parts {
        corrections += found;
        for (ric, mut lines) in rows {
            total += lines.len();
            out.entry(ric).or_default().append(&mut lines);
        }
        ensure!(total <= SEGMENT_LIMIT, "too many witness rows in one batch");
    }
    Ok((out, corrections))
}

fn extract_file(path: &std::path::Path) -> Result<(BTreeMap<String, Vec<String>>, usize)> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut total = 0usize;
    let mut corrections = 0usize;
    {
        let mut reader = open_csv(path)?;
        let header = reader.byte_headers()?.clone();
        let pos = |name: &str| header.iter().position(|h| h == name.as_bytes());
        let ty = pos("Type").context("CSV lacks Type")?;
        let ric = pos("#RIC").context("CSV lacks #RIC")?;
        let (price, volume) = (pos("Price"), pos("Volume"));
        let kept: Vec<(&str, usize)> = WITNESS_FIELDS
            .iter()
            .filter_map(|n| pos(n).map(|i| (*n, i)))
            .collect();
        let row_json = |r: &csv::ByteRecord, line: u64| -> Result<String> {
            let mut fields = serde_json::Map::new();
            for (n, i) in &kept {
                let v = std::str::from_utf8(r.get(*i).unwrap_or_default())?;
                if !v.is_empty() {
                    fields.insert((*n).into(), Value::String(v.into()));
                }
            }
            Ok(serde_json::to_string(
                &serde_json::json!({"source": path, "line": line, "fields": fields}),
            )?)
        };
        // Pass 1: Correction rows and the payloads they refer to.
        let mut pairs: std::collections::HashSet<(Vec<u8>, i64, i64)> = Default::default();
        let mut record = csv::ByteRecord::new();
        let mut line = 1u64;
        let mut found = 0usize;
        while reader.read_byte_record(&mut record)? {
            line += 1;
            if record.get(ty) != Some(b"Correction".as_slice()) {
                continue;
            }
            let json = row_json(&record, line)?;
            let w = Witness::from_json(&serde_json::from_str(&json)?)?;
            for pair in [(w.original_price, w.original_volume), (w.price, w.volume)] {
                if let (Some(p), Some(v)) = pair {
                    pairs.insert((w.ric.as_bytes().to_vec(), p, v));
                }
            }
            out.entry(w.ric).or_default().push(json);
            found += 1;
            total += 1;
            ensure!(total <= SEGMENT_LIMIT, "too many witness rows in one batch");
        }
        corrections += found;
        if pairs.is_empty() {
            return Ok((out, corrections));
        }
        // Pass 2: candidate trades of the same RICs.
        let (Some(price), Some(volume)) = (price, volume) else {
            bail!("CSV lacks Price/Volume: {}", path.display())
        };
        let mut reader = open_csv(path)?;
        let mut line = 1u64;
        while reader.read_byte_record(&mut record)? {
            line += 1;
            if record.get(ty) != Some(b"Trade".as_slice()) {
                continue;
            }
            let (Some(p), Some(v)) = (record.get(price), record.get(volume)) else {
                continue;
            };
            if p.is_empty() || v.is_empty() {
                continue;
            }
            let r = record.get(ric).unwrap_or_default();
            let key = (
                r.to_vec(),
                price_e9(std::str::from_utf8(p)?)?,
                quantity(std::str::from_utf8(v)?)?,
            );
            if pairs.contains(&key) {
                out.entry(std::str::from_utf8(r)?.into())
                    .or_default()
                    .push(row_json(&record, line)?);
                total += 1;
                ensure!(total <= SEGMENT_LIMIT, "too many witness rows in one batch");
            }
        }
    }
    Ok((out, corrections))
}

/// Append one daily batch as a store segment (idempotent by name).
pub fn append_daily_witnesses(
    store: &std::path::Path,
    name: &str,
    year: i32,
    files: &[std::path::PathBuf],
    workers: usize,
) -> Result<(bool, usize, usize)> {
    let (rows, corrections) = extract_daily_witnesses(files, workers)?;
    let n = rows.values().map(Vec::len).sum();
    let added = if rows.is_empty() {
        false
    } else {
        add_segment(
            store,
            name,
            year,
            &rows,
            serde_json::json!({"files": files}),
        )?
    };
    Ok((added, corrections, n))
}

#[cfg(test)]
mod tests {
    //! Cases of `scripts/lseg/test_trade_revision_ledger.py`.
    use super::*;
    use serde_json::json;

    const BASE: u64 = 1_767_312_000_000_000_000;

    fn raw(kind: &str, at: u64, values: &[(&str, &str)]) -> Witness {
        let mut fields = serde_json::Map::new();
        for (k, v) in [
            ("#RIC", "TEST".to_owned()),
            ("Date-Time", format!("2026-01-02T00:00:{at:02}.000000000Z")),
            ("Type", kind.to_owned()),
            ("Price", "100".into()),
            ("Volume", "2".into()),
            ("Exch Time", "00:00:00.000000000".into()),
            ("Date", "2026-01-02".into()),
            ("Unique Trade Identification", "A".into()),
            ("Seq. No.", "100".into()),
        ] {
            fields.insert(k.into(), json!(v));
        }
        if kind == "Correction" {
            for k in ["Price", "Volume", "Unique Trade Identification", "Seq. No."] {
                fields.remove(k);
            }
            for (k, v) in [
                ("Original Price", "100"),
                ("Original Volume", "2"),
                ("Original Unique Trade Identification", "A"),
                ("Qualifiers", "611[CAN_COND];2[CAN_COND_N]"),
            ] {
                fields.insert(k.into(), json!(v));
            }
        }
        for (k, v) in values {
            fields.insert((*k).into(), json!(v));
        }
        Witness::from_json(&json!({"source":"fixture","line":at+1,"fields":fields})).unwrap()
    }
    fn trade(at: u64) -> Witness {
        raw("Trade", at, &[])
    }
    fn cancel(at: u64, values: &[(&str, &str)]) -> Witness {
        raw("Correction", at, values)
    }
    fn tick(row: u64, at: u64, price: i64, volume: i64) -> Candidate {
        Candidate {
            id: (0, BASE + at * 1_000_000_000, row),
            published_ns: BASE + at * 1_000_000_000,
            event_ns: BASE,
            exch_hms_ns: 0,
            price,
            volume,
        }
    }
    fn t0() -> Candidate {
        tick(0, 0, 100_000_000_000, 2)
    }

    #[test]
    fn cancel_by_original_id_and_repeat_is_idempotent() {
        let r = resolve(&[t0()], &[trade(0), cancel(1, &[]), cancel(2, &[])]);
        assert_eq!(r.patches.len(), 1);
        assert_eq!(r.patches[0].action, PatchAction::Cancel);
        assert!(r.unresolved.is_empty());
    }

    #[test]
    fn cancel_with_new_fields_is_not_an_amendment_or_insert() {
        let c = cancel(1, &[("Price", "101"), ("Volume", "9")]);
        let r = resolve(&[t0()], &[trade(0), c]);
        assert_eq!(r.patches[0].action, PatchAction::Cancel);
        assert!(r.insertions.is_empty());
    }

    #[test]
    fn complementary_correction_fragments_apply_once() {
        let blank = cancel(1, &[("Qualifiers", "")]);
        let marked = cancel(1, &[("Price", "101"), ("Volume", "9")]);
        let groups = combine_corrections(&[blank.clone(), marked.clone()]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].action, Action::Cancel);
        let r = resolve(&[t0()], &[trade(0), blank, marked]);
        assert_eq!(r.applied.len(), 1);
    }

    #[test]
    fn missing_id_can_use_unique_full_payload_and_clock() {
        let t = raw("Trade", 0, &[("Unique Trade Identification", "")]);
        let r = resolve(&[t0()], &[t, cancel(1, &[])]);
        assert_eq!(r.patches[0].action, PatchAction::Cancel);
        assert_eq!(r.applied[0].basis, Some("unique_original_payload"));
    }

    #[test]
    fn ambiguous_originals_are_preserved_even_if_current_clock_matches_one() {
        let a = raw("Trade", 0, &[("Unique Trade Identification", "")]);
        let b = raw(
            "Trade",
            1,
            &[
                ("Unique Trade Identification", ""),
                ("Exch Time", "00:00:01.000000000"),
            ],
        );
        let mut other = tick(1, 1, 100_000_000_000, 2);
        other.exch_hms_ns = 1_000_000_000;
        other.event_ns += 1_000_000_000;
        let r = resolve(&[t0(), other], &[a, b, cancel(2, &[])]);
        assert!(r.patches.is_empty());
        assert_eq!(r.unresolved[0].reason, "multiple_original_candidates");
    }

    #[test]
    fn known_conflicting_identifier_is_not_overridden_by_price() {
        let t = raw("Trade", 0, &[("Unique Trade Identification", "B")]);
        let r = resolve(&[t0()], &[t, cancel(1, &[])]);
        assert!(r.patches.is_empty());
        assert_eq!(r.unresolved[0].reason, "original_trade_not_found");
    }

    #[test]
    fn current_last_sale_clock_is_not_assumed_to_be_original_clock() {
        let c = cancel(1, &[("Exch Time", "00:00:10.000000000")]);
        let t = raw("Trade", 0, &[("Unique Trade Identification", "")]);
        let r = resolve(&[t0()], &[t, c]);
        assert!(r.patches.is_empty());
        assert_eq!(r.unresolved[0].reason, "original_clock_not_confirmed");
    }

    fn insert(at: u64, extra: &[(&str, &str)]) -> Witness {
        let mut v = vec![
            ("Price", "101"),
            ("Volume", "3"),
            ("Qualifiers", "2[INSSALCOND]"),
            ("Original Price", ""),
            ("Original Volume", ""),
            ("Original Unique Trade Identification", ""),
        ];
        v.extend_from_slice(extra);
        raw("Correction", at, &v)
    }

    #[test]
    fn new_insert_is_counted_once_and_existing_trade_not_added_again() {
        let r = resolve(&[t0()], &[trade(0), insert(1, &[])]);
        assert_eq!(r.insertions.len(), 1);
        assert_eq!(r.delta(0, u64::MAX), (1, 3));
        let t = tick(0, 1, 101_000_000_000, 3);
        let r = resolve(
            &[t],
            &[
                raw("Trade", 1, &[("Price", "101"), ("Volume", "3")]),
                insert(1, &[]),
            ],
        );
        assert!(r.insertions.is_empty());
    }

    #[test]
    fn real_amendments_preserve_one_trade_and_support_a_later_cancel() {
        let amend = cancel(1, &[("Price", "101"), ("Volume", "3"), ("Qualifiers", "")]);
        let c = cancel(2, &[("Original Price", "101"), ("Original Volume", "3")]);
        let r = resolve(&[t0()], &[trade(0), amend, c]);
        assert_eq!(r.patches.len(), 1);
        assert_eq!(r.patches[0].action, PatchAction::Cancel);
        assert!(r.unresolved.is_empty());
    }

    #[test]
    fn amendment_replaces_price_and_volume_of_one_trade() {
        let amend = cancel(1, &[("Price", "101"), ("Volume", "3"), ("Qualifiers", "")]);
        let r = resolve(&[t0()], &[trade(0), amend]);
        assert_eq!(r.patches.len(), 1);
        assert_eq!(r.patches[0].action, PatchAction::Replace);
        assert_eq!(
            (r.patches[0].price, r.patches[0].volume),
            (101_000_000_000, 3)
        );
        assert_eq!(r.delta(0, u64::MAX), (0, 1));
    }

    #[test]
    fn insert_can_be_cancelled_later_without_a_physical_trade_patch() {
        let ins = insert(1, &[("Unique Trade Identification", "B")]);
        let c = cancel(
            2,
            &[
                ("Original Price", "101"),
                ("Original Volume", "3"),
                ("Original Unique Trade Identification", "B"),
            ],
        );
        let r = resolve(&[t0()], &[trade(0), ins, c]);
        assert!(r.patches.is_empty());
        assert!(r.insertions.is_empty());
        assert!(r.unresolved.is_empty());
    }

    #[test]
    fn weaker_sequence_cannot_override_conflicting_unique_id() {
        let c = cancel(1, &[("Original Seq. No.", "100")]);
        let t = raw("Trade", 0, &[("Unique Trade Identification", "B")]);
        let r = resolve(&[t0()], &[t, c]);
        assert!(r.patches.is_empty());
        assert_eq!(r.unresolved[0].reason, "original_trade_not_found");
    }

    #[test]
    fn ambiguous_amendment_clock_is_not_silently_retimed() {
        let amend = cancel(
            1,
            &[
                ("Price", "101"),
                ("Qualifiers", ""),
                ("Exch Time", "00:00:05.000000000"),
            ],
        );
        let r = resolve(&[t0()], &[trade(0), amend]);
        assert!(r.patches.is_empty());
        assert_eq!(
            r.unresolved[0].reason,
            "amendment_clock_change_requires_review"
        );
    }

    #[test]
    fn missing_source_witness_is_not_treated_as_an_empty_trade_identifier() {
        let r = resolve(&[t0()], &[cancel(1, &[])]);
        assert!(r.patches.is_empty());
        assert_eq!(
            r.unresolved[0].reason,
            "original_source_identity_unrecovered"
        );
    }

    #[test]
    fn archived_control_without_witness_is_pending() {
        let control = |at: u64| ControlRow {
            published_ns: BASE + at * 1_000_000_000,
            price: None,
            volume: None,
            original_price: Some(100_000_000_000),
            original_volume: Some(2),
            qualifiers: "2[CAN_COND_N]".into(),
            origin: "db".into(),
        };
        let p = pending(&[control(1), control(1), control(5)], &[cancel(1, &[])]);
        assert_eq!(p.len(), 2);
        assert!(p.iter().all(|u| u.reason == "source_witness_pending"));
    }

    #[test]
    fn decimal_and_clock_parsing_is_exact() {
        assert_eq!(price_e9("92.64").unwrap(), 92_640_000_000);
        assert_eq!(price_e9("-0.005").unwrap(), -5_000_000);
        assert!(price_e9("1.0000000001").is_err());
        assert_eq!(quantity("25").unwrap(), 25);
        assert!(quantity("2.5").is_err());
        assert_eq!(clock("14:03:35.000000000").unwrap(), 50_615_000_000_000);
        assert_eq!(
            event_time(BASE + 23 * 3_600_000_000_000, Some(3_600_000_000_000)),
            BASE + DAY as u64 + 3_600_000_000_000
        );
        assert_eq!(
            tags("611[CAN_COND];2[BLKSALCOND];Block Trade[USER]")["USER"],
            "Block Trade"
        );
        assert_eq!(tags("a[b]c[d]")["d"], "a[b]c");
        assert!(tags("x[]").is_empty());
        assert_eq!(tags("\"v\"[F]; y [G]")["G"], "y");
    }

    #[test]
    fn daily_evidence_is_appended_as_a_segment_and_netted() {
        use std::io::Write;
        let t = tempfile::tempdir().unwrap();
        let csv = t.path().join("CL_2026-01-02_tick.csv.gz");
        let mut gz = flate2::write::GzEncoder::new(
            std::fs::File::create(&csv).unwrap(),
            flate2::Compression::fast(),
        );
        let header = "#RIC,Domain,Date-Time,Type,Price,Volume,Qualifiers,Seq. No.,Exch Time,Date,\
Original Price,Original Volume,Unique Trade Identification,Original Unique Trade Identification\n";
        gz.write_all(header.as_bytes()).unwrap();
        for row in [
            "TEST,Market Price,2026-01-02T00:00:00.000000000Z,Trade,100,2,,100,00:00:00.000000000,2026-01-02,,,A,",
            "TEST,Market Price,2026-01-02T00:00:00.500000000Z,Trade,50,1,,101,00:00:00.000000000,2026-01-02,,,B,",
            "OTHER,Market Price,2026-01-02T00:00:00.600000000Z,Trade,100,2,,1,00:00:00.000000000,2026-01-02,,,Z,",
            "TEST,Market Price,2026-01-02T00:00:01.000000000Z,Correction,,,2[CAN_COND_N],102,00:00:00.000000000,,100,2,,A",
        ] {
            gz.write_all(row.as_bytes()).unwrap();
            gz.write_all(b"\n").unwrap();
        }
        gz.finish().unwrap();
        let (rows, corrections) = extract_daily_witnesses(std::slice::from_ref(&csv), 1).unwrap();
        assert_eq!(corrections, 1);
        // The correction and the same-RIC 100 x 2 print only.
        assert_eq!(rows.keys().collect::<Vec<_>>(), ["TEST"]);
        assert_eq!(rows["TEST"].len(), 2);
        let store = t.path().join("store");
        let base = BTreeMap::from([("TEST".to_owned(), vec![])]);
        assert!(add_segment(&store, "base", 2026, &base, json!({})).unwrap());
        let before = WitnessStore::open(&store).unwrap();
        assert_eq!(before.load(2026, "TEST").unwrap().unwrap().0.len(), 0);
        let files = [csv];
        assert_eq!(
            append_daily_witnesses(&store, "daily1_CL", 2026, &files, 2).unwrap(),
            (true, 1, 2)
        );
        // Idempotent by segment name.
        assert!(
            !append_daily_witnesses(&store, "daily1_CL", 2026, &files, 1)
                .unwrap()
                .0
        );
        let after = WitnessStore::open(&store).unwrap();
        assert_ne!(after.hash(2026, "TEST"), before.hash(2026, "TEST"));
        assert!(after.hash(2027, "TEST").is_none());
        let (w, _) = after.load(2026, "TEST").unwrap().unwrap();
        let r = resolve(&[t0()], &w);
        assert_eq!(r.patches.len(), 1);
        assert_eq!(r.patches[0].action, PatchAction::Cancel);
        assert_eq!(r.applied[0].basis, Some("original_uid"));
    }
}
