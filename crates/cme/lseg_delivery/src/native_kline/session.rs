use super::MINUTE;
use anyhow::{ensure, Result};
use regex::Regex;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::OnceLock,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum State {
    Unknown,
    Trading,
    Closed,
    PreOpen,
    Halted,
    NotTrading,
    Expired,
}
impl State {
    pub fn name(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Trading => "TRADING",
            Self::Closed => "CLOSED",
            Self::PreOpen => "PRE_OPEN",
            Self::Halted => "HALTED",
            Self::NotTrading => "NOT_TRADING",
            Self::Expired => "EXPIRED",
        }
    }
    pub fn trading(self) -> Option<bool> {
        if self == Self::Unknown {
            None
        } else {
            Some(self == Self::Trading)
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Unknown,
    Trading,
    Halted,
    Scheduled,
    Expired,
}
#[derive(Clone, Debug)]
pub struct Decoded {
    pub action: &'static str,
    pub state: State,
    pub reason: &'static str,
    pub trading: Option<bool>,
    pub issue: Option<String>,
    pub native: Option<String>,
    pub explicit_halt: bool,
}
type Fields = BTreeMap<String, BTreeSet<String>>;
fn fields(text: &str) -> (Fields, bool) {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    let regex = PATTERN.get_or_init(|| Regex::new(r"(?:^|;)([^;]*)\[([^\]]+)\]").unwrap());
    let mut out = Fields::new();
    for capture in regex.captures_iter(text) {
        out.entry(capture[2].to_string())
            .or_default()
            .insert(capture[1].trim().trim_matches('"').trim().to_string());
    }
    let bad = !regex
        .replace_all(text, "")
        .trim_matches(|c| matches!(c, ';' | ' ' | '\t' | '\r' | '\n'))
        .is_empty();
    (out, bad)
}
fn get<'a>(f: &'a Fields, key: &str) -> Option<&'a str> {
    f.get(key).and_then(|v| v.first().map(String::as_str))
}
const RELEVANT: &[&str] = &[
    "TRD_STATUS",
    "INST_PHASE",
    "TRD_TYPE",
    "ORD_ENT_ST",
    "HALT_REASN",
    "HALT_RSN",
    "PERIOD_CDE",
    "PERIOD_CD2",
    "SECUR_ST",
    "MKT_ST_IND",
    "PRC_QL_CD",
    "STAT_IND",
];
fn decode(text: &str, previous: State, cme: bool) -> Decoded {
    let (f, bad) = fields(text);
    let value = |key| get(&f, key);
    let terminal = previous == State::Expired && cme;
    let mut r = Decoded {
        action: "NO_CHANGE",
        state: previous,
        reason: "UNSPECIFIED",
        trading: previous.trading(),
        issue: None,
        native: f
            .get("HALT_REASN")
            .filter(|v| v.len() == 1)
            .and_then(|v| v.first().cloned()),
        explicit_halt: value("TRD_STATUS") == Some("H"),
    };
    if bad {
        r.action = "UNCLASSIFIED";
        r.state = if terminal {
            State::Expired
        } else {
            State::Unknown
        };
        r.trading = r.state.trading();
        r.reason = if terminal {
            "INSTRUMENT_EXPIRATION"
        } else {
            "UNSPECIFIED"
        };
        r.issue = Some("unparsed_qualifier_text".into());
        return r;
    }
    let conflicts: Vec<_> = f
        .iter()
        .filter(|(k, v)| RELEVANT.contains(&k.as_str()) && v.len() > 1)
        .map(|(k, _)| k.as_str())
        .collect();
    if !conflicts.is_empty() {
        r.action = "UNCLASSIFIED";
        r.state = if terminal {
            State::Expired
        } else {
            State::Unknown
        };
        r.trading = r.state.trading();
        r.reason = if terminal {
            "INSTRUMENT_EXPIRATION"
        } else {
            "UNSPECIFIED"
        };
        r.issue = Some(format!(
            "conflicting_qualifier_values:{}",
            conflicts.join(",")
        ));
        return r;
    }
    let status = value("TRD_STATUS");
    let trade = value("TRD_TYPE");
    let phase = value("INST_PHASE");
    let closed = trade == Some("4") && phase == Some("C");
    let preopen = trade == Some("21") && phase == Some("O");
    let trading = trade == Some("15") || trade == Some("17") && phase == Some("T");
    if terminal {
        r.reason = "INSTRUMENT_EXPIRATION";
        if trading || preopen || status == Some("H") {
            r.issue = Some("session_marker_after_expiration".into())
        }
        return r;
    }
    if status == Some("H") {
        r.action = "HALT";
        r.state = State::Halted;
        r.trading = Some(false);
        r.reason = "EXPLICIT_HALT";
        if trading {
            r.issue = Some("halt_and_trading_in_same_patch".into())
        }
        if value("HALT_REASN") == Some("4") {
            r.issue = Some("expiration_and_explicit_halt_in_same_patch".into())
        }
        return r;
    }
    if u8::from(closed) + u8::from(preopen) + u8::from(trading) > 1 {
        r.action = "UNCLASSIFIED";
        r.state = State::Unknown;
        r.trading = None;
        r.issue = Some("contradictory_session_markers".into());
        return r;
    }
    if !cme {
        if previous == State::Halted && status != Some("N") {
            r.reason = "EXPLICIT_HALT";
            return r;
        }
        r.state = State::Unknown;
        r.trading = None;
        if status == Some("N") {
            r.action = "HALT_RELEASE"
        } else {
            r.action = "UNCLASSIFIED";
            r.issue = Some("uncalibrated_product".into())
        }
        return r;
    }
    if !matches!(phase, None | Some("" | "C" | "O" | "T"))
        || !matches!(
            trade,
            None | Some("" | "2" | "4" | "15" | "17" | "18" | "21" | "24")
        )
    {
        r.action = "UNCLASSIFIED";
        r.state = State::Unknown;
        r.trading = None;
        r.issue = Some(format!(
            "unmapped_session_code:{}/{}",
            trade.unwrap_or("None"),
            phase.unwrap_or("None")
        ));
        return r;
    }
    if value("HALT_REASN") == Some("4") {
        if trading || value("HALT_RSN") == Some("SS") {
            r.action = "UNCLASSIFIED";
            r.state = State::Unknown;
            r.trading = None;
            r.issue = Some("expiration_conflicts_with_live_session_marker".into())
        } else {
            r.action = "EXPIRE";
            r.state = State::Expired;
            r.trading = Some(false);
            r.reason = "INSTRUMENT_EXPIRATION"
        }
        return r;
    }
    if trade == Some("2") || value("HALT_RSN") == Some("SS") {
        r.action = if trade == Some("2") {
            "HALT"
        } else {
            "NOT_AVAILABLE"
        };
        r.state = if matches!(previous, State::Trading | State::Halted) {
            State::Halted
        } else if matches!(previous, State::Closed | State::PreOpen) {
            previous
        } else {
            State::NotTrading
        };
        r.trading = Some(false);
        r.reason = if value("HALT_RSN") == Some("SS") {
            "SYSTEM_SUSPENDED"
        } else {
            "NATIVE_SUSPEND"
        };
    } else if trade == Some("18") && phase == Some("C") {
        r.action = "NOT_AVAILABLE";
        r.state = State::Closed;
        r.trading = Some(false)
    } else if closed {
        r.action = "CLOSE";
        r.state = State::Closed;
        r.trading = Some(false)
    } else if preopen {
        r.action = "PRE_OPEN";
        r.state = if previous == State::Halted {
            State::Halted
        } else {
            State::PreOpen
        };
        r.trading = Some(false);
        if previous == State::Halted {
            r.reason = "HALT_RECOVERY"
        }
    } else if trading {
        r.action = "OPEN";
        r.state = State::Trading;
        r.trading = Some(true)
    } else if status == Some("N") {
        r.action = "HALT_RELEASE";
        if previous == State::Halted {
            r.state = State::Unknown;
            r.trading = None
        }
    } else if value("ORD_ENT_ST") == Some("N") {
        r.action = "NO_CANCEL"
    } else if matches!(value("SECUR_ST"), Some("5" | "6")) {
        r.action = if value("SECUR_ST") == Some("5") {
            "IMPLIED_ON"
        } else {
            "IMPLIED_OFF"
        }
    } else if trade == Some("24") {
        r.action = "PRE_CROSS"
    }
    if !matches!(status, None | Some("" | "H" | "N")) {
        r.issue = Some(format!("unknown_trading_status:{}", status.unwrap()))
    }
    if (!matches!(value("HALT_REASN"), None | Some("" | "0" | "2"))
        || !matches!(value("HALT_RSN"), None | Some("" | "NH")))
        && value("HALT_RSN") != Some("SS")
    {
        if r.issue.is_none() {
            r.issue = Some("halt_reason_without_explicit_halt".into())
        }
        if r.action == "NO_CHANGE" && previous == State::Trading {
            r.state = State::Unknown;
            r.trading = None
        }
    }
    let unknown: Vec<_> = f
        .keys()
        .filter(|k| !RELEVANT.contains(&k.as_str()))
        .map(String::as_str)
        .collect();
    if !unknown.is_empty() && r.issue.is_none() {
        r.issue = Some(format!("unmapped_qualifier_fields:{}", unknown.join(",")))
    }
    r
}
pub struct Decoder {
    cme: bool,
    cache: HashMap<String, HashMap<State, Decoded>>,
    fifo: VecDeque<String>,
    bytes: usize,
}
impl Decoder {
    pub fn new(cme: bool) -> Self {
        Self {
            cme,
            cache: HashMap::new(),
            fifo: VecDeque::new(),
            bytes: 0,
        }
    }
    pub fn decode(&mut self, q: &str, previous: State) -> Decoded {
        if let Some(result) = self.cache.get(q).and_then(|v| v.get(&previous)) {
            return result.clone();
        }
        let result = decode(q, previous, self.cme);
        if !self.cache.contains_key(q) {
            while self.cache.len() >= 4096 || self.bytes + q.len() > 8 * 1024 * 1024 {
                if let Some(key) = self.fifo.pop_front() {
                    self.bytes -= key.len();
                    self.cache.remove(&key);
                } else {
                    break;
                }
            }
            self.bytes += q.len();
            self.fifo.push_back(q.to_owned());
            self.cache.insert(q.to_owned(), HashMap::new());
        }
        self.cache
            .get_mut(q)
            .unwrap()
            .insert(previous, result.clone());
        result
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Interval {
    pub start: u64,
    pub end: u64,
    pub phase: Phase,
}
pub struct Timeline {
    pub previous: State,
    phase: Phase,
    native: Option<String>,
    at: Option<u64>,
    cursor: u64,
    start: u64,
    end: u64,
    before: Phase,
    halt: bool,
    bad: bool,
    close: bool,
    preopen: bool,
    available: bool,
    pub intervals: Vec<Interval>,
    gaps: Vec<(u64, u64)>,
    next_gap: usize,
}
impl Timeline {
    pub fn new(start: u64, end: u64, gaps: Vec<(u64, u64)>) -> Self {
        Self {
            previous: State::Unknown,
            phase: Phase::Unknown,
            native: None,
            at: None,
            cursor: start,
            start,
            end,
            before: Phase::Unknown,
            halt: false,
            bad: false,
            close: false,
            preopen: false,
            available: false,
            intervals: Vec::new(),
            gaps,
            next_gap: 0,
        }
    }
    fn emit(&mut self, end: u64) -> Result<()> {
        let end = end.min(self.end);
        if end <= self.cursor {
            return Ok(());
        }
        if let Some(last) = self
            .intervals
            .last_mut()
            .filter(|r| r.phase == self.phase && r.end == self.cursor)
        {
            last.end = end
        } else {
            ensure!(
                self.intervals.len() < 1_000_000,
                "more than one million actual intervals in one RIC"
            );
            self.intervals.push(Interval {
                start: self.cursor,
                end,
                phase: self.phase,
            })
        }
        self.cursor = end;
        Ok(())
    }
    fn finish_time(&mut self) {
        let Some(at) = self.at else { return };
        let in_gap = self.gaps.iter().any(|&(lo, hi)| lo <= at && at < hi);
        self.phase = if self.previous == State::Expired {
            Phase::Expired
        } else if in_gap {
            self.previous = State::Unknown;
            self.native = None;
            Phase::Unknown
        } else if self.bad {
            Phase::Unknown
        } else if self.previous == State::Trading {
            Phase::Trading
        } else if self.halt {
            Phase::Halted
        } else if self.previous == State::Halted {
            if matches!(self.before, Phase::Trading | Phase::Halted) {
                Phase::Halted
            } else if self.before == Phase::Scheduled {
                self.before
            } else {
                Phase::Unknown
            }
        } else if self.close || self.preopen || self.available {
            if self.before == Phase::Halted && self.preopen && !self.close {
                Phase::Halted
            } else if self.native.as_deref() == Some("0") {
                Phase::Scheduled
            } else if matches!(self.native.as_deref(), Some("1" | "2" | "6"))
                && matches!(self.before, Phase::Trading | Phase::Halted)
            {
                Phase::Halted
            } else if self.before == Phase::Scheduled {
                self.before
            } else {
                Phase::Unknown
            }
        } else if self.previous == State::Unknown {
            Phase::Unknown
        } else {
            self.phase
        };
    }
    pub fn before(&mut self, at: u64) -> Result<()> {
        if self.at == Some(at) {
            return Ok(());
        }
        if let Some(previous) = self.at {
            ensure!(previous <= at, "status stream is not ordered");
            self.finish_time();
        }
        while self.next_gap < self.gaps.len() && self.gaps[self.next_gap].0 <= at {
            let boundary = self.gaps[self.next_gap].0.max(self.start);
            self.emit(boundary)?;
            self.previous = State::Unknown;
            self.phase = Phase::Unknown;
            self.native = None;
            self.next_gap += 1;
        }
        self.emit(at)?;
        self.at = Some(at);
        self.before = self.phase;
        self.halt = false;
        self.bad = false;
        self.close = false;
        self.preopen = false;
        self.available = false;
        Ok(())
    }
    pub fn apply(&mut self, r: &Decoded) {
        self.previous = r.state;
        if let Some(native) = &r.native {
            self.native = Some(native.clone())
        }
        self.halt |= r.explicit_halt;
        self.bad |= r.action == "UNCLASSIFIED";
        self.close |= r.action == "CLOSE";
        self.preopen |= r.action == "PRE_OPEN";
        self.available |= matches!(r.action, "NOT_AVAILABLE" | "HALT");
    }
    pub fn finish(mut self) -> Result<Vec<Interval>> {
        self.finish_time();
        while self.next_gap < self.gaps.len() {
            let at = self.gaps[self.next_gap].0.max(self.start);
            self.emit(at)?;
            self.phase = Phase::Unknown;
            self.previous = State::Unknown;
            self.next_gap += 1;
        }
        self.emit(self.end)?;
        Ok(self.intervals)
    }
}
pub fn label(intervals: &[Interval], at: u64) -> &'static str {
    let mut trading = 0;
    let mut halt = 0;
    let mut scheduled = 0;
    let first = intervals.partition_point(|r| r.end <= at);
    for r in &intervals[first..] {
        if r.start >= at + MINUTE {
            break;
        }
        let n = r.end.min(at + MINUTE).saturating_sub(r.start.max(at));
        match r.phase {
            Phase::Trading => trading += n,
            Phase::Halted => halt += n,
            Phase::Scheduled => scheduled += n,
            _ => {}
        }
    }
    if trading + halt + scheduled != MINUTE {
        "UNKNOWN"
    } else if trading == MINUTE {
        "TRADING"
    } else if halt == MINUTE {
        "HALTED"
    } else if scheduled == MINUTE {
        "SCHEDULED_CLOSED"
    } else {
        "PARTIAL"
    }
}
pub use crate::tasdb::event_time;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn halt_recovery_and_same_time_reason() {
        let mut d = Decoder::new(true);
        let mut t = Timeline::new(0, 10, vec![]);
        for (at, q) in [
            (1, "17[TRD_TYPE];T[INST_PHASE]"),
            (2, "2[TRD_TYPE];2[HALT_REASN]"),
            (3, "21[TRD_TYPE];O[INST_PHASE];0[HALT_REASN]"),
            (4, "17[TRD_TYPE];T[INST_PHASE]"),
        ] {
            t.before(at).unwrap();
            let r = d.decode(q, t.previous);
            t.apply(&r);
        }
        let r = t.finish().unwrap();
        assert_eq!(
            r.iter().map(|r| r.phase).collect::<Vec<_>>(),
            [
                Phase::Unknown,
                Phase::Trading,
                Phase::Halted,
                Phase::Trading
            ]
        );
        assert_eq!(r.iter().map(|r| r.end).collect::<Vec<_>>(), [1, 2, 4, 10]);
    }
    #[test]
    fn expiry_scope_and_gap() {
        let mut d = Decoder::new(true);
        assert_eq!(
            d.decode("4[HALT_REASN]", State::Trading).state,
            State::Expired
        );
        assert_eq!(
            d.decode("15[TRD_TYPE]", State::Expired).state,
            State::Expired
        );
        assert_eq!(
            Decoder::new(false)
                .decode("15[TRD_TYPE]", State::Unknown)
                .state,
            State::Unknown
        );
        let mut t = Timeline::new(0, 10, vec![(3, 5)]);
        for (at, q) in [
            (1, "15[TRD_TYPE]"),
            (6, "0[HALT_REASN]"),
            (7, "15[TRD_TYPE]"),
        ] {
            t.before(at).unwrap();
            let r = d.decode(q, t.previous);
            t.apply(&r);
        }
        assert_eq!(
            t.finish()
                .unwrap()
                .iter()
                .map(|r| r.phase)
                .collect::<Vec<_>>(),
            [
                Phase::Unknown,
                Phase::Trading,
                Phase::Unknown,
                Phase::Trading
            ]
        );
    }
}
