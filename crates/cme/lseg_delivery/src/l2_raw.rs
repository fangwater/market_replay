//! Raw LL2 (`Legacy Level 2`, incremental slot assignments) turned into the
//! rows of a Normalized LL2 file, so the Normalized pipeline (`l2_stream`:
//! segments, delta records, RocksDB) can wash it unchanged.
//!
//! Source: one message = a header row (RIC, time, class, `Number of FIDs`) plus
//! that many `FID` rows. Each depth FID (`BEST_BIDn`, `BEST_BSIZn`, `NO_BIDORDn`,
//! `BEST_ASKn`, `BEST_ASIZn`, `NO_ASKORDn`, n = 1..10) assigns one slot of the
//! book of that RIC; fields a message does not carry keep their value.
//!
//! Semantics are those of `cme_tas_replay::raw_ll2` (the low-frequency Raw
//! extractor): a slot never assigned is unknown and is **never** taken from
//! Normalized data; an unknown and an explicitly empty slot are both written as
//! an empty field; a `REFRESH` discards all earlier knowledge (the CSV carries no
//! ClearCache / RefreshComplete flag) and only the slots it names are known.
//! Output = one 66-column Normalized-format row per message that assigns a depth
//! slot or is a `REFRESH`; the row carries the whole book as known after it.
//! The `Exch Time` column is left empty (the washed book has no such field).
use anyhow::{bail, ensure, Context, Result};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

pub const LEVELS: usize = 10;
const SLOTS: usize = LEVELS * 6;
const MAX_VALUE: usize = 64;

/// Map of a depth FID name to its slot (`level * 6 + field`, field order:
/// bid price, bid size, bid orders, ask price, ask size, ask orders).
fn slot_of(name: &[u8]) -> Result<Option<usize>> {
    match name {
        b"BEST_BSZ10" => return Ok(Some(9 * 6 + 1)),
        b"BEST_ASZ10" => return Ok(Some(9 * 6 + 4)),
        b"NO_BIDRD10" => return Ok(Some(9 * 6 + 2)),
        b"NO_ASKRD10" => return Ok(Some(9 * 6 + 5)),
        _ => {}
    }
    const PREFIXES: [(&[u8], usize); 6] = [
        (b"BEST_BID", 0),
        (b"BEST_BSIZ", 1),
        (b"NO_BIDORD", 2),
        (b"BEST_ASK", 3),
        (b"BEST_ASIZ", 4),
        (b"NO_ASKORD", 5),
    ];
    for (p, field) in PREFIXES {
        if let Some(suffix) = name.strip_prefix(p) {
            if !suffix.is_empty() && suffix.iter().all(u8::is_ascii_digit) {
                let n: usize = std::str::from_utf8(suffix)?.parse()?;
                ensure!(
                    (1..=LEVELS).contains(&n),
                    "unsupported Raw depth level {}",
                    String::from_utf8_lossy(name)
                );
                return Ok(Some((n - 1) * 6 + field));
            }
        }
    }
    Ok(None)
}

/// Splits one CSV row (RFC 4180 quoting; a doubled quote is kept as is).
fn split_row<'a>(line: &'a [u8], out: &mut Vec<&'a [u8]>) {
    out.clear();
    if memchr::memchr(b'"', line).is_none() {
        out.extend(line.split(|b| *b == b','));
        return;
    }
    let (mut start, mut quoted) = (0usize, false);
    for (i, &b) in line.iter().enumerate() {
        match b {
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                out.push(unquote(&line[start..i]));
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(unquote(&line[start..]));
}

fn unquote(f: &[u8]) -> &[u8] {
    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        &f[1..f.len() - 1]
    } else {
        f
    }
}

pub struct RawConverter {
    /// Only these RICs are converted (all when `None`).
    wanted: Option<Arc<BTreeSet<String>>>,
    ric: Vec<u8>,
    ric_wanted: bool,
    seen: HashSet<Vec<u8>>,
    /// Time of the last message of the current RIC (monotonic check).
    last_ts: Vec<u8>,
    slots: Vec<Vec<u8>>,
    // message being read
    open: bool,
    declared: usize,
    got: usize,
    refresh: bool,
    changed: bool,
    ts: Vec<u8>,
    offset: Vec<u8>,
    /// Orphan FID rows are tolerated until the first header (a stream started in
    /// the middle of a message when it resumes at a later part).
    started: bool,
    pub messages: u64,
    pub rows_out: u64,
}

impl RawConverter {
    pub fn new(wanted: Option<Arc<BTreeSet<String>>>) -> Self {
        Self {
            wanted,
            ric: Vec::new(),
            ric_wanted: false,
            seen: HashSet::new(),
            last_ts: Vec::new(),
            slots: (0..SLOTS).map(|_| Vec::with_capacity(MAX_VALUE)).collect(),
            open: false,
            declared: 0,
            got: 0,
            refresh: false,
            changed: false,
            ts: Vec::new(),
            offset: Vec::new(),
            started: false,
            messages: 0,
            rows_out: 0,
        }
    }

    /// Converts whole raw lines; appends Normalized rows to `out`. A message
    /// that is not complete at the end of `block` is finished by the next call.
    pub fn convert(&mut self, block: &[u8], out: &mut Vec<u8>) -> Result<()> {
        let mut fields: Vec<&[u8]> = Vec::with_capacity(16);
        let mut pos = 0usize;
        while pos < block.len() {
            let eol = memchr::memchr(b'\n', &block[pos..]).map_or(block.len(), |i| pos + i);
            let mut line = &block[pos..eol];
            pos = (eol + 1).min(block.len());
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            if line.is_empty() || line[0] == b'#' {
                continue;
            }
            if line[0] == b',' {
                self.fid_row(line, &mut fields)?;
            } else {
                self.header_row(line, &mut fields, out)?;
            }
            if self.open && self.got == self.declared {
                self.finish_message(out)?;
            }
        }
        Ok(())
    }

    /// The next input is not the continuation of the previous one (a separately cut segment of a
    /// package): a message cut at the end of the previous segment is dropped, orphan FID rows at the
    /// start of the next one are tolerated. The RIC and its state are kept.
    pub fn discontinuity(&mut self) {
        self.open = false;
        self.started = false;
    }

    /// At the end of the stream: a message still open is truncated data.
    pub fn finish(&self) -> Result<()> {
        ensure!(
            !self.open,
            "truncated Raw LL2 message of {}: expected {} FIDs, got {}",
            String::from_utf8_lossy(&self.ric),
            self.declared,
            self.got
        );
        Ok(())
    }

    fn header_row<'a>(
        &mut self,
        line: &'a [u8],
        fields: &mut Vec<&'a [u8]>,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        ensure!(
            !self.open,
            "incomplete Raw LL2 message of {} before the next header",
            String::from_utf8_lossy(&self.ric)
        );
        split_row(line, fields);
        ensure!(
            fields.len() == 14,
            "Raw LL2 message header has {} columns: {}",
            fields.len(),
            String::from_utf8_lossy(line)
        );
        self.started = true;
        self.messages += 1;
        let ric = fields[0];
        if ric != self.ric.as_slice() {
            self.ric.clear();
            self.ric.extend_from_slice(ric);
            self.ric_wanted = match &self.wanted {
                None => true,
                Some(w) => std::str::from_utf8(ric).is_ok_and(|r| w.contains(r)),
            };
            // Only RICs that are washed must be contiguous (separately cut segments may repeat others).
            ensure!(
                !self.ric_wanted || self.seen.insert(ric.to_vec()),
                "RIC {} appears in more than one block of the stream",
                String::from_utf8_lossy(ric)
            );
            self.last_ts.clear();
            for s in &mut self.slots {
                s.clear();
            }
        }
        let class = fields[5];
        ensure!(
            matches!(class, b"UPDATE" | b"REFRESH" | b"STATUS"),
            "unsupported Raw LL2 message class {}",
            String::from_utf8_lossy(class)
        );
        let declared: usize = std::str::from_utf8(fields[13])
            .ok()
            .and_then(|s| s.parse().ok())
            .with_context(|| {
                format!(
                    "bad Number of FIDs {:?}",
                    String::from_utf8_lossy(fields[13])
                )
            })?;
        ensure!(
            class != b"STATUS" || declared == 0,
            "Raw STATUS with FIDs needs an explicit policy"
        );
        self.open = true;
        self.declared = declared;
        self.got = 0;
        self.refresh = class == b"REFRESH";
        self.changed = self.refresh;
        if self.ric_wanted {
            let ts = fields[2];
            if self.last_ts.len() == ts.len() {
                ensure!(
                    ts >= self.last_ts.as_slice(),
                    "decreasing Raw timestamp for {}",
                    String::from_utf8_lossy(ric)
                );
            }
            self.last_ts.clear();
            self.last_ts.extend_from_slice(ts);
            self.ts.clear();
            self.ts.extend_from_slice(ts);
            self.offset.clear();
            self.offset.extend_from_slice(fields[3]);
            if self.refresh {
                // The CSV omits ClearCache / RefreshComplete: earlier knowledge
                // cannot be verified and is dropped (as the Raw extractor does).
                for s in &mut self.slots {
                    s.clear();
                }
            }
        }
        if declared == 0 {
            self.finish_message(out)?;
        }
        Ok(())
    }

    fn fid_row<'a>(&mut self, line: &'a [u8], fields: &mut Vec<&'a [u8]>) -> Result<()> {
        if !self.open {
            ensure!(!self.started, "orphan Raw LL2 FID row");
            return Ok(());
        }
        self.got += 1;
        ensure!(
            self.got <= self.declared,
            "more FID rows than declared for {}",
            String::from_utf8_lossy(&self.ric)
        );
        if !self.ric_wanted {
            return Ok(());
        }
        split_row(line, fields);
        ensure!(
            fields.len() >= 9 && fields[4] == b"FID",
            "invalid Raw LL2 FID row: {}",
            String::from_utf8_lossy(line)
        );
        if let Some(slot) = slot_of(fields[7])? {
            let v = fields[8];
            ensure!(v.len() <= MAX_VALUE, "Raw depth value too long");
            let s = &mut self.slots[slot];
            s.clear();
            s.extend_from_slice(v);
            self.changed = true;
        }
        Ok(())
    }

    fn finish_message(&mut self, out: &mut Vec<u8>) -> Result<()> {
        self.open = false;
        if !(self.ric_wanted && self.changed) {
            return Ok(());
        }
        out.extend_from_slice(&self.ric);
        out.extend_from_slice(b",Market Price,");
        out.extend_from_slice(&self.ts);
        out.push(b',');
        out.extend_from_slice(&self.offset);
        out.extend_from_slice(b",Normalized LL2");
        for s in &self.slots {
            out.push(b',');
            out.extend_from_slice(s);
        }
        out.extend_from_slice(b",\n"); // Exch Time (empty)
        self.rows_out += 1;
        Ok(())
    }
}

/// A RIC list file: one RIC per line (`#` comments and blank lines ignored).
pub fn read_ric_list(path: &std::path::Path) -> Result<BTreeSet<String>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let set: BTreeSet<String> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect();
    if set.is_empty() {
        bail!("{}: no RIC", path.display());
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(text: &str, wanted: Option<&[&str]>, chunk: usize) -> String {
        let w = wanted.map(|v| Arc::new(v.iter().map(|s| s.to_string()).collect()));
        let mut c = RawConverter::new(w);
        let mut out = Vec::new();
        // cut at line boundaries into chunks of about `chunk` bytes
        let mut start = 0;
        while start < text.len() {
            let mut end = (start + chunk).min(text.len());
            while end < text.len() && text.as_bytes()[end - 1] != b'\n' {
                end += 1;
            }
            c.convert(&text.as_bytes()[start..end], &mut out).unwrap();
            start = end;
        }
        c.finish().unwrap();
        String::from_utf8(out).unwrap()
    }

    const SRC: &str = "\
#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs
AAA,Market Price,2024-01-01T10:00:00.000000001Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,1,3
,,,,FID,5,,TIMACT,10:00:00,
,,,,FID,730,,BEST_BSIZ1,3,
,,,,FID,6544,,NO_BIDORD1,2,
AAA,Market Price,2024-01-01T10:00:00.000000002Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,2,2
,,,,FID,22,,BEST_BID1,100.25,
,,,,FID,6615,,HALT_RSN,\"  \",
AAA,Market Price,2024-01-01T10:00:01.000000000Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,3,1
,,,,FID,6517,,HALT_REASN,0,
AAA,Market Price,2024-01-01T10:00:02.000000000Z,-6,Legacy Level 2,REFRESH,,,,,1,38,4,2
,,,,FID,25,,BEST_ASK1,100.5,
,,,,FID,2421,,BEST_BSZ10,7,
BBB,Market Price,2024-01-01T10:00:03.000000000Z,-6,Legacy Level 2,STATUS,,,,,1,,5,0
BBB,Market Price,2024-01-01T10:00:04.000000000Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,6,1
,,,,FID,22,,BEST_BID2,9.5,
";

    #[test]
    fn rows_carry_the_known_book_and_refresh_forgets() {
        let out = run(SRC, None, 1 << 20);
        let rows: Vec<Vec<&str>> = out.lines().map(|l| l.split(',').collect()).collect();
        assert_eq!(rows.len(), 4, "{out}");
        assert!(rows.iter().all(|r| r.len() == 66));
        // row 1: size and orders of level 1 known, nothing else
        assert_eq!(
            &rows[0][..5],
            [
                "AAA",
                "Market Price",
                "2024-01-01T10:00:00.000000001Z",
                "-6",
                "Normalized LL2"
            ]
        );
        assert_eq!(&rows[0][5..11], ["", "3", "2", "", "", ""]);
        // row 2: price added, sizes kept (HALT_RSN is not a depth field)
        assert_eq!(&rows[1][5..11], ["100.25", "3", "2", "", "", ""]);
        // the halt-only message makes no row; REFRESH forgets bid side
        assert_eq!(&rows[2][5..11], ["", "", "", "100.5", "", ""]);
        assert_eq!(rows[2][5 + 9 * 6 + 1], "7");
        // BBB starts empty (state is per RIC); STATUS makes no row
        assert_eq!(&rows[3][..2], ["BBB", "Market Price"]);
        assert_eq!(&rows[3][11..17], ["9.5", "", "", "", "", ""]);
    }

    #[test]
    fn chunking_and_a_ric_filter_do_not_change_the_rows() {
        let whole = run(SRC, None, 1 << 20);
        for chunk in [1, 7, 120, 400] {
            assert_eq!(run(SRC, None, chunk), whole, "chunk {chunk}");
        }
        let only_b = run(SRC, Some(&["BBB"]), 50);
        assert_eq!(only_b.lines().count(), 1);
        assert!(only_b.starts_with("BBB,"));
    }

    #[test]
    fn a_stream_resumed_mid_message_skips_the_orphan_rows() {
        let tail = ",,,,FID,22,,BEST_BID1,1,\nBBB,Market Price,2024-01-01T10:00:04.000000000Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,6,1\n,,,,FID,22,,BEST_BID2,9.5,\n";
        let out = run(tail, None, 1 << 20);
        assert_eq!(out.lines().count(), 1);
    }

    #[test]
    fn separately_cut_segments_drop_the_cut_message_and_may_repeat_unwanted_rics() {
        let hdr = |ric: &str, ts: &str, n: usize| {
            format!("{ric},Market Price,2024-01-01T{ts}Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,1,{n}\n")
        };
        let fid = |name: &str, v: &str| format!(",,,,FID,22,,{name},{v},\n");
        let mut c = RawConverter::new(Some(Arc::new(["BBB".to_string()].into())));
        let mut out = Vec::new();
        // segment 1: unwanted AAA, then BBB, then a BBB message cut after 1 of 2 FIDs
        let seg1 = format!(
            "{}{}{}{}{}{}",
            hdr("AAA", "10:00:00.000000001", 1),
            fid("BEST_BID1", "1"),
            hdr("BBB", "10:00:01.000000000", 1),
            fid("BEST_BID1", "2"),
            hdr("BBB", "10:00:02.000000000", 2),
            fid("BEST_BID1", "3"),
        );
        c.convert(seg1.as_bytes(), &mut out).unwrap();
        c.discontinuity();
        // segment 2: orphan row of the cut message, BBB goes on, then unwanted AAA again
        let seg2 = format!(
            "{}{}{}{}{}",
            fid("BEST_ASK1", "9"),
            hdr("BBB", "10:00:03.000000000", 1),
            fid("BEST_BID1", "4"),
            hdr("AAA", "10:00:04.000000000", 1),
            fid("BEST_BID1", "5"),
        );
        c.convert(seg2.as_bytes(), &mut out).unwrap();
        c.finish().unwrap();
        let rows: Vec<String> = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
        // the cut message (10:00:02) is dropped, the others of BBB are kept
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert!(rows[0].contains("10:00:01") && rows[1].contains("10:00:03"));
        // a wanted RIC coming back after another RIC is still an error
        let mut c = RawConverter::new(Some(Arc::new(["BBB".to_string()].into())));
        let mut out = Vec::new();
        let bad = format!(
            "{}{}{}",
            hdr("BBB", "10:00:01.000000000", 0),
            hdr("CCC", "10:00:02.000000000", 0),
            hdr("BBB", "10:00:03.000000000", 0)
        );
        assert!(c.convert(bad.as_bytes(), &mut out).is_err());
    }

    #[test]
    fn bad_streams_fail() {
        let mut c = RawConverter::new(None);
        let mut out = Vec::new();
        // a RIC returning after another RIC
        let again = format!("{SRC}AAA,Market Price,2024-01-01T11:00:00.000000000Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,7,0\n");
        assert!(c.convert(again.as_bytes(), &mut out).is_err());
        // truncated message at the end
        let mut c = RawConverter::new(None);
        let cut = "AAA,Market Price,2024-01-01T10:00:00.000000001Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,1,3\n,,,,FID,5,,TIMACT,10:00:00,\n";
        c.convert(cut.as_bytes(), &mut out).unwrap();
        assert!(c.finish().is_err());
        // time going backwards
        let mut c = RawConverter::new(None);
        let back = "AAA,Market Price,2024-01-01T10:00:02.000000000Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,1,0\nAAA,Market Price,2024-01-01T10:00:01.000000000Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,1,,2,0\n";
        assert!(c.convert(back.as_bytes(), &mut out).is_err());
    }

    /// Real data: the rows of the converter equal the final state per 15 s bucket
    /// of the low-frequency Raw extractor (`cme_tas_replay::raw_ll2::Extractor`).
    /// `RAW_LL2_SAMPLE=<part.csv.gz> cargo test --release real_sample -- --ignored`
    #[test]
    #[ignore]
    fn real_sample_equals_the_raw_extractor() {
        use cme_tas_replay::raw_ll2::{Extractor, RawScanner};
        use std::io::Read;
        let path = std::env::var("RAW_LL2_SAMPLE").expect("RAW_LL2_SAMPLE");
        let mut text = Vec::new();
        flate2::read::MultiGzDecoder::new(std::fs::File::open(path).unwrap())
            .take(400 << 20)
            .read_to_end(&mut text)
            .unwrap();
        let cut = text.iter().rposition(|b| *b == b'\n').unwrap() + 1;
        let text = &text[..cut];

        let mut conv = RawConverter::new(None);
        let mut rows = Vec::new();
        conv.convert(text, &mut rows).unwrap();
        // last converter row per (ric, 15 s bucket)
        let mut last: std::collections::HashMap<(String, u64), Vec<String>> = Default::default();
        for line in String::from_utf8(rows).unwrap().lines() {
            let f: Vec<String> = line.split(',').map(str::to_string).collect();
            let ts = cme_tas_replay::ll2_source::parse_ll2_datetime_ns(f[2].as_bytes()).unwrap();
            last.insert((f[0].clone(), ts / 15_000_000_000), f);
        }

        let mut scanner = RawScanner::default();
        let mut ex = Extractor::new(15).unwrap();
        let mut snaps = Vec::new();
        scanner
            .scan(text, false, None, &mut |m| {
                ex.apply(&m, &mut |s| {
                    snaps.push(s);
                    Ok(())
                })
            })
            .unwrap();
        ex.finish(&mut |s| {
            snaps.push(s);
            Ok(())
        })
        .unwrap();
        assert!(snaps.len() > 100, "sample too small: {}", snaps.len());
        let mut compared = 0u64;
        for s in &snaps {
            let row = last
                .get(&(s.ric.clone(), s.bucket_ns / 15_000_000_000))
                .unwrap_or_else(|| panic!("no row for {} {}", s.ric, s.bucket_ns));
            for lvl in 0..LEVELS {
                let v = &s.value;
                let vals: [(usize, i64); 6] = [
                    (0, v.bid_prices[lvl]),
                    (1, v.bid_sizes[lvl]),
                    (2, v.bid_counts[lvl] as i64),
                    (3, v.ask_prices[lvl]),
                    (4, v.ask_sizes[lvl]),
                    (5, v.ask_counts[lvl] as i64),
                ];
                for (field, value) in vals {
                    let slot = lvl * 6 + field;
                    let text = &row[5 + slot];
                    let known = s.known >> slot & 1 == 1;
                    let missing = value == cme_tas_replay::MISSING_PRICE
                        || (matches!(field, 2 | 5)
                            && value == cme_tas_replay::MISSING_VOLUME as i64);
                    if !known || missing {
                        assert!(
                            text.is_empty(),
                            "{} slot {slot}: {text:?} but unknown/null",
                            s.ric
                        );
                    } else {
                        let got = if matches!(field, 2 | 5) {
                            text.parse::<i64>().unwrap()
                        } else {
                            (text.parse::<f64>().unwrap() * 1e9).round() as i64
                        };
                        assert_eq!(got, value, "{} slot {slot} bucket {}", s.ric, s.bucket_ns);
                    }
                    compared += 1;
                }
            }
        }
        eprintln!("compared {compared} slots of {} snapshots", snaps.len());
    }
}
