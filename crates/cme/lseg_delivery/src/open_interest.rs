//! Open interest from LSEG TAS (`normalised`) source CSVs.
//!
//! Source facts (checked 2026-11-25 on the TY / ES daily files and the 2021 package):
//! `Open Interest` is filled on a few `Trade` rows per RIC and day that carry no price or
//! volume; their `Date` names the trade date the figure belongs to. CME's figure arrives twice,
//! preliminary about 20:15 CT on the trade date and final about 09:05 CT the next day, each time
//! as two identical rows about 0.2 s apart.
//!
//! Nothing is inferred or filled: every row with a non-empty value is kept. The only folding is
//! that consecutive rows of one RIC with the same (`Date`, value, `Type`) become one observation
//! with the first / last publication time and a row count, so "the value known at time t" is
//! still exact. Values that are not non-negative integers, or rows whose time or date do not
//! parse, are kept as rejects with the raw text; they never become observations.
use crate::gz_members::MemberReader;
use anyhow::{bail, ensure, Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use flate2::read::MultiGzDecoder;
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

/// Positions of the needed columns, looked up by name (the packages differ in layout: before
/// 2021 there is no `Alias Underlying RIC`, so `Open Interest` is column 62 instead of 63).
/// Stored with every loaded source; bump when the meaning of a stored row changes.
pub const RULE_VERSION: &str = "lseg_oi_v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Columns {
    pub ric: usize,
    pub date_time: usize,
    pub row_type: usize,
    pub date: usize,
    pub open_interest: usize,
    pub width: usize,
}

impl Columns {
    pub fn from_header(line: &[u8]) -> Result<Self> {
        let header = parse_record(trim_eol(line)).context("parse TAS header")?;
        ensure!(
            header.get(0) == Some(b"#RIC".as_slice()),
            "TAS header does not start with #RIC"
        );
        let find = |name: &str| {
            let hits: Vec<usize> = header
                .iter()
                .enumerate()
                .filter(|(_, f)| *f == name.as_bytes())
                .map(|(i, _)| i)
                .collect();
            ensure!(
                hits.len() == 1,
                "TAS header has {} {name:?} columns",
                hits.len()
            );
            Ok(hits[0])
        };
        Ok(Self {
            ric: 0,
            date_time: find("Date-Time")?,
            row_type: find("Type")?,
            date: find("Date")?,
            open_interest: find("Open Interest")?,
            width: header.len(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Observation {
    pub ric: String,
    /// Source `Date`: the trade date the figure belongs to. `None` when the row has none.
    pub oi_date: Option<NaiveDate>,
    pub open_interest: i64,
    pub row_type: String,
    pub first_published_ns: i64,
    pub last_published_ns: i64,
    pub rows: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Reject {
    pub ric: String,
    pub published_ns: Option<i64>,
    pub raw_value: String,
    pub raw_date: String,
    pub reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Observation(Observation),
    Reject(Reject),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Stats {
    /// Data rows seen (header and empty lines excluded).
    pub data_rows: u64,
    /// Rows that contain a quote and therefore went through the full CSV parser.
    pub quoted_rows: u64,
    /// Rows with a non-empty `Open Interest`.
    pub oi_rows: u64,
    pub observations: u64,
    pub rejects: u64,
    pub min_published_ns: Option<i64>,
    pub max_published_ns: Option<i64>,
    /// gzip members read (set by `scan_gz`).
    pub gzip_members: u64,
}

pub struct Scanner {
    cols: Columns,
    pending: HashMap<String, Observation>,
    pub stats: Stats,
}

impl Scanner {
    pub fn new(cols: Columns) -> Self {
        Self {
            cols,
            pending: HashMap::new(),
            stats: Stats::default(),
        }
    }

    /// One data line (with or without the line ending).
    pub fn line(&mut self, line: &[u8], out: &mut dyn FnMut(Event) -> Result<()>) -> Result<()> {
        let line = trim_eol(line);
        if line.is_empty() {
            return Ok(());
        }
        self.stats.data_rows += 1;
        // Fast path: almost every row ends here because the value is empty. Only rows with a
        // value go through the full CSV parser.
        let field = if memchr::memchr(b'"', line).is_none() {
            nth_field(line, self.cols.open_interest)
        } else {
            self.stats.quoted_rows += 1;
            nth_field_quoted(line, self.cols.open_interest)
        };
        match field {
            Some([]) | Some(b"\"\"") => return Ok(()),
            Some(_) => {}
            None => bail!(
                "row has fewer than {} fields: {}",
                self.cols.open_interest + 1,
                preview(line)
            ),
        }
        let rec = parse_record(line)?;
        ensure!(
            rec.len() == self.cols.width,
            "row has {} fields, header {}: {}",
            rec.len(),
            self.cols.width,
            preview(line)
        );
        let raw = &rec[self.cols.open_interest];
        if raw.is_empty() {
            return Ok(());
        }
        self.stats.oi_rows += 1;
        let text = |i: usize| String::from_utf8_lossy(&rec[i]).into_owned();
        let ric = text(self.cols.ric);
        let raw_date = text(self.cols.date);
        let published = parse_utc_ns(&rec[self.cols.date_time]);
        let reject = |reason| {
            Event::Reject(Reject {
                ric: ric.clone(),
                published_ns: published,
                raw_value: String::from_utf8_lossy(raw).into_owned(),
                raw_date: raw_date.clone(),
                reason,
            })
        };
        let value = parse_count(raw);
        let date = if raw_date.is_empty() {
            Some(None)
        } else {
            NaiveDate::parse_from_str(&raw_date, "%Y-%m-%d")
                .ok()
                .map(Some)
        };
        let failure = if ric.is_empty() {
            Some("empty RIC")
        } else if published.is_none() {
            Some("bad Date-Time")
        } else if value.is_none() {
            Some("value is not a non-negative integer")
        } else if date.is_none() {
            Some("bad Date")
        } else {
            None
        };
        if let Some(reason) = failure {
            self.stats.rejects += 1;
            return out(reject(reason));
        }
        let (published, value, oi_date) = (published.unwrap(), value.unwrap(), date.unwrap());
        self.stats.min_published_ns = Some(
            self.stats
                .min_published_ns
                .map_or(published, |m| m.min(published)),
        );
        self.stats.max_published_ns = Some(
            self.stats
                .max_published_ns
                .map_or(published, |m| m.max(published)),
        );
        let row_type = text(self.cols.row_type);
        if let Some(p) = self.pending.get_mut(&ric) {
            if p.oi_date == oi_date && p.open_interest == value && p.row_type == row_type {
                p.first_published_ns = p.first_published_ns.min(published);
                p.last_published_ns = p.last_published_ns.max(published);
                p.rows += 1;
                return Ok(());
            }
        }
        let new = Observation {
            ric: ric.clone(),
            oi_date,
            open_interest: value,
            row_type,
            first_published_ns: published,
            last_published_ns: published,
            rows: 1,
        };
        if let Some(old) = self.pending.insert(ric, new) {
            self.stats.observations += 1;
            out(Event::Observation(old))?;
        }
        Ok(())
    }

    /// Emits the observations still open, in RIC order.
    pub fn finish(mut self, out: &mut dyn FnMut(Event) -> Result<()>) -> Result<Stats> {
        let mut rest: Vec<Observation> = self.pending.drain().map(|(_, o)| o).collect();
        rest.sort_by(|a, b| a.ric.cmp(&b.ric));
        for o in rest {
            self.stats.observations += 1;
            out(Event::Observation(o))?;
        }
        Ok(self.stats)
    }
}

/// Reads the header line of a TAS gzip (part 0 of a package, or a daily file).
pub fn read_header(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::new(MultiGzDecoder::new(BufReader::new(file)));
    let mut header = Vec::new();
    reader
        .read_until(b'\n', &mut header)
        .with_context(|| format!("read header of {}", path.display()))?;
    ensure!(
        header.starts_with(b"#RIC,"),
        "{} does not start with a TAS header",
        path.display()
    );
    Ok(header)
}

/// Scans one TAS gzip. `header` is the package header for parts after part 0 (which have none);
/// a file that starts with a header line must carry exactly that header.
/// `max_compressed_bytes` cuts the input for measurements; the cut member is then dropped.
pub fn scan_gz(
    path: &Path,
    header: &[u8],
    max_compressed_bytes: Option<u64>,
    out: &mut dyn FnMut(Event) -> Result<()>,
) -> Result<Stats> {
    let mut reader = MemberReader::open(path, max_compressed_bytes)?;
    let mut scanner = Scanner::new(Columns::from_header(header)?);
    let mut carry: Vec<u8> = Vec::new();
    let mut first = true;
    let mut on_line = |line: &[u8], scanner: &mut Scanner| -> Result<()> {
        if first {
            first = false;
            if line.starts_with(b"#RIC,") {
                ensure!(
                    trim_eol(line) == trim_eol(header),
                    "{} has a header that differs from its package header",
                    path.display()
                );
                return Ok(());
            }
        }
        scanner.line(line, out).with_context(|| {
            format!(
                "{} data row {}",
                path.display(),
                scanner.stats.data_rows + 1
            )
        })
    };
    while let Some(text) = reader
        .next_member()
        .with_context(|| format!("decompress {}", path.display()))?
    {
        let mut rest = text;
        if !carry.is_empty() {
            match memchr::memchr(b'\n', rest) {
                Some(i) => {
                    carry.extend_from_slice(&rest[..i]);
                    rest = &rest[i + 1..];
                    on_line(&carry, &mut scanner)?;
                    carry.clear();
                }
                None => {
                    carry.extend_from_slice(rest);
                    continue;
                }
            }
        }
        let mut pos = 0;
        for nl in memchr::memchr_iter(b'\n', rest) {
            on_line(&rest[pos..nl], &mut scanner)?;
            pos = nl + 1;
        }
        carry.extend_from_slice(&rest[pos..]);
    }
    if !carry.is_empty() && !reader.truncated() {
        on_line(&carry, &mut scanner)?;
    }
    if reader.truncated() {
        log::info!(
            "{}: input cut after {} members",
            path.display(),
            reader.members()
        );
    }
    let mut stats = scanner.finish(out)?;
    stats.gzip_members = reader.members();
    Ok(stats)
}

fn trim_eol(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && matches!(bytes[end - 1], b'\n' | b'\r') {
        end -= 1;
    }
    &bytes[..end]
}

/// The `index`-th comma-separated field of an unquoted line. The rows are mostly commas
/// (about 70% of the bytes), so commas are counted a block at a time instead of searched one
/// by one (memchr per comma was 3x slower than inflating).
fn nth_field(line: &[u8], index: usize) -> Option<&[u8]> {
    const BLOCK: usize = 32;
    let mut left = index; // commas still to pass
    let mut pos = 0;
    if left > 0 {
        while pos + BLOCK <= line.len() {
            let n = line[pos..pos + BLOCK]
                .iter()
                .filter(|&&b| b == b',')
                .count();
            if n >= left {
                break;
            }
            left -= n;
            pos += BLOCK;
        }
        loop {
            let b = *line.get(pos)?;
            pos += 1;
            if b == b',' {
                left -= 1;
                if left == 0 {
                    break;
                }
            }
        }
    }
    let end = memchr::memchr(b',', &line[pos..]).map_or(line.len(), |i| pos + i);
    Some(&line[pos..end])
}

/// Like `nth_field` for a line with quotes: commas inside quotes do not separate fields (an
/// escaped `""` toggles twice). Returns the raw field, quotes included.
fn nth_field_quoted(line: &[u8], index: usize) -> Option<&[u8]> {
    let (mut field, mut start, mut quoted) = (0, 0, false);
    for (i, &b) in line.iter().enumerate() {
        match b {
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                if field == index {
                    return Some(&line[start..i]);
                }
                field += 1;
                start = i + 1;
            }
            _ => {}
        }
    }
    (field == index).then(|| &line[start..])
}

fn parse_record(line: &[u8]) -> Result<csv::ByteRecord> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(line);
    let mut record = csv::ByteRecord::new();
    ensure!(reader.read_byte_record(&mut record)?, "empty CSV row");
    let mut extra = csv::ByteRecord::new();
    ensure!(
        !reader.read_byte_record(&mut extra)?,
        "one line holds more than one CSV row: {}",
        preview(line)
    );
    Ok(record)
}

fn parse_count(raw: &[u8]) -> Option<i64> {
    if raw.is_empty() || raw.len() > 18 || !raw.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(raw).ok()?.parse().ok()
}

/// `2026-11-19T02:13:13.759809748Z` (any number of fraction digits up to 9) to UTC ns.
pub fn parse_utc_ns(raw: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(raw).ok()?.strip_suffix('Z')?;
    let t = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f").ok()?;
    t.and_utc().timestamp_nanos_opt()
}

fn preview(line: &[u8]) -> String {
    String::from_utf8_lossy(&line[..line.len().min(160)]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "#RIC,Alias Underlying RIC,Domain,Date-Time,GMT Offset,Type,Qualifiers,Date,Open Interest,Price";

    fn run(lines: &[&str]) -> (Vec<Event>, Stats) {
        let mut s = Scanner::new(Columns::from_header(HEADER.as_bytes()).unwrap());
        let mut events = Vec::new();
        for l in lines {
            s.line(l.as_bytes(), &mut |e| {
                events.push(e);
                Ok(())
            })
            .unwrap();
        }
        let stats = s
            .finish(&mut |e| {
                events.push(e);
                Ok(())
            })
            .unwrap();
        (events, stats)
    }

    fn obs(e: &Event) -> &Observation {
        match e {
            Event::Observation(o) => o,
            Event::Reject(r) => panic!("unexpected reject {r:?}"),
        }
    }

    #[test]
    fn nth_field_matches_split() {
        let lines: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"a".to_vec(),
            b",,,".to_vec(),
            b"x,yy,,zzz,".to_vec(),
            (0..200)
                .map(|i| if i % 3 == 0 { b'v' } else { b',' })
                .collect(),
            [vec![b','; 100], b"OI".to_vec(), vec![b','; 50]].concat(),
        ];
        for line in &lines {
            let fields: Vec<&[u8]> = line.split(|&b| b == b',').collect();
            for i in 0..fields.len() + 3 {
                assert_eq!(
                    nth_field(line, i),
                    fields.get(i).copied(),
                    "line {line:?} field {i}"
                );
                assert_eq!(
                    nth_field_quoted(line, i),
                    fields.get(i).copied(),
                    "quoted {line:?} {i}"
                );
            }
        }
        let q = b"a,\"x,\"\"y\"\",z\",,\"\",7";
        let want: [&[u8]; 5] = [b"a", b"\"x,\"\"y\"\",z\"", b"", b"\"\"", b"7"];
        for (i, w) in want.iter().enumerate() {
            assert_eq!(nth_field_quoted(q, i), Some(*w));
        }
        assert_eq!(nth_field_quoted(q, 5), None);
    }

    #[test]
    fn header_columns_by_name() {
        let c = Columns::from_header(format!("{HEADER}\r\n").as_bytes()).unwrap();
        assert_eq!(
            (c.date_time, c.row_type, c.date, c.open_interest, c.width),
            (3, 5, 7, 8, 10)
        );
        assert!(Columns::from_header(b"RIC,Date-Time").is_err());
        assert!(Columns::from_header(b"#RIC,Date-Time,Type,Date,Open Interest,Date").is_err());
    }

    #[test]
    fn folds_repeats_and_keeps_revisions() {
        let (events, stats) = run(&[
            "TYZ26,,Market Price,2026-11-19T00:36:12.1Z,-6,Trade,Special Trades[USER],2026-11-19,,112.5",
            "TYZ26,,Market Price,2026-11-19T02:13:13.759809748Z,-6,Trade,,2026-11-18,6002177,",
            "TYZ26,,Market Price,2026-11-19T02:13:13.924791375Z,-6,Trade,,2026-11-18,6002177,",
            "TYH27,,Market Price,2026-11-19T02:13:13.759315648Z,-6,Trade,,2026-11-18,382931,",
            "TYZ26,,Market Price,2026-11-19T15:05:21.916030125Z,-6,Trade,,2026-11-18,5954926,",
            "TYZ26,,Market Price,2026-11-19T15:05:22.451593949Z,-6,Trade,,2026-11-18,5954926,\r\n",
        ]);
        assert_eq!(stats.data_rows, 6);
        assert_eq!(stats.oi_rows, 5);
        assert_eq!(stats.observations, 3);
        assert_eq!(events.len(), 3);
        let first = obs(&events[0]);
        assert_eq!(first.ric, "TYZ26");
        assert_eq!(first.open_interest, 6002177);
        assert_eq!(first.oi_date, NaiveDate::from_ymd_opt(2026, 11, 18));
        assert_eq!(first.rows, 2);
        assert_eq!(
            first.first_published_ns,
            parse_utc_ns(b"2026-11-19T02:13:13.759809748Z").unwrap()
        );
        assert_eq!(
            first.last_published_ns,
            parse_utc_ns(b"2026-11-19T02:13:13.924791375Z").unwrap()
        );
        // finish emits in RIC order
        assert_eq!(obs(&events[1]).ric, "TYH27");
        assert_eq!(obs(&events[2]).open_interest, 5954926);
        assert_eq!(obs(&events[2]).rows, 2);
    }

    #[test]
    fn same_value_again_after_a_change_is_a_new_observation() {
        let (events, _) = run(&[
            "A,,M,2026-01-01T00:00:00Z,0,Trade,,2026-01-01,5,",
            "A,,M,2026-01-01T00:00:01Z,0,Trade,,2026-01-01,6,",
            "A,,M,2026-01-01T00:00:02Z,0,Trade,,2026-01-01,5,",
        ]);
        let v: Vec<i64> = events.iter().map(|e| obs(e).open_interest).collect();
        assert_eq!(v, [5, 6, 5]);
    }

    #[test]
    fn quoted_rows_use_the_full_parser() {
        let (events, stats) = run(&[
            "A,,M,2026-01-01T00:00:00Z,0,Trade,\"x,y\",2026-01-01,7,",
            "A,,M,2026-01-01T00:00:01Z,0,Trade,\"x,y\",2026-01-01,,1.5",
        ]);
        assert_eq!(stats.quoted_rows, 2);
        assert_eq!(stats.oi_rows, 1);
        assert_eq!(obs(&events[0]).open_interest, 7);
    }

    #[test]
    fn rejects_keep_the_raw_text() {
        let (events, stats) = run(&[
            "A,,M,2026-01-01T00:00:00Z,0,Trade,,2026-01-01,1.5,",
            "A,,M,2026-01-01T00:00:00Z,0,Trade,,2026-01-01,-3,",
            "A,,M,bad,0,Trade,,2026-01-01,3,",
            "A,,M,2026-01-01T00:00:00Z,0,Trade,,20260101,3,",
            "A,,M,2026-01-01T00:00:00Z,0,Trade,,,4,",
        ]);
        assert_eq!(stats.rejects, 4);
        let reasons: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                Event::Reject(r) => Some(r.reason),
                _ => None,
            })
            .collect();
        assert_eq!(
            reasons,
            [
                "value is not a non-negative integer",
                "value is not a non-negative integer",
                "bad Date-Time",
                "bad Date"
            ]
        );
        // An empty Date is kept as an observation without a date.
        let o = events.iter().find_map(|e| match e {
            Event::Observation(o) => Some(o),
            _ => None,
        });
        assert_eq!(o.unwrap().oi_date, None);
    }

    #[test]
    fn short_rows_fail_loudly() {
        let mut s = Scanner::new(Columns::from_header(HEADER.as_bytes()).unwrap());
        assert!(s.line(b"A,,M,2026", &mut |_| Ok(())).is_err());
        let mut s = Scanner::new(Columns::from_header(HEADER.as_bytes()).unwrap());
        assert!(s
            .line(
                b"A,,M,2026-01-01T00:00:00Z,0,Trade,,2026-01-01,4",
                &mut |_| Ok(())
            )
            .is_err());
    }
}
