//! Zero-allocation extraction of the three routing fields from one MDL CSV line.
//!
//! Only `InstruID`, `TradDay` and `LocalTime` are looked at; every other byte is
//! passed through untouched, so the published frame equals the live ZMQ frame.

use anyhow::{bail, Context, Result};

pub const BAD_TIME: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Layout {
    pub symbol: usize,
    pub trade_day: usize,
    pub local_time: usize,
    pub fields: usize,
}

impl Layout {
    /// Column positions come from the file header, never from a per-exchange table.
    pub fn from_header(header: &[u8]) -> Result<Self> {
        let header = header.strip_suffix(b"\r").unwrap_or(header);
        let names: Vec<&[u8]> = header.split(|b| *b == b',').collect();
        let find = |name: &str| {
            names
                .iter()
                .position(|n| *n == name.as_bytes())
                .with_context(|| format!("header has no {name} column"))
        };
        let layout = Self {
            symbol: find("InstruID")?,
            trade_day: find("TradDay")?,
            local_time: find("LocalTime")?,
            fields: names.len(),
        };
        Ok(layout)
    }

    fn last_needed(&self) -> usize {
        self.symbol.max(self.trade_day).max(self.local_time)
    }
}

pub struct Fields<'a> {
    pub symbol: &'a [u8],
    pub trade_day: u32,
    /// Vendor receive time, ms of the natural day; `BAD_TIME` if unparsable.
    pub local_ms: u32,
}

/// Splits on commas with SIMD `memchr`, stopping after the last needed column.
#[inline]
pub fn extract<'a>(line: &'a [u8], layout: &Layout) -> Fields<'a> {
    let last = layout.last_needed();
    let mut spans = [(0usize, 0usize); 3];
    let mut start = 0usize;
    let mut index = 0usize;
    let mut iter = memchr::memchr_iter(b',', line);
    loop {
        let end = iter.next().unwrap_or(line.len());
        if index == layout.symbol {
            spans[0] = (start, end);
        }
        if index == layout.trade_day {
            spans[1] = (start, end);
        }
        if index == layout.local_time {
            spans[2] = (start, end);
        }
        if index == last || end == line.len() {
            break;
        }
        start = end + 1;
        index += 1;
    }
    Fields {
        symbol: &line[spans[0].0..spans[0].1],
        trade_day: parse_day(&line[spans[1].0..spans[1].1]),
        local_ms: parse_time(&line[spans[2].0..spans[2].1]),
    }
}

#[inline]
fn digits<const N: usize>(b: &[u8]) -> Option<u32> {
    let mut v = 0u32;
    for &c in &b[..N] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u32;
    }
    Some(v)
}

/// `YYYY-MM-DD` or `YYYYMMDD` -> `YYYYMMDD`; 0 if malformed.
#[inline]
pub fn parse_day(b: &[u8]) -> u32 {
    let v = match b.len() {
        10 if b[4] == b'-' && b[7] == b'-' => (|| {
            Some(digits::<4>(b)? * 10_000 + digits::<2>(&b[5..])? * 100 + digits::<2>(&b[8..])?)
        })(),
        8 => digits::<8>(b),
        _ => None,
    };
    v.unwrap_or(0)
}

/// `HH:MM:SS[.mmm]` -> ms of day; `BAD_TIME` if malformed.
#[inline]
pub fn parse_time(b: &[u8]) -> u32 {
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return BAD_TIME;
    }
    let hms = (|| {
        Some((
            digits::<2>(b)?,
            digits::<2>(&b[3..])?,
            digits::<2>(&b[6..])?,
        ))
    })();
    let Some((h, m, s)) = hms else {
        return BAD_TIME;
    };
    let ms = match b.len() {
        8 => Some(0),
        12 if b[8] == b'.' => digits::<3>(&b[9..]),
        _ => None,
    };
    match ms {
        Some(ms) if h < 24 && m < 60 && s < 61 => ((h * 60 + m) * 60 + s) * 1000 + ms,
        _ => BAD_TIME,
    }
}

/// Leading ASCII letters: `rb2601` -> `rb`, `IF2512` -> `IF`.
#[inline]
pub fn product_of(symbol: &[u8]) -> &[u8] {
    let n = symbol
        .iter()
        .position(|b| !b.is_ascii_alphabetic())
        .unwrap_or(symbol.len());
    &symbol[..n]
}

/// Lowercase canonical contract code. Since 2025-11-04 CZCE uses `YMM`
/// (`AP601`); three-digit codes are widened with the trading day's decade so
/// `AP601` and `AP2601` compare equal.
pub fn canonical_contract(symbol: &[u8], trade_day: u32) -> Result<String> {
    let text = std::str::from_utf8(symbol).context("non-UTF-8 contract")?;
    let lower = text.to_ascii_lowercase();
    let letters = product_of(lower.as_bytes()).len();
    let tail = &lower[letters..];
    if letters == 0 {
        bail!("contract {text:?} has no product letters");
    }
    if tail.len() == 3 && tail.bytes().all(|b| b.is_ascii_digit()) && trade_day != 0 {
        let year = (trade_day / 10_000) as i32;
        let digit = (tail.as_bytes()[0] - b'0') as i32;
        let mut full = year / 10 * 10 + digit;
        if full < year - 1 {
            full += 10;
        }
        return Ok(format!(
            "{}{:02}{}",
            &lower[..letters],
            full % 100,
            &tail[1..]
        ));
    }
    Ok(lower)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHFE_HEADER: &[u8] = b"InstruID,TradDay,ActionDay,UpdateTime,LastPrice,PreSetPrice,PreCloPrice,PreOpenInt,OpenPrice,HighPrice,LowPrice,Volume,Turnover,OpenInt,ClosePrice,SetPrice,ULimitPrice,LLimitPrice,PreDelta,CurrDelta,AveragePrice,BidPrice1,BidVolume1,BidPrice2,BidVolume2,BidPrice3,BidVolume3,BidPrice4,BidVolume4,BidPrice5,BidVolume5,AskPrice1,AskVolume1,AskPrice2,AskVolume2,AskPrice3,AskVolume3,AskPrice4,AskVolume4,AskPrice5,AskVolume5,LocalTime,SeqNo";
    const SHFE_LINE: &[u8] = b"ag2702,2026-11-13,2026-11-13,10:55:04.000,12097.000,12495.000,12341.000,270281.000,12246.000,12294.000,11938.000,724412,131264220765.000,277550.000,,,14994.000,9996.000,,,,12097.000,12,12096.000,11,12095.000,11,12094.000,16,12093.000,15,12099.000,2,12100.000,1,12101.000,10,12102.000,4,12103.000,41,10:55:04.032,4568413";

    #[test]
    fn extracts_shfe() {
        let layout = Layout::from_header(SHFE_HEADER).unwrap();
        assert_eq!(
            (layout.symbol, layout.trade_day, layout.local_time),
            (0, 1, 41)
        );
        let f = extract(SHFE_LINE, &layout);
        assert_eq!(f.symbol, b"ag2702");
        assert_eq!(f.trade_day, 20261113);
        assert_eq!(f.local_ms, ((10 * 60 + 55) * 60 + 4) * 1000 + 32);
    }

    #[test]
    fn truncated_line_is_kept_with_bad_fields() {
        let layout = Layout::from_header(SHFE_HEADER).unwrap();
        let f = extract(b"ss2702,2026-11-13,2026", &layout);
        assert_eq!(f.symbol, b"ss2702");
        assert_eq!(f.trade_day, 20261113);
        assert_eq!(f.local_ms, BAD_TIME);
    }

    #[test]
    fn czce_three_digit_codes() {
        assert_eq!(canonical_contract(b"AP601", 20251120).unwrap(), "ap2601");
        assert_eq!(canonical_contract(b"AP2601", 20251120).unwrap(), "ap2601");
        assert_eq!(canonical_contract(b"SR001", 20291215).unwrap(), "sr3001");
        assert_eq!(canonical_contract(b"rb2601", 20251120).unwrap(), "rb2601");
    }
}
