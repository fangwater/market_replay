//! Tonglian MDL single-frame parser shared by live (`cn-futures-pbs --zmq`)
//! and post-market tools. Moved verbatim from mkt_signal
//! `crates/cn_futures_pbs` (`msg.rs` helpers + `sources/zmq.rs` field table):
//! borrowed bytes, stack slots, no heap allocation, no JSON.
//!
//! Frame: `<topic> || <CSV payload>`; the CSV payload equals one line of the
//! NFS `tonglian_msg/YYYYMMDD/mdl_*.csv` files.

use chrono::NaiveDate;

pub const UNKNOWN_QUANTITY: u64 = u64::MAX;
/// Longest native contract code accepted (the live IPC buffer is 64 bytes).
pub const MAX_SYMBOL_LEN: usize = 63;

/// The six futures quote channels. Other upstream channels (combos, TQS,
/// order queues, quotas) are not single-leg five-level quotes.
pub const TOPICS: [&str; 6] = [
    "mdl.21.1.*",
    "mdl.22.1.*",
    "mdl.22.3.*",
    "mdl.23.1.*",
    "mdl.24.1.*",
    "mdl.26.1.*",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseError(pub &'static str);
impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ParseError {}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MdlLevel {
    /// NaN when absent.
    pub price: f64,
    /// `UNKNOWN_QUANTITY` when absent; an explicit zero stays zero.
    pub quantity: u64,
}

/// One parsed five-level quote. Missing floats are NaN, missing integers
/// `UNKNOWN_QUANTITY`, unknown dates/times 0. No multiplier or scaling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MdlQuote<'a> {
    pub venue: &'static str,
    /// Native contract code as sent (CZCE keeps its 3-digit `YMM`).
    pub symbol: &'a [u8],
    pub trading_day: u32,
    pub action_day: u32,
    /// ActionDay + UpdateTime in Asia/Shanghai, as Unix microseconds.
    pub exchange_ts_us: i64,
    pub source_sequence: u64,
    pub last_price: f64,
    /// Cumulative lots for the trading day.
    pub volume: u64,
    /// Cumulative turnover (CNY, multiplier included).
    pub turnover: f64,
    pub open_interest: f64,
    pub bid: [MdlLevel; 5],
    pub ask: [MdlLevel; 5],
}

pub fn unsigned(s: &[u8]) -> Result<u64, ParseError> {
    if s.is_empty() {
        return Ok(UNKNOWN_QUANTITY);
    }
    let mut n = 0u64;
    for b in s {
        if !b.is_ascii_digit() {
            return Err(ParseError("invalid unsigned quantity"));
        }
        n = n
            .checked_mul(10)
            .and_then(|n| n.checked_add((b - b'0') as u64))
            .ok_or(ParseError("quantity overflow"))?;
    }
    Ok(n)
}

pub fn number(s: &[u8]) -> Result<f64, ParseError> {
    if s.is_empty() {
        return Ok(f64::NAN);
    }
    let x = std::str::from_utf8(s)
        .map_err(|_| ParseError("non-ASCII number"))?
        .parse::<f64>()
        .map_err(|_| ParseError("invalid decimal"))?;
    if !x.is_finite() {
        return Err(ParseError("nonfinite decimal"));
    }
    Ok(if x.abs() >= 1e100 { f64::NAN } else { x })
}

pub fn day(s: &[u8]) -> Result<u32, ParseError> {
    if s.is_empty() {
        return Ok(0);
    }
    let mut digits = [0u8; 8];
    match s.len() {
        8 => digits.copy_from_slice(s),
        10 if s[4] == b'-' && s[7] == b'-' => {
            digits[..4].copy_from_slice(&s[..4]);
            digits[4..6].copy_from_slice(&s[5..7]);
            digits[6..].copy_from_slice(&s[8..]);
        }
        _ => return Err(ParseError("invalid date format")),
    }
    let value = unsigned(&digits)? as u32;
    NaiveDate::from_ymd_opt((value / 10000) as i32, (value / 100) % 100, value % 100)
        .ok_or(ParseError("invalid date"))?;
    Ok(value)
}

/// ActionDay is the calendar date. TradDay must never supply the night-session date.
pub fn timestamp(action_day: u32, time: &[u8], extra_millis: u32) -> Result<i64, ParseError> {
    if action_day == 0 || time.is_empty() {
        return Ok(0);
    }
    if !matches!(time.len(), 8 | 12) || time[2] != b':' || time[5] != b':' || extra_millis >= 1000 {
        return Err(ParseError("invalid quote time"));
    }
    let h = unsigned(&time[..2])?;
    let m = unsigned(&time[3..5])?;
    let s = unsigned(&time[6..8])?;
    let millis = if time.len() == 12 {
        if time[8] != b'.' || extra_millis != 0 {
            return Err(ParseError("ambiguous quote milliseconds"));
        }
        unsigned(&time[9..])? as u32
    } else {
        extra_millis
    };
    let d = NaiveDate::from_ymd_opt(
        (action_day / 10000) as i32,
        (action_day / 100) % 100,
        action_day % 100,
    )
    .ok_or(ParseError("invalid action day"))?;
    let dt = d
        .and_hms_milli_opt(h as u32, m as u32, s as u32, millis)
        .ok_or(ParseError("invalid quote time"))?;
    Ok(dt.and_utc().timestamp_micros() - 8 * 3600 * 1_000_000)
}

/// Same rule the live IPC symbol buffer enforces.
pub fn validate_symbol(s: &[u8]) -> Result<(), ParseError> {
    if s.is_empty() || s.len() > MAX_SYMBOL_LEN || !s.iter().all(u8::is_ascii_alphanumeric) {
        return Err(ParseError("invalid native contract"));
    }
    Ok(())
}

/// Parses one `<topic> || <csv>` frame of the six futures quote channels.
/// Check order is part of the contract (identical error for identical input).
pub fn parse_frame(frame: &[u8]) -> Result<MdlQuote<'_>, ParseError> {
    if frame.get(10..14) != Some(b" || ") {
        return Err(ParseError("invalid single-frame delimiter"));
    }
    // count, symbol, action date, last/volume/turnover/OI, bid start, ask start, stride.
    let (venue, count, symbol, action, last, volume, turnover, oi, bid, ask, stride) =
        match &frame[..10] {
            b"mdl.21.1.*" => ("CFFEX", 43, 3, 0, 4, 8, 9, 10, 19, 29, 2),
            b"mdl.22.1.*" => ("SHFE", 43, 0, 2, 4, 11, 12, 13, 21, 31, 2),
            b"mdl.22.3.*" => ("INE", 43, 0, 2, 4, 11, 12, 13, 21, 31, 2),
            b"mdl.23.1.*" => ("CZCE", 59, 3, 0, 4, 8, 9, 10, 23, 38, 3),
            b"mdl.24.1.*" => ("DCE", 59, 3, 0, 4, 9, 10, 11, 23, 38, 3),
            b"mdl.26.1.*" => ("GFEX", 59, 3, 0, 4, 9, 10, 11, 23, 38, 3),
            _ => return Err(ParseError("not a supported futures L2 topic")),
        };
    let mut fields: [&[u8]; 59] = [b""; 59];
    let mut len = 0;
    for (index, value) in frame[14..].split(|b| *b == b',').enumerate() {
        if index < 59 {
            fields[index] = value;
        }
        len = index + 1;
    }
    if len < count {
        return Err(ParseError("truncated futures quote"));
    }
    validate_symbol(fields[symbol])?;
    let trading_day = day(fields[1])?;
    let action_day = day(fields[action])?;
    let exchange_ts_us = timestamp(action_day, fields[if symbol == 0 { 3 } else { 2 }], 0)?;
    let source_sequence = unsigned(fields[count - 1])?;
    let last_price = number(fields[last])?;
    let volume = unsigned(fields[volume])?;
    let turnover = number(fields[turnover])?;
    let open_interest = number(fields[oi])?;
    let empty = MdlLevel {
        price: f64::NAN,
        quantity: UNKNOWN_QUANTITY,
    };
    let mut quote = MdlQuote {
        venue,
        symbol: fields[symbol],
        trading_day,
        action_day,
        exchange_ts_us,
        source_sequence,
        last_price,
        volume,
        turnover,
        open_interest,
        bid: [empty; 5],
        ask: [empty; 5],
    };
    for level in 0..5 {
        quote.bid[level].price = number(fields[bid + level * stride])?;
        quote.bid[level].quantity = unsigned(fields[bid + level * stride + 1])?;
        quote.ask[level].price = number(fields[ask + level * stride])?;
        quote.ask[level].quantity = unsigned(fields[ask + level * stride + 1])?;
    }
    Ok(quote)
}
