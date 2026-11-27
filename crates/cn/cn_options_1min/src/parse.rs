use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::collections::HashMap;
use std::io::BufRead;

const SHANGHAI_UTC_OFFSET_SECS: i64 = 8 * 3600;

/// Supported domestic commodity option exchanges.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Exchange {
    Xdce,
    Xgfe,
    Xsge,
    Xsie,
    Xzce,
}

impl Exchange {
    pub fn dir_name(self) -> &'static str {
        match self {
            Self::Xdce => "xdce",
            Self::Xgfe => "xgfe",
            Self::Xsge => "xsge",
            Self::Xsie => "xsie",
            Self::Xzce => "xzce",
        }
    }

    pub fn from_hist_market(s: &str) -> Option<Self> {
        match s.trim() {
            "XDCE" | "xdce" => Some(Self::Xdce),
            "XGFE" | "xgfe" => Some(Self::Xgfe),
            "XSGE" | "xsge" => Some(Self::Xsge),
            "XSIE" | "xsie" => Some(Self::Xsie),
            "XZCE" | "xzce" => Some(Self::Xzce),
            _ => None,
        }
    }

    pub const ALL: [Exchange; 5] = [Self::Xdce, Self::Xgfe, Self::Xsge, Self::Xsie, Self::Xzce];
}

#[derive(Clone, Copy, Debug)]
pub struct MdlChannel {
    pub exchange: Exchange,
    pub filename: &'static str,
    pub has_unit_col: bool,
}

/// Commodity option 1-minute MDL channels on `10.61.20.11`.
pub const MDL_CHANNELS: [MdlChannel; 5] = [
    MdlChannel {
        exchange: Exchange::Xzce,
        filename: "mdl_13_40_0.csv",
        has_unit_col: false,
    },
    MdlChannel {
        exchange: Exchange::Xdce,
        filename: "mdl_13_42_0.csv",
        has_unit_col: false,
    },
    MdlChannel {
        exchange: Exchange::Xsge,
        filename: "mdl_13_61_0.csv",
        has_unit_col: false,
    },
    MdlChannel {
        exchange: Exchange::Xsie,
        filename: "mdl_13_94_0.csv",
        has_unit_col: true,
    },
    MdlChannel {
        exchange: Exchange::Xgfe,
        filename: "mdl_13_100_0.csv",
        has_unit_col: true,
    },
];

/// One cleaned 9-column option 1-minute bar row matching `baseline_data_1min`.
#[derive(Clone, Debug, PartialEq)]
pub struct OptionBarRow {
    pub contract_id: String,
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub amount: f64,
    pub open_interest: f64,
    /// Monotonic insertion sequence used to keep the latest row on duplicate `(contract_id, ts)`.
    pub seq: u64,
}

/// Partition key `(exchange, PRODUCT_UPPER)`.
pub type PartitionKey = (Exchange, String);
pub type PartitionedBars = HashMap<PartitionKey, Vec<OptionBarRow>>;

/// Extract uppercase product code from leading ASCII letters of a contract ID.
/// Example: `m2609-C-3000` -> `M`, `AP610C8000` -> `AP`, `sc2609C500` -> `SC`.
pub fn extract_product(contract_id: &str) -> Option<String> {
    let prefix_len = contract_id
        .bytes()
        .take_while(|b| b.is_ascii_alphabetic())
        .count();
    if prefix_len == 0 {
        return None;
    }
    Some(contract_id[..prefix_len].to_ascii_uppercase())
}

/// Normalize contract ID so historical zip and daily MDL are byte-identical:
/// - For `XSIE`, historical zip uses uppercase product prefix (`BC2609C100000`, `SC2109C390`),
///   whereas daily MDL and futures `baseline_data_1min` use lowercase (`bc2609C100000`, `sc2109C390`).
///   We lowercase only the leading product letters for `XSIE` (preserving `C`/`P` intact).
/// - Other four exchanges are kept verbatim.
pub fn normalize_contract_id(exchange: Exchange, raw: &str) -> String {
    let trimmed = raw.trim();
    if exchange != Exchange::Xsie {
        return trimmed.to_string();
    }
    let prefix_len = trimmed
        .bytes()
        .take_while(|b| b.is_ascii_alphabetic())
        .count();
    if prefix_len == 0 {
        return trimmed.to_string();
    }
    let mut out = String::with_capacity(trimmed.len());
    out.push_str(&trimmed[..prefix_len].to_ascii_lowercase());
    out.push_str(&trimmed[prefix_len..]);
    out
}

/// Check if a Tonglian right-endpoint `(hour, minute)` belongs to a valid CN commodity
/// continuous trading minute.
///
/// Tonglian option 1-min `bartime`/`BarTime` is the bar right-endpoint:
/// - `09:00` and `21:00` are call-auction bars -> excluded (matching `baseline_data_1min`).
/// - Valid continuous right-endpoints:
///   - `09:01..=10:15` (morning 1, left `09:00..=10:14`)
///   - `10:31..=11:30` (morning 2, left `10:30..=11:29`)
///   - `13:31..=15:00` (afternoon, left `13:30..=14:59`)
///   - `21:01..=23:59` (night pre-midnight, left `21:00..=23:58`)
///   - `00:00..=02:30` (night post-midnight, left `23:59..=02:29`)
#[inline]
pub fn is_valid_continuous_bar_end(hour: u32, minute: u32) -> bool {
    let mins = hour * 60 + minute;
    (9 * 60 + 1..=10 * 60 + 15).contains(&mins)
        || (10 * 60 + 31..=11 * 60 + 30).contains(&mins)
        || (13 * 60 + 31..=15 * 60).contains(&mins)
        || (21 * 60 + 1..=23 * 60 + 59).contains(&mins)
        || mins <= 2 * 60 + 30
}

/// Convert Shanghai `(action_date, right_endpoint HH:MM)` to UTC left-endpoint Unix seconds (`-60s`).
#[inline]
pub fn shanghai_bar_end_to_left_ts(action_date: NaiveDate, hour: u32, minute: u32) -> Result<i64> {
    let time = NaiveTime::from_hms_opt(hour, minute, 0)
        .with_context(|| format!("invalid bar clock {hour:02}:{minute:02}"))?;
    let dt = NaiveDateTime::new(action_date, time);
    Ok(dt.and_utc().timestamp() - SHANGHAI_UTC_OFFSET_SECS - 60)
}

#[inline]
fn parse_hh_mm(s: &str) -> Option<(u32, u32)> {
    let b = s.as_bytes();
    if b.len() < 5 || b[2] != b':' {
        return None;
    }
    let h = (b[0].checked_sub(b'0')? as u32) * 10 + (b[1].checked_sub(b'0')? as u32);
    let m = (b[3].checked_sub(b'0')? as u32) * 10 + (b[4].checked_sub(b'0')? as u32);
    if h < 24 && m < 60 {
        Some((h, m))
    } else {
        None
    }
}

#[inline]
fn parse_yyyymmdd(s: &str) -> Option<NaiveDate> {
    let b = s.as_bytes();
    if b.len() == 8 {
        let y: i32 = s[0..4].parse().ok()?;
        let m: u32 = s[4..6].parse().ok()?;
        let d: u32 = s[6..8].parse().ok()?;
        NaiveDate::from_ymd_opt(y, m, d)
    } else if b.len() == 10 && b[4] == b'-' && b[7] == b'-' {
        let y: i32 = s[0..4].parse().ok()?;
        let m: u32 = s[5..7].parse().ok()?;
        let d: u32 = s[8..10].parse().ok()?;
        NaiveDate::from_ymd_opt(y, m, d)
    } else {
        None
    }
}

#[derive(Clone, Debug, Default)]
pub struct ParseStats {
    pub raw_rows: usize,
    pub kept_rows: usize,
    pub skipped_non_continuous: usize,
    pub skipped_other_tradday: usize,
    pub skipped_non_1min_unit: usize,
    pub skipped_unknown_market: usize,
}

/// Stream-parse a historical TSV (`future_opt_priceminYYYYMMDD.txt`) into partitioned bars.
pub fn parse_hist_tsv<R: BufRead>(
    mut reader: R,
    expected_trad_day: NaiveDate,
    out: &mut PartitionedBars,
) -> Result<ParseStats> {
    let mut header_line = String::new();
    let n = reader.read_line(&mut header_line)?;
    if n == 0 {
        bail!("empty historical TSV for {expected_trad_day}");
    }
    let cols: Vec<&str> = header_line
        .trim_end_matches(['\r', '\n'])
        .trim_start_matches('\u{feff}')
        .split('\t')
        .collect();
    let idx = |name: &str| -> Result<usize> {
        cols.iter()
            .position(|c| c.eq_ignore_ascii_case(name))
            .with_context(|| format!("missing column {name:?} in historical TSV header {cols:?}"))
    };
    let i_cday = idx("CLEARINGDAY")?;
    let i_tdate = idx("TDATE")?;
    let i_cid = idx("CONTRACTID")?;
    let i_mkt = idx("MARKET")?;
    let i_bt = idx("bartime")?;
    let i_open = idx("openprice")?;
    let i_high = idx("highprice")?;
    let i_low = idx("lowprice")?;
    let i_close = idx("closeprice")?;
    let i_vol = idx("volume")?;
    let i_val = idx("value")?;
    let i_oi = idx("OPENINTS")?;
    let min_cols = [
        i_cday, i_tdate, i_cid, i_mkt, i_bt, i_open, i_high, i_low, i_close, i_vol, i_val, i_oi,
    ]
    .into_iter()
    .max()
    .unwrap()
        + 1;

    let mut stats = ParseStats::default();
    let mut line = String::new();
    let mut seq: u64 = 0;

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        stats.raw_rows += 1;
        let parts: Vec<&str> = trimmed.split('\t').collect();
        if parts.len() < min_cols {
            bail!(
                "historical TSV row {} has {} columns (need >= {}): {:?}",
                stats.raw_rows,
                parts.len(),
                min_cols,
                trimmed
            );
        }
        let Some(exchange) = Exchange::from_hist_market(parts[i_mkt]) else {
            stats.skipped_unknown_market += 1;
            continue;
        };
        let cday = parse_yyyymmdd(parts[i_cday])
            .with_context(|| format!("bad CLEARINGDAY {:?}", parts[i_cday]))?;
        if cday != expected_trad_day {
            stats.skipped_other_tradday += 1;
            continue;
        }
        let Some((hour, minute)) = parse_hh_mm(parts[i_bt]) else {
            bail!("bad bartime {:?}", parts[i_bt]);
        };
        if !is_valid_continuous_bar_end(hour, minute) {
            stats.skipped_non_continuous += 1;
            continue;
        }
        let tdate = parse_yyyymmdd(parts[i_tdate])
            .with_context(|| format!("bad TDATE {:?}", parts[i_tdate]))?;
        let ts = shanghai_bar_end_to_left_ts(tdate, hour, minute)?;
        let contract_id = normalize_contract_id(exchange, parts[i_cid]);
        let product = extract_product(&contract_id)
            .with_context(|| format!("cannot extract product from {contract_id:?}"))?;

        let row = OptionBarRow {
            contract_id,
            ts,
            open: parts[i_open]
                .parse::<f64>()
                .with_context(|| format!("bad open {:?}", parts[i_open]))?,
            high: parts[i_high]
                .parse::<f64>()
                .with_context(|| format!("bad high {:?}", parts[i_high]))?,
            low: parts[i_low]
                .parse::<f64>()
                .with_context(|| format!("bad low {:?}", parts[i_low]))?,
            close: parts[i_close]
                .parse::<f64>()
                .with_context(|| format!("bad close {:?}", parts[i_close]))?,
            volume: parts[i_vol]
                .parse::<f64>()
                .with_context(|| format!("bad volume {:?}", parts[i_vol]))?,
            amount: parts[i_val]
                .parse::<f64>()
                .with_context(|| format!("bad value {:?}", parts[i_val]))?,
            open_interest: parts[i_oi]
                .parse::<f64>()
                .with_context(|| format!("bad OPENINTS {:?}", parts[i_oi]))?,
            seq,
        };
        seq += 1;
        out.entry((exchange, product)).or_default().push(row);
        stats.kept_rows += 1;
    }

    Ok(stats)
}

/// Stream-parse one daily MDL CSV file, appending only rows whose `TradDay == target_trad_day`
/// (and `Unit == 1` when `channel.has_unit_col`).
///
/// `seq_offset` ensures later calendar-day files always sort after earlier calendar-day files
/// when deduplicating identical `(contract_id, ts)`.
pub fn parse_mdl_csv<R: BufRead>(
    mut reader: R,
    channel: MdlChannel,
    target_trad_day: NaiveDate,
    seq_counter: &mut u64,
    out: &mut PartitionedBars,
) -> Result<ParseStats> {
    let mut header_line = String::new();
    let n = reader.read_line(&mut header_line)?;
    if n == 0 {
        return Ok(ParseStats::default());
    }
    let cols: Vec<&str> = header_line
        .trim_end_matches(['\r', '\n'])
        .trim_start_matches('\u{feff}')
        .split(',')
        .collect();
    let idx = |name: &str| -> Result<usize> {
        cols.iter()
            .position(|c| c.eq_ignore_ascii_case(name))
            .with_context(|| {
                format!(
                    "missing column {name:?} in MDL CSV {} header {cols:?}",
                    channel.filename
                )
            })
    };
    let i_cid = idx("SecurityID")?;
    let i_bt = idx("BarTime")?;
    let i_open = idx("OpenPrice")?;
    let i_close = idx("ClosePrice")?;
    let i_high = idx("HighPrice")?;
    let i_low = idx("LowPrice")?;
    let i_vol = idx("TotalVolume")?;
    let i_val = idx("TotalValue")?;
    let i_oi = cols
        .iter()
        .position(|c| {
            c.eq_ignore_ascii_case("OpenInterest") || c.eq_ignore_ascii_case("TotalLongPosition")
        })
        .with_context(|| {
            format!(
                "missing OpenInterest in {} header {cols:?}",
                channel.filename
            )
        })?;
    let i_tday = idx("TradDay")?;
    let i_aday = idx("ActionDay")?;
    let i_unit = if channel.has_unit_col {
        Some(idx("Unit")?)
    } else {
        cols.iter().position(|c| c.eq_ignore_ascii_case("Unit"))
    };
    let min_cols = [
        i_cid, i_bt, i_open, i_close, i_high, i_low, i_vol, i_val, i_oi, i_tday, i_aday,
    ]
    .into_iter()
    .chain(i_unit)
    .max()
    .unwrap()
        + 1;

    let target_iso = target_trad_day.format("%Y-%m-%d").to_string();
    let target_compact = target_trad_day.format("%Y%m%d").to_string();

    let mut stats = ParseStats::default();
    let mut line = String::new();

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        stats.raw_rows += 1;
        let parts: Vec<&str> = trimmed.split(',').collect();
        if parts.len() < min_cols {
            bail!(
                "MDL CSV {} row {} has {} cols (need >= {}): {:?}",
                channel.filename,
                stats.raw_rows,
                parts.len(),
                min_cols,
                trimmed
            );
        }
        if let Some(u_idx) = i_unit {
            if parts[u_idx] != "1" {
                stats.skipped_non_1min_unit += 1;
                continue;
            }
        }
        let td_raw = parts[i_tday];
        if td_raw != target_iso && td_raw != target_compact {
            stats.skipped_other_tradday += 1;
            continue;
        }
        let Some((hour, minute)) = parse_hh_mm(parts[i_bt]) else {
            bail!("bad BarTime {:?} in {}", parts[i_bt], channel.filename);
        };
        if !is_valid_continuous_bar_end(hour, minute) {
            stats.skipped_non_continuous += 1;
            continue;
        }
        let action_date = parse_yyyymmdd(parts[i_aday]).with_context(|| {
            format!("bad ActionDay {:?} in {}", parts[i_aday], channel.filename)
        })?;
        let ts = shanghai_bar_end_to_left_ts(action_date, hour, minute)?;
        let contract_id = normalize_contract_id(channel.exchange, parts[i_cid]);
        let product = extract_product(&contract_id)
            .with_context(|| format!("cannot extract product from {contract_id:?}"))?;

        let row = OptionBarRow {
            contract_id,
            ts,
            open: parts[i_open]
                .parse::<f64>()
                .with_context(|| format!("bad OpenPrice {:?}", parts[i_open]))?,
            high: parts[i_high]
                .parse::<f64>()
                .with_context(|| format!("bad HighPrice {:?}", parts[i_high]))?,
            low: parts[i_low]
                .parse::<f64>()
                .with_context(|| format!("bad LowPrice {:?}", parts[i_low]))?,
            close: parts[i_close]
                .parse::<f64>()
                .with_context(|| format!("bad ClosePrice {:?}", parts[i_close]))?,
            volume: parts[i_vol]
                .parse::<f64>()
                .with_context(|| format!("bad TotalVolume {:?}", parts[i_vol]))?,
            amount: parts[i_val]
                .parse::<f64>()
                .with_context(|| format!("bad TotalValue {:?}", parts[i_val]))?,
            open_interest: parts[i_oi]
                .parse::<f64>()
                .with_context(|| format!("bad OpenInterest {:?}", parts[i_oi]))?,
            seq: *seq_counter,
        };
        *seq_counter += 1;
        out.entry((channel.exchange, product))
            .or_default()
            .push(row);
        stats.kept_rows += 1;
    }

    Ok(stats)
}

/// Sort rows by `(contract_id ASC, ts ASC, seq ASC)` and deduplicate identical `(contract_id, ts)`
/// keeping the **last** (highest `seq`) row. Returns the number of duplicate rows dropped.
pub fn sort_and_dedup_keep_last(rows: &mut Vec<OptionBarRow>) -> usize {
    if rows.len() <= 1 {
        return 0;
    }
    rows.sort_by(|a, b| {
        a.contract_id
            .cmp(&b.contract_id)
            .then_with(|| a.ts.cmp(&b.ts))
            .then_with(|| a.seq.cmp(&b.seq))
    });
    let orig_len = rows.len();
    let mut write = 0usize;
    for read in 0..orig_len {
        if write > 0
            && rows[write - 1].contract_id == rows[read].contract_id
            && rows[write - 1].ts == rows[read].ts
        {
            rows[write - 1] = rows[read].clone();
        } else {
            if write != read {
                rows[write] = rows[read].clone();
            }
            write += 1;
        }
    }
    rows.truncate(write);
    orig_len - write
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_normalize_and_product() {
        assert_eq!(
            normalize_contract_id(Exchange::Xsie, "BC2609C100000"),
            "bc2609C100000"
        );
        assert_eq!(
            normalize_contract_id(Exchange::Xsie, "sc2609P500"),
            "sc2609P500"
        );
        assert_eq!(
            normalize_contract_id(Exchange::Xzce, "AP610C8000"),
            "AP610C8000"
        );
        assert_eq!(
            normalize_contract_id(Exchange::Xdce, "m2609-C-3000"),
            "m2609-C-3000"
        );
        assert_eq!(extract_product("bc2609C100000").as_deref(), Some("BC"));
        assert_eq!(extract_product("AP610C8000").as_deref(), Some("AP"));
        assert_eq!(extract_product("m2609-C-3000").as_deref(), Some("M"));
    }

    #[test]
    fn test_continuous_filter_and_midnight_ts() {
        assert!(!is_valid_continuous_bar_end(9, 0));
        assert!(is_valid_continuous_bar_end(9, 1));
        assert!(is_valid_continuous_bar_end(10, 15));
        assert!(!is_valid_continuous_bar_end(10, 16));
        assert!(!is_valid_continuous_bar_end(10, 30));
        assert!(is_valid_continuous_bar_end(10, 31));
        assert!(is_valid_continuous_bar_end(15, 0));
        assert!(!is_valid_continuous_bar_end(15, 1));
        assert!(!is_valid_continuous_bar_end(21, 0));
        assert!(is_valid_continuous_bar_end(21, 1));
        assert!(is_valid_continuous_bar_end(23, 59));
        assert!(is_valid_continuous_bar_end(0, 0));
        assert!(is_valid_continuous_bar_end(2, 30));
        assert!(!is_valid_continuous_bar_end(2, 31));

        // 2026-07-31 23:59 +08:00 (-60s -> 23:58), 2026-08-01 00:00 +08:00 (-60s -> 23:59), 2026-08-01 00:01 (-60s -> 00:00)
        let d1 = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
        let ts1 = shanghai_bar_end_to_left_ts(d1, 23, 59).unwrap();
        let ts2 = shanghai_bar_end_to_left_ts(d2, 0, 0).unwrap();
        let ts3 = shanghai_bar_end_to_left_ts(d2, 0, 1).unwrap();
        assert_eq!(ts2 - ts1, 60);
        assert_eq!(ts3 - ts2, 60);
        // 2026-07-31 21:01 - 60s = 2026-07-31 21:00 +08:00 = 1785502800 (verified against baseline_data_1min)
        assert_eq!(shanghai_bar_end_to_left_ts(d1, 21, 1).unwrap(), 1785502800);
    }

    #[test]
    fn test_hist_and_mdl_parity_and_dedup() {
        let hist_tsv = "\
CLEARINGDAY\tTDATE\tCONTRACTID\tMARKET\tOffset\tbartime\tcloseprice\topenprice\thighprice\tlowprice\tvolume\tvalue\tvwap\tOPENINTS
20260803\t20260803\tBC2609C100000\tXSIE\t32400000\t09:00\t218.0\t218.0\t218.0\t218.0\t0\t0.0\t218.0\t57
20260803\t20260803\tBC2609C100000\tXSIE\t32460000\t09:01\t184.0\t190.0\t192.0\t180.0\t2\t1840.0\t184.0\t59
";
        let mdl_csv = "\
SecurityID,BarTime,OpenPrice,ClosePrice,HighPrice,LowPrice,TotalVolume,TotalValue,OpenInterest,Unit,TradDay,ActionDay,LocalTime,SeqNo
bc2609C100000,09:00:00.000,218.0,218.0,218.0,218.0,0,0.0,57,1,2026-08-03,2026-08-03,09:00:01.000,1
bc2609C100000,09:01:00.000,190.0,184.0,192.0,180.0,2,1840.0,58,1,2026-08-03,2026-08-03,09:01:01.000,2
bc2609C100000,09:01:00.000,190.0,184.0,192.0,180.0,2,1840.0,59,1,2026-08-03,2026-08-03,09:01:02.000,3
bc2609C100000,09:01:00.000,190.0,184.0,192.0,180.0,2,1840.0,59,3,2026-08-03,2026-08-03,09:01:02.000,4
";
        let td = NaiveDate::from_ymd_opt(2026, 8, 3).unwrap();
        let mut h_map = PartitionedBars::new();
        let hs = parse_hist_tsv(Cursor::new(hist_tsv), td, &mut h_map).unwrap();
        assert_eq!(hs.kept_rows, 1);
        assert_eq!(hs.skipped_non_continuous, 1);

        let mut d_map = PartitionedBars::new();
        let mut seq = 0u64;
        let ch = MDL_CHANNELS
            .iter()
            .find(|c| c.exchange == Exchange::Xsie)
            .unwrap();
        let ds = parse_mdl_csv(Cursor::new(mdl_csv), *ch, td, &mut seq, &mut d_map).unwrap();
        assert_eq!(ds.kept_rows, 2);
        assert_eq!(ds.skipped_non_1min_unit, 1);

        let key = (Exchange::Xsie, "BC".to_string());
        let h_rows = h_map.get_mut(&key).unwrap();
        let d_rows = d_map.get_mut(&key).unwrap();
        assert_eq!(sort_and_dedup_keep_last(h_rows), 0);
        assert_eq!(sort_and_dedup_keep_last(d_rows), 1);
        assert_eq!(h_rows.len(), 1);
        assert_eq!(d_rows.len(), 1);
        let a = &h_rows[0];
        let b = &d_rows[0];
        assert_eq!(a.contract_id, b.contract_id);
        assert_eq!(a.ts, b.ts);
        assert_eq!(a.open, b.open);
        assert_eq!(a.high, b.high);
        assert_eq!(a.low, b.low);
        assert_eq!(a.close, b.close);
        assert_eq!(a.volume, b.volume);
        assert_eq!(a.amount, b.amount);
        assert_eq!(a.open_interest, b.open_interest);
    }
}
