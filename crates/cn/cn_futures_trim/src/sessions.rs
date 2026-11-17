//! Read and apply one immutable continuous-session reference snapshot.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{NaiveTime, Timelike};
use reqwest::blocking::Client;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

pub const DEFAULT_REFERENCE_API: &str = "http://127.0.0.1:8765/api/v1/trading-sessions";
const SHANGHAI_UTC_OFFSET_SECONDS: i64 = 8 * 60 * 60;
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionDataState {
    pub as_of: String,
    pub latest_start_ts: String,
    pub latest_update_ts: String,
    pub session_count: usize,
    pub product_count: usize,
    pub venue_count: usize,
    pub version_count: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionRecord {
    pub venue: String,
    pub product_id: String,
    pub segment_no: u32,
    pub time_begin: String,
    pub time_end: String,
    pub crosses_midnight: bool,
    pub is_active: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SessionApiResponse {
    #[serde(rename = "dataState")]
    data_state: SessionDataState,
    timezone: String,
    sessions: Vec<SessionRecord>,
}

#[derive(Clone, Debug)]
struct SessionWindow {
    begin_sec: u32,
    end_sec: u32,
    crosses_midnight: bool,
}

#[derive(Clone, Debug)]
pub struct ContinuousSessions {
    windows: HashMap<(String, String), Vec<SessionWindow>>,
    state: SessionDataState,
    identity: Value,
    provenance: Value,
}

pub struct SessionMatcher<'a> {
    windows: &'a [SessionWindow],
}

fn parse_clock(value: &str, field: &str) -> Result<u32> {
    let clock = NaiveTime::parse_from_str(value, "%H:%M:%S")
        .with_context(|| format!("continuous-session {field} is not HH:MM:SS: {value:?}"))?;
    Ok(clock.num_seconds_from_midnight())
}

fn key(venue: &str, product: &str) -> (String, String) {
    (
        venue.trim().to_ascii_uppercase(),
        product.trim().to_ascii_uppercase(),
    )
}

impl ContinuousSessions {
    pub fn fetch(api_url: &str, as_of: Option<&str>) -> Result<Self> {
        let mut url = Url::parse(api_url)
            .with_context(|| format!("parse continuous-session API URL {api_url:?}"))?;
        if let Some(as_of) = as_of {
            url.query_pairs_mut().append_pair("asOf", as_of);
        }
        let body = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("build continuous-session API client")?
            .get(url.clone())
            .send()
            .with_context(|| format!("fetch continuous-session snapshot from {url}"))?
            .error_for_status()
            .with_context(|| format!("continuous-session API returned failure for {url}"))?
            .text()
            .context("read continuous-session API response")?;
        Self::parse(&body)
    }

    /// Builds the snapshot from the body of `/api/v1/trading-sessions`.
    pub fn parse(body: &str) -> Result<Self> {
        let provenance: Value =
            serde_json::from_str(body).context("continuous-session API response is not JSON")?;
        let response: SessionApiResponse = serde_json::from_value(provenance.clone())
            .context("continuous-session API response does not match its contract")?;
        Self::from_response(response, provenance)
    }

    fn from_response(response: SessionApiResponse, provenance: Value) -> Result<Self> {
        if response.timezone != "Asia/Shanghai" {
            bail!(
                "continuous-session API timezone must be Asia/Shanghai, got {:?}",
                response.timezone
            );
        }
        if response.sessions.is_empty() {
            bail!("continuous-session API returned no active session rows");
        }
        if response.data_state.session_count != response.sessions.len() {
            bail!(
                "continuous-session API dataState.session_count={} differs from returned rows={}",
                response.data_state.session_count,
                response.sessions.len()
            );
        }

        let mut identity_rows = response
            .sessions
            .iter()
            .map(|row| {
                json!({
                    "venue": row.venue,
                    "product_id": row.product_id,
                    "segment_no": row.segment_no,
                    "time_begin": row.time_begin,
                    "time_end": row.time_end,
                    "crosses_midnight": row.crosses_midnight,
                    "is_active": row.is_active,
                })
            })
            .collect::<Vec<_>>();
        identity_rows.sort_by_key(|row| row.to_string());
        let identity = json!({
            "as_of": response.data_state.as_of,
            "timezone": response.timezone,
            "sessions": identity_rows,
        });

        let mut windows: HashMap<(String, String), Vec<(u32, SessionWindow)>> = HashMap::new();
        for row in response.sessions {
            if !row.is_active {
                bail!(
                    "continuous-session API returned inactive row {}.{} segment {}",
                    row.venue,
                    row.product_id,
                    row.segment_no
                );
            }
            let begin_sec = parse_clock(&row.time_begin, "time_begin")?;
            let end_sec = parse_clock(&row.time_end, "time_end")?;
            if begin_sec == end_sec {
                bail!(
                    "continuous-session row {}.{} segment {} has equal endpoints",
                    row.venue,
                    row.product_id,
                    row.segment_no
                );
            }
            let crosses_midnight = end_sec < begin_sec;
            if row.crosses_midnight != crosses_midnight {
                bail!(
                    "continuous-session row {}.{} segment {} has crosses_midnight={} but endpoints imply {}",
                    row.venue,
                    row.product_id,
                    row.segment_no,
                    row.crosses_midnight,
                    crosses_midnight
                );
            }
            let entry = windows.entry(key(&row.venue, &row.product_id)).or_default();
            if entry.iter().any(|(segment, _)| *segment == row.segment_no) {
                bail!(
                    "continuous-session API has duplicate row {}.{} segment {}",
                    row.venue,
                    row.product_id,
                    row.segment_no
                );
            }
            entry.push((
                row.segment_no,
                SessionWindow {
                    begin_sec,
                    end_sec,
                    crosses_midnight,
                },
            ));
        }
        let windows = windows
            .into_iter()
            .map(|(session_key, mut rows)| {
                rows.sort_by_key(|(segment_no, _)| *segment_no);
                (
                    session_key,
                    rows.into_iter().map(|(_, window)| window).collect(),
                )
            })
            .collect();
        Ok(Self {
            windows,
            state: response.data_state,
            identity,
            provenance,
        })
    }

    pub fn state(&self) -> &SessionDataState {
        &self.state
    }

    pub fn provenance(&self) -> &Value {
        &self.provenance
    }

    pub fn identity(&self) -> &Value {
        &self.identity
    }

    pub fn require_product(&self, venue: &str, product: &str) -> Result<()> {
        self.matcher(venue, product).map(|_| ())
    }

    pub fn matcher(&self, venue: &str, product: &str) -> Result<SessionMatcher<'_>> {
        let windows = self.windows.get(&key(venue, product)).ok_or_else(|| {
            anyhow!(
                "continuous-session snapshot {} has no active rows for {}.{}",
                self.state.as_of,
                venue,
                product
            )
        })?;
        Ok(SessionMatcher { windows })
    }
}

impl SessionMatcher<'_> {
    /// The product's active continuous windows in segment order as
    /// `(begin_sec, end_sec)`: Asia/Shanghai seconds since midnight, the end
    /// exclusive; `end_sec <= begin_sec` means the window runs past midnight.
    pub fn windows(&self) -> Vec<(u32, u32)> {
        self.windows
            .iter()
            .map(|w| (w.begin_sec, w.end_sec))
            .collect()
    }

    /// If `ts_sec` lies within `slack_sec` of the end of a continuous window
    /// whose following break is at least `min_break_sec` long, returns that
    /// window end (epoch seconds). Used to prove that no more rows can belong
    /// to the segment ending there; a tea break (shorter) never qualifies.
    pub fn break_end(&self, ts_sec: i64, slack_sec: i64, min_break_sec: i64) -> Option<i64> {
        let local = (ts_sec + SHANGHAI_UTC_OFFSET_SECONDS).rem_euclid(SECONDS_PER_DAY);
        let day_start = ts_sec - local;
        for window in self.windows {
            let end = i64::from(window.end_sec);
            let gap = self
                .windows
                .iter()
                .map(|next| (i64::from(next.begin_sec) - end).rem_euclid(SECONDS_PER_DAY))
                .min()?;
            if gap < min_break_sec {
                continue;
            }
            for shift in [-SECONDS_PER_DAY, 0, SECONDS_PER_DAY] {
                let e = day_start + shift + end;
                if (e - ts_sec).abs() <= slack_sec {
                    return Some(e);
                }
            }
        }
        None
    }

    #[inline]
    pub fn contains_ts(&self, ts_sec: i64) -> bool {
        // Asia/Shanghai is UTC+08:00 throughout this archive. This avoids a
        // per-row timezone conversion on the 21.7B-row backtest archive.
        let local_sec = (ts_sec + SHANGHAI_UTC_OFFSET_SECONDS).rem_euclid(SECONDS_PER_DAY) as u32;
        self.windows.iter().any(|window| {
            if window.crosses_midnight {
                local_sec >= window.begin_sec || local_sec < window.end_sec
            } else {
                local_sec >= window.begin_sec && local_sec < window.end_sec
            }
        })
    }
}

pub fn reference_venue(exchange_dir: &str) -> Result<&'static str> {
    match exchange_dir.trim().to_ascii_lowercase().as_str() {
        "ccfx" => Ok("CFFEX"),
        "xdce" => Ok("DCE"),
        "xgfe" => Ok("GFEX"),
        "xsge" => Ok("SHFE"),
        "xsie" => Ok("INE"),
        "xzce" => Ok("CZCE"),
        other => bail!("unknown CN futures exchange directory {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::Asia::Shanghai;
    use serde_json::json;

    fn response(rows: Vec<Value>) -> SessionApiResponse {
        serde_json::from_value(json!({
            "dataState": {
                "as_of": "2026-09-08",
                "latest_start_ts": "2026-09-08 03:31:17+00",
                "latest_update_ts": "2026-09-08 03:34:57+00",
                "session_count": rows.len(),
                "product_count": 1,
                "venue_count": 1,
                "version_count": rows.len()
            },
            "timezone": "Asia/Shanghai",
            "sessions": rows
        }))
        .unwrap()
    }

    fn ts(hour: u32, minute: u32, second: u32) -> i64 {
        Shanghai
            .with_ymd_and_hms(2026, 9, 7, hour, minute, second)
            .single()
            .unwrap()
            .timestamp()
    }

    #[test]
    fn applies_day_and_cross_midnight_windows_with_exclusive_endpoints() {
        let rows = vec![
            json!({"venue":"CFFEX","product_id":"IF","segment_no":1,"time_begin":"09:30:00","time_end":"11:30:00","crosses_midnight":false,"is_active":true}),
            json!({"venue":"CFFEX","product_id":"IF","segment_no":2,"time_begin":"13:00:00","time_end":"15:00:00","crosses_midnight":false,"is_active":true}),
            json!({"venue":"SHFE","product_id":"ad","segment_no":1,"time_begin":"21:00:00","time_end":"01:00:00","crosses_midnight":true,"is_active":true}),
        ];
        let sessions =
            ContinuousSessions::from_response(response(rows.clone()), json!({"sessions": rows}))
                .unwrap();
        let if_matcher = sessions.matcher("CFFEX", "if").unwrap();
        assert!(if_matcher.contains_ts(ts(9, 30, 0)));
        assert!(!if_matcher.contains_ts(ts(11, 30, 0)));
        assert!(!if_matcher.contains_ts(ts(15, 0, 0)));
        let ad_matcher = sessions.matcher("SHFE", "AD").unwrap();
        assert!(ad_matcher.contains_ts(ts(21, 0, 0)));
        assert!(ad_matcher.contains_ts(ts(0, 59, 59)));
        assert!(!ad_matcher.contains_ts(ts(1, 0, 0)));
    }

    #[test]
    fn rejects_inconsistent_cross_midnight_metadata() {
        let rows = vec![
            json!({"venue":"SHFE","product_id":"ad","segment_no":1,"time_begin":"21:00:00","time_end":"01:00:00","crosses_midnight":false,"is_active":true}),
        ];
        let error =
            ContinuousSessions::from_response(response(rows.clone()), json!({"sessions": rows}))
                .unwrap_err();
        assert!(error.to_string().contains("crosses_midnight"));
    }
}

#[cfg(test)]
mod break_end_tests {
    use super::*;

    fn w(begin: u32, end: u32, crosses: bool) -> SessionWindow {
        SessionWindow {
            begin_sec: begin,
            end_sec: end,
            crosses_midnight: crosses,
        }
    }

    #[test]
    fn windows_are_exposed_as_second_pairs_in_segment_order() {
        let ws = windows();
        let m = SessionMatcher { windows: &ws };
        assert_eq!(
            m.windows(),
            vec![
                (21 * 3600, 3600), // night: end <= begin runs past midnight
                (9 * 3600, 10 * 3600 + 900),
                (10 * 3600 + 1800, 11 * 3600 + 1800),
                (13 * 3600 + 1800, 15 * 3600),
            ]
        );
    }

    // Commodity-like day: night 21:00-01:00, 09:00-10:15, 10:30-11:30, 13:30-15:00.
    fn windows() -> Vec<SessionWindow> {
        vec![
            w(21 * 3600, 3600, true),
            w(9 * 3600, 10 * 3600 + 900, false),
            w(10 * 3600 + 1800, 11 * 3600 + 1800, false),
            w(13 * 3600 + 1800, 15 * 3600, false),
        ]
    }

    fn at(h: i64, m: i64, s: i64) -> i64 {
        // 2026-11-17 Shanghai wall clock as epoch seconds.
        1_794_844_800 + h * 3600 + m * 60 + s
    }

    #[test]
    fn only_breaks_of_at_least_30_minutes_qualify() {
        let ws = windows();
        let m = SessionMatcher { windows: &ws };
        let slack = 180;
        let min_break = 1800;
        // Lunch (11:30 -> 13:30), afternoon close (15:00 -> 21:00), night close.
        assert_eq!(
            m.break_end(at(11, 29, 58), slack, min_break),
            Some(at(11, 30, 0))
        );
        assert_eq!(
            m.break_end(at(11, 30, 2), slack, min_break),
            Some(at(11, 30, 0))
        );
        assert_eq!(
            m.break_end(at(15, 0, 1), slack, min_break),
            Some(at(15, 0, 0))
        );
        assert_eq!(
            m.break_end(at(0, 59, 59), slack, min_break),
            Some(at(1, 0, 0))
        );
        // Tea break 10:15 -> 10:30 is shorter than 30 minutes.
        assert_eq!(m.break_end(at(10, 14, 59), slack, min_break), None);
        // Mid-session or lagging last row is not at an end.
        assert_eq!(m.break_end(at(11, 25, 0), slack, min_break), None);
        assert_eq!(m.break_end(at(14, 0, 0), slack, min_break), None);
    }
}
