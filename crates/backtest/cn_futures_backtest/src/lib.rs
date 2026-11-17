//! Deterministic CN futures execution and maker-maker calendar-roll research.
pub mod config;
pub mod engine;
pub mod input;
pub mod market_api;
pub mod metadata;
pub mod output;
pub mod replay;
pub mod runner;
pub mod store;

pub use config::Config;

pub fn product(contract: &str) -> String {
    contract
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase()
}

pub fn timestamp(text: &str) -> anyhow::Result<i64> {
    use chrono::{DateTime, NaiveDateTime, TimeZone};
    if let Ok(t) = DateTime::parse_from_rfc3339(text) {
        return Ok(t.timestamp_micros());
    }
    let t = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")?;
    Ok(chrono_tz::Asia::Shanghai
        .from_local_datetime(&t)
        .single()
        .ok_or_else(|| anyhow::anyhow!("ambiguous Shanghai time"))?
        .timestamp_micros())
}

pub fn day_number(text: &str) -> anyhow::Result<u32> {
    mdl_replay::day::parse(text)
}

pub fn sha256(path: &std::path::Path) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = [0; 65536];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}
