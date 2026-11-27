use crate::config::{Config, DaySummary, State};
use crate::parse::{
    parse_hist_tsv, parse_mdl_csv, sort_and_dedup_keep_last, Exchange, OptionBarRow,
    PartitionedBars, MDL_CHANNELS,
};
use crate::writer::write_day_parquet;
use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveTime, Utc};
use chrono_tz::Asia::Shanghai;
use rayon::prelude::*;
use reqwest::blocking::Client;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, Cursor};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use zip::ZipArchive;

pub fn build_http_client() -> Result<Client> {
    Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(300))
        .build()
        .context("build HTTP client")
}

fn fetch_text_retry(client: &Client, url: &str) -> Result<String> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match client.get(url).send().and_then(|r| r.error_for_status()) {
            Ok(resp) => return resp.text().with_context(|| format!("read text from {url}")),
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(500 * attempt));
            }
        }
    }
    Err(last_err.unwrap()).with_context(|| format!("GET {url} failed after 3 retries"))
}

fn fetch_bytes_retry(client: &Client, url: &str) -> Result<Vec<u8>> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match client.get(url).send().and_then(|r| r.error_for_status()) {
            Ok(resp) => {
                return resp
                    .bytes()
                    .map(|b| b.to_vec())
                    .with_context(|| format!("read bytes from {url}"))
            }
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(500 * attempt));
            }
        }
    }
    Err(last_err.unwrap()).with_context(|| format!("GET {url} failed after 3 retries"))
}

/// Extract `href="..."` entries from an nginx autoindex HTML page.
fn extract_hrefs(html: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = html;
    while let Some(pos) = rest.find("href=\"") {
        rest = &rest[pos + 6..];
        if let Some(end) = rest.find('"') {
            let link = &rest[..end];
            if !link.starts_with("../") {
                out.push(link.to_string());
            }
            rest = &rest[end + 1..];
        } else {
            break;
        }
    }
    out
}

/// Discover all historical `(TradDay, full_zip_url)` pairs sorted chronologically.
pub fn list_hist_zips(client: &Client, hist_base_url: &str) -> Result<Vec<(NaiveDate, String)>> {
    let base = hist_base_url.trim_end_matches('/');
    let root_html = fetch_text_retry(client, &format!("{base}/"))?;
    let mut month_dirs: Vec<String> = extract_hrefs(&root_html)
        .into_iter()
        .filter(|h| {
            h.starts_with("future_opt_pricemin")
                && h.ends_with('/')
                && h.len() == "future_opt_priceminYYYYMM/".len()
                && h[19..25].bytes().all(|b| b.is_ascii_digit())
        })
        .collect();
    month_dirs.sort();
    month_dirs.dedup();

    let mut all = Vec::new();
    for mdir in month_dirs {
        let m_url = format!("{base}/{mdir}");
        let m_html = fetch_text_retry(client, &m_url)?;
        for href in extract_hrefs(&m_html) {
            if href.starts_with("future_opt_pricemin")
                && href.ends_with(".zip")
                && href.len() == "future_opt_priceminYYYYMMDD.zip".len()
            {
                let dstr = &href[19..27];
                if let Ok(day) = NaiveDate::parse_from_str(dstr, "%Y%m%d") {
                    all.push((day, format!("{m_url}{href}")));
                }
            }
        }
    }
    all.sort_by_key(|(d, _)| *d);
    all.dedup_by_key(|(d, _)| *d);
    Ok(all)
}

/// Discover all daily MDL calendar-date directories (`YYYYMMDD`) sorted chronologically.
pub fn list_daily_dirs(client: &Client, daily_base_url: &str) -> Result<Vec<NaiveDate>> {
    let base = daily_base_url.trim_end_matches('/');
    let html = fetch_text_retry(client, &format!("{base}/"))?;
    let mut days: Vec<NaiveDate> = extract_hrefs(&html)
        .into_iter()
        .filter_map(|h| {
            let s = h.trim_end_matches('/');
            if s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()) {
                NaiveDate::parse_from_str(s, "%Y%m%d").ok()
            } else {
                None
            }
        })
        .collect();
    days.sort();
    days.dedup();
    Ok(days)
}

/// Check if a daily MDL calendar directory has a day-session channel (meaning it is a real
/// trading day, not a Saturday night-only directory).
fn is_trading_day_dir(client: &Client, daily_base_url: &str, cal_day: NaiveDate) -> Result<bool> {
    let base = daily_base_url.trim_end_matches('/');
    let url = format!("{base}/{}/", cal_day.format("%Y%m%d"));
    let html = fetch_text_retry(client, &url)?;
    // Saturday directories only contain mdl_13_61_0 / mdl_13_94_0 (Friday post-midnight night session);
    // every real trading day has mdl_13_40_0 (XZCE), mdl_13_42_0 (XDCE), or mdl_13_100_0 (XGFE).
    Ok(html.contains("mdl_13_40_0.csv")
        || html.contains("mdl_13_42_0.csv")
        || html.contains("mdl_13_100_0.csv"))
}

/// Load one historical day into partitioned bars (in memory).
pub fn load_hist_day(client: &Client, day: NaiveDate, url: &str) -> Result<PartitionedBars> {
    let zip_bytes = fetch_bytes_retry(client, url)?;
    let mut archive =
        ZipArchive::new(Cursor::new(zip_bytes)).with_context(|| format!("open zip {url}"))?;
    if archive.is_empty() {
        bail!("empty zip archive {url}");
    }
    let mut out = PartitionedBars::new();
    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        let reader = BufReader::with_capacity(256 * 1024, entry);
        parse_hist_tsv(reader, day, &mut out)?;
    }
    Ok(out)
}

/// Load one daily MDL `TradDay` by streaming its preceding calendar dirs (up to 2 existing dirs
/// strictly before `trad_day`, covering previous evening + Saturday post-midnight night session)
/// plus `trad_day` itself.
pub fn load_daily_trad_day(
    client: &Client,
    daily_base_url: &str,
    all_cal_dirs: &[NaiveDate],
    trad_day: NaiveDate,
) -> Result<PartitionedBars> {
    let base = daily_base_url.trim_end_matches('/');
    // Pick up to 2 existing calendar directories strictly before trad_day, plus trad_day itself.
    let mut src_dirs: Vec<NaiveDate> = all_cal_dirs
        .iter()
        .copied()
        .filter(|d| *d < trad_day)
        .rev()
        .take(2)
        .collect();
    src_dirs.reverse();
    if all_cal_dirs.binary_search(&trad_day).is_ok() {
        src_dirs.push(trad_day);
    }

    let mut out = PartitionedBars::new();
    let mut seq_counter = 0u64;

    for cal_day in src_dirs {
        let dir_str = cal_day.format("%Y%m%d");
        for ch in MDL_CHANNELS {
            let url = format!("{base}/{dir_str}/{}", ch.filename);
            let mut last_err = None;
            let mut handled = false;
            for attempt in 1..=3 {
                match client.get(&url).send() {
                    Ok(resp) => {
                        if resp.status() == reqwest::StatusCode::NOT_FOUND {
                            handled = true;
                            break;
                        }
                        match resp.error_for_status() {
                            Ok(ok_resp) => {
                                let reader = BufReader::with_capacity(256 * 1024, ok_resp);
                                parse_mdl_csv(reader, ch, trad_day, &mut seq_counter, &mut out)?;
                                handled = true;
                                break;
                            }
                            Err(e) => {
                                last_err = Some(anyhow::Error::from(e));
                            }
                        }
                    }
                    Err(e) => {
                        last_err = Some(anyhow::Error::from(e));
                    }
                }
                std::thread::sleep(Duration::from_millis(500 * attempt));
            }
            if !handled {
                return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("failed GET {url}")))
                    .with_context(|| format!("streaming {url} for TradDay {trad_day}"));
            }
        }
    }
    Ok(out)
}

fn publish_day(
    out_root: &std::path::Path,
    day: NaiveDate,
    source_tag: &str,
    mut partitioned: PartitionedBars,
) -> Result<DaySummary> {
    let day_str = day.format("%Y%m%d").to_string();
    let mut files = 0usize;
    let mut total_rows = 0usize;
    let mut dedup_dropped = 0usize;

    let mut keys: Vec<_> = partitioned.keys().cloned().collect();
    keys.sort();

    for key in keys {
        let mut rows = partitioned.remove(&key).unwrap_or_default();
        if rows.is_empty() {
            continue;
        }
        dedup_dropped += sort_and_dedup_keep_last(&mut rows);
        let (exchange, product) = key;
        let target: PathBuf = out_root
            .join(exchange.dir_name())
            .join(&product)
            .join(format!("{day_str}.parquet"));
        write_day_parquet(&target, &rows)?;
        files += 1;
        total_rows += rows.len();
    }

    if files == 0 {
        bail!("no commodity option rows produced for TradDay {day} ({source_tag})");
    }

    Ok(DaySummary {
        source: source_tag.to_string(),
        files,
        rows: total_rows,
        dedup_dropped,
        completed_at_utc: Utc::now().to_rfc3339(),
    })
}

pub fn backfill_hist(
    cfg: &Config,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    workers: usize,
    overwrite: bool,
) -> Result<()> {
    let cutoff = cfg.cutoff_date()?;
    let eff_end = match end {
        Some(e) => e.min(cutoff),
        None => cutoff,
    };
    let client = build_http_client()?;
    let all_zips = list_hist_zips(&client, &cfg.hist_base_url)?;
    let state = Arc::new(Mutex::new(State::load(&cfg.state_file)?));

    let todo: Vec<(NaiveDate, String)> = {
        let st = state.lock().unwrap();
        all_zips
            .into_iter()
            .filter(|(d, _)| {
                if let Some(s) = start {
                    if *d < s {
                        return false;
                    }
                }
                if *d > eff_end {
                    return false;
                }
                overwrite || !st.is_done(*d)
            })
            .collect()
    };

    let total = todo.len();
    eprintln!(
        "[backfill-hist] {} days to process (cutoff={}, workers={})",
        total, cutoff, workers
    );
    if total == 0 {
        return Ok(());
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers.clamp(1, 8))
        .build()?;
    let done_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    pool.install(|| -> Result<()> {
        todo.par_iter().try_for_each(|(day, url)| -> Result<()> {
            let c = build_http_client()?;
            let bars = load_hist_day(&c, *day, url)?;
            let summary = publish_day(&cfg.out_root, *day, "hist_zip", bars)?;
            let idx = done_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            {
                let mut st = state.lock().unwrap();
                st.mark_done(*day, summary.clone());
                if idx.is_multiple_of(20) || idx == total {
                    st.save(&cfg.state_file)?;
                }
            }
            if idx.is_multiple_of(25) || idx == total || idx <= 5 {
                eprintln!(
                    "[backfill-hist] ({}/{}) {} -> {} files, {} rows (dedup={})",
                    idx, total, day, summary.files, summary.rows, summary.dedup_dropped
                );
            }
            Ok(())
        })
    })?;

    state.lock().unwrap().save(&cfg.state_file)?;
    eprintln!("[backfill-hist] completed {} days.", total);
    Ok(())
}

/// Determine the latest calendar date eligible to be treated as a finished TradDay.
fn max_ready_trad_day(ready_after: &str) -> Result<NaiveDate> {
    let now_sh = Utc::now().with_timezone(&Shanghai);
    let today = now_sh.date_naive();
    let clock = NaiveTime::parse_from_str(ready_after, "%H:%M")
        .with_context(|| format!("invalid ready_after {ready_after:?}"))?;
    if now_sh.time() >= clock {
        Ok(today)
    } else {
        Ok(today.pred_opt().unwrap())
    }
}

pub fn backfill_daily(
    cfg: &Config,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    workers: usize,
    overwrite: bool,
) -> Result<Vec<(NaiveDate, DaySummary)>> {
    let cutoff = cfg.cutoff_date()?;
    let ready_max = max_ready_trad_day(&cfg.ready_after)?;
    let eff_end = match end {
        Some(e) => e.min(ready_max),
        None => ready_max,
    };
    let client = build_http_client()?;
    let all_cal_dirs = list_daily_dirs(&client, &cfg.daily_base_url)?;
    let state = Arc::new(Mutex::new(State::load(&cfg.state_file)?));

    // Candidate TradDays strictly after cutoff and <= eff_end
    let candidates: Vec<NaiveDate> = {
        let st = state.lock().unwrap();
        all_cal_dirs
            .iter()
            .copied()
            .filter(|d| {
                if *d <= cutoff || *d > eff_end {
                    return false;
                }
                if let Some(s) = start {
                    if *d < s {
                        return false;
                    }
                }
                overwrite || !st.is_done(*d)
            })
            .collect()
    };

    // Filter out Saturday night-only directories (keep only actual trading days).
    let mut trad_days = Vec::new();
    for d in candidates {
        if is_trading_day_dir(&client, &cfg.daily_base_url, d)? {
            trad_days.push(d);
        }
    }

    let total = trad_days.len();
    eprintln!(
        "[backfill-daily] {} trading days to process (cutoff={}, ready_max={}, workers={})",
        total, cutoff, ready_max, workers
    );
    if total == 0 {
        return Ok(Vec::new());
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers.clamp(1, 8))
        .build()?;
    let results = Arc::new(Mutex::new(Vec::new()));
    let done_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    pool.install(|| -> Result<()> {
        trad_days.par_iter().try_for_each(|day| -> Result<()> {
            let c = build_http_client()?;
            let bars = load_daily_trad_day(&c, &cfg.daily_base_url, &all_cal_dirs, *day)?;
            let summary = publish_day(&cfg.out_root, *day, "daily_mdl", bars)?;
            let idx = done_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            {
                let mut st = state.lock().unwrap();
                st.mark_done(*day, summary.clone());
                st.save(&cfg.state_file)?;
            }
            results.lock().unwrap().push((*day, summary.clone()));
            eprintln!(
                "[backfill-daily] ({}/{}) {} -> {} files, {} rows (dedup={})",
                idx, total, day, summary.files, summary.rows, summary.dedup_dropped
            );
            Ok(())
        })
    })?;

    let mut out = results.lock().unwrap().clone();
    out.sort_by_key(|(d, _)| *d);
    Ok(out)
}

/// Daily scheduled job entry point (`cn-options-1min run --config ...`).
pub fn run_daily(cfg: &Config, workers: usize) -> Result<()> {
    let run_res = backfill_daily(cfg, None, None, workers, false);
    let maybe_bot = cfg.dingtalk_webhook_env.as_deref().and_then(|wh_env| {
        let sec_env = cfg.dingtalk_secret_env.as_deref().unwrap_or("");
        dingtalk::DingTalk::from_env(wh_env, sec_env).ok().flatten()
    });

    match run_res {
        Ok(done) => {
            if !done.is_empty() {
                let total_files: usize = done.iter().map(|(_, s)| s.files).sum();
                let total_rows: usize = done.iter().map(|(_, s)| s.rows).sum();
                let first = done.first().unwrap().0;
                let last = done.last().unwrap().0;
                let body = format!(
                    "[{}] cn-options-1min 日更完成\n交易日：{} .. {}（{} 天）\n写入文件：{} 个\n总行数：{}\n目录：{}",
                    cfg.host_tag,
                    first,
                    last,
                    done.len(),
                    total_files,
                    total_rows,
                    cfg.out_root.display()
                );
                if let Some(bot) = maybe_bot {
                    let _ = bot.send_text(&body);
                }
            }
            Ok(())
        }
        Err(err) => {
            if let Some(bot) = maybe_bot {
                let body = format!("[{}] cn-options-1min 失败\n{err:#}", cfg.host_tag);
                let _ = bot.send_text(&body);
            }
            Err(err)
        }
    }
}

/// Compare historical zip vs daily MDL on a given overlap `TradDay` (e.g. `2026-08-03`).
pub fn verify_overlap(cfg: &Config, day: NaiveDate) -> Result<()> {
    let client = build_http_client()?;
    let hist_url = format!(
        "{}/future_opt_pricemin{}/future_opt_pricemin{}.zip",
        cfg.hist_base_url.trim_end_matches('/'),
        day.format("%Y%m"),
        day.format("%Y%m%d")
    );
    let all_cal_dirs = list_daily_dirs(&client, &cfg.daily_base_url)?;
    eprintln!("[verify-overlap] loading hist zip: {hist_url}");
    let mut hist = load_hist_day(&client, day, &hist_url)?;
    eprintln!("[verify-overlap] loading daily MDL for TradDay {day}...");
    let mut daily = load_daily_trad_day(&client, &cfg.daily_base_url, &all_cal_dirs, day)?;

    let mut per_ex: BTreeMap<Exchange, (usize, usize, usize, usize, usize)> = BTreeMap::new();
    let mut all_keys: Vec<_> = hist.keys().chain(daily.keys()).cloned().collect();
    all_keys.sort();
    all_keys.dedup();

    for key in all_keys {
        let mut h_rows = hist.remove(&key).unwrap_or_default();
        let mut d_rows = daily.remove(&key).unwrap_or_default();
        sort_and_dedup_keep_last(&mut h_rows);
        sort_and_dedup_keep_last(&mut d_rows);

        let h_map: HashMap<(&str, i64), &OptionBarRow> = h_rows
            .iter()
            .map(|r| ((r.contract_id.as_str(), r.ts), r))
            .collect();
        let d_map: HashMap<(&str, i64), &OptionBarRow> = d_rows
            .iter()
            .map(|r| ((r.contract_id.as_str(), r.ts), r))
            .collect();

        let entry = per_ex.entry(key.0).or_default();
        for (k, hr) in &h_map {
            match d_map.get(k) {
                None => entry.1 += 1,
                Some(dr) => {
                    entry.0 += 1;
                    let ohlcv_diff = (hr.open - dr.open).abs() > 1e-6
                        || (hr.high - dr.high).abs() > 1e-6
                        || (hr.low - dr.low).abs() > 1e-6
                        || (hr.close - dr.close).abs() > 1e-6
                        || (hr.volume - dr.volume).abs() > 1e-6
                        || (hr.amount - dr.amount).abs() > 1e-6;
                    let oi_diff = (hr.open_interest - dr.open_interest).abs() > 1e-6;
                    if ohlcv_diff {
                        entry.3 += 1;
                    }
                    if oi_diff {
                        entry.4 += 1;
                    }
                }
            }
        }
        for k in d_map.keys() {
            if !h_map.contains_key(k) {
                entry.2 += 1;
            }
        }
    }

    println!("=== Overlap Verification for TradDay {day} ===");
    println!(
        "{:<6} {:>10} {:>10} {:>10} {:>12} {:>10}",
        "exch", "common", "only_hist", "only_mdl", "ohlcva_diff", "oi_diff"
    );
    for (ex, (common, oh, od, ohlcva_d, oi_d)) in per_ex {
        println!(
            "{:<6} {:>10} {:>10} {:>10} {:>12} {:>10}",
            ex.dir_name(),
            common,
            oh,
            od,
            ohlcva_d,
            oi_d
        );
    }
    Ok(())
}
