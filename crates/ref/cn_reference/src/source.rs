//! OpenCTP transport and HTML parsing (port of `cn_futures_ref/source.py`).
use crate::models::{
    data_error, header_aliases, header_key, normalize_row, DataError, Record, REQUIRED_FIELDS,
};
use anyhow::Result;
use chrono::{NaiveDate, Utc};
use regex::Regex;
use std::collections::{BTreeSet, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

// ---------------------------------------------------------------- HTML

/// Minimal tokenizer with the behaviour of Python's `HTMLParser`
/// (`convert_charrefs=True`) for well-formed table markup: lower-cased tag
/// names, quoted attributes, comments/declarations skipped, script/style
/// bodies treated as raw text.
enum Tok {
    Start(String),
    End(String),
    Text(String),
}

fn unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let end = rest.find(';').filter(|e| *e <= 12);
        let decoded = end.and_then(|e| {
            let name = &rest[1..e];
            let ch = if let Some(num) = name.strip_prefix('#') {
                let code = match num.strip_prefix(['x', 'X']) {
                    Some(h) => u32::from_str_radix(h, 16).ok(),
                    None => num.parse().ok(),
                };
                code.and_then(char::from_u32)
            } else {
                match name {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    "nbsp" => Some('\u{a0}'),
                    _ => None,
                }
            };
            ch.map(|c| (c, e))
        });
        match decoded {
            Some((c, e)) => {
                out.push(c);
                rest = &rest[e + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn tokenize(html: &str) -> Vec<Tok> {
    let b = html.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    let mut text_start = 0;
    let flush = |toks: &mut Vec<Tok>, from: usize, to: usize| {
        if to > from {
            toks.push(Tok::Text(unescape(&html[from..to])));
        }
    };
    while i < b.len() {
        if b[i] != b'<' {
            i += 1;
            continue;
        }
        let rest = &html[i..];
        if rest.starts_with("<!--") {
            flush(&mut toks, text_start, i);
            i = rest.find("-->").map_or(b.len(), |e| i + e + 3);
            text_start = i;
        } else if rest.starts_with("<!") || rest.starts_with("<?") {
            flush(&mut toks, text_start, i);
            i = rest.find('>').map_or(b.len(), |e| i + e + 1);
            text_start = i;
        } else if rest.starts_with("</") && rest.len() > 2 && b[i + 2].is_ascii_alphabetic() {
            flush(&mut toks, text_start, i);
            let end = rest.find('>').map_or(b.len(), |e| i + e + 1);
            let name: String = rest[2..]
                .chars()
                .take_while(|c| !c.is_whitespace() && *c != '>' && *c != '/')
                .collect();
            toks.push(Tok::End(name.to_ascii_lowercase()));
            i = end;
            text_start = i;
        } else if rest.len() > 1 && b[i + 1].is_ascii_alphabetic() {
            flush(&mut toks, text_start, i);
            let name: String = rest[1..]
                .chars()
                .take_while(|c| !c.is_whitespace() && *c != '>' && *c != '/')
                .collect();
            let lname = name.to_ascii_lowercase();
            // Find the closing '>' outside quoted attribute values.
            let mut j = i + 1 + name.len();
            let mut quote: Option<u8> = None;
            while j < b.len() {
                match (quote, b[j]) {
                    (Some(q), c) if c == q => quote = None,
                    (None, b'"') | (None, b'\'') => quote = Some(b[j]),
                    (None, b'>') => break,
                    _ => {}
                }
                j += 1;
            }
            let self_closing = j > 0 && j <= b.len() && j < b.len() && b[j - 1] == b'/';
            toks.push(Tok::Start(lname.clone()));
            if self_closing {
                toks.push(Tok::End(lname.clone()));
            }
            i = (j + 1).min(b.len());
            text_start = i;
            if (lname == "script" || lname == "style") && !self_closing {
                let close = format!("</{lname}");
                let lower = html[i..].to_ascii_lowercase();
                let at = lower.find(&close).map_or(b.len(), |e| i + e);
                if at > i {
                    toks.push(Tok::Text(html[i..at].to_string()));
                }
                i = at;
                text_start = i;
            }
        } else {
            i += 1;
        }
    }
    flush(&mut toks, text_start, b.len());
    toks
}

/// Tables as lists of rows of cell text (`FeeTableParser`).
pub fn parse_tables(html: &str) -> Vec<Vec<Vec<String>>> {
    let mut tables = Vec::new();
    let mut depth = 0u32;
    let mut rows: Option<Vec<Vec<String>>> = None;
    let mut row: Option<Vec<String>> = None;
    let mut cell: Option<String> = None;
    let mut parts: Vec<String> = Vec::new();
    for tok in tokenize(html) {
        match tok {
            Tok::Start(tag) => {
                if tag == "table" {
                    depth += 1;
                    if depth == 1 {
                        rows = Some(Vec::new());
                    }
                } else if depth == 1 && tag == "tr" {
                    row = Some(Vec::new());
                } else if depth == 1 && (tag == "th" || tag == "td") {
                    cell = Some(tag);
                    parts.clear();
                } else if cell.is_some() && tag == "br" {
                    parts.push(" ".to_string());
                }
            }
            Tok::Text(t) => {
                if cell.is_some() {
                    parts.push(t);
                }
            }
            Tok::End(tag) => {
                if depth == 1 && (tag == "th" || tag == "td") && cell.is_some() {
                    let text = parts
                        .concat()
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ");
                    if let Some(r) = row.as_mut() {
                        r.push(text);
                    }
                    cell = None;
                    parts.clear();
                } else if depth == 1 && tag == "tr" {
                    if let (Some(r), Some(all)) = (row.take(), rows.as_mut()) {
                        if r.iter().any(|c| !c.is_empty()) {
                            all.push(r);
                        }
                    }
                } else if tag == "table" && depth > 0 {
                    if depth == 1 {
                        if let Some(all) = rows.clone().filter(|r| !r.is_empty()) {
                            tables.push(all);
                        }
                    }
                    depth -= 1;
                }
            }
        }
    }
    tables
}

pub fn validate_rows(rows: &[Record]) -> Result<()> {
    if rows.is_empty() {
        return data_error("source contained no instrument rows");
    }
    let mut available: HashSet<&str> = HashSet::new();
    for r in rows {
        for (k, v) in &r.f {
            if v.is_some() {
                available.insert(k);
            }
        }
    }
    let mut missing: Vec<&str> = REQUIRED_FIELDS
        .iter()
        .copied()
        .filter(|f| !available.contains(f))
        .collect();
    missing.sort_unstable();
    if !missing.is_empty() {
        return data_error(format!(
            "source is missing required fields: {}",
            missing.join(", ")
        ));
    }
    let mut seen = HashSet::new();
    let mut dups = Vec::new();
    for r in rows {
        let venue = r.get("venue").unwrap_or("None").to_string();
        let key = (
            venue.clone(),
            r.get("instrument_id").unwrap_or("None").to_uppercase(),
        );
        if !seen.insert(key.clone()) {
            dups.push(format!("{}.{}", key.0, key.1));
        }
    }
    if !dups.is_empty() {
        dups.truncate(5);
        return data_error(format!(
            "duplicate instruments in source: {}",
            dups.join(", ")
        ));
    }
    Ok(())
}

pub fn parse_html(text: &str) -> Result<Vec<Record>> {
    let aliases = header_aliases();
    let required: BTreeSet<&str> = REQUIRED_FIELDS.iter().copied().collect();
    let tables = parse_tables(text);
    // (score, rows, table index, header index); first maximum wins.
    let mut best: Option<(usize, usize, usize, usize)> = None;
    for (ti, table) in tables.iter().enumerate() {
        for (hi, header) in table.iter().take(4).enumerate() {
            let mapped: BTreeSet<&str> = header
                .iter()
                .filter_map(|v| aliases.get(&header_key(v)).copied())
                .collect();
            let score = mapped.intersection(&required).count();
            if score >= 7 && best.is_none_or(|(s, n, _, _)| (score, table.len()) > (s, n)) {
                best = Some((score, table.len(), ti, hi));
            }
        }
    }
    let Some((_, _, ti, hi)) = best else {
        return data_error("no OpenCTP fee table found in HTML");
    };
    let table = &tables[ti];
    let header = &table[hi];
    let mut records = Vec::new();
    for row in &table[hi + 1..] {
        if row.len() < 5 {
            continue;
        }
        let mut row = row.clone();
        while row.len() < header.len() {
            row.push(String::new());
        }
        let n = header.len().min(row.len());
        let raw: Vec<(String, String)> = (0..n)
            .map(|i| (header[i].clone(), row[i].clone()))
            .collect();
        let rec = normalize_row(&raw);
        if rec.get("venue").is_some() && rec.get("instrument_id").is_some() {
            records.push(rec);
        }
    }
    validate_rows(&records)?;
    Ok(records)
}

// ----------------------------------------------------------- decoding

fn lookup_encoding(name: &str) -> Option<&'static encoding_rs::Encoding> {
    encoding_rs::Encoding::for_label(name.as_bytes())
}

/// `source.decode`: declared charsets first, then utf-8-sig, utf-8, gb18030.
pub fn decode(content: &[u8], content_type: &str) -> Result<String> {
    static CT: OnceLock<Regex> = OnceLock::new();
    static META: OnceLock<regex::bytes::Regex> = OnceLock::new();
    let ct = CT.get_or_init(|| Regex::new(r"(?i)charset=([A-Za-z0-9_-]+)").unwrap());
    let meta = META.get_or_init(|| {
        regex::bytes::Regex::new(r#"(?i)charset\s*=\s*["']?([A-Za-z0-9_-]+)"#).unwrap()
    });
    let mut encodings: Vec<String> = Vec::new();
    if let Some(m) = ct.captures(content_type) {
        encodings.push(m[1].to_string());
    }
    if let Some(m) = meta.captures(&content[..content.len().min(4096)]) {
        encodings.push(String::from_utf8_lossy(&m[1]).to_string());
    }
    encodings.extend(["utf-8-sig", "utf-8", "gb18030"].map(String::from));
    let mut tried = HashSet::new();
    for name in encodings {
        let lower = name.to_lowercase();
        if !tried.insert(lower.clone()) {
            continue;
        }
        let decoded = if lower == "utf-8-sig" || lower == "utf_8_sig" {
            let body = content.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(content);
            std::str::from_utf8(body).ok().map(String::from)
        } else {
            lookup_encoding(&lower).and_then(|enc| {
                enc.decode_without_bom_handling_and_without_replacement(content)
                    .map(|c| c.into_owned())
            })
        };
        if let Some(text) = decoded {
            return Ok(text);
        }
    }
    data_error("could not decode source as UTF-8 or GB18030")
}

// -------------------------------------------------------------- fetch

fn is_openctp_host(host: &str) -> bool {
    matches!(host, "openctp.cn" | "www.openctp.cn" | "dict.openctp.cn")
}

/// `source.fetch`: returns (content, content-type, final URL). https URLs of
/// the OpenCTP hosts also try plain http; `dict.openctp.cn` tries a direct
/// connection before the environment proxy.
pub async fn fetch(
    url: &str,
    timeout: u64,
    retries: u32,
    user_agent: &str,
    accept: Option<&str>,
) -> Result<(Vec<u8>, String, String)> {
    let parsed = reqwest::Url::parse(url).map_err(|e| DataError(format!("bad source URL: {e}")))?;
    let mut candidates = vec![url.to_string()];
    if parsed.scheme().eq_ignore_ascii_case("https")
        && is_openctp_host(&parsed.host_str().unwrap_or("").to_lowercase())
    {
        let mut http = parsed.clone();
        let _ = http.set_scheme("http");
        candidates.push(http.to_string());
    }
    let mut errors = Vec::new();
    for candidate in candidates {
        let host = reqwest::Url::parse(&candidate)
            .ok()
            .and_then(|u| u.host_str().map(str::to_lowercase))
            .unwrap_or_default();
        let transports: Vec<(&str, bool)> = if host == "dict.openctp.cn" {
            vec![("direct", true), ("environment", false)]
        } else {
            vec![("environment", false)]
        };
        for (name, direct) in transports {
            let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(timeout));
            if direct {
                builder = builder.no_proxy();
            }
            let client = builder.build()?;
            for attempt in 0..=retries {
                let result = async {
                    let resp = client
                        .get(&candidate)
                        .header(
                            "Accept",
                            accept.unwrap_or("text/html,application/xhtml+xml,*/*;q=0.8"),
                        )
                        .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.5")
                        .header("User-Agent", user_agent)
                        .send()
                        .await?
                        .error_for_status()?;
                    let content_type = resp
                        .headers()
                        .get("content-type")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    let final_url = resp.url().to_string();
                    let bytes = resp.bytes().await?;
                    if bytes.is_empty() {
                        anyhow::bail!("source returned an empty response");
                    }
                    anyhow::Ok((bytes.to_vec(), content_type, final_url))
                }
                .await;
                match result {
                    Ok(v) => return Ok(v),
                    Err(e) => {
                        errors.push(format!("{candidate} via {name}: {e}"));
                        if attempt < retries {
                            tokio::time::sleep(Duration::from_secs(
                                (1u64 << attempt.min(3)).min(8),
                            ))
                            .await;
                        }
                    }
                }
            }
        }
    }
    data_error(format!("failed to fetch source; {}", errors.join("; ")))
}

// -------------------------------------------------------------- dates

pub fn parse_date(value: &str, label: &str) -> Result<NaiveDate> {
    match NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        Ok(d) => Ok(d),
        Err(_) => data_error(format!(
            "{label} must use YYYY-MM-DD: {}",
            crate::json::py_repr(value)
        )),
    }
}

/// `source.source_date`, ISO text.
pub fn source_date(text: &str, records: &[Record], supplied: Option<&str>) -> Result<String> {
    if let Some(s) = supplied.filter(|s| !s.is_empty()) {
        return Ok(parse_date(s, "source date")?.to_string());
    }
    static TITLE: OnceLock<Regex> = OnceLock::new();
    static DATE: OnceLock<Regex> = OnceLock::new();
    let title =
        TITLE.get_or_init(|| Regex::new(r"期货交易手续费与保证金\s*[-—:]?\s*(20\d{6})").unwrap());
    if let Some(m) = title.captures(text) {
        if let Ok(d) = NaiveDate::parse_from_str(&m[1], "%Y%m%d") {
            return Ok(d.to_string());
        }
    }
    let date = DATE.get_or_init(|| Regex::new(r"(20\d{2})[-/]?(\d{2})[-/]?(\d{2})").unwrap());
    let mut order: Vec<String> = Vec::new();
    let mut counts: std::collections::HashMap<String, usize> = Default::default();
    for r in records {
        if let Some(m) = date.captures(r.get("source_updated_at").unwrap_or("")) {
            let d = format!("{}-{}-{}", &m[1], &m[2], &m[3]);
            if !counts.contains_key(&d) {
                order.push(d.clone());
            }
            *counts.entry(d).or_default() += 1;
        }
    }
    // Counter.most_common: highest count, ties in first-seen order.
    let best = order.iter().fold(None::<&String>, |acc, d| match acc {
        Some(a) if counts[a] >= counts[d] => Some(a),
        _ => Some(d),
    });
    Ok(best
        .cloned()
        .unwrap_or_else(|| Utc::now().date_naive().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_follow_html_parser_rules() {
        let html = "<html><body><table><tr><th>a</th><th>b<br>c</th></tr>\
            <tr><td>1&amp;2</td><td> x\n y </td></tr><tr><td></td></tr></table></body></html>";
        let t = parse_tables(html);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0][0], vec!["a", "b c"]);
        assert_eq!(t[0][1], vec!["1&2", "x y"]);
        assert_eq!(t[0].len(), 2, "all-empty rows are dropped");
    }

    #[test]
    fn decode_prefers_declared_charset_then_falls_back() {
        let gbk = encoding_rs::GB18030.encode("手续费").0.into_owned();
        assert_eq!(
            decode(&gbk, "text/html; charset=gb18030").unwrap(),
            "手续费"
        );
        assert_eq!(decode(&gbk, "").unwrap(), "手续费");
        assert_eq!(decode("\u{feff}ok".as_bytes(), "").unwrap(), "ok");
    }

    #[test]
    fn source_date_rules() {
        assert_eq!(
            source_date("期货交易手续费与保证金 - 20261117", &[], None).unwrap(),
            "2026-11-17"
        );
        assert_eq!(
            source_date("", &[], Some("2026-1-5")).unwrap(),
            "2026-01-05"
        );
        assert!(source_date("", &[], Some("nope")).is_err());
    }
}
