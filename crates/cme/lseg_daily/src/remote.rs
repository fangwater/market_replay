//! Remote LSEG daily TAS listing and direct (never proxied) downloads.
//!
//! Port of `scripts/lseg/inventory_tas_remote.py` (daily part) and
//! `scripts/lseg/download_tas_daily.py`. The source is on the private LAN:
//! the HTTP client is built with `no_proxy()`, so proxy environment variables
//! are ignored.
use anyhow::{bail, ensure, Context, Result};
use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Columns the replay needs; the daily template has 293 columns.
const REQUIRED: [&str; 6] = ["#RIC", "Date-Time", "Type", "Price", "Volume", "Qualifiers"];
const BLOCK: usize = 4 * 1024 * 1024;

pub fn client() -> Result<reqwest::blocking::Client> {
    Ok(reqwest::blocking::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(3600))
        .build()?)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listed {
    pub product: String,
    /// YYYY-MM-DD from the file name (= UTC day of its records).
    pub date: String,
    pub name: String,
    pub url: String,
    /// Directory-listing size text, e.g. `246M`, `1810`, `0`.
    pub listing_size: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fingerprint {
    pub bytes: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

fn get_text(client: &reqwest::blocking::Client, url: &str) -> Result<String> {
    let r = client
        .get(url)
        .timeout(Duration::from_secs(60))
        .send()
        .with_context(|| format!("GET {url}"))?;
    ensure!(r.status().is_success(), "GET {url}: {}", r.status());
    Ok(r.text()?)
}

/// `(href, rest of the line after </a>)` for every link except `../`.
pub fn links(html: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in html.lines() {
        let Some(start) = line.find("<a href=\"") else {
            continue;
        };
        let rest = &line[start + 9..];
        let Some(end) = rest.find('"') else {
            continue;
        };
        let href = &rest[..end];
        if href == "../" || href.is_empty() {
            continue;
        }
        let tail = rest
            .find("</a>")
            .map(|i| rest[i + 4..].to_string())
            .unwrap_or_default();
        out.push((percent_decode(href), tail));
    }
    out
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `<product>_<YYYY-MM-DD>_tick.csv.gz` -> date; `.part` and other names -> None.
pub fn formal_date(product: &str, name: &str) -> Option<String> {
    let rest = name.strip_prefix(product)?.strip_prefix('_')?;
    let date = rest.strip_suffix("_tick.csv.gz")?;
    let ok = date.len() == 10
        && date.bytes().enumerate().all(|(i, c)| {
            if i == 4 || i == 7 {
                c == b'-'
            } else {
                c.is_ascii_digit()
            }
        });
    (ok && chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok()).then(|| date.to_string())
}

pub struct Inventory {
    pub products: Vec<String>,
    pub files: Vec<Listed>,
    pub partials: usize,
}

/// Lists `<root>/` and every `<root>/<product>/` (sequential, small pages).
pub fn inventory(
    client: &reqwest::blocking::Client,
    root: &str,
    only: &[String],
) -> Result<Inventory> {
    let root = if root.ends_with('/') {
        root.to_string()
    } else {
        format!("{root}/")
    };
    let mut products: Vec<String> = links(&get_text(client, &root)?)
        .into_iter()
        .filter_map(|(h, _)| h.strip_suffix('/').map(str::to_string))
        .filter(|p| only.is_empty() || only.contains(p))
        .collect();
    products.sort();
    let mut files = Vec::new();
    let mut partials = 0;
    for p in &products {
        let base = format!("{root}{p}/");
        for (name, tail) in links(&get_text(client, &base)?) {
            if name.ends_with(".part") {
                partials += 1;
                continue;
            }
            let Some(date) = formal_date(p, &name) else {
                continue;
            };
            files.push(Listed {
                product: p.clone(),
                date,
                url: format!("{base}{name}"),
                name,
                listing_size: tail.split_whitespace().last().unwrap_or("").to_string(),
            });
        }
    }
    files.sort_by(|a, b| (&a.product, &a.date).cmp(&(&b.product, &b.date)));
    Ok(Inventory {
        products,
        files,
        partials,
    })
}

pub fn head(client: &reqwest::blocking::Client, url: &str) -> Result<Fingerprint> {
    let r = client
        .head(url)
        .timeout(Duration::from_secs(60))
        .send()
        .with_context(|| format!("HEAD {url}"))?;
    ensure!(r.status().is_success(), "HEAD {url}: {}", r.status());
    let h = r.headers();
    let text = |k: &str| h.get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
    Ok(Fingerprint {
        bytes: text("content-length")
            .context("HEAD without Content-Length")?
            .parse()?,
        etag: text("etag"),
        last_modified: text("last-modified"),
    })
}

/// Sidecar `<file>.download.json`, same fields as the Python downloader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sidecar {
    pub status: String,
    pub product: String,
    pub date: String,
    pub source_url: String,
    pub source_file: PathBuf,
    pub remote: Fingerprint,
    pub bytes: u64,
    pub sha256: String,
    pub columns: usize,
    pub header_sha256: String,
    pub full_gzip_crc_verified: bool,
    #[serde(default)]
    pub empty_gzip_payload: bool,
    #[serde(default)]
    pub source_zero_byte_placeholder: bool,
    pub completed_at: String,
}

pub fn sidecar_path(file: &Path) -> PathBuf {
    let mut s = file.as_os_str().to_owned();
    s.push(".download.json");
    PathBuf::from(s)
}

pub fn read_sidecar(file: &Path) -> Result<Option<Sidecar>> {
    let p = sidecar_path(file);
    if !p.exists() {
        return Ok(None);
    }
    Ok(Some(
        serde_json::from_slice(&fs::read(&p)?).with_context(|| format!("parse {p:?}"))?,
    ))
}

/// An already-downloaded copy is reused only with its sidecar and an
/// unchanged remote fingerprint; anything else is preserved and reported.
pub fn check_existing(target: &Path, remote: &Fingerprint) -> Result<Option<Sidecar>> {
    if !target.exists() {
        return Ok(None);
    }
    let Some(info) = read_sidecar(target)? else {
        bail!("unrecorded pre-existing source file; preserved: {target:?}");
    };
    ensure!(
        &info.remote == remote && fs::metadata(target)?.len() == remote.bytes,
        "existing source or remote changed; preserved: {target:?} (local {:?}, remote {:?})",
        info.remote,
        remote
    );
    Ok(Some(info))
}

/// Reads only the first CSV line; full gzip CRC is checked by tas_daily_stage.
fn header(path: &Path) -> Result<Option<Vec<String>>> {
    let mut r = BufReader::new(MultiGzDecoder::new(File::open(path)?));
    let mut line = Vec::new();
    if r.read_until(b'\n', &mut line)? == 0 {
        return Ok(None);
    }
    let line = line.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&line);
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(line);
    let rec = csv.records().next().context("empty header")??;
    Ok(Some(rec.iter().map(str::to_string).collect()))
}

fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Downloads `url` into `dir/<name>`: streamed, sha256, size + If-Match +
/// re-HEAD check, header check, sidecar first, then a no-overwrite hard link.
pub fn download(
    client: &reqwest::blocking::Client,
    item: &Listed,
    remote: &Fingerprint,
    dir: &Path,
) -> Result<Sidecar> {
    fs::create_dir_all(dir)?;
    let target = dir.join(&item.name);
    if let Some(info) = check_existing(&target, remote)? {
        return Ok(info);
    }
    ensure!(remote.bytes > 0, "zero-byte placeholder is not downloaded");
    let partial = dir.join(format!(".{}.{}.partial", item.name, std::process::id()));
    let mut req = client.get(&item.url);
    if let Some(etag) = &remote.etag {
        req = req.header("If-Match", etag);
    }
    let mut resp = req.send().with_context(|| format!("GET {}", item.url))?;
    ensure!(
        resp.status().is_success(),
        "GET {}: {}",
        item.url,
        resp.status()
    );
    ensure!(
        resp.content_length() == Some(remote.bytes),
        "Content-Length {:?} != HEAD {}",
        resp.content_length(),
        remote.bytes
    );
    let mut digest = Sha256::new();
    let mut count = 0u64;
    {
        let mut out = File::options()
            .write(true)
            .create_new(true)
            .open(&partial)
            .with_context(|| format!("create {partial:?}"))?;
        let mut buf = vec![0u8; BLOCK];
        loop {
            let n = resp.read(&mut buf)?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n])?;
            digest.update(&buf[..n]);
            count += n as u64;
        }
        out.sync_all()?;
    }
    let result = (|| -> Result<Sidecar> {
        ensure!(
            count == remote.bytes,
            "{}: got {count} bytes, expected {}",
            item.url,
            remote.bytes
        );
        ensure!(
            &head(client, &item.url)? == remote,
            "remote fingerprint changed during download: {}",
            item.url
        );
        let fields = header(&partial)?;
        if let Some(f) = &fields {
            for name in REQUIRED {
                ensure!(
                    f.iter().any(|c| c == name),
                    "{}: header lacks {name}",
                    item.name
                );
            }
        }
        let cols = fields.clone().unwrap_or_default();
        let info = Sidecar {
            status: if fields.is_none() {
                "download_complete_empty_gzip_verified".into()
            } else {
                "download_complete_header_verified".into()
            },
            product: item.product.clone(),
            date: item.date.clone(),
            source_url: item.url.clone(),
            source_file: target.clone(),
            remote: remote.clone(),
            bytes: count,
            sha256: format!("{:x}", digest.finalize()),
            columns: cols.len(),
            header_sha256: format!("{:x}", Sha256::digest(cols.join(",").as_bytes())),
            full_gzip_crc_verified: fields.is_none(),
            empty_gzip_payload: fields.is_none(),
            source_zero_byte_placeholder: false,
            completed_at: chrono::Utc::now().to_rfc3339(),
        };
        // Sidecar first: a crash before the link leaves only a retryable partial.
        let side = sidecar_path(&target);
        let tmp = dir.join(format!(".{}.download.json.partial", item.name));
        fs::write(&tmp, serde_json::to_vec_pretty(&info)?)?;
        File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, &side)?;
        fs::hard_link(&partial, &target)
            .with_context(|| format!("publish {target:?} (no overwrite)"))?;
        fsync_dir(dir)?;
        Ok(info)
    })();
    let _ = fs::remove_file(&partial);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nginx_listing() {
        let html = r#"<html><body><pre><a href="../">../</a>
<a href="ES_2026-11-08_tick.csv.gz">ES_2026-11-08_tick.csv.gz</a>                          12-Nov-2026 03:16      5M
<a href="ES_2026-11-09_tick.csv.gz.part">ES_2026-11-09_tick.csv.gz.part</a>                     12-Nov-2026 03:16      1G
<a href="ES_2026-11-10_tick.csv.gz">ES_2026-11-10_tick.csv.gz</a>                          12-Nov-2026 18:37    246M
<a href="A%26B/">A&amp;B/</a>     12-Nov-2026 18:37    -
</pre></body></html>"#;
        let l = links(html);
        assert_eq!(l.len(), 4);
        assert_eq!(l[0].1.split_whitespace().last(), Some("5M"));
        assert_eq!(l[3].0, "A&B/");
        assert_eq!(formal_date("ES", &l[0].0).as_deref(), Some("2026-11-08"));
        assert_eq!(formal_date("ES", &l[1].0), None);
        assert_eq!(formal_date("E", "ES_2026-11-08_tick.csv.gz"), None);
        assert_eq!(formal_date("ES", "ES_2026-13-08_tick.csv.gz"), None);
    }

    #[test]
    fn existing_file_needs_matching_sidecar() {
        let t = tempfile::tempdir().unwrap();
        let f = t.path().join("X_2026-11-10_tick.csv.gz");
        let remote = Fingerprint {
            bytes: 3,
            etag: Some("\"a\"".into()),
            last_modified: None,
        };
        assert!(check_existing(&f, &remote).unwrap().is_none());
        fs::write(&f, b"abc").unwrap();
        assert!(check_existing(&f, &remote).is_err());
        let info = Sidecar {
            status: "download_complete_header_verified".into(),
            product: "X".into(),
            date: "2026-11-10".into(),
            source_url: "u".into(),
            source_file: f.clone(),
            remote: remote.clone(),
            bytes: 3,
            sha256: "s".into(),
            columns: 0,
            header_sha256: "h".into(),
            full_gzip_crc_verified: false,
            empty_gzip_payload: false,
            source_zero_byte_placeholder: false,
            completed_at: "t".into(),
        };
        fs::write(sidecar_path(&f), serde_json::to_vec(&info).unwrap()).unwrap();
        assert!(check_existing(&f, &remote).unwrap().is_some());
        let changed = Fingerprint {
            etag: Some("\"b\"".into()),
            ..remote
        };
        assert!(check_existing(&f, &changed).is_err());
    }
}
