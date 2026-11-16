//! Select source identities using header-located simple leading CSV columns.
//! Never parse quoted price/qualifier payloads here; Python retains the full CSV parser.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use serde_json::Value;
use std::{
    collections::HashSet,
    fs,
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    plan: PathBuf,
    #[arg(long)]
    patterns: PathBuf,
    #[arg(long)]
    header_json: Option<PathBuf>,
}

#[derive(Clone, Copy)]
struct Leading {
    ric: usize,
    stamp: usize,
    kind: usize,
}
impl Leading {
    fn header(fields: &[String]) -> Result<Self> {
        let at = |name: &str| {
            fields
                .iter()
                .position(|f| f.trim_start_matches('\u{feff}') == name)
                .with_context(|| format!("missing CSV identity header {name}"))
        };
        let value = Self {
            ric: at("#RIC")?,
            stamp: at("Date-Time")?,
            kind: at("Type")?,
        };
        ensure!(
            value.ric < 8 && value.stamp < 8 && value.kind < 8,
            "CSV identity columns outside simple leading fields"
        );
        Ok(value)
    }
    fn inferred(line: &[u8]) -> Self {
        let fields: Vec<_> = line.splitn(7, |b| *b == b',').collect();
        if fields.get(3).is_some_and(|f| {
            f.len() > 10 && f[0..4].iter().all(u8::is_ascii_digit) && f[4] == b'-' && f[10] == b'T'
        }) {
            Self {
                ric: 0,
                stamp: 3,
                kind: 5,
            }
        } else {
            Self {
                ric: 0,
                stamp: 2,
                kind: 4,
            }
        }
    }
}

fn selected_at(
    line: &[u8],
    rics: &HashSet<Vec<u8>>,
    times: &HashSet<Vec<u8>>,
    leading: Leading,
) -> bool {
    let fields: Vec<_> = line
        .splitn(leading.ric.max(leading.stamp).max(leading.kind) + 2, |b| {
            *b == b','
        })
        .collect();
    let ric = fields.get(leading.ric).copied().unwrap_or_default();
    if !rics.contains(ric) {
        return false;
    }
    let stamp = fields.get(leading.stamp).copied().unwrap_or_default();
    let kind = fields.get(leading.kind).copied().unwrap_or_default();
    kind == b"Correction" || (kind == b"Trade" && times.contains(stamp))
}

#[cfg(test)]
fn selected(line: &[u8], rics: &HashSet<Vec<u8>>, times: &HashSet<Vec<u8>>) -> bool {
    selected_at(line, rics, times, Leading::inferred(line))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let plan: Value = serde_json::from_slice(&fs::read(args.plan)?)?;
    let rics: HashSet<Vec<u8>> = plan["rics"]
        .as_array()
        .context("rics")?
        .iter()
        .map(|v| v.as_str().unwrap().as_bytes().to_vec())
        .collect();
    let times: HashSet<Vec<u8>> = fs::read_to_string(args.patterns)?
        .lines()
        .filter(|line| {
            line.len() > 4
                && line.as_bytes()[0..4].iter().all(u8::is_ascii_digit)
                && line.as_bytes()[4] == b'-'
        })
        .map(|s| s.as_bytes().to_vec())
        .collect();
    let mut leading = if let Some(path) = args.header_json {
        let fields: Vec<String> = serde_json::from_slice(&fs::read(path)?)?;
        Some(Leading::header(&fields)?)
    } else {
        None
    };
    let mut input = BufReader::with_capacity(32 * 1024 * 1024, io::stdin().lock());
    let mut output = BufWriter::with_capacity(1024 * 1024, io::stdout().lock());
    let mut line = Vec::with_capacity(4096);
    let mut number = 0u64;
    let mut bytes = 0u64;
    loop {
        line.clear();
        let n = input.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        number += 1;
        bytes += n as u64;
        if line.starts_with(b"#RIC,") || line.starts_with(b"\"#RIC\",") {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(line.as_slice());
            let record = reader.records().next().context("empty header")??;
            leading = Some(Leading::header(
                &record.iter().map(str::to_owned).collect::<Vec<_>>(),
            )?);
            continue;
        }
        let layout = *leading.get_or_insert_with(|| Leading::inferred(&line));
        if selected_at(&line, &rics, &times, layout) {
            write!(output, "{number}:")?;
            output.write_all(&line)?;
        }
        if number % 100_000_000 == 0 {
            eprintln!("source_lines={number} source_bytes={bytes}");
        }
    }
    output.flush()?;
    eprintln!("source_lines={number} source_bytes={bytes} complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filters_quotes_other_rics_and_nonmatching_trade_timestamps() {
        let rics = HashSet::from([b"ESM26".to_vec()]);
        let times = HashSet::from([b"2026-05-01T00:00:00.000000000Z".to_vec()]);
        assert!(selected(
            b"ESM26,Market Price,2026-05-01T00:00:00.000000000Z,-5,Trade,100,",
            &rics,
            &times
        ));
        assert!(selected(
            b"ESM26,Market Price,unselected,-5,Correction,",
            &rics,
            &times
        ));
        assert!(!selected(
            b"ESM26,Market Price,unselected,-5,Trade,",
            &rics,
            &times
        ));
        assert!(!selected(
            b"ESM26,Market Price,2026-05-01T00:00:00.000000000Z,-5,Quote,",
            &rics,
            &times
        ));
        assert!(!selected(
            b"NQM26,Market Price,2026-05-01T00:00:00.000000000Z,-5,Trade,",
            &rics,
            &times
        ));
    }
    #[test]
    fn normalized_daily_alias_column_and_historical_year() {
        let rics = HashSet::from([b"CLN26".to_vec()]);
        let times = HashSet::from([b"2010-06-10T17:11:23.000000000Z".to_vec()]);
        assert!(selected(
            b"CLN26,,Market Price,2010-06-10T17:11:23.000000000Z,-5,Trade,100,",
            &rics,
            &times
        ));
        let header = vec![
            "#RIC",
            "Alias Underlying RIC",
            "Domain",
            "Date-Time",
            "GMT Offset",
            "Type",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        let layout = Leading::header(&header).unwrap();
        assert_eq!((layout.stamp, layout.kind), (3, 5));
        assert!(selected_at(
            b"CLN26,,Market Price,unselected,-5,Correction,",
            &rics,
            &times,
            layout
        ));
    }
}
