//! Convert independently headed daily gzip CSVs into the vetted TAS shard format.
//! No market fields are interpreted or altered; every source gets a distinct part.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use cme_tas_replay::shard::{CsvGroup, TasShardEntry, TasShardManifest, TasShardSource, shard_file_name};
use flate2::read::MultiGzDecoder;
use rayon::prelude::*;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{fs::{self, File}, io::{BufRead, BufReader, BufWriter, Read, Write}, path::{Path, PathBuf}};

#[derive(Parser)]
struct Args {
    #[arg(long)] downloads: PathBuf,
    #[arg(long)] period: String,
    #[arg(long)] output: PathBuf,
    #[arg(long, default_value_t=4)] workers: usize,
}

#[derive(Deserialize)]
struct Downloads { files: Vec<Source> }
#[derive(Deserialize)]
struct Source { source_file: PathBuf, bytes: u64, sha256: String }

struct HashingReader<R> { inner: R, digest: Sha256, bytes: u64 }
impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n=self.inner.read(buf)?;
        self.digest.update(&buf[..n]);self.bytes+=n as u64;
        Ok(n)
    }
}

struct Staged { source: TasShardSource, shard: Option<TasShardEntry>, header: String }

fn trim_eol(bytes: &[u8]) -> &[u8] {
    let mut end=bytes.len();
    while end>0 && matches!(bytes[end-1],b'\n'|b'\r') { end-=1; }
    &bytes[..end]
}

fn group(line: &[u8], ric: usize, time: usize) -> Result<CsvGroup> {
    let mut csv=csv::ReaderBuilder::new().has_headers(false).from_reader(line);
    let record=csv.records().next().context("missing source row")??;
    Ok(CsvGroup { ric: record.get(ric).context("missing RIC")?.to_string(),
                  date_time: record.get(time).context("missing Date-Time")?.to_string() })
}

fn stage(source: &Source, part: u16, output: &Path) -> Result<Staged> {
    ensure!(fs::metadata(&source.source_file)?.len()==source.bytes,"source size changed");
    if source.bytes==0 {
        ensure!(source.sha256==format!("{:x}",Sha256::digest([])),"zero-byte source hash differs");
        return Ok(Staged { source: TasShardSource { file: source.source_file.to_string_lossy().into_owned(),
            original_part: part, compressed_bytes: 0, data_rows: 0, data_bytes: 0,
            shard_count: 0, complete: true }, shard: None, header: String::new() });
    }
    let raw=HashingReader { inner: File::open(&source.source_file)?, digest: Sha256::new(), bytes: 0 };
    let decoder=MultiGzDecoder::new(BufReader::with_capacity(4*1024*1024,raw));
    let mut input=BufReader::with_capacity(4*1024*1024,decoder);
    let mut header=Vec::new();
    if input.read_until(b'\n',&mut header)?==0 {
        let raw=input.into_inner().into_inner().into_inner();
        ensure!(raw.bytes==source.bytes,"empty gzip byte count differs");
        ensure!(format!("{:x}",raw.digest.finalize())==source.sha256,"empty gzip SHA256 changed");
        return Ok(Staged { source: TasShardSource { file: source.source_file.to_string_lossy().into_owned(),
            original_part: part, compressed_bytes: source.bytes, data_rows: 0, data_bytes: 0,
            shard_count: 0, complete: true }, shard: None, header: String::new() });
    }
    let header_text=String::from_utf8(trim_eol(&header).to_vec())?;
    let mut csv=csv::ReaderBuilder::new().has_headers(false).from_reader(header_text.as_bytes());
    let fields=csv.records().next().context("empty header")??;
    ensure!(fields.get(0)==Some("#RIC"),"source does not begin with #RIC");
    let ric=fields.iter().position(|v|v=="#RIC").context("RIC column")?;
    let time=fields.iter().position(|v|v=="Date-Time").context("Date-Time column")?;
    let name=shard_file_name(part,0);
    let path=output.join(&name);
    let encoder=zstd::stream::Encoder::new(BufWriter::with_capacity(4*1024*1024,File::create(&path)?),1)?;
    let mut encoded=BufWriter::with_capacity(8*1024*1024,encoder);
    encoded.write_all(header_text.as_bytes())?;encoded.write_all(b"\n")?;
    let mut line=Vec::new();
    let mut last=Vec::new();
    let mut first=None;
    let mut rows=0_u64;
    let mut bytes=0_u64;
    loop {
        line.clear();
        if input.read_until(b'\n',&mut line)?==0 { break; }
        if trim_eol(&line).is_empty() { continue; }
        if first.is_none() { first=Some(group(&line,ric,time)?); }
        encoded.write_all(&line)?;
        bytes+=line.len() as u64;rows+=1;
        std::mem::swap(&mut last,&mut line);
    }
    let encoder=encoded.into_inner().map_err(|e|anyhow::anyhow!("flush raw CSV staging buffer: {e}"))?;
    let mut output_file=encoder.finish()?;output_file.flush()?;output_file.get_ref().sync_data()?;
    let raw=input.into_inner().into_inner().into_inner();
    ensure!(raw.bytes==source.bytes,"source compressed byte count differs");
    ensure!(format!("{:x}",raw.digest.finalize())==source.sha256,"source SHA256 changed");
    let (shard, count)=if rows>0 {
        (Some(TasShardEntry { file: name, original_part: part, shard_index: 0, rows,
            data_bytes: bytes, compressed_bytes: fs::metadata(&path)?.len(),
            first_group: first.context("first row")?, last_group: group(&last,ric,time)? }),1)
    } else { fs::remove_file(&path)?;(None,0) };
    eprintln!("daily stage part={part} rows={rows} source={}",source.source_file.display());
    Ok(Staged { source: TasShardSource { file: source.source_file.to_string_lossy().into_owned(),
        original_part: part, compressed_bytes: source.bytes, data_rows: rows, data_bytes: bytes,
        shard_count: count, complete: true }, shard, header: header_text })
}

fn main() -> Result<()> {
    let args=Args::parse();
    ensure!(args.workers>0,"workers must be positive");
    ensure!(!args.output.exists(),"output already exists");
    let downloads: Downloads=serde_json::from_reader(File::open(&args.downloads)?)?;
    ensure!(!downloads.files.is_empty() && downloads.files.len()<=65536,"invalid source count");
    let building=args.output.with_extension("building");
    ensure!(!building.exists(),"unfinished staging directory exists");
    fs::create_dir_all(&building)?;
    let pool=rayon::ThreadPoolBuilder::new().num_threads(args.workers).build()?;
    let staged: Vec<Staged>=pool.install(||downloads.files.par_iter().enumerate()
        .map(|(i,s)|stage(s,i as u16,&building)).collect::<Result<_>>())?;
    let header=staged.iter().find(|s|!s.header.is_empty()).map(|s|s.header.clone()).unwrap_or_default();
    ensure!(staged.iter().all(|s|s.header.is_empty() || s.header==header),"daily source headers differ");
    let manifest=TasShardManifest { format_version: 1, period: args.period.clone(), complete: true,
        header, rows_per_shard: u64::MAX, zstd_level: 1,
        sources: staged.iter().map(|s|s.source.clone()).collect(),
        shards: staged.into_iter().filter_map(|s|s.shard).collect() };
    if !manifest.shards.is_empty() { manifest.validate(&args.period,true)?; }
    manifest.write(&building)?;
    fs::rename(&building,&args.output)?;
    eprintln!("daily stage complete sources={} rows={} shards={} empty_sources={}",
        manifest.sources.len(),manifest.sources.iter().map(|s|s.data_rows).sum::<u64>(),manifest.shards.len(),
        manifest.sources.iter().filter(|s|s.data_rows==0).count());
    Ok(())
}
