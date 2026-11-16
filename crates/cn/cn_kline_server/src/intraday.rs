//! Durable, bounded SN MDL cache. NFS supplies missing prefixes; a matching
//! native snapshot in the current continuous ZMQ epoch proves the live bridge.
use anyhow::{ensure, Context, Result};
use chrono::{Datelike, Duration as Days, NaiveDate, Utc, Weekday};
use chrono_tz::Asia::Shanghai;
use cn_futures_core::mdl;
use cn_market_proto::{
    Exchange, GetIntradayFramesRequest, IntradayCoverage, IntradayFrame, IntradayFrameBatch,
};
use futures_util::TryStreamExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Acquire, Row, SqlitePool,
};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

const PREFIX: &[u8] = b"mdl.22.1.* || ";
const MAX_ROWS: usize = 200_000;
const BATCH: usize = 256;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub directory: PathBuf,
    pub msg_root: PathBuf,
    pub zmq_endpoint: String,
    pub retention_days: u32,
}
#[derive(Clone)]
struct Frame {
    contract: String,
    day: i64,
    action_day: i64,
    cursor: String,
    event_us: i64,
    signature: Vec<u8>,
    payload: Vec<u8>,
    received_us: i64,
}
impl Frame {
    fn parse(payload: &[u8], received_us: i64) -> Result<Option<Self>> {
        let Some(csv) = payload.strip_prefix(PREFIX) else {
            return Ok(None);
        };
        let symbol = csv.split(|c| *c == b',').next().unwrap_or_default();
        if symbol.len() != 6
            || !symbol.starts_with(b"sn")
            || !symbol[2..].iter().all(u8::is_ascii_digit)
        {
            return Ok(None);
        }
        let q = mdl::parse_frame(payload).context("parse SN MDL snapshot")?;
        ensure!(
            q.exchange_ts_us > 0
                && q.trading_day > 0
                && q.action_day > 0
                && q.source_sequence < i64::MAX as u64,
            "MDL snapshot has no native clock/day/sequence"
        );
        let mut hash = Sha256::new();
        hash.update(q.symbol);
        hash.update(q.trading_day.to_be_bytes());
        hash.update(q.action_day.to_be_bytes());
        hash.update(q.exchange_ts_us.to_be_bytes());
        hash.update(q.source_sequence.to_be_bytes());
        hash.update(q.volume.to_be_bytes());
        let float = |v: f64| if v.is_nan() { u64::MAX } else { v.to_bits() }.to_be_bytes();
        for v in [q.last_price, q.turnover, q.open_interest] {
            hash.update(float(v));
        }
        for level in q.bid.iter().chain(q.ask.iter()) {
            hash.update(float(level.price));
            hash.update(level.quantity.to_be_bytes());
        }
        Ok(Some(Self {
            contract: std::str::from_utf8(q.symbol)?.into(),
            day: q.trading_day.into(),
            action_day: q.action_day.into(),
            cursor: format!("{:08}:{:020}", q.action_day, q.source_sequence),
            event_us: q.exchange_ts_us,
            signature: hash.finalize().to_vec(),
            payload: payload.into(),
            received_us,
        }))
    }
}

pub struct Cache {
    cfg: Config,
    pool: SqlitePool,
    readers: Arc<Semaphore>,
    stop: AtomicBool,
    healthy: AtomicBool,
    _lock: File,
}
impl Cache {
    async fn open(cfg: Config) -> Result<Arc<Self>> {
        ensure!(
            (2..=14).contains(&cfg.retention_days),
            "intraday retention must be 2..14 days"
        );
        std::fs::create_dir_all(&cfg.directory)?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(cfg.directory.join(".lock"))?;
        lock.try_lock()
            .context("intraday cache already owned by another server")?;
        let options = SqliteConnectOptions::new()
            .filename(cfg.directory.join("snapshots.sqlite"))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5))
            .pragma("cache_size", "-8192")
            .pragma("wal_autocheckpoint", "1000");
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        for query in [
            "CREATE TABLE IF NOT EXISTS frames(contract TEXT NOT NULL,day INTEGER NOT NULL,cursor TEXT NOT NULL,action_day INTEGER NOT NULL,event_us INTEGER NOT NULL,signature BLOB NOT NULL,payload BLOB NOT NULL,received_us INTEGER NOT NULL,live_epoch INTEGER NOT NULL DEFAULT 0,disk_day INTEGER,PRIMARY KEY(contract,day,cursor)) WITHOUT ROWID",
            "CREATE INDEX IF NOT EXISTS frames_epoch ON frames(live_epoch,event_us)",
            "CREATE TABLE IF NOT EXISTS source_files(day INTEGER PRIMARY KEY,offset INTEGER NOT NULL,published INTEGER NOT NULL,size INTEGER NOT NULL,guard BLOB NOT NULL,error TEXT)",
            "CREATE TABLE IF NOT EXISTS source_state(id INTEGER PRIMARY KEY CHECK(id=1),epoch INTEGER NOT NULL,started_us INTEGER NOT NULL,connected INTEGER NOT NULL,last_live_us INTEGER NOT NULL)",
            "INSERT OR IGNORE INTO source_state VALUES(1,0,0,0,0)",
            "UPDATE source_state SET connected=0 WHERE id=1",
        ] {sqlx::query(query).execute(&pool).await?;}
        Ok(Arc::new(Self {
            cfg,
            pool,
            readers: Arc::new(Semaphore::new(2)),
            stop: AtomicBool::new(false),
            healthy: AtomicBool::new(false),
            _lock: lock,
        }))
    }
    pub async fn start(cfg: Config) -> Result<Arc<Self>> {
        let cache = Self::open(cfg).await?;
        let live = cache.clone();
        let runtime = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name("sn-intraday-zmq".into())
            .spawn(move || {
                while !live.stop.load(Ordering::Acquire) {
                    if let Err(e) = live.capture(&runtime) {
                        eprintln!("intraday ZMQ reconnecting; coverage unavailable: {e:#}");
                    }
                    live.healthy.store(false, Ordering::Release);
                    let _ = runtime.block_on(live.epoch(false));
                    if !live.stop.load(Ordering::Acquire) {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            })?;
        let archive = cache.clone();
        tokio::spawn(async move {
            while !archive.stop.load(Ordering::Acquire) {
                if let Err(e) = archive.import_recent().await {
                    eprintln!("intraday delayed import: {e:#}");
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        Ok(cache)
    }
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.healthy.store(false, Ordering::Release);
    }
    async fn epoch(&self, connected: bool) -> Result<i64> {
        let now = Utc::now().timestamp_micros();
        let row=sqlx::query("UPDATE source_state SET epoch=epoch+1,started_us=?,connected=?,last_live_us=0 WHERE id=1 RETURNING epoch").bind(now).bind(connected).fetch_one(&self.pool).await?;
        self.healthy.store(connected, Ordering::Release);
        Ok(row.try_get("epoch")?)
    }
    async fn store(
        &self,
        frames: &[Frame],
        epoch: Option<i64>,
        disk: Option<i64>,
        cursor: Option<(i64, u64, u64, u64, Vec<u8>)>,
    ) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        for f in frames {
            let previous =
                sqlx::query("SELECT signature FROM frames WHERE contract=? AND day=? AND cursor=?")
                    .bind(&f.contract)
                    .bind(f.day)
                    .bind(&f.cursor)
                    .fetch_optional(&mut *tx)
                    .await?;
            if let Some(previous) = previous {
                ensure!(
                    previous.try_get::<Vec<u8>, _>("signature")? == f.signature,
                    "native source identity changed for {} {} {}",
                    f.contract,
                    f.day,
                    f.cursor
                );
            }
            sqlx::query("INSERT INTO frames(contract,day,cursor,action_day,event_us,signature,payload,received_us,live_epoch,disk_day) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(contract,day,cursor) DO UPDATE SET live_epoch=CASE WHEN excluded.live_epoch>0 THEN excluded.live_epoch ELSE frames.live_epoch END,received_us=CASE WHEN excluded.live_epoch>0 THEN excluded.received_us ELSE frames.received_us END,disk_day=COALESCE(excluded.disk_day,frames.disk_day)")
                .bind(&f.contract).bind(f.day).bind(&f.cursor).bind(f.action_day).bind(f.event_us).bind(&f.signature).bind(&f.payload).bind(f.received_us).bind(epoch.unwrap_or(0)).bind(disk).execute(&mut *tx).await?;
        }
        if let Some(epoch) = epoch {
            if let Some(last) = frames.last() {
                sqlx::query("UPDATE source_state SET last_live_us=? WHERE id=1 AND epoch=?")
                    .bind(last.received_us)
                    .bind(epoch)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        if let Some((day, offset, published, size, guard)) = cursor {
            sqlx::query("INSERT INTO source_files VALUES(?,?,?,?,?,NULL) ON CONFLICT(day) DO UPDATE SET offset=excluded.offset,published=excluded.published,size=excluded.size,guard=excluded.guard,error=NULL").bind(day).bind(offset as i64).bind(published as i64).bind(size as i64).bind(guard).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    fn capture(&self, runtime: &tokio::runtime::Handle) -> Result<()> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::SUB)?;
        socket.set_linger(0)?;
        socket.set_rcvhwm(131072)?;
        socket.set_maxmsgsize(1_048_576)?;
        socket.monitor(
            "inproc://sn-cache-monitor",
            zmq::SocketEvent::CONNECTED as i32 | zmq::SocketEvent::DISCONNECTED as i32,
        )?;
        let monitor = context.socket(zmq::PAIR)?;
        monitor.set_linger(0)?;
        monitor.connect("inproc://sn-cache-monitor")?;
        socket.set_subscribe(PREFIX)?;
        socket.connect(&self.cfg.zmq_endpoint)?;
        let mut epoch = 0;
        let mut connected = false;
        let mut pending = Vec::with_capacity(256);
        let mut flush = std::time::Instant::now();
        while !self.stop.load(Ordering::Acquire) {
            match monitor.recv_msg(zmq::DONTWAIT) {
                Ok(event) => {
                    ensure!(
                        event.len() == 6 && monitor.get_rcvmore()?,
                        "invalid cache ZMQ monitor event"
                    );
                    let _ = monitor.recv_msg(0)?;
                    if !pending.is_empty() {
                        runtime.block_on(self.store(&pending, Some(epoch), None, None))?;
                        pending.clear();
                    }
                    connected = u16::from_ne_bytes([event[0], event[1]])
                        == zmq::SocketEvent::CONNECTED.to_raw();
                    epoch = runtime.block_on(self.epoch(connected))?;
                }
                Err(zmq::Error::EAGAIN | zmq::Error::EINTR) => {}
                Err(e) => return Err(e.into()),
            }
            match socket.recv_msg(zmq::DONTWAIT) {
                Ok(raw) => {
                    if socket.get_rcvmore()? {
                        while socket.get_rcvmore()? {
                            socket.recv_msg(0)?;
                        }
                        pending.clear();
                        epoch = runtime.block_on(self.epoch(connected))?;
                    } else if connected {
                        match Frame::parse(&raw, Utc::now().timestamp_micros()) {
                            Ok(Some(frame)) => pending.push(frame),
                            Ok(None) => {}
                            Err(e) => {
                                eprintln!("invalid native cache frame: {e:#}");
                                pending.clear();
                                epoch = runtime.block_on(self.epoch(connected))?;
                            }
                        }
                    }
                }
                Err(zmq::Error::EAGAIN | zmq::Error::EINTR) => {
                    std::thread::sleep(Duration::from_millis(1))
                }
                Err(e) => return Err(e.into()),
            }
            if !pending.is_empty()
                && (pending.len() >= 256 || flush.elapsed() >= Duration::from_millis(100))
            {
                runtime.block_on(self.store(&pending, Some(epoch), None, None))?;
                pending.clear();
                flush = std::time::Instant::now();
            }
        }
        if !pending.is_empty() {
            runtime.block_on(self.store(&pending, Some(epoch), None, None))?;
        }
        runtime.block_on(self.epoch(false))?;
        Ok(())
    }
    async fn import_recent(&self) -> Result<()> {
        let today = Utc::now().with_timezone(&Shanghai).date_naive();
        for back in 0..self.cfg.retention_days {
            let day = today - Days::days(back.into());
            let number = day.format("%Y%m%d").to_string().parse::<i64>()?;
            let directory = self.cfg.msg_root.join(format!("{number:08}"));
            let path = directory.join("mdl_22_1_0.csv");
            if !path.is_file() {
                continue;
            }
            loop {
                if self.stop.load(Ordering::Acquire) {
                    return Ok(());
                }
                let row = sqlx::query("SELECT offset,guard,error FROM source_files WHERE day=?")
                    .bind(number)
                    .fetch_optional(&self.pool)
                    .await?;
                let offset = row
                    .as_ref()
                    .map(|r| r.get::<i64, _>("offset") as u64)
                    .unwrap_or(0);
                let guard = row
                    .as_ref()
                    .map(|r| r.get::<Vec<u8>, _>("guard"))
                    .unwrap_or_default();
                let source = path.clone();
                let chunk =
                    tokio::task::spawn_blocking(move || read_chunk(&source, offset, &guard))
                        .await?;
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        sqlx::query("INSERT INTO source_files VALUES(?,0,0,0,X'',?) ON CONFLICT(day) DO UPDATE SET error=excluded.error").bind(number).bind(format!("{e:#}")).execute(&self.pool).await?;
                        break;
                    }
                };
                let published = published_bytes(&directory).unwrap_or(chunk.size);
                self.store(
                    &chunk.frames,
                    None,
                    Some(number),
                    Some((number, chunk.offset, published, chunk.size, chunk.guard)),
                )
                .await?;
                if chunk.offset <= offset || chunk.offset >= chunk.size {
                    break;
                }
            }
        }
        let cutoff = (today - Days::days(i64::from(self.cfg.retention_days)))
            .format("%Y%m%d")
            .to_string()
            .parse::<i64>()?;
        sqlx::query("DELETE FROM frames WHERE action_day<?")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM source_files WHERE day<?")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn coverage(&self, request: &GetIntradayFramesRequest) -> Result<IntradayCoverage> {
        ensure!(
            request.exchange == Exchange::Shfe as i32
                && request.contract.len() == 6
                && request.contract.starts_with("sn")
                && request.contract[2..].bytes().all(|b| b.is_ascii_digit()),
            "intraday cache owns SHFE/SN only"
        );
        let now = Utc::now();
        let start = chrono::DateTime::from_timestamp_millis(request.historical_end_ms)
            .context("invalid historical cutoff")?;
        ensure!(
            start <= now && now - start <= Days::days(self.cfg.retention_days.into()),
            "historical cutoff is outside retained intraday coverage"
        );
        NaiveDate::parse_from_str(&request.trading_day.to_string(), "%Y%m%d")
            .context("invalid native trading day")?;
        let row = sqlx::query("SELECT * FROM source_state WHERE id=1")
            .fetch_one(&self.pool)
            .await?;
        let epoch: i64 = row.try_get("epoch")?;
        let began: i64 = row.try_get("started_us")?;
        let last: i64 = row.try_get("last_live_us")?;
        let connected: bool = row.try_get("connected")?;
        let mut result = IntradayCoverage {
            ready: false,
            source_generation: epoch as u64,
            connected_since_us: began as u64,
            last_live_received_us: last as u64,
            disk_overlap_us: 0,
            reason: String::new(),
        };
        if !connected || !self.healthy.load(Ordering::Acquire) || last == 0 {
            result.reason = "cache ZMQ connection is not receiving durable native quotes".into();
            return Ok(result);
        }
        if began <= request.historical_end_ms * 1000 {
            result.ready = true;
            return Ok(result);
        }
        let anchor=sqlx::query("SELECT event_us,disk_day FROM frames WHERE contract=? AND live_epoch=? AND disk_day IS NOT NULL AND event_us>=? ORDER BY cursor LIMIT 1").bind(&request.contract).bind(epoch).bind(began).fetch_optional(&self.pool).await?;
        let Some(anchor) = anchor else {
            result.reason =
                "waiting for delayed MDL disk prefix to overlap the current live connection".into();
            return Ok(result);
        };
        let anchor_day: i64 = anchor.try_get("disk_day")?;
        let mut date = start.with_timezone(&Shanghai).date_naive();
        let end = NaiveDate::parse_from_str(&anchor_day.to_string(), "%Y%m%d")?;
        while date < end {
            let number = date.format("%Y%m%d").to_string().parse::<i64>()?;
            let file =
                sqlx::query("SELECT offset,published,size,error FROM source_files WHERE day=?")
                    .bind(number)
                    .fetch_optional(&self.pool)
                    .await?;
            let complete = file.as_ref().is_some_and(|r| {
                r.get::<i64, _>("offset") >= r.get::<i64, _>("published")
                    && r.get::<i64, _>("offset") == r.get::<i64, _>("size")
                    && r.get::<Option<String>, _>("error").is_none()
            });
            let absent_sunday = date.weekday() == Weekday::Sun
                && !self.cfg.msg_root.join(format!("{number:08}")).exists();
            if !complete && !absent_sunday {
                result.reason = format!("delayed source date {number} is not completely imported");
                return Ok(result);
            }
            date += Days::days(1);
        }
        let healthy = sqlx::query("SELECT error FROM source_files WHERE day=?")
            .bind(anchor_day)
            .fetch_optional(&self.pool)
            .await?;
        if healthy
            .as_ref()
            .is_none_or(|r| r.get::<Option<String>, _>("error").is_some())
        {
            result.reason = "overlap source file is unavailable or changed".into();
            return Ok(result);
        }
        result.ready = true;
        result.disk_overlap_us = anchor.try_get("event_us")?;
        Ok(result)
    }
    pub async fn stream(
        self: Arc<Self>,
        request: GetIntradayFramesRequest,
    ) -> Result<ReceiverStream<Result<IntradayFrameBatch, Status>>, Status> {
        let permit = self
            .readers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("intraday readers busy"))?;
        let coverage = self
            .coverage(&request)
            .await
            .map_err(|e| Status::failed_precondition(format!("{e:#}")))?;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let _permit = permit;
            let result = self.send_snapshot(&request, coverage, &tx).await;
            if let Err(e) = result {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    tx.send(Err(Status::data_loss(format!("{e:#}")))),
                )
                .await;
            }
        });
        Ok(ReceiverStream::new(rx))
    }
    async fn send_snapshot(
        &self,
        request: &GetIntradayFramesRequest,
        coverage: IntradayCoverage,
        tx: &mpsc::Sender<Result<IntradayFrameBatch, Status>>,
    ) -> Result<()> {
        if !coverage.ready {
            tx.send(Ok(IntradayFrameBatch {
                frames: Vec::new(),
                next_cursor: request.after_cursor.clone(),
                complete: true,
                coverage: Some(coverage),
            }))
            .await?;
            return Ok(());
        }
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;
        let mut rows=sqlx::query("SELECT payload,cursor,received_us FROM frames WHERE contract=? AND day=? AND cursor>? ORDER BY cursor").bind(&request.contract).bind(i64::from(request.trading_day)).bind(&request.after_cursor).fetch(&mut *transaction);
        let mut batch = Vec::with_capacity(BATCH);
        let mut cursor = request.after_cursor.clone();
        let mut count = 0;
        while let Some(row) = rows.try_next().await? {
            count += 1;
            ensure!(
                count <= MAX_ROWS,
                "bounded intraday native-day snapshot limit exceeded"
            );
            cursor = row.try_get("cursor")?;
            batch.push(IntradayFrame {
                payload: row.try_get("payload")?,
                cursor: cursor.clone(),
                received_us: row.try_get::<i64, _>("received_us")? as u64,
            });
            if batch.len() == BATCH {
                tokio::time::timeout(
                    Duration::from_secs(15),
                    tx.send(Ok(IntradayFrameBatch {
                        frames: std::mem::take(&mut batch),
                        next_cursor: cursor.clone(),
                        complete: false,
                        coverage: Some(coverage.clone()),
                    })),
                )
                .await??;
            }
        }
        drop(rows);
        transaction.commit().await?;
        let current = self.coverage(request).await?;
        ensure!(
            current.ready && current.source_generation == coverage.source_generation,
            "source epoch changed during snapshot; replay again"
        );
        tokio::time::timeout(
            Duration::from_secs(15),
            tx.send(Ok(IntradayFrameBatch {
                frames: batch,
                next_cursor: cursor,
                complete: true,
                coverage: Some(coverage),
            })),
        )
        .await??;
        Ok(())
    }
}

struct Chunk {
    frames: Vec<Frame>,
    offset: u64,
    size: u64,
    guard: Vec<u8>,
}
fn guard(file: &mut File, offset: u64) -> Result<Vec<u8>> {
    let start = offset.saturating_sub(256);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = vec![0; (offset - start) as usize];
    file.read_exact(&mut bytes)?;
    Ok(Sha256::digest(bytes).to_vec())
}
fn read_chunk(path: &Path, offset: u64, previous_guard: &[u8]) -> Result<Chunk> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    ensure!(size >= offset, "delayed source was truncated: {path:?}");
    if offset > 0 {
        ensure!(
            guard(&mut file, offset)? == previous_guard,
            "delayed source prefix changed: {path:?}"
        );
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file.take(size - offset));
    let mut position = offset;
    let mut raw = Vec::new();
    let mut frames = Vec::new();
    while frames.len() < 512 && position - offset < 8 * 1024 * 1024 {
        raw.clear();
        let n = reader.read_until(b'\n', &mut raw)?;
        ensure!(raw.len() <= 65536, "oversized delayed MDL row");
        if n == 0 || raw.last() != Some(&b'\n') {
            break;
        }
        position += n as u64;
        if offset == 0 && position == n as u64 {
            ensure!(
                raw.starts_with(b"InstruID,TradDay,ActionDay,UpdateTime,"),
                "unexpected SHFE source header"
            );
            continue;
        }
        while raw.last().is_some_and(|b| matches!(b, b'\n' | b'\r')) {
            raw.pop();
        }
        if !raw.starts_with(b"sn") {
            continue;
        }
        let mut payload = Vec::with_capacity(PREFIX.len() + raw.len());
        payload.extend_from_slice(PREFIX);
        payload.extend_from_slice(&raw);
        if let Some(frame) = Frame::parse(&payload, Utc::now().timestamp_micros())? {
            frames.push(frame);
        }
    }
    let mut file = File::open(path)?;
    Ok(Chunk {
        frames,
        offset: position,
        size,
        guard: guard(&mut file, position)?,
    })
}
fn published_bytes(directory: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(directory.join("state.csv")).ok()?;
    for line in text.lines() {
        let mut fields = line.split(',').map(str::trim);
        if fields.next() == Some("mdl_22_1_0.csv") {
            return fields.nth(1)?.parse().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(seq: u64) -> Frame {
        let now = Utc::now().with_timezone(&Shanghai);
        let day = now.format("%Y%m%d").to_string();
        let mut fields = vec![String::new(); 43];
        for (i, v) in [
            (0, "sn2612".to_owned()),
            (1, day.clone()),
            (2, day),
            (3, now.format("%H:%M:%S%.3f").to_string()),
            (4, "349180".into()),
            (11, "89".into()),
            (12, "31077020".into()),
            (13, "35000".into()),
            (42, seq.to_string()),
        ] {
            fields[i] = v;
        }
        for i in 0..5 {
            fields[21 + 2 * i] = (349170 - i * 10).to_string();
            fields[22 + 2 * i] = "2".into();
            fields[31 + 2 * i] = (349190 + i * 10).to_string();
            fields[32 + 2 * i] = "3".into();
        }
        let raw = format!("mdl.22.1.* || {}", fields.join(","));
        Frame::parse(raw.as_bytes(), Utc::now().timestamp_micros())
            .unwrap()
            .unwrap()
    }
    async fn cache() -> (tempfile::TempDir, Arc<Cache>) {
        let temp = tempfile::tempdir().unwrap();
        let cache = Cache::open(Config {
            directory: temp.path().join("cache"),
            msg_root: temp.path().join("source"),
            zmq_endpoint: "unused".into(),
            retention_days: 7,
        })
        .await
        .unwrap();
        (temp, cache)
    }
    fn request(f: &Frame) -> GetIntradayFramesRequest {
        GetIntradayFramesRequest {
            exchange: Exchange::Shfe as i32,
            contract: f.contract.clone(),
            trading_day: f.day as u32,
            historical_end_ms: Utc::now().timestamp_millis() - 1000,
            after_cursor: String::new(),
        }
    }
    #[tokio::test]
    async fn exact_disk_overlap_deduplicates_nonzero_open_and_reconnect_invalidates() {
        let (_temp, c) = cache().await;
        let epoch = c.epoch(true).await.unwrap();
        let f = frame(135);
        let day = f.action_day;
        let req = request(&f);
        c.store(std::slice::from_ref(&f), Some(epoch), None, None)
            .await
            .unwrap();
        assert!(!c.coverage(&req).await.unwrap().ready);
        c.store(
            std::slice::from_ref(&f),
            None,
            Some(day),
            Some((day, 100, 100, 100, vec![])),
        )
        .await
        .unwrap();
        // Millisecond source clock can precede the connection wall clock;
        // make this fixture's native event unambiguously follow epoch start.
        sqlx::query("UPDATE source_state SET started_us=?")
            .bind(f.event_us - 1)
            .execute(&c.pool)
            .await
            .unwrap();
        assert!(c.coverage(&req).await.unwrap().ready);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM frames")
            .fetch_one(&c.pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
        c.epoch(true).await.unwrap();
        assert!(!c.coverage(&req).await.unwrap().ready);
    }
    #[tokio::test]
    async fn native_identity_conflict_is_atomic() {
        let (_temp, c) = cache().await;
        let f = frame(7);
        c.store(std::slice::from_ref(&f), None, None, None)
            .await
            .unwrap();
        let mut changed = f.clone();
        changed.signature[0] ^= 1;
        assert!(c
            .store(&[changed], None, Some(f.action_day), None)
            .await
            .is_err());
        let disk: Option<i64> = sqlx::query_scalar("SELECT disk_day FROM frames")
            .fetch_one(&c.pool)
            .await
            .unwrap();
        assert!(disk.is_none());
    }
    #[tokio::test]
    async fn delayed_import_and_live_writes_do_not_upgrade_read_locks() {
        let (_temp, c) = cache().await;
        let live: Vec<_> = (1..=256).map(frame).collect();
        let disk: Vec<_> = (257..=512).map(frame).collect();
        let epoch = c.epoch(true).await.unwrap();
        let (a, b) = tokio::join!(
            c.store(&live, Some(epoch), None, None),
            c.store(&disk, None, Some(disk[0].action_day), None)
        );
        a.unwrap();
        b.unwrap();
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM frames")
            .fetch_one(&c.pool)
            .await
            .unwrap();
        assert_eq!(rows, 512);
    }

    #[test]
    fn partial_line_waits_and_prefix_replacement_is_rejected() {
        use std::io::Write;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mdl.csv");
        let f = frame(1);
        let header = b"InstruID,TradDay,ActionDay,UpdateTime,rest\n";
        let csv = &f.payload[PREFIX.len()..];
        let mut file = File::create(&path).unwrap();
        file.write_all(header).unwrap();
        file.write_all(csv).unwrap();
        let first = read_chunk(&path, 0, &[]).unwrap();
        assert!(first.frames.is_empty());
        assert_eq!(first.offset, header.len() as u64);
        file.write_all(b"\n").unwrap();
        let next = read_chunk(&path, first.offset, &first.guard).unwrap();
        assert_eq!(next.frames.len(), 1);
        assert_eq!(next.frames[0].payload, f.payload);
        file.seek(SeekFrom::Start(next.offset - 3)).unwrap();
        file.write_all(b"xx").unwrap();
        assert!(read_chunk(&path, next.offset, &next.guard).is_err());
    }
    #[tokio::test]
    async fn bounded_snapshot_has_final_marker_and_cursor() {
        use tokio_stream::StreamExt;
        let (_temp, c) = cache().await;
        let epoch = c.epoch(true).await.unwrap();
        let f = frame(8);
        let mut req = request(&f);
        sqlx::query("UPDATE source_state SET started_us=?")
            .bind(req.historical_end_ms * 1000 - 1)
            .execute(&c.pool)
            .await
            .unwrap();
        c.store(std::slice::from_ref(&f), Some(epoch), None, None)
            .await
            .unwrap();
        let mut stream = c.clone().stream(req.clone()).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert!(batch.complete);
        assert!(batch.coverage.unwrap().ready);
        assert_eq!(batch.frames[0].payload, f.payload);
        assert!(stream.next().await.is_none());
        req.after_cursor = batch.next_cursor;
        let mut stream = c.stream(req).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert!(batch.complete);
        assert!(batch.frames.is_empty());
    }
    #[tokio::test]
    async fn stale_native_snapshot_does_not_prove_new_epoch() {
        let (_temp, c) = cache().await;
        let epoch = c.epoch(true).await.unwrap();
        let f = frame(9);
        let req = request(&f);
        c.store(
            std::slice::from_ref(&f),
            Some(epoch),
            Some(f.action_day),
            Some((f.action_day, 100, 100, 100, vec![])),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE source_state SET started_us=?")
            .bind(f.event_us + 1_000_000)
            .execute(&c.pool)
            .await
            .unwrap();
        assert!(!c.coverage(&req).await.unwrap().ready);
    }
}
