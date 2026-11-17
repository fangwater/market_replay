//! Dedicated lossless XPUB/SUB session. Raw frames are never rewritten.
use crate::{config::Config, engine::Quote, metadata::Metadata};
use anyhow::{ensure, Context, Result};
use mdl_replay::{
    cache::Cache,
    channel::Channel,
    plan::{self, Plan, Selection},
    publish,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

pub struct Replay {
    pub plan: Arc<Plan>,
    session: publish::Session,
    sub: zmq::Socket,
    at: usize,
    pub digest: Sha256,
    pub bytes: u64,
    pub verified_frames: u64,
}
impl Replay {
    pub fn open(cfg: &Config, selection: Selection) -> Result<Self> {
        let cache = Cache::new(cfg.msg_root.clone(), cfg.cache_root.clone(), 2)
            .with_max_bytes(48 * 1024 * 1024 * 1024);
        let (_, _, dirs) = plan::resolve_dirs(&cache, &selection)?;
        let channels = if selection.channels.is_empty() {
            mdl_replay::channel::DEFAULT_CHANNELS
                .iter()
                .map(|s| Channel::parse(s))
                .collect::<Result<Vec<_>>>()?
        } else {
            selection
                .channels
                .iter()
                .map(|s| Channel::parse(s))
                .collect::<Result<Vec<_>>>()?
        };
        // Warm sequentially: existing prepare() can then open, rather than build, in parallel.
        for d in dirs {
            for c in &channels {
                cache.shard(d, c, false)?;
            }
        }
        let plan = Arc::new(plan::prepare(&cache, &selection)?);
        ensure!(!plan.entries.is_empty(), "no MDL frames for selection");
        ensure!(
            cfg.allow_partial || !plan.summary.partial,
            "source is still partial; set allow_partial explicitly for research"
        );
        let ctx = zmq::Context::new();
        let params = publish::Params {
            speed: cfg.replay_speed,
            manual_start: false,
            wait_subscribers: 1,
            settle_ms: 20,
            lossless: true,
            sndhwm: 4096,
            hold_open_s: 600,
            max_gap_ms: Some(1),
            ..Default::default()
        };
        let session = publish::spawn(
            ctx.clone(),
            1,
            plan.clone(),
            params,
            "tcp://127.0.0.1:*".into(),
            "127.0.0.1",
        )?;
        let sub = ctx.socket(zmq::SUB)?;
        sub.set_rcvhwm(4096)?;
        sub.set_rcvtimeo(1000)?;
        sub.set_linger(0)?;
        sub.set_subscribe(b"")?;
        sub.connect(&session.endpoint)?;
        Ok(Self {
            plan,
            session,
            sub,
            at: 0,
            digest: Sha256::new(),
            bytes: 0,
            verified_frames: 0,
        })
    }
    pub fn bounds(&self) -> (i64, i64) {
        (self.time(0), self.time(self.plan.entries.len() - 1))
    }
    fn time(&self, i: usize) -> i64 {
        let (s, r) = self.plan.row(self.plan.entries[i]);
        (mdl_replay::day::to_epoch_days(s.index.dir_day) * 86_400_000 + i64::from(r.ms)
            - 8 * 3600 * 1000)
            * 1000
    }
    pub fn next(
        &mut self,
        metadata: &Metadata,
        cancel: &impl Fn() -> bool,
    ) -> Result<Option<Quote>> {
        loop {
            ensure!(!cancel(), "cancelled");
            let frame = match self.sub.recv_bytes(0) {
                Ok(x) => x,
                Err(zmq::Error::EAGAIN) => {
                    ensure!(
                        self.session.ctl.error.lock().unwrap().is_none(),
                        "MDL publisher failed"
                    );
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            if frame.starts_with(b"replay.ctl") {
                let value: serde_json::Value = serde_json::from_slice(
                    frame
                        .splitn(2, |b| *b == b'|')
                        .nth(1)
                        .and_then(|x| x.strip_prefix(b"| "))
                        .context("bad control frame")?,
                )?;
                if value["event"] == "end" {
                    ensure!(
                        value["state"] == "done",
                        "replay did not finish successfully"
                    );
                    ensure!(
                        value["sent"].as_u64() == Some(self.at as u64)
                            && self.at == self.plan.entries.len(),
                        "MDL frame count mismatch"
                    );
                    return Ok(None);
                }
                continue;
            }
            ensure!(self.at < self.plan.entries.len(), "excess MDL frame");
            let (shard, row) = self.plan.row(self.plan.entries[self.at]);
            ensure!(
                frame == shard.frame(row),
                "MDL byte/ordering mismatch at {}",
                self.at
            );
            self.digest.update(&frame);
            self.bytes += frame.len() as u64;
            self.verified_frames += 1;
            let ts = self.time(self.at);
            let cursor = self.at as u64;
            self.at += 1;
            let parsed = cn_futures_core::mdl::parse_frame(&frame).with_context(|| {
                format!("parse source {} row {}", shard.index.source_path, row.row)
            })?;
            let c = mdl_replay::line::canonical_contract(parsed.symbol, parsed.trading_day)?;
            let spec = metadata.spec(&c)?;
            let mut bid = [None; 5];
            let mut ask = [None; 5];
            let mut bid_qty = [0; 5];
            let mut ask_qty = [0; 5];
            for i in 0..5 {
                bid[i] = spec.ticks(parsed.bid[i].price);
                ask[i] = spec.ticks(parsed.ask[i].price);
                if parsed.bid[i].quantity != u64::MAX {
                    bid_qty[i] = parsed.bid[i].quantity.min(i64::MAX as u64) as i64;
                }
                if parsed.ask[i].quantity != u64::MAX {
                    ask_qty[i] = parsed.ask[i].quantity.min(i64::MAX as u64) as i64;
                }
            }
            // Source dates and exchange-clock boundaries both constrain trading.
            let end = spec
                .segment_end(ts)
                .filter(|_| spec.segment_end(parsed.exchange_ts_us).is_some());
            return Ok(Some(Quote {
                ts,
                exchange_ts: parsed.exchange_ts_us,
                cursor,
                day: parsed.trading_day,
                contract: c,
                bid,
                ask,
                bid_qty,
                ask_qty,
                last: spec.ticks(parsed.last_price),
                volume: (parsed.volume != u64::MAX).then_some(parsed.volume),
                end,
            }));
        }
    }
    pub fn sources(&self) -> Vec<serde_json::Value> {
        self.plan.shards.iter().map(|s|serde_json::json!({"path":s.index.source_path,"size":s.index.source_size,"mtime_ns":s.index.source_mtime_ns,"indexed_bytes":s.index.indexed_bytes,"header":s.index.header,"complete":s.index.complete})).collect()
    }
    pub fn verify_sources(&self) -> Result<()> {
        for s in &self.plan.shards {
            let m = std::fs::metadata(&s.index.source_path)?;
            let ns = m
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos() as u64;
            ensure!(
                m.len() == s.index.source_size && ns == s.index.source_mtime_ns,
                "source changed during run: {}",
                s.index.source_path
            );
        }
        Ok(())
    }
    pub fn hashes(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([(
            "frames".into(),
            serde_json::json!({"sha256":format!("{:x}",self.digest.clone().finalize()),"bytes":self.bytes,"verified_frames":self.verified_frames}),
        )])
    }
}
impl Drop for Replay {
    fn drop(&mut self) {
        self.session.ctl.cancel.store(true, Ordering::Relaxed);
        self.session.ctl.close.store(true, Ordering::Relaxed);
        if let Some(h) = self.session.thread.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}
