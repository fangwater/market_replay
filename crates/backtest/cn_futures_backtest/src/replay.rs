//! API-only historical MDL client. No NFS, cache or source-plan file access.
use crate::{config::Config, engine::Quote, market_api::MarketApi, metadata::Metadata};
use anyhow::{ensure, Context, Result};
use cn_market_proto::{GetReplayFramesRequest, ReplayFrame, ReplayFrameBatch};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

pub struct Replay {
    api: MarketApi,
    stream: tonic::Streaming<ReplayFrameBatch>,
    pub manifest: Value,
    buffered: VecDeque<ReplayFrame>,
    count: u64,
    digest: Sha256,
    bytes: u64,
    last_ts: Option<i64>,
    complete: bool,
}
impl Replay {
    pub fn open(
        cfg: &Config,
        req: GetReplayFramesRequest,
        cancel: &impl Fn() -> bool,
    ) -> Result<Self> {
        let trading_day = req.trading_day;
        let mut api = MarketApi::connect(&cfg.market_grpc)?;
        let mut stream = api.replay(req)?;
        let first = loop {
            ensure!(!cancel(), "cancelled");
            match api.runtime.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(1), stream.message()).await
            }) {
                Ok(batch) => break batch?.context("replay API omitted manifest")?,
                Err(_) => continue,
            }
        };
        ensure!(
            !first.manifest_json.is_empty() && first.frames.is_empty() && !first.complete,
            "invalid first replay batch"
        );
        let manifest: Value = serde_json::from_str(&first.manifest_json)?;
        ensure!(
            manifest["schema"] == "mdl-grpc-replay/v1",
            "unsupported replay API schema"
        );
        ensure!(
            manifest["plan"]["trade_day"].as_u64() == Some(u64::from(trading_day)),
            "API returned another trading day"
        );
        ensure!(
            manifest["plan"]["rows"].as_u64().is_some_and(|n| n > 0),
            "empty replay plan"
        );
        let partial = manifest["plan"]["partial"]
            .as_bool()
            .context("missing source completeness")?;
        ensure!(cfg.allow_partial || !partial, "source is partial");
        let first = manifest["first_us"]
            .as_i64()
            .context("missing first time")?;
        let last = manifest["last_us"].as_i64().context("missing last time")?;
        ensure!(first <= last, "invalid replay bounds");
        Ok(Self {
            api,
            stream,
            manifest,
            buffered: VecDeque::new(),
            count: 0,
            digest: Sha256::new(),
            bytes: 0,
            last_ts: None,
            complete: false,
        })
    }
    pub fn bounds(&self) -> (i64, i64) {
        (
            self.manifest["first_us"].as_i64().unwrap(),
            self.manifest["last_us"].as_i64().unwrap(),
        )
    }
    pub fn next(
        &mut self,
        metadata: &Metadata,
        cancel: &impl Fn() -> bool,
    ) -> Result<Option<Quote>> {
        loop {
            ensure!(!cancel(), "cancelled");
            if let Some(f) = self.buffered.pop_front() {
                ensure!(f.cursor == self.count, "replay frame cursor mismatch");
                ensure!(
                    self.last_ts.is_none_or(|p| p <= f.baseline_us),
                    "replay clock went backwards"
                );
                let (first, last) = self.bounds();
                ensure!(
                    first <= f.baseline_us && f.baseline_us <= last,
                    "frame outside replay bounds"
                );
                self.last_ts = Some(f.baseline_us);
                self.digest.update(&f.payload);
                self.bytes += f.payload.len() as u64;
                self.count += 1;
                let p = cn_futures_core::mdl::parse_frame(&f.payload).with_context(|| {
                    format!(
                        "parse API frame {} {} row {}",
                        f.source_day, f.channel, f.source_row
                    )
                })?;
                let c = mdl_replay::line::canonical_contract(p.symbol, p.trading_day)?;
                let spec = metadata.spec_ref(&c)?;
                let mut bid = [None; 5];
                let mut ask = [None; 5];
                let mut bid_qty = [0; 5];
                let mut ask_qty = [0; 5];
                for i in 0..5 {
                    bid[i] = spec.ticks(p.bid[i].price);
                    ask[i] = spec.ticks(p.ask[i].price);
                    if p.bid[i].quantity != u64::MAX {
                        bid_qty[i] = p.bid[i].quantity.min(i64::MAX as u64) as i64;
                    }
                    if p.ask[i].quantity != u64::MAX {
                        ask_qty[i] = p.ask[i].quantity.min(i64::MAX as u64) as i64;
                    }
                }
                let end = spec
                    .segment_end(f.baseline_us)
                    .filter(|_| spec.segment_end(p.exchange_ts_us).is_some());
                return Ok(Some(Quote {
                    ts: f.baseline_us,
                    exchange_ts: p.exchange_ts_us,
                    cursor: f.cursor,
                    day: p.trading_day,
                    contract: c,
                    bid,
                    ask,
                    bid_qty,
                    ask_qty,
                    last: spec.ticks(p.last_price),
                    volume: (p.volume != u64::MAX).then_some(p.volume),
                    end,
                }));
            }
            if self.complete {
                return Ok(None);
            }
            let batch = match self.api.runtime.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(1), self.stream.message()).await
            }) {
                Ok(batch) => batch?.context("replay ended without verified completion")?,
                Err(_) => continue,
            };
            ensure!(batch.manifest_json.is_empty(), "duplicate replay manifest");
            if batch.complete {
                ensure!(batch.frames.is_empty(), "completion batch contains frames");
                ensure!(
                    batch.frame_count == self.count
                        && Some(self.count) == self.manifest["plan"]["rows"].as_u64(),
                    "replay frame count mismatch"
                );
                ensure!(
                    batch.frames_sha256 == format!("{:x}", self.digest.clone().finalize()),
                    "replay frame digest mismatch"
                );
                ensure!(
                    self.api.runtime.block_on(self.stream.message())?.is_none(),
                    "frames after replay completion"
                );
                self.complete = true;
            } else {
                ensure!(!batch.frames.is_empty(), "empty replay data batch");
                self.buffered = batch.frames.into();
            }
        }
    }
    pub fn verify_sources(&self) -> Result<()> {
        ensure!(
            self.complete,
            "replay API has not verified source stability"
        );
        Ok(())
    }
    pub fn hashes(&self) -> Value {
        json!({"frames":{"sha256":format!("{:x}",self.digest.clone().finalize()),"bytes":self.bytes,"verified_frames":self.count}})
    }
}
