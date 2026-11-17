//! Bounded historical replay over gRPC. The service owns NFS/cache access;
//! clients receive original frames, source cursors and the virtual clock.
use anyhow::{ensure, Context, Result};
use cn_market_proto::{Exchange, GetReplayFramesRequest, ReplayFrame, ReplayFrameBatch};
use mdl_replay::{
    cache::Cache,
    plan::{self, Plan, Selection},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};
use tokio::sync::{mpsc, Semaphore};
use tonic::Status;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub msg_root: PathBuf,
    pub cache_root: PathBuf,
}
pub struct ReplayService {
    cfg: Config,
    slot: Arc<Semaphore>,
}
impl ReplayService {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            slot: Arc::new(Semaphore::new(1)),
        }
    }
    pub fn stream(
        &self,
        req: GetReplayFramesRequest,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<Result<ReplayFrameBatch, Status>>, Status>
    {
        let selection = selection(&req).map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("one historical replay at a time"))?;
        let (tx, rx) = mpsc::channel(2);
        let cfg = self.cfg.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if let Err(e) = send(&cfg, &selection, req.allow_partial, &tx) {
                let _ = tx.blocking_send(Err(Status::failed_precondition(format!("{e:#}"))));
            }
        });
        Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}
fn selection(req: &GetReplayFramesRequest) -> Result<Selection> {
    let day = mdl_replay::day::parse(&req.trading_day.to_string())?;
    ensure!(day >= 20231207, "MDL starts on 2023-12-07");
    ensure!(
        !req.contracts.is_empty() && req.contracts.len() <= 256,
        "require 1..256 contracts"
    );
    let mut channels = BTreeSet::new();
    for ex in &req.exchanges {
        let (archive, _) =
            super::archive_exchange(Exchange::try_from(*ex)?).context("exchange is required")?;
        let exchange =
            cn_futures_core::session::Exchange::parse(archive).context("unknown exchange")?;
        channels.insert(exchange.msg_stem().to_string());
    }
    ensure!(!channels.is_empty(), "exchange is required");
    for c in &req.contracts {
        mdl_replay::line::canonical_contract(c.as_bytes(), day)?;
    }
    Ok(Selection {
        trade_day: Some(day.to_string()),
        contracts: req.contracts.clone(),
        channels: channels.into_iter().collect(),
        ..Default::default()
    })
}
pub fn baseline(plan: &Plan, i: usize) -> i64 {
    let (s, r) = plan.row(plan.entries[i]);
    (mdl_replay::day::to_epoch_days(s.index.dir_day) * 86_400_000 + i64::from(r.ms)
        - 8 * 3600 * 1000)
        * 1000
}
fn send(
    cfg: &Config,
    selection: &Selection,
    partial: bool,
    tx: &mpsc::Sender<Result<ReplayFrameBatch, Status>>,
) -> Result<()> {
    let cache = Cache::new(cfg.msg_root.clone(), cfg.cache_root.clone(), 2)
        .with_max_bytes(48 * 1024 * 1024 * 1024);
    let (_, _, dirs) = plan::resolve_dirs(&cache, selection)?;
    let channels = plan::channels(selection)?;
    for d in dirs {
        for c in &channels {
            if tx.is_closed() {
                return Ok(());
            }
            cache.shard(d, c, false)?;
        }
    }
    let plan = plan::prepare(&cache, selection)?;
    ensure!(!plan.entries.is_empty(), "no MDL frames for selection");
    ensure!(partial || !plan.summary.partial, "source is still partial");
    send_plan(&plan, tx)
}
fn send_plan(plan: &Plan, tx: &mpsc::Sender<Result<ReplayFrameBatch, Status>>) -> Result<()> {
    let sources: Vec<_> = plan
        .shards
        .iter()
        .map(|s| {
            serde_json::json!({
        "source_day":s.index.dir_day,"channel":s.index.channel,"size":s.index.source_size,
        "mtime_ns":s.index.source_mtime_ns,"indexed_bytes":s.index.indexed_bytes,
        "header":s.index.header,"complete":s.index.complete})
        })
        .collect();
    let manifest = serde_json::json!({"schema":"mdl-grpc-replay/v1","plan":plan.summary,
        "sources":sources,"first_us":baseline(plan,0),"last_us":baseline(plan,plan.entries.len()-1)});
    if tx
        .blocking_send(Ok(ReplayFrameBatch {
            manifest_json: serde_json::to_string(&manifest)?,
            ..Default::default()
        }))
        .is_err()
    {
        return Ok(());
    }
    let mut digest = Sha256::new();
    let mut frames = Vec::with_capacity(256);
    let mut bytes = 0;
    for (i, e) in plan.entries.iter().enumerate() {
        if tx.is_closed() {
            return Ok(());
        }
        let (s, r) = plan.row(*e);
        let payload = s.frame(r).to_vec();
        digest.update(&payload);
        bytes += payload.len();
        frames.push(ReplayFrame {
            payload,
            cursor: i as u64,
            baseline_us: baseline(plan, i),
            source_day: s.index.dir_day,
            channel: s.index.channel.clone(),
            source_row: u64::from(r.row),
        });
        if frames.len() >= 256 || bytes >= 1024 * 1024 {
            if tx
                .blocking_send(Ok(ReplayFrameBatch {
                    frames: std::mem::take(&mut frames),
                    ..Default::default()
                }))
                .is_err()
            {
                return Ok(());
            }
            bytes = 0;
        }
    }
    if !frames.is_empty()
        && tx
            .blocking_send(Ok(ReplayFrameBatch {
                frames,
                ..Default::default()
            }))
            .is_err()
    {
        return Ok(());
    }
    for s in &plan.shards {
        let stamp = mdl_replay::shard::source_stamp(std::path::Path::new(&s.index.source_path))?;
        ensure!(
            stamp == (s.index.source_size, s.index.source_mtime_ns),
            "source changed during replay: {} {}",
            s.index.dir_day,
            s.index.channel
        );
    }
    let _ = tx.blocking_send(Ok(ReplayFrameBatch {
        complete: true,
        frame_count: plan.entries.len() as u64,
        frames_sha256: format!("{:x}", digest.finalize()),
        ..Default::default()
    }));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streams_original_bytes_in_source_order_with_prefix_max_clock() {
        let root = tempfile::tempdir().unwrap();
        let msg = root.path().join("msg");
        std::fs::create_dir_all(msg.join("20261113")).unwrap();
        let lines = [
            "rb2701,2026-11-13,2026-11-13,09:00:00.500,3500,09:00:00.500,1",
            "rb2701,2026-11-13,2026-11-13,09:00:00.400,3501,09:00:00.400,2",
            "rb2701,2026-11-13,2026-11-13,09:00:01.000,3502,09:00:01.000,3",
        ];
        std::fs::write(
            msg.join("20261113/mdl_22_1_0.csv"),
            format!(
                "InstruID,TradDay,ActionDay,UpdateTime,LastPrice,LocalTime,SeqNo\n{}\n",
                lines.join("\n")
            ),
        )
        .unwrap();
        let svc = ReplayService::new(Config {
            msg_root: msg,
            cache_root: root.path().join("cache"),
        });
        let req = GetReplayFramesRequest {
            trading_day: 20261113,
            contracts: vec!["rb2701".into()],
            exchanges: vec![Exchange::Shfe as i32],
            allow_partial: false,
        };
        let mut stream = svc.stream(req).unwrap();
        let first = stream.next().await.unwrap().unwrap();
        let manifest: serde_json::Value = serde_json::from_str(&first.manifest_json).unwrap();
        assert_eq!(manifest["plan"]["rows"], 3);
        let mut frames = Vec::new();
        let mut final_count = None;
        let mut digest = Sha256::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            for f in batch.frames {
                digest.update(&f.payload);
                frames.push(f);
            }
            if batch.complete {
                final_count = Some(batch.frame_count);
                assert_eq!(
                    batch.frames_sha256,
                    format!("{:x}", digest.clone().finalize())
                );
            }
        }
        assert_eq!(final_count, Some(3));
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(f.payload, format!("mdl.22.1.* || {}", lines[i]).as_bytes());
            assert_eq!(f.cursor, i as u64);
            assert_eq!(f.source_row, i as u64);
        }
        assert_eq!(frames[0].baseline_us, frames[1].baseline_us);
        assert_eq!(frames[2].baseline_us - frames[0].baseline_us, 500_000);
    }

    #[tokio::test]
    async fn rejects_second_replay_before_allocating_another_plan() {
        let svc = ReplayService::new(Config {
            msg_root: "/unused".into(),
            cache_root: "/unused".into(),
        });
        let _permit = svc.slot.clone().acquire_owned().await.unwrap();
        let req = GetReplayFramesRequest {
            trading_day: 20261113,
            contracts: vec!["rb2701".into()],
            exchanges: vec![Exchange::Shfe as i32],
            allow_partial: false,
        };
        assert!(matches!(svc.stream(req),Err(e) if e.code()==tonic::Code::ResourceExhausted));
    }
}
