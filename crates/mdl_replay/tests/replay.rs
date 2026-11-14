//! Synthetic NFS tree -> shards -> plan -> ZMQ frames, end to end.

use mdl_replay::cache::{Cache, Fetch};
use mdl_replay::channel::Channel;
use mdl_replay::plan::{self, Selection};
use mdl_replay::publish::{self, Params, State, CTL_TOPIC};
use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CFFEX_HDR: &str = "ActionDay,TradDay,UpdateTime,InstruID,LastPrice,LocalTime,SeqNo";
const SHFE_HDR: &str = "InstruID,TradDay,ActionDay,UpdateTime,LastPrice,LocalTime,SeqNo";

fn shfe(sym: &str, td: &str, t: &str, seq: u32) -> String {
    format!("{sym},{td},{td},{t},1.0,{t},{seq}")
}
fn cffex(sym: &str, td: &str, t: &str, seq: u32) -> String {
    format!("{td},{td},{t},{sym},2.0,{t},{seq}")
}

fn write(root: &Path, dir: &str, file: &str, header: &str, rows: &[String]) {
    let d = root.join(dir);
    fs::create_dir_all(&d).unwrap();
    let mut body = format!("{header}\n");
    for r in rows {
        body.push_str(r);
        body.push('\n');
    }
    fs::write(d.join(file), body).unwrap();
}

/// Thursday 20261112 (full day) and Friday 20261113; trade day 20261113's
/// night session starts Thursday 21:00 and crosses midnight.
fn fixture(root: &Path) {
    write(
        root,
        "20261112",
        "mdl_21_1_0.csv",
        CFFEX_HDR,
        &[cffex("IF2612", "2026-11-12", "09:30:00.100", 1)],
    );
    write(
        root,
        "20261112",
        "mdl_22_1_0.csv",
        SHFE_HDR,
        &[
            shfe("rb2601", "2026-11-12", "10:00:00.000", 1), // other trade day
            shfe("rb2601", "2026-11-13", "21:00:00.500", 2),
            shfe("ag2702", "2026-11-13", "21:00:00.400", 3), // jitter: earlier than previous row
            shfe("rb2601", "2026-11-13", "23:59:59.900", 4),
        ],
    );
    write(
        root,
        "20261112",
        "mdl_23_1_0.csv",
        CFFEX_HDR,
        &[cffex("AP601", "2026-11-13", "21:00:00.450", 1)],
    );
    write(
        root,
        "20261113",
        "mdl_21_1_0.csv",
        CFFEX_HDR,
        &[
            cffex("IF2612", "2026-11-13", "09:30:00.200", 1),
            cffex("IF2612", "2026-11-13", "09:30:00.300", 2),
        ],
    );
    write(
        root,
        "20261113",
        "mdl_22_1_0.csv",
        SHFE_HDR,
        &[
            shfe("rb2601", "2026-11-13", "00:30:00.000", 1),
            shfe("rb2601", "2026-11-13", "09:30:00.250", 2),
            shfe("rb2601", "2026-11-16", "21:00:00.000", 3), // Monday's night session
        ],
    );
}

fn setup() -> (tempfile::TempDir, Cache) {
    let tmp = tempfile::tempdir().unwrap();
    let msg = tmp.path().join("msg");
    fixture(&msg);
    let cache = Cache::new(msg, tmp.path().join("cache"), 3);
    (tmp, cache)
}

fn sel(td: &str, products: &[&str], contracts: &[&str]) -> Selection {
    Selection {
        trade_day: Some(td.into()),
        products: products.iter().map(|s| s.to_string()).collect(),
        contracts: contracts.iter().map(|s| s.to_string()).collect(),
        ..Selection::default()
    }
}

fn frames(p: &plan::Plan) -> Vec<String> {
    p.entries
        .iter()
        .map(|e| {
            let (s, r) = p.row(*e);
            String::from_utf8(s.frame(r).to_vec()).unwrap()
        })
        .collect()
}

#[test]
fn trade_day_spans_night_session_and_keeps_file_order() {
    let (_t, cache) = setup();
    let p = plan::prepare(&cache, &sel("20261113", &["RB", "ag"], &[])).unwrap();
    assert_eq!(p.summary.dirs, vec![20261112, 20261113]);
    let pre = "mdl.22.1.* || ";
    assert_eq!(
        frames(&p),
        vec![
            format!("{pre}{}", shfe("rb2601", "2026-11-13", "21:00:00.500", 2)),
            // ag's LocalTime jitters back by 100 ms; source order is kept.
            format!("{pre}{}", shfe("ag2702", "2026-11-13", "21:00:00.400", 3)),
            format!("{pre}{}", shfe("rb2601", "2026-11-13", "23:59:59.900", 4)),
            format!("{pre}{}", shfe("rb2601", "2026-11-13", "00:30:00.000", 1)),
            format!("{pre}{}", shfe("rb2601", "2026-11-13", "09:30:00.250", 2)),
        ]
    );
    assert_eq!(p.summary.contracts, 2);
}

#[test]
fn channels_interleave_by_receive_time() {
    let (_t, cache) = setup();
    let p = plan::prepare(&cache, &sel("20261113", &[], &[])).unwrap();
    let got = frames(&p);
    let topics: Vec<&str> = got.iter().map(|f| &f[..10]).collect();
    // Thursday dir: AP (CZCE, 21:00:00.450) precedes the SHFE rows (>= .500 after
    // the running max); Friday dir: SHFE 00:30, CFFEX .200, SHFE .250, CFFEX .300.
    assert_eq!(
        topics,
        vec![
            "mdl.23.1.*",
            "mdl.22.1.*",
            "mdl.22.1.*",
            "mdl.22.1.*", // Thursday dir
            "mdl.22.1.*",
            "mdl.21.1.*",
            "mdl.22.1.*",
            "mdl.21.1.*", // Friday dir
        ]
    );
    // Thursday's own day session and Monday's night are excluded.
    assert!(got
        .iter()
        .all(|f| !f.contains("2026-11-12,2026-11-12") && !f.contains("2026-11-16")));
}

#[test]
fn czce_three_digit_contract_matches_four_digit_request() {
    let (_t, cache) = setup();
    let p = plan::prepare(&cache, &sel("20261113", &[], &["AP2601"])).unwrap();
    assert_eq!(p.entries.len(), 1);
    let p = plan::prepare(&cache, &sel("20261113", &[], &["ap601"])).unwrap();
    assert_eq!(p.entries.len(), 1);
}

#[test]
fn cache_hits_then_rebuilds_when_source_grows() {
    let (t, cache) = setup();
    let ch = Channel::parse("mdl_22_1_0").unwrap();
    let (s, f) = cache.shard(20261113, &ch, false).unwrap().unwrap();
    assert_eq!((f, s.index.rows), (Fetch::Built, 3));
    assert_eq!(
        cache.shard(20261113, &ch, false).unwrap().unwrap().1,
        Fetch::Hit
    );
    // A fresh process re-validates the on-disk shard.
    let cold = Cache::new(cache.msg_root.clone(), cache.cache_root.clone(), 2);
    assert_eq!(
        cold.shard(20261113, &ch, false).unwrap().unwrap().1,
        Fetch::Hit
    );
    // Vendor appends one full row and half of the next.
    let path = t.path().join("msg/20261113/mdl_22_1_0.csv");
    let mut body = fs::read_to_string(&path).unwrap();
    body.push_str(&shfe("rb2601", "2026-11-16", "21:00:01.000", 4));
    body.push_str("\nrb2601,2026-11-16,20");
    fs::write(&path, body).unwrap();
    let (s, f) = cache.shard(20261113, &ch, false).unwrap().unwrap();
    assert_eq!((f, s.index.rows), (Fetch::Built, 4));
}

#[test]
fn missing_channel_file_is_skipped() {
    let (_t, cache) = setup();
    let mut s = sel("20261113", &[], &[]);
    s.channels = vec!["mdl_26_1_0".into(), "mdl_22_1_0".into()];
    let p = plan::prepare(&cache, &s).unwrap();
    assert_eq!(
        p.summary.per_channel.keys().collect::<Vec<_>>(),
        vec!["mdl_22_1_0"]
    );
}

#[test]
fn zmq_session_sends_exact_frames_with_ctl_events() {
    let (t, cache) = setup();
    let plan = Arc::new(plan::prepare(&cache, &sel("20261113", &[], &[])).unwrap());
    let expected = frames(&plan);
    let ep = format!("ipc://{}", t.path().join("replay.sock").display());
    let ctx = zmq::Context::new();
    let params = Params {
        endpoint: Some(ep.clone()),
        speed: 0.0,
        hold_open_s: 5,
        settle_ms: 50,
        ..Params::default()
    };
    let session = publish::spawn(ctx.clone(), 7, plan, params, String::new(), "127.0.0.1").unwrap();
    assert_eq!(session.endpoint, ep);
    let sub = ctx.socket(zmq::SUB).unwrap();
    sub.set_subscribe(b"").unwrap();
    sub.set_rcvtimeo(5000).unwrap();
    sub.connect(&ep).unwrap();
    let mut got = Vec::new();
    let mut events = Vec::new();
    loop {
        let f = String::from_utf8(sub.recv_bytes(0).unwrap()).unwrap();
        if let Some(body) = f.strip_prefix(&format!("{CTL_TOPIC} || ")) {
            let v: serde_json::Value = serde_json::from_str(body).unwrap();
            events.push(v["event"].as_str().unwrap().to_string());
            if v["event"] == "end" {
                assert_eq!(v["sent"], expected.len() as u64);
                break;
            }
        } else {
            got.push(f);
        }
    }
    assert_eq!(events, vec!["begin", "end"]);
    assert_eq!(got, expected);
    drop(sub);
    let deadline = Instant::now() + Duration::from_secs(10);
    while session.ctl.state() != State::Done && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(session.ctl.state(), State::Done);
    assert_eq!(
        session.ctl.sent.load(Ordering::Relaxed),
        expected.len() as u64
    );
}

#[test]
fn paced_replay_follows_receive_time() {
    let (t, cache) = setup();
    // IF2612 on Friday: 09:30:00.200 -> .300, i.e. 100 ms; at speed 2 -> ~50 ms.
    let mut s = sel("20261113", &[], &["IF2612"]);
    s.channels = vec!["mdl_21_1_0".into()];
    let plan = Arc::new(plan::prepare(&cache, &s).unwrap());
    assert_eq!(plan.entries.len(), 2);
    let ep = format!("ipc://{}", t.path().join("paced.sock").display());
    let ctx = zmq::Context::new();
    let params = Params {
        endpoint: Some(ep.clone()),
        speed: 2.0,
        settle_ms: 20,
        hold_open_s: 1,
        ..Params::default()
    };
    let _session = publish::spawn(ctx.clone(), 8, plan, params, String::new(), "h").unwrap();
    let sub = ctx.socket(zmq::SUB).unwrap();
    sub.set_subscribe(b"mdl.").unwrap();
    sub.set_rcvtimeo(5000).unwrap();
    sub.connect(&ep).unwrap();
    sub.recv_bytes(0).unwrap();
    let t0 = Instant::now();
    sub.recv_bytes(0).unwrap();
    let dt = t0.elapsed();
    assert!(
        dt >= Duration::from_millis(45) && dt < Duration::from_millis(80),
        "{dt:?}"
    );
}

#[test]
fn lru_eviction_removes_least_recently_used_day() {
    let (t, cache) = setup();
    let ch = Channel::parse("mdl_22_1_0").unwrap();
    cache.shard(20261112, &ch, false).unwrap();
    cache.shard(20261113, &ch, false).unwrap();
    let v1 = t.path().join("cache/v1");
    let age = |day: &str, secs: u64| {
        let f = fs::File::create(v1.join(day).join(".last_used")).unwrap();
        f.set_modified(std::time::SystemTime::now() - Duration::from_secs(secs))
            .unwrap();
    };
    age("20261112", 7200);
    age("20261113", 3600);
    // A fresh process (no in-memory recent-use grace) with a 1-byte budget.
    let cold = Cache::new(cache.msg_root.clone(), cache.cache_root.clone(), 1).with_max_bytes(1);
    let r = cold.evict().unwrap();
    assert_eq!(r.removed_days, vec![20261112, 20261113]);
    assert_eq!(r.bytes_after, 0);
    // Recently used days survive even over budget (rebuilt + touched by `cold`).
    cold.shard(20261113, &ch, false).unwrap();
    let r = cold.evict().unwrap();
    assert!(r.removed_days.is_empty() && r.bytes_after > 0, "{r:?}");
    // A generous budget keeps everything.
    let roomy =
        Cache::new(cache.msg_root.clone(), cache.cache_root.clone(), 1).with_max_bytes(1 << 30);
    assert!(roomy.evict().unwrap().removed_days.is_empty());
}
