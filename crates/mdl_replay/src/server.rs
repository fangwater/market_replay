//! Request/response control plane on a ROUTER socket (clients use REQ).
//!
//! Every request is one JSON frame with `cmd`; every reply is one JSON frame
//! with `ok`. Heavy commands run on worker threads so the loop stays responsive.

use crate::cache::{Cache, Fetch};
use crate::config::Config;
use crate::plan::{self, PlanSummary, Selection};
use crate::publish::{self, Params, Session};
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const REPLIES: &str = "inproc://mdl-replay-replies";

struct Entry {
    session: Session,
    summary: PlanSummary,
}

pub struct Server {
    cfg: Config,
    ctx: zmq::Context,
    cache: Arc<Cache>,
    sessions: Arc<Mutex<BTreeMap<u64, Arc<Entry>>>>,
    next_id: AtomicU64,
    jobs: Arc<AtomicUsize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    cmd: String,
    #[serde(default)]
    select: Option<Selection>,
    #[serde(default)]
    params: Option<Params>,
    #[serde(default)]
    session: Option<u64>,
    #[serde(default)]
    force: bool,
}

impl Server {
    pub fn new(cfg: Config) -> Result<Self> {
        let cache = Arc::new(cfg.cache());
        cache.dirs().context("msg_root is not readable")?;
        std::fs::create_dir_all(&cfg.cache_root)?;
        Ok(Self {
            ctx: zmq::Context::new(),
            cache,
            sessions: Arc::default(),
            next_id: AtomicU64::new(1),
            jobs: Arc::default(),
            cfg,
        })
    }

    pub fn run(self: Arc<Self>) -> Result<()> {
        let router = self.ctx.socket(zmq::ROUTER)?;
        router.set_linger(0)?;
        router
            .bind(&self.cfg.control_endpoint)
            .with_context(|| format!("bind control {}", self.cfg.control_endpoint))?;
        let replies = self.ctx.socket(zmq::PULL)?;
        replies.bind(REPLIES)?;
        eprintln!(
            "mdl-replay: control {} msg_root {:?} cache {:?}",
            self.cfg.control_endpoint, self.cfg.msg_root, self.cfg.cache_root
        );
        let mut last_reap = Instant::now();
        let mut last_evict: Option<Instant> = None;
        loop {
            let mut items = [
                router.as_poll_item(zmq::POLLIN),
                replies.as_poll_item(zmq::POLLIN),
            ];
            zmq::poll(&mut items, 200)?;
            if items[0].is_readable() {
                let frames = router.recv_multipart(0)?;
                // REQ envelope: [identity, "", body]
                if frames.len() == 3 && frames[1].is_empty() {
                    let ident = frames[0].clone();
                    if let Some(reply) = self.clone().dispatch(ident.clone(), &frames[2]) {
                        router.send_multipart(
                            [ident, Vec::new(), reply.to_string().into_bytes()],
                            0,
                        )?;
                    }
                }
            }
            if items[1].is_readable() {
                let mut frames = replies.recv_multipart(0)?;
                if frames.len() == 2 {
                    let body = frames.pop().unwrap();
                    let ident = frames.pop().unwrap();
                    router.send_multipart([ident, Vec::new(), body], 0)?;
                }
            }
            if last_reap.elapsed() > Duration::from_secs(5) {
                self.reap();
                last_reap = Instant::now();
            }
            if last_evict.is_none_or(|t| t.elapsed() > Duration::from_secs(600)) {
                last_evict = Some(Instant::now());
                let cache = self.cache.clone();
                std::thread::spawn(move || match cache.evict() {
                    Ok(r) if !r.removed_days.is_empty() || r.removed_temp > 0 => {
                        eprintln!(
                            "mdl-replay: cache eviction {}",
                            serde_json::to_string(&r).unwrap_or_default()
                        )
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("mdl-replay: cache eviction failed: {e:#}"),
                });
            }
        }
    }

    fn reap(&self) {
        let now = publish::now_unix();
        self.sessions.lock().unwrap().retain(|_, e| {
            let fin = e.session.ctl.finished_unix.load(Ordering::Relaxed);
            let keep = !(e.session.ctl.state().finished()
                && fin > 0
                && now.saturating_sub(fin) > self.cfg.session_ttl_s);
            if !keep {
                e.session.ctl.close.store(true, Ordering::Relaxed);
            }
            keep
        });
    }

    /// Returns the reply now, or `None` when a worker will reply later.
    fn dispatch(self: Arc<Self>, ident: Vec<u8>, body: &[u8]) -> Option<Value> {
        let req: Request = match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => return Some(err(format!("bad request: {e}"))),
        };
        match req.cmd.as_str() {
            "replay" | "build" | "symbols" => {
                if self.jobs.fetch_add(1, Ordering::AcqRel) >= self.cfg.max_jobs {
                    self.jobs.fetch_sub(1, Ordering::AcqRel);
                    return Some(err(format!(
                        "busy: {} heavy requests running",
                        self.cfg.max_jobs
                    )));
                }
                let me = self.clone();
                std::thread::spawn(move || {
                    let reply = wrap(me.heavy(req));
                    me.jobs.fetch_sub(1, Ordering::AcqRel);
                    let push = me.ctx.socket(zmq::PUSH).and_then(|s| {
                        s.connect(REPLIES)?;
                        Ok(s)
                    });
                    if let Ok(push) = push {
                        let _ = push.send_multipart([ident, reply.to_string().into_bytes()], 0);
                    }
                });
                None
            }
            _ => Some(wrap(self.light(req))),
        }
    }

    fn light(&self, req: Request) -> Result<Value> {
        let session = || -> Result<Arc<Entry>> {
            let id = req.session.context("missing session")?;
            self.sessions
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .with_context(|| format!("no session {id}"))
        };
        match req.cmd.as_str() {
            "ping" => Ok(json!({"version": env!("CARGO_PKG_VERSION"), "sessions": self.sessions.lock().unwrap().len()})),
            "days" => {
                let dirs = self.cache.dirs()?;
                Ok(json!({"first": dirs.first(), "last": dirs.last(), "count": dirs.len(), "dirs": dirs}))
            }
            "cache" => Ok(serde_json::to_value(self.cache.evict()?)?),
            "status" => Ok(describe(&*session()?)),
            "sessions" => Ok(Value::Array(self.sessions.lock().unwrap().values().map(|e| describe(e)).collect())),
            "start" => flag(&*session()?, |c| &c.start, true),
            "pause" => flag(&*session()?, |c| &c.pause, true),
            "resume" => flag(&*session()?, |c| &c.pause, false),
            "cancel" => flag(&*session()?, |c| &c.cancel, true),
            "close" => {
                let e = session()?;
                e.session.ctl.cancel.store(true, Ordering::Relaxed);
                e.session.ctl.close.store(true, Ordering::Relaxed);
                self.sessions.lock().unwrap().remove(&e.session.id);
                Ok(json!({"closed": e.session.id}))
            }
            other => bail!("unknown cmd {other:?}; expected ping|days|cache|symbols|build|replay|status|sessions|start|pause|resume|cancel|close"),
        }
    }

    fn heavy(&self, req: Request) -> Result<Value> {
        let sel = req.select.clone().context("missing select")?;
        match req.cmd.as_str() {
            "symbols" => Ok(json!({ "symbols": plan::symbols(&self.cache, &sel)? })),
            "build" => {
                let t = Instant::now();
                let (_, _, dirs) = plan::resolve_dirs(&self.cache, &sel)?;
                let chans = plan::channels(&sel)?;
                let shards = plan::load_shards(&self.cache, &dirs, &chans, req.force)?;
                let list: Vec<Value> = shards
                    .iter()
                    .map(|(d, _, s, f)| {
                        json!({
                            "dir": d, "channel": s.index.channel, "rows": s.index.rows,
                            "bytes": s.index.frames_bytes, "complete": s.index.complete,
                            "built": *f == Fetch::Built, "build_ms": s.index.build_ms,
                        })
                    })
                    .collect();
                Ok(json!({"shards": list, "elapsed_ms": t.elapsed().as_millis() as u64}))
            }
            "replay" => {
                if self.sessions.lock().unwrap().len() >= self.cfg.max_sessions {
                    self.reap();
                    if self.sessions.lock().unwrap().len() >= self.cfg.max_sessions {
                        bail!(
                            "max_sessions {} reached; close finished sessions",
                            self.cfg.max_sessions
                        );
                    }
                }
                let params = req.params.clone().unwrap_or_default();
                let plan = Arc::new(plan::prepare(&self.cache, &sel)?);
                if plan.entries.is_empty() {
                    bail!(
                        "selection matched no rows: {}",
                        serde_json::to_string(&plan.summary)?
                    );
                }
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let summary = plan.summary.clone();
                let session = publish::spawn(
                    self.ctx.clone(),
                    id,
                    plan,
                    params,
                    self.cfg.session_endpoint(),
                    &self.cfg.advertise_host,
                )?;
                let entry = Arc::new(Entry { session, summary });
                self.sessions.lock().unwrap().insert(id, entry.clone());
                Ok(describe(&entry))
            }
            _ => unreachable!(),
        }
    }
}

fn flag(
    e: &Entry,
    field: impl Fn(&publish::Control) -> &std::sync::atomic::AtomicBool,
    v: bool,
) -> Result<Value> {
    if e.session.ctl.state().finished() {
        bail!(
            "session {} already {:?}",
            e.session.id,
            e.session.ctl.state()
        );
    }
    field(&e.session.ctl).store(v, Ordering::Relaxed);
    Ok(describe(e))
}

fn describe(e: &Entry) -> Value {
    let s = &e.session;
    json!({
        "session": s.id,
        "endpoint": s.endpoint,
        "ctl_topic": publish::CTL_TOPIC,
        "params": s.params,
        "plan": e.summary,
        "status": publish::status(&s.ctl, s.total, s.params.repeat),
        "created_unix": s.created_unix,
    })
}

fn err(msg: String) -> Value {
    json!({"ok": false, "error": msg})
}

fn wrap(r: Result<Value>) -> Value {
    match r {
        Ok(Value::Object(mut m)) => {
            m.insert("ok".into(), Value::Bool(true));
            Value::Object(m)
        }
        Ok(v) => json!({"ok": true, "result": v}),
        Err(e) => err(format!("{e:#}")),
    }
}

pub fn request(endpoint: &str, body: &str, timeout_ms: i32) -> Result<Value> {
    let ctx = zmq::Context::new();
    let req = ctx.socket(zmq::REQ)?;
    req.set_linger(0)?;
    req.set_rcvtimeo(timeout_ms)?;
    req.connect(endpoint)?;
    req.send(body, 0)?;
    let reply = req
        .recv_bytes(0)
        .with_context(|| format!("no reply from {endpoint} within {timeout_ms} ms"))?;
    Ok(serde_json::from_slice(&reply)?)
}
