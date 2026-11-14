//! One replay session: an XPUB socket that sends the planned frames.
//!
//! SUB clients (e.g. `cn-futures-pbs --zmq`) connect to it exactly like to the
//! company Tonglian forwarder. Session events go on topic `replay.ctl`, which
//! the six `mdl.*` subscriptions never match.

use crate::day::MS_PER_DAY;
use crate::plan::Plan;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const CTL_TOPIC: &str = "replay.ctl";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Params {
    /// 1.0 = original pacing, 10 = 10x, 0 = as fast as possible.
    #[serde(default)]
    pub speed: f64,
    /// Gaps longer than this (receive time) are shortened to it; null = keep all.
    #[serde(default = "default_max_gap")]
    pub max_gap_ms: Option<u64>,
    /// Bind address; default `tcp://<bind_host>:*` (ephemeral port).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Auto-start once this many subscribers connected and subscribed.
    #[serde(default = "one")]
    pub wait_subscribers: u32,
    /// Wait for an explicit `start` command instead.
    #[serde(default)]
    pub manual_start: bool,
    /// Pause after subscribers are ready, letting their filters settle.
    #[serde(default = "default_settle")]
    pub settle_ms: u64,
    /// Cancel if not started within this time.
    #[serde(default = "default_start_timeout")]
    pub start_timeout_s: u64,
    /// Block on slow subscribers instead of dropping (XPUB_NODROP).
    #[serde(default = "yes")]
    pub lossless: bool,
    #[serde(default = "default_hwm")]
    pub sndhwm: i32,
    /// Replay the plan this many times (stress tests).
    #[serde(default = "one")]
    pub repeat: u32,
    /// Keep the socket this long after the end so buffered frames drain.
    #[serde(default = "default_linger")]
    pub linger_ms: i32,
    /// After the end event keep the connection up this long (or until all
    /// subscribers disconnect / the session is closed) so receivers drain.
    #[serde(default = "default_hold")]
    pub hold_open_s: u64,
}

fn default_max_gap() -> Option<u64> {
    Some(60_000)
}
fn one() -> u32 {
    1
}
fn yes() -> bool {
    true
}
fn default_settle() -> u64 {
    200
}
fn default_start_timeout() -> u64 {
    600
}
fn default_hwm() -> i32 {
    1_000_000
}
fn default_hold() -> u64 {
    600
}
fn default_linger() -> i32 {
    30_000
}

impl Default for Params {
    fn default() -> Self {
        serde_json::from_str("{}").expect("defaults")
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Waiting = 0,
    Running = 1,
    Paused = 2,
    Done = 3,
    Cancelled = 4,
    Failed = 5,
}

impl State {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Waiting,
            1 => Self::Running,
            2 => Self::Paused,
            3 => Self::Done,
            4 => Self::Cancelled,
            _ => Self::Failed,
        }
    }
    pub fn finished(self) -> bool {
        matches!(self, Self::Done | Self::Cancelled | Self::Failed)
    }
}

#[derive(Default)]
pub struct Control {
    state: AtomicU8,
    pub start: AtomicBool,
    pub pause: AtomicBool,
    pub cancel: AtomicBool,
    /// Release the socket (ends the post-replay hold).
    pub close: AtomicBool,
    pub sent: AtomicU64,
    pub sent_bytes: AtomicU64,
    pub backpressure_waits: AtomicU64,
    pub subscribers: AtomicU32,
    pub loop_index: AtomicU32,
    /// Receive time (ms of day) of the last sent row.
    pub cursor_ms: AtomicU32,
    pub run_us: AtomicU64,
    pub finished_unix: AtomicU64,
    pub error: Mutex<Option<String>>,
}

impl Control {
    pub fn state(&self) -> State {
        State::from_u8(self.state.load(Ordering::Acquire))
    }
    fn set(&self, s: State) {
        self.state.store(s as u8, Ordering::Release);
    }
}

#[derive(Serialize)]
pub struct Status {
    pub state: State,
    pub sent: u64,
    pub sent_bytes: u64,
    pub total: u64,
    pub loop_index: u32,
    pub repeat: u32,
    pub subscribers: u32,
    pub cursor_local: String,
    pub backpressure_waits: u64,
    pub run_ms: f64,
    pub msgs_per_s: f64,
    pub mb_per_s: f64,
    pub error: Option<String>,
}

pub fn status(ctl: &Control, total: u64, repeat: u32) -> Status {
    let sent = ctl.sent.load(Ordering::Relaxed);
    let bytes = ctl.sent_bytes.load(Ordering::Relaxed);
    let us = ctl.run_us.load(Ordering::Relaxed).max(1) as f64;
    Status {
        state: ctl.state(),
        sent,
        sent_bytes: bytes,
        total: total * repeat as u64,
        loop_index: ctl.loop_index.load(Ordering::Relaxed),
        repeat,
        subscribers: ctl.subscribers.load(Ordering::Relaxed),
        cursor_local: crate::plan::fmt_ms(ctl.cursor_ms.load(Ordering::Relaxed)),
        backpressure_waits: ctl.backpressure_waits.load(Ordering::Relaxed),
        run_ms: us / 1000.0,
        msgs_per_s: sent as f64 / us * 1e6,
        mb_per_s: bytes as f64 / us,
        error: ctl.error.lock().unwrap().clone(),
    }
}

struct Pub {
    sock: zmq::Socket,
    monitor: zmq::Socket,
    raw: *mut std::ffi::c_void,
    lossless: bool,
    subscriptions: u64,
}

// SAFETY: the socket is created and used only on the session thread.
unsafe impl Send for Pub {}

impl Pub {
    fn bind(ctx: &zmq::Context, endpoint: &str, p: &Params, id: u64) -> Result<(Self, String)> {
        let mut sock = ctx.socket(zmq::XPUB)?;
        sock.set_sndhwm(p.sndhwm)?;
        sock.set_linger(p.linger_ms)?;
        sock.set_xpub_verbose(true)?;
        let raw = sock.as_mut_ptr();
        if p.lossless {
            let one: i32 = 1;
            // SAFETY: valid socket pointer, option takes an int.
            let rc = unsafe {
                zmq_sys::zmq_setsockopt(
                    raw,
                    zmq_sys::ZMQ_XPUB_NODROP as i32,
                    (&one as *const i32).cast(),
                    4,
                )
            };
            if rc != 0 {
                bail!(
                    "set ZMQ_XPUB_NODROP: {}",
                    zmq::Error::from_raw(unsafe { zmq_sys::zmq_errno() })
                );
            }
        }
        let mon = format!("inproc://mdl-replay-monitor-{id}");
        sock.monitor(
            &mon,
            (zmq::SocketEvent::HANDSHAKE_SUCCEEDED as i32)
                | (zmq::SocketEvent::DISCONNECTED as i32),
        )?;
        let monitor = ctx.socket(zmq::PAIR)?;
        monitor.set_linger(0)?;
        monitor.connect(&mon)?;
        sock.bind(endpoint)
            .with_context(|| format!("bind replay endpoint {endpoint}"))?;
        let bound = sock
            .get_last_endpoint()?
            .map_err(|_| anyhow::anyhow!("non-UTF-8 endpoint"))?;
        Ok((
            Self {
                sock,
                monitor,
                raw,
                lossless: p.lossless,
                subscriptions: 0,
            },
            bound,
        ))
    }

    /// Drains monitor events and XPUB subscription frames.
    fn pump(&mut self, ctl: &Control) -> Result<()> {
        loop {
            match self.monitor.recv_bytes(zmq::DONTWAIT) {
                Ok(ev) => {
                    if self.monitor.get_rcvmore()? {
                        let _ = self.monitor.recv_bytes(0)?;
                    }
                    if ev.len() >= 2 {
                        let code = u16::from_ne_bytes([ev[0], ev[1]]);
                        if code == zmq::SocketEvent::HANDSHAKE_SUCCEEDED.to_raw() {
                            ctl.subscribers.fetch_add(1, Ordering::Relaxed);
                        } else if code == zmq::SocketEvent::DISCONNECTED.to_raw() {
                            let _ = ctl.subscribers.fetch_update(
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                |v| v.checked_sub(1),
                            );
                        }
                    }
                }
                Err(zmq::Error::EAGAIN) => break,
                Err(zmq::Error::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        loop {
            match self.sock.recv_bytes(zmq::DONTWAIT) {
                Ok(m) => {
                    if m.first() == Some(&1) {
                        self.subscriptions += 1;
                    }
                }
                Err(zmq::Error::EAGAIN) => return Ok(()),
                Err(zmq::Error::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Hot path: one `zmq_send` from the mmap slice (libzmq copies once).
    #[inline]
    fn send(&mut self, frame: &[u8], ctl: &Control) -> Result<()> {
        let flags = if self.lossless {
            zmq_sys::ZMQ_DONTWAIT as i32
        } else {
            0
        };
        loop {
            // SAFETY: raw is this thread's live socket; frame outlives the call.
            let rc =
                unsafe { zmq_sys::zmq_send(self.raw, frame.as_ptr().cast(), frame.len(), flags) };
            if rc >= 0 {
                return Ok(());
            }
            let err = zmq::Error::from_raw(unsafe { zmq_sys::zmq_errno() });
            match err {
                zmq::Error::EINTR => continue,
                zmq::Error::EAGAIN => {
                    ctl.backpressure_waits.fetch_add(1, Ordering::Relaxed);
                    self.pump(ctl)?;
                    if ctl.cancel.load(Ordering::Relaxed) {
                        bail!("cancelled");
                    }
                    let _ = self.sock.poll(zmq::POLLOUT, 5);
                }
                e => return Err(e.into()),
            }
        }
    }

    fn ctl(&mut self, json: &serde_json::Value, ctl: &Control) -> Result<()> {
        let frame = format!("{CTL_TOPIC} || {json}");
        self.send(frame.as_bytes(), ctl)
    }
}

pub struct Session {
    pub id: u64,
    pub endpoint: String,
    pub ctl: Arc<Control>,
    pub params: Params,
    pub total: u64,
    pub created_unix: u64,
    pub thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Binds the socket and starts the session thread; returns once bound.
pub fn spawn(
    ctx: zmq::Context,
    id: u64,
    plan: Arc<Plan>,
    params: Params,
    default_endpoint: String,
    advertise_host: &str,
) -> Result<Session> {
    anyhow::ensure!(
        params.speed >= 0.0 && params.speed.is_finite(),
        "speed must be >= 0"
    );
    anyhow::ensure!(params.repeat >= 1, "repeat must be >= 1");
    let endpoint = params.endpoint.clone().unwrap_or(default_endpoint);
    let ctl = Arc::new(Control::default());
    let total = plan.entries.len() as u64;
    let (tx, rx) = mpsc::sync_channel::<Result<String>>(1);
    let thread = {
        let (ctl, params) = (ctl.clone(), params.clone());
        std::thread::Builder::new()
            .name(format!("replay-{id}"))
            .spawn(move || {
                let mut publisher = match Pub::bind(&ctx, &endpoint, &params, id) {
                    Ok((p, bound)) => {
                        let _ = tx.send(Ok(bound));
                        p
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                };
                let result = run(&mut publisher, &plan, &params, &ctl, id);
                let end_state = match &result {
                    Ok(()) => State::Done,
                    Err(_) if ctl.cancel.load(Ordering::Relaxed) => State::Cancelled,
                    Err(e) => {
                        *ctl.error.lock().unwrap() = Some(format!("{e:#}"));
                        State::Failed
                    }
                };
                ctl.finished_unix.store(now_unix(), Ordering::Relaxed);
                let _ = publisher.ctl(
                    &serde_json::json!({
                        "event": "end",
                        "session": id,
                        "state": end_state,
                        "sent": ctl.sent.load(Ordering::Relaxed),
                    }),
                    &ctl,
                );
                ctl.set(end_state);
                // Stay connected: SUB clients such as cn-futures-pbs discard
                // still-queued frames once they see the TCP disconnect.
                let hold = Instant::now() + Duration::from_secs(params.hold_open_s);
                let mut seen = ctl.subscribers.load(Ordering::Relaxed) > 0;
                while Instant::now() < hold && !ctl.close.load(Ordering::Relaxed) {
                    if publisher.pump(&ctl).is_err() {
                        break;
                    }
                    let subs = ctl.subscribers.load(Ordering::Relaxed);
                    seen |= subs > 0;
                    if seen && subs == 0 {
                        break; // every subscriber left on its own
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                // Dropping the socket honours linger for anything still unsent.
            })?
    };
    let bound = rx.recv().context("session thread died")??;
    Ok(Session {
        id,
        endpoint: advertise(&bound, advertise_host),
        ctl,
        total,
        params,
        created_unix: now_unix(),
        thread: Mutex::new(Some(thread)),
    })
}

fn advertise(bound: &str, host: &str) -> String {
    match bound.strip_prefix("tcp://") {
        Some(rest) => match rest.rsplit_once(':') {
            Some((h, port)) if h == "0.0.0.0" || h == "*" || h == "[::]" => {
                format!("tcp://{host}:{port}")
            }
            _ => bound.to_string(),
        },
        None => bound.to_string(),
    }
}

fn wait_start(p: &mut Pub, params: &Params, ctl: &Control) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(params.start_timeout_s);
    let mut ready_at: Option<Instant> = None;
    loop {
        p.pump(ctl)?;
        if ctl.cancel.load(Ordering::Relaxed) {
            bail!("cancelled");
        }
        if ctl.start.load(Ordering::Relaxed) {
            return Ok(());
        }
        let subs = ctl.subscribers.load(Ordering::Relaxed);
        if !params.manual_start
            && subs >= params.wait_subscribers
            && p.subscriptions >= params.wait_subscribers as u64
        {
            let at = *ready_at.get_or_insert_with(Instant::now);
            if at.elapsed() >= Duration::from_millis(params.settle_ms) {
                return Ok(());
            }
        } else {
            ready_at = None;
        }
        if Instant::now() > deadline {
            ctl.cancel.store(true, Ordering::Relaxed);
            bail!("start timeout after {} s", params.start_timeout_s);
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Sleeps coarsely, then spins the last ~150 us for sub-millisecond pacing.
#[inline]
fn wait_until(target: Instant) {
    loop {
        let now = Instant::now();
        if now >= target {
            return;
        }
        let left = target - now;
        if left > Duration::from_micros(400) {
            std::thread::sleep(left - Duration::from_micros(150));
        } else {
            std::hint::spin_loop();
        }
    }
}

fn run(p: &mut Pub, plan: &Plan, params: &Params, ctl: &Control, id: u64) -> Result<()> {
    ctl.set(State::Waiting);
    wait_start(p, params, ctl)?;
    p.ctl(
        &serde_json::json!({
            "event": "begin",
            "session": id,
            "rows": plan.entries.len(),
            "repeat": params.repeat,
            "speed": params.speed,
        }),
        ctl,
    )?;
    ctl.set(State::Running);
    let started = Instant::now();
    let max_gap = params.max_gap_ms.unwrap_or(u64::MAX);
    let ns_per_ms = if params.speed > 0.0 {
        1e6 / params.speed
    } else {
        0.0
    };
    let (mut sent, mut sent_bytes) = (0u64, 0u64);
    for loop_index in 0..params.repeat {
        ctl.loop_index.store(loop_index, Ordering::Relaxed);
        let t0 = Instant::now();
        let mut virt_ms: u64 = 0;
        let mut prev_abs: Option<u64> = None;
        let mut paused_for = Duration::ZERO;
        for (i, e) in plan.entries.iter().enumerate() {
            let (shard, rec) = plan.row(*e);
            if ns_per_ms > 0.0 {
                let abs = crate::day::to_epoch_days(shard.index.dir_day) as u64 * MS_PER_DAY
                    + rec.ms as u64;
                if let Some(prev) = prev_abs {
                    virt_ms += abs.saturating_sub(prev).min(max_gap);
                }
                prev_abs = Some(abs);
                wait_until(
                    t0 + paused_for + Duration::from_nanos((virt_ms as f64 * ns_per_ms) as u64),
                );
            }
            p.send(shard.frame(rec), ctl)?;
            sent += 1;
            sent_bytes += rec.len as u64;
            if i & 1023 == 0 {
                ctl.sent.store(sent, Ordering::Relaxed);
                ctl.sent_bytes.store(sent_bytes, Ordering::Relaxed);
                ctl.cursor_ms.store(rec.ms, Ordering::Relaxed);
                ctl.run_us
                    .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                p.pump(ctl)?;
                if ctl.cancel.load(Ordering::Relaxed) {
                    bail!("cancelled");
                }
                if ctl.pause.load(Ordering::Relaxed) {
                    let at = Instant::now();
                    ctl.set(State::Paused);
                    while ctl.pause.load(Ordering::Relaxed) {
                        if ctl.cancel.load(Ordering::Relaxed) {
                            bail!("cancelled");
                        }
                        p.pump(ctl)?;
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    ctl.set(State::Running);
                    paused_for += at.elapsed();
                }
            }
        }
    }
    ctl.sent.store(sent, Ordering::Relaxed);
    ctl.sent_bytes.store(sent_bytes, Ordering::Relaxed);
    ctl.run_us
        .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
    if let Some(e) = plan.entries.last() {
        ctl.cursor_ms.store(plan.row(*e).1.ms, Ordering::Relaxed);
    }
    Ok(())
}
