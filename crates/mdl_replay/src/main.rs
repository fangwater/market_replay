use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use mdl_replay::config::Config;
use mdl_replay::plan::{self, Selection};
use mdl_replay::publish::{self, Params, CTL_TOPIC};
use mdl_replay::server::{self, Server};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(
    name = "mdl-replay",
    about = "Replay Tonglian MDL days as live-identical ZMQ frames"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the request/response server.
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Build (or validate) the shard cache for a day.
    Build {
        #[arg(long)]
        config: PathBuf,
        #[command(flatten)]
        sel: SelArgs,
        #[arg(long)]
        force: bool,
    },
    /// Show cache usage and run LRU eviction down to `cache_max_gb`.
    Cache {
        #[arg(long)]
        config: PathBuf,
    },
    /// One-shot local replay without the server.
    Play {
        #[arg(long)]
        config: PathBuf,
        #[command(flatten)]
        sel: SelArgs,
        #[command(flatten)]
        params: ParamArgs,
    },
    /// Send one JSON request to a server and print the reply.
    Req {
        #[arg(long, default_value = "tcp://127.0.0.1:13100")]
        endpoint: String,
        #[arg(long, default_value_t = 600_000)]
        timeout_ms: i32,
        json: String,
    },
    /// Subscribe, count and optionally dump frames (stops at the replay end event).
    Sub {
        #[arg(long)]
        endpoint: String,
        /// Topic prefixes; default everything.
        #[arg(long)]
        topic: Vec<String>,
        /// Write each frame + '\n' to this file.
        #[arg(long)]
        dump: Option<PathBuf>,
        #[arg(long, default_value_t = 3600)]
        max_seconds: u64,
    },
}

#[derive(Args)]
struct SelArgs {
    #[arg(long)]
    trade_day: Option<String>,
    #[arg(long)]
    natural_day: Option<String>,
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    contracts: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    channels: Vec<String>,
}

impl SelArgs {
    fn selection(&self) -> Selection {
        Selection {
            trade_day: self.trade_day.clone(),
            natural_day: self.natural_day.clone(),
            products: self.products.clone(),
            contracts: self.contracts.clone(),
            channels: self.channels.clone(),
        }
    }
}

#[derive(Args)]
struct ParamArgs {
    /// 1 = original pacing, 0 = as fast as possible.
    #[arg(long, default_value_t = 0.0)]
    speed: f64,
    /// Cap on replayed gaps; negative = keep all gaps.
    #[arg(long, default_value_t = 60_000)]
    max_gap_ms: i64,
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long, default_value_t = 1)]
    wait_subscribers: u32,
    #[arg(long, default_value_t = 1)]
    repeat: u32,
    /// Allow dropping on HWM instead of blocking.
    #[arg(long)]
    lossy: bool,
    /// Keep the socket up after the end (until subscribers leave).
    #[arg(long, default_value_t = 600)]
    hold_open_s: u64,
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Serve { config } => Arc::new(Server::new(Config::load(&config)?)?).run(),
        Cmd::Build { config, sel, force } => {
            let cfg = Config::load(&config)?;
            let cache = cfg.cache();
            let sel = sel.selection();
            let t = Instant::now();
            let (_, _, dirs) = plan::resolve_dirs(&cache, &sel)?;
            let chans = plan::channels(&sel)?;
            for (d, _, s, f) in plan::load_shards(&cache, &dirs, &chans, force)? {
                let i = &s.index;
                println!(
                    "{d} {:<11} rows {:>9} bytes {:>11} groups {:>5} complete {} {:?} read {} parse {} write {} total {} ms",
                    i.channel, i.rows, i.frames_bytes, i.groups.len(), i.complete, f,
                    i.build_ms.read, i.build_ms.parse, i.build_ms.write, i.build_ms.total
                );
            }
            println!("elapsed {} ms", t.elapsed().as_millis());
            Ok(())
        }
        Cmd::Cache { config } => {
            let report = Config::load(&config)?.cache().evict()?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Cmd::Play {
            config,
            sel,
            params,
        } => play(Config::load(&config)?, sel.selection(), params),
        Cmd::Req {
            endpoint,
            timeout_ms,
            json,
        } => {
            let reply = server::request(&endpoint, &json, timeout_ms)?;
            println!("{}", serde_json::to_string_pretty(&reply)?);
            Ok(())
        }
        Cmd::Sub {
            endpoint,
            topic,
            dump,
            max_seconds,
        } => sub(&endpoint, &topic, dump, max_seconds),
    }
}

fn play(cfg: Config, sel: Selection, a: ParamArgs) -> Result<()> {
    let cache = cfg.cache();
    let plan = Arc::new(plan::prepare(&cache, &sel)?);
    eprintln!("plan {}", serde_json::to_string(&plan.summary)?);
    anyhow::ensure!(!plan.entries.is_empty(), "selection matched no rows");
    let params = Params {
        speed: a.speed,
        max_gap_ms: (a.max_gap_ms >= 0).then_some(a.max_gap_ms as u64),
        endpoint: a.endpoint,
        wait_subscribers: a.wait_subscribers,
        repeat: a.repeat,
        lossless: !a.lossy,
        hold_open_s: a.hold_open_s,
        ..Params::default()
    };
    let s = publish::spawn(
        zmq::Context::new(),
        1,
        plan,
        params.clone(),
        cfg.session_endpoint(),
        &cfg.advertise_host,
    )?;
    eprintln!(
        "endpoint {} (waiting for {} subscriber(s))",
        s.endpoint, params.wait_subscribers
    );
    loop {
        std::thread::sleep(Duration::from_millis(1000));
        let st = publish::status(&s.ctl, s.total, params.repeat);
        let done = st.state.finished();
        eprintln!("{}", serde_json::to_string(&st)?);
        if done {
            eprintln!(
                "replay {:?}; holding the connection until subscribers leave (max {} s)",
                st.state, params.hold_open_s
            );
            break;
        }
    }
    if let Some(t) = s.thread.lock().unwrap().take() {
        let _ = t.join();
    }
    let st = publish::status(&s.ctl, s.total, params.repeat);
    println!("{}", serde_json::to_string_pretty(&st)?);
    anyhow::ensure!(
        st.state == publish::State::Done,
        "replay ended {:?}",
        st.state
    );
    Ok(())
}

fn sub(endpoint: &str, topics: &[String], dump: Option<PathBuf>, max_seconds: u64) -> Result<()> {
    let ctx = zmq::Context::new();
    let s = ctx.socket(zmq::SUB)?;
    s.set_rcvhwm(0)?;
    s.set_rcvtimeo(1000)?;
    if topics.is_empty() {
        s.set_subscribe(b"")?;
    } else {
        for t in topics {
            s.set_subscribe(t.as_bytes())?;
        }
        s.set_subscribe(CTL_TOPIC.as_bytes())?;
    }
    s.connect(endpoint)?;
    let mut out = dump
        .map(|p| {
            std::fs::File::create(&p)
                .with_context(|| format!("create {p:?}"))
                .map(|f| std::io::BufWriter::with_capacity(8 << 20, f))
        })
        .transpose()?;
    let mut counts: Vec<(Vec<u8>, u64)> = Vec::new();
    let (mut frames, mut bytes) = (0u64, 0u64);
    let mut first: Option<Instant> = None;
    let deadline = Instant::now() + Duration::from_secs(max_seconds);
    let mut msg = zmq::Message::new();
    let mut end = None;
    while Instant::now() < deadline {
        match s.recv(&mut msg, 0) {
            Ok(()) => {}
            Err(zmq::Error::EAGAIN) => continue,
            Err(e) => return Err(e.into()),
        }
        let f: &[u8] = &msg;
        let topic_end = memchr::memmem::find(f, b" || ").unwrap_or(f.len());
        let topic = &f[..topic_end];
        if topic == CTL_TOPIC.as_bytes() {
            let v: serde_json::Value =
                serde_json::from_slice(&f[(topic_end + 4).min(f.len())..]).unwrap_or_default();
            eprintln!("ctl {v}");
            if v["event"] == "begin" {
                first = Some(Instant::now());
            }
            if v["event"] == "end" {
                end = Some(v);
                break;
            }
            continue;
        }
        first.get_or_insert_with(Instant::now);
        frames += 1;
        bytes += f.len() as u64;
        match counts.iter_mut().find(|(t, _)| t.as_slice() == topic) {
            Some((_, c)) => *c += 1,
            None => counts.push((topic.to_vec(), 1)),
        }
        if let Some(o) = out.as_mut() {
            o.write_all(f)?;
            o.write_all(b"\n")?;
        }
    }
    if let Some(o) = out.as_mut() {
        o.flush()?;
    }
    let secs = first.map_or(0.0, |t| t.elapsed().as_secs_f64());
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "frames": frames, "bytes": bytes,
            "per_topic": counts.iter().map(|(t, c)| (String::from_utf8_lossy(t).into_owned(), *c)).collect::<BTreeMap<_, _>>(), "seconds": secs,
            "msgs_per_s": frames as f64 / secs.max(1e-9), "end": end,
            "complete": end.as_ref().is_some_and(|e| e["sent"].as_u64() == Some(frames)),
        }))?
    );
    Ok(())
}
