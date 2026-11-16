//! `market-replay-server`: the one long-running process of market_replay.
//!
//! - CN futures market service: gRPC `cn_market.v1` on the cn_market config's
//!   `listen` (18312) and the dominant-contract HTTP API on the shared HTTP
//!   port (18313).
//! - Supervises the long-running children in `[[service]]` (restart with
//!   backoff) and runs the daily `[[job]]`s on their schedules through one
//!   queue. Status, history, logs and manual runs: `/admin` + `/api/admin/*`.
//!
//! PM2 manages only this process (and PostgreSQL); see AGENTS.md.

mod api;
mod config;
mod manager;
mod process;

use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::sync::watch;

#[derive(Parser)]
#[command(name = "market-replay-server")]
struct Cli {
    #[arg(long)]
    config: PathBuf,
    /// Validate the config and print the schedule, then exit.
    #[arg(long)]
    check: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::Config::load(&cli.config)?;
    if let Some(env) = &cfg.env_file {
        if env.exists() {
            let n = config::load_env_file(env)?;
            eprintln!("market-replay-server: loaded {n} variables from {env:?}");
        } else {
            eprintln!("market-replay-server: {env:?} not found; DingTalk disabled");
        }
    }
    if cli.check {
        for s in &cfg.services {
            println!(
                "service {:<28} enabled={} {:?} {:?}",
                s.name, s.enabled, s.run.command, s.run.args
            );
        }
        for j in &cfg.jobs {
            println!(
                "job     {:<28} enabled={} {} {:?} {:?}",
                j.name,
                j.enabled,
                j.schedule.at.join(",") + " " + &j.schedule.tz,
                j.run.command,
                j.run.args
            );
        }
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?
        .block_on(run(cfg))
}

async fn run(cfg: config::Config) -> Result<()> {
    let (stop_tx, stop_rx) = watch::channel(false);
    let market_cfg = cn_kline_server::Config::load(&cfg.cn_market.config)?;
    let grpc_addr: SocketAddr = market_cfg.listen.parse().context("cn_market listen")?;
    // Children first: the market metadata comes from the reference service,
    // which is one of them.
    let manager = manager::Manager::new(cfg.clone(), stop_rx.clone())?;
    let tasks = manager.start();
    let mut tries = 0;
    let market = loop {
        match cn_kline_server::components(market_cfg.clone()).await {
            Ok(m) => break m,
            Err(e) if tries < 60 => {
                tries += 1;
                eprintln!(
                    "market-replay-server: cn_market not ready ({e:#}); retry {tries}/60 in 5 s"
                );
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(e) => {
                let _ = stop_tx.send(true);
                for t in tasks {
                    let _ = t.await;
                }
                return Err(e.context("cn_market metadata never became available"));
            }
        }
    };

    let mut http = api::router(manager.clone(), cfg.dashboard.is_some());
    if let Some(dominant) = market.http {
        http = http.merge(dominant);
    }
    if let Some(d) = &cfg.dashboard {
        // Same port: UI at `/`, `/api/dashboard/*`; the page reads
        // `/api/admin/*` for services and jobs. Its refresher is one task on
        // this runtime, its IO bounded by the dashboard's `io_permits`.
        let dcfg = data_dashboard::Config::load(&d.config)?;
        let state = data_dashboard::AppState::new(dcfg)?;
        data_dashboard::spawn_background(state.clone());
        http = http.merge(data_dashboard::router(state));
    }
    let listener = tokio::net::TcpListener::bind(&cfg.http.listen)
        .await
        .with_context(|| format!("bind {}", cfg.http.listen))?;
    eprintln!(
        "market-replay-server: HTTP on {}, gRPC on {grpc_addr}",
        cfg.http.listen
    );

    let mut s1 = stop_rx.clone();
    let http_task = tokio::spawn(async move {
        let app = http.into_make_service_with_connect_info::<SocketAddr>();
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = s1.wait_for(|v| *v).await;
            })
            .await
        {
            eprintln!("market-replay-server: HTTP stopped: {e}");
        }
    });
    let mut s2 = stop_rx.clone();
    let grpc_task = tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(market.grpc)
            .serve_with_shutdown(grpc_addr, async move {
                let _ = s2.wait_for(|v| *v).await;
            })
            .await
        {
            eprintln!("market-replay-server: gRPC stopped: {e}");
        }
    });

    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    eprintln!("market-replay-server: stopping (children get SIGTERM)");
    let _ = stop_tx.send(true);
    for t in tasks {
        let _ = t.await;
    }
    market.refresh.abort();
    let _ = http_task.await;
    let _ = grpc_task.await;
    eprintln!("market-replay-server: stopped");
    Ok(())
}
