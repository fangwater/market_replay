//! Thin binary: serves `data_dashboard::router` on `listen`.
use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "数据处理进度与品种查询（只读）")]
struct Cli {
    #[arg(long, default_value = "config/data_dashboard.toml")]
    config: PathBuf,
    /// Overrides `listen` from the config (e.g. a test port).
    #[arg(long)]
    listen: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut cfg = data_dashboard::Config::load(&cli.config)?;
    if let Some(l) = cli.listen {
        cfg.listen = l;
    }
    let workers = cfg.workers.clamp(1, 4);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(cfg.io_permits.clamp(1, 8))
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let listen = cfg.listen.clone();
        let state = data_dashboard::AppState::new(cfg)?;
        data_dashboard::spawn_background(state.clone());
        let app = data_dashboard::router(state);
        let listener = tokio::net::TcpListener::bind(&listen)
            .await
            .with_context(|| format!("bind {listen}"))?;
        eprintln!("data-dashboard listening on http://{listen}/");
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
        Ok(())
    })
}
