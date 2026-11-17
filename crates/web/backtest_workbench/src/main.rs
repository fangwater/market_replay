use anyhow::Result;
use clap::Parser;
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    config: std::path::PathBuf,
    #[arg(long, default_value = "127.0.0.1:18314")]
    listen: String,
}
fn main() -> Result<()> {
    let cli = Cli::parse();
    let state = backtest_workbench::AppState::new(&cli.config)?;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(async {
            let listener = tokio::net::TcpListener::bind(&cli.listen).await?;
            eprintln!("backtest workbench: http://{}/backtest", cli.listen);
            axum::serve(listener, backtest_workbench::router(state))
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
            Ok(())
        })
}
