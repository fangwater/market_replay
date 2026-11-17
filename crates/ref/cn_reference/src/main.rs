//! `cn-reference`: daily OpenCTP sync (the scheduled job), a standalone API
//! server (what `market-replay-server` embeds), and offline import/parse
//! helpers used for verification.
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use cn_reference::api::AppState;
use cn_reference::models::{DEFAULT_SESSION_SOURCE_URL, DEFAULT_SOURCE_URL};
use cn_reference::storage::{Db, PRODUCTION_SCHEMA};
use cn_reference::{database_url, json::J, services, source};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "CN futures reference data service tools")]
struct Cli {
    /// KEY=VALUE file holding CN_FUTURES_REF_DATABASE_URL when the variable is unset.
    #[arg(long, global = true)]
    env_file: Option<PathBuf>,
    /// PostgreSQL schema (production by default; tests use a throwaway one).
    #[arg(long, global = true, default_value = PRODUCTION_SCHEMA)]
    schema: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// One guarded sync of fees and continuous sessions (the daily job).
    Sync {
        #[arg(long, default_value = "CN_FUTURES_DB_DINGTALK_WEBHOOK_URL")]
        dingtalk_webhook_env: String,
        #[arg(long, default_value = "CN_FUTURES_DB_DINGTALK_SECRET")]
        dingtalk_secret_env: String,
    },
    /// Serve the /api/v1 API on its own (market-replay-server embeds this).
    Serve {
        #[arg(long, default_value = "0.0.0.0:8765")]
        listen: String,
        #[arg(long, default_value = "http://127.0.0.1:18766")]
        fallback: String,
    },
    /// Parse a saved fee page and print the records as JSON (no database).
    ParseHtml { file: PathBuf },
    /// Import a saved fee page into `--schema`.
    ImportHtml {
        file: PathBuf,
        #[arg(long)]
        source_date: Option<String>,
        #[arg(long, default_value = "file://fixture")]
        source_url: String,
    },
    /// Import a saved OpenCTP sessions JSON into `--schema`.
    ImportSessions {
        file: PathBuf,
        #[arg(long)]
        source_date: Option<String>,
        #[arg(long, default_value = "file://fixture")]
        source_url: String,
    },
}

async fn connect(cli: &Cli) -> Result<Db> {
    Db::connect(&database_url(cli.env_file.as_deref())?, &cli.schema).await
}

fn truncate(text: &str, max: usize) -> String {
    dingtalk::truncate(text, max)
}

fn notify(webhook_env: &str, secret_env: &str, text: &str) {
    match dingtalk::DingTalk::from_env(webhook_env, secret_env) {
        Ok(Some(d)) => {
            if let Err(e) = d.send_text(text) {
                eprintln!("cn-reference: DingTalk send failed: {e:#}");
            }
        }
        Ok(None) => eprintln!("cn-reference: {webhook_env} unset; DingTalk notification skipped"),
        Err(e) => eprintln!("cn-reference: DingTalk config invalid: {e:#}"),
    }
}

async fn run_sync(cli: &Cli, webhook_env: &str, secret_env: &str) -> Result<()> {
    let db = connect(cli).await?;
    let Some(lock) = db.try_daily_lock().await? else {
        println!(
            "{}",
            J::obj(vec![
                ("status", J::s("skipped")),
                ("reason", J::s("another daily sync holds the lock")),
            ])
            .dumps(false)
        );
        return Ok(());
    };
    let env_u64 = |k: &str, d: u64| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let timeout = env_u64("CN_FUTURES_REF_SYNC_TIMEOUT", 30);
    let retries = env_u64("CN_FUTURES_REF_SYNC_RETRIES", 2) as u32;
    let fees_url =
        std::env::var("CN_FUTURES_REF_SOURCE_URL").unwrap_or_else(|_| DEFAULT_SOURCE_URL.into());
    let sessions_url = std::env::var("CN_FUTURES_REF_SESSION_SOURCE_URL")
        .unwrap_or_else(|_| DEFAULT_SESSION_SOURCE_URL.into());
    let result = async {
        let fees = services::sync(&db, Some(&fees_url), None, timeout, retries).await?;
        let sessions =
            services::sync_continuous_sessions(&db, Some(&sessions_url), None, timeout, retries)
                .await?;
        let mut body = fees.body;
        body.set("tradingSessions", sessions.body);
        anyhow::Ok(body)
    }
    .await;
    Db::release_daily_lock(lock).await.ok();
    match result {
        Ok(body) => {
            println!("{}", body.dumps(false));
            let n = |j: &J, k: &str| match j.get(k) {
                Some(J::Int(i)) => *i,
                _ => 0,
            };
            let sess = body.get("tradingSessions").cloned().unwrap_or(J::Null);
            notify(
                webhook_env,
                secret_env,
                &truncate(
                    &format!(
                        "国内期货参考数据同步完成\n手续费与保证金：新增 {} 确认 {} 共 {}，来源日期 {}\n连续交易时段：新增 {} 确认 {} 停用 {} 共 {}",
                        n(&body, "inserted"),
                        n(&body, "confirmed"),
                        n(&body, "total"),
                        body.str_of("sourceDate").unwrap_or("?"),
                        n(&sess, "inserted"),
                        n(&sess, "confirmed"),
                        n(&sess, "deactivated"),
                        n(&sess, "total"),
                    ),
                    3000,
                ),
            );
            Ok(())
        }
        Err(e) => {
            notify(
                webhook_env,
                secret_env,
                &truncate(&format!("国内期货参考数据同步失败\n{e:#}"), 3000),
            );
            Err(e)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.cmd {
        Cmd::Sync {
            dingtalk_webhook_env,
            dingtalk_secret_env,
        } => run_sync(&cli, dingtalk_webhook_env, dingtalk_secret_env).await,
        Cmd::Serve { listen, fallback } => {
            let db = connect(&cli).await?;
            let state = AppState::new(db, fallback.clone())?;
            eprintln!("cn-reference: API on {listen}, fallback {fallback}");
            cn_reference::serve(state, listen, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await
        }
        Cmd::ParseHtml { file } => {
            let bytes = std::fs::read(file).with_context(|| format!("read {}", file.display()))?;
            let text = source::decode(&bytes, "text/html")?;
            let records = source::parse_html(&text)?;
            let date = source::source_date(&text, &records, None)?;
            let observed = services::observed_at(&records, &date)?;
            let rows: Vec<J> = records
                .iter()
                .map(|r| {
                    J::obj(vec![
                        ("venue", J::opt(r.get("venue").map(String::from))),
                        ("id", J::opt(r.get("instrument_id").map(String::from))),
                        ("hash", J::s(cn_reference::storage::record_hash(r))),
                        (
                            "cols",
                            J::Arr(
                                cn_reference::models::DB_COLUMNS
                                    .iter()
                                    .map(|c| J::opt(r.get(c).map(String::from)))
                                    .collect(),
                            ),
                        ),
                    ])
                })
                .collect();
            let out = J::obj(vec![
                ("count", J::Int(rows.len() as i64)),
                ("source_date", J::s(date)),
                (
                    "observed_at",
                    J::s(observed.format("%Y-%m-%dT%H:%M:%S+00:00").to_string()),
                ),
                ("records", J::Arr(rows)),
            ]);
            println!("{}", out.dumps(false));
            Ok(())
        }
        Cmd::ImportHtml {
            file,
            source_date,
            source_url,
        } => {
            let db = connect(&cli).await?;
            let bytes = std::fs::read(file)?;
            let out =
                services::import_html(&db, &bytes, source_url, source_date.as_deref(), "text/html")
                    .await?;
            println!("{}", out.body.dumps(false));
            Ok(())
        }
        Cmd::ImportSessions {
            file,
            source_date,
            source_url,
        } => {
            let db = connect(&cli).await?;
            let bytes = std::fs::read(file)?;
            let out = services::import_continuous_sessions(
                &db,
                &bytes,
                source_url,
                source_date.as_deref(),
                "application/json",
            )
            .await?;
            println!("{}", out.body.dumps(false));
            Ok(())
        }
    }
}
