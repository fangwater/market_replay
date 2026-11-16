//! Sends stdin as one DingTalk text message using
//! CN_FUTURES_DB_DINGTALK_WEBHOOK_URL / CN_FUTURES_DB_DINGTALK_SECRET.
//! Exits 0 without sending when the webhook is not configured.
use std::io::Read;

fn main() -> anyhow::Result<()> {
    let mut text = String::new();
    std::io::stdin().read_to_string(&mut text)?;
    let text = text.trim();
    anyhow::ensure!(!text.is_empty(), "empty message on stdin");
    match dingtalk::DingTalk::from_env(
        "CN_FUTURES_DB_DINGTALK_WEBHOOK_URL",
        "CN_FUTURES_DB_DINGTALK_SECRET",
    )? {
        Some(d) => d.send_text(text),
        None => {
            eprintln!("DingTalk not configured; message not sent");
            Ok(())
        }
    }
}
