//! DingTalk robot notifications, following mkt_signal `fr_monitor` rules:
//! credentials only from environment variables, destination pinned to
//! `https://oapi.dingtalk.com`, and errors never echo the URL or response
//! body (both may contain the secret token).

use anyhow::{ensure, Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// DingTalk rejects text messages above ~4 KB; keep a margin.
pub const MAX_CONTENT_BYTES: usize = 3800;

pub struct DingTalk {
    client: reqwest::blocking::Client,
    url: reqwest::Url,
    secret: Option<String>,
}

impl DingTalk {
    /// `Ok(None)` when the webhook variable is unset (notifications disabled).
    pub fn from_env(webhook_env: &str, secret_env: &str) -> Result<Option<Self>> {
        let Ok(raw) = std::env::var(webhook_env) else {
            return Ok(None);
        };
        let url = reqwest::Url::parse(raw.trim())
            .map_err(|_| anyhow::anyhow!("invalid webhook URL in {webhook_env}"))?;
        ensure!(
            url.scheme() == "https"
                && url.host_str() == Some("oapi.dingtalk.com")
                && url.username().is_empty()
                && url.password().is_none(),
            "invalid DingTalk webhook destination in {webhook_env}"
        );
        let secret = std::env::var(secret_env)
            .ok()
            .filter(|s| !s.trim().is_empty());
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("build DingTalk HTTP client")?;
        Ok(Some(Self {
            client,
            url,
            secret,
        }))
    }

    pub fn send_text(&self, content: &str) -> Result<()> {
        let content = truncate(content, MAX_CONTENT_BYTES);
        let mut url = self.url.clone();
        if let Some(secret) = &self.secret {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .to_string();
            url.query_pairs_mut()
                .append_pair("timestamp", &timestamp)
                .append_pair("sign", &sign(secret, &timestamp)?);
        }
        let response = self
            .client
            .post(url)
            .json(&serde_json::json!({
                "msgtype": "text", "text": {"content": content}, "at": {"isAtAll": false}
            }))
            .send()
            .map_err(|_| anyhow::anyhow!("DingTalk transport failed"))?;
        ensure!(
            response.status().is_success(),
            "DingTalk HTTP failure: {}",
            response.status().as_u16()
        );
        let body: serde_json::Value = response
            .json()
            .map_err(|_| anyhow::anyhow!("invalid DingTalk response"))?;
        ensure!(
            body["errcode"].as_i64() == Some(0),
            "DingTalk rejected notice"
        );
        Ok(())
    }
}

/// `base64(HMAC-SHA256(secret, "{timestamp}\n{secret}"))`, per DingTalk docs.
pub fn sign(secret: &str, timestamp: &str) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(format!("{timestamp}\n{secret}").as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut end = max.saturating_sub(16);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n…(已截断)", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches_reference_vector() {
        // Expected value computed independently with Python hmac/hashlib.
        assert_eq!(
            sign("SEC", "1600000000000").unwrap(),
            "C72kx4c0NUGpRzKRqG/YBFvxO6ddH42i13qMig+aiB8="
        );
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        let t = truncate(&"交易".repeat(2000), 100);
        assert!(t.len() <= 100 + 20 && t.ends_with("(已截断)"));
    }

    #[test]
    fn rejects_non_dingtalk_destination() {
        std::env::set_var(
            "DT_TEST_URL",
            "https://example.com/robot/send?access_token=x",
        );
        assert!(DingTalk::from_env("DT_TEST_URL", "DT_TEST_SECRET").is_err());
        std::env::remove_var("DT_TEST_URL");
        assert!(DingTalk::from_env("DT_TEST_URL", "DT_TEST_SECRET")
            .unwrap()
            .is_none());
    }
}
