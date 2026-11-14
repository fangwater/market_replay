//! `mdl_22_1_0.csv` <-> live topic `mdl.22.1.*`.

use anyhow::{bail, Result};

/// The six futures quote channels `cn-futures-pbs --zmq` subscribes to.
pub const DEFAULT_CHANNELS: [&str; 6] = [
    "mdl_21_1_0",
    "mdl_22_1_0",
    "mdl_22_3_0",
    "mdl_23_1_0",
    "mdl_24_1_0",
    "mdl_26_1_0",
];

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Channel {
    /// File stem, e.g. `mdl_22_1_0`.
    pub name: String,
    /// Live topic, e.g. `mdl.22.1.*`.
    pub topic: String,
    prefix: Vec<u8>,
}

impl Channel {
    /// Accepts `mdl_22_1_0`, `mdl_22_1_0.csv` or `mdl.22.1.*`.
    pub fn parse(text: &str) -> Result<Self> {
        let stem = text.strip_suffix(".csv").unwrap_or(text);
        let parts: Vec<&str> = stem.split(['_', '.']).collect();
        let ok = parts.len() == 4
            && parts[0] == "mdl"
            && parts[1..3]
                .iter()
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
            && (parts[3] == "*"
                || parts[3].bytes().all(|b| b.is_ascii_digit()) && !parts[3].is_empty());
        if !ok {
            bail!("invalid MDL channel {text:?}; expected e.g. mdl_22_1_0 or mdl.22.1.*");
        }
        let suffix = if parts[3] == "*" { "0" } else { parts[3] };
        let topic = format!("mdl.{}.{}.*", parts[1], parts[2]);
        Ok(Self {
            name: format!("mdl_{}_{}_{}", parts[1], parts[2], suffix),
            prefix: format!("{topic} || ").into_bytes(),
            topic,
        })
    }

    /// `<topic> || `, the exact bytes the company Tonglian forwarder prepends.
    pub fn frame_prefix(&self) -> &[u8] {
        &self.prefix
    }

    pub fn file_name(&self) -> String {
        format!("{}.csv", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_topics() {
        for text in ["mdl_22_1_0", "mdl_22_1_0.csv", "mdl.22.1.*"] {
            let c = Channel::parse(text).unwrap();
            assert_eq!(c.name, "mdl_22_1_0");
            assert_eq!(c.topic, "mdl.22.1.*");
            assert_eq!(c.frame_prefix(), b"mdl.22.1.* || ");
        }
        assert!(Channel::parse("state").is_err());
    }
}
