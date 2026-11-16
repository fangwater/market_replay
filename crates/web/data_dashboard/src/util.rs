use chrono::{DateTime, SecondsFormat, Utc};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

pub fn now_utc() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn fmt_time(t: SystemTime) -> String {
    DateTime::<Utc>::from(t).to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).ok()?.modified().ok()
}

pub fn read_json(path: &Path) -> anyhow::Result<serde_json::Value> {
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// `YYYYMMDD` -> `YYYY-MM-DD` (unchanged when not 8 digits).
pub fn dash_day(d: &str) -> String {
    if d.len() == 8 && d.bytes().all(|b| b.is_ascii_digit()) {
        format!("{}-{}-{}", &d[..4], &d[4..6], &d[6..])
    } else {
        d.to_string()
    }
}

pub fn is_day_name(s: &str) -> bool {
    s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit())
}

/// Tiny bounded LRU with per-entry TTL (drill-down results).
pub struct Lru<K, V> {
    cap: usize,
    ttl: Duration,
    map: HashMap<K, (Instant, V)>,
    order: VecDeque<K>,
}

impl<K: Eq + Hash + Clone, V: Clone> Lru<K, V> {
    pub fn new(cap: usize, ttl: Duration) -> Self {
        Self {
            cap,
            ttl,
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, k: &K) -> Option<V> {
        let fresh = match self.map.get(k) {
            Some((t, _)) => t.elapsed() < self.ttl,
            None => return None,
        };
        if !fresh {
            self.map.remove(k);
            self.order.retain(|x| x != k);
            return None;
        }
        self.order.retain(|x| x != k);
        self.order.push_back(k.clone());
        self.map.get(k).map(|(_, v)| v.clone())
    }

    pub fn put(&mut self, k: K, v: V) {
        if self.map.insert(k.clone(), (Instant::now(), v)).is_some() {
            self.order.retain(|x| x != &k);
        }
        self.order.push_back(k);
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_oldest() {
        let mut l = Lru::new(2, Duration::from_secs(60));
        l.put(1, "a");
        l.put(2, "b");
        assert_eq!(l.get(&1), Some("a"));
        l.put(3, "c");
        assert_eq!(l.get(&2), None);
        assert_eq!(l.get(&1), Some("a"));
        assert_eq!(l.len(), 2);
    }

    #[test]
    fn days() {
        assert_eq!(dash_day("20261116"), "2026-11-16");
        assert!(is_day_name("20261116"));
        assert!(!is_day_name("2026111"));
    }
}
