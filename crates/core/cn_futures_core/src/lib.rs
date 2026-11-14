//! Pure CN futures market logic shared by live trading (mkt_signal) and the
//! post-market pipeline (market_replay). Keep this crate free of IO crates
//! (RocksDB, parquet, iceoryx, network): mkt_signal pins it by git rev.

pub mod baseline_1min;
pub mod codec;
pub mod events;
pub mod mdl;
pub mod segments;
pub mod session;
pub mod universe;
