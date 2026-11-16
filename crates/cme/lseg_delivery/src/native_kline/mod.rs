//! Direct RocksDB -> bounded in-memory state -> daily Parquet.
//! No Python, market-event JSON, external calendar, Databento or IB input.
pub mod engine;
pub mod io;
mod publication;
pub mod session;

pub const DAY: u64 = 86_400_000_000_000;
pub const MINUTE: u64 = 60_000_000_000;
pub const BATCH: usize = 8192;

pub use crate::tasdb::component;
