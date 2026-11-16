//! Fixed resource budget for the LSEG delivery tools.
//!
//! Every tool sizes its own pools from these constants or an explicit
//! `--workers` flag; nothing scales with the machine's CPU or memory. Daily
//! runs are serialized by the PM2 driver, so independent tasks never share
//! the budget concurrently.

/// Source aggregation workers (each holds one RIC's streaming state).
pub const AGGREGATION_WORKERS: usize = 2;
/// Parquet publication threads (NFS publish is metadata-latency bound;
/// 8 measured 2.7–3.0x faster than serial, 16 no better).
pub const IO_THREADS: usize = 8;
