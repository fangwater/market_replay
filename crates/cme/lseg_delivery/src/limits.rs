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

/// Workers of `lseg_l2_slice` (one file each) for ONE-OFF BACKFILLS only.
///
/// Exception to `AGGREGATION_WORKERS` approved by the user for that tool alone
/// (2026-11-17): a worker needs about 70 MiB (one raw line plus a row-group
/// buffer), the work is CPU-bound gzip + parsing and the source files are
/// independent. The default stays `AGGREGATION_WORKERS`; the daily path must not
/// use more. Never scale this with the machine.
pub const L2_SLICE_WORKERS: usize = 8;

/// Workers of `lseg_tas_parquet` (one product column family each) for ONE-OFF HISTORICAL BACKFILLS only.
///
/// Exception to `AGGREGATION_WORKERS` approved by the user for that tool alone (2026-11-24, the 2020-2025
/// trade / quote export). The default stays `AGGREGATION_WORKERS`; daily runs (`lseg-daily`) must not use more.
/// Never scale this with the machine.
pub const TAS_EXPORT_WORKERS: usize = 8;

/// Workers of `tas_open_interest sync --backfill` (one source gzip each) for ONE-OFF BACKFILLS only.
///
/// Exception to `AGGREGATION_WORKERS` approved by the user for that tool alone (2026-11-25, the 2020+
/// package and daily open interest load): a worker needs about 0.7 GiB (one compressed window and one
/// member buffer) and the work is CPU-bound inflate + scanning of independent files. The daily path
/// (`lseg-daily`) must not use more than `AGGREGATION_WORKERS`. Never scale this with the machine.
pub const OI_BACKFILL_WORKERS: usize = 8;
