//! Domestic-futures roll tables.
//!
//! Rust port of `/home/fanghaizhou/preprocess/cn_futures` `roll_replay
//! --eod-input` (dominant selection, additive prev_close_spread adjustment,
//! continuous prices and continuity audit). Output files are byte-identical
//! to the Python implementation, including Python `repr(float)` formatting
//! and `csv` module `\r\n` line endings.
//!
//! The EOD cross-section extractor (Tonglian L2 / msg -> one row per contract
//! per TradDay) is the `cn_futures_roll` binary in `src/main.rs`.

pub mod adjustment;
pub mod dominant;
pub mod instrument;
pub mod pyfmt;
pub mod replay;
