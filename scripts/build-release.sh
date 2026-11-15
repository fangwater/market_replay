#!/usr/bin/env bash
# Release build; profile copied from mkt_signal (thin LTO, codegen-units=4, stripped).
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ne 0 ]]; then
  echo "Usage: scripts/build-release.sh" >&2
  exit 2
fi
cd "$ROOT_DIR"
# librocksdb-sys runs bindgen; point it at the system libclang (same shim mkt_signal uses).
if [[ -z "${LIBCLANG_PATH:-}" ]]; then
  if [[ -e "$HOME/libclang-shim/libclang.so" ]]; then LIBCLANG_PATH="$HOME/libclang-shim"; else LIBCLANG_PATH=/usr/lib/llvm-18/lib; fi
fi
export LIBCLANG_PATH
cargo build --release --locked -p mdl_replay --bin mdl-replay
cargo build --release --locked -p cn_futures_l2 --bins
cargo build --release --locked -p cn_futures_trim -p cn_daily -p cn_kline_server
echo "[INFO] built release binaries under $ROOT_DIR/target/release"
sha256sum "$ROOT_DIR/target/release/mdl-replay" "$ROOT_DIR/target/release/cn_futures_l2" "$ROOT_DIR/target/release/export_baseline_1min" \
  "$ROOT_DIR/target/release/trim_cn_futures_continuous_sessions" "$ROOT_DIR/target/release/cn-daily" \
  "$ROOT_DIR/target/release/cn-kline-server"
